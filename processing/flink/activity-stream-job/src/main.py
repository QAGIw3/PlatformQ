from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udtf

def activity_stream_job():
    """
    A Flink job that consumes events from all tenant topics, transforms them
    into a unified format, and sinks them into a Cassandra table.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_environment=env)

    # In a real job, connection details would come from a config file.
    PULSAR_SERVICE_URL = "pulsar://pulsar:6650"
    PULSAR_ADMIN_URL = "http://pulsar:8080"
    CASSANDRA_HOST = "cassandra"
    
    # 1. Create source tables. The 'format' = 'avro' will now
    # automatically use the schema from the Pulsar Schema Registry.
    t_env.execute_sql(f"""
        CREATE TABLE user_events_source (
            -- The schema is now inferred from the registry
            `event_timestamp` BIGINT,
            `user_id` STRING,
            `email` STRING,
            `full_name` STRING,
            `__metadata_topic` STRING METADATA VIRTUAL
        ) WITH (
            'connector' = 'pulsar',
            'service-url' = '{PULSAR_SERVICE_URL}',
            'admin-url' = '{PULSAR_ADMIN_URL}',
            'topic-pattern' = 'persistent://platformq/.*/user-events',
            'scan.startup.mode' = 'latest',
            'format' = 'avro'
        )
    """)
    t_env.execute_sql(f"""
        CREATE TABLE document_events_source (
            `event_timestamp` BIGINT,
            `tenant_id` STRING,
            `document_id` STRING,
            `document_path` STRING,
            `saved_by_user_id` STRING,
            `__metadata_topic` STRING METADATA VIRTUAL
        ) WITH (
            'connector' = 'pulsar',
            'service-url' = '{PULSAR_SERVICE_URL}',
            'admin-url' = '{PULSAR_ADMIN_URL}',
            'topic-pattern' = 'persistent://platformq/.*/document-events',
            'scan.startup.mode' = 'latest',
            'format' = 'avro'
        )
    """)

    # 2. Create the unified sink table for Cassandra
    t_env.execute_sql(f"""
        CREATE TABLE activity_stream_sink (
            `tenant_id` STRING,
            `event_timestamp` TIMESTAMP(3),
            `event_id` STRING,
            `user_id` STRING,
            `event_source` STRING,
            `event_type` STRING,
            `details` MAP<STRING, STRING>
        ) WITH (
            'connector' = 'cassandra',
            'host' = '{CASSANDRA_HOST}',
            'keyspace' = 'auth_keyspace'
        )
    """)

    # 2b. Create a sink table that writes to our Minio Data Lake
    t_env.execute_sql(f"""
        CREATE TABLE data_lake_sink (
            `tenant_id` STRING,
            `event_timestamp` TIMESTAMP(3),
            `user_id` STRING,
            `event_source` STRING,
            `event_type` STRING,
            `details` MAP<STRING, STRING>,
            `dt` STRING -- date partition
        ) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://platformq-datalake/activity_stream',
            'format' = 'parquet',
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.delay' = '1 min',
            'sink.partition-commit.policy.kind' = 'metastore,success-file'
        )
    """)

    # 3. Transform each source stream into the unified format
    user_events = t_env.from_path("user_events_source")
    transformed_user_events = user_events.select(
        # Extract tenant_id from topic: persistent://platformq/{tenant_id}/user-events
        "REGEXP_EXTRACT(__metadata_topic, 'platformq/(.*)/user-events', 1) AS tenant_id",
        "TO_TIMESTAMP_LTZ(event_timestamp, 3) AS event_timestamp",
        "UUID() AS event_id", # Generate a new UUID for the activity event
        "user_id",
        "'auth-service' AS event_source",
        "'USER_CREATED' AS event_type",
        # Create a map for the details
        "MAP('email', email, 'full_name', full_name) AS details"
    )

    doc_events = t_env.from_path("document_events_source")
    transformed_doc_events = doc_events.select(
        "tenant_id",
        "TO_TIMESTAMP_LTZ(event_timestamp, 3) AS event_timestamp",
        "UUID() AS event_id",
        "saved_by_user_id AS user_id",
        "'onlyoffice' AS event_source",
        "'DOCUMENT_UPDATED' AS event_type",
        "MAP('document_id', document_id, 'path', document_path) AS details"
    )

    # 4. Union the streams and sink them
    # Note: We are only sinking to Cassandra here for simplicity. A full implementation
    # would also sink the new document events to the data lake.
    unified_stream = transformed_user_events.union_all(transformed_doc_events)
    unified_stream.execute_insert("activity_stream_sink").wait()


if __name__ == '__main__':
    activity_stream_job() 