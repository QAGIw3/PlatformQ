from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col, lit
from pyflink.table.udf import udtf
import json
from datetime import datetime

def activity_stream_job():
    """
    A Flink job that consumes events from all tenant topics, transforms them
    into a unified format, and sinks them into Cassandra and MinIO data lake.
    """
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    
    # Enable checkpointing for fault tolerance
    env.enable_checkpointing(10000)  # 10 seconds
    
    t_env = StreamTableEnvironment.create(stream_environment=env)
    
    # Configure job parameters
    PULSAR_SERVICE_URL = "pulsar://pulsar:6650"
    PULSAR_ADMIN_URL = "http://pulsar:8080"
    CASSANDRA_HOST = "cassandra"
    CASSANDRA_PORT = 9042
    MINIO_ENDPOINT = "http://minio:9000"
    MINIO_ACCESS_KEY = "minioadmin"
    MINIO_SECRET_KEY = "minioadmin"
    
    # Register UDFs for custom transformations
    t_env.create_temporary_function("extract_tenant_id", extract_tenant_id_from_topic)
    t_env.create_temporary_function("generate_uuid", generate_uuid)
    
    # 1. Create source tables for different event types
    # User events source
    t_env.execute_sql(f"""
        CREATE TABLE user_events_source (
            event_timestamp BIGINT,
            user_id STRING,
            email STRING,
            full_name STRING,
            tenant_id STRING,
            roles ARRAY<STRING>,
            created_at TIMESTAMP(3),
            `__metadata_topic` STRING METADATA VIRTUAL,
            WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'pulsar',
            'service-url' = '{PULSAR_SERVICE_URL}',
            'admin-url' = '{PULSAR_ADMIN_URL}',
            'topic-pattern' = 'persistent://platformq/.*/user-events',
            'scan.startup.mode' = 'latest',
            'format' = 'avro'
        )
    """)
    
    # Document events source
    t_env.execute_sql(f"""
        CREATE TABLE document_events_source (
            event_timestamp BIGINT,
            tenant_id STRING,
            document_id STRING,
            document_path STRING,
            document_name STRING,
            saved_by_user_id STRING,
            file_size BIGINT,
            file_type STRING,
            updated_at TIMESTAMP(3),
            `__metadata_topic` STRING METADATA VIRTUAL,
            WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'pulsar',
            'service-url' = '{PULSAR_SERVICE_URL}',
            'admin-url' = '{PULSAR_ADMIN_URL}',
            'topic-pattern' = 'persistent://platformq/.*/document-events',
            'scan.startup.mode' = 'latest',
            'format' = 'avro'
        )
    """)
    
    # Digital asset events source
    t_env.execute_sql(f"""
        CREATE TABLE asset_events_source (
            event_timestamp BIGINT,
            tenant_id STRING,
            asset_id STRING,
            asset_name STRING,
            asset_type STRING,
            source_tool STRING,
            created_by STRING,
            created_at TIMESTAMP(3),
            raw_data_uri STRING,
            metadata MAP<STRING, STRING>,
            `__metadata_topic` STRING METADATA VIRTUAL,
            WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'pulsar',
            'service-url' = '{PULSAR_SERVICE_URL}',
            'admin-url' = '{PULSAR_ADMIN_URL}',
            'topic-pattern' = 'persistent://platformq/.*/digital-asset-created-events',
            'scan.startup.mode' = 'latest',
            'format' = 'avro'
        )
    """)
    
    # Project events source
    t_env.execute_sql(f"""
        CREATE TABLE project_events_source (
            event_timestamp BIGINT,
            tenant_id STRING,
            project_id STRING,
            project_name STRING,
            created_by STRING,
            status STRING,
            created_at TIMESTAMP(3),
            `__metadata_topic` STRING METADATA VIRTUAL,
            WATERMARK FOR created_at AS created_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'pulsar',
            'service-url' = '{PULSAR_SERVICE_URL}',
            'admin-url' = '{PULSAR_ADMIN_URL}',
            'topic-pattern' = 'persistent://platformq/.*/project-events',
            'scan.startup.mode' = 'latest',
            'format' = 'avro'
        )
    """)
    
    # 2. Create the unified sink table for Cassandra
    t_env.execute_sql(f"""
        CREATE TABLE activity_stream_sink (
            tenant_id STRING,
            event_timestamp TIMESTAMP(3),
            event_id STRING,
            user_id STRING,
            event_source STRING,
            event_type STRING,
            entity_type STRING,
            entity_id STRING,
            details MAP<STRING, STRING>,
            PRIMARY KEY (tenant_id, event_timestamp, event_id) NOT ENFORCED
        ) WITH (
            'connector' = 'cassandra',
            'cassandra.host' = '{CASSANDRA_HOST}',
            'cassandra.port' = '{CASSANDRA_PORT}',
            'cassandra.keyspace' = 'platformq',
            'cassandra.table' = 'activity_stream',
            'cassandra.consistency' = 'LOCAL_QUORUM',
            'cassandra.ttl' = '2592000'  -- 30 days TTL
        )
    """)
    
    # 3. Create a sink table for MinIO Data Lake
    t_env.execute_sql(f"""
        CREATE TABLE data_lake_sink (
            tenant_id STRING,
            event_timestamp TIMESTAMP(3),
            event_id STRING,
            user_id STRING,
            event_source STRING,
            event_type STRING,
            entity_type STRING,
            entity_id STRING,
            details MAP<STRING, STRING>,
            year STRING,
            month STRING,
            day STRING,
            hour STRING
        ) PARTITIONED BY (tenant_id, year, month, day, hour) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://platformq-datalake/activity_stream/',
            'format' = 'parquet',
            'parquet.compression' = 'SNAPPY',
            'sink.partition-commit.trigger' = 'process-time',
            'sink.partition-commit.delay' = '1 hour',
            'sink.partition-commit.policy.kind' = 'metastore,success-file',
            'sink.rolling-policy.file-size' = '128MB',
            'sink.rolling-policy.rollover-interval' = '30 min',
            'sink.rolling-policy.check-interval' = '1 min'
        )
    """)
    
    # 4. Transform each source stream into the unified format
    # Transform user events
    user_events_transformed = t_env.sql_query("""
        SELECT
            extract_tenant_id(__metadata_topic) AS tenant_id,
            created_at AS event_timestamp,
            generate_uuid() AS event_id,
            user_id,
            'auth-service' AS event_source,
            'USER_CREATED' AS event_type,
            'user' AS entity_type,
            user_id AS entity_id,
            MAP[
                'email', email,
                'full_name', full_name,
                'roles', CAST(roles AS STRING)
            ] AS details,
            CAST(YEAR(created_at) AS STRING) AS year,
            CAST(MONTH(created_at) AS STRING) AS month,
            CAST(DAY(created_at) AS STRING) AS day,
            CAST(HOUR(created_at) AS STRING) AS hour
        FROM user_events_source
    """)
    
    # Transform document events
    document_events_transformed = t_env.sql_query("""
        SELECT
            tenant_id,
            updated_at AS event_timestamp,
            generate_uuid() AS event_id,
            saved_by_user_id AS user_id,
            'document-service' AS event_source,
            'DOCUMENT_UPDATED' AS event_type,
            'document' AS entity_type,
            document_id AS entity_id,
            MAP[
                'document_id', document_id,
                'document_name', document_name,
                'path', document_path,
                'file_type', file_type,
                'file_size', CAST(file_size AS STRING)
            ] AS details,
            CAST(YEAR(updated_at) AS STRING) AS year,
            CAST(MONTH(updated_at) AS STRING) AS month,
            CAST(DAY(updated_at) AS STRING) AS day,
            CAST(HOUR(updated_at) AS STRING) AS hour
        FROM document_events_source
    """)
    
    # Transform asset events
    asset_events_transformed = t_env.sql_query("""
        SELECT
            tenant_id,
            created_at AS event_timestamp,
            generate_uuid() AS event_id,
            created_by AS user_id,
            'digital-asset-service' AS event_source,
            'ASSET_CREATED' AS event_type,
            'digital_asset' AS entity_type,
            asset_id AS entity_id,
            MAP[
                'asset_id', asset_id,
                'asset_name', asset_name,
                'asset_type', asset_type,
                'source_tool', COALESCE(source_tool, 'unknown')
            ] AS details,
            CAST(YEAR(created_at) AS STRING) AS year,
            CAST(MONTH(created_at) AS STRING) AS month,
            CAST(DAY(created_at) AS STRING) AS day,
            CAST(HOUR(created_at) AS STRING) AS hour
        FROM asset_events_source
    """)
    
    # Transform project events
    project_events_transformed = t_env.sql_query("""
        SELECT
            tenant_id,
            created_at AS event_timestamp,
            generate_uuid() AS event_id,
            created_by AS user_id,
            'projects-service' AS event_source,
            'PROJECT_CREATED' AS event_type,
            'project' AS entity_type,
            project_id AS entity_id,
            MAP[
                'project_id', project_id,
                'project_name', project_name,
                'status', status
            ] AS details,
            CAST(YEAR(created_at) AS STRING) AS year,
            CAST(MONTH(created_at) AS STRING) AS month,
            CAST(DAY(created_at) AS STRING) AS day,
            CAST(HOUR(created_at) AS STRING) AS hour
        FROM project_events_source
    """)
    
    # 5. Union all transformed streams
    unified_stream = user_events_transformed.union_all(
        document_events_transformed
    ).union_all(
        asset_events_transformed  
    ).union_all(
        project_events_transformed
    )
    
    # 6. Create temporary view for the unified stream
    t_env.create_temporary_view("unified_activity_stream", unified_stream)
    
    # 7. Insert into both sinks
    # Insert into Cassandra (hot path for real-time queries)
    t_env.execute_sql("""
        INSERT INTO activity_stream_sink
        SELECT 
            tenant_id,
            event_timestamp,
            event_id,
            user_id,
            event_source,
            event_type,
            entity_type,
            entity_id,
            details
        FROM unified_activity_stream
    """)
    
    # Insert into Data Lake (cold path for historical analytics)
    t_env.execute_sql("""
        INSERT INTO data_lake_sink
        SELECT 
            tenant_id,
            event_timestamp,
            event_id,
            user_id,
            event_source,
            event_type,
            entity_type,
            entity_id,
            details,
            year,
            month,
            day,
            hour
        FROM unified_activity_stream
    """)
    
    # Execute the job
    env.execute("Unified Activity Stream Processing")

# User-defined functions
from pyflink.table import ScalarFunction
from pyflink.table.udf import udf
import uuid
import re

class ExtractTenantIdFromTopic(ScalarFunction):
    def eval(self, topic: str) -> str:
        """Extract tenant_id from Pulsar topic name"""
        # Topic format: persistent://platformq/{tenant_id}/...
        match = re.search(r'persistent://platformq/([^/]+)/', topic)
        if match:
            return match.group(1)
        return "unknown"

class GenerateUUID(ScalarFunction):
    def eval(self) -> str:
        """Generate a new UUID"""
        return str(uuid.uuid4())

# Register UDFs
extract_tenant_id_from_topic = udf(ExtractTenantIdFromTopic(), 
                                   result_type=DataTypes.STRING())
generate_uuid = udf(GenerateUUID(), result_type=DataTypes.STRING())

if __name__ == "__main__":
    activity_stream_job() 