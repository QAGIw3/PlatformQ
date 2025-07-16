# SeaTunnel Service

## Purpose

The **SeaTunnel Service** provides a unified data integration layer for platformQ, enabling seamless data synchronization between various sources and sinks. It leverages Apache SeaTunnel's powerful connector ecosystem to support batch processing, real-time streaming, and Change Data Capture (CDC) workflows.

## Key Features

- **100+ Pre-built Connectors**: Support for databases, message queues, file systems, and APIs
- **Multiple Sync Modes**: Batch, streaming, and CDC for different use cases
- **Multi-tenant Isolation**: Complete data isolation between tenants
- **Kubernetes-native**: Jobs run as isolated Kubernetes Jobs with resource limits
- **Template Library**: Pre-configured templates for common integration patterns
- **Automatic Schema Evolution**: Handle schema changes gracefully
- **Monitoring & Metrics**: Track job performance and data quality

## Architecture

The service acts as a control plane for SeaTunnel jobs:

1. **API Layer**: RESTful API for job management
2. **Configuration Generator**: Converts high-level job specs to SeaTunnel configs
3. **Kubernetes Job Manager**: Launches and monitors SeaTunnel jobs
4. **Job Monitor**: Background thread tracking job status and metrics
5. **Event Publisher**: Publishes job completion events to Pulsar

## Supported Connectors

### Sources
- **Databases**: MySQL, PostgreSQL, Oracle, SQL Server, MongoDB, Cassandra
- **CDC**: MySQL CDC, PostgreSQL CDC, MongoDB CDC
- **Message Queues**: Kafka, Pulsar, RabbitMQ
- **File Systems**: S3, MinIO, HDFS, FTP
- **APIs**: HTTP, GraphQL, REST
- **Data Warehouses**: Snowflake, BigQuery, Redshift

### Sinks
- **Data Lake**: MinIO/S3 (Parquet, ORC, Avro)
- **Databases**: Cassandra, PostgreSQL, MySQL, MongoDB
- **Search**: Elasticsearch, OpenSearch
- **Message Queues**: Pulsar, Kafka
- **Cache**: Apache Ignite, Redis
- **Analytics**: ClickHouse, Doris

## API Endpoints

### Job Management

- `POST /api/v1/jobs`: Create a new SeaTunnel job
- `GET /api/v1/jobs`: List all jobs for the tenant
- `GET /api/v1/jobs/{job_id}`: Get job details
- `DELETE /api/v1/jobs/{job_id}`: Delete a job
- `POST /api/v1/jobs/{job_id}/run`: Manually trigger a job

### Templates

- `GET /api/v1/templates`: Get pre-configured job templates

## Job Configuration

### Basic Job Structure

```json
{
  "name": "MySQL to Data Lake Sync",
  "description": "Sync customer data to data lake",
  "source": {
    "connector_type": "jdbc",
    "connection_params": {
      "url": "jdbc:mysql://mysql:3306/mydb",
      "driver": "com.mysql.cj.jdbc.Driver",
      "user": "reader",
      "password": "password"
    },
    "query": "SELECT * FROM customers WHERE updated_at > '${last_run_time}'"
  },
  "sink": {
    "connector_type": "minio",
    "connection_params": {
      "endpoint": "http://minio:9000",
      "access_key": "minioadmin",
      "secret_key": "minioadmin"
    },
    "table_or_topic": "bronze/mysql/customers",
    "options": {
      "format": "parquet",
      "partition_by": ["year", "month", "day"]
    }
  },
  "transforms": [
    {
      "type": "sql",
      "config": {
        "query": "SELECT *, current_timestamp() as ingestion_time FROM customers"
      }
    }
  ],
  "sync_mode": "batch",
  "schedule": "0 */6 * * *",  // Every 6 hours
  "parallelism": 4
}
```

### CDC Configuration Example

```json
{
  "name": "PostgreSQL CDC to Pulsar",
  "source": {
    "connector_type": "postgres-cdc",
    "connection_params": {
      "hostname": "postgres",
      "port": 5432,
      "database": "mydb",
      "username": "postgres",
      "password": "password",
      "schema": "public",
      "table.whitelist": "users,orders,products",
      "slot.name": "platformq_cdc",
      "publication.name": "platformq_pub"
    }
  },
  "sink": {
    "connector_type": "pulsar",
    "connection_params": {
      "service-url": "pulsar://pulsar:6650",
      "admin-url": "http://pulsar:8080"
    },
    "table_or_topic": "postgres-cdc-events"
  },
  "sync_mode": "cdc"
}
```

### Streaming Configuration Example

```json
{
  "name": "Pulsar to Elasticsearch",
  "source": {
    "connector_type": "pulsar",
    "connection_params": {
      "service-url": "pulsar://pulsar:6650",
      "admin-url": "http://pulsar:8080",
      "subscription-name": "elasticsearch-indexer"
    },
    "table_or_topic": "activity-events"
  },
  "sink": {
    "connector_type": "elasticsearch",
    "connection_params": {
      "hosts": ["http://elasticsearch:9200"],
      "index": "activities",
      "index.type": "activity"
    }
  },
  "transforms": [
    {
      "type": "field_mapper",
      "config": {
        "field_mapper": {
          "event_timestamp": "timestamp",
          "user_id": "userId",
          "event_type": "type"
        }
      }
    }
  ],
  "sync_mode": "streaming",
  "checkpoint_interval": 5000
}
```

## Transform Operations

The service supports various transformation operations:

1. **SQL Transform**: Execute SQL queries on data
2. **Field Mapper**: Rename or map fields
3. **Filter**: Filter rows based on conditions
4. **Split**: Split one stream into multiple
5. **Join**: Join multiple data sources
6. **Aggregate**: Perform aggregations

## Multi-tenancy

The service ensures complete tenant isolation:

- Jobs run in tenant-specific Kubernetes namespaces
- Data paths automatically include tenant ID
- Pulsar topics are tenant-scoped
- Resource quotas enforced per tenant

## Monitoring

Each job tracks the following metrics:

- `rows_read`: Total rows read from source
- `rows_written`: Total rows written to sink
- `duration_ms`: Job execution time
- `throughput_rows_per_sec`: Processing throughput
- `error_count`: Number of errors encountered

## Events

### Events Produced

- **Topic**: `persistent://platformq/<tenant_id>/seatunnel-job-completed`
- **Schema**: Contains job ID, status, metrics, and completion time

## Integration with Platform

The SeaTunnel service integrates with other platform components:

1. **Hive Metastore**: Automatically registers tables created in data lake
2. **Trino**: Query synchronized data immediately
3. **Pulsar**: Use as source or sink for event streaming
4. **Workflow Service**: Trigger jobs via Airflow DAGs
5. **Graph Intelligence**: Sync relationship data to JanusGraph

## Deployment

The service is deployed as a Kubernetes Deployment with:

- Horizontal pod autoscaling based on API load
- Persistent volume for job metadata
- Service mesh integration for secure communication
- Prometheus metrics exposure

## Configuration

Environment variables:

- `DATABASE_URL`: Connection string for job metadata database
- `PULSAR_URL`: Pulsar broker URL
- `KUBERNETES_NAMESPACE`: Default namespace for jobs
- `MINIO_ENDPOINT`: MinIO endpoint for data lake
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key

## Best Practices

1. **Incremental Sync**: Use watermarks for incremental data sync
2. **Partitioning**: Partition data lake tables by date for efficient queries
3. **Schema Registry**: Use Pulsar schema registry for event schemas
4. **Error Handling**: Configure retry policies and dead letter queues
5. **Monitoring**: Set up alerts for failed jobs
6. **Resource Limits**: Configure appropriate CPU/memory limits for jobs

## Troubleshooting

### Common Issues

1. **Job Stuck in Pending**: Check Kubernetes resource availability
2. **Connection Errors**: Verify network policies and credentials
3. **Schema Mismatch**: Enable schema evolution or use transforms
4. **Performance Issues**: Increase parallelism or optimize queries

### Debugging

1. Check job logs: `kubectl logs -n <tenant-id> job/<job-name>`
2. View job status: `GET /api/v1/jobs/{job_id}`
3. Check Kubernetes events: `kubectl describe job -n <tenant-id> <job-name>`

## Examples

See the `/api/v1/templates` endpoint for ready-to-use job templates covering common integration patterns. 