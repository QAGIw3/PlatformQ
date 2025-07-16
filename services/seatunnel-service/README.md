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
- **Automated Pipeline Generation**: AI-powered pipeline configuration from asset metadata
- **Real-time Monitoring**: Apache Ignite-based metrics storage with alerting
- **Airflow Integration**: Seamless orchestration with workflow-service

## Architecture

The service acts as a control plane for SeaTunnel jobs:

1. **API Layer**: RESTful API for job management
2. **Configuration Generator**: Converts high-level job specs to SeaTunnel configs
3. **Pipeline Generator**: Intelligent pipeline generation from asset metadata
4. **Kubernetes Job Manager**: Launches and monitors SeaTunnel jobs
5. **Job Monitor**: Background thread tracking job status and metrics
6. **Event Publisher**: Publishes job completion events to Pulsar
7. **Monitoring Layer**: Real-time metrics collection and alerting via Ignite

## Automated Pipeline Generation

### Overview

The service can automatically generate optimized data pipelines based on asset metadata, eliminating the need for manual configuration in many cases.

### How It Works

1. **Asset Analysis**: When a DigitalAssetCreated event is received, the service analyzes the asset type and metadata
2. **Pattern Detection**: Determines the appropriate pipeline pattern (ingestion, ETL, streaming, CDC, etc.)
3. **Configuration Generation**: Creates optimized source, transform, and sink configurations
4. **Auto-Deployment**: Optionally deploys the pipeline automatically

### Supported Patterns

- **Ingestion**: Basic data loading into the data lake
- **ETL**: Extract, transform, and load between systems
- **Streaming**: Real-time stream processing
- **CDC**: Change Data Capture for database replication
- **Enrichment**: Enhance data with additional context
- **Aggregation**: Summarize and aggregate data
- **Replication**: Copy data to multiple targets
- **Federation**: Combine data from multiple sources

### Auto-Generation API

```bash
# Generate pipeline from asset metadata
POST /api/v1/pipelines/generate
{
  "asset_id": "12345",
  "asset_type": "CSV",
  "raw_data_uri": "s3://data/sales.csv",
  "metadata": {
    "estimated_volume": "large",
    "update_frequency": "daily"
  },
  "auto_deploy": true
}

# Analyze data source
POST /api/v1/pipelines/analyze?source_uri=s3://data/file.csv&source_type=file

# Generate multiple pipelines
POST /api/v1/pipelines/batch/generate
{
  "assets": [...],
  "pipeline_template": {...}
}
```

## Monitoring & Alerting

### Real-time Metrics

The service provides comprehensive monitoring using Apache Ignite for metrics storage:

- **Performance Metrics**: Throughput, processing time, lag
- **Data Quality**: Error rates, validation failures
- **Resource Usage**: CPU, memory, network I/O
- **Pattern-specific Metrics**: CDC lag, streaming backpressure, etc.

### Monitoring Endpoints

```bash
# Get job metrics
GET /api/v1/jobs/{job_id}/metrics?time_range_hours=1

# Overall monitoring dashboard
GET /api/v1/monitoring/dashboard

# Configure alerts
POST /api/v1/jobs/{job_id}/alerts
[
  {
    "metric": "error_rate",
    "threshold": 0.05,
    "window": "5m",
    "severity": "warning"
  }
]

# Export Prometheus metrics
GET /api/v1/monitoring/metrics/export
```

### Alert Configuration

Alerts can be configured per job with customizable rules:

```json
{
  "alert_rules": [
    {
      "metric": "processing_lag",
      "threshold": 300,
      "window": "10m",
      "severity": "critical",
      "action": "webhook"
    }
  ]
}
```

## Configuration Management

### Pipeline Templates

Pre-configured templates for common patterns:

```bash
# List available templates
GET /api/v1/templates

# Get specific template
GET /api/v1/templates/{template_id}
```

### Template Categories

- **data-lake**: Data lake ingestion patterns
- **cdc**: Change Data Capture templates
- **streaming**: Real-time processing templates
- **etl**: Traditional ETL patterns

### Configuration Validation

The service validates all pipeline configurations before deployment:

```bash
POST /api/v1/pipelines/validate
{
  "source": {...},
  "transforms": [...],
  "sinks": [...],
  "sync_mode": "batch"
}
```

## Integration with Airflow

### DAG Template

The service integrates with Apache Airflow for advanced orchestration:

```python
# Trigger SeaTunnel pipeline from Airflow
from airflow.providers.http.operators.http import SimpleHttpOperator

trigger_pipeline = SimpleHttpOperator(
    task_id='trigger_seatunnel_pipeline',
    http_conn_id='seatunnel_service',
    endpoint='/api/v1/pipelines/generate',
    method='POST',
    data={
        'asset_metadata': {...},
        'auto_deploy': True
    }
)
```

### Event-Driven Orchestration

The workflow-service automatically triggers the SeaTunnel orchestration DAG when:
- New data sources are discovered
- Asset metadata indicates data pipeline requirements
- Manual pipeline requests are submitted

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

### Pipeline Generation

- `POST /api/v1/pipelines/generate`: Generate pipeline from metadata
- `POST /api/v1/pipelines/analyze`: Analyze data source
- `GET /api/v1/pipelines/patterns`: List available patterns
- `POST /api/v1/pipelines/batch/generate`: Batch pipeline generation

### Monitoring

- `GET /api/v1/jobs/{job_id}/metrics`: Get job metrics
- `GET /api/v1/monitoring/dashboard`: Overall monitoring dashboard
- `POST /api/v1/jobs/{job_id}/alerts`: Configure alerts
- `GET /api/v1/monitoring/metrics/export`: Export Prometheus metrics

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
  "parallelism": 4,
  "monitoring": {
    "alert_rules": [
      {
        "metric": "error_rate",
        "threshold": 0.05,
        "window": "5m"
      }
    ]
  }
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
7. **Data Quality**: Validate and clean data
8. **Schema Inference**: Automatically detect schemas
9. **NLP Enrichment**: Extract entities and sentiment
10. **Window Operations**: Time-based windowing for streams

## Multi-tenancy

The service ensures complete tenant isolation:

- Jobs run in tenant-specific Kubernetes namespaces
- Data paths automatically include tenant ID
- Pulsar topics are tenant-scoped
- Resource quotas enforced per tenant
- Metrics and monitoring data isolated per tenant

## Events

### Events Consumed

- **Topic Pattern**: `persistent://platformq/.*/digital-asset-created-events`
- **Purpose**: Auto-generate pipelines for new assets

### Events Produced

- **Topic**: `persistent://platformq/<tenant_id>/seatunnel-job-completed`
- **Schema**: Contains job ID, status, metrics, and completion time

- **Topic**: `persistent://platformq/<tenant_id>/seatunnel-pipeline-created-events`
- **Schema**: Contains pipeline ID, pattern, and configuration

## Integration with Platform

The SeaTunnel service integrates with other platform components:

1. **Hive Metastore**: Automatically registers tables created in data lake
2. **Trino**: Query synchronized data immediately
3. **Pulsar**: Use as source or sink for event streaming
4. **Workflow Service**: Trigger jobs via Airflow DAGs
5. **Graph Intelligence**: Sync relationship data to JanusGraph
6. **Apache Ignite**: Store metrics and enable real-time monitoring
7. **Digital Asset Service**: Auto-generate pipelines from asset metadata

## Deployment

The service is deployed as a Kubernetes Deployment with:

- Horizontal pod autoscaling based on API load
- Persistent volume for job metadata
- Service mesh integration for secure communication
- Prometheus metrics exposure
- Ignite client for metrics storage

## Configuration

Environment variables:

- `DATABASE_URL`: Connection string for job metadata database
- `PULSAR_URL`: Pulsar broker URL
- `KUBERNETES_NAMESPACE`: Default namespace for jobs
- `MINIO_ENDPOINT`: MinIO endpoint for data lake
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key
- `IGNITE_HOSTS`: Apache Ignite cluster hosts
- `ALERT_WEBHOOK_URL`: Webhook URL for alerts
- `AIRFLOW_ENABLED`: Enable Airflow integration

## Best Practices

1. **Incremental Sync**: Use watermarks for incremental data sync
2. **Partitioning**: Partition data lake tables by date for efficient queries
3. **Schema Registry**: Use Pulsar schema registry for event schemas
4. **Error Handling**: Configure retry policies and dead letter queues
5. **Monitoring**: Set up alerts for failed jobs
6. **Resource Limits**: Configure appropriate CPU/memory limits for jobs
7. **Pipeline Templates**: Use templates for consistent configurations
8. **Auto-generation**: Leverage automatic pipeline generation for standard patterns
9. **Metrics Collection**: Monitor all pipelines for performance optimization
10. **Cost Optimization**: Review pipeline costs and optimize parallelism

## Troubleshooting

### Common Issues

1. **Job Stuck in Pending**
   - Check Kubernetes resources
   - Verify namespace exists
   - Check resource quotas

2. **High Error Rate**
   - Review job logs
   - Check source connectivity
   - Validate transformation logic

3. **Poor Performance**
   - Increase parallelism
   - Enable compression
   - Optimize transformations

4. **Pipeline Generation Fails**
   - Verify asset metadata completeness
   - Check connector availability
   - Review validation errors

5. **Monitoring Data Missing**
   - Verify Ignite connectivity
   - Check metrics retention settings
   - Review monitor initialization

### Debug Commands

```bash
# Check job status
kubectl get jobs -n <tenant-namespace>

# View job logs
kubectl logs job/<job-name> -n <tenant-namespace>

# Check SeaTunnel service logs
kubectl logs deployment/seatunnel-service -f

# View metrics in Ignite
kubectl exec -it ignite-0 -- /opt/ignite/bin/sqlline.sh
```

## Migration Guide

### From Manual Configuration

1. Export existing job configurations
2. Use the analyze endpoint to understand data characteristics
3. Generate new pipeline using automated generation
4. Compare and merge custom settings
5. Test in staging environment
6. Deploy with monitoring enabled

### Upgrading Pipelines

1. Use `/api/v1/pipelines/analyze` to get optimization suggestions
2. Apply recommendations using pipeline update API
3. Monitor metrics before and after changes
4. Rollback if performance degrades 