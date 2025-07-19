# PlatformQ Analytics Infrastructure

This directory contains the Docker Compose configuration for the complete analytics infrastructure supporting the unified analytics service.

## Components

### 1. Apache Druid
- **Purpose**: Time-series OLAP database for real-time analytics
- **Components**:
  - Coordinator: Manages data availability and load balancing
  - Broker: Handles queries and merges results
  - Historical: Stores and serves immutable data segments
  - MiddleManager: Manages data ingestion
  - Router: Routes requests to appropriate services
- **Access**: http://localhost:8888 (Router UI)

### 2. Apache Ignite
- **Purpose**: In-memory data grid for distributed caching and compute
- **Features**:
  - Query result caching with TTL
  - Real-time metrics storage
  - ML model caching
  - Feature store backend
- **Cluster**: 2-node setup for high availability
- **Access**: http://localhost:10800 (REST API)

### 3. Apache Trino
- **Purpose**: Distributed SQL query engine for data lake analytics
- **Architecture**:
  - 1 Coordinator node
  - 2 Worker nodes
- **Catalogs**:
  - `hive`: Access to Hive-compatible data
  - `iceberg`: Apache Iceberg tables
  - `postgresql`: Direct PostgreSQL access
  - `elasticsearch`: Query Elasticsearch indices
- **Access**: http://localhost:8080 (Web UI)

### 4. MinIO
- **Purpose**: S3-compatible object storage
- **Buckets**:
  - `datalake`: Main data lake storage
  - `mlflow`: MLflow artifacts
  - `models`: Model storage
  - `features`: Feature store data
  - `druid-deep-storage`: Druid segments
- **Access**: http://localhost:9001 (Console)
- **Credentials**: minioadmin/minioadmin

### 5. MLflow
- **Purpose**: ML experiment tracking and model registry
- **Features**:
  - Experiment tracking
  - Model versioning
  - Model registry
  - Artifact storage (MinIO backend)
- **Access**: http://localhost:5000
- **Backend**: PostgreSQL + MinIO

### 6. Apache Superset
- **Purpose**: Modern data exploration and visualization
- **Features**:
  - SQL Lab for ad-hoc queries
  - Rich visualizations
  - Dashboard creation
  - Connected to Trino and Druid
- **Access**: http://localhost:8088
- **Credentials**: admin/admin

## Quick Start

1. **Start the infrastructure**:
   ```bash
   cd infra/docker-compose
   ./scripts/start-analytics.sh
   ```

2. **Verify all services are running**:
   ```bash
   docker-compose -f docker-compose.analytics.yml ps
   ```

3. **Check service health**:
   - Druid: http://localhost:8888/status
   - Trino: http://localhost:8080/ui
   - MLflow: http://localhost:5000/health
   - MinIO: http://localhost:9000/minio/health/live

## Configuration

### Environment Variables
Create a `.env` file in the docker-compose directory:
```env
# PostgreSQL
POSTGRES_PASSWORD=your_secure_password

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=your_secure_password

# MLflow
MLFLOW_DB_PASSWORD=your_secure_password

# Superset
SUPERSET_SECRET_KEY=your_secret_key
```

### Memory Requirements
- **Minimum**: 16GB RAM
- **Recommended**: 32GB RAM
- **Storage**: 100GB+ free space

### Port Mappings
| Service | Port | Description |
|---------|------|-------------|
| Druid Coordinator | 8081 | Coordination service |
| Druid Broker | 8082 | Query broker |
| Druid Router | 8888 | Main entry point |
| Trino | 8080 | SQL query engine |
| Ignite | 10800 | Cache/compute grid |
| MinIO | 9000/9001 | S3 API/Console |
| MLflow | 5000 | ML tracking |
| Superset | 8088 | Analytics UI |
| PostgreSQL | 5432 | Metadata store |

## Data Ingestion

### Druid Data Ingestion
1. **Batch ingestion from S3/MinIO**:
   ```json
   {
     "type": "index_parallel",
     "spec": {
       "dataSchema": {
         "dataSource": "platform_metrics",
         "timestampSpec": {
           "column": "timestamp",
           "format": "iso"
         },
         "dimensionsSpec": {
           "dimensions": ["service", "endpoint", "status"]
         },
         "metricsSpec": [
           {"type": "count", "name": "count"},
           {"type": "doubleSum", "name": "duration", "fieldName": "duration"}
         ]
       },
       "ioConfig": {
         "type": "index_parallel",
         "inputSource": {
           "type": "s3",
           "uris": ["s3://datalake/metrics/"]
         }
       }
     }
   }
   ```

2. **Streaming ingestion via Kafka** (requires Kafka setup):
   ```json
   {
     "type": "kafka",
     "dataSchema": {...},
     "ioConfig": {
       "topic": "platform-metrics",
       "consumerProperties": {
         "bootstrap.servers": "kafka:9092"
       }
     }
   }
   ```

### Trino Table Creation
```sql
-- Create external table on S3
CREATE TABLE hive.datalake.events (
    event_id VARCHAR,
    timestamp TIMESTAMP,
    user_id VARCHAR,
    event_type VARCHAR,
    properties MAP<VARCHAR, VARCHAR>
)
WITH (
    external_location = 's3a://datalake/events/',
    format = 'PARQUET'
);

-- Create Iceberg table
CREATE TABLE iceberg.analytics.user_metrics (
    user_id VARCHAR,
    metric_date DATE,
    active_minutes BIGINT,
    event_count BIGINT
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['metric_date']
);
```

## Maintenance

### Backup
1. **PostgreSQL databases**:
   ```bash
   docker-compose -f docker-compose.analytics.yml exec postgres \
     pg_dumpall -U analytics > backup.sql
   ```

2. **MinIO data**:
   ```bash
   docker run --rm -v minio-data:/data \
     -v $(pwd):/backup alpine \
     tar czf /backup/minio-backup.tar.gz /data
   ```

3. **Ignite persistence**:
   ```bash
   docker-compose -f docker-compose.analytics.yml stop ignite-1 ignite-2
   # Backup ignite-data-1 and ignite-data-2 volumes
   docker-compose -f docker-compose.analytics.yml start ignite-1 ignite-2
   ```

### Monitoring
- **Druid metrics**: http://localhost:8888/status
- **Trino metrics**: http://localhost:8080/ui/
- **Ignite metrics**: Available via JMX
- **Container metrics**: `docker stats`

### Scaling
1. **Add more Trino workers**:
   ```yaml
   trino-worker-3:
     image: trinodb/trino:426
     # ... same config as other workers
   ```

2. **Scale Druid Historical nodes**:
   ```yaml
   druid-historical-2:
     image: apache/druid:27.0.0
     # ... same config as druid-historical
   ```

3. **Add Ignite nodes**:
   ```yaml
   ignite-3:
     image: apacheignite/ignite:2.15.0
     # ... same config as other ignite nodes
   ```

## Troubleshooting

### Common Issues

1. **Out of Memory errors**:
   - Increase Docker memory limits
   - Adjust JVM heap sizes in configurations

2. **Connection refused**:
   - Ensure all services are healthy
   - Check network connectivity between containers

3. **Slow queries**:
   - Check if data is properly partitioned
   - Verify indexes are created
   - Monitor resource usage

### Logs
```bash
# View all logs
docker-compose -f docker-compose.analytics.yml logs -f

# View specific service logs
docker-compose -f docker-compose.analytics.yml logs -f druid-broker
docker-compose -f docker-compose.analytics.yml logs -f trino-coordinator
```

### Reset
To completely reset the infrastructure:
```bash
docker-compose -f docker-compose.analytics.yml down -v
rm -rf data/
./scripts/start-analytics.sh
```

## Integration with Analytics Service

The unified analytics service connects to this infrastructure using these endpoints:

```yaml
# In analytics-service configuration
DRUID_BROKER_URL: http://druid-broker:8082
DRUID_COORDINATOR_URL: http://druid-coordinator:8081
DRUID_OVERLORD_URL: http://druid-overlord:8090
IGNITE_HOST: ignite-1
IGNITE_PORT: 10800
TRINO_HOST: trino-coordinator
TRINO_PORT: 8080
MLFLOW_TRACKING_URI: http://mlflow-server:5000
```

## Security Considerations

⚠️ **This configuration is for development/testing only!**

For production:
1. Use strong passwords (not defaults)
2. Enable TLS/SSL for all services
3. Configure proper authentication
4. Set up network isolation
5. Enable audit logging
6. Implement backup strategies
7. Monitor security events

## Next Steps

1. **Configure data pipelines** to ingest data into Druid
2. **Create Trino tables** for your data lake
3. **Set up MLflow experiments** for model tracking
4. **Build Superset dashboards** for visualization
5. **Optimize Ignite caches** based on query patterns 