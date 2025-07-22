# Stream Processing Service

A unified service for all real-time stream processing needs, consolidating multiple Flink jobs into a single, manageable service.

## Overview

The Stream Processing Service provides a centralized platform for:
- Real-time event processing
- Complex Event Processing (CEP)
- Stream analytics
- Risk monitoring
- Fraud detection
- Settlement processing

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                Stream Processing Service                 │
├─────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │   Job       │  │   Pattern   │  │   State     │    │
│  │  Manager    │  │   Library   │  │  Manager    │    │
│  └─────────────┘  └─────────────┘  └─────────────┘    │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │
│  │   Flink     │  │    CEP      │  │  Analytics  │    │
│  │  Runtime    │  │   Engine    │  │   Engine    │    │
│  └─────────────┘  └─────────────┘  └─────────────┘    │
└─────────────────────────────────────────────────────────┘
```

## Consolidated Jobs

### 1. Event Processing
- **Activity Stream**: Unified event processing from all services
- **Graph Ingestion**: Real-time graph updates
- **Lineage Ingestion**: Data lineage tracking

### 2. CEP & Pattern Detection
- **Complex Event Processing**: Multi-stream pattern detection
- **Fraud Detection**: Real-time fraud pattern matching
- **Derivatives CEP**: Trading pattern detection

### 3. Risk & Analytics
- **Risk Analytics**: Real-time risk calculations
- **Compute Futures Settlement**: Settlement processing
- **Royalty Calculation**: Usage-based royalty processing

### 4. Specialized Processing
- **Simulation Engine**: Real-time simulation processing
- **Resilience Monitoring**: System resilience patterns
- **Workflow Federation**: Distributed workflow processing

## API Endpoints

```yaml
POST   /jobs                    # Submit new job
GET    /jobs                    # List all jobs
GET    /jobs/{id}              # Get job status
DELETE /jobs/{id}              # Cancel job
POST   /jobs/{id}/savepoint    # Create savepoint
GET    /patterns               # List CEP patterns
POST   /patterns               # Register new pattern
```

## Configuration

```yaml
stream-processing:
  flink:
    parallelism: 8
    checkpointing:
      interval: 30000
      mode: EXACTLY_ONCE
    state-backend:
      type: rocksdb
      checkpoint-dir: hdfs://namenode:9000/flink/checkpoints
  
  patterns:
    fraud:
      velocity-check:
        window: 5m
        threshold: 10
      wash-trading:
        window: 5m
        same-user: true
    
    risk:
      liquidation-cascade:
        window: 1h
        threshold: 0.2
      price-spike:
        window: 1m
        threshold: 0.05
```

## Job Types

### 1. Streaming SQL Jobs
```sql
-- Example: Activity Stream Processing
CREATE TABLE activity_stream (
  event_id STRING,
  tenant_id STRING,
  event_type STRING,
  timestamp TIMESTAMP(3),
  data MAP<STRING, STRING>,
  WATERMARK FOR timestamp AS timestamp - INTERVAL '5' SECOND
) WITH (
  'connector' = 'pulsar',
  'topic' = 'activity-events',
  'scan.startup.mode' = 'latest'
);

-- Process and sink to Cassandra
INSERT INTO cassandra_activity_stream
SELECT 
  event_id,
  tenant_id,
  event_type,
  TUMBLE_START(timestamp, INTERVAL '1' MINUTE) as window_start,
  COUNT(*) as event_count
FROM activity_stream
GROUP BY 
  TUMBLE(timestamp, INTERVAL '1' MINUTE),
  tenant_id,
  event_type;
```

### 2. CEP Pattern Jobs
```python
# Fraud detection pattern
pattern = Pattern.begin("first").where(lambda x: x["amount"] > 1000) \
    .followedBy("second").where(lambda x: x["amount"] > 1000) \
    .within(Time.minutes(5))
```

### 3. Stateful Processing Jobs
```python
class RiskCalculator(KeyedProcessFunction):
    def process_element(self, value, ctx):
        # Maintain state per user
        current_exposure = self.exposure_state.value() or 0
        new_exposure = self.calculate_exposure(value)
        self.exposure_state.update(new_exposure)
```

## Integration Points

- **Pulsar**: Event sourcing and sinking
- **Cassandra**: Hot data storage
- **MinIO**: Cold storage and checkpoints
- **Ignite**: State caching
- **Elasticsearch**: Analytics results

## Monitoring

- Prometheus metrics: `/metrics`
- Health check: `/health`
- Job metrics: `/jobs/{id}/metrics`

## Migration Guide

To migrate existing Flink jobs:

1. **Convert job to service module**:
   ```python
   # Old: Standalone job
   env = StreamExecutionEnvironment.get_execution_environment()
   
   # New: Service module
   class MyJobModule(StreamJobModule):
       def configure(self, env: StreamExecutionEnvironment):
           # Job logic here
   ```

2. **Register patterns**:
   ```yaml
   patterns:
     my-pattern:
       type: cep
       definition: pattern.yml
   ```

3. **Update deployment**:
   ```bash
   # Old: Submit to Flink cluster
   flink run -c MyJob job.jar
   
   # New: Deploy via service
   curl -X POST /jobs -d @job-config.json
   ``` 