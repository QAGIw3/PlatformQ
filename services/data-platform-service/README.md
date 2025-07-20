# Data Platform Service

Comprehensive data management platform for the platformQ ecosystem with advanced trading data lake capabilities.

## Overview

The Data Platform Service provides a unified data infrastructure with:

- **Query Federation**: Unified SQL interface across multiple data sources via Apache Trino
- **Data Catalog**: Centralized metadata management with discovery and lineage tracking
- **Data Governance**: Policy-based access control with GDPR/CCPA/HIPAA compliance
- **Data Quality**: Automated profiling, validation, and remediation
- **Data Lake Management**: Medallion architecture (Bronze/Silver/Gold) with MinIO storage
- **Trading Data Lake**: Specialized medallion architecture for trading data
- **Pipeline Orchestration**: ETL/ELT management with Apache SeaTunnel
- **Feature Store**: ML feature management and serving
- **Real-time Analytics**: Integration with Druid for time-series analytics

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Data Platform Service                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │   Query     │    Data      │    Data     │  Feature   │ │
│  │ Federation  │   Catalog    │ Governance  │   Store    │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                  Data Lake (Medallion)                       │
│  ┌─────────────┬──────────────┬─────────────────────────┐  │
│  │   Bronze    │    Silver    │      Gold               │  │
│  │    (Raw)    │  (Cleaned)   │  (Analytics-Ready)      │  │
│  └─────────────┴──────────────┴─────────────────────────┘  │
├─────────────────────────────────────────────────────────────┤
│                   Trading Data Lake                          │
│  ┌─────────────┬──────────────┬─────────────────────────┐  │
│  │   Market    │   Trader     │     Risk                │  │
│  │    Data     │  Behavior    │  Indicators             │  │
│  └─────────────┴──────────────┴─────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Features

### Query Federation
- Execute SQL queries across PostgreSQL, Cassandra, Elasticsearch, and more
- Query optimization and caching
- Result materialization for performance
- Cross-database joins

### Data Catalog
- Asset discovery with full-text search
- Column-level metadata and statistics
- Data lineage visualization
- Tagging and classification
- Quality scores and metrics

### Data Governance
- Fine-grained access control
- Compliance frameworks (GDPR, CCPA, HIPAA)
- Data privacy controls (masking, encryption)
- Access request workflows
- Audit logging

### Trading Data Lake
- **Bronze Layer**: Raw trading events (trades, orderbook, positions)
- **Silver Layer**: Validated and enriched trading data
- **Gold Layer**: ML-ready features and aggregations
- **Real-time Ingestion**: Stream processing for market data
- **Quality Validation**: Automated data quality checks

### Trading Features
- **Market Microstructure**: Spread, depth, liquidity metrics
- **Trader Behavior**: Win rate, holding period, strategy consistency
- **Risk Indicators**: VaR, CVaR, concentration risk
- **Technical Indicators**: Price-based technical analysis

## API Endpoints

### Query Federation
- `POST /api/v1/query/execute` - Execute federated query
- `GET /api/v1/query/{query_id}/status` - Check query status
- `GET /api/v1/query/{query_id}/results` - Get query results

### Data Catalog
- `GET /api/v1/catalog/search` - Search catalog
- `POST /api/v1/catalog/assets` - Register new asset
- `GET /api/v1/catalog/assets/{asset_id}` - Get asset details
- `PUT /api/v1/catalog/assets/{asset_id}/tags` - Update tags

### Data Governance
- `POST /api/v1/governance/policies` - Create access policy
- `GET /api/v1/governance/compliance/report` - Compliance report
- `POST /api/v1/governance/access-requests` - Request access

### Trading Data Lake
- `POST /api/v1/lake/trading/ingest` - Ingest trading events
- `POST /api/v1/lake/trading/process/silver` - Process to Silver
- `POST /api/v1/lake/trading/generate/features` - Generate Gold features
- `GET /api/v1/lake/trading/quality/report` - Data quality report
- `GET /api/v1/lake/trading/features/available` - List available features

### Feature Store
- `POST /api/v1/features/register` - Register feature set
- `POST /api/v1/features/serve` - Get feature values
- `POST /api/v1/features/compute` - Compute features

## Trading Data Types

```python
ORDER_BOOK = "order_book"      # Orderbook snapshots
TRADES = "trades"              # Executed trades
MARKET_DATA = "market_data"    # Price/volume data
POSITIONS = "positions"        # Open positions
RISK_METRICS = "risk_metrics"  # Risk calculations
TRADER_ACTIVITY = "trader_activity"  # Trader actions
STRATEGY_SIGNALS = "strategy_signals"  # Trading signals
```

## Configuration

### Environment Variables

```bash
# Database Connections
TRINO_HOST=trino-coordinator:8080
POSTGRES_URL=postgresql://user:pass@postgres:5432/db
CASSANDRA_HOSTS=cassandra-0,cassandra-1
ELASTICSEARCH_URL=http://elasticsearch:9200

# Storage
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123

# Analytics
DRUID_BROKER_URL=http://druid-broker:8082
SPARK_MASTER=spark://spark-master:7077

# Service Configuration
SERVICE_NAME=data-platform-service
LOG_LEVEL=INFO

# Trading Lake Configuration
BRONZE_RETENTION_DAYS=90
SILVER_RETENTION_DAYS=365
GOLD_RETENTION_DAYS=730
```

## Usage Examples

### Ingest Trading Data

```python
import httpx

# Ingest trade events
events = [
    {
        "trade_id": "T-123",
        "market_id": "BTC-USD",
        "trader_id": "trader123",
        "price": 45000,
        "quantity": 0.5,
        "side": "BUY",
        "timestamp": "2024-01-15T10:30:00Z"
    }
]

response = httpx.post(
    "http://data-platform:8000/api/v1/lake/trading/ingest",
    json={
        "events": events,
        "event_type": "trades"
    }
)
```

### Generate Trading Features

```python
# Generate market microstructure features
response = httpx.post(
    "http://data-platform:8000/api/v1/lake/trading/generate/features",
    json={
        "feature_sets": ["market_microstructure", "trader_behavior"],
        "start_date": "2024-01-01T00:00:00Z",
        "end_date": "2024-01-15T23:59:59Z"
    }
)

# Response includes features like:
# - avg_spread, spread_volatility
# - trade_frequency, win_rate
# - kyle_lambda, amihud_illiquidity
```

### Query Federated Data

```python
# Query across multiple data sources
query = {
    "sql": """
        SELECT 
            t.trader_id,
            t.total_volume,
            r.risk_score,
            f.win_rate
        FROM postgres.trading.traders t
        JOIN cassandra.risk.scores r ON t.trader_id = r.trader_id
        JOIN features.trader_behavior f ON t.trader_id = f.trader_id
        WHERE t.total_volume > 1000000
    """,
    "limit": 100
}

response = httpx.post(
    "http://data-platform:8000/api/v1/query/execute",
    json=query
)
```

## Data Quality

### Quality Metrics
- **Completeness**: Percentage of non-null required fields
- **Accuracy**: Data validation against business rules
- **Timeliness**: Data freshness and latency
- **Consistency**: Cross-dataset consistency checks

### Quality Thresholds
- Bronze → Silver: 95% completeness required
- Silver → Gold: 99% accuracy required
- Real-time data: < 5 second latency

## Monitoring

### Metrics
- Query execution time and throughput
- Data ingestion rate and latency
- Storage usage by layer
- Feature computation time
- Quality score trends

### Alerts
- Data quality threshold breaches
- Pipeline failures
- Storage capacity warnings
- Query timeout alerts

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Start dependencies
docker-compose up -d postgres cassandra elasticsearch minio

# Run service
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Testing

```bash
# Run tests
pytest tests/

# Run integration tests
pytest tests/integration/
```

## Integration

The Data Platform Service integrates with:
- **ML Platform Service**: Provides training data and features
- **Event Router Service**: Ingests real-time event streams
- **Analytics Service**: Federated analytics queries
- **Trading Platform Service**: Receives trading data for processing
- **Storage Service**: Uses MinIO for object storage 

### Trading Data Lake Features
- **Real-time Ingestion**: Stream trading data from multiple sources
- **Medallion Architecture**: Bronze → Silver → Gold data refinement
- **Time-series Optimization**: Specialized storage for tick data
- **Feature Engineering**: Automated technical indicator calculation
- **Quality Monitoring**: Track data quality metrics
- **Batch Processing**: Daily aggregations and rollups

### Trading Lake API
```bash
# Ingest trading data
POST /api/v1/trading-lake/ingest

# Process to Silver layer
POST /api/v1/trading-lake/process-silver

# Generate Gold features
POST /api/v1/trading-lake/generate-features

# Query trading data
POST /api/v1/trading-lake/query

# Get quality report
GET /api/v1/trading-lake/quality/{dataset_id}
```

## ML Data Lake

Specialized medallion architecture for ML workloads:

### ML Data Layers
- **Raw Data**: Original training datasets
- **Processed Data**: Cleaned and normalized data
- **Feature Data**: Engineered features
- **Training Data**: Train/val/test splits
- **Model Artifacts**: Trained model storage
- **Predictions**: Model output tracking
- **Metrics**: Performance metrics aggregation

### ML Lake Features
- **Dataset Versioning**: Track dataset versions and lineage
- **Feature Engineering**: Automated feature generation
  - Polynomial features
  - Interaction features
  - Time-based features
  - Statistical features
- **Data Processing**: Configurable processing pipeline
  - Missing value handling
  - Normalization
  - Categorical encoding
  - Outlier removal
- **Training Splits**: Automated train/val/test splitting
- **Model Artifact Management**: Store and version models
- **Prediction Tracking**: Monitor model predictions
- **Metrics Aggregation**: Track model performance over time

### ML Lake API
```bash
# Ingest training dataset
POST /api/v1/ml-lake/datasets/ingest

# Process dataset
POST /api/v1/ml-lake/datasets/{dataset_id}/process

# Engineer features
POST /api/v1/ml-lake/features/engineer

# Create training splits
POST /api/v1/ml-lake/training/splits

# Save model artifact
POST /api/v1/ml-lake/models/artifacts

# Track predictions
POST /api/v1/ml-lake/predictions/track

# Get model metrics
GET /api/v1/ml-lake/models/{model_id}/metrics

# Query ML data
POST /api/v1/ml-lake/query

# Get dataset info
GET /api/v1/ml-lake/datasets/{dataset_id}

# Get feature set info
GET /api/v1/ml-lake/features/{feature_set_id}
```

### Data Processing Configuration
```json
{
  "handle_missing": true,
  "missing_strategy": "mean",
  "normalize": true,
  "encode_categorical": true,
  "remove_outliers": false,
  "outlier_threshold": 3.0
}
```

### Feature Engineering Configuration
```json
{
  "polynomial_features": true,
  "interaction_features": true,
  "time_features": false,
  "statistical_features": true,
  "custom_features": []
}
``` 