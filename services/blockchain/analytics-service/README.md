# Blockchain Analytics Service

A comprehensive analytics platform that provides real-time and historical insights into blockchain data, including transaction patterns, wallet behavior, token metrics, DeFi protocols, and predictive modeling across multiple blockchain networks.

## Overview

The Blockchain Analytics Service processes and analyzes on-chain data to provide actionable insights for PlatformQ users. It combines real-time data processing with historical analysis, machine learning models for predictions, and sophisticated visualization capabilities. The service supports multiple blockchain networks and provides APIs for custom queries and automated reporting.

## Key Features

- **Multi-Chain Analytics**: Support for Ethereum, BSC, Polygon, Arbitrum, and more
- **Real-Time Processing**: Stream processing for live blockchain data
- **Historical Analysis**: Deep historical data analysis with time-series capabilities
- **Wallet Analytics**: Track wallet behavior, holdings, and transaction patterns
- **Token Metrics**: Comprehensive token analytics including liquidity and volume
- **DeFi Analytics**: Protocol TVL, yield farming, and liquidity pool analysis
- **NFT Analytics**: Collection statistics, rarity analysis, and market trends
- **Predictive Models**: ML-based predictions for gas prices, token prices, and trends
- **Custom Queries**: SQL-like query interface for blockchain data
- **Automated Reports**: Scheduled reports with customizable templates
- **API Integration**: RESTful APIs for all analytics functions

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   API Layer (FastAPI)                    │
├─────────────────────────────────────────────────────────┤
│                Analytics Engine Core                     │
├──────────────┬────────────┬────────────┬───────────────┤
│ Transaction  │   Wallet   │   Token    │     DeFi      │
│  Analyzer    │  Analyzer  │  Analyzer  │   Analyzer    │
├──────────────┴────────────┴────────────┴───────────────┤
│          Data Processing Pipeline                        │
├──────────────┬────────────┬────────────┬───────────────┤
│  TimescaleDB │ ClickHouse │  MongoDB   │   Apache      │
│  (Time Series)│ (Analytics)│ (Documents)│    Ignite     │
├──────────────┴────────────┴────────────┴───────────────┤
│  Stream Processor  │  ML Engine   │  Report Generator   │
│  (Apache Pulsar)   │ (Scikit/Prophet)│   (Celery)      │
└─────────────────────────────────────────────────────────┘
```

## API Endpoints

### Analytics Queries
- `POST /api/v1/analytics/query` - Execute custom analytics query
- `GET /api/v1/analytics/queries/{query_id}` - Get query results
- `GET /api/v1/analytics/queries/templates` - List query templates
- `POST /api/v1/analytics/queries/save` - Save custom query

### Chain Metrics
- `GET /api/v1/chains/{chain}/metrics` - Get chain-level metrics
- `GET /api/v1/chains/{chain}/metrics/history` - Historical chain metrics
- `GET /api/v1/chains/{chain}/gas/analysis` - Gas price analysis
- `GET /api/v1/chains/{chain}/blocks/analysis` - Block analysis

### Wallet Analytics
- `GET /api/v1/wallets/{address}/overview` - Wallet overview
- `GET /api/v1/wallets/{address}/transactions` - Transaction history
- `GET /api/v1/wallets/{address}/holdings` - Token holdings
- `GET /api/v1/wallets/{address}/pnl` - Profit/loss analysis
- `GET /api/v1/wallets/{address}/behavior` - Behavioral analysis
- `POST /api/v1/wallets/compare` - Compare multiple wallets

### Token Analytics
- `GET /api/v1/tokens/{token}/metrics` - Token metrics
- `GET /api/v1/tokens/{token}/holders` - Holder distribution
- `GET /api/v1/tokens/{token}/liquidity` - Liquidity analysis
- `GET /api/v1/tokens/{token}/price/prediction` - Price prediction
- `GET /api/v1/tokens/trending` - Trending tokens

### DeFi Analytics
- `GET /api/v1/defi/protocols` - List DeFi protocols
- `GET /api/v1/defi/protocols/{protocol}/tvl` - Protocol TVL
- `GET /api/v1/defi/protocols/{protocol}/users` - User analytics
- `GET /api/v1/defi/pools/{pool}/analysis` - Pool analysis
- `GET /api/v1/defi/yield/opportunities` - Yield opportunities

### NFT Analytics
- `GET /api/v1/nft/collections` - NFT collection stats
- `GET /api/v1/nft/collections/{collection}/analysis` - Collection analysis
- `GET /api/v1/nft/collections/{collection}/rarity` - Rarity analysis
- `GET /api/v1/nft/market/trends` - Market trends

### Reports
- `GET /api/v1/reports` - List available reports
- `POST /api/v1/reports/generate` - Generate custom report
- `GET /api/v1/reports/{report_id}` - Get report
- `POST /api/v1/reports/schedule` - Schedule recurring report

### Alerts
- `POST /api/v1/alerts` - Create analytics alert
- `GET /api/v1/alerts` - List alerts
- `PUT /api/v1/alerts/{alert_id}` - Update alert
- `DELETE /api/v1/alerts/{alert_id}` - Delete alert

### Time Series Data
- `GET /api/v1/timeseries/{metric}` - Get time series data
- `POST /api/v1/timeseries/aggregate` - Aggregate time series
- `GET /api/v1/timeseries/correlations` - Find correlations

### Predictions
- `GET /api/v1/predictions/gas` - Gas price predictions
- `GET /api/v1/predictions/volume` - Volume predictions
- `GET /api/v1/predictions/trends` - Trend predictions

### Monitoring
- `GET /health` - Service health check
- `GET /metrics` - Prometheus metrics

## Configuration

The service is configured via environment variables:

```bash
# Service Configuration
SERVICE_NAME=blockchain-analytics-service
SERVICE_HOST=0.0.0.0
SERVICE_PORT=8006
ENVIRONMENT=production

# Database Configuration
TIMESCALEDB_URL=postgresql://user:pass@timescaledb:5432/analytics
CLICKHOUSE_URL=clickhouse://clickhouse:9000/analytics
MONGODB_URL=mongodb://mongodb:27017/analytics
REDIS_URL=redis://redis:6379/1

# Data Sources
BLOCKCHAIN_CONNECTOR_URL=http://blockchain-connector:8000
EVENT_MONITORING_URL=http://event-monitoring:8005
EXTERNAL_DATA_APIS=coingecko,etherscan,dune

# Analytics Configuration
DEFAULT_TIME_RANGE_DAYS=30
MAX_QUERY_RESULTS=10000
ENABLE_CACHING=true
CACHE_TTL_SECONDS=300

# ML Model Configuration
MODEL_UPDATE_INTERVAL_HOURS=24
PREDICTION_CONFIDENCE_THRESHOLD=0.8
ENABLE_AUTO_RETRAIN=true

# Report Generation
REPORT_STORAGE_PATH=/data/reports
REPORT_RETENTION_DAYS=90
MAX_CONCURRENT_REPORTS=10

# Apache Ignite Configuration
IGNITE_ENDPOINTS=ignite-node1:10800,ignite-node2:10800
IGNITE_ANALYTICS_CACHE=analytics-cache

# Pulsar Configuration
PULSAR_SERVICE_URL=pulsar://pulsar:6650
PULSAR_ANALYTICS_TOPIC=persistent://platformq/analytics/events

# Celery Configuration
CELERY_BROKER_URL=redis://redis:6379/2
CELERY_RESULT_BACKEND=redis://redis:6379/3

# External APIs
COINGECKO_API_KEY=${COINGECKO_API_KEY}
ETHERSCAN_API_KEY=${ETHERSCAN_API_KEY}
DUNE_API_KEY=${DUNE_API_KEY}

# Monitoring
ENABLE_METRICS=true
LOG_LEVEL=INFO
ENABLE_PROFILING=false
```

## Dependencies

- **FastAPI**: REST API framework
- **pandas**: Data analysis and manipulation
- **numpy**: Numerical computations
- **scikit-learn**: Machine learning models
- **prophet**: Time series forecasting
- **sqlalchemy**: Database ORM
- **asyncpg**: PostgreSQL async driver
- **clickhouse-driver**: ClickHouse client
- **motor**: MongoDB async driver
- **redis**: Caching and queuing
- **pyignite**: Distributed caching
- **celery**: Task scheduling
- **plotly**: Data visualization
- **prometheus-client**: Metrics collection

## Running the Service

### Using Docker

```bash
# Build the image
docker build -t blockchain-analytics-service .

# Run the container
docker run -d \
  --name blockchain-analytics \
  -p 8006:8006 \
  -e TIMESCALEDB_URL="postgresql://user:pass@timescaledb:5432/analytics" \
  -e CLICKHOUSE_URL="clickhouse://clickhouse:9000/analytics" \
  -e BLOCKCHAIN_CONNECTOR_URL="http://blockchain-connector:8000" \
  blockchain-analytics-service
```

### Using Docker Compose

```yaml
services:
  blockchain-analytics:
    build: ./services/blockchain/analytics-service
    ports:
      - "8006:8006"
    environment:
      - TIMESCALEDB_URL=postgresql://user:pass@timescaledb:5432/analytics
      - CLICKHOUSE_URL=clickhouse://clickhouse:9000/analytics
      - MONGODB_URL=mongodb://mongodb:27017/analytics
      - REDIS_URL=redis://redis:6379/1
      - PULSAR_SERVICE_URL=pulsar://pulsar:6650
    depends_on:
      - timescaledb
      - clickhouse
      - mongodb
      - redis
      - pulsar
    volumes:
      - ./data/reports:/data/reports
```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run database migrations
alembic upgrade head

# Train initial ML models
python scripts/train_models.py

# Set environment variables
export TIMESCALEDB_URL="postgresql://localhost:5432/analytics"
export CLICKHOUSE_URL="clickhouse://localhost:9000/analytics"

# Run the service
uvicorn app.main:app --host 0.0.0.0 --port 8006 --reload
```

## Query Examples

### Custom Analytics Query

```json
{
  "query": "SELECT date_trunc('day', timestamp) as day, COUNT(*) as tx_count, SUM(value) as volume FROM ethereum_transactions WHERE timestamp > NOW() - INTERVAL '30 days' GROUP BY day ORDER BY day",
  "chain": "ethereum",
  "format": "json"
}
```

### Wallet Analysis Query

```json
{
  "address": "0x1234...",
  "metrics": ["balance_history", "transaction_count", "gas_spent", "profit_loss"],
  "timeframe": "30d",
  "include_tokens": true
}
```

### DeFi Protocol Comparison

```json
{
  "protocols": ["uniswap", "sushiswap", "curve"],
  "metrics": ["tvl", "volume_24h", "unique_users", "fee_revenue"],
  "timeframe": "7d"
}
```

## Report Templates

### Daily Chain Summary

```json
{
  "template": "daily_chain_summary",
  "parameters": {
    "chains": ["ethereum", "polygon"],
    "metrics": ["transaction_count", "unique_addresses", "gas_used", "average_gas_price"],
    "format": "pdf"
  },
  "schedule": "0 8 * * *"  // Daily at 8 AM
}
```

### Token Performance Report

```json
{
  "template": "token_performance",
  "parameters": {
    "tokens": ["USDC", "DAI", "WETH"],
    "metrics": ["price", "volume", "liquidity", "holder_count"],
    "comparison_period": "7d"
  }
}
```

## Analytics Models

### Gas Price Prediction

- **Features**: Historical gas prices, network congestion, time of day, pending tx count
- **Model**: Random Forest + LSTM hybrid
- **Accuracy**: MAE < 5 Gwei
- **Update Frequency**: Hourly

### Wallet Clustering

- **Features**: Transaction frequency, volume, token diversity, DeFi usage
- **Model**: K-means clustering
- **Categories**: Trader, Holder, DeFi User, NFT Collector, Bot

### Anomaly Detection

- **Features**: Transaction patterns, volume spikes, new contract deployments
- **Model**: Isolation Forest
- **Use Cases**: Fraud detection, market manipulation, unusual activity

## Monitoring

### Health Checks

The service provides comprehensive health status at `/health`:

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "databases": {
    "timescaledb": "connected",
    "clickhouse": "connected",
    "mongodb": "connected",
    "redis": "connected"
  },
  "analyzers": {
    "transaction": "active",
    "wallet": "active",
    "token": "active",
    "defi": "active"
  },
  "models": {
    "gas_prediction": {
      "last_update": "2024-01-10T10:00:00Z",
      "accuracy": 0.92
    }
  },
  "queries": {
    "active": 12,
    "queued": 45,
    "average_time_ms": 230
  }
}
```

### Metrics

Prometheus metrics available at `/metrics`:

- `analytics_queries_total` - Total queries by type
- `analytics_query_duration_seconds` - Query execution time
- `analytics_data_points_processed` - Data points processed
- `analytics_model_predictions_total` - ML predictions made
- `analytics_cache_hit_rate` - Cache effectiveness
- `analytics_report_generation_duration` - Report generation time

### Performance Dashboard

```json
{
  "period": "1h",
  "queries_processed": 5420,
  "average_query_time_ms": 145,
  "cache_hit_rate": 0.78,
  "data_ingested_gb": 12.5,
  "active_users": 342,
  "top_queries": [
    "wallet_analysis",
    "token_metrics",
    "gas_prediction"
  ]
}
```

## Optimization Strategies

1. **Query Optimization**
   - Materialized views for common queries
   - Query result caching
   - Parallel query execution

2. **Data Pipeline**
   - Stream processing for real-time data
   - Batch processing for historical analysis
   - Data partitioning by time and chain

3. **Storage Optimization**
   - TimescaleDB for time-series data
   - ClickHouse for analytical queries
   - MongoDB for flexible documents

4. **Caching Strategy**
   - Redis for hot data
   - Ignite for distributed caching
   - CDN for static reports

## Troubleshooting

### Common Issues

1. **Slow Queries**
   - Check query complexity
   - Verify indexes exist
   - Review data partitioning

2. **Model Accuracy Degradation**
   - Trigger model retraining
   - Update feature engineering
   - Check data quality

3. **Memory Issues**
   - Adjust query result limits
   - Optimize cache settings
   - Scale horizontally

### Debug Tools

```bash
# Analyze query performance
curl -X POST http://localhost:8006/api/v1/analytics/query/explain \
  -H "Content-Type: application/json" \
  -d '{"query": "..."}'

# Check model performance
curl http://localhost:8006/api/v1/models/gas_prediction/metrics

# View active queries
curl http://localhost:8006/api/v1/analytics/queries/active
```

## Contributing

1. Add tests for new analyzers
2. Document new metrics and models
3. Optimize query performance
4. Update visualization templates

## License

Copyright © 2024 PlatformQ. All rights reserved. 