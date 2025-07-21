# Infrastructure Oracle Service

Real-time pricing and metrics oracle for Infrastructure DeFi on PlatformQ.

## Overview

The Infrastructure Oracle Service provides accurate, real-time pricing data for compute resources (CPU, GPU, Storage, Bandwidth, Memory) by aggregating data from multiple sources. It serves as the authoritative price feed for the Infrastructure DeFi ecosystem.

## Features

- **Multi-Source Price Aggregation**: Combines data from CloudKitty, Prometheus, market data, and external providers
- **Real-Time Pricing**: Current prices for all resource types, regions, and service tiers
- **Historical Data**: Price history and trend analysis
- **Volatility Calculation**: Risk metrics for resource pricing
- **Price Forecasting**: ML-based price predictions
- **Resource Metrics**: Utilization, capacity, and SLA compliance data
- **On-Chain Updates**: Pushes prices to ResourceToken smart contract

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   API Layer (FastAPI)                    │
├─────────────────────────────────────────────────────────┤
│                  Oracle Engine Core                      │
├─────────────────────────────────────────────────────────┤
│                 Price Aggregator                         │
├─────────────────────────────────────────────────────────┤
│                   Data Sources                           │
│  ┌─────────────┬──────────────┬─────────────┬─────────┐ │
│  │ CloudKitty  │  Prometheus  │Market Data  │External │ │
│  │   Source    │   Source     │  Source     │Providers│ │
│  └─────────────┴──────────────┴─────────────┴─────────┘ │
├─────────────────────────────────────────────────────────┤
│              Blockchain Integration                      │
│  ┌─────────────────────────────────────────────────────┐ │
│  │          ResourceToken Smart Contract                │ │
│  └─────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

## API Endpoints

### Pricing

- `GET /api/v1/price` - Get current resource price
  ```json
  {
    "resource_type": "gpu",
    "region": "us-east-1",
    "tier": "premium",
    "quantity": 10,
    "duration_hours": 24
  }
  ```

- `GET /api/v1/price/history` - Get historical prices
- `GET /api/v1/volatility/{resource_type}` - Calculate price volatility
- `GET /api/v1/forecast/{resource_type}` - Get price forecast

### Metrics

- `GET /api/v1/metrics/{resource_type}` - Get resource metrics
  ```json
  {
    "utilization": 0.75,
    "available_capacity": 250,
    "total_capacity": 1000,
    "average_sla_compliance": 0.998,
    "price_volatility": 0.12
  }
  ```

### Admin

- `POST /api/v1/price/update` - Manual price update (requires admin)

### Health

- `GET /health` - Service health check
- `GET /metrics` - Prometheus metrics

## Configuration

### Environment Variables

```bash
# Service configuration
SERVICE_PORT=8095
ENVIRONMENT=production

# Blockchain
CHAIN_ID=1
RPC_URL=https://eth-mainnet.example.com
ORACLE_CONTRACT_ADDRESS=0x...
ORACLE_PRIVATE_KEY=0x...

# Data sources
CLOUDKITTY_URL=http://cloudkitty:8889
PROMETHEUS_URL=http://prometheus:9090
MARKET_DATA_URL=http://market-data-service:8080

# Oracle settings
UPDATE_INTERVAL_SECONDS=300
PRICE_AGGREGATION_METHOD=weighted_average
MINIMUM_DATA_SOURCES=2
PRICE_DEVIATION_THRESHOLD=0.1

# Cache
IGNITE_HOST=ignite
IGNITE_PORT=10800
CACHE_TTL_SECONDS=300
```

## Price Aggregation

The oracle uses a weighted average approach to aggregate prices from multiple sources:

1. **CloudKitty**: Historical cost data (weight: 30%)
2. **Prometheus**: Real-time utilization-based pricing (weight: 30%)
3. **Market Data**: DeFi market prices (weight: 30%)
4. **External Providers**: AWS/GCP spot prices (weight: 10%)

### Outlier Detection

Prices that deviate more than the configured threshold from the median are excluded.

### Confidence Score

Each price update includes a confidence score (0-1) based on:
- Number of available data sources
- Consistency between sources
- Data freshness
- Historical accuracy

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export ENVIRONMENT=development
export RPC_URL=http://localhost:8545

# Run the service
uvicorn app.main:app --reload --port 8095
```

### Testing

```bash
# Run unit tests
pytest tests/

# Run integration tests
pytest tests/integration/
```

### Building Docker Image

```bash
docker build -t infrastructure-oracle-service:latest .
```

## Deployment

### Docker Compose

```yaml
infrastructure-oracle:
  image: infrastructure-oracle-service:latest
  ports:
    - "8095:8095"
  environment:
    - RPC_URL=${RPC_URL}
    - ORACLE_CONTRACT_ADDRESS=${ORACLE_CONTRACT_ADDRESS}
    - ORACLE_PRIVATE_KEY=${ORACLE_PRIVATE_KEY}
  depends_on:
    - ignite
    - prometheus
  networks:
    - platformq-network
```

### Kubernetes

See `iac/kubernetes/charts/infrastructure-oracle/` for Helm charts.

## Security

- Private keys are stored in HashiCorp Vault
- All external API calls use TLS
- Rate limiting on public endpoints
- JWT authentication for admin endpoints
- Input validation and sanitization

## Monitoring

### Metrics

- `oracle_price_updates_total` - Total price updates by resource type
- `oracle_update_latency_seconds` - Update latency by data source
- `resource_price_usd` - Current prices by resource/region/tier
- `resource_utilization_percent` - Resource utilization

### Alerts

- Price deviation > 20% from previous
- Data source unavailable > 5 minutes
- Update failures > 10 in 5 minutes
- Low confidence scores < 0.7

## Troubleshooting

### Common Issues

1. **Price updates failing**
   - Check blockchain connection
   - Verify contract address and ABI
   - Ensure sufficient gas in oracle wallet

2. **Data source timeout**
   - Check network connectivity
   - Verify data source health
   - Increase timeout settings

3. **Price volatility too high**
   - Review aggregation weights
   - Check for outlier sources
   - Adjust deviation threshold

## License

Copyright (c) 2024 PlatformQ. All rights reserved. 