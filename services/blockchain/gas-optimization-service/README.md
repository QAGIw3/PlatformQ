# Gas Optimization Service

An intelligent microservice that optimizes blockchain transaction costs through advanced strategies including batching, meta-transactions, Layer 2 solutions, and machine learning-based gas price prediction.

## Overview

The Gas Optimization Service helps minimize blockchain transaction costs for PlatformQ by implementing multiple optimization strategies. It analyzes transaction patterns, predicts optimal gas prices, batches transactions when possible, and intelligently routes transactions through Layer 2 solutions or meta-transaction relayers to achieve significant cost savings.

## Key Features

- **Multi-Strategy Optimization**: Batch processing, meta-transactions, L2 routing, and time-based optimization
- **Machine Learning Predictions**: RandomForest model for gas price forecasting
- **Dynamic Strategy Selection**: Automatic selection of best optimization strategy
- **Transaction Batching**: Combine multiple operations into single transactions
- **Meta-Transaction Support**: Gasless transactions for end users
- **Layer 2 Integration**: Automatic routing through Optimism, Arbitrum, and Polygon
- **Time-Based Optimization**: Schedule transactions during low-congestion periods
- **Real-Time Gas Monitoring**: Track gas prices across multiple networks
- **Cost Analysis**: Detailed savings reports and analytics
- **Smart Contract Integration**: Deploy and manage optimization contracts
- **High Performance**: Process thousands of optimization requests per second

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   API Layer (FastAPI)                    │
├─────────────────────────────────────────────────────────┤
│                  Gas Optimizer Core                      │
├──────────────┬────────────┬────────────┬───────────────┤
│    Batch     │    Meta    │    L2     │  Time-Based   │
│  Strategy    │ Transaction│  Strategy │   Strategy    │
├──────────────┴────────────┴────────────┴───────────────┤
│          ML Prediction Engine (scikit-learn)            │
├─────────────────────────────────────────────────────────┤
│  Gas Monitor │  Contract   │  Analytics │   Cache      │
│              │  Deployer   │   Engine   │ (Ignite)     │
└─────────────────────────────────────────────────────────┘
```

## API Endpoints

### Optimization Operations
- `POST /api/v1/optimize` - Optimize a transaction or batch
- `POST /api/v1/optimize/analyze` - Analyze optimization potential
- `GET /api/v1/optimize/{optimization_id}` - Get optimization details
- `POST /api/v1/optimize/{optimization_id}/execute` - Execute optimized transaction

### Gas Price Information
- `GET /api/v1/gas/prices` - Current gas prices by network
- `GET /api/v1/gas/predictions` - ML-based gas price predictions
- `GET /api/v1/gas/history` - Historical gas price data
- `POST /api/v1/gas/simulate` - Simulate transaction gas costs

### Batch Operations
- `POST /api/v1/batch/create` - Create transaction batch
- `POST /api/v1/batch/{batch_id}/add` - Add transaction to batch
- `POST /api/v1/batch/{batch_id}/execute` - Execute batch
- `GET /api/v1/batch/{batch_id}/savings` - Calculate batch savings

### Strategy Management
- `GET /api/v1/strategies` - List available strategies
- `GET /api/v1/strategies/recommend` - Get strategy recommendation
- `POST /api/v1/strategies/simulate` - Simulate strategy outcomes

### Analytics
- `GET /api/v1/analytics/savings` - Total savings report
- `GET /api/v1/analytics/usage` - Strategy usage statistics
- `GET /api/v1/analytics/performance` - Optimization performance metrics

### Monitoring
- `GET /health` - Service health check
- `GET /metrics` - Prometheus metrics endpoint

## Configuration

The service is configured via environment variables:

```bash
# Service Configuration
SERVICE_NAME=gas-optimization-service
SERVICE_HOST=0.0.0.0
SERVICE_PORT=8003
ENVIRONMENT=production

# Optimization Settings
BATCH_SIZE_LIMIT=100
BATCH_TIMEOUT_SECONDS=300
MIN_BATCH_SIZE=5
GAS_PRICE_BUFFER_PERCENT=10

# ML Model Configuration
MODEL_UPDATE_INTERVAL_HOURS=6
PREDICTION_HORIZON_BLOCKS=100
FEATURE_WINDOW_BLOCKS=1000
MODEL_CONFIDENCE_THRESHOLD=0.85

# External Services
BLOCKCHAIN_CONNECTOR_URL=http://blockchain-connector:8000
KEY_MANAGEMENT_URL=http://key-management:8002
TRANSACTION_PROCESSOR_URL=http://transaction-processor:8001

# Layer 2 Configuration
L2_OPTIMISM_RPC=https://mainnet.optimism.io
L2_ARBITRUM_RPC=https://arb1.arbitrum.io/rpc
L2_POLYGON_RPC=https://polygon-rpc.com

# Meta-Transaction Relayers
BICONOMY_API_KEY=${BICONOMY_API_KEY}
GELATO_API_KEY=${GELATO_API_KEY}

# Contract Addresses
BATCH_EXECUTOR_CONTRACT=0x1234...
META_TX_FORWARDER_CONTRACT=0x5678...

# Pulsar Configuration
PULSAR_SERVICE_URL=pulsar://pulsar:6650
PULSAR_OPTIMIZATION_TOPIC=persistent://platformq/gas/optimizations

# Ignite Cache Configuration
IGNITE_ENDPOINTS=ignite-node1:10800,ignite-node2:10800
IGNITE_GAS_CACHE=gas-price-cache

# Monitoring
ENABLE_METRICS=true
LOG_LEVEL=INFO
```

## Dependencies

- **FastAPI**: REST API framework
- **scikit-learn**: Machine learning for gas prediction
- **pandas**: Data analysis and manipulation
- **numpy**: Numerical computations
- **web3.py**: Ethereum interaction
- **httpx**: Async HTTP client
- **aiopulsar**: Event streaming
- **pyignite**: Distributed caching
- **prometheus-client**: Metrics collection
- **python-consul**: Service discovery

## Running the Service

### Using Docker

```bash
# Build the image
docker build -t gas-optimization-service .

# Run the container
docker run -d \
  --name gas-optimization \
  -p 8003:8003 \
  -e BLOCKCHAIN_CONNECTOR_URL="http://blockchain-connector:8000" \
  -e KEY_MANAGEMENT_URL="http://key-management:8002" \
  -e PULSAR_SERVICE_URL="pulsar://pulsar:6650" \
  -e IGNITE_ENDPOINTS="ignite:10800" \
  gas-optimization-service
```

### Using Docker Compose

```yaml
services:
  gas-optimization:
    build: ./services/gas-optimization-service
    ports:
      - "8003:8003"
    environment:
      - BLOCKCHAIN_CONNECTOR_URL=http://blockchain-connector:8000
      - KEY_MANAGEMENT_URL=http://key-management:8002
      - TRANSACTION_PROCESSOR_URL=http://transaction-processor:8001
      - PULSAR_SERVICE_URL=pulsar://pulsar:6650
      - IGNITE_ENDPOINTS=ignite:10800
    depends_on:
      - blockchain-connector
      - key-management
      - transaction-processor
      - pulsar
      - ignite
```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Train initial ML model
python scripts/train_gas_model.py

# Set environment variables
export BLOCKCHAIN_CONNECTOR_URL="http://localhost:8000"
export KEY_MANAGEMENT_URL="http://localhost:8002"

# Run the service
uvicorn app.main:app --host 0.0.0.0 --port 8003 --reload
```

## Optimization Strategies

### 1. Batch Strategy
Combines multiple transactions into a single batch transaction:
- Deploys multicall contract if needed
- Groups compatible operations
- Calculates optimal batch size
- Saves 40-60% on gas costs

### 2. Meta-Transaction Strategy
Enables gasless transactions for users:
- Integrates with Biconomy and Gelato
- Manages relayer relationships
- Handles signature verification
- Ideal for user onboarding

### 3. Layer 2 Strategy
Routes transactions through L2 networks:
- Automatic bridge detection
- Cost comparison across L2s
- Seamless failover to L1
- 90%+ gas savings possible

### 4. Time-Based Strategy
Schedules transactions during low-congestion periods:
- ML-based congestion prediction
- Automatic scheduling system
- Priority override support
- 20-40% typical savings

## Machine Learning Model

### Features
- Historical gas prices (1h, 4h, 24h windows)
- Block utilization percentage
- Pending transaction count
- Day of week and hour
- Network hashrate
- ETH price correlation

### Model Performance
- MAE: ~5 Gwei
- R²: 0.85
- Update frequency: 6 hours
- Training window: 30 days

## Monitoring

### Health Checks

The service provides detailed health status at `/health`:

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "strategies": {
    "batch": "active",
    "meta_tx": "active",
    "l2": "active",
    "time_based": "active"
  },
  "ml_model": {
    "last_update": "2024-01-10T08:00:00Z",
    "accuracy": 0.87,
    "predictions_made": 15420
  },
  "gas_prices": {
    "ethereum": {
      "fast": 35,
      "standard": 25,
      "slow": 20
    }
  }
}
```

### Metrics

Prometheus metrics available at `/metrics`:

- `gas_opt_optimizations_total` - Total optimizations by strategy
- `gas_opt_savings_wei` - Gas saved in Wei
- `gas_opt_ml_predictions_total` - ML predictions made
- `gas_opt_ml_accuracy` - Model accuracy score
- `gas_opt_batch_size` - Average batch size
- `gas_opt_strategy_selection` - Strategy selection distribution

### Cost Savings Report

```json
{
  "period": "24h",
  "total_transactions": 5420,
  "total_gas_saved": "152.3 ETH",
  "average_savings_percent": 42.5,
  "by_strategy": {
    "batch": {
      "count": 2100,
      "saved": "85.2 ETH"
    },
    "l2": {
      "count": 1800,
      "saved": "52.1 ETH"
    },
    "time_based": {
      "count": 1000,
      "saved": "10.5 ETH"
    },
    "meta_tx": {
      "count": 520,
      "saved": "4.5 ETH"
    }
  }
}
```

## Integration Examples

### Python SDK

```python
from gas_optimization_client import GasOptimizer

optimizer = GasOptimizer("http://gas-optimization:8003")

# Optimize a single transaction
result = await optimizer.optimize({
    "chain": "ethereum",
    "to": "0x...",
    "data": "0x...",
    "value": "1000000000000000000"
})

# Create and execute a batch
batch = await optimizer.create_batch("ethereum")
await batch.add_transaction(tx1)
await batch.add_transaction(tx2)
savings = await batch.calculate_savings()
await batch.execute()
```

### Direct API Usage

```bash
# Get optimization recommendation
curl -X POST http://localhost:8003/api/v1/optimize/analyze \
  -H "Content-Type: application/json" \
  -d '{
    "transactions": [...],
    "chain": "ethereum",
    "urgency": "low"
  }'

# Get current gas predictions
curl http://localhost:8003/api/v1/gas/predictions?chain=ethereum&blocks=10
```

## Smart Contracts

### Batch Executor
Executes multiple transactions in a single call:
```solidity
contract BatchExecutor {
    function batchExecute(
        address[] targets,
        bytes[] calldata datas,
        uint256[] values
    ) external payable;
}
```

### Meta-Transaction Forwarder
Handles meta-transaction verification and execution:
```solidity
contract MetaTxForwarder {
    function execute(
        ForwardRequest calldata req,
        bytes calldata signature
    ) external payable;
}
```

## Troubleshooting

### Common Issues

1. **Low Savings Rate**
   - Review transaction patterns
   - Adjust batch timeout settings
   - Check ML model accuracy

2. **Failed Optimizations**
   - Verify contract deployments
   - Check relayer balances
   - Review gas price buffers

3. **ML Model Drift**
   - Retrain with recent data
   - Adjust feature windows
   - Update model parameters

### Performance Tuning

1. **Batch Processing**
   - Optimize batch sizes
   - Adjust timeout parameters
   - Enable parallel processing

2. **ML Predictions**
   - Increase training frequency
   - Add additional features
   - Implement ensemble models

## Contributing

1. Test optimization strategies thoroughly
2. Validate gas savings calculations
3. Update ML model documentation
4. Add integration tests

## License

Copyright © 2024 PlatformQ. All rights reserved. 