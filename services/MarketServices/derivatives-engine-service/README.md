# Derivatives Engine Service

A high-performance derivatives trading engine for the platformQ ecosystem with comprehensive compute market integration.

## Features

### Core Trading Features
- Real-time order matching engine with sub-millisecond latency
- Multi-asset derivatives support (futures, options, swaps, structured products)
- Advanced risk management with real-time position monitoring
- Portfolio margining with cross-product netting
- Market making support with dedicated APIs
- Compliant liquidity pools with KYC/AML integration
- DeFi lending integration for collateral efficiency

### Compute Market Integration
- **Compute Futures Trading** - Trade compute capacity futures contracts for GPU, CPU, memory, and storage
- **Partner Capacity Management** - Wholesale capacity procurement from cloud partners (Rackspace, AWS, Azure, GCP)
- **Cross-Service Capacity Coordination** - Unified compute allocation across all platform services
- **Wholesale Arbitrage Engine** - Automated arbitrage between wholesale and retail compute prices
- **Compute Quality Derivatives** - Latency futures, uptime swaps, performance bonds
- **Physical Settlement** - Automatic compute resource provisioning on contract expiry
- **SLA Enforcement** - Automated monitoring and penalty application

### Advanced Features
- **Variance Swaps** - Trade realized vs implied volatility
- **Structured Products** - Create custom payoff structures
- **Dynamic Risk Limits** - Real-time risk limit management
- **Monitoring Dashboard** - Comprehensive system monitoring

### Graph Intelligence Integration
- **Risk Assessment** - Multi-dimensional risk scoring (counterparty, operational, market, liquidity, reputation, systemic)
- **Trust-Based Collateral** - Reputation directly reduces margin requirements (up to $50k unsecured for top users)
- **Smart Recommendations** - AI-powered risk mitigation strategies and position sizing
- **Network Analysis** - Centrality and cluster analysis for systemic risk assessment
- **Behavior Prediction** - ML-based prediction of trader behavior and defaults

### Asset-Compute-Model Nexus
- **Digital Asset Collateral** - Accept NFTs, ML models, datasets, algorithms as collateral
- **Model-Compute Bundles** - Create tradeable packages of ML models with guaranteed compute
- **Asset-Backed Futures** - Issue compute futures backed by digital asset collateral
- **Synthetic Data Derivatives** - Trade contracts for on-demand synthetic data generation
- **Portfolio Optimization** - Balance digital assets and compute resources for maximum capital efficiency

## Architecture

The service is built using:
- **FastAPI** for high-performance REST APIs
- **Apache Ignite** for in-memory caching and distributed computing
- **Apache Pulsar** for event streaming
- **Cassandra** for persistent storage
- **Milvus** for vector similarity search
- **Apache Flink** for stream processing

## API Endpoints

### Trading APIs
- `POST /api/v1/orders` - Place trading orders
- `GET /api/v1/orders/{order_id}` - Get order status
- `DELETE /api/v1/orders/{order_id}` - Cancel order
- `GET /api/v1/positions` - Get current positions
- `GET /api/v1/markets` - Get available markets

### Compute Futures APIs
- `GET /api/v1/compute-futures/markets` - Get compute futures markets
- `POST /api/v1/compute-futures/day-ahead/bid` - Submit day-ahead bid
- `POST /api/v1/compute-futures/day-ahead/offer` - Submit day-ahead offer
- `GET /api/v1/compute-futures/prices/{delivery_date}` - Get cleared prices
- `POST /api/v1/compute-futures/settlement/initiate` - Initiate physical settlement

### Partner Capacity APIs
- `POST /api/v1/partners/contracts` - Register partner contract
- `GET /api/v1/partners/best-price` - Get best wholesale price
- `POST /api/v1/partners/purchase` - Purchase wholesale capacity
- `GET /api/v1/partners/inventory` - Get available inventory
- `GET /api/v1/partners/arbitrage/opportunities` - Get arbitrage opportunities

### Cross-Service Capacity APIs
- `POST /api/v1/capacity/request` - Request capacity allocation
- `GET /api/v1/capacity/allocation/{allocation_id}` - Get allocation status
- `GET /api/v1/capacity/forecast` - Get capacity demand forecast
- `POST /api/v1/capacity/optimize` - Trigger allocation optimization

### Risk Intelligence APIs (Graph Intelligence)
- `POST /api/v1/risk-intelligence/assess-risk` - Comprehensive counterparty risk assessment
- `GET /api/v1/risk-intelligence/trust-score/{entity_id}` - Get multi-dimensional trust score
- `GET /api/v1/risk-intelligence/usage-patterns/{entity_id}` - Historical compute usage patterns
- `POST /api/v1/risk-intelligence/recommendations` - Get AI-powered recommendations
- `GET /api/v1/risk-intelligence/similar-entities/{entity_id}` - Find similar traders
- `POST /api/v1/risk-intelligence/predict-behavior` - Predict future trading behavior
- `GET /api/v1/risk-intelligence/network-analysis/{entity_id}` - Analyze network position

### Asset-Compute Nexus APIs
- `POST /api/v1/asset-compute-nexus/value-asset` - Value digital asset as collateral
- `POST /api/v1/asset-compute-nexus/create-bundle` - Create model-compute bundle
- `POST /api/v1/asset-compute-nexus/create-asset-future` - Create asset-backed compute future
- `GET /api/v1/asset-compute-nexus/model-metrics/{model_id}` - Get ML model performance
- `GET /api/v1/asset-compute-nexus/model-collateral-value/{model_id}` - Evaluate model as collateral
- `POST /api/v1/asset-compute-nexus/create-synthetic-data` - Create synthetic data derivative
- `POST /api/v1/asset-compute-nexus/optimize-portfolio` - Optimize asset-compute portfolio
- `GET /api/v1/asset-compute-nexus/asset-types` - List supported asset types

## Configuration

Key environment variables:
```
PULSAR_URL=pulsar://pulsar:6650
IGNITE_HOST=ignite
CASSANDRA_HOSTS=cassandra
MILVUS_HOST=milvus
ORACLE_SERVICE_URL=http://blockchain-gateway-service:8000
```

## Integration with Other Services

### MLOps Service
- Automatic compute reservation for model training
- GPU resource allocation for ML workloads
- Cost optimization for training jobs

### Simulation Service
- Compute allocation for large-scale simulations
- Performance-based derivatives for simulation accuracy

### Digital Asset Service
- Model training compute futures
- Dataset processing capacity contracts

### Provisioning Service
- Automatic resource provisioning on settlement
- Partner capacity allocation
- SLA monitoring and enforcement

## Monitoring

The service includes comprehensive monitoring:
- Prometheus metrics at `/metrics`
- Health check at `/health`
- Readiness check at `/ready`
- Custom dashboards for trading, risk, and capacity metrics

## Development

### Running Locally
```bash
cd services/derivatives-engine-service
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app/main.py
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t platformq/derivatives-engine-service .
```

## Security

- JWT-based authentication
- Role-based access control (RBAC)
- API key management
- Rate limiting
- Audit logging
- Encryption at rest and in transit

## Performance

- Handles 100,000+ orders per second
- Sub-millisecond order matching latency
- Horizontal scaling support
- In-memory caching for hot data
- Optimized for high-frequency trading
