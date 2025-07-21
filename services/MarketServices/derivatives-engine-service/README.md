# Derivatives Engine Service

## Overview

The Derivatives Engine Service is the core trading platform for advanced derivative products on PlatformQ. After recent refactoring, this service now focuses on compute-based derivatives, synthetic derivatives, and specialized trading mechanisms while delegating options, structured products, and risk management to dedicated services.

## Architecture

### Current Focus Areas

1. **Compute Derivatives**
   - Compute Futures Trading
   - Compute Spot Market
   - Burst Compute Derivatives
   - Compute-backed Stablecoins

2. **Synthetic Derivatives**
   - Custom synthetic instruments
   - Cross-market derivatives
   - Variance swaps

3. **Trading Infrastructure**
   - High-performance matching engine
   - Real-time settlement
   - Advanced margin system
   - Partner capacity management
   - Wholesale arbitrage engine

4. **Integrations**
   - Graph Intelligence for risk analysis
   - Digital Asset collateral support
   - Cross-service capacity coordination

### Migrated Components

The following components have been migrated to dedicated services:

- **Options Trading** → [Options Service](../options-service)
- **Options AMM** → [Market Making Service](../market-making-service)
- **Structured Products** → [Structured Products Service](../structured-products-service)
- **Risk Calculations** → [Risk Engine Service](../risk-engine-service)
- **Risk Monitoring** → [Risk Management Service](../risk-management-service)

## Key Features

- **Compute Futures**: Trade futures contracts on computational resources
- **Burst Compute Derivatives**: Short-term compute capacity derivatives
- **Compute Stablecoins**: Stablecoins backed by compute resources
- **Synthetic Derivatives**: Create custom derivative instruments
- **Variance Swaps**: Trade realized vs implied volatility
- **Partner Capacity**: Integrate external compute providers
- **Cross-service Coordination**: Optimize capacity across services

## API Endpoints

### Markets
- `GET /api/v1/markets` - List available markets
- `POST /api/v1/markets` - Create new market
- `GET /api/v1/markets/{market_id}` - Get market details

### Trading
- `POST /api/v1/trading/orders` - Place order
- `GET /api/v1/trading/orders/{order_id}` - Get order status
- `DELETE /api/v1/trading/orders/{order_id}` - Cancel order

### Positions
- `GET /api/v1/positions` - List positions
- `GET /api/v1/positions/{position_id}` - Get position details

### Compute Derivatives
- `POST /api/v1/compute-futures/orders` - Trade compute futures
- `GET /api/v1/compute-spot/prices` - Get spot compute prices
- `POST /api/v1/burst-compute/request` - Request burst compute

### WebSocket
- `WS /ws/market/{market_id}` - Real-time market data

## Dependencies

- FastAPI for REST API
- Apache Ignite for distributed caching
- Apache Pulsar for event streaming
- Graph Intelligence Service for analytics
- Digital Asset Service for collateral
- Oracle Service for price feeds

## Configuration

Key configuration parameters:

```python
# Matching Engine
MATCHING_ENGINE_THREADS = 4
ORDER_BOOK_DEPTH = 100

# Settlement
SETTLEMENT_INTERVAL_SECONDS = 60
SETTLEMENT_BATCH_SIZE = 1000

# Compute Markets
COMPUTE_SPOT_UPDATE_INTERVAL = 5
BURST_COMPUTE_MAX_DURATION = 3600

# Partner Capacity
PARTNER_SYNC_INTERVAL = 300
CAPACITY_RESERVE_RATIO = 0.2
```

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run service
python -m uvicorn app.main:app --reload --port 8000
```

### Testing

```bash
# Run unit tests
pytest tests/unit

# Run integration tests
pytest tests/integration
```

## Integration with Other Services

- **Risk Engine Service**: Receives risk calculations and VaR metrics
- **Risk Management Service**: Receives real-time risk alerts
- **Options Service**: Handles all options trading
- **Structured Products Service**: Manages structured products
- **Market Making Service**: Provides options AMM functionality

## Monitoring

- Prometheus metrics at `/metrics`
- Health check at `/health`
- Performance dashboard via integrated monitoring

## Future Enhancements

- Additional compute derivative types
- Enhanced synthetic derivative builder
- Improved cross-market arbitrage
- Advanced partner capacity algorithms
