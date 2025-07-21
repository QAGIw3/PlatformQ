# Futures Service

## Overview

The Futures Service manages futures contracts trading within the PlatformQ ecosystem. It provides functionality for creating futures contracts, managing positions, calculating funding rates, handling settlement, and integrating with the Trading Core Service for order execution. The service supports perpetual futures, dated futures, and custom contract specifications.

## Architecture

### Key Components

1. **Contract Manager**: Creates and manages futures contract specifications
2. **Funding Rate Engine**: Calculates and applies funding rates for perpetual contracts
3. **Settlement Engine**: Handles contract expiration and settlement
4. **Mark Price Calculator**: Determines fair value for margin calculations
5. **Basis Tracker**: Monitors spot-futures basis and arbitrage opportunities
6. **Position Manager**: Tracks futures positions and P&L
7. **Integration Layer**: Connects with Trading Core Service

### Technology Stack

- **FastAPI**: REST API framework
- **HTTPX**: Async HTTP client for service communication
- **Pydantic**: Data validation and serialization
- **Apache Pulsar**: Event streaming
- **Redis**: Caching for funding rates and mark prices
- **PostgreSQL**: Contract specifications storage

## Features

- **Perpetual Futures**: Continuous contracts with funding mechanism
- **Dated Futures**: Fixed expiration date contracts
- **Custom Contracts**: Configurable contract specifications
- **Funding Rates**: Real-time funding rate calculation
- **Auto-Deleveraging**: Risk management for extreme markets
- **Cross/Isolated Margin**: Flexible margin modes
- **Settlement Types**: Cash and physical settlement
- **Index Price**: Composite price from multiple sources

## API Endpoints

### Contract Management
- `POST /api/v1/contracts` - Create new futures contract
- `GET /api/v1/contracts` - List all contracts
- `GET /api/v1/contracts/{symbol}` - Get contract details
- `PUT /api/v1/contracts/{symbol}` - Update contract specs

### Trading
- `POST /api/v1/orders` - Place futures order
- `GET /api/v1/orders/{order_id}` - Get order status
- `DELETE /api/v1/orders/{order_id}` - Cancel order
- `GET /api/v1/positions` - Get user positions

### Funding and Settlement
- `GET /api/v1/funding/current` - Current funding rates
- `GET /api/v1/funding/history` - Historical funding rates
- `GET /api/v1/settlement/upcoming` - Upcoming settlements
- `GET /api/v1/mark-price/{symbol}` - Get mark price

### Market Data
- `GET /api/v1/basis/{symbol}` - Spot-futures basis
- `GET /api/v1/open-interest/{symbol}` - Open interest
- `GET /api/v1/liquidations` - Recent liquidations
- `GET /api/v1/insurance-fund` - Insurance fund status

## Contract Types

### Perpetual Futures
```json
{
  "symbol": "BTC-PERP",
  "underlying": "BTC",
  "quoteCurrency": "USDT",
  "contractSize": 0.001,
  "type": "perpetual",
  "fundingInterval": 28800,  // 8 hours
  "maxLeverage": 100
}
```

### Dated Futures
```json
{
  "symbol": "ETH-0330",
  "underlying": "ETH",
  "quoteCurrency": "USDT",
  "contractSize": 0.01,
  "type": "dated",
  "expirationDate": "2024-03-30T08:00:00Z",
  "settlementType": "cash"
}
```

## Funding Rate Calculation

### Formula
```
Funding Rate = (Premium Index + clamp(Interest Rate - Premium Index, 0.05%, -0.05%))

Premium Index = (Max(0, Impact Bid Price - Mark Price) - Max(0, Mark Price - Impact Ask Price)) / Spot Price

Interest Rate = 0.01% (default)
```

### Application
- Calculated every minute
- Applied every 8 hours (configurable)
- Paid between long and short positions
- Capped at ±0.5% per period

## Mark Price

### Calculation Method
1. **Index Price**: Weighted average from multiple exchanges
2. **Fair Price**: Index + 30-second EMA of (Futures - Index)
3. **Mark Price**: Used for margin calculations and liquidations

### Protection Mechanisms
- Price band limits (±0.5% from index)
- Single exchange failure handling
- Manipulation detection

## Settlement Process

### Perpetual Contracts
- No expiration
- Continuous trading
- Funding payments only

### Dated Contracts
1. **Trading Halt**: 1 hour before expiration
2. **Settlement Price**: 1-hour TWAP of index
3. **Position Settlement**: Automatic cash settlement
4. **Final P&L**: Calculated and distributed

## Risk Management

### Margin Requirements
- **Initial Margin**: 1% - 10% based on leverage
- **Maintenance Margin**: 0.5% - 5% of position
- **Auto-Deleveraging**: When insurance fund depleted

### Position Limits
```python
# Per-user limits
MAX_POSITION_SIZE = 1000000  # USD notional
MAX_OPEN_ORDERS = 200
MAX_LEVERAGE = 100

# Per-contract limits  
MAX_OPEN_INTEREST = 50000000  # USD
CONCENTRATION_LIMIT = 0.1  # 10% of OI
```

## Integration with Trading Core

### Order Flow
1. User submits order to Futures Service
2. Service validates contract and margin
3. Order forwarded to Trading Core
4. Execution confirmation received
5. Position and margin updated

### Event Streams
- Order updates
- Trade executions
- Position changes
- Liquidation events

## Configuration

```python
# Funding Configuration
DEFAULT_FUNDING_INTERVAL = 28800  # 8 hours
MAX_FUNDING_RATE = 0.005  # 0.5%
FUNDING_RATE_CLAMP = 0.0005  # 0.05%

# Settlement Configuration
SETTLEMENT_WINDOW = 3600  # 1 hour
TWAP_INTERVALS = 60  # 1 minute

# Risk Parameters
DEFAULT_INITIAL_MARGIN = 0.01  # 1%
DEFAULT_MAINTENANCE_MARGIN = 0.005  # 0.5%
LIQUIDATION_PENALTY = 0.0025  # 0.25%

# Trading Core Integration
TRADING_CORE_URL = "http://trading-core-service:8001"
REQUEST_TIMEOUT = 5.0  # seconds
```

## Monitoring

Prometheus metrics at `/metrics`:

- `futures_contracts_active`: Active contracts count
- `futures_open_interest`: Open interest by contract
- `futures_funding_rate`: Current funding rates
- `futures_liquidations_total`: Liquidation count
- `futures_settlement_value`: Settlement values
- `futures_basis_spread`: Spot-futures basis

## Development

### Project Structure

```
futures-service/
├── app/
│   ├── api/          # REST endpoints
│   ├── core/         # Business logic
│   ├── models/       # Data models
│   ├── services/     # External services
│   ├── utils/        # Utilities
│   ├── config.py     # Configuration
│   └── main.py       # FastAPI app
├── scripts/          # Management scripts
├── tests/           # Tests
├── Dockerfile       # Container config
└── requirements.in  # Dependencies
```

### Testing

```bash
# Unit tests
pytest tests/unit

# Integration tests
pytest tests/integration

# Contract simulation
python scripts/simulate_contract.py --type perpetual
```

## Performance

- **Order Latency**: < 10ms to Trading Core
- **Funding Calculation**: < 100ms per contract
- **Settlement Processing**: < 5 minutes
- **API Response Time**: < 50ms p99

## Security

- **Authentication**: JWT via Auth Service
- **Authorization**: Role-based access
- **Rate Limiting**: Per-user and global
- **Position Validation**: Real-time checks
- **Audit Trail**: All actions logged

## Deployment

### Dependencies
- Trading Core Service
- Auth Service
- Market Data feeds
- PostgreSQL database
- Redis cache

### High Availability
- Multiple service instances
- Database replication
- Cache clustering
- Circuit breakers

## Best Practices

1. **Contract Design**: Clear specifications
2. **Risk Parameters**: Conservative defaults
3. **Testing**: Thorough settlement testing
4. **Monitoring**: Real-time metrics
5. **Documentation**: Clear user guides

## Future Enhancements

- Quanto futures support
- Options on futures
- Spread trading
- Portfolio margining
- Cross-chain settlement

## Contributing

See the main PlatformQ [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines. 