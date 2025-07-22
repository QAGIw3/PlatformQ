# Market Making Service

Automated market making and liquidity provision service with integrated risk management.

## Features

### Market Making Strategies
- **Grid Trading**: Automated buy/sell orders at preset intervals
- **Cross-Market Arbitrage**: Exploit price differences across markets
- **Delta-Neutral Options MM**: Hedged options market making
- **Volatility Arbitrage**: Trade volatility mispricings
- **Liquidity Mining**: Earn rewards for providing liquidity

### Risk Integration
- **Real-time Risk Checks**: Ultra-low latency risk validation via direct communication
- **Pre-trade Risk Assessment**: Validate orders before placement
- **Margin Verification**: Ensure sufficient collateral
- **Position Limits**: Enforce maximum exposure limits
- **Dynamic Risk Adjustment**: Adapt strategies based on risk metrics

### Performance Features
- **Direct Communication**: Sub-millisecond risk checks via Apache Ignite
- **Strategy Runner**: Centralized strategy lifecycle management
- **Event-Driven Architecture**: Apache Pulsar for real-time events
- **High-Performance Caching**: Redis and Ignite for state management

## Architecture

### Components
1. **Strategy Runner**: Manages strategy lifecycle and dependencies
2. **Grid Trading Strategy**: Implements grid-based market making
3. **Cross-Market Arbitrage**: Identifies and executes arbitrage opportunities
4. **Delta-Neutral Options MM**: Options market making with delta hedging
5. **Risk Checker**: Integrates with Risk Engine for real-time risk validation
6. **AMM Engine**: Automated market maker for liquidity pools

### Technology Stack
- **Framework**: FastAPI with async/await
- **State Management**: Apache Ignite + Redis
- **Event Streaming**: Apache Pulsar
- **Risk Integration**: Direct communication via platformq-direct-comm
- **Serialization**: MessagePack for performance
- **Async Runtime**: uvloop for enhanced performance

## API Endpoints

### AMM Operations
- `POST /api/v1/amm/pools` - Create liquidity pool
- `GET /api/v1/amm/pools` - List pools
- `POST /api/v1/amm/pools/{pool_id}/liquidity` - Add liquidity
- `DELETE /api/v1/amm/pools/{pool_id}/liquidity` - Remove liquidity
- `POST /api/v1/amm/swap` - Execute swap
- `GET /api/v1/amm/quote` - Get swap quote

### Strategy Management
- `POST /api/v1/strategies` - Deploy strategy
- `GET /api/v1/strategies` - List strategies
- `PUT /api/v1/strategies/{strategy_id}` - Update strategy
- `DELETE /api/v1/strategies/{strategy_id}` - Stop strategy
- `GET /api/v1/strategies/{strategy_id}/performance` - Get performance

### Market Makers
- `POST /api/v1/market-makers` - Register market maker
- `GET /api/v1/market-makers` - List market makers
- `PUT /api/v1/market-makers/{mm_id}/params` - Update parameters
- `GET /api/v1/market-makers/{mm_id}/performance` - Get metrics

### Monitoring
- `GET /api/v1/monitoring/metrics` - Strategy metrics
- `GET /api/v1/monitoring/pnl` - P&L tracking
- `GET /api/v1/monitoring/inventory` - Inventory status
- `GET /api/v1/monitoring/orders` - Order activity

## Risk Integration

### Risk Checker
The service integrates with the Risk Engine Service for real-time risk validation:

```python
# Risk check before placing orders
risk_check = await risk_checker.check_pre_trade_risk(
    user_id=user_id,
    order={
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "price": price,
        "order_type": order_type
    }
)

# Margin verification
margin_check = await risk_checker.check_margin(
    user_id=user_id,
    required_margin=calculated_margin
)
```

### Direct Communication
- **Protocol**: Binary serialization over Ignite shared memory
- **Latency**: < 1ms for risk checks
- **Fallback**: HTTP API if direct communication fails
- **Caching**: Risk limits cached locally for performance

## Strategy Configuration

### Grid Trading
```python
GridConfig(
    grid_type=GridType.ARITHMETIC,
    lower_price=Decimal("90"),
    upper_price=Decimal("110"),
    grid_levels=20,
    order_size=Decimal("10"),
    max_position=Decimal("1000"),
    stop_loss=Decimal("85"),
    take_profit=Decimal("115")
)
```

### Cross-Market Arbitrage
```python
CrossMarketArbConfig(
    max_capital_per_trade=Decimal("100000"),
    min_profit_threshold=Decimal("100"),
    min_return_threshold=Decimal("0.001"),
    max_slippage=Decimal("0.002")
)
```

## Configuration

### Environment Variables
```bash
# Service Configuration
SERVICE_NAME=market-making-service
SERVICE_PORT=8080
SERVICE_ID=market-making-001

# Apache Ignite
IGNITE_HOST=localhost
IGNITE_PORT=10800

# Apache Pulsar
PULSAR_URL=pulsar://localhost:6650

# Redis
REDIS_URL=redis://localhost:6379

# Direct Communication
ENABLE_DIRECT_COMM=true
DIRECT_COMM_BATCH_SIZE=100
DIRECT_COMM_TIMEOUT_MS=5

# Risk Management
RISK_CHECK_REQUIRED=true
MAX_POSITION_VALUE=1000000
MAX_LEVERAGE=10
```

## Performance Metrics

### Latency Targets
- Order placement: < 10ms
- Risk checks: < 1ms (direct), < 50ms (HTTP)
- Strategy calculations: < 5ms
- Market data updates: < 2ms

### Throughput
- Orders per second: 10,000+
- Risk checks per second: 100,000+
- Active strategies: 1,000+
- Concurrent users: 5,000+

## Development

### Running Locally
```bash
cd services/MarketServices/market-making-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8080
```

### Running Tests
```bash
pytest tests/ -v
```

### Strategy Development
```python
# Example custom strategy
class MyStrategy(BaseStrategy):
    async def initialize(self):
        # Setup strategy
        pass
    
    async def on_market_data(self, data):
        # React to market updates
        pass
    
    async def execute(self):
        # Main strategy logic
        pass
```

## Monitoring

### Metrics
- Strategy P&L and returns
- Order fill rates
- Inventory levels
- Risk utilization
- System performance

### Health Checks
- `/health` - Service health
- `/ready` - Readiness probe
- `/metrics` - Prometheus metrics

## Security

- JWT-based authentication
- Role-based access control
- Rate limiting per strategy
- Order validation and sanitization
- Audit logging for all trades

## Dependencies

- **Risk Engine Service**: Real-time risk validation
- **Trading Core Service**: Order execution
- **Oracle Service**: Market data feeds
- **Auth Service**: Authentication
- **Apache Ignite**: State management
- **Apache Pulsar**: Event streaming 