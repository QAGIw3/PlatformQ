# Options Service

## Overview

The Options Service provides comprehensive options trading functionality within the PlatformQ ecosystem. It supports vanilla options (calls and puts), exotic options, and complex multi-leg strategies. The service handles option pricing, Greeks calculation, volatility modeling, and integrates with the Trading Core Service for order execution.

## Architecture

### Key Components

1. **Option Pricing Engine**: Black-Scholes and advanced pricing models
2. **Greeks Calculator**: Real-time Greeks computation
3. **Volatility Surface**: Implied volatility modeling and interpolation
4. **Strategy Builder**: Multi-leg option strategy construction
5. **Exercise Manager**: Automatic and manual exercise handling
6. **Risk Analytics**: Portfolio-level Greeks and risk metrics
7. **Integration Layer**: Connection to Trading Core Service

### Technology Stack

- **FastAPI**: REST API framework
- **NumPy/SciPy**: Numerical computations
- **QuantLib**: Advanced option pricing
- **HTTPX**: Async service communication
- **Redis**: Caching for volatility surface
- **PostgreSQL**: Option specifications storage

## Features

- **Vanilla Options**: European and American style calls/puts
- **Exotic Options**: Barriers, Asians, Lookbacks
- **Option Strategies**: Spreads, Straddles, Condors, etc.
- **Real-time Greeks**: Delta, Gamma, Theta, Vega, Rho
- **Volatility Modeling**: SABR, SVI, parametric surfaces
- **Auto-Exercise**: ITM options at expiration
- **Portfolio Margining**: Cross-margining with futures
- **Market Making**: Automated quote generation

## API Endpoints

### Option Contracts
- `POST /api/v1/options` - Create new option contract
- `GET /api/v1/options` - List available options
- `GET /api/v1/options/{symbol}` - Get option details
- `GET /api/v1/options/chain/{underlying}` - Get option chain

### Trading
- `POST /api/v1/orders` - Place option order
- `GET /api/v1/orders/{order_id}` - Get order status
- `DELETE /api/v1/orders/{order_id}` - Cancel order
- `GET /api/v1/positions` - Get option positions

### Greeks and Pricing
- `GET /api/v1/greeks/{option_id}` - Calculate Greeks
- `POST /api/v1/greeks/portfolio` - Portfolio Greeks
- `GET /api/v1/pricing/{option_id}` - Get theoretical price
- `GET /api/v1/volatility/{underlying}` - Volatility surface

### Strategies
- `GET /api/v1/strategies` - List available strategies
- `POST /api/v1/strategies/build` - Build custom strategy
- `POST /api/v1/strategies/execute` - Execute strategy
- `GET /api/v1/strategies/{strategy_id}` - Strategy details

## Option Types

### Vanilla Options
```json
{
  "symbol": "BTC-31DEC24-50000-C",
  "underlying": "BTC",
  "strike": 50000,
  "expiration": "2024-12-31T08:00:00Z",
  "type": "call",
  "style": "european",
  "contractSize": 0.01
}
```

### Exotic Options
```json
{
  "symbol": "ETH-BARRIER-3000-KO",
  "underlying": "ETH",
  "strike": 3000,
  "barrier": 3500,
  "type": "up-and-out-call",
  "style": "european",
  "expiration": "2024-06-30T08:00:00Z"
}
```

## Pricing Models

### Black-Scholes
- European options
- Assumes log-normal distribution
- Constant volatility

### Binomial Trees
- American options
- Early exercise feature
- Discrete time steps

### Monte Carlo
- Path-dependent options
- Complex payoffs
- Stochastic volatility

## Greeks Calculation

### First-Order Greeks
- **Delta (Δ)**: Price sensitivity to underlying
- **Vega (ν)**: Sensitivity to volatility
- **Theta (Θ)**: Time decay
- **Rho (ρ)**: Interest rate sensitivity

### Second-Order Greeks
- **Gamma (Γ)**: Delta sensitivity to underlying
- **Vanna**: Delta sensitivity to volatility
- **Volga**: Vega sensitivity to volatility

### Portfolio Greeks
```python
# Aggregate Greeks calculation
portfolio_delta = sum(position.quantity * position.delta)
portfolio_gamma = sum(position.quantity * position.gamma)
```

## Volatility Surface

### Construction Methods
1. **Sticky Strike**: Fixed strikes across time
2. **Sticky Delta**: Fixed deltas across time
3. **SABR Model**: Stochastic volatility
4. **SVI Model**: Parametric surface

### Interpolation
- Linear in variance
- Cubic splines
- No-arbitrage constraints

## Option Strategies

### Supported Strategies

| Strategy | Legs | Risk Profile | Use Case |
|----------|------|--------------|----------|
| Call Spread | 2 | Limited | Bullish |
| Put Spread | 2 | Limited | Bearish |
| Straddle | 2 | Unlimited | Volatility |
| Strangle | 2 | Unlimited | Volatility |
| Iron Condor | 4 | Limited | Range-bound |
| Butterfly | 3-4 | Limited | Low volatility |
| Calendar | 2 | Complex | Time decay |

### Strategy Builder
```json
{
  "strategy": "iron_condor",
  "legs": [
    {"action": "buy", "type": "put", "strike": 45000},
    {"action": "sell", "type": "put", "strike": 47000},
    {"action": "sell", "type": "call", "strike": 53000},
    {"action": "buy", "type": "call", "strike": 55000}
  ]
}
```

## Risk Management

### Position Limits
- Maximum contracts per strike
- Concentration limits
- Net exposure limits

### Margin Requirements
- SPAN-based margining
- Portfolio margining
- Stress testing

### Exercise Risk
- Pin risk management
- Auto-exercise parameters
- Early exercise alerts

## Configuration

```python
# Pricing Configuration
RISK_FREE_RATE = 0.05
DIVIDEND_YIELD = 0.02
PRICING_MODEL = "black_scholes"

# Greeks Configuration
GREEK_BUMP_SIZE = 0.01
GREEK_TIME_BUMP = 1/365

# Volatility Surface
VOL_SURFACE_MODEL = "SABR"
VOL_INTERPOLATION = "cubic"

# Risk Parameters
MAX_POSITION_SIZE = 10000
MAX_PORTFOLIO_VEGA = 100000
MARGIN_MULTIPLIER = 1.2

# Trading Core Integration  
TRADING_CORE_URL = "http://trading-core-service:8001"
```

## Market Making

### Quote Generation
- Two-sided quotes
- Dynamic spread adjustment
- Volatility-based pricing
- Inventory management

### Risk Controls
- Delta hedging
- Vega limits
- Gamma scalping
- Stop-loss rules

## Integration

### Trading Core Service
1. Option order validation
2. Forward to Trading Core
3. Execution confirmation
4. Position update

### Market Data
- Underlying price feeds
- Interest rate curves
- Dividend schedules
- Volatility indices

## Monitoring

Prometheus metrics at `/metrics`:

- `options_contracts_active`: Active option contracts
- `options_open_interest`: Open interest by strike
- `options_volume_daily`: Daily trading volume
- `options_iv_spread`: Bid-ask IV spread
- `options_exercise_total`: Exercise count
- `options_pricing_latency`: Pricing calculation time

## Development

### Project Structure

```
options-service/
├── app/
│   ├── api/          # REST endpoints
│   ├── core/         # Core logic
│   ├── models/       # Data models  
│   ├── pricing/      # Pricing engines
│   ├── greeks/       # Greeks calculators
│   ├── strategies/   # Strategy builders
│   ├── config.py     # Configuration
│   └── main.py       # FastAPI app
├── notebooks/        # Research notebooks
├── scripts/          # Utility scripts
├── tests/           # Tests
└── requirements.in  # Dependencies
```

### Testing

```bash
# Unit tests
pytest tests/unit

# Pricing tests
pytest tests/pricing

# Strategy tests
pytest tests/strategies

# Integration tests
pytest tests/integration
```

## Performance

- **Pricing Latency**: < 5ms per option
- **Greeks Calculation**: < 10ms full set
- **Surface Update**: < 100ms
- **Strategy Analysis**: < 50ms
- **API Response**: < 25ms p99

## Backtesting

### Historical Analysis
- Strategy performance
- Greeks accuracy
- Volatility model validation
- Exercise optimization

### Simulation
```bash
# Run strategy backtest
python scripts/backtest_strategy.py \
  --strategy iron_condor \
  --start 2023-01-01 \
  --end 2023-12-31
```

## Security

- **Authentication**: JWT tokens
- **Authorization**: Trading permissions
- **Rate Limiting**: Per-user quotas
- **Validation**: Strike/expiry checks
- **Audit Trail**: All trades logged

## Best Practices

1. **Accurate Pricing**: Regular model calibration
2. **Risk Limits**: Conservative position limits
3. **Exercise Management**: Clear policies
4. **Market Making**: Proper hedging
5. **Documentation**: Strategy guides

## Future Enhancements

- Variance swaps
- Exotic option support expansion
- Machine learning for volatility
- Cross-asset options
- Decentralized settlement

## Contributing

See the main PlatformQ [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines. 