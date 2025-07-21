# Risk Engine Service

## Overview

The Risk Engine Service provides real-time risk assessment, margin calculation, and portfolio risk management for the PlatformQ trading platform. It integrates with Apache Flink for streaming risk analytics and Apache Ignite for distributed risk state management, enabling microsecond-latency risk decisions.

## Architecture

### Key Components

1. **Risk Calculator**: Real-time position and portfolio risk assessment
2. **Margin Calculator**: Dynamic margin requirements based on market conditions
3. **VaR Calculator**: Value at Risk calculation using multiple methodologies
4. **Stress Tester**: Scenario-based stress testing for extreme market conditions
5. **Liquidation Engine**: Automated position liquidation for margin violations
6. **Flink Risk Processor**: Streaming risk analytics and alerting
7. **Ignite Risk Cache**: Distributed cache for risk metrics and limits

### Technology Stack

- **FastAPI**: High-performance REST API framework
- **Apache Flink**: Real-time risk analytics and event processing
- **Apache Ignite**: Distributed risk state and cache management
- **Apache Pulsar**: Event streaming for risk events
- **NumPy/SciPy**: Numerical computations for risk models
- **scikit-learn**: Machine learning for risk prediction
- **Prometheus**: Metrics and monitoring

## Features

- **Real-time Risk Monitoring**: Microsecond-latency risk calculations
- **Portfolio Risk**: Cross-asset portfolio risk aggregation
- **Dynamic Margins**: Market-based margin adjustments
- **VaR Models**: Historical, Parametric, and Monte Carlo VaR
- **Stress Testing**: Configurable market scenarios
- **Risk Limits**: Position, exposure, and leverage limits
- **Liquidation Management**: Automated and manual liquidation
- **ML Risk Models**: Predictive risk analytics

## API Endpoints

### Risk Assessment
- `GET /api/v1/risk/portfolio/{user_id}` - Get portfolio risk metrics
- `GET /api/v1/risk/position/{position_id}` - Get position risk
- `POST /api/v1/risk/calculate` - Calculate risk for hypothetical position

### Margin Management
- `GET /api/v1/margin/{user_id}` - Get margin requirements
- `GET /api/v1/margin/available/{user_id}` - Get available margin
- `POST /api/v1/margin/call/{user_id}` - Issue margin call

### VaR and Stress Testing
- `GET /api/v1/var/{portfolio_id}` - Calculate VaR
- `POST /api/v1/stress-test` - Run stress test scenarios
- `GET /api/v1/stress-test/results/{test_id}` - Get stress test results

### Risk Limits
- `GET /api/v1/limits/{user_id}` - Get risk limits
- `PUT /api/v1/limits/{user_id}` - Update risk limits
- `GET /api/v1/limits/breaches` - Get limit breaches

## Configuration

Key configuration parameters in `app/config.py`:

```python
# Risk Parameters
VAR_CONFIDENCE_LEVEL = 0.99
VAR_TIME_HORIZON = 1  # days
MARGIN_BUFFER = 1.2
LIQUIDATION_THRESHOLD = 0.8

# Flink Configuration
FLINK_WINDOW_SIZE = 60  # seconds
FLINK_PARALLELISM = 4

# Ignite Configuration
IGNITE_RISK_CACHE = "risk_metrics"
IGNITE_CACHE_EXPIRY = 300  # seconds

# ML Models
RISK_MODEL_PATH = "/models/risk_predictor.pkl"
UPDATE_FREQUENCY = 3600  # seconds
```

## Risk Models

### Value at Risk (VaR)
- **Historical VaR**: Based on historical price movements
- **Parametric VaR**: Assumes normal distribution
- **Monte Carlo VaR**: Simulation-based approach

### Margin Calculation
- **Initial Margin**: Based on position size and volatility
- **Maintenance Margin**: Minimum required margin
- **Variation Margin**: Daily P&L adjustments

### Stress Testing
- **Market Crash**: -20% to -50% price movements
- **Volatility Spike**: 2x to 5x volatility increase
- **Liquidity Crisis**: Widened spreads and reduced depth
- **Custom Scenarios**: User-defined stress scenarios

## Integration

### Dependencies
- **Trading Core Service**: Position and trade data
- **Market Data Service**: Real-time price feeds
- **Auth Service**: User authentication
- **Apache Ignite**: Distributed state
- **Apache Flink**: Stream processing
- **Apache Pulsar**: Event streaming

### Event Streams
- **Position Updates**: Real-time position changes
- **Market Data**: Price and volatility updates
- **Risk Alerts**: Limit breaches and margin calls
- **Liquidation Events**: Forced closures

## Monitoring

Prometheus metrics exposed at `/metrics`:

- `risk_calculations_total`: Total risk calculations
- `risk_calculation_latency_seconds`: Calculation latency
- `margin_calls_total`: Number of margin calls
- `liquidations_total`: Number of liquidations
- `var_breaches_total`: VaR limit breaches
- `risk_limit_utilization`: Risk limit usage percentage

## Development

### Project Structure

```
risk-engine-service/
├── app/
│   ├── api/          # REST API endpoints
│   ├── core/         # Risk calculation engines
│   ├── models/       # Pydantic models
│   ├── ml/           # Machine learning models
│   ├── events/       # Flink event processing
│   ├── state/        # Ignite state management
│   ├── config.py     # Configuration
│   └── main.py       # FastAPI application
├── scripts/          # Utility scripts
├── tests/           # Unit and integration tests
└── requirements.in  # Python dependencies
```

### Testing

```bash
# Run unit tests
pytest tests/unit

# Run integration tests
pytest tests/integration

# Run risk model backtests
python scripts/backtest_risk_models.py
```

## Performance

- **Risk Calculation**: < 10ms per portfolio
- **Margin Updates**: < 5ms latency
- **VaR Computation**: < 100ms for 1000 positions
- **Stress Tests**: < 1 second per scenario
- **Event Processing**: < 50ms end-to-end

## Machine Learning

### Risk Prediction Models
- **XGBoost**: For non-linear risk patterns
- **LSTM**: For time-series risk forecasting
- **Ensemble**: Combined model for robustness

### Feature Engineering
- Price volatility and returns
- Volume and liquidity metrics
- Correlation matrices
- Market microstructure features

## Security

- **Authentication**: JWT-based via Auth Service
- **Authorization**: Role-based (risk managers, traders, admins)
- **Encryption**: TLS for all communications
- **Audit Trail**: All risk decisions logged
- **Data Privacy**: PII handling compliance

## Deployment

### Scaling Considerations
1. **Horizontal Scaling**: Add more service instances
2. **Flink Scaling**: Increase task parallelism
3. **Ignite Scaling**: Add cache nodes for capacity
4. **Model Serving**: Use model serving infrastructure

### High Availability
- Multi-instance deployment
- Ignite cluster with replication
- Flink checkpointing
- Circuit breakers for dependencies

## Alerts and Notifications

- **Margin Calls**: Automated notifications
- **Risk Breaches**: Real-time alerts
- **System Health**: Performance degradation alerts
- **Model Drift**: ML model performance monitoring

## Contributing

See the main PlatformQ [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines. 