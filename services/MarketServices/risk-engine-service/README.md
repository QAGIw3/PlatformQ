# Risk Engine Service

A comprehensive real-time risk assessment and management service that combines traditional risk calculations with advanced ML-based predictive capabilities for the PlatformQ derivatives trading platform.

## Overview

The Risk Engine Service is a critical component providing:

### Core Risk Management
- **Real-time Risk Calculations**: Position-level and portfolio-level risk metrics
- **Margin Management**: Dynamic margin requirements and real-time margin monitoring
- **Value at Risk (VaR)**: Historical simulation and parametric VaR calculations  
- **Stress Testing**: Comprehensive stress scenarios for extreme market conditions
- **Position Limits**: Configurable limits with real-time enforcement

### ML-Enhanced Capabilities
- **Volatility Prediction**: Random Forest models predict future volatility
- **Anomaly Detection**: Isolation Forest identifies unusual market behavior
- **Liquidation Probability**: ML models assess position-specific liquidation risk
- **Dynamic Risk Parameters**: Risk parameters adapt to market conditions
- **Stress Testing**: ML-powered stress scenarios (flash crash, black swan, etc.)

### Real-time Monitoring
- **Continuous Risk Monitoring**: Monitor trader portfolios in real-time
- **WebSocket Updates**: Real-time risk updates via WebSocket connections
- **Alert System**: Multi-level alerts for various risk scenarios
- **Margin Call Management**: Automated margin call generation and tracking

## Architecture

```
risk-engine-service/
├── app/
│   ├── api/              # REST API endpoints
│   │   ├── risk.py       # Risk calculation endpoints
│   │   ├── margin.py     # Margin management endpoints
│   │   ├── var.py        # VaR calculation endpoints
│   │   ├── stress.py     # Stress testing endpoints
│   │   ├── limits.py     # Position limit endpoints
│   │   ├── monitoring.py # Real-time monitoring endpoints
│   │   └── direct.py     # Direct communication endpoints
│   ├── core/             # Core business logic
│   │   ├── risk_calculator.py    # Risk calculations
│   │   ├── var_calculator.py     # VaR engine
│   │   ├── stress_tester.py      # Stress testing
│   │   ├── ml_risk_engine.py     # ML-based risk engine
│   │   └── risk_monitor.py       # Real-time monitoring
│   ├── models/           # Data models
│   │   ├── risk.py       # Risk models and structures
│   │   ├── margin.py     # Margin models
│   │   ├── var.py        # VaR models
│   │   └── stress.py     # Stress test models
│   ├── ml/               # Machine learning components
│   │   └── risk_prediction.py    # ML prediction models
│   ├── state/            # State management
│   │   └── state_manager.py      # Ignite-based state
│   ├── integrations/     # External integrations
│   │   ├── direct_comm_integration.py  # Direct communication
│   │   └── flink_integration.py        # Flink streaming
│   ├── config.py         # Service configuration
│   ├── dependencies.py   # Dependency injection
│   └── main.py          # FastAPI application
├── requirements.in       # Python dependencies
└── Dockerfile           # Container definition
```

## Features

### Risk Calculation Engine
- Portfolio-level risk aggregation
- Greeks calculation for options
- Correlation-based risk adjustments
- Real-time P&L tracking
- Cross-margining support

### Margin System
- Initial and maintenance margin calculations
- Cross-product margin optimization
- Real-time margin monitoring
- Margin call generation
- Collateral management

### VaR Engine
- Historical simulation VaR
- Parametric VaR
- Monte Carlo VaR
- Conditional VaR (CVaR)
- Backtesting framework

### Stress Testing
- Predefined stress scenarios
- Custom scenario builder
- Historical scenario replay
- Sensitivity analysis
- Portfolio stress results

### Position Limits
- Per-trader limits
- Per-product limits
- Concentration limits
- Exposure limits
- Real-time limit checking

### ML Risk Models

#### Volatility Predictor
- **Model**: Random Forest Regressor
- **Features**: Market metrics, technical indicators, microstructure data
- **Output**: Predicted volatility for next period

#### Anomaly Detector
- **Model**: Isolation Forest
- **Features**: Same as volatility predictor
- **Output**: Anomaly score (0-1, higher is more anomalous)

#### Liquidation Predictor
- **Model**: Random Forest Classifier
- **Features**: Position health, leverage, market conditions, user history
- **Output**: Probability of liquidation

## API Endpoints

### Risk Management
- `POST /api/v1/risk/calculate` - Calculate risk for positions
- `GET /api/v1/risk/portfolio/{user_id}` - Get portfolio risk
- `POST /api/v1/risk/batch` - Batch risk calculation

### Margin Management
- `GET /api/v1/margin/requirements` - Get margin requirements
- `GET /api/v1/margin/status/{user_id}` - Check margin status
- `POST /api/v1/margin/call` - Generate margin call

### VaR Calculations
- `POST /api/v1/var/calculate` - Calculate VaR
- `GET /api/v1/var/historical/{portfolio_id}` - Historical VaR
- `POST /api/v1/var/backtest` - Backtest VaR model

### Stress Testing
- `POST /api/v1/stress/test` - Run stress test
- `GET /api/v1/stress/scenarios` - List scenarios
- `POST /api/v1/stress/custom` - Custom stress test

### Position Limits
- `POST /api/v1/limits/set/{user_id}` - Set position limits
- `GET /api/v1/limits/check/{user_id}` - Check limit compliance
- `GET /api/v1/limits/usage/{user_id}` - Get limit usage

### Real-time Monitoring
- `POST /api/v1/monitoring/users/{user_id}/start` - Start monitoring
- `POST /api/v1/monitoring/users/{user_id}/stop` - Stop monitoring
- `GET /api/v1/monitoring/users/{user_id}/check` - Check user risk
- `GET /api/v1/monitoring/status` - Overall monitoring status
- `POST /api/v1/monitoring/market/{market_id}/assess` - ML market assessment
- `POST /api/v1/monitoring/position/{position_id}/assess` - ML position assessment
- `WS /api/v1/monitoring/ws/{user_id}` - WebSocket for real-time updates

### Direct Communication
- `POST /api/v1/direct/risk-check` - Ultra-low latency risk check
- `POST /api/v1/direct/margin-check` - Direct margin verification

## Configuration

Key configuration parameters:

```python
# Risk calculation settings
RISK_CALCULATION_INTERVAL_SECONDS = 5
VAR_CONFIDENCE_LEVEL = 0.95

# Default risk limits
DEFAULT_MAX_LEVERAGE = 20
DEFAULT_MIN_MARGIN_LEVEL = 120  # 120%
DEFAULT_CONCENTRATION_LIMIT = 30  # 30%

# Margin thresholds
LIQUIDATION_THRESHOLD = 100  # 100%
MARGIN_CALL_THRESHOLD = 130  # 130%
WARNING_THRESHOLD = 150  # 150%

# ML settings
ML_MODEL_UPDATE_INTERVAL = 3600  # 1 hour
ML_PREDICTION_CACHE_TTL = 30  # 30 seconds
```

## Integration

### Dependencies
- **Apache Ignite**: Distributed state and caching
- **Apache Pulsar**: Event streaming for risk events
- **Apache Flink**: Real-time risk analytics
- **Cassandra**: Historical data storage
- **Elasticsearch**: Risk metrics search and analytics
- **MLflow**: ML model management

### Event Publishing
The service publishes various events:
- Risk limit breaches
- Margin calls
- Liquidation warnings
- Market anomalies
- Position updates

## Performance

### Latency Targets
- Risk calculation: < 10ms
- Margin check: < 5ms  
- VaR calculation: < 100ms
- Stress test: < 500ms
- Direct communication: < 1ms
- ML predictions: < 50ms (cached: < 1ms)

### Scalability
- Horizontal scaling via multiple instances
- Ignite distributed cache for shared state
- Flink for distributed risk processing
- Supports 100,000+ concurrent positions
- 10,000+ monitored users

## Running the Service

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run the service
uvicorn app.main:app --reload --port 8021
```

### Docker
```bash
# Build image
docker build -t risk-engine-service .

# Run container
docker run -p 8021:8021 risk-engine-service
```

### Environment Variables
```bash
RISK_ENGINE_SERVICE_PORT=8021
RISK_ENGINE_PULSAR_URL=pulsar://pulsar:6650
RISK_ENGINE_IGNITE_ADDRESSES=["ignite:10800"]
RISK_ENGINE_CASSANDRA_HOSTS=["cassandra"]
RISK_ENGINE_ELASTICSEARCH_URL=http://elasticsearch:9200
```

## Monitoring

### Prometheus Metrics
- `risk_calculations_total` - Total risk calculations
- `var_calculations_total` - VaR calculations by method
- `margin_calls_total` - Margin calls generated
- `stress_tests_total` - Stress tests performed
- `position_limit_breaches_total` - Limit violations
- `monitored_users_total` - Users being monitored
- `ml_predictions_total` - ML predictions made
- `risk_alerts_total` - Risk alerts by severity

### Health Checks
- `GET /health` - Service health status
- `GET /ready` - Readiness check
- `GET /metrics` - Prometheus metrics

## Security

- JWT-based authentication
- Role-based access control
- Service-to-service auth for direct communication
- Vault integration for secrets
- TLS for all external communication

## Future Enhancements

1. **Deep Learning Models**: LSTM/Transformer for time series
2. **Reinforcement Learning**: Adaptive risk limits
3. **Graph Neural Networks**: Correlation and contagion risk
4. **Real-time Model Training**: Online learning
5. **Explainable AI**: Model interpretability
6. **Risk Dashboard**: Real-time visualization
7. **Advanced Scenarios**: More sophisticated stress tests 