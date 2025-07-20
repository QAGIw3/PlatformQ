# Risk Management Service

A comprehensive real-time risk monitoring and management service with advanced ML-based predictive capabilities for the PlatformQ derivatives trading platform.

## Overview

The Risk Management Service combines traditional risk management techniques with cutting-edge machine learning models to provide:

- **Real-time Risk Monitoring**: Continuous monitoring of trader portfolios and positions
- **ML-based Risk Predictions**: Advanced predictive models for volatility, liquidation probability, and market anomalies
- **Dynamic Risk Parameters**: Adaptive margin requirements based on market conditions
- **Comprehensive Alerts**: Multi-level alert system for various risk scenarios
- **Stress Testing**: Position-level stress testing for extreme market scenarios

## Features

### Core Risk Management
- Real-time margin monitoring and calculations
- Position limit enforcement
- Concentration risk management
- Value at Risk (VaR) calculations
- Margin call and liquidation management

### ML-Enhanced Capabilities
- **Volatility Prediction**: Random Forest models predict future volatility
- **Anomaly Detection**: Isolation Forest identifies unusual market behavior
- **Liquidation Probability**: ML models assess position-specific liquidation risk
- **Dynamic Risk Adjustment**: Risk parameters adapt to market conditions
- **Stress Testing**: ML-powered stress scenarios (flash crash, black swan, etc.)

## Architecture

```
risk-management-service/
├── app/
│   ├── api/              # REST API endpoints
│   │   └── risk.py       # Risk management endpoints
│   ├── core/             # Core business logic
│   │   ├── risk_monitor.py      # Real-time monitoring engine
│   │   └── ml_risk_engine.py    # ML-based risk engine
│   ├── models/           # Data models
│   │   ├── risk_state.py        # Risk state models
│   │   └── risk_models.py       # ML risk models
│   ├── integrations/     # External service clients
│   ├── config.py         # Service configuration
│   ├── dependencies.py   # Dependency injection
│   └── main.py          # FastAPI application
├── requirements.in       # Python dependencies
└── Dockerfile           # Container definition
```

## API Endpoints

### Risk Monitoring
- `POST /api/v1/risk/limits/{trader_id}` - Set risk limits for a trader
- `GET /api/v1/risk/check/{trader_id}` - Check current risk status
- `GET /api/v1/risk/portfolio/{trader_id}` - Get portfolio risk analysis
- `GET /api/v1/risk/alerts` - Get risk alerts

### ML-based Assessment
- `POST /api/v1/risk/market/assess` - Get ML-based market risk assessment
- `POST /api/v1/risk/position/assess` - Get ML-based position risk assessment

### Monitoring Control
- `POST /api/v1/risk/monitoring/start/{trader_id}` - Start monitoring
- `POST /api/v1/risk/monitoring/stop/{trader_id}` - Stop monitoring
- `GET /api/v1/risk/monitoring/status` - Get monitoring status

## Configuration

Key configuration parameters in `app/config.py`:

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
```

## ML Models

### Volatility Predictor
- **Model**: Random Forest Regressor
- **Features**: Market metrics, technical indicators, microstructure data
- **Output**: Predicted volatility for next period

### Anomaly Detector
- **Model**: Isolation Forest
- **Features**: Same as volatility predictor
- **Output**: Anomaly score (0-1, higher is more anomalous)

### Liquidation Predictor
- **Model**: Random Forest Classifier
- **Features**: Position health, leverage, market conditions, user history
- **Output**: Probability of liquidation

## Integration

### Dependencies
- **Apache Ignite**: Distributed caching for ML models and market data
- **Apache Pulsar**: Event streaming for risk alerts and margin calls
- **Cassandra**: Historical data storage
- **Market Data Service**: Real-time price feeds
- **Position Service**: Position and portfolio data

### Event Publishing
The service publishes various risk events to Pulsar topics:
- Risk limit breaches
- Margin calls
- Liquidation warnings
- Market anomalies

## Running the Service

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Run the service
uvicorn app.main:app --reload --port 8082
```

### Docker
```bash
# Build image
docker build -t risk-management-service .

# Run container
docker run -p 8082:8082 risk-management-service
```

### Environment Variables
```bash
RMS_SERVICE_PORT=8082
RMS_PULSAR_URL=pulsar://pulsar:6650
RMS_IGNITE_ADDRESSES=["ignite:10800"]
RMS_CASSANDRA_HOSTS=["cassandra"]
```

## Monitoring

### Prometheus Metrics
- `rms_requests_total` - Total API requests
- `rms_request_duration_seconds` - Request latency
- `rms_monitored_traders` - Number of monitored traders
- `rms_margin_calls_total` - Total margin calls
- `rms_liquidations_total` - Total liquidations
- `rms_risk_alerts_total` - Risk alerts by severity

### Health Check
- `GET /health` - Service health status
- `GET /metrics` - Prometheus metrics endpoint

## Security

- JWT-based authentication via API Gateway
- Role-based access control (trader vs admin)
- Secure communication with HashiCorp Vault for secrets
- TLS encryption for all external communications

## Performance

- Supports monitoring 10,000+ concurrent traders
- Sub-second risk calculations
- ML predictions cached for 30 seconds
- Horizontal scaling via multiple instances
- Ignite distributed cache for shared state

## Future Enhancements

1. **Deep Learning Models**: LSTM/Transformer models for time series prediction
2. **Reinforcement Learning**: Adaptive risk limits based on market regime
3. **Graph Neural Networks**: Correlation and contagion risk modeling
4. **Real-time Model Training**: Online learning for adapting to market changes
5. **Explainable AI**: Model interpretability for risk decisions 