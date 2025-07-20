# Unified Trading Platform Service

Comprehensive trading platform for the platformQ ecosystem with advanced event-driven architecture and risk network analysis.

## Overview

The Unified Trading Platform Service consolidates all trading-related functionality with:

- **Social Trading**: Follow successful traders, copy strategies, and share trading insights
- **Strategy NFTs**: Tokenized trading strategies with verifiable performance
- **Copy Trading**: Automated portfolio replication with risk management
- **Prediction Markets**: Binary, categorical, scalar, and conditional markets
- **Event-Driven Architecture**: Real-time event processing and risk propagation
- **Graph-Based Risk Analysis**: Network-based risk assessment and cascade simulation
- **Medallion Data Lake**: Bronze/Silver/Gold architecture for trading data
- **Real-time Analytics**: Performance tracking and market statistics

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Unified Trading Platform Service                │
├─────────────────────────────────────────────────────────────┤
│                    Shared Components                         │
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │   Order     │  Analytics   │  Reputation │   Event    │ │
│  │  Matching   │   Engine     │   System    │   Driven   │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
├─────────────────────────────────────────────────────────────┤
│     Social Trading          │       Prediction Markets       │
│  ┌──────────┬─────────┐    │    ┌──────────┬─────────────┐ │
│  │ Strategy │  Copy   │    │    │  Market  │   Oracle    │ │
│  │  Engine  │ Trading │    │    │  Engine  │  Resolver   │ │
│  └──────────┴─────────┘    │    └──────────┴─────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                  Event-Driven Integration                    │
│  ┌─────────────┬──────────────┬─────────────────────────┐  │
│  │   Event     │    Graph     │      Data Lake          │  │
│  │   Router    │ Intelligence │    (Medallion)          │  │
│  └─────────────┴──────────────┴─────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Features

### Event-Driven Trading
- **Real-time Event Publishing**: Trade executions, position updates, risk alerts
- **Event Enrichment**: Automatic enrichment with market data and risk metrics
- **Risk Propagation Analysis**: Network-based risk contagion modeling
- **Cascade Simulation**: What-if analysis for trader failures
- **Systemic Risk Detection**: Identify systemically important traders

### Social Trading
- **Strategy Creation**: Build and backtest trading strategies
- **Performance Tracking**: Real-time P&L, win rate, and risk metrics
- **Copy Trading**: Automatically replicate successful traders' portfolios
- **Social Feed**: Share insights, analysis, and trading ideas
- **Reputation System**: Trust scores based on historical performance
- **Strategy NFTs**: Mint and trade tokenized strategies

### Prediction Markets
- **Market Types**: Binary, categorical, scalar, and conditional
- **Liquidity Provision**: AMM pools for instant trading
- **Oracle Integration**: Multiple oracle sources for reliable resolution
- **Market Creation**: Permissionless market creation with DAO approval

### Risk Management
- **Network-Based Risk**: Graph analysis of trading relationships
- **Real-time Risk Updates**: Dynamic risk scoring as trades occur
- **Margin Requirements**: Trust-adjusted margin calculations
- **Position Limits**: Automatic enforcement based on risk profile
- **Risk Clustering**: Detection of correlated risk groups

## API Endpoints

### Trading Operations
- `POST /api/v1/trading/orders` - Submit new order
- `DELETE /api/v1/trading/orders/{order_id}` - Cancel order
- `GET /api/v1/trading/orders` - List user's orders
- `GET /api/v1/trading/markets/{market_id}/orderbook` - Get order book
- `WS /api/v1/trading/markets/{market_id}/stream` - Real-time market data

### Social Trading
- `POST /api/v1/social/strategies` - Create trading strategy
- `POST /api/v1/social/strategies/{strategy_id}/mint` - Mint strategy NFT
- `POST /api/v1/social/copy/{trader_id}` - Start copying trader
- `GET /api/v1/social/leaderboard` - Get top traders

### Prediction Markets
- `POST /api/v1/prediction/markets` - Create prediction market
- `POST /api/v1/prediction/markets/{market_id}/trade` - Trade in market
- `GET /api/v1/prediction/markets/{market_id}/odds` - Get current odds

## Event-Driven Integration

### Published Events
```python
# Trade Execution
{
    "event_type": "trade.executed",
    "trader_id": "trader123",
    "market_id": "BTC-USD",
    "data": {
        "trade_id": "T-123456",
        "price": "45000.00",
        "quantity": "0.5",
        "side": "BUY"
    }
}

# Risk Alert
{
    "event_type": "risk.alert",
    "trader_id": "trader123",
    "data": {
        "risk_level": "high",
        "risk_score": 0.85,
        "alert_type": "margin_call"
    }
}
```

### Integration Services
- **Event Router Service**: Routes and enriches trading events
- **Graph Intelligence Service**: Analyzes risk propagation networks
- **Data Platform Service**: Stores trading data in medallion architecture

## Configuration

### Environment Variables

```bash
# Infrastructure
IGNITE_HOST=ignite:10800
PULSAR_URL=pulsar://pulsar:6650
ELASTICSEARCH_URL=http://elasticsearch:9200
JANUSGRAPH_URL=ws://janusgraph:8182/gremlin

# Blockchain
ETHEREUM_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
POLYGON_RPC_URL=https://polygon-mainnet.g.alchemy.com/v2/YOUR_KEY

# Event Processing
EVENT_ROUTER_URL=http://event-router-service:8000
GRAPH_INTELLIGENCE_URL=http://graph-intelligence-service:8000
DATA_PLATFORM_URL=http://data-platform-service:8000

# Risk Configuration
RISK_PROPAGATION_ENABLED=true
SYSTEMIC_RISK_THRESHOLD=0.7
CASCADE_SIMULATION_DEPTH=5
```

## Usage Examples

### Execute Trade with Event Processing

```python
import httpx

# Submit order
order = {
    "market_id": "BTC-USD",
    "side": "BUY",
    "order_type": "LIMIT",
    "quantity": 0.5,
    "price": 45000
}

response = httpx.post(
    "http://trading-platform:8000/api/v1/trading/orders",
    json=order,
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)

# The service automatically:
# 1. Executes the order through matching engine
# 2. Publishes trade event to Event Router
# 3. Updates trader risk profile in Graph Intelligence
# 4. Ingests trade data to Data Platform
```

### Start Copy Trading with Risk Analysis

```python
# Copy a trader
copy_request = {
    "allocation_percent": 20,
    "max_position_size": 10000,
    "risk_limits": {
        "max_drawdown": 0.15,
        "stop_loss": 0.05
    }
}

response = httpx.post(
    "http://trading-platform:8000/api/v1/social/copy/top_trader_001",
    json=copy_request
)

# The service:
# 1. Creates copy trading relationship
# 2. Adds relationship to risk network graph
# 3. Analyzes systemic risk impact
# 4. Sets appropriate margin requirements
```

### Create Prediction Market

```python
market = {
    "market_type": "BINARY",
    "question": "Will BTC reach $50,000 by end of month?",
    "end_time": "2024-01-31T23:59:59Z",
    "liquidity": 10000,
    "oracle_config": {
        "primary": "chainlink",
        "fallback": "band_protocol"
    }
}

response = httpx.post(
    "http://trading-platform:8000/api/v1/prediction/markets",
    json=market
)
```

## Risk Network Integration

### Automatic Updates
- Trade executions update trader risk profiles
- Copy trading creates risk relationships
- Position changes trigger risk recalculation
- Liquidations simulate cascade effects

### Risk Metrics Tracked
- **Trader Level**: Risk score, exposure, leverage, margin usage
- **Network Level**: Systemic importance, cluster membership
- **System Level**: Total risk, cascade potential

## Monitoring

### Metrics
- Order matching latency
- Event publishing rate
- Risk calculation time
- Copy trading performance
- Market maker efficiency

### Alerts
- High systemic risk detected
- Large position concentration
- Cascade simulation shows impact
- Event processing failures

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Start dependencies
docker-compose up -d redis postgres pulsar janusgraph

# Run service
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Testing

```bash
# Run tests
pytest tests/

# Run integration tests
pytest tests/integration/

# Test event processing
python tests/simulate_trading_events.py
```

## Security

- **Authentication**: JWT tokens with role-based access
- **Risk Limits**: Automatic position and leverage limits
- **Event Validation**: All events validated before processing
- **Audit Trail**: Complete event history maintained 