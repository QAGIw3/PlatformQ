# Unified Trading Platform Service

Comprehensive trading platform for the platformQ ecosystem, combining social trading, copy trading, prediction markets, and advanced market mechanisms into a single, powerful service.

## Overview

The Unified Trading Platform Service consolidates all trading-related functionality, providing:

- **Social Trading**: Follow successful traders, copy strategies, and share trading insights
- **Strategy NFTs**: Tokenized trading strategies with verifiable performance
- **Copy Trading**: Automated portfolio replication with risk management
- **Prediction Markets**: Binary, categorical, scalar, and conditional markets for real-world events
- **Unified Order Matching**: High-performance order matching engine for all market types
- **AMM Liquidity**: Automated market makers for prediction markets
- **Oracle Resolution**: Decentralized oracle system for market outcomes
- **Real-time Analytics**: Performance tracking and market statistics

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│              Unified Trading Platform Service                │
├─────────────────────────────────────────────────────────────┤
│                    Shared Components                         │
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │   Order     │  Analytics   │  Reputation │   Real-    │ │
│  │  Matching   │   Engine     │   System    │   time     │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
├─────────────────────────────────────────────────────────────┤
│     Social Trading          │       Prediction Markets       │
│  ┌──────────┬─────────┐    │    ┌──────────┬─────────────┐ │
│  │ Strategy │  Copy   │    │    │  Market  │   Oracle    │ │
│  │  Engine  │ Trading │    │    │  Engine  │  Resolver   │ │
│  └──────────┴─────────┘    │    └──────────┴─────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Features

### Social Trading
- **Strategy Creation**: Build and backtest trading strategies
- **Performance Tracking**: Real-time P&L, win rate, and risk metrics
- **Copy Trading**: Automatically replicate successful traders' portfolios
- **Social Feed**: Share insights, analysis, and trading ideas
- **Reputation System**: Trust scores based on historical performance
- **Strategy NFTs**: Mint and trade tokenized strategies
- **Trader DAO**: Decentralized governance for platform features

### Prediction Markets
- **Market Types**:
  - Binary: Yes/No outcomes
  - Categorical: Multiple discrete outcomes
  - Scalar: Range-based outcomes
  - Conditional: Markets dependent on other outcomes
- **Liquidity Provision**: AMM pools for instant trading
- **Oracle Integration**: Multiple oracle sources for reliable resolution
- **Market Creation**: Permissionless market creation with DAO approval
- **Dynamic Pricing**: Real-time pricing based on supply and demand

### Unified Trading
- **Order Types**: Market, limit, stop, and stop-limit orders
- **Order Matching**: High-performance matching engine
- **Real-time Data**: WebSocket streams for live updates
- **Market Statistics**: Volume, price history, and depth charts
- **Risk Management**: Position limits and margin requirements

## API Endpoints

### Unified Trading
- `POST /api/v1/trading/orders` - Submit new order
- `DELETE /api/v1/trading/orders/{order_id}` - Cancel order
- `GET /api/v1/trading/orders` - List user's orders
- `GET /api/v1/trading/markets/{market_id}/orderbook` - Get order book
- `GET /api/v1/trading/markets/{market_id}/trades` - Get recent trades
- `GET /api/v1/trading/markets/{market_id}/stats` - Get market statistics
- `WS /api/v1/trading/markets/{market_id}/stream` - Real-time market data

### Social Trading
- `POST /api/v1/social/strategies` - Create trading strategy
- `GET /api/v1/social/strategies/{strategy_id}` - Get strategy details
- `POST /api/v1/social/strategies/{strategy_id}/mint` - Mint strategy NFT
- `POST /api/v1/social/copy/{trader_id}` - Start copying trader
- `GET /api/v1/social/leaderboard` - Get top traders
- `GET /api/v1/social/reputation/{trader_id}` - Get trader reputation

### Prediction Markets
- `POST /api/v1/prediction/markets` - Create prediction market
- `GET /api/v1/prediction/markets` - List active markets
- `POST /api/v1/prediction/markets/{market_id}/trade` - Trade in market
- `POST /api/v1/prediction/markets/{market_id}/provide-liquidity` - Add liquidity
- `POST /api/v1/prediction/markets/{market_id}/resolve` - Submit resolution
- `GET /api/v1/prediction/markets/{market_id}/odds` - Get current odds

## Configuration

### Environment Variables

```bash
# Infrastructure
IGNITE_HOST=ignite:10800
PULSAR_URL=pulsar://pulsar:6650
ELASTICSEARCH_URL=http://elasticsearch:9200
JANUSGRAPH_URL=http://janusgraph:8182

# Blockchain
BLOCKCHAIN_GATEWAY_URL=http://blockchain-gateway-service:8000
WEB3_PROVIDER_URL=https://eth-mainnet.example.com

# Services
DERIVATIVES_ENGINE_URL=http://derivatives-engine-service:8000
GRAPH_INTELLIGENCE_URL=http://graph-intelligence-service:8000
NEUROMORPHIC_SERVICE_URL=http://neuromorphic-service:8000

# Trading Configuration
MAX_ORDER_SIZE=1000000
MIN_ORDER_SIZE=0.001
MAKER_FEE=0.001
TAKER_FEE=0.002

# Service Configuration
SERVICE_NAME=trading-platform-service
LOG_LEVEL=INFO
```

## Usage Examples

### Submit Trading Order
```python
import httpx

async with httpx.AsyncClient() as client:
    response = await client.post(
        "http://trading-platform:8000/api/v1/trading/orders",
        json={
            "market_id": "ETH-USD",
            "side": "buy",
            "order_type": "limit",
            "quantity": 1.5,
            "price": 2000.00
        }
    )
    order_id = response.json()["order_id"]
```

### Create Prediction Market
```python
response = await client.post(
    "http://trading-platform:8000/api/v1/prediction/markets",
    json={
        "question": "Will ETH reach $5000 by end of 2024?",
        "market_type": "binary",
        "resolution_source": "coingecko",
        "end_date": "2024-12-31T23:59:59Z",
        "initial_liquidity": 10000
    }
)
market_id = response.json()["market_id"]
```

### Start Copy Trading
```python
response = await client.post(
    "http://trading-platform:8000/api/v1/social/copy/trader123",
    json={
        "allocation": 5000,
        "max_position_size": 0.2,
        "stop_loss": 0.1
    }
)
```

## Development

### Running Locally
```bash
cd services/trading-platform-service
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t platformq/trading-platform-service .
```

## Monitoring

The service exposes Prometheus metrics at `/metrics`:

- `trading_orders_total` - Total orders by type and status
- `trading_volume_total` - Total trading volume by market
- `copy_trading_active` - Number of active copy trading relationships
- `prediction_markets_active` - Number of active prediction markets
- `matching_engine_latency` - Order matching latency histogram

## Security Considerations

- All trading operations require authentication
- Position limits enforced per user and market
- Anti-manipulation measures for prediction markets
- Slippage protection for copy trading
- Rate limiting on all endpoints
- Audit logs for all trades

## Migration from Legacy Services

This service replaces:
- `social-trading-service` - All social trading features integrated
- `prediction-markets-service` - All prediction market functionality integrated

Update service references:
- `http://social-trading-service:8000` → `http://trading-platform-service:8000/api/v1/social`
- `http://prediction-markets-service:8000` → `http://trading-platform-service:8000/api/v1/prediction` 