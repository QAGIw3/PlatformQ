# Unified Trading Platform Service

Comprehensive trading platform for the platformQ ecosystem with advanced event-driven architecture, social trading, and risk network analysis. This service consolidates all trading-related functionality including the former social-trading-service.

## Overview

The Unified Trading Platform Service consolidates all trading-related functionality with:

- **Social Trading**: Follow successful traders, copy strategies, and share trading insights
- **Trader Profiles**: Create and manage trader profiles with social features
- **Copy Trading**: Automated portfolio replication with multiple copy modes and risk management
- **Social Feed**: Share posts, follow traders, and engage with the community
- **Reputation System**: Trust scores based on performance, social engagement, and reliability
- **Strategy NFTs**: Tokenized trading strategies with verifiable performance
- **Automated Trading**: Prediction market-based signal trading with multiple strategies
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
│                    Core Components                           │
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │   Order     │  Analytics   │  Reputation │   Event    │ │
│  │  Matching   │   Engine     │   System    │   Driven   │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                   Social Trading Module                      │
│  ┌──────────┬─────────┬──────────┬────────┬─────────────┐ │
│  │ Profiles │  Copy   │  Social  │ Leader │ Automated   │ │
│  │         │ Trading │   Feed   │ Board  │  Strategies │ │
│  └──────────┴─────────┴──────────┴────────┴─────────────┘ │
├─────────────────────────────────────────────────────────────┤
│     Prediction Markets      │      Strategy Markets         │
│  ┌──────────┬───────────┐  │  ┌────────────┬────────────┐ │
│  │  Market  │  Oracle   │  │  │   Market    │  Hedging  │ │
│  │  Engine  │ Resolver  │  │  │ Competition │  Markets  │ │
│  └──────────┴───────────┘  │  └────────────┴────────────┘ │
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
- **Trader Profiles**: Create profiles with trading stats, social features, and verification
- **Copy Trading Modes**:
  - Proportional: Copy trades proportional to leader's position
  - Fixed Amount: Fixed amount per trade
  - Percentage: Percentage of follower's portfolio
- **Social Following**: Follow traders and build a personalized feed
- **Social Posts**: Share trading insights, analysis, and educational content
- **Reputation System**: Multi-factor trust scores including:
  - Performance metrics (40% weight)
  - Social engagement (30% weight)
  - Trust and reliability (30% weight)
- **Leaderboards**: Daily, weekly, monthly, and all-time trader rankings

### Copy Trading Risk Management
- **Position Limits**: Maximum position size per trade
- **Daily Trade Limits**: Control number of trades per day
- **Stop Loss**: Automatic stop loss percentage
- **Drawdown Limits**: Maximum drawdown protection
- **Slippage Control**: Maximum allowed slippage for copy trades

### Automated Trading Strategies
- **Momentum**: Follow strong market sentiment
- **Contrarian**: Trade against extreme sentiment
- **Mean Reversion**: Bet on return to average
- **Breakout**: Trade on sentiment breakouts
- **Hedged**: Always maintain hedged positions

### Prediction Markets
- **Market Types**: Binary, categorical, scalar, and conditional
- **Liquidity Provision**: AMM pools for instant trading
- **Oracle Integration**: Multiple oracle sources for reliable resolution
- **Market Creation**: Permissionless market creation with DAO approval

### Strategy Markets
- **Strategy Performance Markets**: Bet on strategy performance metrics
- **Competition Markets**: Multi-strategy competition betting
- **Risk Hedging**: Create hedging markets for strategy risks

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
- `GET /api/v1/trading/markets/{market_id}/trades` - Get recent trades
- `POST /api/v1/trading/batch-orders` - Submit multiple orders
- `WS /api/v1/trading/markets/{market_id}/stream` - Real-time market data

### Social Trading - Profiles
- `GET /api/v1/social/profile/{user_id}` - Get trader profile
- `POST /api/v1/social/profile` - Create/update trader profile
- `POST /api/v1/social/follow/{leader_id}` - Follow a trader
- `DELETE /api/v1/social/follow/{leader_id}` - Unfollow a trader

### Social Trading - Copy Trading
- `POST /api/v1/social/copy/{leader_id}` - Start copying a trader
- `DELETE /api/v1/social/copy/{relation_id}` - Stop copying
- `GET /api/v1/social/copy/active` - Get active copy relations

### Social Trading - Social Features
- `POST /api/v1/social/posts` - Create social post
- `GET /api/v1/social/feed` - Get personalized feed
- `GET /api/v1/social/leaderboard` - Get trader leaderboard

### Automated Trading
- `POST /api/v1/social/automated/strategies` - Create automated strategy
- `GET /api/v1/social/automated/strategies` - List strategies
- `PUT /api/v1/social/automated/strategies/{strategy_id}` - Update strategy
- `DELETE /api/v1/social/automated/strategies/{strategy_id}` - Delete strategy
- `GET /api/v1/social/automated/signals/recent` - Get recent signals
- `GET /api/v1/social/automated/performance/summary` - Get performance summary
- `POST /api/v1/social/automated/backtest` - Backtest strategy

### Strategy Markets
- `POST /api/v1/social/strategy-markets/create` - Create strategy market
- `POST /api/v1/social/strategy-markets/competition` - Create competition
- `POST /api/v1/social/strategy-markets/hedge` - Create hedging markets
- `GET /api/v1/social/strategy-markets/sentiment/{strategy_id}` - Get sentiment
- `GET /api/v1/social/strategy-markets/opportunities` - Find opportunities

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
IGNITE_PORT=10800
PULSAR_URL=pulsar://pulsar:6650
ELASTICSEARCH_URL=http://elasticsearch:9200
JANUSGRAPH_URL=ws://janusgraph:8182/gremlin
CASSANDRA_HOSTS=cassandra:9042
CASSANDRA_KEYSPACE=social_trading

# Service URLs
ORDER_MATCHING_SERVICE_URL=http://trading-core-service:8000
RISK_SERVICE_URL=http://risk-service:8004

# Social Trading Configuration
MAX_COPY_ALLOCATION=0.5  # Maximum 50% portfolio allocation for copy trading
MIN_LEADER_TRACK_RECORD=90  # Days required before allowing copy trading
MAX_FOLLOWERS_PER_LEADER=1000
COPY_TRADE_SLIPPAGE=0.01  # 1% max slippage

# Reputation System
REPUTATION_UPDATE_INTERVAL=3600  # Update reputation scores every hour
REPUTATION_DECAY_RATE=0.95  # Monthly decay rate for inactive traders
MIN_TRADES_FOR_REPUTATION=10

# Social Features
MAX_POSTS_PER_DAY=50
MAX_FOLLOW_COUNT=1000
TRENDING_CALCULATION_INTERVAL=300  # Calculate trending topics every 5 minutes

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

### Create Trader Profile

```python
# Create or update trader profile
profile_request = {
    "username": "crypto_trader_pro",
    "display_name": "Professional Crypto Trader",
    "bio": "10+ years experience in crypto markets",
    "allows_copy_trading": True,
    "copy_trading_fee": 0.02,  # 2% fee
    "min_copy_amount": 1000
}

response = httpx.post(
    "http://trading-platform:8000/api/v1/social/profile",
    json=profile_request,
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)
```

### Start Copy Trading with Risk Analysis

```python
# Copy a trader with specific mode and risk limits
copy_request = {
    "copy_mode": "PERCENTAGE",  # PROPORTIONAL, FIXED_AMOUNT, or PERCENTAGE
    "allocation_percent": 20,  # 20% of portfolio
    "max_position_size": 10000,
    "max_daily_trades": 10,
    "stop_loss_percent": 5,  # 5% stop loss
    "max_drawdown_percent": 15  # 15% max drawdown
}

response = httpx.post(
    "http://trading-platform:8000/api/v1/social/copy/top_trader_001",
    json=copy_request,
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)

# The service:
# 1. Creates copy trading relationship
# 2. Adds relationship to risk network graph
# 3. Analyzes systemic risk impact
# 4. Automatically replicates leader's trades
# 5. Sets appropriate margin requirements
```

### Create Social Post

```python
# Share trading insight
post_request = {
    "content": "BTC showing strong support at $45k. Good entry point for long-term holders.",
    "tags": ["bitcoin", "technical-analysis", "support-levels"],
    "assets_mentioned": ["BTC"],
    "is_educational": True
}

response = httpx.post(
    "http://trading-platform:8000/api/v1/social/posts",
    json=post_request,
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)
```

### Create Automated Trading Strategy

```python
# Create momentum-based automated strategy
strategy_request = {
    "name": "BTC Momentum Trader",
    "strategy_type": "MOMENTUM",
    "assets": ["BTC", "ETH"],
    "min_confidence": 0.75,  # 75% confidence threshold
    "max_position_size": 50000,
    "risk_per_trade": 0.02,  # 2% risk per trade
    "sentiment_threshold": 0.8,
    "volume_threshold": 1000000,
    "max_daily_trades": 5,
    "max_open_positions": 3,
    "correlation_limit": 0.7
}

response = httpx.post(
    "http://trading-platform:8000/api/v1/social/automated/strategies",
    json=strategy_request,
    headers={"Authorization": "Bearer YOUR_TOKEN"}
)
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
docker-compose up -d ignite cassandra pulsar janusgraph elasticsearch

# Set environment variables
export IGNITE_HOST=localhost
export PULSAR_URL=pulsar://localhost:6650
export ORDER_MATCHING_SERVICE_URL=http://localhost:8003
export RISK_SERVICE_URL=http://localhost:8004

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
- **Copy Trading Security**: 
  - Leader verification required
  - Maximum allocation limits enforced
  - Automatic stop-loss protection
  - Daily trade limits
- **Social Features Security**:
  - Content moderation for posts
  - Rate limiting on all endpoints
  - Profile verification system
  - Anti-spam measures

## Architecture Notes

### Social Trading Components

The social trading functionality is organized into the following modules:

- **models.py**: Core data models for social trading (profiles, strategies, copy relations)
- **copy/**: Copy trading execution engine
- **reputation/**: Reputation calculation and management
- **api/**: REST API endpoints for all social features

### Integration Points

- **Trading Core Service**: For order execution and market data
- **Risk Service**: For portfolio valuation and risk metrics
- **Event Router**: For publishing trading events
- **Graph Intelligence**: For risk network analysis
- **Apache Ignite**: For caching trader profiles and real-time data
- **Apache Pulsar**: For event streaming and real-time updates
- **Cassandra**: For storing social posts and historical data
- **JanusGraph**: For social graph relationships 