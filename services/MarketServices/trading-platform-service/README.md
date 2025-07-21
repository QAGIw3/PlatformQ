# Trading Platform Service

## Overview

The Trading Platform Service provides comprehensive social trading features for the PlatformQ ecosystem, including copy trading, automated strategies, reputation systems, and strategy markets. It integrates seamlessly with the Trading Core Service to deliver a complete trading experience.

**New in v2.0:** Ultra-fast copy trading through direct binary communication with Trading Core Service, achieving sub-millisecond latency and 20-100x performance improvements.

## Architecture

### Key Components

1. **Copy Trading Executor**: High-performance copy trading engine with batch processing
2. **Reputation Engine**: Real-time trader reputation calculation and tracking
3. **Strategy Engine**: Automated trading strategy execution
4. **Event Integration**: Event-driven trading with risk propagation analysis
5. **Social Features**: Trading feed, posts, and follower management
6. **Strategy Markets**: Prediction markets for trading strategies

### Technology Stack

- **FastAPI**: High-performance REST API framework
- **Apache Ignite**: Shared distributed state with Trading Core
- **Apache Pulsar**: Event streaming and messaging
- **Direct Communication**: Binary protocol for ultra-low latency
- **WebSocket**: Real-time updates and notifications
- **Vault/Consul**: Secure configuration and service discovery
- **msgpack**: Fast binary serialization
- **uvloop**: High-performance event loop

## Features

### Copy Trading (Enhanced in v2.0)
- **Sub-millisecond Execution**: Direct communication with Trading Core
- **Batch Processing**: Process 100+ follower orders in parallel
- **Smart Allocation**: Fixed amount, percentage, or proportional copying
- **Risk Controls**: Per-follower limits and drawdown protection
- **Real-time Monitoring**: Live P&L and performance tracking

### Social Trading
- **Trader Profiles**: Comprehensive profiles with performance metrics
- **Reputation System**: Multi-factor reputation scoring
- **Social Feed**: Share strategies and market insights
- **Follow System**: Follow traders and get notifications
- **Leaderboards**: Daily, weekly, monthly rankings

### Automated Trading
- **Strategy Types**: Momentum, Contrarian, Mean Reversion, Breakout, Hedged
- **ML Enhancement**: Optional ML-based signal enhancement
- **Risk Management**: Built-in position limits and stop losses
- **Backtesting**: Historical performance analysis
- **Real-time Signals**: Live strategy recommendations

### Strategy Markets
- **Prediction Markets**: Bet on strategy performance
- **Competition Markets**: Multi-strategy competitions
- **Hedging Markets**: Hedge strategy risks
- **Sentiment Analysis**: Real-time strategy sentiment

## Performance Metrics (v2.0)

| Operation | HTTP (v1.0) | Direct (v2.0) | Improvement |
|-----------|-------------|----------------|-------------|
| Copy Trade Execution | 50-100ms | <1ms | **50-100x** |
| Batch Copy (100 followers) | 1000-2000ms | 10-50ms | **20-100x** |
| Risk Check | 10-20ms | <0.1ms | **100-200x** |
| Profile Update | 5-10ms | <0.5ms | **10-20x** |

## API Endpoints

### Trading
- `POST /api/v1/trading/orders` - Submit order through unified API
- `DELETE /api/v1/trading/orders/{order_id}` - Cancel order
- `GET /api/v1/trading/orders` - List orders
- `GET /api/v1/trading/markets/{market_id}/orderbook` - Get orderbook
- `GET /api/v1/trading/markets/{market_id}/trades` - Get recent trades
- `GET /api/v1/trading/metrics` - Get trading metrics
- `WS /api/v1/trading/markets/{market_id}/stream` - Real-time market stream

### Social Trading
- `GET /api/v1/social/profile/{user_id}` - Get trader profile
- `POST /api/v1/social/profile` - Create/update profile
- `POST /api/v1/social/follow/{leader_id}` - Follow trader
- `DELETE /api/v1/social/follow/{leader_id}` - Unfollow trader
- `POST /api/v1/social/copy/{leader_id}` - Start copy trading
- `DELETE /api/v1/social/copy/{relation_id}` - Stop copy trading
- `GET /api/v1/social/copy/active` - Get active copy relations
- `POST /api/v1/social/posts` - Create post
- `GET /api/v1/social/feed` - Get social feed
- `GET /api/v1/social/leaderboard` - Get trader leaderboard

### Automated Trading
- `POST /api/v1/automated/strategies` - Create automated strategy
- `GET /api/v1/automated/strategies` - List strategies
- `GET /api/v1/automated/strategies/{strategy_id}` - Get strategy details
- `PUT /api/v1/automated/strategies/{strategy_id}` - Update strategy
- `DELETE /api/v1/automated/strategies/{strategy_id}` - Delete strategy
- `GET /api/v1/automated/signals/recent` - Get recent signals
- `GET /api/v1/automated/performance/summary` - Get performance summary
- `POST /api/v1/automated/backtest` - Run backtest

### Strategy Markets
- `POST /api/v1/strategy-markets/create` - Create strategy market
- `POST /api/v1/strategy-markets/competition` - Create competition
- `POST /api/v1/strategy-markets/hedge` - Create hedging market
- `GET /api/v1/strategy-markets/sentiment/{strategy_id}` - Get sentiment
- `GET /api/v1/strategy-markets/markets/{strategy_id}` - List markets
- `GET /api/v1/strategy-markets/opportunities` - Get opportunities

### WebSocket
- `WS /ws` - Main WebSocket endpoint for real-time updates

## Configuration

Key configuration parameters:

```python
# Direct Communication (v2.0)
ENABLE_DIRECT_COMM = True
COPY_TRADE_BATCH_SIZE = 100
COPY_TRADE_BATCH_WINDOW_MS = 10

# Apache Ignite (Shared with Trading Core)
IGNITE_HOST = "ignite-unified"
IGNITE_PORT = 10800

# External Services
TRADING_CORE_URL = "http://trading-core:8020"  # Fallback
RISK_SERVICE_URL = "http://risk-engine:8021"

# Copy Trading
MAX_COPY_ALLOCATION = 0.5  # 50% max allocation

# Reputation System
REPUTATION_UPDATE_INTERVAL = 3600  # 1 hour
REPUTATION_DECAY_RATE = 0.95
MIN_TRADES_FOR_REPUTATION = 10

# Social Features
MAX_POSTS_PER_DAY = 50
```

## Running the Service

### Local Development

```bash
cd services/MarketServices/trading-platform-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8030
```

### Docker

```bash
docker build -t trading-platform-service .
docker run -p 8030:8030 trading-platform-service
```

### Docker Compose (Unified Architecture)

```bash
docker-compose -f docker-compose.trading-unified.yml up trading-platform
```

## Integration

### Dependencies
- **Apache Ignite**: Shared state with Trading Core Service
- **Apache Pulsar**: Event streaming
- **Trading Core Service**: Direct binary communication
- **HashiCorp Vault**: Secrets management
- **HashiCorp Consul**: Service discovery

### Direct Communication (v2.0)

The service uses the `platformq-direct-comm` library for ultra-low latency communication:

```python
from platformq_direct_comm import DirectCommunicator, MessageType

# Initialize
communicator = DirectCommunicator("trading-platform", ignite_client)
await communicator.start()

# Send copy trade batch
await communicator.send_direct(
    target_service="trading-core",
    msg_type=MessageType.COPY_TRADE,
    data={
        "leader_trade": {...},
        "follower_orders": [...]
    },
    wait_response=False
)
```

## Copy Trading Flow (v2.0)

```
1. Leader executes trade in Trading Core
2. Trading Core notifies Platform Service via direct message
3. FastCopyExecutor receives notification (<0.1ms)
4. Checks cached follower relations (in-memory)
5. Calculates follower sizes in parallel
6. Batches follower orders (up to 100)
7. Sends batch to Trading Core (<1ms)
8. Trading Core executes all orders in parallel
9. Positions updated, notifications sent
Total: <10ms for 100 followers
```

## Monitoring

The service exposes Prometheus metrics at `/metrics`:

- `copy_trades_total`: Total copy trades executed
- `copy_trade_latency_ms`: Copy trade execution latency
- `reputation_updates_total`: Reputation calculations
- `active_strategies_total`: Active automated strategies
- `social_posts_total`: Social posts created
- `direct_comm_latency_us`: Direct communication latency
- `follower_cache_hit_rate`: Copy relation cache efficiency

## Development

### Project Structure

```
trading-platform-service/
├── app/
│   ├── api/                    # API endpoints
│   │   └── unified_trading.py  # Unified trading API
│   ├── social_trading/         # Social trading features
│   │   ├── api/               # Social API endpoints
│   │   ├── copy/              # Copy trading
│   │   │   └── fast_copy_executor.py  # v2.0 executor
│   │   ├── reputation/        # Reputation system
│   │   ├── trading/           # Trading strategies
│   │   └── models.py          # Data models
│   ├── integrations/          # External integrations
│   ├── shared/                # Shared utilities
│   ├── vault_consul_integration.py  # Security
│   ├── dependencies.py        # Dependency injection
│   └── main.py               # FastAPI application
├── tests/                     # Test suite
├── Dockerfile                 # Container definition
└── requirements.in            # Python dependencies
```

### Testing

```bash
# Run unit tests
pytest tests/unit

# Run integration tests
pytest tests/integration

# Run with coverage
pytest --cov=app tests/
```

## Performance Optimization (v2.0)

### Copy Trading
- **Hot Path Caching**: Active relations cached in memory
- **Batch Processing**: Up to 100 orders per batch
- **Parallel Execution**: All calculations done concurrently
- **Pre-allocation**: Buffers pre-allocated for performance

### State Management
- **Shared Ignite**: Single cluster with Trading Core
- **Cache Partitioning**: Optimal data distribution
- **SQL Indexing**: Fast queries on leader_id, follower_id

### Event Processing
- **Direct Events**: Binary protocol for trade notifications
- **Batch Windows**: 10ms batching for optimal throughput
- **Fire-and-Forget**: No waiting for non-critical operations

## Security

- **Authentication**: JWT-based via Auth Service
- **Authorization**: Role-based access control
- **Encryption**: TLS for external communications
- **Vault Integration**: Secure credential management
- **Rate Limiting**: API rate limits per user
- **Risk Controls**: Built-in trading limits

## Deployment Considerations

1. **Co-location**: Deploy with Trading Core for optimal latency
2. **Memory**: 2GB+ for service, ensure Ignite has sufficient memory
3. **Monitoring**: Set up alerts for copy trade latency
4. **Scaling**: Horizontal scaling supported
5. **Cache Warming**: Pre-load active copy relations on startup

## Migration from v1.0 to v2.0

1. **Enable Direct Communication**: Set `ENABLE_DIRECT_COMM=true`
2. **Update Ignite**: Point to unified cluster
3. **Deploy FastCopyExecutor**: Replace standard executor
4. **Monitor Migration**: Watch latency improvements
5. **Fallback Ready**: HTTP remains available

## Future Enhancements

- **GPU Acceleration**: ML model inference on GPU
- **Advanced Strategies**: Reinforcement learning strategies
- **Cross-Platform Copy**: Copy across multiple exchanges
- **Social Sentiment**: NLP-based sentiment analysis
- **Decentralized Reputation**: Blockchain-based reputation

## Contributing

See the main PlatformQ [CONTRIBUTING.md](../../../CONTRIBUTING.md) for guidelines. 