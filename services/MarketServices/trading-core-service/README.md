# Trading Core Service

## Overview

The Trading Core Service is the central component of the PlatformQ trading infrastructure, providing a high-performance, distributed trading engine with real-time order matching, position management, and market data handling. It leverages Apache Flink for complex event processing and Apache Ignite for distributed state management.

**New in v2.0:** Ultra-low latency direct communication with Trading Platform Service, achieving sub-millisecond copy trading execution and 20-100x performance improvements over HTTP-based communication.

## Architecture

### Key Components

1. **Matching Engine**: Price-time priority order matching with support for multiple order types
2. **Order Manager**: Order lifecycle management with Ignite-backed distributed state
3. **Position Manager**: Real-time position tracking and P&L calculation
4. **Market Manager**: Market configuration and trading session management
5. **Flink Event Processor**: Real-time event processing for order aggregation and risk monitoring
6. **Ignite State Manager**: Distributed state management for orders, positions, and markets
7. **Platform Direct Integration**: Sub-millisecond communication with Trading Platform Service

### Technology Stack

- **FastAPI**: High-performance REST API framework
- **Apache Flink**: Complex event processing and stream analytics
- **Apache Ignite**: In-memory distributed database and compute grid
- **Apache Pulsar**: Event streaming and messaging
- **WebSocket**: Real-time market data and order updates
- **Pydantic**: Data validation and serialization
- **Direct Communication**: Binary protocol for ultra-low latency inter-service communication
- **msgpack**: Fast binary serialization
- **uvloop**: High-performance event loop

## Features

### Trading Features
- **Order Types**: Market, Limit, Stop, Stop-Limit, Iceberg, Post-Only
- **Time-in-Force**: IOC, FOK, GTC, GTD
- **Real-time Processing**: Sub-millisecond order matching
- **Distributed State**: Scalable state management with Ignite
- **Event Streaming**: Real-time event processing with Flink
- **Risk Monitoring**: Built-in risk checks and position limits
- **Market Data**: Real-time orderbook updates via WebSocket
- **High Availability**: Distributed architecture with failover support

### Performance Features (v2.0)
- **Direct Communication**: Binary protocol achieving <100μs message latency
- **Batch Processing**: Optimized batch handling for copy trades
- **Hot Data Regions**: In-memory caching for frequently accessed data
- **Zero-Copy Operations**: Minimal serialization overhead
- **Parallel Execution**: Concurrent processing of follower orders

## Performance Metrics

| Operation | HTTP (v1.0) | Direct (v2.0) | Improvement |
|-----------|-------------|----------------|-------------|
| Single Order | 10-15ms | <0.5ms | **20-30x** |
| Copy Trade (100 followers) | 1000-1500ms | 10-50ms | **20-100x** |
| Risk Check | 5-10ms | <0.1ms | **50-100x** |
| State Access | 2-5ms | <0.01ms | **200-500x** |

## API Endpoints

### Orders
- `POST /api/v1/orders` - Submit new order
- `GET /api/v1/orders/{order_id}` - Get order details
- `DELETE /api/v1/orders/{order_id}` - Cancel order
- `GET /api/v1/orders` - List orders with filters

### Markets
- `GET /api/v1/markets` - List all markets
- `GET /api/v1/markets/{market_id}` - Get market details
- `GET /api/v1/markets/{market_id}/orderbook` - Get orderbook snapshot
- `GET /api/v1/markets/{market_id}/trades` - Get recent trades

### Positions
- `GET /api/v1/positions` - List all positions
- `GET /api/v1/positions/{market_id}` - Get position for market

### Internal APIs (v2.0)
- `/internal/derivatives/*` - Derivatives integration endpoints
- `/internal/compute/*` - Compute market endpoints
- `/internal/health` - Internal health check

### WebSocket
- `ws://localhost:8020/ws` - Real-time market data and order updates

## Configuration

Key configuration parameters in `app/config.py`:

```python
# Direct Communication (v2.0)
ENABLE_DIRECT_COMM = True
DIRECT_COMM_BATCH_SIZE = 1000
DIRECT_COMM_TIMEOUT_MS = 100

# Flink Configuration
FLINK_JOBMANAGER_HOST = "flink-jobmanager"
FLINK_CHECKPOINT_INTERVAL = 60000  # 1 minute
FLINK_STATE_BACKEND = "rocksdb"

# Ignite Configuration
IGNITE_HOSTS = ["ignite-unified:10800"]  # Unified cluster in v2.0
IGNITE_CACHE_MODE = "PARTITIONED"
IGNITE_BACKUPS = 1

# Trading Parameters
MAX_ORDER_SIZE = 1000000
MIN_ORDER_SIZE = 0.001
PRICE_TICK_SIZE = 0.01
```

## Running the Service

### Local Development

```bash
cd services/MarketServices/trading-core-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8020
```

### Docker

```bash
docker build -t trading-core-service .
docker run -p 8020:8020 trading-core-service
```

### Docker Compose (Unified Architecture)

The service is integrated with the platform's unified docker-compose setup:

```bash
docker-compose -f docker-compose.trading-unified.yml up trading-core
```

## Integration

### Dependencies
- **Apache Ignite**: Unified distributed state management (shared with Platform Service)
- **Apache Flink**: Event processing
- **Apache Pulsar**: Event streaming
- **Apache Cassandra**: Historical data storage
- **HashiCorp Vault**: Secrets management
- **HashiCorp Consul**: Service discovery

### Connected Services
- **Trading Platform Service**: Direct binary communication for copy trading
- **Risk Engine Service**: Real-time risk monitoring
- **Market Data Service**: External market data feeds
- **Futures Service**: Futures contract trading
- **Options Service**: Options contract trading
- **Auth Service**: Authentication and authorization

## Direct Communication Protocol (v2.0)

The service uses a high-performance binary protocol for inter-service communication:

### Message Types
- `ORDER_SUBMIT`: Direct order submission
- `COPY_TRADE`: Batch copy trade execution
- `RISK_CHECK`: Ultra-fast risk validation
- `TRADE_EXECUTE`: Trade execution notification
- `POSITION_UPDATE`: Position update notification

### Integration Example

```python
from platformq_direct_comm import DirectCommunicator, MessageType

# Initialize
communicator = DirectCommunicator("trading-core", ignite_client)
await communicator.start()

# Send order
result = await communicator.send_direct(
    target_service="trading-platform",
    msg_type=MessageType.ORDER_SUBMIT,
    data={"user_id": "123", "market_id": "BTC-USDT", ...},
    wait_response=True,
    timeout_ms=100
)
```

## Monitoring

The service exposes Prometheus metrics at `/metrics`:

- `trading_orders_total`: Total orders processed
- `trading_trades_total`: Total trades executed
- `trading_order_latency_seconds`: Order processing latency
- `trading_matching_latency_seconds`: Matching engine latency
- `trading_positions_total`: Active positions
- `trading_orderbook_depth`: Orderbook depth by market
- `direct_comm_latency_us`: Direct communication latency (v2.0)
- `direct_comm_messages_total`: Total direct messages processed (v2.0)

## Development

### Project Structure

```
trading-core-service/
├── app/
│   ├── api/          # REST API endpoints
│   ├── core/         # Core trading engine
│   ├── events/       # Flink event processing
│   ├── integrations/ # Service integrations
│   │   └── platform_direct.py  # Direct communication (v2.0)
│   ├── models/       # Pydantic models
│   ├── state/        # Ignite state management
│   ├── config.py     # Configuration
│   ├── dependencies.py # Dependency injection
│   └── main.py       # FastAPI application
├── scripts/          # Utility scripts
├── tests/           # Unit and integration tests
├── Dockerfile       # Container definition
└── requirements.in  # Python dependencies
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

## Performance Tuning (v2.0)

### Ignite Optimization
- **Hot Data Region**: 4GB in-memory for orderbooks and messaging
- **Cache Partitioning**: 1024 partitions for order cache
- **Binary Serialization**: Compact footer enabled

### Direct Communication
- **Batch Size**: 100-1000 messages per batch
- **Process Interval**: 1ms for optimal latency/throughput balance
- **Message TTL**: 10 seconds for messaging caches

### Threading
- **Order Processing**: 8 dedicated threads
- **Public Thread Pool**: 32 threads
- **Striped Pool**: 8 threads for cache operations

## Security

- **Authentication**: JWT-based authentication via Auth Service
- **Authorization**: Role-based access control (RBAC)
- **Encryption**: TLS for all external communications
- **Internal Communication**: Trusted binary protocol within cluster
- **Secrets**: Managed via HashiCorp Vault
- **Audit**: All operations logged with correlation IDs

## Deployment Considerations

1. **Scalability**: Deploy multiple instances behind a load balancer
2. **State Management**: Ensure Ignite cluster has sufficient memory (8GB+ recommended)
3. **Event Processing**: Scale Flink taskmanagers based on load
4. **Monitoring**: Set up alerts for latency and error rates
5. **Backup**: Regular snapshots of Ignite state
6. **Network**: Co-locate with Trading Platform Service for optimal latency

## Migration from v1.0 to v2.0

1. **Update Ignite Configuration**: Point to unified cluster
2. **Enable Direct Communication**: Set `ENABLE_DIRECT_COMM=true`
3. **Update Dependencies**: Install `platformq-direct-comm` library
4. **Monitor Performance**: Watch latency metrics during migration
5. **Fallback**: HTTP endpoints remain available for compatibility

## Contributing

See the main PlatformQ [CONTRIBUTING.md](../../../CONTRIBUTING.md) for guidelines. 