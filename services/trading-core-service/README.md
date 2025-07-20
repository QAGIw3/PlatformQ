# Trading Core Service

## Overview

The Trading Core Service is the central component of the PlatformQ trading infrastructure, providing a high-performance, distributed trading engine with real-time order matching, position management, and market data handling. It leverages Apache Flink for complex event processing and Apache Ignite for distributed state management.

## Architecture

### Key Components

1. **Matching Engine**: Price-time priority order matching with support for multiple order types
2. **Order Manager**: Order lifecycle management with Ignite-backed distributed state
3. **Position Manager**: Real-time position tracking and P&L calculation
4. **Market Manager**: Market configuration and trading session management
5. **Flink Event Processor**: Real-time event processing for order aggregation and risk monitoring
6. **Ignite State Manager**: Distributed state management for orders, positions, and markets

### Technology Stack

- **FastAPI**: High-performance REST API framework
- **Apache Flink**: Complex event processing and stream analytics
- **Apache Ignite**: In-memory distributed database and compute grid
- **Apache Pulsar**: Event streaming and messaging
- **WebSocket**: Real-time market data and order updates
- **Pydantic**: Data validation and serialization
- **HTTPX**: Async HTTP client for inter-service communication

## Features

- **Order Types**: Market, Limit, Stop, Stop-Limit, Iceberg, Post-Only
- **Time-in-Force**: IOC, FOK, GTC, GTD
- **Real-time Processing**: Sub-millisecond order matching
- **Distributed State**: Scalable state management with Ignite
- **Event Streaming**: Real-time event processing with Flink
- **Risk Monitoring**: Built-in risk checks and position limits
- **Market Data**: Real-time orderbook updates via WebSocket
- **High Availability**: Distributed architecture with failover support

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

### WebSocket
- `ws://localhost:8001/ws` - Real-time market data and order updates

## Configuration

Key configuration parameters in `app/config.py`:

```python
# Flink Configuration
FLINK_JOBMANAGER_HOST = "flink-jobmanager"
FLINK_CHECKPOINT_INTERVAL = 60000  # 1 minute
FLINK_STATE_BACKEND = "rocksdb"

# Ignite Configuration
IGNITE_HOSTS = ["ignite-1:10800", "ignite-2:10800"]
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
cd services/trading-core-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8001
```

### Docker

```bash
docker build -t trading-core-service .
docker run -p 8001:8001 trading-core-service
```

### Docker Compose

The service is integrated with the platform's docker-compose setup:

```bash
docker-compose up trading-core-service
```

## Integration

### Dependencies
- **Apache Ignite**: Distributed state management
- **Apache Flink**: Event processing
- **Apache Pulsar**: Event streaming
- **Apache Cassandra**: Historical data storage
- **HashiCorp Vault**: Secrets management
- **HashiCorp Consul**: Service discovery

### Connected Services
- **Risk Engine Service**: Real-time risk monitoring
- **Market Data Service**: External market data feeds
- **Futures Service**: Futures contract trading
- **Options Service**: Options contract trading
- **Auth Service**: Authentication and authorization

## Monitoring

The service exposes Prometheus metrics at `/metrics`:

- `trading_orders_total`: Total orders processed
- `trading_trades_total`: Total trades executed
- `trading_order_latency_seconds`: Order processing latency
- `trading_matching_latency_seconds`: Matching engine latency
- `trading_positions_total`: Active positions
- `trading_orderbook_depth`: Orderbook depth by market

## Development

### Project Structure

```
trading-core-service/
├── app/
│   ├── api/          # REST API endpoints
│   ├── core/         # Core trading engine
│   ├── events/       # Flink event processing
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

## Performance

- **Order Throughput**: 100,000+ orders/second
- **Matching Latency**: < 100 microseconds (99th percentile)
- **State Operations**: < 1ms read/write latency
- **Event Processing**: < 10ms end-to-end

## Security

- **Authentication**: JWT-based authentication via Auth Service
- **Authorization**: Role-based access control (RBAC)
- **Encryption**: TLS for all communications
- **Secrets**: Managed via HashiCorp Vault
- **Audit**: All operations logged with correlation IDs

## Deployment Considerations

1. **Scalability**: Deploy multiple instances behind a load balancer
2. **State Management**: Ensure Ignite cluster has sufficient memory
3. **Event Processing**: Scale Flink taskmanagers based on load
4. **Monitoring**: Set up alerts for latency and error rates
5. **Backup**: Regular snapshots of Ignite state

## Contributing

See the main PlatformQ [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines. 