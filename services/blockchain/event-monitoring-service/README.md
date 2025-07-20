# Event Monitoring Service

A high-performance microservice that monitors blockchain events in real-time, providing webhook notifications, alerting, and comprehensive event analytics across multiple blockchain networks.

## Overview

The Event Monitoring Service continuously scans configured blockchain networks for specific events, smart contract interactions, and on-chain activities. It provides real-time notifications through webhooks, supports complex event filtering, and maintains a queryable history of all monitored events. The service is designed for high reliability with automatic recovery and comprehensive monitoring capabilities.

## Key Features

- **Multi-Chain Monitoring**: Simultaneous monitoring of Ethereum, BSC, Polygon, and other EVM chains
- **Real-Time Detection**: Sub-second event detection and notification
- **Flexible Subscriptions**: Create custom event filters with complex criteria
- **Webhook Delivery**: Reliable webhook delivery with retry logic
- **Event Decoding**: Automatic ABI-based event decoding
- **Alert System**: Configurable alerts for critical events
- **Historical Data**: Queryable event history with powerful search
- **Contract Registry**: Manage and version contract ABIs
- **High Availability**: Distributed monitoring with automatic failover
- **Performance Metrics**: Detailed monitoring and analytics
- **Batch Processing**: Efficient batch event processing

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   API Layer (FastAPI)                    │
├─────────────────────────────────────────────────────────┤
│                 Event Processor Core                     │
├──────────────┬────────────┬────────────┬───────────────┤
│   Monitor    │   Filter   │  Webhook   │    Alert      │
│   Manager    │   Engine   │  Delivery  │   Manager     │
├──────────────┴────────────┴────────────┴───────────────┤
│              Blockchain Monitors                         │
├──────────────┬────────────┬────────────┬───────────────┤
│     EVM      │   Solana   │   Cosmos   │    NEAR      │
│   Monitor    │  Monitor   │  Monitor   │   Monitor     │
├──────────────┴────────────┴────────────┴───────────────┤
│  Event Store │  ABI Registry │  Message Queue          │
│   (Redis)    │   (Ignite)    │ (Apache Pulsar)         │
└─────────────────────────────────────────────────────────┘
```

## API Endpoints

### Monitor Management
- `GET /api/v1/monitors` - List all monitors
- `POST /api/v1/monitors` - Create new monitor
- `GET /api/v1/monitors/{monitor_id}` - Get monitor details
- `PUT /api/v1/monitors/{monitor_id}` - Update monitor
- `DELETE /api/v1/monitors/{monitor_id}` - Delete monitor
- `POST /api/v1/monitors/{monitor_id}/start` - Start monitor
- `POST /api/v1/monitors/{monitor_id}/stop` - Stop monitor

### Subscription Management
- `GET /api/v1/subscriptions` - List subscriptions
- `POST /api/v1/subscriptions` - Create subscription
- `GET /api/v1/subscriptions/{subscription_id}` - Get subscription
- `PUT /api/v1/subscriptions/{subscription_id}` - Update subscription
- `DELETE /api/v1/subscriptions/{subscription_id}` - Delete subscription
- `POST /api/v1/subscriptions/test` - Test subscription filters

### Event Queries
- `GET /api/v1/events` - Search events
- `GET /api/v1/events/{event_id}` - Get event details
- `GET /api/v1/events/stats` - Event statistics
- `POST /api/v1/events/export` - Export events

### Alert Management
- `GET /api/v1/alerts` - List alert rules
- `POST /api/v1/alerts` - Create alert rule
- `GET /api/v1/alerts/{alert_id}` - Get alert details
- `PUT /api/v1/alerts/{alert_id}` - Update alert
- `DELETE /api/v1/alerts/{alert_id}` - Delete alert
- `GET /api/v1/alerts/history` - Alert history

### Contract ABI Management
- `GET /api/v1/contracts` - List contracts
- `POST /api/v1/contracts` - Register contract
- `GET /api/v1/contracts/{address}` - Get contract ABI
- `PUT /api/v1/contracts/{address}` - Update contract ABI
- `POST /api/v1/contracts/verify` - Verify contract ABI

### Webhook Management
- `GET /api/v1/webhooks` - List webhook endpoints
- `POST /api/v1/webhooks` - Register webhook
- `PUT /api/v1/webhooks/{webhook_id}` - Update webhook
- `DELETE /api/v1/webhooks/{webhook_id}` - Delete webhook
- `POST /api/v1/webhooks/{webhook_id}/test` - Test webhook

### Monitoring
- `GET /health` - Service health check
- `GET /metrics` - Prometheus metrics

## Configuration

The service is configured via environment variables:

```bash
# Service Configuration
SERVICE_NAME=event-monitoring-service
SERVICE_HOST=0.0.0.0
SERVICE_PORT=8005
ENVIRONMENT=production

# Monitor Configuration
DEFAULT_BLOCK_CONFIRMATIONS=12
MAX_BLOCK_RANGE=1000
SCAN_INTERVAL_SECONDS=1
BATCH_SIZE=100
MAX_RETRIES=3

# Blockchain Configuration
ETHEREUM_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/${API_KEY}
BSC_RPC_URL=https://bsc-dataseed.binance.org
POLYGON_RPC_URL=https://polygon-rpc.com
ENABLE_ARCHIVE_NODES=true

# Storage Configuration
REDIS_URL=redis://redis:6379/0
EVENT_RETENTION_DAYS=90
MAX_EVENTS_PER_QUERY=10000

# Webhook Configuration
WEBHOOK_TIMEOUT_SECONDS=30
WEBHOOK_MAX_RETRIES=5
WEBHOOK_RETRY_DELAY_SECONDS=60
WEBHOOK_BATCH_SIZE=50

# Alert Configuration
ALERT_CHECK_INTERVAL_SECONDS=60
ALERT_COOLDOWN_MINUTES=15
SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
PAGERDUTY_API_KEY=${PAGERDUTY_API_KEY}

# Pulsar Configuration
PULSAR_SERVICE_URL=pulsar://pulsar:6650
PULSAR_EVENT_TOPIC=persistent://platformq/blockchain/events
PULSAR_ALERT_TOPIC=persistent://platformq/blockchain/alerts

# Ignite Cache Configuration
IGNITE_ENDPOINTS=ignite-node1:10800,ignite-node2:10800
IGNITE_ABI_CACHE=contract-abi-cache

# Security
ENABLE_WEBHOOK_SIGNING=true
WEBHOOK_SIGNING_SECRET=${WEBHOOK_SECRET}
MAX_SUBSCRIPTIONS_PER_USER=100

# Monitoring
ENABLE_METRICS=true
LOG_LEVEL=INFO
TRACE_SAMPLE_RATE=0.1
```

## Dependencies

- **FastAPI**: REST API framework
- **web3.py**: Ethereum/EVM blockchain interaction
- **aioredis**: Async Redis client for event storage
- **aiopulsar**: Event streaming
- **pyignite**: Contract ABI caching
- **httpx**: Webhook delivery
- **sqlalchemy**: Event metadata storage
- **prometheus-client**: Metrics collection
- **python-consul**: Service discovery
- **structlog**: Structured logging

## Running the Service

### Using Docker

```bash
# Build the image
docker build -t event-monitoring-service .

# Run the container
docker run -d \
  --name event-monitoring \
  -p 8005:8005 \
  -e ETHEREUM_RPC_URL="https://eth-mainnet.g.alchemy.com/v2/your-key" \
  -e REDIS_URL="redis://redis:6379/0" \
  -e PULSAR_SERVICE_URL="pulsar://pulsar:6650" \
  -e IGNITE_ENDPOINTS="ignite:10800" \
  event-monitoring-service
```

### Using Docker Compose

```yaml
services:
  event-monitoring:
    build: ./services/blockchain/event-monitoring-service
    ports:
      - "8005:8005"
    environment:
      - ETHEREUM_RPC_URL=${ETHEREUM_RPC_URL}
      - REDIS_URL=redis://redis:6379/0
      - PULSAR_SERVICE_URL=pulsar://pulsar:6650
      - IGNITE_ENDPOINTS=ignite:10800
    depends_on:
      - redis
      - pulsar
      - ignite
      - postgres
```

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export ETHEREUM_RPC_URL="your-rpc-url"
export REDIS_URL="redis://localhost:6379/0"

# Run database migrations
alembic upgrade head

# Run the service
uvicorn app.main:app --host 0.0.0.0 --port 8005 --reload
```

## Event Subscription Examples

### Basic Event Filter

```json
{
  "name": "USDC Transfers",
  "chain": "ethereum",
  "contract_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
  "event_signature": "Transfer(address,address,uint256)",
  "filters": {
    "value": {
      "gte": "1000000000000"  // >= 1M USDC
    }
  }
}
```

### Complex Filter with Multiple Conditions

```json
{
  "name": "Large DEX Swaps",
  "chain": "ethereum",
  "contract_address": "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
  "event_signature": "Swap(address,uint256,uint256,uint256,uint256,address)",
  "filters": {
    "and": [
      {
        "amount0In": {
          "gte": "10000000000000000000"  // >= 10 ETH
        }
      },
      {
        "or": [
          {"to": "0x1234..."},
          {"to": "0x5678..."}
        ]
      }
    ]
  }
}
```

### Cross-Contract Monitoring

```json
{
  "name": "NFT Marketplace Activity",
  "chain": "ethereum",
  "contracts": [
    "0xOpenSeaContract",
    "0xLooksRareContract",
    "0xBlurContract"
  ],
  "event_signatures": [
    "OrderFulfilled(bytes32,address,address,address,uint256,uint256)",
    "TakerBid(bytes32,uint256,address,address,address,uint256)",
    "OrdersMatched(bytes32,bytes32,address,address,uint256)"
  ]
}
```

## Webhook Payload Format

```json
{
  "event_id": "evt_1234567890",
  "subscription_id": "sub_0987654321",
  "chain": "ethereum",
  "block_number": 18500000,
  "block_timestamp": "2024-01-10T10:00:00Z",
  "transaction_hash": "0x123...",
  "contract_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
  "event_name": "Transfer",
  "event_data": {
    "from": "0x123...",
    "to": "0x456...",
    "value": "1000000000000"
  },
  "decoded_data": {
    "from": "0x123...",
    "to": "0x456...",
    "value": "1000000.0",
    "token": "USDC"
  },
  "metadata": {
    "confirmations": 12,
    "log_index": 125
  }
}
```

## Alert Configuration

### Price Alert

```json
{
  "name": "ETH Price Spike",
  "type": "threshold",
  "condition": {
    "metric": "eth_price_usd",
    "operator": "gt",
    "value": 5000,
    "window": "5m"
  },
  "actions": ["slack", "email", "webhook"]
}
```

### Anomaly Detection Alert

```json
{
  "name": "Unusual Transfer Volume",
  "type": "anomaly",
  "condition": {
    "metric": "transfer_count",
    "method": "z-score",
    "threshold": 3,
    "baseline_window": "7d",
    "check_window": "1h"
  },
  "severity": "high"
}
```

## Monitoring

### Health Checks

The service provides detailed health status at `/health`:

```json
{
  "status": "healthy",
  "version": "1.0.0",
  "monitors": {
    "active": 15,
    "paused": 2,
    "failed": 0
  },
  "subscriptions": {
    "total": 247,
    "active": 230,
    "pending_webhooks": 12
  },
  "event_processing": {
    "events_per_second": 1250,
    "average_latency_ms": 45,
    "queue_depth": 180
  },
  "chains": {
    "ethereum": {
      "status": "synced",
      "latest_block": 18500000,
      "lag_blocks": 2
    }
  }
}
```

### Metrics

Prometheus metrics available at `/metrics`:

- `event_monitor_events_total` - Total events by chain and type
- `event_monitor_webhook_deliveries_total` - Webhook delivery attempts
- `event_monitor_webhook_latency_seconds` - Webhook delivery latency
- `event_monitor_processing_lag_blocks` - Block processing lag
- `event_monitor_active_subscriptions` - Active subscriptions
- `event_monitor_alert_triggers_total` - Alert triggers by rule

### Performance Metrics

```json
{
  "period": "1h",
  "events_processed": 125420,
  "webhooks_delivered": 98.5,
  "average_latency_ms": 125,
  "error_rate": 0.02,
  "by_chain": {
    "ethereum": {
      "events": 75000,
      "latency_ms": 150
    },
    "polygon": {
      "events": 50420,
      "latency_ms": 85
    }
  }
}
```

## Troubleshooting

### Common Issues

1. **Missing Events**
   - Verify RPC endpoint reliability
   - Check block confirmation settings
   - Review filter configuration

2. **Webhook Failures**
   - Check webhook endpoint availability
   - Verify webhook authentication
   - Review retry settings

3. **High Latency**
   - Monitor RPC performance
   - Adjust batch sizes
   - Scale monitor instances

### Debug Tools

```bash
# Test event filter
curl -X POST http://localhost:8005/api/v1/subscriptions/test \
  -H "Content-Type: application/json" \
  -d '{
    "filter": {...},
    "sample_events": [...]
  }'

# Check monitor status
curl http://localhost:8005/api/v1/monitors/{id}/status

# View recent webhooks
curl http://localhost:8005/api/v1/webhooks/{id}/deliveries
```

## Best Practices

1. **Filter Design**
   - Use indexed event parameters for efficiency
   - Minimize wildcard filters
   - Test filters before production

2. **Webhook Reliability**
   - Implement idempotent webhook handlers
   - Verify webhook signatures
   - Handle duplicate deliveries

3. **Performance Optimization**
   - Use appropriate block ranges
   - Enable event batching
   - Cache frequently accessed data

4. **Monitoring Strategy**
   - Set up alerts for critical events
   - Monitor webhook delivery rates
   - Track processing latency

## Security Considerations

1. **Webhook Security**
   - HMAC signature verification
   - IP allowlisting
   - Rate limiting

2. **Data Privacy**
   - Event data encryption at rest
   - PII filtering options
   - Audit logging

3. **Access Control**
   - API key authentication
   - Role-based permissions
   - Subscription quotas

## Contributing

1. Add tests for new event types
2. Document filter syntax changes
3. Update webhook payload examples
4. Performance test at scale

## License

Copyright © 2024 PlatformQ. All rights reserved. 