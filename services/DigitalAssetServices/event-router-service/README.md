# Event Router Service

Central event routing and transformation hub for the platformQ ecosystem with advanced trading event support.

## Overview

The Event Router Service provides intelligent event routing, transformation, and enrichment capabilities with:

- **Content-based Routing**: Route events based on their content and metadata
- **Event Transformation**: Transform events between different formats and schemas
- **Dead Letter Queue (DLQ) Management**: Automatic handling of failed events with recovery strategies
- **Trading Event Processing**: Specialized handling for trading events with real-time enrichment
- **Pulsar Functions**: Serverless event processing for enrichment and validation
- **Multi-tenant Support**: Complete isolation between tenants
- **Schema Registry**: Event schema management and evolution

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Event Router Service                       │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┬──────────────┬─────────────┬────────────┐ │
│  │   Event     │   Trading    │    DLQ     │   Schema   │ │
│  │   Router    │   Events     │  Monitor   │  Registry  │ │
│  └─────────────┴──────────────┴─────────────┴────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Pulsar Functions                          │
│  ┌─────────────┬──────────────┬─────────────────────────┐  │
│  │   Event     │    Trade     │    Event                │  │
│  │ Validator   │   Enricher   │  Transformer            │  │
│  └─────────────┴──────────────┴─────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Features

### Event Routing
- Dynamic routing rules based on event content
- Multi-destination routing
- Conditional routing with complex expressions
- Priority-based routing for critical events

### Trading Event Processing

The service provides specialized handling for trading events with enrichment and validation:

### Trading Event Types
- Trade executions
- Position updates
- Market risk events
- Copy trading events

### Trading Event Features
- Real-time trade enrichment with market data
- Risk metric calculation
- Trader information enhancement
- Automated routing to risk analysis systems

### Trading Event API
```bash
# Submit trading event
POST /api/v1/trading-events/trade-events

# Update routing rules
PUT /api/v1/trading-events/routing-rules/{event_type}
```

## ML Event Processing

The service provides comprehensive ML lifecycle event routing and enrichment:

### ML Event Types
- **Training Lifecycle**: Training started/completed/failed
- **Model Management**: Model registered/deployed/retired
- **Inference**: Inference requests and results
- **Monitoring**: Drift detection, performance metrics
- **Features**: Feature computation events
- **Experiments**: Experiment creation and completion
- **Federated Learning**: Round started/completed

### ML Event Features
- Model lineage enrichment
- Cost calculation for training jobs
- Performance comparison with baselines
- Drift severity assessment
- Automated retraining recommendations
- Compliance metadata addition

### ML Event API
```bash
# Submit training events
POST /api/v1/ml-events/training-events

# Submit inference events
POST /api/v1/ml-events/inference-events

# Submit drift events
POST /api/v1/ml-events/drift-events

# Submit experiment events
POST /api/v1/ml-events/experiment-events

# Batch event submission
POST /api/v1/ml-events/batch-events
```

### ML Model Enricher Function
A dedicated Pulsar Function enriches ML events with:
- Model lineage information
- Resource utilization and cost analysis
- Performance benchmarking
- Compliance and bias assessment
- Explainability scores

## Digital Asset Event Processing

The service provides comprehensive digital asset lifecycle event routing:

### Asset Event Types
- **Asset Lifecycle**: Created/updated/deleted/published
- **Peer Reviews**: Review submitted/completed
- **Marketplace**: Asset purchased/licensed
- **License Management**: License issued/expired/revoked
- **Royalties**: Royalty distribution events
- **Metadata**: Asset metadata updates
- **Verification**: Asset verification events

### Asset Event Features
- Provenance and lineage tracking
- Processing requirement estimation
- Platform fee calculation
- Review impact assessment
- Transaction verification
- Duplicate detection support

### Asset Event API
```bash
# Submit asset creation event
POST /api/v1/asset-events/asset-created

# Submit review events
POST /api/v1/asset-events/review-events

# Submit marketplace events
POST /api/v1/asset-events/marketplace-events

# Submit license events
POST /api/v1/asset-events/license-events

# Submit royalty events
POST /api/v1/asset-events/royalty-events

# Batch event submission
POST /api/v1/asset-events/batch-events
```

### Asset Event Enrichment
Events are enriched with:
- Asset provenance information
- Processing requirements (GPU, memory)
- Platform fees and costs
- Review sentiment analysis
- Transaction blockchain verification

### Pulsar Functions
- **Event Validator**: Validates event format and business rules
- **Event Enricher**: Adds contextual data to events
- **Trade Enricher**: Specialized enrichment for trading events
- **Custom Functions**: Deploy custom processing logic

### Dead Letter Queue Management
- Automatic retry with exponential backoff
- Intelligent recovery strategies
- Alert generation for persistent failures
- Manual intervention support

## API Endpoints

### Event Routing
- `POST /api/v1/blockchain/events` - Submit blockchain events
- `GET /api/v1/blockchain/event-types` - List supported event types
- `GET /api/v1/blockchain/mappings` - Get event routing mappings
- `PUT /api/v1/blockchain/mappings` - Update event mappings

### Trading Events
- `POST /api/v1/trading/events` - Submit trading events
- `POST /api/v1/trading/events/batch` - Batch submit trading events
- `GET /api/v1/trading/event-mappings` - Get trading event mappings
- `PUT /api/v1/trading/event-mappings/{event_type}` - Update mappings
- `GET /api/v1/trading/dlq/stats` - Get DLQ statistics

### Health & Monitoring
- `GET /` - Service information
- `GET /health` - Health check endpoint

## Trading Event Types

```python
TRADE_EXECUTED = "trade.executed"
POSITION_UPDATED = "position.updated"
RISK_ALERT = "risk.alert"
MARGIN_CALL = "margin.call"
LIQUIDATION_TRIGGERED = "liquidation.triggered"
STRATEGY_SIGNAL = "strategy.signal"
```

## Configuration

### Environment Variables

```bash
# Pulsar Configuration
PULSAR_URL=pulsar://pulsar:6650
PULSAR_ADMIN_URL=http://pulsar:8080

# Service Configuration
SERVICE_NAME=event-router-service
LOG_LEVEL=INFO

# DLQ Configuration
DLQ_MAX_RETRIES=3
DLQ_RETRY_DELAY=60
DLQ_ALERT_THRESHOLD=100

# Function Configuration
FUNCTION_PARALLELISM=4
FUNCTION_CPU=2
FUNCTION_RAM=2147483648
```

## Pulsar Functions Deployment

Deploy the included Pulsar Functions:

```bash
# Deploy all functions
./app/pulsar_functions/deploy_functions.sh

# Deploy individual function
pulsar-admin functions create \
  --name trade-enricher \
  --py app/pulsar_functions/trade_enricher.py \
  --classname trade_enricher.TradeEnricherFunction \
  --inputs persistent://platformq/trading/trade-events \
  --output persistent://platformq/trading/enriched-trades
```

## Usage Examples

### Submit Trading Event

```python
import httpx

event = {
    "event_type": "trade.executed",
    "trader_id": "trader123",
    "market_id": "BTC-USD",
    "data": {
        "trade_id": "T-123456",
        "price": "45000.00",
        "quantity": "0.5",
        "side": "BUY",
        "order_id": "O-789"
    }
}

response = httpx.post(
    "http://event-router-service:8000/api/v1/trading/events",
    json=event
)
```

### Configure Event Routing

```python
# Update routing for trade execution events
routing_update = {
    "event_type": "trade.executed",
    "topics": [
        "persistent://platformq/trading/trade-analytics",
        "persistent://platformq/trading/risk-assessment",
        "persistent://platformq/trading/compliance-monitoring"
    ]
}

response = httpx.put(
    "http://event-router-service:8000/api/v1/trading/event-mappings/trade.executed",
    json=routing_update
)
```

## Monitoring

### Metrics
- Event routing rate and latency
- DLQ size and processing rate
- Function execution metrics
- Error rates by event type

### Alerts
- High DLQ message count
- Function processing failures
- Routing errors
- Schema validation failures

## Development

### Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run service
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Testing

```bash
# Run tests
pytest tests/

# Run with coverage
pytest --cov=app tests/
```

## Integration

The Event Router Service integrates with:
- **Trading Platform Service**: Processes trading events
- **Graph Intelligence Service**: Sends risk events for network analysis
- **Data Platform Service**: Routes events for data lake ingestion
- **Analytics Service**: Streams events for real-time analytics 