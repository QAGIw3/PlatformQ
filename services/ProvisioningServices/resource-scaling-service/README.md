# Resource Scaling Service

The Resource Scaling Service provides intelligent auto-scaling, predictive scaling, and resource optimization for Platform Q workloads.

## Overview

This service enables:
- Automatic horizontal and vertical scaling
- Predictive scaling based on ML models
- Multi-metric scaling decisions
- Cost-aware scaling strategies
- Cross-service scaling coordination

## Features

- **Auto-scaling**: Automatic scaling based on metrics and policies
- **Predictive Scaling**: ML-based prediction of future resource needs
- **Vertical Scaling**: Adjust CPU/memory limits without restarts
- **Horizontal Scaling**: Add/remove instances based on load
- **Cost Optimization**: Scale within budget constraints
- **Custom Metrics**: Scale based on business metrics

## API Endpoints

### Scaling Operations
- `POST /api/v1/scaling/evaluate` - Trigger scaling evaluation
- `GET /api/v1/scaling/decisions` - Get recent scaling decisions
- `POST /api/v1/scaling/apply/{decision_id}` - Apply scaling decision
- `GET /api/v1/scaling/status/{service_name}` - Get scaling status

### Policy Management
- `GET /api/v1/policies` - List scaling policies
- `POST /api/v1/policies` - Create scaling policy
- `PUT /api/v1/policies/{service_name}` - Update policy
- `DELETE /api/v1/policies/{service_name}` - Delete policy

### Predictions
- `GET /api/v1/predictions/{service_name}` - Get load predictions
- `POST /api/v1/predictions/train` - Train prediction models

### Configuration
- `GET /api/v1/config` - Get scaling configuration
- `PUT /api/v1/config` - Update configuration

### Health & Metrics
- `GET /health` - Health check endpoint
- `GET /ready` - Readiness check endpoint
- `GET /metrics` - Prometheus metrics

## Configuration

Environment variables:
- `SERVICE_PORT` - Service port (default: 8007)
- `KUBERNETES_CONFIG` - Kubernetes configuration
- `PULSAR_URL` - Pulsar broker URL
- `MONITORING_SERVICE_URL` - Resource monitoring service URL
- `SCALING_INTERVAL` - Scaling evaluation interval (seconds)
- `PREDICTIVE_SCALING_ENABLED` - Enable predictive scaling
- `VERTICAL_SCALING_ENABLED` - Enable vertical scaling

## Scaling Policies

### Policy Configuration
```json
{
  "service_name": "api-service",
  "namespace": "production",
  "min_replicas": 2,
  "max_replicas": 20,
  "target_cpu_utilization": 70,
  "target_memory_utilization": 80,
  "scale_up_threshold": 80,
  "scale_down_threshold": 30,
  "cooldown_seconds": 300,
  "enable_vertical_scaling": true,
  "enable_predictive_scaling": true,
  "cost_aware": true,
  "priority": 1
}
```

### Scaling Metrics
- **CPU Utilization**: Primary metric for most services
- **Memory Utilization**: For memory-intensive workloads
- **Request Rate**: For API services
- **Queue Length**: For worker services
- **Custom Metrics**: Business-specific metrics

## Scaling Strategies

### Horizontal Scaling
- **Scale Out**: Add replicas when load increases
- **Scale In**: Remove replicas when load decreases
- **Gradual Scaling**: Incremental changes
- **Burst Scaling**: Rapid scaling for spikes

### Vertical Scaling
- **CPU Adjustment**: Increase/decrease CPU limits
- **Memory Adjustment**: Resize memory allocation
- **Resource Rebalancing**: Optimize resource distribution
- **Live Updates**: No downtime scaling

### Predictive Scaling
- **Time-series Analysis**: Historical pattern detection
- **ML Models**: LSTM, ARIMA predictions
- **Seasonal Patterns**: Daily, weekly patterns
- **Event-based**: Scale before known events

## Cost-Aware Scaling

### Budget Constraints
- Set maximum cost thresholds
- Prefer spot instances when scaling out
- Optimize instance types for cost
- Scale down during off-peak hours

### Cost Optimization
- Right-sizing recommendations
- Instance type optimization
- Scheduling for cost savings
- Reserved capacity utilization

## Scaling Decision Process

1. **Metric Collection**: Gather current metrics
2. **Policy Evaluation**: Check against thresholds
3. **Prediction Analysis**: Consider future load
4. **Cost Calculation**: Estimate scaling costs
5. **Decision Making**: Generate scaling action
6. **Validation**: Verify constraints
7. **Execution**: Apply scaling changes

## Event Processing

### Incoming Events
- Resource anomaly events
- Budget alert events
- Performance degradation events
- Custom trigger events

### Outgoing Events
- Scaling decision events
- Scaling applied events
- Scaling failed events
- Cost impact events

## Integration

### Service Dependencies
- Resource Monitoring Service for metrics
- Quota Management Service for limits
- Cost Optimization Service for budgets
- Kubernetes API for scaling actions

### Event Streams
- Subscribe to metric events
- Publish scaling decisions
- React to system events

## Development

### Running Locally
```bash
cd services/resource-scaling-service
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8007
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t resource-scaling-service:latest -f services/resource-scaling-service/Dockerfile .
```

## Architecture

The service consists of:

- **Scaling Engine**: Core scaling logic
- **Predictive Scaler**: ML-based predictions
- **Policy Manager**: Scaling policy management
- **Event Processor**: Handle scaling events
- **Executor**: Apply scaling decisions

## Machine Learning Models

### Time-series Prediction
- LSTM for complex patterns
- ARIMA for seasonal data
- Prophet for trend analysis
- Ensemble methods

### Model Training
- Continuous learning
- Weekly retraining
- Performance monitoring
- A/B testing

## Safety Mechanisms

- **Cooldown Periods**: Prevent flapping
- **Gradual Scaling**: Controlled changes
- **Circuit Breakers**: Stop runaway scaling
- **Rollback**: Undo failed scaling
- **Limits**: Respect min/max bounds

## Monitoring

The service exposes Prometheus metrics for:
- Scaling decisions per service
- Scaling success/failure rates
- Prediction accuracy
- Decision latency
- Cost impact of scaling 