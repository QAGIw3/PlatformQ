# MLOps Service

Comprehensive machine learning operations service for the platformQ ecosystem with integrated compute resource management and marketplace features.

## Features

### Core MLOps Capabilities
- **Model Registry** - Centralized model versioning and management with MLflow
- **Automated Training** - Event-driven model retraining pipelines
- **Model Deployment** - Seamless deployment to Knative via functions-service
- **Performance Monitoring** - Real-time model performance tracking
- **A/B Testing** - Built-in experiment management
- **Feedback Loop** - Continuous model improvement based on production data

### Compute Resource Integration
- **Automatic GPU Allocation** - Seamless integration with derivatives engine for compute
- **Cost-Optimized Training** - Automatic selection of most cost-effective compute resources
- **Resource Reservation** - Pre-book compute capacity for scheduled training jobs
- **Compute Futures Trading** - Hedge training costs with compute futures contracts
- **Multi-Provider Support** - Access compute from platform-owned and partner resources

### Model Marketplace
- **Model Trading** - Buy, sell, and license ML models
- **IP Protection** - Model watermarking and usage tracking
- **Revenue Sharing** - Automatic royalty distribution
- **License Management** - Multiple license types (perpetual, time-based, usage-based)
- **Blockchain Integration** - Transparent ownership and transaction records

## Architecture

Built with:
- **MLflow** for model lifecycle management
- **Apache Spark** for distributed training
- **Apache Pulsar** for event streaming
- **Knative** for serverless deployment
- **Apache Ignite** for distributed caching
- **Kubernetes** for orchestration

## API Endpoints

### Model Management
- `POST /api/v1/models/register` - Register new model version
- `GET /api/v1/models/{model_name}/versions` - List model versions
- `POST /api/v1/models/{model_name}/stage` - Transition model stage
- `GET /api/v1/models/metrics` - Get model performance metrics

### Training & Deployment
- `POST /api/v1/deployments/create` - Deploy model to production
- `GET /api/v1/deployments/{deployment_id}` - Get deployment status
- `POST /api/v1/retraining/trigger` - Manually trigger retraining
- `GET /api/v1/retraining/policies` - List retraining policies

### Marketplace
- `POST /api/v1/marketplace/list` - List model for sale
- `POST /api/v1/marketplace/purchase` - Purchase model license
- `GET /api/v1/marketplace/models` - Browse available models
- `GET /api/v1/marketplace/earnings` - Track earnings from model sales

### Compute Resources
- `GET /api/v1/models/training-requirements` - Get compute requirements
- `POST /api/v1/compute/reservation-confirmed` - Confirm compute reservation
- `GET /api/v1/training/scheduled` - List scheduled training jobs
- `POST /api/v1/models/estimate-cost` - Estimate training cost

## Configuration

Key environment variables:
```
MLFLOW_TRACKING_URI=http://mlflow:5000
PULSAR_URL=pulsar://pulsar:6650
FUNCTIONS_SERVICE_URL=http://functions-service:80
WORKFLOW_SERVICE_URL=http://workflow-service:80
DIGITAL_ASSET_SERVICE_URL=http://digital-asset-service:80
DERIVATIVES_ENGINE_URL=http://derivatives-engine-service:8000
SPARK_MASTER_URL=spark://spark-master:7077
```

## Integration with Derivatives Engine

### Automatic Compute Procurement
1. Training job scheduled → compute requirements calculated
2. MLOps requests capacity from derivatives engine
3. Cross-service coordinator allocates from best source:
   - Platform-owned resources
   - Partner wholesale capacity
   - Spot market via futures
4. Resources provisioned automatically
5. Training job executed with allocated compute
6. Resources released after completion

### Cost Optimization
- Real-time price comparison across providers
- Automatic selection of most economical option
- Ability to set cost constraints
- Integration with arbitrage engine for best prices

## Model Lifecycle

1. **Development** - Local experimentation and prototyping
2. **Registration** - Version control in MLflow registry
3. **Staging** - Testing in production-like environment
4. **Production** - Full deployment with monitoring
5. **Retraining** - Automated or manual model updates
6. **Retirement** - Graceful model deprecation

## Monitoring

- Model performance metrics (accuracy, latency, drift)
- Resource utilization (GPU, CPU, memory)
- Training job status and history
- Deployment health checks
- Cost tracking and optimization
- A/B test results

## Development

### Running Locally
```bash
cd services/mlops-service
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python app/main.py
```

### Running Tests
```bash
pytest tests/
```

### Building Docker Image
```bash
docker build -t platformq/mlops-service .
```

## Security

- JWT authentication for API access
- Model encryption at rest
- Secure model serving with access control
- Audit logging for all operations
- IP protection through watermarking

## Performance

- Distributed training on Spark clusters
- GPU acceleration for deep learning
- Model caching for low-latency serving
- Horizontal scaling for API endpoints
- Optimized model storage with compression 