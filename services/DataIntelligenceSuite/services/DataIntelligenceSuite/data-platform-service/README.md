# Data Platform Service

Part of DataIntelligenceSuite v2.0

## Overview

Unified data platform service providing ingestion, processing, storage, and lakehouse capabilities.

## Architecture

This service follows a clean architecture pattern with:
- **API Layer**: FastAPI-based REST API with versioning
- **Service Layer**: Business logic and orchestration
- **Core Layer**: Domain models and interfaces
- **Infrastructure Layer**: External service integrations

## Features

- Multi-source data ingestion
- Batch and stream processing
- Lakehouse architecture (Iceberg, Delta, Hudi)
- Automatic data optimization
- Schema evolution support

## API Documentation

When running, API documentation is available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Configuration

Configuration is managed through environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| SERVICE_NAME | Service identifier | data-platform-service |
| PORT | Service port | 8000 |
| LOG_LEVEL | Logging level | INFO |
| CONSUL_URL | Consul URL | http://localhost:8500 |
| VAULT_URL | Vault URL | http://localhost:8200 |

## Development

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- Access to Consul and Vault

### Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt

# Run locally
uvicorn app.main:app --reload --port 8000
```

### Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/unit/test_health.py
```

### Docker

```bash
# Build image
docker build -t data-platform-service:latest .

# Run container
docker run -p 8000:8000 data-platform-service:latest

# Using docker-compose
docker-compose up
```

## Deployment

### Kubernetes

```bash
# Apply manifests
kubectl apply -f helm/

# Or using Helm
helm install data-platform-service ./helm
```

### Monitoring

The service exposes Prometheus metrics at `/metrics`.

Key metrics:
- Request count and latency
- Error rates
- Business metrics (service-specific)

## Contributing

Please follow the project's contribution guidelines.

## License

See LICENSE file in the project root.
