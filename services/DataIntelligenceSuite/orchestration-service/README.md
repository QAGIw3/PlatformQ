# Orchestration Service

Workflow orchestration service using Airflow, Prefect, and custom DAGs

## Overview

This service is part of the DataIntelligenceSuite v2.0 architecture, providing consolidated functionality with enterprise-grade features.

## Features

- FastAPI-based REST API with automatic documentation
- GraphQL support (optional)
- Async/await throughout for high performance
- Pulsar integration for event-driven architecture
- Vault/Consul integration for security and configuration
- Prometheus metrics and OpenTelemetry tracing
- Structured logging with correlation IDs
- Health checks and readiness probes

## Quick Start

### Development

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set environment variables:
```bash
export VAULT_ADDR=http://localhost:8200
export CONSUL_ADDR=http://localhost:8500
export PULSAR_URL=pulsar://localhost:6650
```

3. Run the service:
```bash
python -m uvicorn app.main:app --reload
```

### Docker

```bash
docker-compose up --build
```

## API Documentation

Once running, visit:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc
- GraphQL Playground: http://localhost:8000/graphql (if enabled)

## Configuration

Configuration is managed through environment variables and Consul. See `app/core/config.py` for all available settings.

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=app --cov-report=html

# Run specific test file
pytest tests/unit/test_example.py
```

## Monitoring

- Metrics: http://localhost:8000/metrics
- Health: http://localhost:8000/health
- Readiness: http://localhost:8000/api/v1/health/ready
- Liveness: http://localhost:8000/api/v1/health/live

## Architecture

This service follows the consolidated architecture pattern:

```
orchestration-service/
├── app/
│   ├── api/          # API endpoints
│   ├── core/         # Core functionality
│   ├── models/       # Data models
│   ├── services/     # Business logic
│   └── utils/        # Utilities
├── tests/            # Test suite
├── scripts/          # Utility scripts
└── configs/          # Configuration files
```

## Contributing

Please follow the contribution guidelines in the main repository.

## License

See LICENSE in the root directory.
