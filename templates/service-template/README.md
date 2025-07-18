# PlatformQ Service Template

A [Cookiecutter](https://github.com/cookiecutter/cookiecutter) template for creating standardized PlatformQ microservices with all best practices built-in.

## Features

This template creates a fully-featured microservice with:

- ✅ **FastAPI** framework with async support
- ✅ **Event-driven architecture** using Apache Pulsar
- ✅ **Standardized patterns** from platformq-shared library
- ✅ **Repository pattern** for database operations (PostgreSQL/Cassandra)
- ✅ **Error handling** with custom exceptions
- ✅ **Service client** for resilient inter-service communication
- ✅ **Health checks** and readiness endpoints
- ✅ **Docker** support with multi-stage builds
- ✅ **Kubernetes** manifests and Helm charts
- ✅ **Testing** setup with pytest
- ✅ **CI/CD** templates for GitLab

## Usage

1. Install cookiecutter:
```bash
pip install cookiecutter
```

2. Generate a new service:
```bash
cookiecutter templates/service-template/
```

3. Answer the prompts:
- `service_name`: Human-readable service name (e.g., "Order Management Service")
- `service_slug`: URL-friendly name (auto-generated from service_name)
- `service_description`: Brief description of the service
- `service_port`: Port number (default: 8000)
- `database_type`: Choose from postgresql, cassandra, both, or none
- `use_event_processing`: Enable event-driven features (default: true)
- `use_ignite_cache`: Enable Apache Ignite caching (default: true)
- `include_websocket`: Add WebSocket support (default: false)
- `include_grpc`: Add gRPC support (default: false)

4. Navigate to your new service:
```bash
cd your-service-name/
```

5. Install dependencies:
```bash
pip install -r requirements.txt
```

6. Run the service:
```bash
uvicorn app.main:app --reload
```

## Project Structure

```
your-service-name/
├── app/
│   ├── api/
│   │   ├── __init__.py
│   │   ├── endpoints.py      # API endpoints with standardized patterns
│   │   ├── deps.py           # FastAPI dependencies
│   │   └── websocket.py      # WebSocket endpoints (if enabled)
│   ├── core/
│   │   ├── __init__.py
│   │   └── config.py         # Service configuration
│   ├── models.py             # Database models
│   ├── schemas.py            # Pydantic schemas
│   ├── repository.py         # Repository pattern implementation
│   ├── event_processors.py   # Event handlers
│   ├── cache.py              # Ignite cache manager (if enabled)
│   └── main.py               # Application entry point
├── tests/
│   ├── __init__.py
│   ├── test_api.py
│   ├── test_repository.py
│   └── test_events.py
├── helm/
│   └── charts/               # Helm chart for Kubernetes
├── Dockerfile                # Multi-stage Docker build
├── requirements.txt          # Python dependencies
├── requirements-dev.txt      # Development dependencies
├── README.md                 # Service documentation
└── .gitlab-ci.yml           # GitLab CI/CD pipeline
```

## Key Features Explained

### Event Processing

The template includes a complete event processing setup:

```python
@event_handler("persistent://platformq/*/user-created-events", UserCreatedEvent)
async def handle_user_created(self, event: UserCreatedEvent, msg):
    # Your business logic here
    return ProcessingResult(status=ProcessingStatus.SUCCESS)
```

### Repository Pattern

Database operations are abstracted using the repository pattern:

```python
# PostgreSQL
class YourRepository(PostgresRepository[YourModel]):
    def create(self, data: YourCreate) -> YourModel:
        # Implementation using SQLAlchemy

# Cassandra
class YourRepository(CassandraRepository[YourModel]):
    def create(self, data: YourCreate) -> YourModel:
        # Implementation using Cassandra driver
```

### Service Communication

Inter-service calls use the resilient service client:

```python
response = await service_clients.post(
    "other-service",
    "/api/v1/endpoint",
    json={"data": "value"}
)
```

### Error Handling

Standardized error responses across all services:

```python
if not found:
    raise NotFoundError(f"Resource {id} not found")
    
if duplicate:
    raise ConflictError(f"Resource already exists")
```

## Configuration

Services are configured via environment variables:

- `DATABASE_URL`: PostgreSQL connection string
- `CASSANDRA_HOSTS`: Comma-separated Cassandra nodes
- `PULSAR_URL`: Apache Pulsar broker URL
- `IGNITE_HOSTS`: Apache Ignite nodes
- `SERVICE_PORT`: HTTP port (default: 8000)

## Development

### Running Tests

```bash
pytest tests/
```

### Building Docker Image

```bash
docker build -t your-service-name:latest .
```

### Deploying to Kubernetes

```bash
helm install your-service-name helm/charts/your-service-name/
```

## Best Practices

1. **Always use the repository pattern** for database operations
2. **Publish events** for significant state changes
3. **Handle errors gracefully** with proper error types
4. **Log important operations** with correlation IDs
5. **Write tests** for all endpoints and event handlers
6. **Document your API** with OpenAPI schemas
7. **Use dependency injection** for testability

## Contributing

When adding new features to services:

1. Update the repository with new methods
2. Add corresponding API endpoints
3. Create event handlers if needed
4. Write comprehensive tests
5. Update documentation

## Support

For questions or issues with the template, please contact the PlatformQ infrastructure team. 