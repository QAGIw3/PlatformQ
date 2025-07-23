#!/usr/bin/env python3
"""
Service Template Generator for DataIntelligenceSuite v2.0

Generates scaffolding for new consolidated services.
"""

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime


class ServiceTemplate:
    """Generates service scaffolding based on templates"""
    
    def __init__(self, service_name: str, service_type: str, output_dir: Path):
        self.service_name = service_name
        self.service_type = service_type
        self.output_dir = output_dir
        self.service_dir = output_dir / service_name
        
    def generate(self):
        """Generate complete service structure"""
        print(f"Generating {self.service_type} service: {self.service_name}")
        
        # Create directory structure
        self._create_directories()
        
        # Generate files based on service type
        self._generate_main()
        self._generate_config()
        self._generate_api_routes()
        self._generate_core_modules()
        self._generate_models()
        self._generate_services()
        self._generate_tests()
        self._generate_docker_files()
        self._generate_documentation()
        
        print(f"Service generated at: {self.service_dir}")
        
    def _create_directories(self):
        """Create service directory structure"""
        directories = [
            "app",
            "app/api",
            "app/api/v1",
            "app/api/v1/endpoints",
            "app/api/v2",
            "app/api/v2/endpoints",
            "app/core",
            "app/models",
            "app/services",
            "app/utils",
            "tests",
            "tests/unit",
            "tests/integration",
            "tests/e2e",
            "scripts",
            "docs",
            "docs/api",
            "helm",
            "helm/templates"
        ]
        
        # Add service-specific directories
        if self.service_type == "data-platform":
            directories.extend([
                "app/connectors",
                "app/processors",
                "app/processors/batch",
                "app/processors/stream",
                "app/core/lakehouse",
                "app/core/ingestion"
            ])
        elif self.service_type == "analytics":
            directories.extend([
                "app/engines",
                "app/engines/sql",
                "app/engines/streaming",
                "app/dashboards",
                "app/visualizations"
            ])
        elif self.service_type == "ml-platform":
            directories.extend([
                "app/training",
                "app/serving",
                "app/experiments",
                "app/models/registry",
                "app/pipelines"
            ])
            
        for dir_path in directories:
            full_path = self.service_dir / dir_path
            full_path.mkdir(parents=True, exist_ok=True)
            
            # Create __init__.py
            init_file = full_path / "__init__.py"
            if not init_file.exists():
                init_file.write_text(f'"""{dir_path.replace("/", ".")}"""\n')
                
    def _generate_main(self):
        """Generate main.py"""
        content = f'''"""
{self.service_name.replace("-", " ").title()} Service

Enterprise-scale service for DataIntelligenceSuite v2.0
"""

import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from platformq_shared.logging import setup_logging
from data_intelligence_common.base_service import create_app
from data_intelligence_common.monitoring import setup_monitoring
from data_intelligence_common.vault_consul import UnifiedIntegration

from .core.config import settings
from .core.container import Container
from .api.v1 import api as v1_api
from .api.v2 import api as v2_api

# Setup structured logging
logger = setup_logging(__name__)

# Dependency injection container
container = Container()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info(f"Starting {settings.SERVICE_NAME} v2.0")
    
    # Initialize Vault/Consul integration
    integration = UnifiedIntegration(settings)
    await integration.initialize()
    
    # Initialize container
    await container.init_resources()
    
    # Wire dependencies
    container.wire(modules=[v1_api, v2_api])
    
    yield
    
    # Cleanup
    logger.info(f"Shutting down {settings.SERVICE_NAME}")
    await container.shutdown_resources()
    await integration.close()


# Create FastAPI application
app = create_app(
    title=settings.SERVICE_NAME,
    description="{self.service_name.replace("-", " ").title()} - Part of DataIntelligenceSuite v2.0",
    version="2.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Setup monitoring
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)

# Include API routers
app.include_router(v1_api.router, prefix="/api/v1", tags=["v1"])
app.include_router(v2_api.router, prefix="/api/v2", tags=["v2"])


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {{
        "status": "healthy",
        "service": settings.SERVICE_NAME,
        "version": "2.0.0",
        "timestamp": datetime.utcnow().isoformat()
    }}


@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint"""
    # Check dependencies
    checks = await container.health_checker().check_all()
    
    if all(check["status"] == "healthy" for check in checks):
        return {{"status": "ready", "checks": checks}}
    else:
        return {{"status": "not_ready", "checks": checks}}, 503


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.PORT,
        reload=settings.DEBUG,
        log_config=None  # Use our custom logging
    )
'''
        
        (self.service_dir / "app" / "main.py").write_text(content)
        
    def _generate_config(self):
        """Generate configuration files"""
        # settings.py
        settings_content = f'''"""
Service configuration using Pydantic settings
"""

from typing import List, Optional
from pydantic import BaseSettings, Field
from data_intelligence_common.core.config import BaseServiceConfig


class Settings(BaseServiceConfig):
    """Service settings with environment variable support"""
    
    # Service identification
    SERVICE_NAME: str = "{self.service_name}"
    SERVICE_VERSION: str = "2.0.0"
    
    # API settings
    API_V1_PREFIX: str = "/api/v1"
    API_V2_PREFIX: str = "/api/v2"
    
    # Service-specific settings
    ENABLE_CACHING: bool = True
    CACHE_TTL_SECONDS: int = 300
    
    # Performance settings
    MAX_WORKERS: int = Field(default=4, ge=1, le=32)
    REQUEST_TIMEOUT: int = Field(default=30, ge=1, le=300)
    
    # Feature flags
    ENABLE_ML_FEATURES: bool = True
    ENABLE_STREAMING: bool = True
    ENABLE_BATCH_PROCESSING: bool = True
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Global settings instance
settings = Settings()
'''
        
        (self.service_dir / "app" / "core" / "config.py").write_text(settings_content)
        
        # container.py for dependency injection
        container_content = '''"""
Dependency injection container
"""

from dependency_injector import containers, providers
from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.monitoring import MetricsCollector
from data_intelligence_common.core.events import EventBus

from .config import settings
from ..services.health import HealthChecker


class Container(containers.DeclarativeContainer):
    """DI container for service dependencies"""
    
    config = providers.Singleton(lambda: settings)
    
    # Infrastructure
    cache_manager = providers.Singleton(
        CacheManager,
        config=config
    )
    
    metrics_collector = providers.Singleton(
        MetricsCollector,
        service_name=config().SERVICE_NAME
    )
    
    event_bus = providers.Singleton(
        EventBus,
        config=config
    )
    
    # Services
    health_checker = providers.Singleton(
        HealthChecker,
        cache_manager=cache_manager,
        event_bus=event_bus
    )
    
    async def init_resources(self):
        """Initialize resources"""
        await self.cache_manager().initialize()
        await self.event_bus().initialize()
        
    async def shutdown_resources(self):
        """Shutdown resources"""
        await self.event_bus().close()
        await self.cache_manager().close()
'''
        
        (self.service_dir / "app" / "core" / "container.py").write_text(container_content)
        
    def _generate_api_routes(self):
        """Generate API route files"""
        # V1 API
        v1_init = '''"""
API v1 routes
"""

from fastapi import APIRouter
from .endpoints import health, status

router = APIRouter()

# Include endpoint routers
router.include_router(health.router, prefix="/health", tags=["health"])
router.include_router(status.router, prefix="/status", tags=["status"])
'''
        
        (self.service_dir / "app" / "api" / "v1" / "__init__.py").write_text(v1_init)
        
        # V2 API with more features
        v2_init = '''"""
API v2 routes with enhanced features
"""

from fastapi import APIRouter
from .endpoints import health, status

router = APIRouter()

# Include endpoint routers
router.include_router(health.router, prefix="/health", tags=["health"])
router.include_router(status.router, prefix="/status", tags=["status"])

# Add service-specific endpoints here
'''
        
        (self.service_dir / "app" / "api" / "v2" / "__init__.py").write_text(v2_init)
        
        # Sample endpoint
        endpoint_content = '''"""
Health endpoint
"""

from fastapi import APIRouter, Depends
from dependency_injector.wiring import inject, Provide

from ....core.container import Container
from ....services.health import HealthChecker

router = APIRouter()


@router.get("/")
@inject
async def health_status(
    health_checker: HealthChecker = Depends(Provide[Container.health_checker])
):
    """Get health status"""
    return await health_checker.get_status()
'''
        
        (self.service_dir / "app" / "api" / "v1" / "endpoints" / "health.py").write_text(endpoint_content)
        
    def _generate_core_modules(self):
        """Generate core modules based on service type"""
        if self.service_type == "data-platform":
            self._generate_data_platform_core()
        elif self.service_type == "analytics":
            self._generate_analytics_core()
        elif self.service_type == "ml-platform":
            self._generate_ml_platform_core()
            
    def _generate_data_platform_core(self):
        """Generate data platform specific core modules"""
        # Lakehouse manager
        lakehouse_content = '''"""
Lakehouse management for data platform
"""

from typing import Optional, Dict, Any
from data_intelligence_common.core.lakehouse import LakehouseManager, TableFormat

from ..config import settings


class DataPlatformLakehouse:
    """Enhanced lakehouse management for data platform"""
    
    def __init__(self, lakehouse_manager: LakehouseManager):
        self.lakehouse = lakehouse_manager
        self.default_format = TableFormat.ICEBERG
        
    async def create_managed_table(
        self,
        table_name: str,
        schema: Dict[str, Any],
        partition_by: Optional[List[str]] = None
    ):
        """Create a managed table with platform defaults"""
        return await self.lakehouse.create_table(
            table_name,
            schema,
            format=self.default_format,
            properties={
                "managed_by": "data-platform-service",
                "created_at": datetime.utcnow().isoformat()
            }
        )
'''
        
        (self.service_dir / "app" / "core" / "lakehouse" / "manager.py").write_text(lakehouse_content)
        
    def _generate_analytics_core(self):
        """Generate analytics specific core modules"""
        # Query engine
        query_engine_content = '''"""
Unified query engine for analytics
"""

from typing import Any, Dict, List, Optional
from enum import Enum

class QueryEngine(Enum):
    """Available query engines"""
    TRINO = "trino"
    SPARK_SQL = "spark_sql"
    DUCKDB = "duckdb"
    

class UnifiedQueryEngine:
    """Unified interface for multiple query engines"""
    
    def __init__(self):
        self.engines = {}
        self._initialize_engines()
        
    async def execute_query(
        self,
        query: str,
        engine: QueryEngine = QueryEngine.TRINO,
        parameters: Optional[Dict[str, Any]] = None
    ):
        """Execute query on specified engine"""
        if engine not in self.engines:
            raise ValueError(f"Engine {engine} not available")
            
        return await self.engines[engine].execute(query, parameters)
'''
        
        (self.service_dir / "app" / "engines" / "query_engine.py").write_text(query_engine_content)
        
    def _generate_ml_platform_core(self):
        """Generate ML platform specific core modules"""
        # Model registry
        registry_content = '''"""
ML Model Registry
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
from data_intelligence_common.core.ml import ModelRegistry

class MLPlatformRegistry(ModelRegistry):
    """Enhanced model registry for ML platform"""
    
    async def register_model(
        self,
        name: str,
        version: str,
        model_path: str,
        metadata: Dict[str, Any],
        tags: Optional[List[str]] = None
    ):
        """Register a new model version"""
        model_info = {
            "name": name,
            "version": version,
            "path": model_path,
            "metadata": metadata,
            "tags": tags or [],
            "registered_at": datetime.utcnow(),
            "status": "pending_validation"
        }
        
        # Validate model
        await self._validate_model(model_path)
        
        # Register
        return await super().register(model_info)
'''
        
        (self.service_dir / "app" / "models" / "registry" / "registry.py").write_text(registry_content)
        
    def _generate_models(self):
        """Generate Pydantic models"""
        # Base models
        base_models = '''"""
Base Pydantic models
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
from pydantic import BaseModel, Field
from uuid import UUID


class BaseRequest(BaseModel):
    """Base request model"""
    request_id: Optional[str] = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    

class BaseResponse(BaseModel):
    """Base response model"""
    success: bool = True
    message: Optional[str] = None
    data: Optional[Any] = None
    errors: List[Dict[str, Any]] = Field(default_factory=list)
    

class PaginatedResponse(BaseResponse):
    """Paginated response model"""
    total: int
    page: int
    page_size: int
    has_next: bool
    has_prev: bool
'''
        
        (self.service_dir / "app" / "models" / "base.py").write_text(base_models)
        
    def _generate_services(self):
        """Generate service layer files"""
        # Health service
        health_service = '''"""
Health checking service
"""

from typing import Dict, List, Any
from datetime import datetime


class HealthChecker:
    """Service health checker"""
    
    def __init__(self, cache_manager, event_bus):
        self.cache = cache_manager
        self.event_bus = event_bus
        
    async def get_status(self) -> Dict[str, Any]:
        """Get current health status"""
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "version": "2.0.0"
        }
        
    async def check_all(self) -> List[Dict[str, Any]]:
        """Check all dependencies"""
        checks = []
        
        # Check cache
        try:
            await self.cache.ping()
            checks.append({"name": "cache", "status": "healthy"})
        except Exception as e:
            checks.append({"name": "cache", "status": "unhealthy", "error": str(e)})
            
        # Check event bus
        try:
            await self.event_bus.ping()
            checks.append({"name": "event_bus", "status": "healthy"})
        except Exception as e:
            checks.append({"name": "event_bus", "status": "unhealthy", "error": str(e)})
            
        return checks
'''
        
        (self.service_dir / "app" / "services" / "health.py").write_text(health_service)
        
    def _generate_tests(self):
        """Generate test files"""
        # conftest.py
        conftest = '''"""
Test configuration and fixtures
"""

import pytest
import asyncio
from typing import Generator
from fastapi.testclient import TestClient

from app.main import app
from app.core.container import Container


@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create event loop for async tests"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def client() -> TestClient:
    """Create test client"""
    return TestClient(app)


@pytest.fixture
async def container() -> Container:
    """Create test container"""
    container = Container()
    await container.init_resources()
    yield container
    await container.shutdown_resources()
'''
        
        (self.service_dir / "tests" / "conftest.py").write_text(conftest)
        
        # Sample unit test
        unit_test = '''"""
Unit tests for health service
"""

import pytest
from app.services.health import HealthChecker


@pytest.mark.asyncio
async def test_health_status(container):
    """Test health status"""
    health_checker = container.health_checker()
    status = await health_checker.get_status()
    
    assert status["status"] == "healthy"
    assert "timestamp" in status
    assert status["version"] == "2.0.0"
'''
        
        (self.service_dir / "tests" / "unit" / "test_health.py").write_text(unit_test)
        
    def _generate_docker_files(self):
        """Generate Docker-related files"""
        # Dockerfile
        dockerfile = f'''FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    g++ \\
    curl \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \\
  CMD curl -f http://localhost:8000/health || exit 1

# Run application
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
'''
        
        (self.service_dir / "Dockerfile").write_text(dockerfile)
        
        # docker-compose.yml
        docker_compose = f'''version: '3.8'

services:
  {self.service_name}:
    build: .
    ports:
      - "8000:8000"
    environment:
      - SERVICE_NAME={self.service_name}
      - ENVIRONMENT=development
      - CONSUL_URL=http://consul:8500
      - VAULT_URL=http://vault:8200
    depends_on:
      - consul
      - vault
    networks:
      - platform-network

networks:
  platform-network:
    external: true
'''
        
        (self.service_dir / "docker-compose.yml").write_text(docker_compose)
        
        # .dockerignore
        dockerignore = '''__pycache__
*.pyc
*.pyo
*.pyd
.Python
env/
venv/
.venv/
pip-log.txt
pip-delete-this-directory.txt
.tox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
*.log
.git
.gitignore
.mypy_cache
.pytest_cache
.hypothesis
.env
.env.*
!.env.example
'''
        
        (self.service_dir / ".dockerignore").write_text(dockerignore)
        
    def _generate_documentation(self):
        """Generate documentation files"""
        # README.md
        readme = f'''# {self.service_name.replace("-", " ").title()}

Part of DataIntelligenceSuite v2.0

## Overview

{self._get_service_description()}

## Architecture

This service follows a clean architecture pattern with:
- **API Layer**: FastAPI-based REST API with versioning
- **Service Layer**: Business logic and orchestration
- **Core Layer**: Domain models and interfaces
- **Infrastructure Layer**: External service integrations

## Features

{self._get_service_features()}

## API Documentation

When running, API documentation is available at:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Configuration

Configuration is managed through environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| SERVICE_NAME | Service identifier | {self.service_name} |
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
source venv/bin/activate  # On Windows: venv\\Scripts\\activate

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
docker build -t {self.service_name}:latest .

# Run container
docker run -p 8000:8000 {self.service_name}:latest

# Using docker-compose
docker-compose up
```

## Deployment

### Kubernetes

```bash
# Apply manifests
kubectl apply -f helm/

# Or using Helm
helm install {self.service_name} ./helm
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
'''
        
        (self.service_dir / "README.md").write_text(readme)
        
        # API documentation template
        api_doc = f'''# {self.service_name.replace("-", " ").title()} API Documentation

## Base URL

```
https://api.platform.com/{self.service_name}
```

## Authentication

All API requests require authentication via JWT token:

```
Authorization: Bearer <token>
```

## API Versioning

The API is versioned through the URL path:
- v1: `/api/v1` - Stable API
- v2: `/api/v2` - Latest features (may have breaking changes)

## Common Responses

### Success Response

```json
{{
  "success": true,
  "data": {{...}},
  "message": "Operation successful"
}}
```

### Error Response

```json
{{
  "success": false,
  "errors": [
    {{
      "code": "ERROR_CODE",
      "message": "Error description",
      "field": "field_name"  // Optional
    }}
  ]
}}
```

## Endpoints

### Health Check

```
GET /health
```

Returns service health status.

### Ready Check

```
GET /ready
```

Returns service readiness status including dependency checks.

{self._get_service_endpoints()}

## Rate Limiting

API requests are rate limited:
- Anonymous: 100 requests/hour
- Authenticated: 1000 requests/hour
- Enterprise: Custom limits

## Webhooks

The service supports webhooks for real-time notifications.

### Webhook Events

{self._get_webhook_events()}

## SDKs

Official SDKs are available for:
- Python
- JavaScript/TypeScript
- Go
- Java

## Support

For API support, contact the platform team.
'''
        
        (self.service_dir / "docs" / "api" / "README.md").write_text(api_doc)
        
    def _get_service_description(self) -> str:
        """Get service-specific description"""
        descriptions = {
            "data-platform": "Unified data platform service providing ingestion, processing, storage, and lakehouse capabilities.",
            "analytics": "Advanced analytics engine supporting real-time and batch analytics, ML-powered insights, and visualization.",
            "ml-platform": "Complete machine learning platform for model training, serving, monitoring, and experimentation.",
            "governance": "Data governance service for catalog management, quality assurance, lineage tracking, and compliance.",
            "orchestration": "Workflow orchestration service for managing complex data pipelines and job scheduling.",
            "integration": "Integration hub providing unified API gateway and service mesh capabilities."
        }
        return descriptions.get(self.service_type, "Enterprise-scale service for data intelligence.")
        
    def _get_service_features(self) -> str:
        """Get service-specific features"""
        features = {
            "data-platform": """- Multi-source data ingestion
- Batch and stream processing
- Lakehouse architecture (Iceberg, Delta, Hudi)
- Automatic data optimization
- Schema evolution support""",
            "analytics": """- Unified SQL interface
- Real-time analytics
- Complex event processing
- ML-powered insights
- Custom dashboards""",
            "ml-platform": """- Model training orchestration
- Model registry and versioning
- A/B testing framework
- Real-time inference
- Model monitoring""",
            "governance": """- Data catalog with search
- Automated quality checks
- Lineage tracking
- Privacy compliance
- Access control""",
            "orchestration": """- DAG-based workflows
- Distributed job execution
- Fault tolerance
- Resource optimization
- Event-driven triggers""",
            "integration": """- GraphQL federation
- Service discovery
- Load balancing
- Circuit breaking
- Request routing"""
        }
        return features.get(self.service_type, "- Enterprise features\n- High performance\n- Scalability")
        
    def _get_service_endpoints(self) -> str:
        """Get service-specific API endpoints"""
        # This would return service-specific endpoint documentation
        return "See service-specific endpoint documentation."
        
    def _get_webhook_events(self) -> str:
        """Get service-specific webhook events"""
        # This would return service-specific webhook events
        return "- `resource.created`\n- `resource.updated`\n- `resource.deleted`"


def main():
    """Generate service from command line"""
    parser = argparse.ArgumentParser(description="Generate DataIntelligenceSuite v2.0 service")
    parser.add_argument("service_name", help="Name of the service (e.g., data-platform-service)")
    parser.add_argument(
        "--type",
        choices=["data-platform", "analytics", "ml-platform", "governance", "orchestration", "integration"],
        default="data-platform",
        help="Type of service to generate"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("services/DataIntelligenceSuite"),
        help="Output directory for generated service"
    )
    
    args = parser.parse_args()
    
    # Generate service
    generator = ServiceTemplate(args.service_name, args.type, args.output_dir)
    generator.generate()


if __name__ == "__main__":
    main() 