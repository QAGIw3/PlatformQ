"""
Pytest Configuration and Fixtures

Provides common test fixtures for the Data Catalog Hub tests.
"""

import pytest
import asyncio
from typing import Generator, AsyncGenerator
from unittest.mock import Mock, AsyncMock

from fastapi.testclient import TestClient
from dependency_injector import containers

from app.application import create_application
from app.core.container import Container
from app.core.config import Settings


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def test_settings() -> Settings:
    """Create test settings."""
    return Settings(
        ENV="test",
        DEBUG=True,
        TESTING=True,
        LOG_LEVEL="DEBUG",
        
        # Test database connections
        ATLAS_URL="http://test-atlas:21000",
        ELASTICSEARCH_HOSTS=["http://test-es:9200"],
        IGNITE_HOST="test-ignite",
        IGNITE_PORT=10800,
        
        # Disable external services in tests
        ENABLE_PULSAR=False,
        ENABLE_CONSUL=False,
        ENABLE_VAULT=False
    )


@pytest.fixture
def mock_atlas_client():
    """Create a mock Atlas client."""
    client = AsyncMock()
    client.initialize = AsyncMock()
    client.cleanup = AsyncMock()
    client.get_entity = AsyncMock(return_value={"guid": "test-guid", "typeName": "test_table"})
    client.create_entity = AsyncMock(return_value={"guid": "new-guid"})
    client.search_entities = AsyncMock(return_value={"entities": []})
    return client


@pytest.fixture
def mock_elasticsearch():
    """Create a mock Elasticsearch client."""
    client = AsyncMock()
    client.info = AsyncMock(return_value={"version": {"number": "8.0.0"}})
    client.search = AsyncMock(return_value={"hits": {"hits": [], "total": {"value": 0}}})
    client.index = AsyncMock(return_value={"_id": "test-doc"})
    client.close = AsyncMock()
    return client


@pytest.fixture
def mock_ignite_cache():
    """Create a mock Ignite cache adapter."""
    cache = AsyncMock()
    cache.initialize = AsyncMock()
    cache.cleanup = AsyncMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()
    cache.delete = AsyncMock()
    return cache


@pytest.fixture
def test_container(
    test_settings,
    mock_atlas_client,
    mock_elasticsearch,
    mock_ignite_cache
) -> Container:
    """Create a test DI container with mocks."""
    container = Container()
    
    # Override configuration
    container.config.from_pydantic(test_settings)
    
    # Override infrastructure components with mocks
    container.atlas_client.override(lambda: mock_atlas_client)
    container.elasticsearch_client.override(lambda: mock_elasticsearch)
    container.ignite_cache_adapter.override(lambda: mock_ignite_cache)
    
    # Override event bus with test implementation
    mock_event_bus = Mock()
    mock_event_bus.emit = AsyncMock()
    mock_event_bus.register_handler = Mock()
    container.event_bus.override(lambda: mock_event_bus)
    
    return container


@pytest.fixture
async def test_app(test_container):
    """Create a test FastAPI application."""
    app = create_application(container=test_container, testing=True)
    yield app


@pytest.fixture
async def client(test_app) -> AsyncGenerator[TestClient, None]:
    """Create a test client."""
    with TestClient(test_app) as test_client:
        yield test_client


@pytest.fixture
def auth_headers():
    """Create test authentication headers."""
    return {
        "Authorization": "Bearer test-token",
        "X-User-ID": "test-user",
        "X-Tenant-ID": "test-tenant"
    }


# Service-specific fixtures

@pytest.fixture
async def entity_service(test_container):
    """Get entity service from test container."""
    return await test_container.entity_service()


@pytest.fixture
async def schema_service(test_container):
    """Get schema service from test container."""
    return await test_container.schema_service()


@pytest.fixture
async def search_service(test_container):
    """Get unified search service from test container."""
    service = await test_container.unified_search_service()
    await service.initialize()
    return service


@pytest.fixture
async def lineage_service(test_container):
    """Get lineage service from test container."""
    return await test_container.lineage_service()


@pytest.fixture
async def classification_service(test_container):
    """Get classification service from test container."""
    return await test_container.classification_service()


@pytest.fixture
async def glossary_service(test_container):
    """Get glossary service from test container."""
    return await test_container.glossary_service()


# Test data fixtures

@pytest.fixture
def sample_entity():
    """Create a sample entity for testing."""
    return {
        "typeName": "test_table",
        "attributes": {
            "name": "test_table",
            "qualifiedName": "test_db.test_schema.test_table",
            "description": "Test table for unit tests"
        },
        "guid": "test-guid-123"
    }


@pytest.fixture
def sample_schema():
    """Create a sample schema for testing."""
    return {
        "name": "test_schema",
        "type": "AVRO",
        "schema": {
            "type": "record",
            "name": "TestRecord",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"}
            ]
        }
    }


@pytest.fixture
def sample_lineage():
    """Create sample lineage data for testing."""
    return {
        "process_name": "test_etl",
        "process_type": "ETL",
        "inputs": ["input-guid-1", "input-guid-2"],
        "outputs": ["output-guid-1"],
        "metadata": {
            "runtime": 300,
            "records_processed": 1000
        }
    } 