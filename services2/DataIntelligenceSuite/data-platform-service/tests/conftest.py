"""
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
