"""
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
