"""
Tests for policy evaluation functionality.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

from ..app.main import app
from ..app.services.policy_engine import PolicyEngine


client = TestClient(app)


def test_policy_evaluation_endpoint():
    """Test the policy evaluation endpoint"""
    
    # Test data
    request_data = {
        "subject": {
            "user_id": "test-user-123",
            "roles": ["admin"]
        },
        "action": "read_asset",
        "resource": {
            "asset_id": "asset-456",
            "owner_id": "test-user-123"
        }
    }
    
    response = client.post("/api/v1/evaluate", json=request_data)
    
    assert response.status_code == 200
    data = response.json()
    assert data["allow"] is True
    assert "reason" in data


def test_policy_evaluation_denied():
    """Test policy evaluation when access is denied"""
    
    request_data = {
        "subject": {
            "user_id": "test-user-123",
            "roles": ["viewer"]
        },
        "action": "delete_asset",
        "resource": {
            "asset_id": "asset-456",
            "owner_id": "other-user"
        }
    }
    
    response = client.post("/api/v1/evaluate", json=request_data)
    
    assert response.status_code == 200
    data = response.json()
    assert data["allow"] is False
    assert "reason" in data


def test_policy_evaluation_owner_access():
    """Test that resource owners can access their own resources"""
    
    request_data = {
        "subject": {
            "user_id": "test-user-123",
            "roles": ["member"]
        },
        "action": "write_asset",
        "resource": {
            "asset_id": "asset-456",
            "owner_id": "test-user-123"
        }
    }
    
    response = client.post("/api/v1/evaluate", json=request_data)
    
    assert response.status_code == 200
    data = response.json()
    assert data["allow"] is True
    assert "owns the resource" in data["reason"]


@pytest.mark.asyncio
async def test_policy_engine_rbac():
    """Test the policy engine RBAC logic directly"""
    
    engine = PolicyEngine()
    
    # Test admin access
    result = await engine.evaluate({
        "subject": {"user_id": "1", "roles": ["admin"]},
        "action": "manage_tenant",
        "resource": {},
        "context": {}
    })
    assert result["allow"] is True
    
    # Test member cannot manage tenant
    result = await engine.evaluate({
        "subject": {"user_id": "2", "roles": ["member"]},
        "action": "manage_tenant",
        "resource": {},
        "context": {}
    })
    assert result["allow"] is False
    
    # Test viewer can read
    result = await engine.evaluate({
        "subject": {"user_id": "3", "roles": ["viewer"]},
        "action": "view_analytics",
        "resource": {},
        "context": {}
    })
    assert result["allow"] is True


def test_get_available_actions():
    """Test getting available actions endpoint"""
    
    # This would need auth in real test
    response = client.get("/api/v1/policies/actions")
    
    # For now, we expect it to fail due to missing auth
    assert response.status_code == 422  # Unprocessable Entity due to missing deps 