"""
Tests for Federated Learning Service
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch
import json

# Mock the dependencies before importing the app
with patch('pyignite.Client'):
    with patch('pulsar.Client'):
        from app.main import app

client = TestClient(app)


def test_health_check():
    """Test health check endpoint"""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "federated-learning-service"


@patch('app.main.coordinator.ignite_client')
@patch('app.main.coordinator.event_publisher')
def test_create_session(mock_publisher, mock_ignite):
    """Test creating a federated learning session"""
    # Mock Ignite cache
    mock_cache = Mock()
    mock_ignite.get_or_create_cache.return_value = mock_cache
    
    # Test request
    request_data = {
        "model_type": "CLASSIFICATION",
        "algorithm": "LogisticRegression",
        "dataset_requirements": {
            "min_samples": 100,
            "features": ["feature1", "feature2"],
            "label_column": "label"
        },
        "training_parameters": {
            "rounds": 5,
            "min_participants": 2,
            "model_hyperparameters": {
                "maxIter": 10
            }
        }
    }
    
    # Mock authentication
    headers = {"X-Tenant-ID": "test-tenant", "X-User-ID": "test-user"}
    
    response = client.post(
        "/api/v1/sessions",
        json=request_data,
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    assert "session_id" in data
    assert data["status"] == "CREATED"
    assert data["total_rounds"] == 5
    
    # Verify Ignite was called
    mock_cache.put.assert_called_once()


@patch('app.main.coordinator.ignite_client')
def test_get_session_status(mock_ignite):
    """Test getting session status"""
    # Mock session data
    session_data = {
        "session_id": "test-session",
        "status": "TRAINING",
        "current_round": 3,
        "total_rounds": 10,
        "participants": ["participant1", "participant2"],
        "created_at": "2024-01-01T00:00:00"
    }
    
    mock_cache = Mock()
    mock_cache.get.return_value = json.dumps(session_data)
    mock_ignite.get_cache.return_value = mock_cache
    
    # Mock participants cache
    mock_participants_cache = Mock()
    mock_participants_cache.keys.return_value = [
        "test-session:participant1",
        "test-session:participant2"
    ]
    mock_participants_cache.get.side_effect = [
        json.dumps({
            "participant_id": "participant1",
            "dataset_stats": {"num_samples": 1000},
            "status": "ACTIVE"
        }),
        json.dumps({
            "participant_id": "participant2",
            "dataset_stats": {"num_samples": 2000},
            "status": "ACTIVE"
        })
    ]
    
    mock_ignite.get_cache.side_effect = [mock_cache, mock_participants_cache]
    
    headers = {"X-Tenant-ID": "test-tenant", "X-User-ID": "test-user"}
    response = client.get("/api/v1/sessions/test-session/status", headers=headers)
    
    assert response.status_code == 200
    data = response.json()
    assert data["session_id"] == "test-session"
    assert data["status"] == "TRAINING"
    assert data["current_round"] == 3
    assert len(data["participants"]) == 2


@patch('app.main.coordinator.ignite_client')
@patch('app.main.coordinator.verify_participant_credentials')
@patch('app.main.coordinator.get_user_reputation')
def test_join_session(mock_reputation, mock_verify_creds, mock_ignite):
    """Test joining a federated learning session"""
    # Mock reputation
    mock_reputation.return_value = 75
    
    # Mock credential verification
    mock_verify_creds.return_value = [{
        "credential_id": "cred-123",
        "credential_type": "DataProviderCredential",
        "issuer": "platformq"
    }]
    
    # Mock session data
    session_data = {
        "session_id": "test-session",
        "status": "WAITING_FOR_PARTICIPANTS",
        "participants": [],
        "participation_criteria": {
            "required_credentials": [{
                "credential_type": "DataProviderCredential"
            }],
            "min_reputation_score": 50
        },
        "tenant_id": "test-tenant"
    }
    
    mock_cache = Mock()
    mock_cache.get.return_value = json.dumps(session_data)
    mock_ignite.get_cache.return_value = mock_cache
    
    # Mock participant cache
    mock_participant_cache = Mock()
    mock_ignite.get_or_create_cache.return_value = mock_participant_cache
    
    # Join request
    join_request = {
        "session_id": "test-session",
        "dataset_stats": {
            "num_samples": 5000,
            "num_features": 10,
            "data_hash": "abc123"
        },
        "compute_capabilities": {
            "spark_executors": 4,
            "memory_gb": 16.0,
            "gpu_available": False
        },
        "public_key": "test-public-key"
    }
    
    headers = {"X-Tenant-ID": "test-tenant", "X-User-ID": "test-user"}
    response = client.post(
        "/api/v1/sessions/test-session/join",
        json=join_request,
        headers=headers
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "JOINED"
    assert data["participant_id"] == "test-tenant:test-user"


def test_session_not_found():
    """Test accessing non-existent session"""
    with patch('app.main.coordinator.ignite_client') as mock_ignite:
        mock_cache = Mock()
        mock_cache.get.return_value = None
        mock_ignite.get_cache.return_value = mock_cache
        
        headers = {"X-Tenant-ID": "test-tenant", "X-User-ID": "test-user"}
        response = client.get("/api/v1/sessions/non-existent/status", headers=headers)
        
        assert response.status_code == 404
        assert "Session not found" in response.json()["detail"]


if __name__ == "__main__":
    pytest.main([__file__]) 