"""
Test JWT Authentication Integration for Quantum Optimization Service

Verifies that the service properly validates JWT tokens and extracts user/tenant information
"""

import pytest
from unittest.mock import patch, MagicMock
from fastapi import HTTPException
from jose import jwt
from datetime import datetime, timedelta

from app.api.deps import (
    verify_token,
    get_current_user,
    get_current_tenant_id,
    get_current_user_id,
    RoleChecker,
    require_quantum_access
)


class TestJWTAuthentication:
    """Test JWT token validation and user extraction"""
    
    @pytest.fixture
    def valid_token_payload(self):
        """Create a valid JWT payload"""
        return {
            "sub": "user-123",
            "tid": "tenant-456",
            "email": "test@platformq.io",
            "groups": ["quantum_user", "premium_user"],
            "exp": datetime.utcnow() + timedelta(hours=1),
            "iat": datetime.utcnow(),
            "iss": "platformq-auth"
        }
    
    @pytest.fixture
    def valid_jwt_token(self, valid_token_payload):
        """Create a valid JWT token"""
        secret_key = "your-secret-key"  # Same as in deps.py
        return jwt.encode(valid_token_payload, secret_key, algorithm="HS256")
    
    @pytest.fixture
    def mock_credentials(self, valid_jwt_token):
        """Mock HTTPAuthorizationCredentials"""
        credentials = MagicMock()
        credentials.credentials = valid_jwt_token
        return credentials
    
    @pytest.mark.asyncio
    async def test_verify_token_success(self, mock_credentials, valid_token_payload):
        """Test successful token verification"""
        result = await verify_token(mock_credentials)
        
        assert result["user_id"] == valid_token_payload["sub"]
        assert result["tenant_id"] == valid_token_payload["tid"]
        assert result["groups"] == valid_token_payload["groups"]
        assert result["email"] == valid_token_payload["email"]
    
    @pytest.mark.asyncio
    async def test_verify_token_missing_user_id(self, mock_credentials):
        """Test token verification fails with missing user ID"""
        # Create token without 'sub' claim
        payload = {
            "tid": "tenant-456",
            "email": "test@platformq.io",
            "exp": datetime.utcnow() + timedelta(hours=1)
        }
        token = jwt.encode(payload, "your-secret-key", algorithm="HS256")
        mock_credentials.credentials = token
        
        with pytest.raises(HTTPException) as exc_info:
            await verify_token(mock_credentials)
        
        assert exc_info.value.status_code == 401
        assert "missing user or tenant information" in exc_info.value.detail
    
    @pytest.mark.asyncio
    async def test_verify_token_expired(self, mock_credentials):
        """Test token verification fails with expired token"""
        payload = {
            "sub": "user-123",
            "tid": "tenant-456",
            "exp": datetime.utcnow() - timedelta(hours=1)  # Expired
        }
        token = jwt.encode(payload, "your-secret-key", algorithm="HS256")
        mock_credentials.credentials = token
        
        with pytest.raises(HTTPException) as exc_info:
            await verify_token(mock_credentials)
        
        assert exc_info.value.status_code == 401
        assert "Could not validate credentials" in exc_info.value.detail
    
    @pytest.mark.asyncio
    async def test_get_current_user(self, valid_token_payload):
        """Test current user extraction"""
        token_data = {
            "user_id": valid_token_payload["sub"],
            "tenant_id": valid_token_payload["tid"],
            "email": valid_token_payload["email"],
            "groups": valid_token_payload["groups"]
        }
        
        user = await get_current_user(token_data)
        
        assert user["id"] == valid_token_payload["sub"]
        assert user["tenant_id"] == valid_token_payload["tid"]
        assert user["email"] == valid_token_payload["email"]
        assert user["groups"] == valid_token_payload["groups"]
    
    @pytest.mark.asyncio
    async def test_get_current_tenant_id(self):
        """Test tenant ID extraction"""
        user = {
            "id": "user-123",
            "tenant_id": "tenant-456",
            "email": "test@platformq.io",
            "groups": ["quantum_user"]
        }
        
        tenant_id = await get_current_tenant_id(user)
        assert tenant_id == "tenant-456"
    
    @pytest.mark.asyncio
    async def test_get_current_user_id(self):
        """Test user ID extraction"""
        user = {
            "id": "user-123",
            "tenant_id": "tenant-456",
            "email": "test@platformq.io",
            "groups": ["quantum_user"]
        }
        
        user_id = await get_current_user_id(user)
        assert user_id == "user-123"


class TestRoleBasedAccess:
    """Test role-based access control"""
    
    def test_role_checker_success(self):
        """Test role checker allows access with correct role"""
        checker = RoleChecker(["quantum_user", "admin"])
        user = {
            "id": "user-123",
            "groups": ["quantum_user", "premium_user"]
        }
        
        # Should not raise exception
        result = checker(user)
        assert result is True
    
    def test_role_checker_failure(self):
        """Test role checker denies access without required role"""
        checker = RoleChecker(["admin", "platform_admin"])
        user = {
            "id": "user-123",
            "groups": ["quantum_user", "premium_user"]
        }
        
        with pytest.raises(HTTPException) as exc_info:
            checker(user)
        
        assert exc_info.value.status_code == 403
        assert "Insufficient permissions" in exc_info.value.detail
    
    def test_require_quantum_access(self):
        """Test quantum access requirement"""
        # Test with quantum_user role
        user = {"id": "user-123", "groups": ["quantum_user"]}
        assert require_quantum_access(user) is True
        
        # Test with quantum_admin role
        user = {"id": "user-123", "groups": ["quantum_admin"]}
        assert require_quantum_access(user) is True
        
        # Test with admin role
        user = {"id": "user-123", "groups": ["admin"]}
        assert require_quantum_access(user) is True
        
        # Test without required role
        user = {"id": "user-123", "groups": ["basic_user"]}
        with pytest.raises(HTTPException) as exc_info:
            require_quantum_access(user)
        assert exc_info.value.status_code == 403


class TestServiceAuthentication:
    """Test service-to-service authentication"""
    
    @pytest.mark.asyncio
    async def test_verify_service_token_valid(self):
        """Test valid service token verification"""
        from app.api.deps import verify_service_token
        
        mock_credentials = MagicMock()
        mock_credentials.credentials = "service-simulation-token"
        
        result = await verify_service_token(mock_credentials)
        assert result is True
    
    @pytest.mark.asyncio
    async def test_verify_service_token_invalid(self):
        """Test invalid service token verification"""
        from app.api.deps import verify_service_token
        
        mock_credentials = MagicMock()
        mock_credentials.credentials = "service-unknown-token"
        
        result = await verify_service_token(mock_credentials)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_verify_service_token_none(self):
        """Test service token verification with no credentials"""
        from app.api.deps import verify_service_token
        
        result = await verify_service_token(None)
        assert result is False


@pytest.mark.integration
class TestAuthIntegration:
    """Integration tests for authentication flow"""
    
    @pytest.mark.asyncio
    async def test_create_problem_with_auth(self, client, valid_jwt_token):
        """Test creating optimization problem with valid authentication"""
        headers = {"Authorization": f"Bearer {valid_jwt_token}"}
        
        problem_data = {
            "name": "Test Portfolio Optimization",
            "problem_type": "portfolio",
            "objective_function": {
                "type": "maximize",
                "expression": "returns - risk"
            },
            "variables": {
                "assets": ["AAPL", "GOOGL", "MSFT"],
                "weights": {"min": 0, "max": 1}
            }
        }
        
        response = await client.post(
            "/api/v1/problems",
            json=problem_data,
            headers=headers
        )
        
        assert response.status_code == 200
        assert "problem_id" in response.json()
    
    @pytest.mark.asyncio
    async def test_create_problem_without_auth(self, client):
        """Test creating optimization problem without authentication fails"""
        problem_data = {
            "name": "Test Portfolio Optimization",
            "problem_type": "portfolio",
            "objective_function": {"type": "maximize"},
            "variables": {"assets": ["AAPL"]}
        }
        
        response = await client.post("/api/v1/problems", json=problem_data)
        
        assert response.status_code == 403  # Forbidden
    
    @pytest.mark.asyncio
    async def test_quota_enforcement(self, client):
        """Test quantum resource quota enforcement"""
        # Create token with limited quota
        payload = {
            "sub": "user-123",
            "tid": "tenant-456",
            "groups": ["quantum_user"],
            "exp": datetime.utcnow() + timedelta(hours=1)
        }
        token = jwt.encode(payload, "your-secret-key", algorithm="HS256")
        headers = {"Authorization": f"Bearer {token}"}
        
        # Mock quota response
        with patch("app.api.deps.get_quantum_resource_quota") as mock_quota:
            mock_quota.return_value = {
                "max_qubits": 10,
                "max_jobs_per_day": 5,
                "priority": "basic"
            }
            
            # Try to create job exceeding quota
            job_data = {
                "problem_id": "problem-123",
                "backend": "quantum_hardware",
                "num_qubits": 15  # Exceeds quota of 10
            }
            
            response = await client.post(
                "/api/v1/problems/problem-123/solve",
                json=job_data,
                headers=headers
            )
            
            assert response.status_code == 403
            assert "exceeds quota" in response.json()["detail"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 