"""
Integration tests for refactored collaboration platform services
"""

import asyncio
import pytest
import httpx
import websockets
import json
from typing import Dict, Any


# Service URLs
STATE_SERVICE_URL = "http://localhost:8015"
COMPUTE_SERVICE_URL = "http://localhost:8016"
COLLABORATION_SERVICE_URL = "http://localhost:8017"


class TestStateManagementService:
    """Test state management service operations"""
    
    @pytest.mark.asyncio
    async def test_health_check(self):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{STATE_SERVICE_URL}/health")
            assert response.status_code == 200
            data = response.json()
            assert data["service"] == "state-management"
    
    @pytest.mark.asyncio
    async def test_cache_operations(self):
        async with httpx.AsyncClient() as client:
            # Create cache
            cache_config = {
                "name": "test_cache",
                "mode": "PARTITIONED",
                "backups": 1,
                "atomicity": "ATOMIC"
            }
            response = await client.post(
                f"{STATE_SERVICE_URL}/api/v1/caches",
                json=cache_config
            )
            assert response.status_code == 200
            
            # Put value
            response = await client.put(
                f"{STATE_SERVICE_URL}/api/v1/caches/test_cache/keys/test_key",
                json={"value": {"data": "test_value"}}
            )
            assert response.status_code == 200
            
            # Get value
            response = await client.get(
                f"{STATE_SERVICE_URL}/api/v1/caches/test_cache/keys/test_key"
            )
            assert response.status_code == 200
            data = response.json()
            assert data["value"]["data"] == "test_value"
            
            # Delete value
            response = await client.delete(
                f"{STATE_SERVICE_URL}/api/v1/caches/test_cache/keys/test_key"
            )
            assert response.status_code == 200


class TestComputeAllocationService:
    """Test compute allocation service operations"""
    
    @pytest.mark.asyncio
    async def test_health_check(self):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{COMPUTE_SERVICE_URL}/health")
            assert response.status_code == 200
            data = response.json()
            assert data["service"] == "compute-allocation"
    
    @pytest.mark.asyncio
    async def test_resource_allocation(self):
        async with httpx.AsyncClient() as client:
            # Allocate resources
            allocation_request = {
                "workload_type": "simulation",
                "workload_id": "test-sim-123",
                "requirements": {
                    "cpu_cores": 4,
                    "memory_gb": 16,
                    "gpu_count": 0
                },
                "strategy": "COST_OPTIMIZED",
                "duration_hours": 2.0
            }
            
            # Mock auth header
            headers = {"X-User-ID": "test-user", "X-Tenant-ID": "test-tenant"}
            
            response = await client.post(
                f"{COMPUTE_SERVICE_URL}/api/v1/allocations",
                json=allocation_request,
                headers=headers
            )
            assert response.status_code == 200
            data = response.json()
            assert data["success"] is True
            assert "allocation" in data
            
            allocation_id = data["allocation"]["allocation_id"]
            
            # Get allocation
            response = await client.get(
                f"{COMPUTE_SERVICE_URL}/api/v1/allocations/{allocation_id}",
                headers=headers
            )
            assert response.status_code == 200
            
            # Release allocation
            response = await client.delete(
                f"{COMPUTE_SERVICE_URL}/api/v1/allocations/{allocation_id}",
                headers=headers
            )
            assert response.status_code == 200
    
    @pytest.mark.asyncio
    async def test_pricing_endpoints(self):
        async with httpx.AsyncClient() as client:
            # Get current pricing
            response = await client.get(
                f"{COMPUTE_SERVICE_URL}/api/v1/pricing/current"
            )
            assert response.status_code == 200
            data = response.json()
            assert "pricing" in data
            
            # Get cost forecast
            forecast_request = {
                "workload_type": "simulation",
                "requirements": {
                    "cpu_cores": 8,
                    "memory_gb": 32
                },
                "duration_hours": 4.0
            }
            response = await client.get(
                f"{COMPUTE_SERVICE_URL}/api/v1/costs/forecast",
                params=forecast_request
            )
            assert response.status_code == 200
            data = response.json()
            assert "forecasts" in data


class TestCollaborationPlatformService:
    """Test collaboration platform service operations"""
    
    @pytest.mark.asyncio
    async def test_health_check(self):
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{COLLABORATION_SERVICE_URL}/health")
            assert response.status_code == 200
            data = response.json()
            assert data["service"] == "collaboration-platform"
    
    @pytest.mark.asyncio
    async def test_session_lifecycle(self):
        async with httpx.AsyncClient() as client:
            headers = {"X-User-ID": "test-user", "X-Tenant-ID": "test-tenant"}
            
            # Create session
            session_request = {
                "domain_type": "simulation",
                "project_name": "Test Simulation",
                "description": "Integration test simulation"
            }
            response = await client.post(
                f"{COLLABORATION_SERVICE_URL}/api/v1/sessions",
                json=session_request,
                headers=headers
            )
            assert response.status_code == 200
            data = response.json()
            session_id = data["session_id"]
            
            # Get session
            response = await client.get(
                f"{COLLABORATION_SERVICE_URL}/api/v1/sessions/{session_id}",
                headers=headers
            )
            assert response.status_code == 200
            data = response.json()
            assert data["domain_type"] == "simulation"
            
            # List sessions
            response = await client.get(
                f"{COLLABORATION_SERVICE_URL}/api/v1/sessions",
                headers=headers
            )
            assert response.status_code == 200
            data = response.json()
            assert len(data["sessions"]) > 0
            
            # Delete session
            response = await client.delete(
                f"{COLLABORATION_SERVICE_URL}/api/v1/sessions/{session_id}",
                headers=headers
            )
            assert response.status_code == 200
    
    @pytest.mark.asyncio
    async def test_websocket_collaboration(self):
        async with httpx.AsyncClient() as client:
            headers = {"X-User-ID": "test-user", "X-Tenant-ID": "test-tenant"}
            
            # Create session
            response = await client.post(
                f"{COLLABORATION_SERVICE_URL}/api/v1/sessions",
                json={"domain_type": "cad"},
                headers=headers
            )
            session_id = response.json()["session_id"]
            
            # Connect via WebSocket
            ws_url = f"ws://localhost:8017/ws/collaborate/{session_id}?user_id=user1&user_name=User1"
            
            async with websockets.connect(ws_url) as websocket:
                # Receive initial state
                message = await websocket.recv()
                data = json.loads(message)
                assert data["type"] == "initial_state"
                
                # Send operation
                operation = {
                    "type": "operation",
                    "subtype": "create_object",
                    "data": {
                        "object_type": "cube",
                        "position": [0, 0, 0],
                        "size": [1, 1, 1]
                    }
                }
                await websocket.send(json.dumps(operation))
                
                # Receive acknowledgment
                message = await websocket.recv()
                data = json.loads(message)
                assert data["type"] == "operation_ack"
                
                # Send ping
                await websocket.send(json.dumps({"type": "ping"}))
                
                # Receive pong
                message = await websocket.recv()
                data = json.loads(message)
                assert data["type"] == "pong"
            
            # Cleanup
            await client.delete(
                f"{COLLABORATION_SERVICE_URL}/api/v1/sessions/{session_id}",
                headers=headers
            )
    
    @pytest.mark.asyncio
    async def test_domain_capabilities(self):
        async with httpx.AsyncClient() as client:
            # List domains
            response = await client.get(
                f"{COLLABORATION_SERVICE_URL}/api/v1/domains"
            )
            assert response.status_code == 200
            data = response.json()
            assert len(data["domains"]) >= 2  # At least simulation and CAD
            
            # Get simulation capabilities
            response = await client.get(
                f"{COLLABORATION_SERVICE_URL}/api/v1/domains/simulation/capabilities"
            )
            assert response.status_code == 200
            data = response.json()
            assert "operation_types" in data
            
            # Get CAD capabilities
            response = await client.get(
                f"{COLLABORATION_SERVICE_URL}/api/v1/domains/cad/capabilities"
            )
            assert response.status_code == 200
            data = response.json()
            assert "operation_types" in data


class TestServiceIntegration:
    """Test integration between services"""
    
    @pytest.mark.asyncio
    async def test_full_collaboration_flow(self):
        """Test a complete collaboration flow using all services"""
        async with httpx.AsyncClient() as client:
            headers = {"X-User-ID": "test-user", "X-Tenant-ID": "test-tenant"}
            
            # 1. Create collaboration session
            response = await client.post(
                f"{COLLABORATION_SERVICE_URL}/api/v1/sessions",
                json={
                    "domain_type": "simulation",
                    "project_name": "Integration Test"
                },
                headers=headers
            )
            session_id = response.json()["session_id"]
            
            # 2. Check if compute resources needed
            response = await client.get(
                f"{COLLABORATION_SERVICE_URL}/api/v1/sessions/{session_id}/resource-usage",
                headers=headers
            )
            assert response.status_code == 200
            resource_data = response.json()
            
            # 3. Allocate resources if needed
            if not resource_data["allocated"] and resource_data.get("requirements"):
                response = await client.post(
                    f"{COLLABORATION_SERVICE_URL}/api/v1/sessions/{session_id}/allocate-resources",
                    headers=headers
                )
                assert response.status_code == 200
            
            # 4. Verify state is persisted
            # The collaboration service should have created state in the state management service
            # This is verified implicitly by successful session operations
            
            # 5. Create checkpoint
            response = await client.post(
                f"{COLLABORATION_SERVICE_URL}/api/v1/sessions/{session_id}/checkpoint?name=test_checkpoint",
                headers=headers
            )
            assert response.status_code == 200
            checkpoint_data = response.json()
            assert checkpoint_data["status"] == "checkpoint_created"
            
            # 6. Cleanup
            response = await client.delete(
                f"{COLLABORATION_SERVICE_URL}/api/v1/sessions/{session_id}",
                headers=headers
            )
            assert response.status_code == 200


if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"]) 