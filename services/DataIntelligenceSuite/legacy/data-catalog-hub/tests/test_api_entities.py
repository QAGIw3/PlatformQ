"""
Test Entity API Endpoints

Tests for the entity management API using the new architecture.
"""

import pytest
from httpx import AsyncClient


class TestEntityAPI:
    """Test entity API endpoints."""
    
    @pytest.mark.asyncio
    async def test_create_entity(self, client, auth_headers, sample_entity):
        """Test creating a new entity."""
        response = client.post(
            "/api/v1/entities",
            json=sample_entity,
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["guid"] == "new-guid"
    
    @pytest.mark.asyncio
    async def test_get_entity(self, client, auth_headers):
        """Test retrieving an entity by GUID."""
        response = client.get(
            "/api/v1/entities/test-guid-123",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["guid"] == "test-guid"
        assert data["typeName"] == "test_table"
    
    @pytest.mark.asyncio
    async def test_search_entities(self, client, auth_headers):
        """Test searching for entities."""
        response = client.get(
            "/api/v1/entities/search",
            params={"query": "test", "limit": 10},
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert "results" in data
        assert isinstance(data["results"], list)
    
    @pytest.mark.asyncio
    async def test_update_entity(self, client, auth_headers):
        """Test updating an entity."""
        updates = {
            "attributes": {
                "description": "Updated description"
            }
        }
        
        response = client.put(
            "/api/v1/entities/test-guid-123",
            json=updates,
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
    
    @pytest.mark.asyncio
    async def test_delete_entity(self, client, auth_headers):
        """Test deleting an entity."""
        response = client.delete(
            "/api/v1/entities/test-guid-123",
            headers=auth_headers
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["success"] is True
    
    @pytest.mark.asyncio
    async def test_entity_not_found(self, client, auth_headers, mock_atlas_client):
        """Test handling of non-existent entity."""
        # Configure mock to return None
        mock_atlas_client.get_entity.return_value = None
        
        response = client.get(
            "/api/v1/entities/non-existent-guid",
            headers=auth_headers
        )
        
        assert response.status_code == 404
        data = response.json()
        assert "detail" in data
    
    @pytest.mark.asyncio
    async def test_invalid_entity_data(self, client, auth_headers):
        """Test validation of entity data."""
        invalid_entity = {
            "typeName": "",  # Invalid: empty type name
            "attributes": {}
        }
        
        response = client.post(
            "/api/v1/entities",
            json=invalid_entity,
            headers=auth_headers
        )
        
        assert response.status_code == 400
        data = response.json()
        assert "detail" in data 