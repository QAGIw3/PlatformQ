"""
Test Entity Service

Unit tests for the EntityService with the new architecture.
"""

import pytest
from unittest.mock import AsyncMock, Mock, call
from datetime import datetime

from app.services.catalog import EntityService
from app.domain.models import Entity
from app.domain.events import EntityCreated, EntityUpdated, EntityDeleted
from app.services.interfaces import ServiceResult


class TestEntityService:
    """Test entity service business logic."""
    
    @pytest.fixture
    def entity_service(self, mock_atlas_client, test_container):
        """Create entity service with mocked dependencies."""
        # Create repository mock
        mock_repository = AsyncMock()
        
        # Get event bus from container
        event_bus = test_container.event_bus()
        
        # Create service
        service = EntityService(
            repository=mock_repository,
            event_bus=event_bus
        )
        
        # Attach repository to service for test access
        service._test_repository = mock_repository
        
        return service
    
    @pytest.mark.asyncio
    async def test_create_entity_success(self, entity_service):
        """Test successful entity creation."""
        # Arrange
        entity_data = {
            "typeName": "test_table",
            "attributes": {
                "name": "test_table",
                "qualifiedName": "test.test_table"
            }
        }
        
        # Configure mock
        created_entity = Entity(
            guid="new-guid-123",
            typeName="test_table",
            attributes=entity_data["attributes"],
            createdBy="test-user",
            createdTime=datetime.utcnow()
        )
        entity_service._test_repository.create.return_value = created_entity
        
        # Act
        result = await entity_service.create(
            type_name=entity_data["typeName"],
            attributes=entity_data["attributes"],
            created_by="test-user"
        )
        
        # Assert
        assert result.success
        assert result.data.guid == "new-guid-123"
        assert result.data.typeName == "test_table"
        
        # Verify event was emitted
        entity_service.event_bus.emit.assert_called_once()
        emitted_event = entity_service.event_bus.emit.call_args[0][1]
        assert isinstance(emitted_event, EntityCreated)
        assert emitted_event.entity_id == "new-guid-123"
    
    @pytest.mark.asyncio
    async def test_create_entity_validation_failure(self, entity_service):
        """Test entity creation with invalid data."""
        # Act
        result = await entity_service.create(
            type_name="",  # Invalid: empty type name
            attributes={},
            created_by="test-user"
        )
        
        # Assert
        assert not result.success
        assert "validation" in result.error.lower()
        
        # Verify no repository call was made
        entity_service._test_repository.create.assert_not_called()
        
        # Verify no event was emitted
        entity_service.event_bus.emit.assert_not_called()
    
    @pytest.mark.asyncio
    async def test_get_entity_success(self, entity_service):
        """Test retrieving an entity."""
        # Arrange
        entity = Entity(
            guid="test-guid",
            typeName="test_table",
            attributes={"name": "test"},
            createdBy="user1",
            createdTime=datetime.utcnow()
        )
        entity_service._test_repository.get.return_value = entity
        
        # Act
        result = await entity_service.get("test-guid")
        
        # Assert
        assert result.success
        assert result.data.guid == "test-guid"
        assert result.data.typeName == "test_table"
    
    @pytest.mark.asyncio
    async def test_get_entity_not_found(self, entity_service):
        """Test retrieving non-existent entity."""
        # Arrange
        entity_service._test_repository.get.return_value = None
        
        # Act
        result = await entity_service.get("non-existent")
        
        # Assert
        assert not result.success
        assert "not found" in result.error.lower()
    
    @pytest.mark.asyncio
    async def test_update_entity_success(self, entity_service):
        """Test updating an entity."""
        # Arrange
        existing_entity = Entity(
            guid="test-guid",
            typeName="test_table",
            attributes={"name": "old_name"},
            createdBy="user1",
            createdTime=datetime.utcnow()
        )
        
        updated_entity = Entity(
            guid="test-guid",
            typeName="test_table",
            attributes={"name": "new_name"},
            createdBy="user1",
            createdTime=existing_entity.createdTime,
            updatedBy="user2",
            updatedTime=datetime.utcnow()
        )
        
        entity_service._test_repository.get.return_value = existing_entity
        entity_service._test_repository.update.return_value = updated_entity
        
        # Act
        result = await entity_service.update(
            guid="test-guid",
            attributes={"name": "new_name"},
            updated_by="user2"
        )
        
        # Assert
        assert result.success
        assert result.data.attributes["name"] == "new_name"
        
        # Verify event was emitted
        entity_service.event_bus.emit.assert_called_once()
        emitted_event = entity_service.event_bus.emit.call_args[0][1]
        assert isinstance(emitted_event, EntityUpdated)
        assert emitted_event.entity_id == "test-guid"
    
    @pytest.mark.asyncio
    async def test_delete_entity_success(self, entity_service):
        """Test deleting an entity."""
        # Arrange
        entity = Entity(
            guid="test-guid",
            typeName="test_table",
            attributes={"name": "test"},
            createdBy="user1",
            createdTime=datetime.utcnow()
        )
        entity_service._test_repository.get.return_value = entity
        entity_service._test_repository.delete.return_value = True
        
        # Act
        result = await entity_service.delete("test-guid", deleted_by="user2")
        
        # Assert
        assert result.success
        assert result.data is True
        
        # Verify event was emitted
        entity_service.event_bus.emit.assert_called_once()
        emitted_event = entity_service.event_bus.emit.call_args[0][1]
        assert isinstance(emitted_event, EntityDeleted)
        assert emitted_event.entity_id == "test-guid"
    
    @pytest.mark.asyncio
    async def test_search_entities(self, entity_service):
        """Test searching for entities."""
        # Arrange
        search_results = [
            Entity(
                guid="guid1",
                typeName="table",
                attributes={"name": "table1"},
                createdBy="user1",
                createdTime=datetime.utcnow()
            ),
            Entity(
                guid="guid2",
                typeName="table",
                attributes={"name": "table2"},
                createdBy="user1",
                createdTime=datetime.utcnow()
            )
        ]
        
        entity_service._test_repository.search.return_value = (search_results, 2)
        
        # Act
        result = await entity_service.search(
            query="table",
            filters={"typeName": "table"},
            limit=10,
            offset=0
        )
        
        # Assert
        assert result.success
        entities, total = result.data
        assert len(entities) == 2
        assert total == 2
        assert entities[0].guid == "guid1"
        assert entities[1].guid == "guid2"
    
    @pytest.mark.asyncio
    async def test_bulk_create_entities(self, entity_service):
        """Test bulk entity creation."""
        # Arrange
        entities_data = [
            {
                "typeName": "table",
                "attributes": {"name": "table1"}
            },
            {
                "typeName": "table",
                "attributes": {"name": "table2"}
            }
        ]
        
        created_entities = [
            Entity(
                guid=f"guid{i}",
                typeName=data["typeName"],
                attributes=data["attributes"],
                createdBy="bulk-user",
                createdTime=datetime.utcnow()
            )
            for i, data in enumerate(entities_data, 1)
        ]
        
        entity_service._test_repository.bulk_create.return_value = created_entities
        
        # Act
        result = await entity_service.bulk_create(
            entities=entities_data,
            created_by="bulk-user"
        )
        
        # Assert
        assert result.success
        assert len(result.data) == 2
        
        # Verify events were emitted for each entity
        assert entity_service.event_bus.emit.call_count == 2
        
        # Check that EntityCreated events were emitted for both entities
        emitted_calls = entity_service.event_bus.emit.call_args_list
        for i, call_args in enumerate(emitted_calls):
            event = call_args[0][1]
            assert isinstance(event, EntityCreated)
            assert event.entity_id == f"guid{i+1}"
    
    @pytest.mark.asyncio
    async def test_error_handling(self, entity_service):
        """Test service error handling."""
        # Arrange
        entity_service._test_repository.create.side_effect = Exception("Database error")
        
        # Act
        result = await entity_service.create(
            type_name="test_table",
            attributes={"name": "test"},
            created_by="user1"
        )
        
        # Assert
        assert not result.success
        assert "error" in result.error.lower()
        assert result.details is not None
        
        # Verify no event was emitted on error
        entity_service.event_bus.emit.assert_not_called() 