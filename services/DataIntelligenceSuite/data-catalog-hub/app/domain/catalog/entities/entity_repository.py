"""
Entity Repository Interface

Defines the contract for entity persistence.
"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime

from .entity import Entity
from .entity_specification import EntitySpecification


class EntityRepository(ABC):
    """
    Abstract repository interface for entity persistence.
    
    This interface defines the contract that infrastructure
    implementations must fulfill.
    """
    
    @abstractmethod
    async def save(self, entity: Entity) -> Entity:
        """
        Save an entity (create or update).
        
        Args:
            entity: The entity to save
            
        Returns:
            The saved entity with updated metadata
        """
        pass
    
    @abstractmethod
    async def find_by_id(self, guid: str) -> Optional[Entity]:
        """
        Find an entity by its GUID.
        
        Args:
            guid: The entity GUID
            
        Returns:
            The entity if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def find_by_qualified_name(
        self,
        qualified_name: str,
        type_name: str
    ) -> Optional[Entity]:
        """
        Find an entity by qualified name and type.
        
        Args:
            qualified_name: The qualified name
            type_name: The entity type
            
        Returns:
            The entity if found, None otherwise
        """
        pass
    
    @abstractmethod
    async def find_by_specification(
        self,
        specification: EntitySpecification,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[Entity], int]:
        """
        Find entities matching a specification.
        
        Args:
            specification: The search specification
            limit: Maximum results to return
            offset: Result offset for pagination
            
        Returns:
            Tuple of (entities, total_count)
        """
        pass
    
    @abstractmethod
    async def find_all(
        self,
        type_name: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[Entity], int]:
        """
        Find all entities, optionally filtered by type.
        
        Args:
            type_name: Optional type filter
            limit: Maximum results to return
            offset: Result offset for pagination
            
        Returns:
            Tuple of (entities, total_count)
        """
        pass
    
    @abstractmethod
    async def delete(self, guid: str, soft: bool = True) -> bool:
        """
        Delete an entity.
        
        Args:
            guid: The entity GUID
            soft: Whether to soft delete (default) or hard delete
            
        Returns:
            True if deleted, False if not found
        """
        pass
    
    @abstractmethod
    async def exists(self, guid: str) -> bool:
        """
        Check if an entity exists.
        
        Args:
            guid: The entity GUID
            
        Returns:
            True if exists, False otherwise
        """
        pass
    
    @abstractmethod
    async def count(
        self,
        type_name: Optional[str] = None,
        include_deleted: bool = False
    ) -> int:
        """
        Count entities.
        
        Args:
            type_name: Optional type filter
            include_deleted: Whether to include deleted entities
            
        Returns:
            The count of entities
        """
        pass
    
    @abstractmethod
    async def bulk_save(self, entities: List[Entity]) -> List[Entity]:
        """
        Save multiple entities in a batch.
        
        Args:
            entities: List of entities to save
            
        Returns:
            List of saved entities
        """
        pass
    
    @abstractmethod
    async def search(
        self,
        query: str,
        type_name: Optional[str] = None,
        limit: int = 20,
        offset: int = 0
    ) -> Tuple[List[Entity], int]:
        """
        Full-text search for entities.
        
        Args:
            query: Search query
            type_name: Optional type filter
            limit: Maximum results
            offset: Result offset
            
        Returns:
            Tuple of (entities, total_count)
        """
        pass
    
    @abstractmethod
    async def get_recently_updated(
        self,
        since: datetime,
        type_name: Optional[str] = None,
        limit: int = 100
    ) -> List[Entity]:
        """
        Get recently updated entities.
        
        Args:
            since: Get entities updated after this time
            type_name: Optional type filter
            limit: Maximum results
            
        Returns:
            List of recently updated entities
        """
        pass 