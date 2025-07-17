"""Base adapter interface for store-specific implementations"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type
from ..models import BaseModel
from ..query import Query


class StoreAdapter(ABC):
    """Abstract base class for store adapters"""
    
    @abstractmethod
    async def create(self, instance: BaseModel) -> BaseModel:
        """Create a new instance in the store"""
        pass
    
    @abstractmethod
    async def get_by_id(self, model_class: Type[BaseModel], id: Any, tenant_id: Optional[str] = None) -> Optional[BaseModel]:
        """Get instance by ID from the store"""
        pass
    
    @abstractmethod
    async def update(self, instance: BaseModel, tenant_id: Optional[str] = None) -> BaseModel:
        """Update an instance in the store"""
        pass
    
    @abstractmethod
    async def delete(self, model_class: Type[BaseModel], id: Any, tenant_id: Optional[str] = None) -> bool:
        """Delete an instance from the store"""
        pass
    
    @abstractmethod
    async def find(self, query: Query) -> List[BaseModel]:
        """Find instances matching query"""
        pass
    
    @abstractmethod
    async def count(self, query: Query) -> int:
        """Count instances matching query"""
        pass
    
    @abstractmethod
    async def execute_raw(self, query: str, params: Optional[List[Any]] = None) -> Any:
        """Execute raw query specific to this store"""
        pass
    
    async def bulk_create(self, instances: List[BaseModel]) -> List[BaseModel]:
        """Bulk create instances (default implementation)"""
        results = []
        for instance in instances:
            result = await self.create(instance)
            results.append(result)
        return results
    
    async def bulk_update(self, instances: List[BaseModel], tenant_id: Optional[str] = None) -> List[BaseModel]:
        """Bulk update instances (default implementation)"""
        results = []
        for instance in instances:
            result = await self.update(instance, tenant_id)
            results.append(result)
        return results
    
    async def bulk_delete(self, model_class: Type[BaseModel], ids: List[Any], tenant_id: Optional[str] = None) -> int:
        """Bulk delete instances (default implementation)"""
        count = 0
        for id in ids:
            if await self.delete(model_class, id, tenant_id):
                count += 1
        return count 