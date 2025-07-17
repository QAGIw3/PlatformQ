"""Repository pattern implementation for unified data access"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Type, Union, Generic, TypeVar
from datetime import datetime
from abc import ABC, abstractmethod

from .models import BaseModel
from .query import Query, QueryBuilder
from .cache import CacheManager, CacheKey
from .exceptions import NotFoundError, ValidationError, DataAccessError
from prometheus_client import Counter, Histogram

logger = logging.getLogger(__name__)

# Type variable for generic repository
T = TypeVar('T', bound=BaseModel)

# Prometheus metrics
data_operations = Counter('platformq_data_operations_total', 'Total data operations', ['operation', 'model', 'store'])
data_operation_duration = Histogram('platformq_data_operation_duration_seconds', 'Data operation duration', ['operation', 'model', 'store'])
data_operation_errors = Counter('platformq_data_operation_errors_total', 'Data operation errors', ['operation', 'model', 'store', 'error_type'])


class RepositoryInterface(ABC, Generic[T]):
    """Abstract base class for repository pattern"""
    
    @abstractmethod
    async def create(self, instance: T) -> T:
        """Create a new instance"""
        pass
    
    @abstractmethod
    async def get_by_id(self, id: Any) -> Optional[T]:
        """Get instance by ID"""
        pass
    
    @abstractmethod
    async def update(self, instance: T) -> T:
        """Update an instance"""
        pass
    
    @abstractmethod
    async def delete(self, id: Any) -> bool:
        """Delete an instance by ID"""
        pass
    
    @abstractmethod
    async def find(self, query: Query) -> List[T]:
        """Find instances matching query"""
        pass
    
    @abstractmethod
    async def count(self, query: Query) -> int:
        """Count instances matching query"""
        pass
    
    @abstractmethod
    def query(self) -> Query:
        """Create a new query builder"""
        pass


class Repository(RepositoryInterface[T]):
    """Base repository implementation with multi-store support"""
    
    def __init__(
        self,
        model_class: Type[T],
        store_adapters: Dict[str, Any],
        cache_manager: Optional[CacheManager] = None,
        tenant_id: Optional[str] = None
    ):
        self.model_class = model_class
        self.store_adapters = store_adapters
        self.cache_manager = cache_manager
        self.tenant_id = tenant_id
        self.query_builder = QueryBuilder()
        
        # Determine primary store based on model configuration
        self.primary_store = self._get_primary_store()
        
    def _get_primary_store(self) -> str:
        """Get the primary store for this model"""
        if hasattr(self.model_class, '__stores__') and self.model_class.__stores__:
            return self.model_class.__stores__[0]
        return 'cassandra'  # Default
    
    async def create(self, instance: T) -> T:
        """Create a new instance across configured stores"""
        with data_operation_duration.labels(operation='create', model=self.model_class.__name__, store=self.primary_store).time():
            try:
                # Validate instance
                instance.validate()
                
                # Add tenant ID if multi-tenant
                if self.tenant_id:
                    instance.tenant_id = self.tenant_id
                
                # Set auto fields
                self._set_auto_fields(instance, is_create=True)
                
                # Create in primary store first
                primary_adapter = self.store_adapters.get(self.primary_store)
                if not primary_adapter:
                    raise DataAccessError(f"Primary store {self.primary_store} not configured")
                
                created_instance = await primary_adapter.create(instance)
                
                # Create in secondary stores (async, best effort)
                secondary_tasks = []
                for store_name in self.model_class.__stores__[1:]:
                    if store_name in self.store_adapters:
                        task = self._create_in_secondary_store(store_name, created_instance)
                        secondary_tasks.append(task)
                
                if secondary_tasks:
                    await asyncio.gather(*secondary_tasks, return_exceptions=True)
                
                # Cache the created instance
                if self.cache_manager:
                    cache_key = self._build_cache_key('get_by_id', id=self._get_instance_id(created_instance))
                    await self.cache_manager.set(cache_key, created_instance, ttl=self.model_class.__cache_ttl__)
                
                # Update metrics
                data_operations.labels(operation='create', model=self.model_class.__name__, store=self.primary_store).inc()
                
                return created_instance
                
            except Exception as e:
                data_operation_errors.labels(
                    operation='create',
                    model=self.model_class.__name__,
                    store=self.primary_store,
                    error_type=type(e).__name__
                ).inc()
                logger.error(f"Failed to create {self.model_class.__name__}: {e}")
                raise
    
    async def get_by_id(self, id: Any) -> Optional[T]:
        """Get instance by ID with caching"""
        cache_key = self._build_cache_key('get_by_id', id=id)
        
        # Try cache first
        if self.cache_manager:
            cached = await self.cache_manager.get(cache_key)
            if cached:
                return cached
        
        with data_operation_duration.labels(operation='get_by_id', model=self.model_class.__name__, store=self.primary_store).time():
            try:
                # Get from primary store
                primary_adapter = self.store_adapters.get(self.primary_store)
                if not primary_adapter:
                    raise DataAccessError(f"Primary store {self.primary_store} not configured")
                
                instance = await primary_adapter.get_by_id(self.model_class, id, self.tenant_id)
                
                if instance:
                    # Cache the result
                    if self.cache_manager:
                        await self.cache_manager.set(cache_key, instance, ttl=self.model_class.__cache_ttl__)
                    
                    data_operations.labels(operation='get_by_id', model=self.model_class.__name__, store=self.primary_store).inc()
                
                return instance
                
            except Exception as e:
                data_operation_errors.labels(
                    operation='get_by_id',
                    model=self.model_class.__name__,
                    store=self.primary_store,
                    error_type=type(e).__name__
                ).inc()
                logger.error(f"Failed to get {self.model_class.__name__} by id {id}: {e}")
                raise
    
    async def update(self, instance: T) -> T:
        """Update an instance across stores"""
        with data_operation_duration.labels(operation='update', model=self.model_class.__name__, store=self.primary_store).time():
            try:
                # Validate instance
                instance.validate()
                
                # Set auto fields
                self._set_auto_fields(instance, is_create=False)
                
                # Update in primary store first
                primary_adapter = self.store_adapters.get(self.primary_store)
                if not primary_adapter:
                    raise DataAccessError(f"Primary store {self.primary_store} not configured")
                
                updated_instance = await primary_adapter.update(instance, self.tenant_id)
                
                # Update in secondary stores (async, best effort)
                secondary_tasks = []
                for store_name in self.model_class.__stores__[1:]:
                    if store_name in self.store_adapters:
                        task = self._update_in_secondary_store(store_name, updated_instance)
                        secondary_tasks.append(task)
                
                if secondary_tasks:
                    await asyncio.gather(*secondary_tasks, return_exceptions=True)
                
                # Invalidate cache
                if self.cache_manager:
                    instance_id = self._get_instance_id(updated_instance)
                    cache_key = self._build_cache_key('get_by_id', id=instance_id)
                    await self.cache_manager.delete(cache_key)
                    
                    # Invalidate query caches for this model
                    pattern = CacheKey.build_pattern(self.model_class.__name__)
                    await self.cache_manager.invalidate(pattern)
                
                data_operations.labels(operation='update', model=self.model_class.__name__, store=self.primary_store).inc()
                
                return updated_instance
                
            except Exception as e:
                data_operation_errors.labels(
                    operation='update',
                    model=self.model_class.__name__,
                    store=self.primary_store,
                    error_type=type(e).__name__
                ).inc()
                logger.error(f"Failed to update {self.model_class.__name__}: {e}")
                raise
    
    async def delete(self, id: Any) -> bool:
        """Delete an instance by ID"""
        with data_operation_duration.labels(operation='delete', model=self.model_class.__name__, store=self.primary_store).time():
            try:
                # Delete from primary store first
                primary_adapter = self.store_adapters.get(self.primary_store)
                if not primary_adapter:
                    raise DataAccessError(f"Primary store {self.primary_store} not configured")
                
                success = await primary_adapter.delete(self.model_class, id, self.tenant_id)
                
                if success:
                    # Delete from secondary stores (async, best effort)
                    secondary_tasks = []
                    for store_name in self.model_class.__stores__[1:]:
                        if store_name in self.store_adapters:
                            task = self._delete_from_secondary_store(store_name, id)
                            secondary_tasks.append(task)
                    
                    if secondary_tasks:
                        await asyncio.gather(*secondary_tasks, return_exceptions=True)
                    
                    # Invalidate cache
                    if self.cache_manager:
                        cache_key = self._build_cache_key('get_by_id', id=id)
                        await self.cache_manager.delete(cache_key)
                        
                        # Invalidate query caches
                        pattern = CacheKey.build_pattern(self.model_class.__name__)
                        await self.cache_manager.invalidate(pattern)
                
                data_operations.labels(operation='delete', model=self.model_class.__name__, store=self.primary_store).inc()
                
                return success
                
            except Exception as e:
                data_operation_errors.labels(
                    operation='delete',
                    model=self.model_class.__name__,
                    store=self.primary_store,
                    error_type=type(e).__name__
                ).inc()
                logger.error(f"Failed to delete {self.model_class.__name__} with id {id}: {e}")
                raise
    
    async def find(self, query: Query) -> List[T]:
        """Find instances matching query"""
        # Build cache key from query
        cache_key = self._build_cache_key('find', **query.to_dict())
        
        # Try cache first
        if self.cache_manager:
            cached = await self.cache_manager.get(cache_key)
            if cached:
                return cached
        
        with data_operation_duration.labels(operation='find', model=self.model_class.__name__, store=self.primary_store).time():
            try:
                # Add tenant filter if multi-tenant
                if self.tenant_id:
                    query.filter(tenant_id=self.tenant_id)
                
                # Execute query on primary store
                primary_adapter = self.store_adapters.get(self.primary_store)
                if not primary_adapter:
                    raise DataAccessError(f"Primary store {self.primary_store} not configured")
                
                results = await primary_adapter.find(query)
                
                # Cache results
                if self.cache_manager and results:
                    await self.cache_manager.set(cache_key, results, ttl=self.model_class.__cache_ttl__)
                
                data_operations.labels(operation='find', model=self.model_class.__name__, store=self.primary_store).inc()
                
                return results
                
            except Exception as e:
                data_operation_errors.labels(
                    operation='find',
                    model=self.model_class.__name__,
                    store=self.primary_store,
                    error_type=type(e).__name__
                ).inc()
                logger.error(f"Failed to find {self.model_class.__name__}: {e}")
                raise
    
    async def count(self, query: Query) -> int:
        """Count instances matching query"""
        # Build cache key from query
        cache_key = self._build_cache_key('count', **query.to_dict())
        
        # Try cache first
        if self.cache_manager:
            cached = await self.cache_manager.get(cache_key)
            if cached is not None:
                return cached
        
        with data_operation_duration.labels(operation='count', model=self.model_class.__name__, store=self.primary_store).time():
            try:
                # Add tenant filter if multi-tenant
                if self.tenant_id:
                    query.filter(tenant_id=self.tenant_id)
                
                # Execute count on primary store
                primary_adapter = self.store_adapters.get(self.primary_store)
                if not primary_adapter:
                    raise DataAccessError(f"Primary store {self.primary_store} not configured")
                
                count = await primary_adapter.count(query)
                
                # Cache result
                if self.cache_manager:
                    await self.cache_manager.set(cache_key, count, ttl=self.model_class.__cache_ttl__)
                
                data_operations.labels(operation='count', model=self.model_class.__name__, store=self.primary_store).inc()
                
                return count
                
            except Exception as e:
                data_operation_errors.labels(
                    operation='count',
                    model=self.model_class.__name__,
                    store=self.primary_store,
                    error_type=type(e).__name__
                ).inc()
                logger.error(f"Failed to count {self.model_class.__name__}: {e}")
                raise
    
    def query(self) -> Query:
        """Create a new query builder"""
        return Query(self.model_class)
    
    # Helper methods
    
    def _build_cache_key(self, operation: str, **params) -> str:
        """Build cache key for operation"""
        return CacheKey.build(self.model_class.__name__, operation, **params)
    
    def _get_instance_id(self, instance: T) -> Any:
        """Get instance ID"""
        pk_field = self.model_class.get_primary_key_field()
        if pk_field:
            return getattr(instance, pk_field.name)
        return None
    
    def _set_auto_fields(self, instance: T, is_create: bool):
        """Set auto-generated fields"""
        for field_name, field_def in instance._fields.items():
            if is_create and field_def.auto_now_add:
                setattr(instance, field_name, datetime.utcnow())
            elif field_def.auto_now:
                setattr(instance, field_name, datetime.utcnow())
    
    async def _create_in_secondary_store(self, store_name: str, instance: T):
        """Create instance in secondary store (best effort)"""
        try:
            adapter = self.store_adapters.get(store_name)
            if adapter:
                await adapter.create(instance)
        except Exception as e:
            logger.warning(f"Failed to create in secondary store {store_name}: {e}")
    
    async def _update_in_secondary_store(self, store_name: str, instance: T):
        """Update instance in secondary store (best effort)"""
        try:
            adapter = self.store_adapters.get(store_name)
            if adapter:
                await adapter.update(instance, self.tenant_id)
        except Exception as e:
            logger.warning(f"Failed to update in secondary store {store_name}: {e}")
    
    async def _delete_from_secondary_store(self, store_name: str, id: Any):
        """Delete from secondary store (best effort)"""
        try:
            adapter = self.store_adapters.get(store_name)
            if adapter:
                await adapter.delete(self.model_class, id, self.tenant_id)
        except Exception as e:
            logger.warning(f"Failed to delete from secondary store {store_name}: {e}")
    
    # Convenience methods
    
    async def find_one(self, **kwargs) -> Optional[T]:
        """Find a single instance matching criteria"""
        query = self.query().filter(**kwargs).limit(1)
        results = await self.find(query)
        return results[0] if results else None
    
    async def find_by(self, **kwargs) -> List[T]:
        """Find all instances matching criteria"""
        query = self.query().filter(**kwargs)
        return await self.find(query)
    
    async def exists(self, **kwargs) -> bool:
        """Check if any instance exists matching criteria"""
        query = self.query().filter(**kwargs)
        count = await self.count(query)
        return count > 0
    
    async def bulk_create(self, instances: List[T]) -> List[T]:
        """Create multiple instances"""
        created = []
        for instance in instances:
            try:
                created_instance = await self.create(instance)
                created.append(created_instance)
            except Exception as e:
                logger.error(f"Failed to create instance in bulk: {e}")
                # Decide whether to continue or rollback
        return created
    
    async def bulk_update(self, instances: List[T]) -> List[T]:
        """Update multiple instances"""
        updated = []
        for instance in instances:
            try:
                updated_instance = await self.update(instance)
                updated.append(updated_instance)
            except Exception as e:
                logger.error(f"Failed to update instance in bulk: {e}")
        return updated
    
    async def bulk_delete(self, ids: List[Any]) -> int:
        """Delete multiple instances by IDs"""
        deleted_count = 0
        for id in ids:
            try:
                if await self.delete(id):
                    deleted_count += 1
            except Exception as e:
                logger.error(f"Failed to delete instance {id} in bulk: {e}")
        return deleted_count


class AsyncRepository(Repository[T]):
    """Async-specific repository implementation"""
    
    async def transaction(self, operations: List[callable]):
        """Execute multiple operations in a transaction (where supported)"""
        # This would implement transaction support for stores that support it
        # For now, just execute operations sequentially
        results = []
        for operation in operations:
            result = await operation()
            results.append(result)
        return results
    
    async def paginate(self, page: int = 1, per_page: int = 20, **filters) -> Dict[str, Any]:
        """Paginate results"""
        offset = (page - 1) * per_page
        
        # Build query
        query = self.query().filter(**filters).limit(per_page).offset(offset)
        
        # Get results and total count in parallel
        results_task = self.find(query)
        count_query = self.query().filter(**filters)
        count_task = self.count(count_query)
        
        results, total = await asyncio.gather(results_task, count_task)
        
        return {
            'items': results,
            'page': page,
            'per_page': per_page,
            'total': total,
            'pages': (total + per_page - 1) // per_page
        } 