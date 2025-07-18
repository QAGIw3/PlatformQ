"""
Enhanced Repository Pattern for PlatformQ

Provides base repository classes with common CRUD operations,
query builders, and batch operations.
"""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar, List, Optional, Dict, Type, Union
from datetime import datetime
import logging
import asyncio

from sqlalchemy import and_, or_, func, desc, asc
from sqlalchemy.orm import Session, Query
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic import BaseModel

logger = logging.getLogger(__name__)

T = TypeVar("T")
CreateSchema = TypeVar("CreateSchema", bound=BaseModel)
UpdateSchema = TypeVar("UpdateSchema", bound=BaseModel)


class QueryBuilder(Generic[T]):
    """Fluent query builder for repositories"""
    
    def __init__(self, query: Query, model_class: Type[T]):
        self._query = query
        self._model_class = model_class
        
    def filter(self, **kwargs) -> 'QueryBuilder[T]':
        """Add filter conditions"""
        conditions = []
        for key, value in kwargs.items():
            if hasattr(self._model_class, key):
                attr = getattr(self._model_class, key)
                if isinstance(value, list):
                    conditions.append(attr.in_(value))
                elif value is None:
                    conditions.append(attr.is_(None))
                else:
                    conditions.append(attr == value)
        
        if conditions:
            self._query = self._query.filter(and_(*conditions))
        return self
        
    def filter_by_range(self, field: str, min_value: Any = None, max_value: Any = None) -> 'QueryBuilder[T]':
        """Filter by range"""
        if hasattr(self._model_class, field):
            attr = getattr(self._model_class, field)
            if min_value is not None:
                self._query = self._query.filter(attr >= min_value)
            if max_value is not None:
                self._query = self._query.filter(attr <= max_value)
        return self
        
    def order_by(self, field: str, descending: bool = False) -> 'QueryBuilder[T]':
        """Add ordering"""
        if hasattr(self._model_class, field):
            attr = getattr(self._model_class, field)
            self._query = self._query.order_by(desc(attr) if descending else asc(attr))
        return self
        
    def limit(self, limit: int) -> 'QueryBuilder[T]':
        """Limit results"""
        self._query = self._query.limit(limit)
        return self
        
    def offset(self, offset: int) -> 'QueryBuilder[T]':
        """Offset results"""
        self._query = self._query.offset(offset)
        return self
        
    def paginate(self, page: int, page_size: int) -> 'QueryBuilder[T]':
        """Paginate results"""
        self._query = self._query.limit(page_size).offset((page - 1) * page_size)
        return self
        
    def all(self) -> List[T]:
        """Execute query and return all results"""
        return self._query.all()
        
    def first(self) -> Optional[T]:
        """Execute query and return first result"""
        return self._query.first()
        
    def one(self) -> T:
        """Execute query and return exactly one result"""
        return self._query.one()
        
    def one_or_none(self) -> Optional[T]:
        """Execute query and return one result or None"""
        return self._query.one_or_none()
        
    def count(self) -> int:
        """Return count of results"""
        return self._query.count()
        
    def exists(self) -> bool:
        """Check if any results exist"""
        return self._query.first() is not None


class AbstractRepository(ABC, Generic[T]):
    """Abstract base repository with common operations"""
    
    @abstractmethod
    def add(self, entity: T) -> T:
        raise NotImplementedError

    @abstractmethod
    def get(self, id: Any) -> Optional[T]:
        raise NotImplementedError

    @abstractmethod
    def list(self) -> List[T]:
        raise NotImplementedError

    @abstractmethod
    def update(self, entity: T) -> T:
        raise NotImplementedError

    @abstractmethod
    def delete(self, id: Any) -> None:
        raise NotImplementedError


class BaseRepository(AbstractRepository[T], Generic[T, CreateSchema, UpdateSchema]):
    """
    Base repository implementation for SQLAlchemy models.
    
    Provides common CRUD operations, query builder, and batch operations.
    """
    
    def __init__(self, session: Session, model_class: Type[T]):
        self.session = session
        self.model_class = model_class
        
    def add(self, entity: Union[T, CreateSchema], **kwargs) -> T:
        """Add a new entity"""
        if isinstance(entity, BaseModel):
            # Convert Pydantic model to SQLAlchemy model
            db_entity = self.model_class(**entity.dict(), **kwargs)
        else:
            db_entity = entity
            
        self.session.add(db_entity)
        self.session.commit()
        self.session.refresh(db_entity)
        return db_entity
        
    def get(self, id: Any) -> Optional[T]:
        """Get entity by ID"""
        return self.session.query(self.model_class).filter(
            self.model_class.id == id
        ).first()
        
    def get_by(self, **kwargs) -> Optional[T]:
        """Get entity by arbitrary fields"""
        return self.query().filter(**kwargs).first()
        
    def list(self, skip: int = 0, limit: int = 100) -> List[T]:
        """List all entities with pagination"""
        return self.session.query(self.model_class).offset(skip).limit(limit).all()
        
    def update(self, id: Any, entity: Union[T, UpdateSchema, Dict[str, Any]]) -> Optional[T]:
        """Update an entity"""
        db_entity = self.get(id)
        if not db_entity:
            return None
            
        if isinstance(entity, BaseModel):
            update_data = entity.dict(exclude_unset=True)
        elif isinstance(entity, dict):
            update_data = entity
        else:
            # Assume it's a model instance
            update_data = {c.name: getattr(entity, c.name) 
                          for c in entity.__table__.columns
                          if hasattr(entity, c.name)}
            
        for key, value in update_data.items():
            if hasattr(db_entity, key):
                setattr(db_entity, key, value)
                
        self.session.commit()
        self.session.refresh(db_entity)
        return db_entity
        
    def delete(self, id: Any) -> bool:
        """Delete an entity"""
        db_entity = self.get(id)
        if db_entity:
            self.session.delete(db_entity)
            self.session.commit()
            return True
        return False
        
    def query(self) -> QueryBuilder[T]:
        """Get a query builder"""
        return QueryBuilder(self.session.query(self.model_class), self.model_class)
        
    def count(self, **kwargs) -> int:
        """Count entities matching criteria"""
        return self.query().filter(**kwargs).count()
        
    def exists(self, **kwargs) -> bool:
        """Check if entity exists"""
        return self.query().filter(**kwargs).exists()
        
    def bulk_insert(self, entities: List[Union[T, CreateSchema]]) -> List[T]:
        """Bulk insert entities"""
        db_entities = []
        for entity in entities:
            if isinstance(entity, BaseModel):
                db_entity = self.model_class(**entity.dict())
            else:
                db_entity = entity
            db_entities.append(db_entity)
            
        self.session.bulk_save_objects(db_entities, return_defaults=True)
        self.session.commit()
        return db_entities
        
    def bulk_update(self, updates: List[Dict[str, Any]]) -> int:
        """Bulk update entities"""
        count = 0
        for update in updates:
            id_value = update.pop('id', None)
            if id_value:
                result = self.session.query(self.model_class).filter(
                    self.model_class.id == id_value
                ).update(update)
                count += result
                
        self.session.commit()
        return count
        
    def find_by_ids(self, ids: List[Any]) -> List[T]:
        """Find multiple entities by IDs"""
        return self.session.query(self.model_class).filter(
            self.model_class.id.in_(ids)
        ).all()
        
    def refresh(self, entity: T) -> T:
        """Refresh entity from database"""
        self.session.refresh(entity)
        return entity
        
    def expunge(self, entity: T) -> None:
        """Remove entity from session"""
        self.session.expunge(entity)


class AsyncBaseRepository(AbstractRepository[T], Generic[T, CreateSchema, UpdateSchema]):
    """
    Async base repository implementation for SQLAlchemy models.
    """
    
    def __init__(self, session: AsyncSession, model_class: Type[T]):
        self.session = session
        self.model_class = model_class
        
    async def add(self, entity: Union[T, CreateSchema], **kwargs) -> T:
        """Add a new entity"""
        if isinstance(entity, BaseModel):
            db_entity = self.model_class(**entity.dict(), **kwargs)
        else:
            db_entity = entity
            
        self.session.add(db_entity)
        await self.session.commit()
        await self.session.refresh(db_entity)
        return db_entity
        
    async def get(self, id: Any) -> Optional[T]:
        """Get entity by ID"""
        result = await self.session.execute(
            self.session.query(self.model_class).filter(
                self.model_class.id == id
            )
        )
        return result.scalar_one_or_none()
        
    async def list(self, skip: int = 0, limit: int = 100) -> List[T]:
        """List all entities with pagination"""
        result = await self.session.execute(
            self.session.query(self.model_class).offset(skip).limit(limit)
        )
        return result.scalars().all()
        
    async def update(self, id: Any, entity: Union[T, UpdateSchema, Dict[str, Any]]) -> Optional[T]:
        """Update an entity"""
        db_entity = await self.get(id)
        if not db_entity:
            return None
            
        if isinstance(entity, BaseModel):
            update_data = entity.dict(exclude_unset=True)
        elif isinstance(entity, dict):
            update_data = entity
        else:
            update_data = {c.name: getattr(entity, c.name) 
                          for c in entity.__table__.columns
                          if hasattr(entity, c.name)}
            
        for key, value in update_data.items():
            if hasattr(db_entity, key):
                setattr(db_entity, key, value)
                
        await self.session.commit()
        await self.session.refresh(db_entity)
        return db_entity
        
    async def delete(self, id: Any) -> bool:
        """Delete an entity"""
        db_entity = await self.get(id)
        if db_entity:
            await self.session.delete(db_entity)
            await self.session.commit()
            return True
        return False


class CassandraRepository(Generic[T]):
    """
    Base repository for Cassandra models.
    """
    
    def __init__(self, session: Session, model_class: Type[T], keyspace: str):
        self.session = session
        self.model_class = model_class
        self.keyspace = keyspace
        self.table_name = model_class.__table_name__
        
    def add(self, entity: T) -> T:
        """Add entity to Cassandra"""
        # Implementation depends on Cassandra driver used
        pass
        
    def get(self, **primary_keys) -> Optional[T]:
        """Get entity by primary keys"""
        # Implementation depends on Cassandra driver used  
        pass
        
    def list(self, limit: int = 100) -> List[T]:
        """List entities"""
        # Implementation depends on Cassandra driver used
        pass
        
    def update(self, entity: T) -> T:
        """Update entity"""
        # Implementation depends on Cassandra driver used
        pass
        
    def delete(self, **primary_keys) -> bool:
        """Delete entity"""
        # Implementation depends on Cassandra driver used
        pass 