"""
CQRS (Command Query Responsibility Segregation) pattern implementation.

Provides separation of read and write operations with event sourcing support.
"""

import asyncio
import uuid
from typing import Any, Dict, List, Optional, Type, TypeVar, Generic, Callable, Union
from datetime import datetime
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import logging
from collections import defaultdict

from ...monitoring import StructuredLogger
from ..events import Event, EventBus

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')
TCommand = TypeVar('TCommand', bound='Command')
TQuery = TypeVar('TQuery', bound='Query')
TResult = TypeVar('TResult')


@dataclass
class Command(ABC):
    """Base class for commands"""
    command_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Get command name"""
        pass


@dataclass
class Query(ABC):
    """Base class for queries"""
    query_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Get query name"""
        pass


class CommandHandler(ABC, Generic[TCommand, TResult]):
    """Base class for command handlers"""
    
    @abstractmethod
    async def handle(self, command: TCommand) -> TResult:
        """Handle command and return result"""
        pass
        
    @property
    @abstractmethod
    def command_type(self) -> Type[TCommand]:
        """Get handled command type"""
        pass


class QueryHandler(ABC, Generic[TQuery, TResult]):
    """Base class for query handlers"""
    
    @abstractmethod
    async def handle(self, query: TQuery) -> TResult:
        """Handle query and return result"""
        pass
        
    @property
    @abstractmethod
    def query_type(self) -> Type[TQuery]:
        """Get handled query type"""
        pass


class CommandBus:
    """
    Command bus for routing commands to handlers.
    
    Features:
    - Command routing
    - Middleware support
    - Event publishing
    - Metrics collection
    """
    
    def __init__(self, event_bus: Optional[EventBus] = None):
        self.event_bus = event_bus
        self._handlers: Dict[Type[Command], CommandHandler] = {}
        self._middleware: List[Callable] = []
        
        self._metrics = {
            "commands_sent": 0,
            "commands_succeeded": 0,
            "commands_failed": 0,
            "commands_by_type": defaultdict(int)
        }
        
    def register_handler(
        self,
        command_type: Type[TCommand],
        handler: CommandHandler[TCommand, Any]
    ):
        """Register command handler"""
        if command_type in self._handlers:
            raise ValueError(f"Handler already registered for {command_type}")
            
        self._handlers[command_type] = handler
        logger.info(f"Registered handler for command {command_type.__name__}")
        
    def register_middleware(self, middleware: Callable):
        """Register middleware function"""
        self._middleware.append(middleware)
        
    async def send(self, command: TCommand) -> Any:
        """
        Send command to handler.
        
        Args:
            command: Command to send
            
        Returns:
            Command result
        """
        self._metrics["commands_sent"] += 1
        self._metrics["commands_by_type"][type(command).__name__] += 1
        
        # Get handler
        handler = self._handlers.get(type(command))
        if not handler:
            raise ValueError(f"No handler registered for {type(command)}")
            
        # Apply middleware
        for middleware in self._middleware:
            command = await self._apply_middleware(middleware, command)
            
        try:
            # Publish command received event
            await self._publish_event("command.received", {
                "command_id": command.command_id,
                "command_type": command.name,
                "timestamp": command.timestamp
            })
            
            # Execute handler
            result = await handler.handle(command)
            
            self._metrics["commands_succeeded"] += 1
            
            # Publish command completed event
            await self._publish_event("command.completed", {
                "command_id": command.command_id,
                "command_type": command.name,
                "result": result
            })
            
            return result
            
        except Exception as e:
            self._metrics["commands_failed"] += 1
            
            # Publish command failed event
            await self._publish_event("command.failed", {
                "command_id": command.command_id,
                "command_type": command.name,
                "error": str(e)
            })
            
            raise
            
    async def _apply_middleware(self, middleware: Callable, command: Command) -> Command:
        """Apply middleware to command"""
        if asyncio.iscoroutinefunction(middleware):
            return await middleware(command)
        else:
            return middleware(command)
            
    async def _publish_event(self, event_type: str, data: Any):
        """Publish event"""
        if self.event_bus:
            event = Event(
                type=event_type,
                data=data,
                timestamp=datetime.utcnow()
            )
            await self.event_bus.publish(event)
            
    def get_metrics(self) -> Dict[str, Any]:
        """Get command bus metrics"""
        return {
            **self._metrics,
            "commands_by_type": dict(self._metrics["commands_by_type"])
        }


class QueryBus:
    """
    Query bus for routing queries to handlers.
    
    Features:
    - Query routing
    - Result caching
    - Middleware support
    - Metrics collection
    """
    
    def __init__(self, cache_enabled: bool = True, cache_ttl: int = 300):
        self._handlers: Dict[Type[Query], QueryHandler] = {}
        self._middleware: List[Callable] = []
        self._cache_enabled = cache_enabled
        self._cache_ttl = cache_ttl
        self._cache: Dict[str, Tuple[Any, datetime]] = {}
        
        self._metrics = {
            "queries_sent": 0,
            "queries_succeeded": 0,
            "queries_failed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "queries_by_type": defaultdict(int)
        }
        
    def register_handler(
        self,
        query_type: Type[TQuery],
        handler: QueryHandler[TQuery, Any]
    ):
        """Register query handler"""
        if query_type in self._handlers:
            raise ValueError(f"Handler already registered for {query_type}")
            
        self._handlers[query_type] = handler
        logger.info(f"Registered handler for query {query_type.__name__}")
        
    def register_middleware(self, middleware: Callable):
        """Register middleware function"""
        self._middleware.append(middleware)
        
    async def send(self, query: TQuery, use_cache: bool = True) -> Any:
        """
        Send query to handler.
        
        Args:
            query: Query to send
            use_cache: Whether to use cache
            
        Returns:
            Query result
        """
        self._metrics["queries_sent"] += 1
        self._metrics["queries_by_type"][type(query).__name__] += 1
        
        # Check cache
        cache_key = self._get_cache_key(query)
        if self._cache_enabled and use_cache:
            cached_result = self._get_from_cache(cache_key)
            if cached_result is not None:
                self._metrics["cache_hits"] += 1
                return cached_result
            else:
                self._metrics["cache_misses"] += 1
                
        # Get handler
        handler = self._handlers.get(type(query))
        if not handler:
            raise ValueError(f"No handler registered for {type(query)}")
            
        # Apply middleware
        for middleware in self._middleware:
            query = await self._apply_middleware(middleware, query)
            
        try:
            # Execute handler
            result = await handler.handle(query)
            
            self._metrics["queries_succeeded"] += 1
            
            # Cache result
            if self._cache_enabled and use_cache:
                self._add_to_cache(cache_key, result)
                
            return result
            
        except Exception as e:
            self._metrics["queries_failed"] += 1
            raise
            
    async def _apply_middleware(self, middleware: Callable, query: Query) -> Query:
        """Apply middleware to query"""
        if asyncio.iscoroutinefunction(middleware):
            return await middleware(query)
        else:
            return middleware(query)
            
    def _get_cache_key(self, query: Query) -> str:
        """Generate cache key for query"""
        # Simple implementation - can be customized
        return f"{type(query).__name__}:{query.query_id}"
        
    def _get_from_cache(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        if key in self._cache:
            value, timestamp = self._cache[key]
            
            # Check if expired
            age = (datetime.utcnow() - timestamp).total_seconds()
            if age < self._cache_ttl:
                return value
            else:
                # Remove expired entry
                del self._cache[key]
                
        return None
        
    def _add_to_cache(self, key: str, value: Any):
        """Add value to cache"""
        self._cache[key] = (value, datetime.utcnow())
        
    def clear_cache(self):
        """Clear query cache"""
        self._cache.clear()
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get query bus metrics"""
        return {
            **self._metrics,
            "queries_by_type": dict(self._metrics["queries_by_type"]),
            "cache_size": len(self._cache)
        }


class CQRSMediator:
    """
    Mediator for CQRS pattern.
    
    Provides unified interface for sending commands and queries.
    """
    
    def __init__(
        self,
        command_bus: Optional[CommandBus] = None,
        query_bus: Optional[QueryBus] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.command_bus = command_bus or CommandBus(event_bus)
        self.query_bus = query_bus or QueryBus()
        self.event_bus = event_bus
        
    def register_command_handler(
        self,
        command_type: Type[TCommand],
        handler: CommandHandler[TCommand, Any]
    ):
        """Register command handler"""
        self.command_bus.register_handler(command_type, handler)
        
    def register_query_handler(
        self,
        query_type: Type[TQuery],
        handler: QueryHandler[TQuery, Any]
    ):
        """Register query handler"""
        self.query_bus.register_handler(query_type, handler)
        
    async def send_command(self, command: TCommand) -> Any:
        """Send command"""
        return await self.command_bus.send(command)
        
    async def send_query(self, query: TQuery, use_cache: bool = True) -> Any:
        """Send query"""
        return await self.query_bus.send(query, use_cache)
        
    def get_metrics(self) -> Dict[str, Any]:
        """Get mediator metrics"""
        return {
            "command_bus": self.command_bus.get_metrics(),
            "query_bus": self.query_bus.get_metrics()
        }


# Example implementations

@dataclass
class CreateEntityCommand(Command):
    """Example create entity command"""
    entity_type: str
    entity_data: Dict[str, Any]
    
    @property
    def name(self) -> str:
        return f"create_{self.entity_type}"


@dataclass
class UpdateEntityCommand(Command):
    """Example update entity command"""
    entity_type: str
    entity_id: str
    updates: Dict[str, Any]
    
    @property
    def name(self) -> str:
        return f"update_{self.entity_type}"


@dataclass
class DeleteEntityCommand(Command):
    """Example delete entity command"""
    entity_type: str
    entity_id: str
    
    @property
    def name(self) -> str:
        return f"delete_{self.entity_type}"


@dataclass
class GetEntityQuery(Query):
    """Example get entity query"""
    entity_type: str
    entity_id: str
    
    @property
    def name(self) -> str:
        return f"get_{self.entity_type}"


@dataclass
class ListEntitiesQuery(Query):
    """Example list entities query"""
    entity_type: str
    filters: Dict[str, Any] = field(default_factory=dict)
    limit: int = 100
    offset: int = 0
    
    @property
    def name(self) -> str:
        return f"list_{self.entity_type}"


class EntityCommandHandler(CommandHandler[CreateEntityCommand, str]):
    """Example entity command handler"""
    
    def __init__(self, repository):
        self.repository = repository
        
    @property
    def command_type(self) -> Type[CreateEntityCommand]:
        return CreateEntityCommand
        
    async def handle(self, command: CreateEntityCommand) -> str:
        """Handle create entity command"""
        # Create entity in repository
        entity_id = await self.repository.create(
            command.entity_type,
            command.entity_data
        )
        
        return entity_id


class EntityQueryHandler(QueryHandler[GetEntityQuery, Dict[str, Any]]):
    """Example entity query handler"""
    
    def __init__(self, repository):
        self.repository = repository
        
    @property
    def query_type(self) -> Type[GetEntityQuery]:
        return GetEntityQuery
        
    async def handle(self, query: GetEntityQuery) -> Dict[str, Any]:
        """Handle get entity query"""
        # Get entity from repository
        entity = await self.repository.get(
            query.entity_type,
            query.entity_id
        )
        
        return entity 