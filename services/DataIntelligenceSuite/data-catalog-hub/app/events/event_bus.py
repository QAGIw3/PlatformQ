"""
Event Bus Implementation

Provides in-process event publishing and handling with async support.
"""

from typing import Dict, List, Callable, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime
import asyncio
import logging
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass
class DomainEvent:
    """Base class for all domain events"""
    event_id: str = field(default_factory=lambda: str(uuid4()))
    aggregate_id: str = ""
    event_type: str = ""
    occurred_at: datetime = field(default_factory=datetime.utcnow)
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.event_type:
            self.event_type = self.__class__.__name__


class EventBus:
    """
    In-process event bus for domain events.
    
    Supports both sync and async handlers.
    """
    
    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = {}
        self._middleware: List[Callable] = []
        
    def register_handler(
        self,
        event_type: str,
        handler: Callable,
        priority: int = 0
    ):
        """Register an event handler"""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
            
        # Insert handler based on priority
        self._handlers[event_type].append((priority, handler))
        self._handlers[event_type].sort(key=lambda x: x[0], reverse=True)
        
        logger.debug(f"Registered handler for {event_type}: {handler.__name__}")
        
    def register_middleware(self, middleware: Callable):
        """Register middleware to process all events"""
        self._middleware.append(middleware)
        
    async def publish(self, event: DomainEvent):
        """Publish an event to all registered handlers"""
        try:
            # Apply middleware
            for middleware in self._middleware:
                event = await self._call_handler(middleware, event)
                if event is None:
                    logger.debug(f"Event {event.event_id} filtered by middleware")
                    return
                    
            # Get handlers for this event type
            handlers = self._handlers.get(event.event_type, [])
            
            # Also get handlers for base event type
            if event.__class__.__bases__:
                base_type = event.__class__.__bases__[0].__name__
                handlers.extend(self._handlers.get(base_type, []))
                
            if not handlers:
                logger.debug(f"No handlers for event type: {event.event_type}")
                return
                
            # Execute handlers
            tasks = []
            for priority, handler in handlers:
                tasks.append(self._call_handler(handler, event))
                
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Log any exceptions
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    handler_name = handlers[i][1].__name__
                    logger.error(
                        f"Handler {handler_name} failed for event {event.event_id}: {result}"
                    )
                    
        except Exception as e:
            logger.error(f"Failed to publish event {event.event_id}: {e}")
            raise
            
    async def _call_handler(self, handler: Callable, event: DomainEvent):
        """Call a handler, handling both sync and async functions"""
        if asyncio.iscoroutinefunction(handler):
            return await handler(event)
        else:
            return handler(event)
            
    def clear_handlers(self, event_type: Optional[str] = None):
        """Clear handlers for testing"""
        if event_type:
            self._handlers.pop(event_type, None)
        else:
            self._handlers.clear()
            
    def get_handler_count(self, event_type: str) -> int:
        """Get number of handlers for an event type"""
        return len(self._handlers.get(event_type, [])) 