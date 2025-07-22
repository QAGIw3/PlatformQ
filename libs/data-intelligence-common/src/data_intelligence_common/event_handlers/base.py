"""Base event processing for DataIntelligenceSuite services."""

from typing import Dict, Any, Callable, List, Optional, Set
from dataclasses import dataclass
from datetime import datetime
from abc import ABC, abstractmethod
import asyncio
import logging
import json

from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


@dataclass
class EventHandler:
    """Event handler registration."""
    
    event_type: str
    handler_func: Callable
    filter_func: Optional[Callable] = None
    priority: int = 0  # Higher priority handlers run first
    
    def __post_init__(self):
        if not asyncio.iscoroutinefunction(self.handler_func):
            raise ValueError(f"Handler function must be async: {self.handler_func.__name__}")


class EventRouter:
    """Routes events to appropriate handlers."""
    
    def __init__(self):
        self.handlers: Dict[str, List[EventHandler]] = {}
        self._handler_metrics: Dict[str, Dict[str, int]] = {}
        
    def register_handler(
        self,
        event_type: str,
        handler_func: Callable,
        filter_func: Optional[Callable] = None,
        priority: int = 0
    ):
        """Register an event handler."""
        handler = EventHandler(
            event_type=event_type,
            handler_func=handler_func,
            filter_func=filter_func,
            priority=priority
        )
        
        if event_type not in self.handlers:
            self.handlers[event_type] = []
            
        self.handlers[event_type].append(handler)
        
        # Sort by priority (descending)
        self.handlers[event_type].sort(key=lambda h: h.priority, reverse=True)
        
        logger.info(f"Registered handler for {event_type}: {handler_func.__name__}")
        
    def unregister_handler(self, event_type: str, handler_func: Callable):
        """Unregister an event handler."""
        if event_type in self.handlers:
            self.handlers[event_type] = [
                h for h in self.handlers[event_type]
                if h.handler_func != handler_func
            ]
            
    async def route_event(self, event_type: str, event_data: Dict[str, Any]) -> List[Any]:
        """Route event to appropriate handlers."""
        if event_type not in self.handlers:
            logger.debug(f"No handlers registered for event type: {event_type}")
            return []
            
        results = []
        handlers = self.handlers[event_type]
        
        for handler in handlers:
            try:
                # Apply filter if provided
                if handler.filter_func and not handler.filter_func(event_data):
                    continue
                    
                # Call handler
                result = await handler.handler_func(event_data)
                results.append(result)
                
                # Track metrics
                self._track_handler_success(event_type, handler.handler_func.__name__)
                
            except Exception as e:
                logger.error(
                    f"Error in event handler",
                    extra={
                        "event_type": event_type,
                        "handler": handler.handler_func.__name__,
                        "error": str(e)
                    }
                )
                self._track_handler_error(event_type, handler.handler_func.__name__)
                
        return results
        
    def _track_handler_success(self, event_type: str, handler_name: str):
        """Track successful handler execution."""
        key = f"{event_type}:{handler_name}"
        if key not in self._handler_metrics:
            self._handler_metrics[key] = {"success": 0, "error": 0}
        self._handler_metrics[key]["success"] += 1
        
    def _track_handler_error(self, event_type: str, handler_name: str):
        """Track handler error."""
        key = f"{event_type}:{handler_name}"
        if key not in self._handler_metrics:
            self._handler_metrics[key] = {"success": 0, "error": 0}
        self._handler_metrics[key]["error"] += 1
        
    def get_handler_metrics(self) -> Dict[str, Dict[str, int]]:
        """Get handler execution metrics."""
        return self._handler_metrics.copy()


class BaseEventProcessor(ABC):
    """
    Base event processor for DataIntelligenceSuite services.
    
    Features:
    - Event subscription and processing
    - Event routing
    - Error handling and retries
    - Metrics collection
    - Dead letter queue support
    """
    
    def __init__(
        self,
        service_name: str,
        event_topics: List[str],
        event_publisher: Optional[EventPublisher] = None
    ):
        self.service_name = service_name
        self.event_topics = event_topics
        self.event_publisher = event_publisher
        self.event_router = EventRouter()
        
        # Processing state
        self._running = False
        self._processing_tasks: List[asyncio.Task] = []
        
        # Metrics
        self._events_processed = 0
        self._events_failed = 0
        
        # Dead letter queue
        self._dlq_topic = f"dlq-{service_name}"
        
    async def start(self):
        """Start event processing."""
        if self._running:
            logger.warning("Event processor already running")
            return
            
        self._running = True
        
        # Register handlers
        await self.register_handlers()
        
        # Start processing for each topic
        for topic in self.event_topics:
            task = asyncio.create_task(self._process_topic(topic))
            self._processing_tasks.append(task)
            
        logger.info(f"Started event processor for {self.service_name}")
        
    async def stop(self):
        """Stop event processing."""
        self._running = False
        
        # Cancel processing tasks
        for task in self._processing_tasks:
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self._processing_tasks, return_exceptions=True)
        
        logger.info(f"Stopped event processor for {self.service_name}")
        
    @abstractmethod
    async def register_handlers(self):
        """Register event handlers. Must be implemented by derived classes."""
        pass
        
    async def _process_topic(self, topic: str):
        """Process events from a topic."""
        logger.info(f"Processing events from topic: {topic}")
        
        while self._running:
            try:
                # This is a placeholder - actual implementation would use Pulsar client
                # In production, this would subscribe to the topic and process messages
                await asyncio.sleep(1)
                
                # Simulate event processing
                # event = await self._consume_event(topic)
                # if event:
                #     await self._process_event(event)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing topic {topic}: {e}")
                await asyncio.sleep(5)
                
    async def _process_event(self, event: Dict[str, Any]):
        """Process a single event."""
        event_type = event.get("type", "unknown")
        event_id = event.get("id", "unknown")
        
        try:
            # Route event to handlers
            start_time = datetime.utcnow()
            results = await self.event_router.route_event(event_type, event)
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            self._events_processed += 1
            
            logger.info(
                f"Processed event",
                extra={
                    "event_id": event_id,
                    "event_type": event_type,
                    "duration_seconds": duration,
                    "handler_count": len(results)
                }
            )
            
        except Exception as e:
            self._events_failed += 1
            
            logger.error(
                f"Failed to process event",
                extra={
                    "event_id": event_id,
                    "event_type": event_type,
                    "error": str(e)
                }
            )
            
            # Send to DLQ
            await self._send_to_dlq(event, str(e))
            
    async def _send_to_dlq(self, event: Dict[str, Any], error: str):
        """Send failed event to dead letter queue."""
        if not self.event_publisher:
            return
            
        dlq_event = {
            "original_event": event,
            "error": error,
            "timestamp": datetime.utcnow().isoformat(),
            "service": self.service_name,
            "retry_count": event.get("retry_count", 0) + 1
        }
        
        try:
            await self.event_publisher.publish(self._dlq_topic, dlq_event)
        except Exception as e:
            logger.error(f"Failed to send event to DLQ: {e}")
            
    def get_metrics(self) -> Dict[str, Any]:
        """Get event processing metrics."""
        return {
            "events_processed": self._events_processed,
            "events_failed": self._events_failed,
            "success_rate": self._events_processed / max(self._events_processed + self._events_failed, 1),
            "handler_metrics": self.event_router.get_handler_metrics()
        } 