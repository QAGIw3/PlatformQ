"""
Event Processor for DataIntelligenceSuite

Provides advanced event processing capabilities.
"""

import asyncio
import logging
from typing import Any, Dict, Optional, List, Callable, Union, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json

from .base import (
    Event, EventProcessingMode, EventHandler, EventProcessingConfig,
    BaseEventProcessor as BaseProcessor
)
from .bus import UnifiedEventBus as EventBus, EventSubscription
from ...core.processing import ProcessorConfig, ProcessingResult, ProcessingStatus

logger = logging.getLogger(__name__)


@dataclass
class EventConfig(EventProcessingConfig):
    """Configuration for event processing - extends base config"""
    # Additional fields specific to this processor
    routing_rules: Dict[str, str] = field(default_factory=dict)
    error_topic: Optional[str] = None
    
    # Windowing
    window_size: Optional[timedelta] = None
    window_slide: Optional[timedelta] = None


@dataclass
class EventResult:
    """Result of event processing"""
    event_id: str
    status: str
    processed_at: datetime
    output: Optional[Any] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class EventProcessor(BaseProcessor):
    """
    Advanced event processor with support for various processing patterns.
    
    Features:
    - Sequential and parallel processing
    - Windowed aggregations
    - Stateful processing
    - Event routing and transformation
    - Deduplication
    - Error handling
    """
    
    def __init__(
        self,
        config: EventConfig,
        event_bus: EventBus,
        **kwargs
    ):
        # Initialize with EventMixin functionality
        super().__init__(
            event_bus=event_bus,
            metrics_collector=kwargs.get('metrics_collector'),
            consul_client=kwargs.get('consul_client')
        )
        
        self.config: EventConfig = config
        self.event_bus = event_bus
        
        # Processing state
        self._state: Dict[str, Any] = {}
        self._windows: Dict[str, List[Event]] = {}
        self._processed_events: Set[str] = set()
        
        # Event handlers
        self._handlers: Dict[str, Callable] = {}
        self._transformers: List[Callable] = []
        
        # Subscriptions
        self._subscriptions: List[EventSubscription] = []
        
    async def start(self):
        """Start the processor"""
        await super().start()
        logger.info("Event processor started")
        
    async def stop(self):
        """Stop the processor"""
        # Unsubscribe from all events
        for subscription in self._subscriptions:
            await self.event_bus.unsubscribe(subscription.subscription_id)
        
        await super().stop()
        logger.info("Event processor stopped")
        
    def add_handler(self, event_type: str, handler: Callable):
        """Add event handler for specific event type"""
        self._handlers[event_type] = handler
        
    def add_transformer(self, transformer: Callable):
        """Add event transformer"""
        self._transformers.append(transformer)
        
    async def subscribe_to_events(
        self,
        topic_pattern: str,
        event_types: Optional[List[str]] = None
    ):
        """Subscribe to events"""
        subscription = await self.event_bus.subscribe(
            topic_pattern=topic_pattern,
            handler=self._handle_event,
            event_types=event_types,
            dead_letter_topic=self.config.dead_letter_topic
        )
        self._subscriptions.append(subscription)
        
    async def _handle_event(self, event: Event):
        """Handle incoming event"""
        try:
            # Check deduplication
            if self.config.enable_deduplication:
                if event.event_id in self._processed_events:
                    logger.debug(f"Skipping duplicate event {event.event_id}")
                    return
                    
                # Add to processed set (with TTL handling)
                self._processed_events.add(event.event_id)
                
            # Apply transformers
            transformed_event = event
            for transformer in self._transformers:
                transformed_event = await self._apply_transformer(
                    transformed_event, transformer
                )
                
            # Route event
            if self.config.routing_rules:
                route = self._get_route(transformed_event)
                if route:
                    await self.event_bus.publish(route, transformed_event)
                    return
                    
            # Process based on mode
            if self.config.processing_mode == EventProcessingMode.WINDOWED:
                await self._process_windowed(transformed_event)
            elif self.config.processing_mode == EventProcessingMode.PARALLEL:
                await self._process_parallel(transformed_event)
            else:
                await self._process_sequential(transformed_event)
                
        except Exception as e:
            logger.error(f"Error handling event {event.event_id}: {e}")
            
            # Send to error topic if configured
            if self.config.error_topic:
                event.headers["error"] = str(e)
                await self.event_bus.publish(self.config.error_topic, event)
                
    async def _apply_transformer(self, event: Event, transformer: Callable) -> Event:
        """Apply transformer to event"""
        if asyncio.iscoroutinefunction(transformer):
            return await transformer(event)
        else:
            return transformer(event)
            
    def _get_route(self, event: Event) -> Optional[str]:
        """Get routing destination for event"""
        for pattern, destination in self.config.routing_rules.items():
            if self._matches_pattern(event.event_type, pattern):
                return destination
        return None
        
    def _matches_pattern(self, event_type: str, pattern: str) -> bool:
        """Check if event type matches pattern"""
        # Simple wildcard matching
        if pattern.endswith("*"):
            return event_type.startswith(pattern[:-1])
        return event_type == pattern
        
    async def _process_sequential(self, event: Event):
        """Process event sequentially"""
        handler = self._handlers.get(event.event_type)
        if handler:
            if asyncio.iscoroutinefunction(handler):
                await handler(event)
            else:
                handler(event)
                
    async def _process_parallel(self, event: Event):
        """Process event in parallel"""
        handler = self._handlers.get(event.event_type)
        if handler:
            # Use semaphore to limit concurrency
            async with asyncio.Semaphore(self.config.max_concurrent):
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
                    
    async def _process_windowed(self, event: Event):
        """Process event in window"""
        window_key = self._get_window_key(event)
        
        # Add to window
        if window_key not in self._windows:
            self._windows[window_key] = []
        self._windows[window_key].append(event)
        
        # Check if window is complete
        if self._is_window_complete(window_key):
            await self._process_window(window_key)
            
    def _get_window_key(self, event: Event) -> str:
        """Get window key for event"""
        # Simple time-based windowing
        if self.config.window_size:
            window_start = int(event.timestamp.timestamp() / 
                             self.config.window_size.total_seconds())
            return f"{event.event_type}:{window_start}"
        return event.event_type
        
    def _is_window_complete(self, window_key: str) -> bool:
        """Check if window is complete"""
        # Simple size-based check
        return len(self._windows[window_key]) >= self.config.batch_size
        
    async def _process_window(self, window_key: str):
        """Process completed window"""
        events = self._windows.pop(window_key, [])
        if not events:
            return
            
        # Get handler for event type
        event_type = events[0].event_type
        handler = self._handlers.get(event_type)
        
        if handler:
            # Call handler with all events
            if asyncio.iscoroutinefunction(handler):
                await handler(events)
            else:
                handler(events)
                
    async def process_event(self, event: Event) -> EventResult:
        """Process single event and return result"""
        try:
            await self._handle_event(event)
            
            return EventResult(
                event_id=event.event_id,
                status="success",
                processed_at=datetime.utcnow()
            )
            
        except Exception as e:
            logger.error(f"Failed to process event {event.event_id}: {e}")
            
            return EventResult(
                event_id=event.event_id,
                status="failed",
                processed_at=datetime.utcnow(),
                error=str(e)
            )
            
    async def get_state(self, key: str) -> Any:
        """Get processing state"""
        return self._state.get(key)
        
    async def set_state(self, key: str, value: Any):
        """Set processing state"""
        self._state[key] = value
        
    async def clear_state(self):
        """Clear all processing state"""
        self._state.clear()
        self._windows.clear()
        self._processed_events.clear()


# Re-export for backward compatibility
__all__ = [
    'Event', 'EventProcessingMode', 'EventConfig', 'EventResult',
    'EventProcessor', 'BaseProcessor'
] 