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

from .event_bus import Event, EventBus, EventSubscription
from ...core.processing import BaseProcessor, ProcessorConfig, ProcessingResult, ProcessingStatus

logger = logging.getLogger(__name__)


class EventProcessingMode(Enum):
    """Event processing modes"""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    WINDOWED = "windowed"
    STATEFUL = "stateful"


@dataclass
class EventConfig(ProcessorConfig):
    """Configuration for event processing"""
    processing_mode: EventProcessingMode = EventProcessingMode.SEQUENTIAL
    
    # Parallel processing
    max_concurrent_events: int = 10
    
    # Windowing
    window_size: Optional[timedelta] = None
    window_slide: Optional[timedelta] = None
    
    # State management
    enable_state: bool = False
    state_ttl: Optional[timedelta] = None
    
    # Event routing
    routing_rules: Dict[str, str] = field(default_factory=dict)
    
    # Error handling
    error_topic: Optional[str] = None
    dead_letter_topic: Optional[str] = None
    
    # Deduplication
    enable_deduplication: bool = False
    dedup_window: timedelta = timedelta(minutes=5)


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
        super().__init__(config, **kwargs)
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
        
    async def initialize(self):
        """Initialize event processor"""
        await self.event_bus.initialize()
        logger.info(f"Initialized event processor: {self.config.name}")
        
    async def shutdown(self):
        """Shutdown event processor"""
        # Unsubscribe from all topics
        for subscription in self._subscriptions:
            await self.event_bus.unsubscribe(subscription.subscription_id)
            
        await self.event_bus.shutdown()
        logger.info("Shutdown event processor")
        
    def add_handler(self, event_type: str, handler: Callable[[Event], Any]):
        """Add event handler for specific event type"""
        self._handlers[event_type] = handler
        
    def add_transformer(self, transformer: Callable[[Event], Event]):
        """Add event transformer"""
        self._transformers.append(transformer)
        
    async def subscribe_to_events(
        self,
        topic_pattern: str,
        filter_expression: Optional[str] = None
    ) -> EventSubscription:
        """Subscribe to events from topic"""
        subscription = await self.event_bus.subscribe(
            topic_pattern=topic_pattern,
            handler=self._handle_event,
            filter_expression=filter_expression,
            dead_letter_topic=self.config.dead_letter_topic
        )
        
        self._subscriptions.append(subscription)
        return subscription
        
    async def process(self, data: Any, job_id: Optional[str] = None) -> ProcessingResult:
        """Process events - implements BaseProcessor interface"""
        result = ProcessingResult(
            job_id=job_id or f"event_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            status=ProcessingStatus.RUNNING,
            started_at=datetime.utcnow()
        )
        
        try:
            if isinstance(data, Event):
                # Process single event
                event_result = await self._process_single_event(data)
                result.records_processed = 1 if event_result.status == "success" else 0
                result.records_failed = 0 if event_result.status == "success" else 1
                
            elif isinstance(data, list):
                # Process batch of events
                results = await self._process_batch(data)
                result.records_processed = sum(1 for r in results if r.status == "success")
                result.records_failed = sum(1 for r in results if r.status == "error")
                
            else:
                raise ValueError(f"Unsupported data type: {type(data)}")
                
            result.status = ProcessingStatus.COMPLETED
            result.completed_at = datetime.utcnow()
            
        except Exception as e:
            logger.error(f"Event processing failed: {e}")
            result.status = ProcessingStatus.FAILED
            result.errors.append({"error": str(e)})
            
        return result
        
    async def _handle_event(self, event: Event):
        """Handle incoming event from subscription"""
        # Check deduplication
        if self.config.enable_deduplication:
            if not await self._check_duplicate(event):
                return
                
        # Process based on mode
        if self.config.processing_mode == EventProcessingMode.WINDOWED:
            await self._add_to_window(event)
        else:
            await self._process_single_event(event)
            
    async def _process_single_event(self, event: Event) -> EventResult:
        """Process a single event"""
        result = EventResult(
            event_id=event.event_id,
            status="processing",
            processed_at=datetime.utcnow()
        )
        
        try:
            # Apply transformers
            transformed_event = event
            for transformer in self._transformers:
                transformed_event = transformer(transformed_event)
                
            # Route to handler
            handler = self._handlers.get(transformed_event.event_type)
            if not handler:
                handler = self._handlers.get("*")  # Default handler
                
            if handler:
                # Execute handler
                if asyncio.iscoroutinefunction(handler):
                    output = await handler(transformed_event)
                else:
                    output = handler(transformed_event)
                    
                result.output = output
                result.status = "success"
                
                # Apply routing rules
                await self._apply_routing(transformed_event, output)
                
            else:
                logger.warning(f"No handler for event type: {transformed_event.event_type}")
                result.status = "no_handler"
                
            # Update state if stateful
            if self.config.enable_state:
                await self._update_state(transformed_event, result)
                
            # Record metrics
            if self.metrics:
                self.metrics.increment_counter(
                    "events_processed_total",
                    {"event_type": event.event_type, "status": result.status}
                )
                
        except Exception as e:
            logger.error(f"Error processing event {event.event_id}: {e}")
            result.status = "error"
            result.error = str(e)
            
            # Send to error topic if configured
            if self.config.error_topic:
                await self._send_to_error_topic(event, str(e))
                
        return result
        
    async def _process_batch(self, events: List[Event]) -> List[EventResult]:
        """Process batch of events"""
        if self.config.processing_mode == EventProcessingMode.PARALLEL:
            # Process in parallel
            tasks = [self._process_single_event(event) for event in events]
            
            # Limit concurrency
            semaphore = asyncio.Semaphore(self.config.max_concurrent_events)
            
            async def process_with_limit(event):
                async with semaphore:
                    return await self._process_single_event(event)
                    
            tasks = [process_with_limit(event) for event in events]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Convert exceptions to results
            final_results = []
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    final_results.append(EventResult(
                        event_id=events[i].event_id,
                        status="error",
                        processed_at=datetime.utcnow(),
                        error=str(result)
                    ))
                else:
                    final_results.append(result)
                    
            return final_results
            
        else:
            # Process sequentially
            results = []
            for event in events:
                result = await self._process_single_event(event)
                results.append(result)
            return results
            
    async def _add_to_window(self, event: Event):
        """Add event to window for windowed processing"""
        window_key = self._get_window_key(event.timestamp)
        
        if window_key not in self._windows:
            self._windows[window_key] = []
            
        self._windows[window_key].append(event)
        
        # Check if window is complete
        await self._check_window_completion()
        
    def _get_window_key(self, timestamp: datetime) -> str:
        """Get window key for timestamp"""
        if not self.config.window_size:
            return "default"
            
        # Calculate window start
        window_seconds = int(self.config.window_size.total_seconds())
        timestamp_seconds = int(timestamp.timestamp())
        window_start = (timestamp_seconds // window_seconds) * window_seconds
        
        return str(window_start)
        
    async def _check_window_completion(self):
        """Check and process completed windows"""
        current_time = datetime.utcnow()
        completed_windows = []
        
        for window_key, events in self._windows.items():
            if window_key == "default":
                continue
                
            window_start = datetime.fromtimestamp(int(window_key))
            window_end = window_start + self.config.window_size
            
            if current_time > window_end:
                completed_windows.append(window_key)
                
        # Process completed windows
        for window_key in completed_windows:
            events = self._windows.pop(window_key)
            await self._process_window(window_key, events)
            
    async def _process_window(self, window_key: str, events: List[Event]):
        """Process a completed window of events"""
        logger.info(f"Processing window {window_key} with {len(events)} events")
        
        # Create aggregated event
        aggregated_event = Event(
            event_type="window_aggregate",
            source=self.config.name,
            payload={
                "window_key": window_key,
                "event_count": len(events),
                "events": [e.to_dict() for e in events]
            }
        )
        
        # Process aggregated event
        await self._process_single_event(aggregated_event)
        
    async def _check_duplicate(self, event: Event) -> bool:
        """Check if event is duplicate"""
        if event.event_id in self._processed_events:
            logger.debug(f"Duplicate event detected: {event.event_id}")
            return False
            
        # Add to processed set
        self._processed_events.add(event.event_id)
        
        # Clean old entries
        # In production, use a proper cache with TTL
        if len(self._processed_events) > 10000:
            self._processed_events.clear()
            
        return True
        
    async def _update_state(self, event: Event, result: EventResult):
        """Update processor state"""
        state_key = f"{event.event_type}:{event.correlation_id or 'default'}"
        
        if state_key not in self._state:
            self._state[state_key] = {
                "created_at": datetime.utcnow(),
                "events": []
            }
            
        self._state[state_key]["events"].append({
            "event_id": event.event_id,
            "timestamp": event.timestamp,
            "result": result.status
        })
        
        self._state[state_key]["updated_at"] = datetime.utcnow()
        
    async def _apply_routing(self, event: Event, output: Any):
        """Apply routing rules to event output"""
        for pattern, target_topic in self.config.routing_rules.items():
            if self._matches_pattern(event.event_type, pattern):
                # Create routed event
                routed_event = Event(
                    event_type=f"routed.{event.event_type}",
                    source=self.config.name,
                    correlation_id=event.correlation_id,
                    causation_id=event.event_id,
                    payload={
                        "original_event": event.to_dict(),
                        "output": output
                    }
                )
                
                # Publish to target topic
                await self.event_bus.publish(target_topic, routed_event)
                
    def _matches_pattern(self, event_type: str, pattern: str) -> bool:
        """Check if event type matches pattern"""
        if pattern == "*":
            return True
        elif pattern.endswith("*"):
            return event_type.startswith(pattern[:-1])
        else:
            return event_type == pattern
            
    async def _send_to_error_topic(self, event: Event, error: str):
        """Send event to error topic"""
        error_event = Event(
            event_type="processing.error",
            source=self.config.name,
            causation_id=event.event_id,
            payload={
                "original_event": event.to_dict(),
                "error": error,
                "processor": self.config.name
            }
        )
        
        await self.event_bus.publish(self.config.error_topic, error_event)
        
    def get_state(self, state_key: Optional[str] = None) -> Dict[str, Any]:
        """Get processor state"""
        if state_key:
            return self._state.get(state_key, {})
        return self._state
        
    def clear_state(self, state_key: Optional[str] = None):
        """Clear processor state"""
        if state_key:
            self._state.pop(state_key, None)
        else:
            self._state.clear() 