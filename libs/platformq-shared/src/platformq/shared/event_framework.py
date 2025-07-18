"""
Event Processing Framework for PlatformQ

Provides standardized patterns for consuming and processing events from Apache Pulsar,
reducing boilerplate code across services.
"""

import asyncio
import logging
import inspect
from abc import ABC, abstractmethod
from typing import Dict, Type, Callable, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum
import traceback

import pulsar
from pulsar.schema import AvroSchema, Schema
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class ProcessingStatus(Enum):
    """Event processing status"""
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    SKIP = "skip"


@dataclass
class ProcessingResult:
    """Result of event processing"""
    status: ProcessingStatus
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None


class EventHandler:
    """Decorator for event handler methods"""
    
    def __init__(self, topic_pattern: str, schema: Type, 
                 subscription_name: Optional[str] = None,
                 consumer_type: pulsar.ConsumerType = pulsar.ConsumerType.Shared):
        self.topic_pattern = topic_pattern
        self.schema = schema
        self.subscription_name = subscription_name
        self.consumer_type = consumer_type
        
    def __call__(self, func: Callable) -> Callable:
        func._event_handler = True
        func._topic_pattern = self.topic_pattern
        func._schema = self.schema
        func._subscription_name = self.subscription_name or f"{func.__name__}_subscription"
        func._consumer_type = self.consumer_type
        return func


def event_handler(topic_pattern: str, schema: Type, **kwargs):
    """Decorator to mark methods as event handlers"""
    return EventHandler(topic_pattern, schema, **kwargs)


class EventProcessor(ABC):
    """
    Base class for event processors.
    
    Subclasses should use @event_handler decorators on methods to define
    which events they handle.
    """
    
    def __init__(self, 
                 service_name: str,
                 pulsar_url: str,
                 tenant_id: Optional[str] = None,
                 event_publisher: Optional[EventPublisher] = None,
                 max_retries: int = 3,
                 retry_delay: float = 1.0):
        self.service_name = service_name
        self.pulsar_url = pulsar_url
        self.tenant_id = tenant_id
        self.event_publisher = event_publisher
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        
        self.client = None
        self.consumers: Dict[str, pulsar.Consumer] = {}
        self.handler_tasks: List[asyncio.Task] = []
        self._running = False
        
    async def start(self):
        """Start the event processor"""
        if self._running:
            logger.warning(f"{self.service_name} event processor already running")
            return
            
        logger.info(f"Starting {self.service_name} event processor")
        
        # Initialize Pulsar client
        self.client = pulsar.Client(self.pulsar_url)
        
        # Discover and register event handlers
        self._register_handlers()
        
        self._running = True
        
    async def stop(self):
        """Stop the event processor"""
        if not self._running:
            return
            
        logger.info(f"Stopping {self.service_name} event processor")
        
        self._running = False
        
        # Cancel all handler tasks
        for task in self.handler_tasks:
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self.handler_tasks, return_exceptions=True)
        
        # Close consumers
        for consumer in self.consumers.values():
            consumer.close()
            
        # Close client
        if self.client:
            self.client.close()
            
    def _register_handlers(self):
        """Discover and register event handler methods"""
        for name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            if hasattr(method, '_event_handler') and method._event_handler:
                logger.info(f"Registering handler {name} for topic {method._topic_pattern}")
                
                # Create consumer
                topic = method._topic_pattern
                if self.tenant_id and '{tenant_id}' in topic:
                    topic = topic.format(tenant_id=self.tenant_id)
                    
                consumer = self.client.subscribe(
                    topic,
                    subscription_name=f"{self.service_name}_{method._subscription_name}",
                    consumer_type=method._consumer_type,
                    schema=AvroSchema(method._schema) if hasattr(method._schema, '_avro_schema') else method._schema
                )
                
                self.consumers[name] = consumer
                
                # Create handler task
                task = asyncio.create_task(self._handle_events(consumer, method))
                self.handler_tasks.append(task)
                
    async def _handle_events(self, consumer: pulsar.Consumer, handler: Callable):
        """Handle events from a consumer"""
        while self._running:
            try:
                # Receive message with timeout
                msg = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: consumer.receive(timeout_millis=1000)
                )
                
                if msg:
                    # Process the event
                    await self._process_event(consumer, msg, handler)
                    
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error in event handler loop: {e}")
                await asyncio.sleep(0.1)
                
    async def _process_event(self, consumer: pulsar.Consumer, msg: pulsar.Message, handler: Callable):
        """Process a single event"""
        retry_count = 0
        
        while retry_count <= self.max_retries:
            try:
                # Extract event data
                event = msg.value()
                
                # Call handler
                result = await handler(event, msg)
                
                # Handle result
                if isinstance(result, ProcessingResult):
                    if result.status == ProcessingStatus.SUCCESS:
                        consumer.acknowledge(msg)
                        return
                    elif result.status == ProcessingStatus.SKIP:
                        consumer.acknowledge(msg)
                        logger.warning(f"Skipped event: {result.message}")
                        return
                    elif result.status == ProcessingStatus.RETRY:
                        retry_count += 1
                        if retry_count <= self.max_retries:
                            await asyncio.sleep(self.retry_delay * retry_count)
                            continue
                    elif result.status == ProcessingStatus.FAILED:
                        consumer.negative_acknowledge(msg)
                        logger.error(f"Failed to process event: {result.message}")
                        return
                else:
                    # Default: consider success if no exception
                    consumer.acknowledge(msg)
                    return
                    
            except Exception as e:
                logger.error(f"Error processing event: {e}\n{traceback.format_exc()}")
                retry_count += 1
                
                if retry_count <= self.max_retries:
                    await asyncio.sleep(self.retry_delay * retry_count)
                else:
                    consumer.negative_acknowledge(msg)
                    
    @abstractmethod
    async def on_start(self):
        """Called when the processor starts. Override for initialization logic."""
        pass
        
    @abstractmethod
    async def on_stop(self):
        """Called when the processor stops. Override for cleanup logic."""
        pass


class BatchEventProcessor(EventProcessor):
    """
    Event processor that batches events for more efficient processing.
    """
    
    def __init__(self, 
                 service_name: str,
                 pulsar_url: str,
                 batch_size: int = 100,
                 batch_timeout: float = 5.0,
                 **kwargs):
        super().__init__(service_name, pulsar_url, **kwargs)
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.batches: Dict[str, List[tuple]] = {}
        
    async def _handle_events(self, consumer: pulsar.Consumer, handler: Callable):
        """Handle events with batching"""
        handler_name = handler.__name__
        self.batches[handler_name] = []
        
        # Start batch timeout task
        asyncio.create_task(self._batch_timeout_handler(handler_name, consumer, handler))
        
        while self._running:
            try:
                # Receive message with timeout
                msg = await asyncio.get_event_loop().run_in_executor(
                    None, lambda: consumer.receive(timeout_millis=100)
                )
                
                if msg:
                    event = msg.value()
                    self.batches[handler_name].append((event, msg))
                    
                    # Process batch if full
                    if len(self.batches[handler_name]) >= self.batch_size:
                        await self._process_batch(handler_name, consumer, handler)
                        
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error in batch event handler loop: {e}")
                await asyncio.sleep(0.01)
                
    async def _batch_timeout_handler(self, handler_name: str, consumer: pulsar.Consumer, handler: Callable):
        """Process batches on timeout"""
        while self._running:
            await asyncio.sleep(self.batch_timeout)
            if self.batches.get(handler_name):
                await self._process_batch(handler_name, consumer, handler)
                
    async def _process_batch(self, handler_name: str, consumer: pulsar.Consumer, handler: Callable):
        """Process a batch of events"""
        batch = self.batches[handler_name]
        self.batches[handler_name] = []
        
        if not batch:
            return
            
        try:
            events = [item[0] for item in batch]
            messages = [item[1] for item in batch]
            
            # Call handler with batch
            result = await handler(events, messages)
            
            # Acknowledge all messages in batch
            for msg in messages:
                consumer.acknowledge(msg)
                
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            # Negative acknowledge all messages
            for msg in messages:
                consumer.negative_acknowledge(msg)


class CompositeEventProcessor(EventProcessor):
    """
    Combines multiple event processors into one.
    Useful for services that need to handle many different event types.
    """
    
    def __init__(self, service_name: str, pulsar_url: str, **kwargs):
        super().__init__(service_name, pulsar_url, **kwargs)
        self.processors: List[EventProcessor] = []
        
    def add_processor(self, processor: EventProcessor):
        """Add a child processor"""
        self.processors.append(processor)
        
    async def start(self):
        """Start all processors"""
        await super().start()
        await asyncio.gather(*[p.start() for p in self.processors])
        
    async def stop(self):
        """Stop all processors"""
        await asyncio.gather(*[p.stop() for p in self.processors])
        await super().stop()
        
    async def on_start(self):
        """Initialize composite processor"""
        pass
        
    async def on_stop(self):
        """Cleanup composite processor"""
        pass 