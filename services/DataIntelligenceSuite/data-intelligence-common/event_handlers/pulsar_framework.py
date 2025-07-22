"""
Apache Pulsar Event Framework for DataIntelligence Services
Provides event publishing, subscription, and processing capabilities
"""

import asyncio
import logging
import json
from typing import Dict, Any, Optional, List, Callable, Union, Type
from datetime import datetime
from dataclasses import dataclass, asdict, field
from enum import Enum
import pulsar
from pulsar.schema import Schema, Record, String, Integer, Float, Boolean, Array, Map
import uuid
from functools import wraps

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event priority levels"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class EventType(Enum):
    """Standard event types for DataIntelligence"""
    # Data events
    DATA_INGESTED = "data.ingested"
    DATA_PROCESSED = "data.processed"
    DATA_QUALITY_CHECK = "data.quality.check"
    DATA_VALIDATED = "data.validated"
    DATA_TRANSFORMED = "data.transformed"
    
    # ML events
    MODEL_TRAINED = "model.trained"
    MODEL_DEPLOYED = "model.deployed"
    MODEL_PREDICTION = "model.prediction"
    MODEL_EVALUATION = "model.evaluation"
    MODEL_DRIFT_DETECTED = "model.drift.detected"
    
    # Pipeline events
    PIPELINE_STARTED = "pipeline.started"
    PIPELINE_COMPLETED = "pipeline.completed"
    PIPELINE_FAILED = "pipeline.failed"
    PIPELINE_STAGE_COMPLETED = "pipeline.stage.completed"
    
    # Service events
    SERVICE_STARTED = "service.started"
    SERVICE_STOPPED = "service.stopped"
    SERVICE_HEALTH_CHECK = "service.health.check"
    SERVICE_ERROR = "service.error"
    
    # Query events
    QUERY_EXECUTED = "query.executed"
    QUERY_OPTIMIZED = "query.optimized"
    QUERY_CACHED = "query.cached"
    
    # Workflow events
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED = "workflow.failed"
    WORKFLOW_TASK_COMPLETED = "workflow.task.completed"
    
    # Custom events
    CUSTOM = "custom"


@dataclass
class EventMetadata:
    """Standard metadata for all events"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = EventType.CUSTOM.value
    source_service: str = ""
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: Optional[str] = None
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    priority: int = EventPriority.NORMAL.value
    version: str = "1.0"
    tags: List[str] = field(default_factory=list)
    

@dataclass
class DataEvent(Record):
    """Base event for data-related events"""
    metadata: EventMetadata
    dataset_id: str
    dataset_name: str
    operation: str
    record_count: Optional[int] = None
    size_bytes: Optional[int] = None
    schema_version: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ModelEvent(Record):
    """Base event for ML model events"""
    metadata: EventMetadata
    model_id: str
    model_name: str
    model_version: str
    model_type: str
    operation: str
    metrics: Dict[str, float] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineEvent(Record):
    """Base event for pipeline events"""
    metadata: EventMetadata
    pipeline_id: str
    pipeline_name: str
    stage: Optional[str] = None
    status: str = "unknown"
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryEvent(Record):
    """Base event for query events"""
    metadata: EventMetadata
    query_id: str
    query_type: str
    query_text: Optional[str] = None
    execution_time_ms: Optional[float] = None
    rows_returned: Optional[int] = None
    cache_hit: bool = False
    attributes: Dict[str, Any] = field(default_factory=dict)


class PulsarEventBus:
    """Main event bus for publishing and consuming events"""
    
    def __init__(self, 
                 pulsar_url: str,
                 service_name: str,
                 tenant: str = "public",
                 namespace: str = "dataintelligence",
                 auth_token: Optional[str] = None,
                 tls_trust_certs_file_path: Optional[str] = None,
                 tls_allow_insecure_connection: bool = False):
        """
        Initialize Pulsar event bus
        
        Args:
            pulsar_url: Pulsar broker URL
            service_name: Name of the service using the event bus
            tenant: Pulsar tenant
            namespace: Pulsar namespace
            auth_token: JWT auth token
            tls_trust_certs_file_path: Path to TLS certificate
            tls_allow_insecure_connection: Allow insecure TLS connections
        """
        self.pulsar_url = pulsar_url
        self.service_name = service_name
        self.tenant = tenant
        self.namespace = namespace
        
        # Authentication
        auth = None
        if auth_token:
            auth = pulsar.AuthenticationToken(auth_token)
            
        # Create client
        self.client = pulsar.Client(
            service_url=pulsar_url,
            authentication=auth,
            tls_trust_certs_file_path=tls_trust_certs_file_path,
            tls_allow_insecure_connection=tls_allow_insecure_connection
        )
        
        # Producers and consumers cache
        self._producers: Dict[str, pulsar.Producer] = {}
        self._consumers: Dict[str, pulsar.Consumer] = {}
        self._handlers: Dict[str, List[Callable]] = {}
        
        # Event metrics
        self._events_published = 0
        self._events_consumed = 0
        self._events_failed = 0
        
    def get_topic_name(self, event_type: Union[EventType, str]) -> str:
        """Get full topic name for event type"""
        if isinstance(event_type, EventType):
            event_type = event_type.value
            
        # Replace dots with dashes for Pulsar topic names
        event_type_clean = event_type.replace(".", "-")
        return f"persistent://{self.tenant}/{self.namespace}/{event_type_clean}"
        
    def publish(self, 
                event: Union[DataEvent, ModelEvent, PipelineEvent, QueryEvent, Dict[str, Any]],
                event_type: Optional[Union[EventType, str]] = None,
                properties: Optional[Dict[str, str]] = None,
                partition_key: Optional[str] = None,
                ordering_key: Optional[str] = None,
                delivery_after: Optional[int] = None) -> pulsar.MessageId:
        """
        Publish an event
        
        Args:
            event: Event to publish
            event_type: Override event type
            properties: Message properties
            partition_key: Key for partitioning
            ordering_key: Key for ordering
            delivery_after: Delay delivery in seconds
            
        Returns:
            Message ID
        """
        try:
            # Determine event type
            if event_type is None:
                if hasattr(event, 'metadata'):
                    event_type = event.metadata.event_type
                else:
                    event_type = EventType.CUSTOM
                    
            topic = self.get_topic_name(event_type)
            
            # Get or create producer
            if topic not in self._producers:
                schema = None
                if hasattr(event, '__class__') and hasattr(event.__class__, 'schema'):
                    schema = event.__class__.schema()
                    
                self._producers[topic] = self.client.create_producer(
                    topic=topic,
                    schema=schema,
                    producer_name=f"{self.service_name}-producer",
                    batching_enabled=True,
                    batching_max_publish_delay_ms=100,
                    compression_type=pulsar.CompressionType.LZ4
                )
                
            producer = self._producers[topic]
            
            # Prepare message
            if isinstance(event, dict):
                message = json.dumps(event).encode()
            else:
                # Add service name to metadata
                if hasattr(event, 'metadata'):
                    event.metadata.source_service = self.service_name
                message = event
                
            # Send message
            msg_id = producer.send(
                message,
                properties=properties,
                partition_key=partition_key,
                ordering_key=ordering_key,
                deliver_after_ms=delivery_after * 1000 if delivery_after else None
            )
            
            self._events_published += 1
            logger.debug(f"Published event {event_type} with ID {msg_id}")
            
            return msg_id
            
        except Exception as e:
            self._events_failed += 1
            logger.error(f"Failed to publish event: {e}")
            raise
            
    async def publish_async(self,
                           event: Union[DataEvent, ModelEvent, PipelineEvent, QueryEvent, Dict[str, Any]],
                           event_type: Optional[Union[EventType, str]] = None,
                           properties: Optional[Dict[str, str]] = None,
                           partition_key: Optional[str] = None,
                           ordering_key: Optional[str] = None,
                           delivery_after: Optional[int] = None) -> pulsar.MessageId:
        """Async version of publish"""
        # For now, wrap sync version
        return await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: self.publish(
                event, event_type, properties, partition_key, ordering_key, delivery_after
            )
        )
        
    def subscribe(self,
                  event_types: Union[EventType, str, List[Union[EventType, str]]],
                  handler: Callable[[Any], None],
                  subscription_name: Optional[str] = None,
                  consumer_type: pulsar.ConsumerType = pulsar.ConsumerType.Shared,
                  initial_position: pulsar.InitialPosition = pulsar.InitialPosition.Latest,
                  negative_ack_redelivery_delay_ms: int = 60000,
                  max_total_receiver_queue_size_across_partitions: int = 50000,
                  auto_ack: bool = True) -> pulsar.Consumer:
        """
        Subscribe to events
        
        Args:
            event_types: Event types to subscribe to
            handler: Function to handle events
            subscription_name: Name of subscription
            consumer_type: Type of consumer (Shared, Exclusive, etc.)
            initial_position: Where to start consuming
            negative_ack_redelivery_delay_ms: Delay for redelivery
            max_total_receiver_queue_size_across_partitions: Max queue size
            auto_ack: Automatically acknowledge messages
            
        Returns:
            Consumer instance
        """
        # Normalize event types
        if not isinstance(event_types, list):
            event_types = [event_types]
            
        topics = [self.get_topic_name(et) for et in event_types]
        
        # Generate subscription name if not provided
        if not subscription_name:
            subscription_name = f"{self.service_name}-subscription-{uuid.uuid4().hex[:8]}"
            
        try:
            # Create consumer
            consumer = self.client.subscribe(
                topics,
                subscription_name,
                consumer_type=consumer_type,
                initial_position=initial_position,
                negative_ack_redelivery_delay_ms=negative_ack_redelivery_delay_ms,
                max_total_receiver_queue_size_across_partitions=max_total_receiver_queue_size_across_partitions
            )
            
            self._consumers[subscription_name] = consumer
            
            # Start consumer loop
            asyncio.create_task(
                self._consumer_loop(consumer, handler, auto_ack)
            )
            
            logger.info(f"Subscribed to {topics} with subscription {subscription_name}")
            return consumer
            
        except Exception as e:
            logger.error(f"Failed to subscribe: {e}")
            raise
            
    async def _consumer_loop(self, consumer: pulsar.Consumer, 
                           handler: Callable, auto_ack: bool):
        """Consumer message processing loop"""
        while True:
            try:
                msg = consumer.receive(timeout_millis=1000)
                
                try:
                    # Parse message
                    data = msg.data()
                    
                    # Try to deserialize as JSON first
                    try:
                        event = json.loads(data.decode()) if isinstance(data, bytes) else data
                    except:
                        event = data
                        
                    # Call handler
                    if asyncio.iscoroutinefunction(handler):
                        await handler(event)
                    else:
                        await asyncio.get_event_loop().run_in_executor(
                            None, handler, event
                        )
                        
                    # Acknowledge if auto_ack
                    if auto_ack:
                        consumer.acknowledge(msg)
                        
                    self._events_consumed += 1
                    
                except Exception as e:
                    logger.error(f"Error handling message: {e}")
                    consumer.negative_acknowledge(msg)
                    self._events_failed += 1
                    
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Consumer loop error: {e}")
                await asyncio.sleep(0.1)
                
    def create_event_handler(self, event_type: Union[EventType, str]):
        """Decorator for event handlers"""
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
                
            # Register handler
            if event_type not in self._handlers:
                self._handlers[event_type] = []
            self._handlers[event_type].append(wrapper)
            
            # Subscribe to event
            self.subscribe(
                event_type,
                wrapper,
                subscription_name=f"{self.service_name}-{func.__name__}"
            )
            
            return wrapper
        return decorator
        
    def close(self):
        """Close all connections"""
        # Close producers
        for producer in self._producers.values():
            producer.close()
            
        # Close consumers
        for consumer in self._consumers.values():
            consumer.close()
            
        # Close client
        self.client.close()
        
        logger.info(f"Event bus closed. Published: {self._events_published}, "
                   f"Consumed: {self._events_consumed}, Failed: {self._events_failed}")
        

class EventProcessor:
    """Base class for event processors"""
    
    def __init__(self, event_bus: PulsarEventBus):
        self.event_bus = event_bus
        self.processors: Dict[EventType, List[Callable]] = {}
        
    def register_processor(self, event_type: EventType, processor: Callable):
        """Register a processor for an event type"""
        if event_type not in self.processors:
            self.processors[event_type] = []
        self.processors[event_type].append(processor)
        
    def process(self, event_type: EventType):
        """Decorator to register a processor"""
        def decorator(func):
            self.register_processor(event_type, func)
            return func
        return decorator
        
    async def handle_event(self, event: Dict[str, Any]):
        """Handle incoming event"""
        event_type = event.get("metadata", {}).get("event_type")
        
        if event_type in self.processors:
            for processor in self.processors[event_type]:
                try:
                    if asyncio.iscoroutinefunction(processor):
                        await processor(event)
                    else:
                        processor(event)
                except Exception as e:
                    logger.error(f"Error in processor {processor.__name__}: {e}")
                    
    def start(self):
        """Start processing events"""
        for event_type in self.processors:
            self.event_bus.subscribe(
                event_type,
                self.handle_event,
                subscription_name=f"{self.event_bus.service_name}-processor"
            )


class EventStore:
    """Store for event sourcing and replay"""
    
    def __init__(self, event_bus: PulsarEventBus, 
                 retention_days: int = 30):
        self.event_bus = event_bus
        self.retention_days = retention_days
        
    async def store_event(self, event: Any) -> str:
        """Store event for replay"""
        # Publish to special event-store topic
        msg_id = self.event_bus.publish(
            event,
            event_type="event.store",
            properties={
                "retention_days": str(self.retention_days)
            }
        )
        return str(msg_id)
        
    async def replay_events(self, 
                           start_time: datetime,
                           end_time: Optional[datetime] = None,
                           event_types: Optional[List[EventType]] = None,
                           filters: Optional[Dict[str, Any]] = None) -> List[Any]:
        """Replay events within time range"""
        # This would query the event store topic with filters
        # Implementation depends on storage backend
        pass
        

def create_standard_topics(admin_url: str, tenant: str = "public", 
                          namespace: str = "dataintelligence"):
    """Create standard topics for DataIntelligence"""
    import requests
    
    base_url = f"{admin_url}/admin/v2/persistent/{tenant}/{namespace}"
    
    # Standard topics
    topics = [
        # Data topics
        "data-ingested",
        "data-processed",
        "data-quality-check",
        "data-validated",
        "data-transformed",
        
        # ML topics
        "model-trained",
        "model-deployed",
        "model-prediction",
        "model-evaluation",
        "model-drift-detected",
        
        # Pipeline topics
        "pipeline-started",
        "pipeline-completed",
        "pipeline-failed",
        "pipeline-stage-completed",
        
        # Service topics
        "service-started",
        "service-stopped",
        "service-health-check",
        "service-error",
        
        # Query topics
        "query-executed",
        "query-optimized",
        "query-cached",
        
        # Workflow topics
        "workflow-started",
        "workflow-completed",
        "workflow-failed",
        "workflow-task-completed",
        
        # Event store
        "event-store"
    ]
    
    for topic in topics:
        try:
            response = requests.put(f"{base_url}/{topic}")
            if response.status_code in [204, 409]:  # Created or already exists
                logger.info(f"Topic {topic} ready")
            else:
                logger.error(f"Failed to create topic {topic}: {response.text}")
        except Exception as e:
            logger.error(f"Error creating topic {topic}: {e}") 