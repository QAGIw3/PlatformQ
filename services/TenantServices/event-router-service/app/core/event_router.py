"""
Event Router Core

Handles intelligent event routing, content-based routing, and event processing orchestration.
"""

import asyncio
import logging
import re
from typing import Dict, Any, List, Optional, Set, Tuple, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import json

import pulsar
from pulsar.schema import AvroSchema, JsonSchema
import aiopulsar

from .schema_registry import SchemaRegistry
from .transformation_engine import TransformationEngine
from .dead_letter_handler import DeadLetterHandler
from .event_store import EventStore
from ..monitoring import EventMetrics

logger = logging.getLogger(__name__)


@dataclass
class Route:
    """Routing configuration"""
    name: str
    source_pattern: str  # Topic pattern to consume from
    target_topics: List[str]  # Target topics to route to
    filters: List[Dict[str, Any]]  # Content-based filters
    transformations: List[Dict[str, Any]]  # Transformations to apply
    enrichments: List[Dict[str, Any]]  # Data enrichments
    error_handling: str = "dead_letter"  # dead_letter, retry, ignore
    max_retries: int = 3
    batch_size: int = 1
    enabled: bool = True
    tenant_aware: bool = True


@dataclass
class EventContext:
    """Context for event processing"""
    event_id: str
    source_topic: str
    timestamp: datetime
    tenant_id: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = None


class EventRouter:
    """Main event routing engine"""
    
    def __init__(self, pulsar_url: str, schema_registry: SchemaRegistry,
                 transformation_engine: TransformationEngine,
                 dead_letter_handler: DeadLetterHandler,
                 event_store: EventStore,
                 metrics: EventMetrics,
                 config: Dict[str, Any]):
        self.pulsar_url = pulsar_url
        self.schema_registry = schema_registry
        self.transformation_engine = transformation_engine
        self.dead_letter_handler = dead_letter_handler
        self.event_store = event_store
        self.metrics = metrics
        self.config = config
        
        self.client: Optional[pulsar.Client] = None
        self.routes: Dict[str, Route] = {}
        self.consumers: Dict[str, pulsar.Consumer] = {}
        self.producers: Dict[str, pulsar.Producer] = {}
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
    async def initialize(self):
        """Initialize the event router"""
        logger.info("Initializing event router...")
        
        # Create Pulsar client
        self.client = pulsar.Client(
            self.pulsar_url,
            operation_timeout_seconds=30,
            connection_timeout_seconds=30
        )
        
        # Load routing configuration
        await self._load_routes()
        
        # Create consumers and producers
        await self._setup_consumers()
        await self._setup_producers()
        
        logger.info(f"Event router initialized with {len(self.routes)} routes")
        
    async def _load_routes(self):
        """Load routing configuration"""
        # Default routes for common patterns
        default_routes = [
            Route(
                name="asset_events",
                source_pattern="persistent://platformq/.*/asset-.*",
                target_topics=["persistent://platformq/{tenant}/processed-assets"],
                filters=[],
                transformations=[],
                enrichments=[{"type": "metadata", "source": "asset-service"}],
                tenant_aware=True
            ),
            Route(
                name="user_activity",
                source_pattern="persistent://platformq/.*/user-activity-.*",
                target_topics=[
                    "persistent://platformq/{tenant}/activity-stream",
                    "persistent://platformq/{tenant}/analytics-events"
                ],
                filters=[{"field": "event_type", "operator": "not_in", "value": ["heartbeat"]}],
                transformations=[{"type": "flatten", "fields": ["user", "action"]}],
                enrichments=[],
                tenant_aware=True
            ),
            Route(
                name="system_metrics",
                source_pattern="persistent://platformq/.*/metrics-.*",
                target_topics=["persistent://platformq/monitoring/metrics"],
                filters=[],
                transformations=[{"type": "aggregate", "window": "1m", "function": "avg"}],
                enrichments=[],
                tenant_aware=False,
                batch_size=100
            ),
            Route(
                name="error_events",
                source_pattern="persistent://platformq/.*/error-.*",
                target_topics=[
                    "persistent://platformq/{tenant}/error-stream",
                    "persistent://platformq/monitoring/errors"
                ],
                filters=[],
                transformations=[],
                enrichments=[{"type": "stack_trace", "enhanced": True}],
                error_handling="retry",
                max_retries=5,
                tenant_aware=True
            )
        ]
        
        # Load custom routes from configuration
        custom_routes = self.config.get("routes", [])
        
        # Merge routes
        for route in default_routes + custom_routes:
            if isinstance(route, dict):
                route = Route(**route)
            self.routes[route.name] = route
            
    async def _setup_consumers(self):
        """Setup Pulsar consumers for all routes"""
        for route_name, route in self.routes.items():
            if not route.enabled:
                continue
                
            try:
                # Create consumer with pattern subscription
                consumer = self.client.subscribe(
                    route.source_pattern,
                    subscription_name=f"event-router-{route_name}",
                    consumer_type=pulsar.ConsumerType.Shared,
                    pattern_auto_discovery_period=60,
                    is_regex=True
                )
                
                self.consumers[route_name] = consumer
                logger.info(f"Created consumer for route '{route_name}' with pattern '{route.source_pattern}'")
                
            except Exception as e:
                logger.error(f"Failed to create consumer for route '{route_name}': {e}")
                
    async def _setup_producers(self):
        """Setup Pulsar producers for target topics"""
        # Get unique target topics from all routes
        target_topics = set()
        for route in self.routes.values():
            for topic in route.target_topics:
                # Handle tenant-aware topics
                if "{tenant}" in topic:
                    # Create a template producer
                    target_topics.add(topic)
                else:
                    target_topics.add(topic)
                    
        # Create producers
        for topic in target_topics:
            try:
                if "{tenant}" in topic:
                    # Skip template topics, create on demand
                    continue
                    
                producer = self.client.create_producer(
                    topic,
                    producer_name=f"event-router-{topic.split('/')[-1]}",
                    batching_enabled=True,
                    batching_max_messages=1000,
                    batching_max_publish_delay_ms=10
                )
                
                self.producers[topic] = producer
                logger.info(f"Created producer for topic '{topic}'")
                
            except Exception as e:
                logger.error(f"Failed to create producer for topic '{topic}': {e}")
                
    async def start_routing(self):
        """Start the routing engine"""
        if self._running:
            logger.warning("Event router is already running")
            return
            
        self._running = True
        logger.info("Starting event routing...")
        
        # Create routing tasks for each route
        for route_name, route in self.routes.items():
            if route.enabled:
                task = asyncio.create_task(
                    self._route_processor(route_name, route)
                )
                self._tasks.append(task)
                
        logger.info(f"Started {len(self._tasks)} routing tasks")
        
    async def _route_processor(self, route_name: str, route: Route):
        """Process events for a specific route"""
        consumer = self.consumers.get(route_name)
        if not consumer:
            logger.error(f"No consumer found for route '{route_name}'")
            return
            
        batch = []
        
        while self._running:
            try:
                # Receive message with timeout
                msg = consumer.receive(timeout_millis=1000)
                
                # Create event context
                context = EventContext(
                    event_id=str(msg.message_id()),
                    source_topic=msg.topic_name(),
                    timestamp=datetime.fromtimestamp(msg.publish_timestamp() / 1000, tz=timezone.utc),
                    tenant_id=self._extract_tenant_id(msg.topic_name()) if route.tenant_aware else None,
                    metadata={
                        "properties": msg.properties(),
                        "partition_key": msg.partition_key(),
                        "event_timestamp": msg.event_timestamp()
                    }
                )
                
                # Process event
                try:
                    await self._process_event(msg, route, context)
                    
                    # Add to batch if batching is enabled
                    if route.batch_size > 1:
                        batch.append((msg, context))
                        
                        if len(batch) >= route.batch_size:
                            await self._process_batch(batch, route)
                            batch = []
                            
                            # Acknowledge all messages in batch
                            for batch_msg, _ in batch:
                                consumer.acknowledge(batch_msg)
                    else:
                        # Acknowledge immediately for non-batched processing
                        consumer.acknowledge(msg)
                        
                    # Record success metric
                    await self.metrics.record_event_processed(
                        route_name=route_name,
                        source_topic=context.source_topic,
                        success=True,
                        tenant_id=context.tenant_id
                    )
                    
                except Exception as e:
                    logger.error(f"Error processing event in route '{route_name}': {e}")
                    
                    # Handle error based on configuration
                    await self._handle_processing_error(msg, route, context, e)
                    
                    # Record error metric
                    await self.metrics.record_event_processed(
                        route_name=route_name,
                        source_topic=context.source_topic,
                        success=False,
                        error=str(e),
                        tenant_id=context.tenant_id
                    )
                    
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error in route processor '{route_name}': {e}")
                    
                # Process any remaining batched messages
                if batch:
                    await self._process_batch(batch, route)
                    for batch_msg, _ in batch:
                        consumer.acknowledge(batch_msg)
                    batch = []
                    
    async def _process_event(self, msg: pulsar.Message, route: Route, 
                           context: EventContext) -> None:
        """Process a single event through the routing pipeline"""
        # Get event data
        event_data = msg.value() if hasattr(msg, 'value') else json.loads(msg.data())
        
        # Store original event
        await self.event_store.store_event(
            event_id=context.event_id,
            event_data=event_data,
            context=context.__dict__
        )
        
        # Apply filters
        if not self._apply_filters(event_data, route.filters):
            logger.debug(f"Event {context.event_id} filtered out by route '{route.name}'")
            return
            
        # Apply transformations
        if route.transformations:
            event_data = await self.transformation_engine.transform(
                event_data,
                route.transformations,
                context
            )
            
        # Apply enrichments
        if route.enrichments:
            event_data = await self._enrich_event(
                event_data,
                route.enrichments,
                context
            )
            
        # Route to target topics
        for target_topic in route.target_topics:
            # Handle tenant-aware topics
            if "{tenant}" in target_topic and context.tenant_id:
                target_topic = target_topic.replace("{tenant}", context.tenant_id)
                
            await self._send_to_topic(
                target_topic,
                event_data,
                context,
                route
            )
            
    async def _process_batch(self, batch: List[Tuple[pulsar.Message, EventContext]], 
                           route: Route) -> None:
        """Process a batch of events"""
        # Extract event data from batch
        events = []
        for msg, context in batch:
            event_data = msg.value() if hasattr(msg, 'value') else json.loads(msg.data())
            events.append({
                "data": event_data,
                "context": context
            })
            
        # Apply batch transformations
        if route.transformations:
            transformed = await self.transformation_engine.transform_batch(
                events,
                route.transformations
            )
            events = transformed
            
        # Send batch to target topics
        for target_topic in route.target_topics:
            await self._send_batch_to_topic(
                target_topic,
                events,
                route
            )
            
    def _apply_filters(self, event_data: Dict[str, Any], 
                      filters: List[Dict[str, Any]]) -> bool:
        """Apply filters to determine if event should be routed"""
        if not filters:
            return True
            
        for filter_config in filters:
            field = filter_config.get("field")
            operator = filter_config.get("operator")
            value = filter_config.get("value")
            
            # Get field value using dot notation
            field_value = self._get_nested_value(event_data, field)
            
            # Apply operator
            if operator == "equals" and field_value != value:
                return False
            elif operator == "not_equals" and field_value == value:
                return False
            elif operator == "in" and field_value not in value:
                return False
            elif operator == "not_in" and field_value in value:
                return False
            elif operator == "contains" and value not in str(field_value):
                return False
            elif operator == "regex" and not re.match(value, str(field_value)):
                return False
            elif operator == "exists" and field_value is None:
                return False
            elif operator == "not_exists" and field_value is not None:
                return False
                
        return True
        
    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """Get nested value from dict using dot notation"""
        parts = path.split(".")
        value = data
        
        for part in parts:
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                return None
                
        return value
        
    async def _enrich_event(self, event_data: Dict[str, Any],
                          enrichments: List[Dict[str, Any]],
                          context: EventContext) -> Dict[str, Any]:
        """Enrich event with additional data"""
        enriched_data = event_data.copy()
        
        for enrichment in enrichments:
            enrichment_type = enrichment.get("type")
            
            if enrichment_type == "metadata":
                # Add metadata from context
                enriched_data["_metadata"] = {
                    "event_id": context.event_id,
                    "timestamp": context.timestamp.isoformat(),
                    "source_topic": context.source_topic,
                    "tenant_id": context.tenant_id
                }
                
            elif enrichment_type == "lookup":
                # Lookup data from external source
                source = enrichment.get("source")
                key_field = enrichment.get("key_field")
                target_field = enrichment.get("target_field", "enrichment")
                
                if key_field and source:
                    key_value = self._get_nested_value(event_data, key_field)
                    if key_value:
                        # TODO: Implement actual lookup logic
                        enriched_data[target_field] = {
                            "source": source,
                            "key": key_value,
                            "data": {}  # Placeholder
                        }
                        
        return enriched_data
        
    async def _send_to_topic(self, topic: str, event_data: Dict[str, Any],
                           context: EventContext, route: Route) -> None:
        """Send event to target topic"""
        # Get or create producer
        producer = await self._get_producer(topic)
        
        if not producer:
            logger.error(f"No producer available for topic '{topic}'")
            return
            
        try:
            # Create message
            message_data = json.dumps(event_data).encode('utf-8')
            
            # Send message with properties
            await producer.send_async(
                message_data,
                properties={
                    "event_id": context.event_id,
                    "source_route": route.name,
                    "correlation_id": context.correlation_id or context.event_id,
                    "causation_id": context.causation_id or context.event_id
                },
                partition_key=context.tenant_id or ""
            )
            
            logger.debug(f"Routed event {context.event_id} to topic '{topic}'")
            
        except Exception as e:
            logger.error(f"Failed to send event to topic '{topic}': {e}")
            raise
            
    async def _send_batch_to_topic(self, topic: str, events: List[Dict[str, Any]],
                                 route: Route) -> None:
        """Send batch of events to target topic"""
        producer = await self._get_producer(topic)
        
        if not producer:
            logger.error(f"No producer available for topic '{topic}'")
            return
            
        # Send all events in batch
        for event in events:
            event_data = event["data"]
            context = event["context"]
            
            message_data = json.dumps(event_data).encode('utf-8')
            
            await producer.send_async(
                message_data,
                properties={
                    "event_id": context.event_id,
                    "source_route": route.name,
                    "batch": "true"
                }
            )
            
    async def _get_producer(self, topic: str) -> Optional[pulsar.Producer]:
        """Get or create producer for topic"""
        if topic in self.producers:
            return self.producers[topic]
            
        # Create new producer
        try:
            producer = self.client.create_producer(
                topic,
                producer_name=f"event-router-dynamic-{topic.split('/')[-1]}",
                batching_enabled=True,
                batching_max_messages=1000,
                batching_max_publish_delay_ms=10
            )
            
            self.producers[topic] = producer
            logger.info(f"Created dynamic producer for topic '{topic}'")
            
            return producer
            
        except Exception as e:
            logger.error(f"Failed to create producer for topic '{topic}': {e}")
            return None
            
    async def _handle_processing_error(self, msg: pulsar.Message, route: Route,
                                     context: EventContext, error: Exception) -> None:
        """Handle event processing errors"""
        if route.error_handling == "dead_letter":
            # Send to dead letter queue
            await self.dead_letter_handler.send_to_dlq(
                msg,
                context,
                error,
                route.name
            )
            
        elif route.error_handling == "retry":
            # Retry processing
            if context.retry_count < route.max_retries:
                context.retry_count += 1
                
                # Re-queue for retry with delay
                retry_delay = min(2 ** context.retry_count, 60)  # Exponential backoff
                await asyncio.sleep(retry_delay)
                
                # Retry processing
                try:
                    await self._process_event(msg, route, context)
                except Exception as retry_error:
                    # If retry fails, send to DLQ
                    await self.dead_letter_handler.send_to_dlq(
                        msg,
                        context,
                        retry_error,
                        route.name
                    )
            else:
                # Max retries exceeded, send to DLQ
                await self.dead_letter_handler.send_to_dlq(
                    msg,
                    context,
                    error,
                    route.name
                )
                
        elif route.error_handling == "ignore":
            # Log and ignore
            logger.warning(f"Ignoring error for event {context.event_id}: {error}")
            
    def _extract_tenant_id(self, topic: str) -> Optional[str]:
        """Extract tenant ID from topic name"""
        # Pattern: persistent://platformq/{tenant_id}/...
        match = re.match(r"persistent://platformq/([^/]+)/.*", topic)
        if match:
            tenant_id = match.group(1)
            # Skip if it's a system topic
            if tenant_id not in ["monitoring", "system", "public"]:
                return tenant_id
        return None
        
    async def add_route(self, route: Route) -> None:
        """Add a new route dynamically"""
        self.routes[route.name] = route
        
        # Setup consumer if running
        if self._running and route.enabled:
            # Create consumer
            consumer = self.client.subscribe(
                route.source_pattern,
                subscription_name=f"event-router-{route.name}",
                consumer_type=pulsar.ConsumerType.Shared,
                pattern_auto_discovery_period=60,
                is_regex=True
            )
            
            self.consumers[route.name] = consumer
            
            # Start processing task
            task = asyncio.create_task(
                self._route_processor(route.name, route)
            )
            self._tasks.append(task)
            
        logger.info(f"Added new route '{route.name}'")
        
    async def remove_route(self, route_name: str) -> None:
        """Remove a route dynamically"""
        if route_name in self.routes:
            # Disable route
            self.routes[route_name].enabled = False
            
            # Close consumer
            if route_name in self.consumers:
                await self.consumers[route_name].close()
                del self.consumers[route_name]
                
            # Remove route
            del self.routes[route_name]
            
            logger.info(f"Removed route '{route_name}'")
            
    async def get_route_stats(self) -> Dict[str, Any]:
        """Get statistics for all routes"""
        stats = {}
        
        for route_name, route in self.routes.items():
            consumer = self.consumers.get(route_name)
            
            stats[route_name] = {
                "enabled": route.enabled,
                "source_pattern": route.source_pattern,
                "target_topics": route.target_topics,
                "consumer_connected": consumer is not None,
                "metrics": await self.metrics.get_route_metrics(route_name)
            }
            
        return stats
        
    async def shutdown(self):
        """Shutdown the event router"""
        logger.info("Shutting down event router...")
        
        self._running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # Close consumers
        for consumer in self.consumers.values():
            await consumer.close()
            
        # Close producers
        for producer in self.producers.values():
            await producer.close()
            
        # Close client
        if self.client:
            self.client.close()
            
        logger.info("Event router shutdown complete") 