"""
Event Router Service Integration

Provides integration with the Event Router Service for:
- Real-time data ingestion from event streams
- Event-driven pipeline triggers
- Blockchain event data ingestion
- Stream processing pipeline coordination
"""

import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import asyncio
import json
import httpx
import pulsar

from ..lake.ingestion_engine import DataIngestionEngine
from ..pipelines.pipeline_coordinator import PipelineCoordinator
from ..lineage.lineage_tracker import DataLineageTracker
from ..quality.profiler import DataQualityProfiler

logger = logging.getLogger(__name__)


class EventRouterIntegration:
    """Integration with Event Router Service"""
    
    def __init__(self,
                 ingestion_engine: DataIngestionEngine,
                 pipeline_coordinator: PipelineCoordinator,
                 lineage_tracker: DataLineageTracker,
                 quality_profiler: DataQualityProfiler,
                 event_router_url: str = "http://event-router-service:8000",
                 pulsar_url: str = "pulsar://pulsar:6650"):
        self.ingestion_engine = ingestion_engine
        self.pipeline_coordinator = pipeline_coordinator
        self.lineage_tracker = lineage_tracker
        self.quality_profiler = quality_profiler
        self.event_router_url = event_router_url
        self.pulsar_url = pulsar_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Pulsar client and consumers
        self.pulsar_client = None
        self.consumers = {}
        self.producers = {}
        
        # Event handlers
        self.event_handlers = {}
        
    async def initialize(self):
        """Initialize Pulsar connections"""
        try:
            self.pulsar_client = pulsar.Client(self.pulsar_url)
            logger.info("Event router integration initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Pulsar client: {e}")
            raise
            
    async def subscribe_to_data_events(self,
                                      event_types: List[str],
                                      ingestion_config: Dict[str, Any],
                                      auto_start: bool = True) -> str:
        """
        Subscribe to data events for real-time ingestion
        
        Args:
            event_types: List of event types to subscribe to
            ingestion_config: Configuration for data ingestion
            auto_start: Start consuming immediately
            
        Returns:
            Subscription ID
        """
        try:
            subscription_id = f"data_ingestion_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            
            # Register event mappings with event router
            for event_type in event_types:
                await self._register_event_mapping(
                    event_type=event_type,
                    target_topic=f"data-platform.ingestion.{event_type}"
                )
                
            # Create consumer for ingestion topic
            topic = f"data-platform.ingestion.*"
            consumer = self.pulsar_client.subscribe(
                topic=topic,
                subscription_name=subscription_id,
                consumer_type=pulsar.ConsumerType.Shared,
                pattern_auto_discovery_period=60
            )
            
            self.consumers[subscription_id] = {
                "consumer": consumer,
                "config": ingestion_config,
                "handler": self._handle_data_event,
                "active": False
            }
            
            if auto_start:
                asyncio.create_task(self._consume_events(subscription_id))
                self.consumers[subscription_id]["active"] = True
                
            logger.info(f"Subscribed to data events: {event_types}")
            return subscription_id
            
        except Exception as e:
            logger.error(f"Failed to subscribe to data events: {e}")
            raise
            
    async def register_pipeline_trigger(self,
                                       event_type: str,
                                       pipeline_id: str,
                                       trigger_config: Dict[str, Any]) -> str:
        """
        Register event-based pipeline trigger
        
        Args:
            event_type: Event type that triggers pipeline
            pipeline_id: Pipeline to trigger
            trigger_config: Trigger configuration
            
        Returns:
            Trigger ID
        """
        try:
            trigger_id = f"trigger_{event_type}_{pipeline_id}"
            
            # Subscribe to event
            topic = f"persistent://public/default/{event_type}"
            consumer = self.pulsar_client.subscribe(
                topic=topic,
                subscription_name=trigger_id,
                consumer_type=pulsar.ConsumerType.Exclusive
            )
            
            self.consumers[trigger_id] = {
                "consumer": consumer,
                "config": {
                    "pipeline_id": pipeline_id,
                    "trigger_config": trigger_config
                },
                "handler": self._handle_pipeline_trigger,
                "active": True
            }
            
            # Start consuming
            asyncio.create_task(self._consume_events(trigger_id))
            
            logger.info(f"Registered pipeline trigger: {event_type} -> {pipeline_id}")
            return trigger_id
            
        except Exception as e:
            logger.error(f"Failed to register pipeline trigger: {e}")
            raise
            
    async def ingest_blockchain_events(self,
                                      chains: List[str],
                                      event_types: List[str],
                                      target_zone: str = "bronze") -> Dict[str, Any]:
        """
        Ingest blockchain events into data lake
        
        Args:
            chains: List of blockchain chains
            event_types: List of blockchain event types
            target_zone: Target zone in data lake
            
        Returns:
            Ingestion configuration
        """
        try:
            # Create ingestion configuration
            config = {
                "chains": chains,
                "event_types": event_types,
                "target_zone": target_zone,
                "target_path": f"blockchain_events/{datetime.utcnow().strftime('%Y/%m/%d')}",
                "format": "parquet",
                "partitioning": ["chain", "event_type", "date"]
            }
            
            # Subscribe to blockchain events
            subscription_id = await self.subscribe_to_data_events(
                event_types=[f"blockchain.{chain}.{event}" for chain in chains for event in event_types],
                ingestion_config=config
            )
            
            # Register data quality checks
            await self._register_quality_checks(
                data_source=config["target_path"],
                checks=[
                    {"type": "null_check", "columns": ["transaction_hash", "block_number"]},
                    {"type": "schema_validation", "schema": self._get_blockchain_schema()},
                    {"type": "anomaly_detection", "metrics": ["gas_price", "value"]}
                ]
            )
            
            return {
                "subscription_id": subscription_id,
                "config": config,
                "status": "active"
            }
            
        except Exception as e:
            logger.error(f"Failed to setup blockchain event ingestion: {e}")
            raise
            
    async def create_streaming_pipeline(self,
                                       source_events: List[str],
                                       transformations: List[Dict[str, Any]],
                                       sink_config: Dict[str, Any]) -> str:
        """
        Create a streaming data pipeline
        
        Args:
            source_events: Source event types
            transformations: List of transformations
            sink_config: Sink configuration
            
        Returns:
            Pipeline ID
        """
        try:
            # Create pipeline configuration
            pipeline_config = {
                "name": f"Streaming Pipeline {datetime.utcnow().isoformat()}",
                "type": "streaming",
                "steps": [
                    {
                        "name": "source",
                        "type": "source",
                        "connector": "pulsar",
                        "config": {
                            "topics": source_events,
                            "subscription": f"streaming_{datetime.utcnow().timestamp()}"
                        }
                    }
                ]
            }
            
            # Add transformations
            for i, transform in enumerate(transformations):
                pipeline_config["steps"].append({
                    "name": f"transform_{i}",
                    "type": "transform",
                    "config": transform
                })
                
            # Add sink
            pipeline_config["steps"].append({
                "name": "sink",
                "type": "sink",
                "config": sink_config
            })
            
            # Create pipeline
            pipeline_id = await self.pipeline_coordinator.create_pipeline(
                pipeline_id=f"streaming_{datetime.utcnow().timestamp()}",
                config=pipeline_config
            )
            
            # Start pipeline
            await self.pipeline_coordinator.execute_pipeline(pipeline_id)
            
            # Track lineage
            await self._track_streaming_lineage(
                pipeline_id=pipeline_id,
                source_events=source_events,
                sink_config=sink_config
            )
            
            return pipeline_id
            
        except Exception as e:
            logger.error(f"Failed to create streaming pipeline: {e}")
            raise
            
    async def publish_data_event(self,
                               event_type: str,
                               data: Dict[str, Any],
                               metadata: Optional[Dict[str, Any]] = None):
        """
        Publish data event to event router
        
        Args:
            event_type: Type of event
            data: Event data
            metadata: Optional metadata
        """
        try:
            # Get or create producer
            if event_type not in self.producers:
                self.producers[event_type] = self.pulsar_client.create_producer(
                    topic=f"persistent://public/default/{event_type}",
                    batching_enabled=True,
                    batching_max_publish_delay_ms=100
                )
                
            producer = self.producers[event_type]
            
            # Prepare message
            message = {
                "event_type": event_type,
                "timestamp": datetime.utcnow().isoformat(),
                "data": data,
                "metadata": metadata or {},
                "source": "data-platform-service"
            }
            
            # Send message
            producer.send_async(
                json.dumps(message).encode('utf-8'),
                callback=lambda res, msg: logger.debug(f"Event published: {event_type}")
            )
            
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            raise
            
    async def register_custom_handler(self,
                                    event_pattern: str,
                                    handler: Callable,
                                    config: Optional[Dict[str, Any]] = None):
        """
        Register custom event handler
        
        Args:
            event_pattern: Event pattern to match
            handler: Handler function
            config: Handler configuration
        """
        handler_id = f"custom_{len(self.event_handlers)}"
        self.event_handlers[handler_id] = {
            "pattern": event_pattern,
            "handler": handler,
            "config": config or {}
        }
        
        # Subscribe to pattern
        consumer = self.pulsar_client.subscribe(
            topic=event_pattern,
            subscription_name=handler_id,
            consumer_type=pulsar.ConsumerType.Shared,
            pattern_auto_discovery_period=60
        )
        
        self.consumers[handler_id] = {
            "consumer": consumer,
            "config": config or {},
            "handler": handler,
            "active": True
        }
        
        # Start consuming
        asyncio.create_task(self._consume_events(handler_id))
        
        logger.info(f"Registered custom handler for pattern: {event_pattern}")
        
    # Event handlers
    
    async def _handle_data_event(self, message: pulsar.Message, config: Dict[str, Any]):
        """Handle data ingestion event"""
        try:
            event_data = json.loads(message.data().decode('utf-8'))
            
            # Extract event info
            event_type = event_data.get("event_type", "unknown")
            data = event_data.get("data", {})
            
            # Determine target path
            target_path = f"{config['target_path']}/{event_type}/{datetime.utcnow().strftime('%Y%m%d_%H')}"
            
            # Ingest data
            result = await self.ingestion_engine.ingest(
                source_type="event",
                source_config={
                    "event_type": event_type,
                    "data": [data]  # Batch for efficiency
                },
                target_zone=config.get("target_zone", "bronze"),
                target_path=target_path,
                options={
                    "format": config.get("format", "parquet"),
                    "mode": "append"
                }
            )
            
            # Track lineage
            await self.lineage_tracker.add_node(
                node_type="event",
                name=f"{event_type}_event",
                namespace="events",
                attributes={
                    "event_type": event_type,
                    "timestamp": event_data.get("timestamp")
                }
            )
            
            logger.debug(f"Ingested event: {event_type}")
            
        except Exception as e:
            logger.error(f"Failed to handle data event: {e}")
            
    async def _handle_pipeline_trigger(self, message: pulsar.Message, config: Dict[str, Any]):
        """Handle pipeline trigger event"""
        try:
            event_data = json.loads(message.data().decode('utf-8'))
            
            # Extract trigger parameters
            pipeline_id = config["pipeline_id"]
            trigger_config = config["trigger_config"]
            
            # Check trigger conditions
            if self._evaluate_trigger_condition(event_data, trigger_config):
                # Execute pipeline
                run_id = await self.pipeline_coordinator.execute_pipeline(
                    pipeline_id=pipeline_id,
                    execution_params={
                        "trigger_event": event_data,
                        **trigger_config.get("parameters", {})
                    }
                )
                
                logger.info(f"Triggered pipeline {pipeline_id}: run {run_id}")
                
        except Exception as e:
            logger.error(f"Failed to handle pipeline trigger: {e}")
            
    # Helper methods
    
    async def _consume_events(self, subscription_id: str):
        """Consume events for a subscription"""
        consumer_info = self.consumers.get(subscription_id)
        if not consumer_info:
            return
            
        consumer = consumer_info["consumer"]
        handler = consumer_info["handler"]
        config = consumer_info["config"]
        
        while consumer_info["active"]:
            try:
                # Receive message with timeout
                msg = consumer.receive(timeout_millis=1000)
                
                # Handle message
                await handler(msg, config)
                
                # Acknowledge
                consumer.acknowledge(msg)
                
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error consuming events for {subscription_id}: {e}")
                await asyncio.sleep(0.1)
                
    async def _register_event_mapping(self, event_type: str, target_topic: str):
        """Register event mapping with event router"""
        try:
            response = await self.http_client.put(
                f"{self.event_router_url}/api/v1/blockchain/mappings",
                json={
                    "event_type": event_type,
                    "target_topics": [target_topic]
                }
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to register event mapping: {e}")
            
    async def _register_quality_checks(self, data_source: str, checks: List[Dict[str, Any]]):
        """Register data quality checks"""
        for check in checks:
            await self.quality_profiler.register_quality_rule(
                data_source=data_source,
                rule=check
            )
            
    def _get_blockchain_schema(self) -> Dict[str, Any]:
        """Get blockchain event schema"""
        return {
            "type": "object",
            "properties": {
                "transaction_hash": {"type": "string"},
                "block_number": {"type": "integer"},
                "chain": {"type": "string"},
                "event_type": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"},
                "data": {"type": "object"}
            },
            "required": ["transaction_hash", "block_number", "chain", "event_type"]
        }
        
    def _evaluate_trigger_condition(self, event_data: Dict[str, Any], trigger_config: Dict[str, Any]) -> bool:
        """Evaluate if trigger condition is met"""
        condition = trigger_config.get("condition", {})
        
        # Simple condition evaluation
        for field, expected in condition.items():
            if event_data.get(field) != expected:
                return False
                
        return True
        
    async def _track_streaming_lineage(self,
                                     pipeline_id: str,
                                     source_events: List[str],
                                     sink_config: Dict[str, Any]):
        """Track lineage for streaming pipeline"""
        # Create pipeline node
        pipeline_node = await self.lineage_tracker.add_node(
            node_type="process",
            name=f"streaming_pipeline_{pipeline_id}",
            namespace="pipelines",
            attributes={"pipeline_id": pipeline_id}
        )
        
        # Create edges from sources
        for event_type in source_events:
            source_node = await self.lineage_tracker.add_node(
                node_type="stream",
                name=event_type,
                namespace="events"
            )
            
            await self.lineage_tracker.add_edge(
                source_id=source_node["node_id"],
                target_id=pipeline_node["node_id"],
                edge_type="reads"
            )
            
        # Create edge to sink
        sink_node = await self.lineage_tracker.add_node(
            node_type=sink_config.get("type", "table"),
            name=sink_config.get("name", "unknown"),
            namespace=sink_config.get("namespace", "data")
        )
        
        await self.lineage_tracker.add_edge(
            source_id=pipeline_node["node_id"],
            target_id=sink_node["node_id"],
            edge_type="writes"
        )
        
    async def close(self):
        """Clean up resources"""
        # Stop consumers
        for sub_id, info in self.consumers.items():
            info["active"] = False
            info["consumer"].close()
            
        # Close producers
        for producer in self.producers.values():
            producer.close()
            
        # Close Pulsar client
        if self.pulsar_client:
            self.pulsar_client.close()
            
        # Close HTTP client
        await self.http_client.aclose() 