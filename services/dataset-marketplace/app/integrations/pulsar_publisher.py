"""
Apache Pulsar Event Publisher
"""

import asyncio
from typing import Any, Dict, Optional
import logging
import json
from datetime import datetime
import pulsar
from pulsar.schema import AvroSchema, Record, String, Float, Long, Array, Map

logger = logging.getLogger(__name__)


class PulsarEventPublisher:
    """Apache Pulsar event publisher"""
    
    def __init__(self, client: Optional[pulsar.Client] = None):
        self._client = client
        self._producers: Dict[str, pulsar.Producer] = {}
    
    async def initialize(self):
        """Initialize the publisher"""
        if not self._client:
            self._client = pulsar.Client('pulsar://pulsar-broker:6650')
        logger.info("Pulsar event publisher initialized")
    
    def _get_producer(self, topic: str, schema: Optional[Any] = None) -> pulsar.Producer:
        """Get or create a producer for a topic"""
        if topic not in self._producers:
            producer_config = {
                'topic': topic,
                'batching_enabled': True,
                'batching_max_publish_delay_ms': 10,
                'compression_type': pulsar.CompressionType.SNAPPY
            }
            
            if schema:
                producer_config['schema'] = schema
            
            self._producers[topic] = self._client.create_producer(**producer_config)
        
        return self._producers[topic]
    
    async def publish_event(
        self,
        topic: str,
        event_data: Dict[str, Any],
        event_key: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None
    ):
        """Publish an event to a topic"""
        try:
            producer = self._get_producer(topic)
            
            # Add timestamp if not present
            if 'timestamp' not in event_data:
                event_data['timestamp'] = datetime.utcnow().isoformat()
            
            # Convert to JSON bytes
            message = json.dumps(event_data).encode('utf-8')
            
            # Send message
            message_id = producer.send(
                message,
                event_timestamp=int(datetime.utcnow().timestamp() * 1000),
                partition_key=event_key,
                properties=properties or {}
            )
            
            logger.debug(f"Published event to {topic}: {message_id}")
            
        except Exception as e:
            logger.error(f"Failed to publish event to {topic}: {e}")
            raise
    
    async def publish_quality_assessment(self, assessment: Dict[str, Any]):
        """Publish data quality assessment event"""
        await self.publish_event(
            topic='platformq.data.marketplace.quality-assessments',
            event_data=assessment,
            event_key=assessment.get('dataset_id')
        )
    
    async def publish_access_request(self, request: Dict[str, Any]):
        """Publish data access request event"""
        await self.publish_event(
            topic='platformq.data.marketplace.access-requests',
            event_data=request,
            event_key=request.get('dataset_id')
        )
    
    async def publish_access_grant(self, grant: Dict[str, Any]):
        """Publish data access grant event"""
        await self.publish_event(
            topic='platformq.data.marketplace.access-grants',
            event_data=grant,
            event_key=grant.get('dataset_id')
        )
    
    async def publish_pricing_update(self, pricing: Dict[str, Any]):
        """Publish pricing update event"""
        await self.publish_event(
            topic='platformq.data.marketplace.pricing-updates',
            event_data=pricing,
            event_key=pricing.get('dataset_id')
        )
    
    async def publish_lineage_event(self, lineage: Dict[str, Any]):
        """Publish data lineage event"""
        await self.publish_event(
            topic='platformq.data.marketplace.lineage-events',
            event_data=lineage,
            event_key=lineage.get('dataset_id')
        )
    
    async def publish_knowledge_node(self, node: Dict[str, Any]):
        """Publish knowledge graph node creation event"""
        await self.publish_event(
            topic='platformq.knowledge.graph.nodes',
            event_data=node,
            event_key=node.get('node_id')
        )
    
    async def publish_knowledge_edge(self, edge: Dict[str, Any]):
        """Publish knowledge graph edge creation event"""
        await self.publish_event(
            topic='platformq.knowledge.graph.edges',
            event_data=edge,
            event_key=f"{edge.get('source_node_id')}:{edge.get('target_node_id')}"
        )
    
    async def publish_dataset_upload(self, dataset: Dict[str, Any]):
        """Publish dataset upload event for quality monitoring"""
        await self.publish_event(
            topic='dataset-uploads',
            event_data=dataset,
            event_key=dataset.get('dataset_id')
        )
    
    async def close(self):
        """Close all producers"""
        for producer in self._producers.values():
            producer.close()
        
        if self._client:
            self._client.close()
        
        logger.info("Pulsar event publisher closed") 