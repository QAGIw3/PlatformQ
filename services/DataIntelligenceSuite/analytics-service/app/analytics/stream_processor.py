"""
Stream Processor Module

Handles real-time stream processing with Apache Flink integration
and Apache Ignite for in-memory computing.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
import json

from pyignite import Client as IgniteClient
from pyignite.datatypes import String, Double, IntObject, TimestampObject
import httpx
from pulsar import Client as PulsarClient, Consumer, Producer

logger = logging.getLogger(__name__)


class StreamProcessor:
    """Unified stream processor for real-time analytics"""
    
    def __init__(self, druid_config: Dict[str, str], ignite_config: Dict[str, Any]):
        self.druid_config = druid_config
        self.ignite_config = ignite_config
        self.ignite_client = None
        self.pulsar_client = None
        self.consumers = {}
        self.producers = {}
        self.processing_rules = {}
        
    async def initialize(self):
        """Initialize connections"""
        # Initialize Ignite connection
        self.ignite_client = IgniteClient()
        self.ignite_client.connect(self.ignite_config['host'], self.ignite_config['port'])
        
        # Initialize Pulsar connection
        self.pulsar_client = PulsarClient('pulsar://pulsar:6650')
        
        # Create caches
        self._create_caches()
        
    def _create_caches(self):
        """Create Ignite caches for stream processing"""
        # Real-time metrics cache
        self.ignite_client.get_or_create_cache({
            'name': 'realtime_metrics',
            'cache_mode': 'PARTITIONED',
            'atomicity_mode': 'ATOMIC',
            'backups': 1
        })
        
        # Aggregations cache
        self.ignite_client.get_or_create_cache({
            'name': 'stream_aggregations',
            'cache_mode': 'REPLICATED',
            'atomicity_mode': 'ATOMIC'
        })
        
        # State cache for stateful processing
        self.ignite_client.get_or_create_cache({
            'name': 'processing_state',
            'cache_mode': 'PARTITIONED',
            'atomicity_mode': 'TRANSACTIONAL',
            'backups': 2
        })
        
    async def create_stream(self, stream_name: str, topic: str, 
                           processing_func: Callable, 
                           aggregation_window: Optional[int] = None):
        """Create a new stream processing pipeline"""
        consumer = self.pulsar_client.subscribe(
            topic,
            subscription_name=f"{stream_name}_processor",
            consumer_name=stream_name
        )
        
        self.consumers[stream_name] = consumer
        self.processing_rules[stream_name] = {
            'function': processing_func,
            'window': aggregation_window,
            'topic': topic
        }
        
        # Start processing
        asyncio.create_task(self._process_stream(stream_name))
        
    async def _process_stream(self, stream_name: str):
        """Process messages from a stream"""
        consumer = self.consumers[stream_name]
        rule = self.processing_rules[stream_name]
        
        while True:
            try:
                msg = consumer.receive(timeout_millis=1000)
                data = json.loads(msg.data().decode('utf-8'))
                
                # Process the message
                result = await rule['function'](data)
                
                # Store in Ignite for real-time access
                cache = self.ignite_client.get_cache('realtime_metrics')
                key = f"{stream_name}:{datetime.utcnow().timestamp()}"
                cache.put(key, result)
                
                # Handle windowed aggregations
                if rule['window']:
                    await self._update_aggregation(stream_name, result, rule['window'])
                
                consumer.acknowledge(msg)
                
            except Exception as e:
                logger.error(f"Error processing stream {stream_name}: {e}")
                await asyncio.sleep(1)
                
    async def _update_aggregation(self, stream_name: str, data: Dict, window: int):
        """Update windowed aggregations"""
        cache = self.ignite_client.get_cache('stream_aggregations')
        key = f"{stream_name}:window:{window}"
        
        # Get current aggregation
        current = cache.get(key) or {
            'count': 0,
            'sum': {},
            'min': {},
            'max': {},
            'window_start': datetime.utcnow()
        }
        
        # Update aggregations
        current['count'] += 1
        for metric, value in data.items():
            if isinstance(value, (int, float)):
                current['sum'][metric] = current['sum'].get(metric, 0) + value
                current['min'][metric] = min(current['min'].get(metric, float('inf')), value)
                current['max'][metric] = max(current['max'].get(metric, float('-inf')), value)
        
        cache.put(key, current)
        
    async def get_stream_metrics(self, stream_name: str, 
                                last_n_seconds: Optional[int] = 60) -> List[Dict]:
        """Get recent metrics from a stream"""
        cache = self.ignite_client.get_cache('realtime_metrics')
        
        # Query recent entries
        cutoff = datetime.utcnow().timestamp() - last_n_seconds
        query = f"SELECT * FROM realtime_metrics WHERE _key LIKE '{stream_name}:%' AND _key > '{stream_name}:{cutoff}'"
        
        results = []
        with cache.query_scan(query) as cursor:
            for key, value in cursor:
                results.append(value)
                
        return results
        
    async def get_aggregations(self, stream_name: str, window: int) -> Dict:
        """Get windowed aggregations for a stream"""
        cache = self.ignite_client.get_cache('stream_aggregations')
        key = f"{stream_name}:window:{window}"
        return cache.get(key) or {}
        
    async def close(self):
        """Close connections"""
        if self.ignite_client:
            self.ignite_client.close()
        if self.pulsar_client:
            self.pulsar_client.close() 