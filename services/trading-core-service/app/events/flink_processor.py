"""Flink event processor for real-time trading analytics."""

import asyncio
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.connectors import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Types, Row
import pulsar

from .event_types import OrderEvent, TradeEvent, EventType


logger = logging.getLogger(__name__)


class FlinkEventProcessor:
    """Process trading events using Apache Flink."""
    
    def __init__(self, settings):
        self.settings = settings
        self.env = StreamExecutionEnvironment.get_execution_environment()
        self.t_env = StreamTableEnvironment.create(
            self.env,
            EnvironmentSettings.new_instance()
                .in_streaming_mode()
                .use_blink_planner()
                .build()
        )
        
        # Configure Flink
        self.env.set_parallelism(settings.FLINK_PARALLELISM)
        self.env.enable_checkpointing(settings.FLINK_CHECKPOINT_INTERVAL)
        
        # Pulsar client for event publishing
        self.pulsar_client: Optional[pulsar.Client] = None
        self.producers: Dict[str, pulsar.Producer] = {}
    
    async def initialize(self):
        """Initialize the Flink processor."""
        # Set up state backend
        if self.settings.FLINK_STATE_BACKEND == "rocksdb":
            # Configure RocksDB state backend
            pass
        
        # Initialize Pulsar client
        self.pulsar_client = pulsar.Client(
            self.settings.PULSAR_URL,
            authentication=None  # Add authentication if needed
        )
        
        # Create producers for different event types
        self.producers['orders'] = self.pulsar_client.create_producer(
            'persistent://public/trading/orders',
            batching_enabled=True,
            batching_max_messages=100,
            batching_max_publish_delay_ms=10
        )
        
        self.producers['trades'] = self.pulsar_client.create_producer(
            'persistent://public/trading/trades',
            batching_enabled=True,
            batching_max_messages=100,
            batching_max_publish_delay_ms=10
        )
        
        self.producers['market_data'] = self.pulsar_client.create_producer(
            'persistent://public/trading/market-data',
            batching_enabled=True,
            batching_max_messages=1000,
            batching_max_publish_delay_ms=50
        )
        
        self.producers['compute'] = self.pulsar_client.create_producer(
            'persistent://public/trading/compute-events',
            batching_enabled=True,
            batching_max_messages=100,
            batching_max_publish_delay_ms=50
        )
        
        self.producers['settlement'] = self.pulsar_client.create_producer(
            'persistent://public/trading/settlement-events',
            batching_enabled=True,
            batching_max_messages=50,
            batching_max_publish_delay_ms=100
        )
        
        # Define Flink jobs
        self._setup_order_aggregation_job()
        self._setup_risk_monitoring_job()
        self._setup_market_analytics_job()
        self._setup_compute_analytics_job()
        
        logger.info("Flink event processor initialized")
    
    def _setup_order_aggregation_job(self):
        """Set up order aggregation Flink job."""
        # Create order stream table
        self.t_env.execute_sql("""
            CREATE TABLE orders (
                order_id STRING,
                user_id STRING,
                market_id STRING,
                product_type STRING,
                side STRING,
                price DECIMAL(20, 8),
                quantity DECIMAL(20, 8),
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'pulsar',
                'topic' = 'persistent://public/trading/orders',
                'format' = 'json',
                'pulsar.service-url' = '{}'
            )
        """.format(self.settings.PULSAR_URL))
        
        # Order aggregation by user and market
        self.t_env.execute_sql("""
            CREATE VIEW order_aggregates AS
            SELECT 
                user_id,
                market_id,
                COUNT(*) as order_count,
                SUM(quantity) as total_volume,
                AVG(price) as avg_price,
                TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start,
                TUMBLE_END(event_time, INTERVAL '1' MINUTE) as window_end
            FROM orders
            GROUP BY 
                user_id,
                market_id,
                TUMBLE(event_time, INTERVAL '1' MINUTE)
        """)
    
    def _setup_risk_monitoring_job(self):
        """Set up risk monitoring Flink job."""
        # Create trade stream table
        self.t_env.execute_sql("""
            CREATE TABLE trades (
                trade_id STRING,
                market_id STRING,
                product_type STRING,
                price DECIMAL(20, 8),
                quantity DECIMAL(20, 8),
                value DECIMAL(20, 8),
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'pulsar',
                'topic' = 'persistent://public/trading/trades',
                'format' = 'json',
                'pulsar.service-url' = '{}'
            )
        """.format(self.settings.PULSAR_URL))
        
        # Risk monitoring - detect volume spikes
        self.t_env.execute_sql("""
            CREATE VIEW volume_alerts AS
            SELECT 
                market_id,
                SUM(value) as total_value,
                COUNT(*) as trade_count,
                MAX(price) as max_price,
                MIN(price) as min_price,
                TUMBLE_START(event_time, INTERVAL '5' MINUTE) as window_start
            FROM trades
            GROUP BY 
                market_id,
                TUMBLE(event_time, INTERVAL '5' MINUTE)
            HAVING SUM(value) > 1000000  -- Alert on high volume
        """)
    
    def _setup_market_analytics_job(self):
        """Set up market analytics Flink job."""
        # Market depth analysis
        self.t_env.execute_sql("""
            CREATE TABLE market_data (
                market_id STRING,
                best_bid DECIMAL(20, 8),
                best_ask DECIMAL(20, 8),
                bid_volume DECIMAL(20, 8),
                ask_volume DECIMAL(20, 8),
                spread DECIMAL(20, 8),
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
            ) WITH (
                'connector' = 'pulsar',
                'topic' = 'persistent://public/trading/market-data',
                'format' = 'json',
                'pulsar.service-url' = '{}'
            )
        """.format(self.settings.PULSAR_URL))
        
        # Market analytics - spread and liquidity monitoring
        self.t_env.execute_sql("""
            CREATE VIEW market_analytics AS
            SELECT 
                market_id,
                AVG(spread) as avg_spread,
                AVG(bid_volume + ask_volume) as avg_liquidity,
                COUNT(*) as updates_count,
                TUMBLE_START(event_time, INTERVAL '1' MINUTE) as window_start
            FROM market_data
            GROUP BY 
                market_id,
                TUMBLE(event_time, INTERVAL '1' MINUTE)
        """)
    
    def _setup_compute_analytics_job(self):
        """Set up compute resource analytics Flink job."""
        # Create compute events table
        self.t_env.execute_sql("""
            CREATE TABLE compute_events (
                event_type STRING,
                provider_id STRING,
                resource_type STRING,
                quantity DECIMAL(20, 8),
                price_per_hour DECIMAL(20, 8),
                allocation_id STRING,
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
            ) WITH (
                'connector' = 'pulsar',
                'topic' = 'persistent://public/trading/compute-events',
                'format' = 'json',
                'pulsar.service-url' = '{}'
            )
        """.format(self.settings.PULSAR_URL))
        
        # Compute utilization analytics
        self.t_env.execute_sql("""
            CREATE VIEW compute_utilization AS
            SELECT 
                resource_type,
                COUNT(DISTINCT provider_id) as active_providers,
                SUM(quantity) as total_allocated,
                AVG(price_per_hour) as avg_price,
                TUMBLE_START(event_time, INTERVAL '5' MINUTE) as window_start
            FROM compute_events
            WHERE event_type = 'resource_allocated'
            GROUP BY 
                resource_type,
                TUMBLE(event_time, INTERVAL '5' MINUTE)
        """)
    
    async def publish_order_event(self, event: OrderEvent):
        """Publish order event to Pulsar."""
        try:
            producer = self.producers.get('orders')
            if producer:
                message = json.dumps({
                    'order_id': event.order_id,
                    'user_id': event.user_id,
                    'market_id': event.market_id,
                    'product_type': event.product_type,
                    'side': event.order_data.get('side'),
                    'price': str(event.order_data.get('price', 0)),
                    'quantity': str(event.order_data.get('quantity', 0)),
                    'event_type': event.event_type.value,
                    'event_time': datetime.utcnow().isoformat()
                })
                
                await producer.send_async(
                    message.encode('utf-8'),
                    properties={
                        'event_type': event.event_type.value,
                        'market_id': event.market_id,
                        'user_id': event.user_id
                    }
                )
        except Exception as e:
            logger.error(f"Failed to publish order event: {e}")
    
    async def publish_trade_event(self, event: TradeEvent):
        """Publish trade event to Pulsar."""
        try:
            producer = self.producers.get('trades')
            if producer:
                message = json.dumps({
                    'trade_id': event.trade_id,
                    'market_id': event.market_id,
                    'product_type': event.product_type,
                    'price': str(event.price),
                    'quantity': str(event.quantity),
                    'value': str(event.price * event.quantity),
                    'event_type': event.event_type.value,
                    'event_time': datetime.utcnow().isoformat()
                })
                
                await producer.send_async(
                    message.encode('utf-8'),
                    properties={
                        'event_type': event.event_type.value,
                        'market_id': event.market_id
                    }
                )
        except Exception as e:
            logger.error(f"Failed to publish trade event: {e}")
    
    async def publish_market_data(self, snapshot: Any):
        """Publish market data snapshot to Pulsar."""
        try:
            producer = self.producers.get('market_data')
            if producer:
                # Extract data from orderbook snapshot
                best_bid = snapshot.bids[0][0] if snapshot.bids else 0
                best_ask = snapshot.asks[0][0] if snapshot.asks else 0
                bid_volume = sum(order[1] for order in snapshot.bids[:5]) if snapshot.bids else 0
                ask_volume = sum(order[1] for order in snapshot.asks[:5]) if snapshot.asks else 0
                
                message = json.dumps({
                    'market_id': snapshot.market_id,
                    'best_bid': str(best_bid),
                    'best_ask': str(best_ask),
                    'bid_volume': str(bid_volume),
                    'ask_volume': str(ask_volume),
                    'spread': str(best_ask - best_bid) if best_bid and best_ask else '0',
                    'event_time': datetime.utcnow().isoformat()
                })
                
                await producer.send_async(
                    message.encode('utf-8'),
                    properties={
                        'market_id': snapshot.market_id
                    }
                )
        except Exception as e:
            logger.error(f"Failed to publish market data: {e}")
    
    async def publish_compute_event(self, event: Dict[str, Any]):
        """Publish compute resource event to Pulsar."""
        try:
            producer = self.producers.get('compute')
            if producer:
                # Ensure all required fields
                event_data = {
                    'event_type': event.get('event_type'),
                    'provider_id': event.get('provider_id', ''),
                    'resource_type': event.get('resource_type', ''),
                    'quantity': str(event.get('quantity', 0)),
                    'price_per_hour': str(event.get('price_per_hour', 0)),
                    'allocation_id': event.get('allocation_id', ''),
                    'event_time': event.get('timestamp', datetime.utcnow().isoformat())
                }
                
                # Add allocation details if present
                if 'allocation' in event:
                    allocation = event['allocation']
                    event_data.update({
                        'provider_id': allocation.get('provider_id', ''),
                        'resource_type': allocation.get('resource_type', ''),
                        'quantity': allocation.get('quantity', '0'),
                        'price_per_hour': allocation.get('price_per_hour', '0'),
                        'allocation_id': allocation.get('allocation_id', '')
                    })
                
                message = json.dumps(event_data)
                
                await producer.send_async(
                    message.encode('utf-8'),
                    properties={
                        'event_type': event.get('event_type', 'unknown'),
                        'resource_type': event_data['resource_type']
                    }
                )
        except Exception as e:
            logger.error(f"Failed to publish compute event: {e}")
    
    async def publish_settlement_event(self, event: Dict[str, Any]):
        """Publish settlement event to Pulsar."""
        try:
            producer = self.producers.get('settlement')
            if producer:
                message = json.dumps({
                    'market_id': event.get('market_id'),
                    'settlement_price': event.get('settlement_price'),
                    'product_type': event.get('product_type'),
                    'settlement_type': event.get('settlement_type'),
                    'event_type': 'settlement',
                    'event_time': datetime.utcnow().isoformat()
                })
                
                await producer.send_async(
                    message.encode('utf-8'),
                    properties={
                        'event_type': 'settlement',
                        'market_id': event.get('market_id', ''),
                        'product_type': event.get('product_type', '')
                    }
                )
        except Exception as e:
            logger.error(f"Failed to publish settlement event: {e}")
    
    def start(self):
        """Start Flink job execution."""
        # This would typically be run in a separate process
        # self.env.execute("Trading Event Processing")
        pass
    
    async def stop(self):
        """Stop the event processor."""
        # Close producers
        for producer in self.producers.values():
            producer.close()
        
        # Close Pulsar client
        if self.pulsar_client:
            self.pulsar_client.close()
            
        logger.info("Flink event processor stopped") 