from typing import Dict, List, Optional, Any
import asyncio
import logging
from datetime import datetime, timedelta
import json
import uuid
import hmac
import hashlib
from collections import defaultdict

import aiopulsar
from pyignite import AsyncClient as IgniteClient
import aioredis
import httpx
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from tenacity import retry, stop_after_attempt, wait_exponential

from ..config import config
from ..models.event_models import (
    BlockchainEvent, EventStatus, EventType,
    EventSubscription, WebhookDelivery, EventStatistics,
    AlertRule, EventFilter
)
from ..monitors.base_monitor import BaseMonitor
from ..monitors.evm_monitor import EVMMonitor


class EventProcessor:
    """Processes blockchain events and manages webhooks"""
    
    def __init__(
        self,
        pulsar_client: aiopulsar.Client,
        ignite_client: IgniteClient,
        redis_client: aioredis.Redis
    ):
        self.pulsar_client = pulsar_client
        self.ignite_client = ignite_client
        self.redis_client = redis_client
        
        self.logger = logging.getLogger(__name__)
        
        # Database engine
        self.engine = create_async_engine(
            config.database_url,
            pool_size=config.database_pool_size,
            max_overflow=config.database_max_overflow
        )
        self.async_session = sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        # Event queue and monitors
        self.event_queue = asyncio.Queue(maxsize=config.event_queue_size)
        self.monitors: Dict[str, BaseMonitor] = {}
        
        # HTTP client for webhooks
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Pulsar producers
        self.event_producer: Optional[aiopulsar.Producer] = None
        self.webhook_producer: Optional[aiopulsar.Producer] = None
        
        # Background tasks
        self.tasks: List[asyncio.Task] = []
        self._running = False
        
        # Statistics
        self.stats = defaultdict(lambda: defaultdict(int))
        
    async def initialize(self) -> None:
        """Initialize event processor"""
        self.logger.info("Initializing event processor")
        
        # Create database tables
        # In production, would use Alembic migrations
        
        # Setup Pulsar producers
        self.event_producer = await self.pulsar_client.create_producer(
            config.events_topic
        )
        self.webhook_producer = await self.pulsar_client.create_producer(
            config.webhook_topic
        )
        
        # Create Ignite caches
        await self._create_caches()
        
        # Initialize monitors
        await self._initialize_monitors()
        
        self._running = True
        
        # Start background tasks
        self.tasks.append(asyncio.create_task(self._process_events()))
        self.tasks.append(asyncio.create_task(self._process_webhooks()))
        self.tasks.append(asyncio.create_task(self._cleanup_old_events()))
        self.tasks.append(asyncio.create_task(self._update_statistics()))
        
        # Start monitors
        for monitor in self.monitors.values():
            self.tasks.append(asyncio.create_task(monitor.start()))
    
    async def shutdown(self) -> None:
        """Shutdown event processor"""
        self.logger.info("Shutting down event processor")
        self._running = False
        
        # Stop monitors
        for monitor in self.monitors.values():
            await monitor.stop()
        
        # Cancel background tasks
        for task in self.tasks:
            task.cancel()
        
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Close resources
        if self.event_producer:
            await self.event_producer.close()
        if self.webhook_producer:
            await self.webhook_producer.close()
        
        await self.http_client.aclose()
        await self.engine.dispose()
    
    async def _create_caches(self) -> None:
        """Create Ignite caches"""
        # Event cache
        self.event_cache = await self.ignite_client.get_or_create_cache({
            'name': 'blockchain_events',
            'key_type': 'str',
            'value_type': 'str'
        })
        
        # Subscription cache
        self.subscription_cache = await self.ignite_client.get_or_create_cache({
            'name': 'event_subscriptions',
            'key_type': 'str',
            'value_type': 'str'
        })
        
        # Alert rules cache
        self.alert_cache = await self.ignite_client.get_or_create_cache({
            'name': 'alert_rules',
            'key_type': 'str',
            'value_type': 'str'
        })
    
    async def _initialize_monitors(self) -> None:
        """Initialize blockchain monitors"""
        for monitor_config in config.monitors:
            try:
                if self._is_evm_chain(monitor_config.chain):
                    monitor = EVMMonitor(
                        chain=monitor_config.chain,
                        config=monitor_config,
                        event_queue=self.event_queue
                    )
                    self.monitors[monitor_config.chain] = monitor
                    self.logger.info(f"Initialized monitor for {monitor_config.chain}")
                else:
                    self.logger.warning(
                        f"No monitor implementation for {monitor_config.chain}"
                    )
            except Exception as e:
                self.logger.error(
                    f"Failed to initialize monitor for {monitor_config.chain}: {e}"
                )
    
    def _is_evm_chain(self, chain: str) -> bool:
        """Check if chain is EVM-compatible"""
        evm_chains = ['ethereum', 'polygon', 'bsc', 'avalanche', 'arbitrum', 'optimism']
        return chain in evm_chains
    
    async def _process_events(self) -> None:
        """Process events from queue"""
        while self._running:
            try:
                # Get event from queue with timeout
                event = await asyncio.wait_for(
                    self.event_queue.get(),
                    timeout=1.0
                )
                
                try:
                    await self._handle_event(event)
                except Exception as e:
                    self.logger.error(f"Error handling event {event.event_id}: {e}")
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error in event processor: {e}")
                await asyncio.sleep(1)
    
    async def _handle_event(self, event: BlockchainEvent) -> None:
        """Handle a single event"""
        self.logger.debug(f"Processing event {event.event_id}")
        
        # Update status
        event.status = EventStatus.PROCESSING
        event.processed_at = datetime.utcnow()
        
        # Save to cache
        await self.event_cache.put(event.event_id, event.json())
        
        # Publish to Pulsar
        await self.event_producer.send(event.json().encode())
        
        # Check subscriptions
        subscriptions = await self._get_matching_subscriptions(event)
        
        for subscription in subscriptions:
            if subscription.webhook_url:
                await self._queue_webhook_delivery(event, subscription)
        
        # Check alert rules
        await self._check_alert_rules(event)
        
        # Update status
        event.status = EventStatus.PROCESSED
        await self.event_cache.put(event.event_id, event.json())
        
        # Update statistics
        self.stats[event.chain]['events_processed'] += 1
        self.stats[event.chain][f'event_type_{event.event_type.value}'] += 1
    
    async def _get_matching_subscriptions(
        self,
        event: BlockchainEvent
    ) -> List[EventSubscription]:
        """Get subscriptions matching the event"""
        matching = []
        
        # In production, would query from database
        # For now, check all cached subscriptions
        subscriptions = []  # Would load from cache/database
        
        for subscription in subscriptions:
            if not subscription.is_active:
                continue
            
            if subscription.chain != event.chain:
                continue
            
            if subscription.contract_address:
                if subscription.contract_address.lower() != event.contract_address.lower():
                    continue
            
            # Check event filters
            if subscription.event_filters:
                matches = False
                for filter_dict in subscription.event_filters:
                    event_filter = EventFilter(**filter_dict)
                    if self._event_matches_filter(event, event_filter):
                        matches = True
                        break
                if not matches:
                    continue
            
            matching.append(subscription)
        
        return matching
    
    def _event_matches_filter(
        self,
        event: BlockchainEvent,
        filter: EventFilter
    ) -> bool:
        """Check if event matches filter"""
        if filter.chain and filter.chain != event.chain:
            return False
        
        if filter.contract_address:
            if filter.contract_address.lower() != event.contract_address.lower():
                return False
        
        if filter.event_name and filter.event_name != event.event_name:
            return False
        
        if filter.event_type and filter.event_type != event.event_type:
            return False
        
        if filter.from_block and event.block_number < filter.from_block:
            return False
        
        if filter.to_block and event.block_number > filter.to_block:
            return False
        
        return True
    
    async def _queue_webhook_delivery(
        self,
        event: BlockchainEvent,
        subscription: EventSubscription
    ) -> None:
        """Queue webhook for delivery"""
        delivery = WebhookDelivery(
            delivery_id=str(uuid.uuid4()),
            subscription_id=subscription.subscription_id,
            event_id=event.event_id,
            url=subscription.webhook_url,
            headers=subscription.webhook_headers,
            payload={
                'event': event.dict(),
                'subscription_id': subscription.subscription_id,
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        
        # Add signature if secret is configured
        if subscription.webhook_secret:
            signature = self._create_webhook_signature(
                delivery.payload,
                subscription.webhook_secret
            )
            delivery.headers['X-Webhook-Signature'] = signature
        
        # Queue for delivery
        await self.webhook_producer.send(delivery.json().encode())
        
        # Update event status
        event.status = EventStatus.WEBHOOK_PENDING
        await self.event_cache.put(event.event_id, event.json())
    
    def _create_webhook_signature(
        self,
        payload: Dict[str, Any],
        secret: str
    ) -> str:
        """Create HMAC signature for webhook"""
        payload_bytes = json.dumps(payload, sort_keys=True).encode()
        signature = hmac.new(
            secret.encode(),
            payload_bytes,
            hashlib.sha256
        ).hexdigest()
        return f"sha256={signature}"
    
    async def _process_webhooks(self) -> None:
        """Process webhook deliveries"""
        consumer = await self.pulsar_client.subscribe(
            config.webhook_topic,
            subscription_name=f"{config.service_name}-webhooks",
            consumer_type=aiopulsar.ConsumerType.Shared
        )
        
        while self._running:
            try:
                msg = await consumer.receive(timeout_millis=1000)
                
                try:
                    delivery_data = json.loads(msg.data().decode())
                    delivery = WebhookDelivery(**delivery_data)
                    
                    await self._deliver_webhook(delivery)
                    await consumer.acknowledge(msg)
                    
                except Exception as e:
                    self.logger.error(f"Error processing webhook: {e}")
                    await consumer.negative_acknowledge(msg)
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"Error in webhook processor: {e}")
                await asyncio.sleep(1)
        
        await consumer.close()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=60)
    )
    async def _deliver_webhook(self, delivery: WebhookDelivery) -> None:
        """Deliver webhook with retries"""
        delivery.attempts += 1
        
        try:
            response = await self.http_client.post(
                delivery.url,
                json=delivery.payload,
                headers=delivery.headers,
                timeout=config.webhook_timeout_seconds
            )
            
            delivery.response_status = response.status_code
            delivery.response_body = response.text[:1000]  # Limit size
            
            if response.status_code >= 200 and response.status_code < 300:
                delivery.status = "delivered"
                delivery.delivered_at = datetime.utcnow()
                
                # Update event status
                await self._update_event_webhook_status(
                    delivery.event_id,
                    EventStatus.WEBHOOK_DELIVERED
                )
                
                self.stats['webhooks']['successful'] += 1
            else:
                raise Exception(f"HTTP {response.status_code}: {response.text}")
                
        except Exception as e:
            delivery.error_message = str(e)
            delivery.status = "failed"
            
            if delivery.attempts < delivery.max_attempts:
                delivery.next_retry_at = datetime.utcnow() + timedelta(
                    seconds=config.webhook_retry_delay * delivery.attempts
                )
                # Re-queue for retry
                await self.webhook_producer.send(delivery.json().encode())
            else:
                # Max retries reached
                await self._update_event_webhook_status(
                    delivery.event_id,
                    EventStatus.WEBHOOK_FAILED
                )
                
                self.stats['webhooks']['failed'] += 1
    
    async def _update_event_webhook_status(
        self,
        event_id: str,
        status: EventStatus
    ) -> None:
        """Update event webhook status"""
        event_data = await self.event_cache.get(event_id)
        if event_data:
            event = BlockchainEvent(**json.loads(event_data))
            event.status = status
            await self.event_cache.put(event_id, event.json())
    
    async def _check_alert_rules(self, event: BlockchainEvent) -> None:
        """Check if event triggers any alerts"""
        # In production, would load from database
        alert_rules = []  # Would load active alert rules
        
        for rule in alert_rules:
            if await self._evaluate_alert_rule(event, rule):
                await self._trigger_alert(event, rule)
    
    async def _evaluate_alert_rule(
        self,
        event: BlockchainEvent,
        rule: AlertRule
    ) -> bool:
        """Evaluate if event matches alert rule"""
        if rule.chain and rule.chain != event.chain:
            return False
        
        if rule.contract_address:
            if rule.contract_address.lower() != event.contract_address.lower():
                return False
        
        if rule.event_type and rule.event_type != event.event_type:
            return False
        
        # Check cooldown
        if rule.last_triggered_at:
            cooldown_end = rule.last_triggered_at + timedelta(
                seconds=rule.cooldown_seconds
            )
            if datetime.utcnow() < cooldown_end:
                return False
        
        # Evaluate condition
        # Simple example - in production would use more sophisticated rules
        if rule.condition and event.decoded_data:
            for field, criteria in rule.condition.items():
                value = event.decoded_data.get(field)
                if not value:
                    return False
                
                for op, threshold in criteria.items():
                    if op == "$gt" and not (value > threshold):
                        return False
                    elif op == "$lt" and not (value < threshold):
                        return False
                    elif op == "$eq" and not (value == threshold):
                        return False
        
        return True
    
    async def _trigger_alert(
        self,
        event: BlockchainEvent,
        rule: AlertRule
    ) -> None:
        """Trigger alert for matching rule"""
        self.logger.info(f"Alert triggered: {rule.name} for event {event.event_id}")
        
        # Update last triggered time
        rule.last_triggered_at = datetime.utcnow()
        await self.alert_cache.put(rule.rule_id, rule.json())
        
        # Send alerts to configured channels
        for channel in rule.alert_channels:
            if channel == "webhook" and rule.alert_config.get("webhook_url"):
                # Send webhook alert
                await self.http_client.post(
                    rule.alert_config["webhook_url"],
                    json={
                        "alert": rule.name,
                        "event": event.dict(),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
            # Add other channels (email, Slack, etc.) as needed
    
    async def _cleanup_old_events(self) -> None:
        """Clean up old events from storage"""
        while self._running:
            try:
                # Clean up events older than retention period
                cutoff_date = datetime.utcnow() - timedelta(
                    days=config.event_retention_days
                )
                
                # In production, would delete from database
                self.logger.info(f"Cleaning up events older than {cutoff_date}")
                
                await asyncio.sleep(3600)  # Run hourly
                
            except Exception as e:
                self.logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(3600)
    
    async def _update_statistics(self) -> None:
        """Update statistics periodically"""
        while self._running:
            try:
                # Calculate statistics for each chain
                for chain in self.stats:
                    stats = EventStatistics(
                        chain=chain,
                        period_start=datetime.utcnow() - timedelta(hours=1),
                        period_end=datetime.utcnow(),
                        total_events=self.stats[chain]['events_processed'],
                        webhooks_sent=self.stats['webhooks']['successful'] + 
                                     self.stats['webhooks']['failed'],
                        webhooks_successful=self.stats['webhooks']['successful'],
                        webhooks_failed=self.stats['webhooks']['failed']
                    )
                    
                    # Save to cache
                    await self.ignite_client.get_or_create_cache('event_statistics').put(
                        f"{chain}:latest",
                        stats.json()
                    )
                
                await asyncio.sleep(60)  # Update every minute
                
            except Exception as e:
                self.logger.error(f"Error updating statistics: {e}")
                await asyncio.sleep(60)
    
    async def get_monitor_status(self, chain: str) -> Optional[Dict[str, Any]]:
        """Get status of a specific monitor"""
        monitor = self.monitors.get(chain)
        if monitor:
            return monitor.get_status().dict()
        return None
    
    async def get_all_monitor_statuses(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all monitors"""
        statuses = {}
        for chain, monitor in self.monitors.items():
            statuses[chain] = monitor.get_status().dict()
        return statuses
    
    async def create_subscription(
        self,
        subscription: EventSubscription
    ) -> EventSubscription:
        """Create a new event subscription"""
        subscription.subscription_id = str(uuid.uuid4())
        subscription.created_at = datetime.utcnow()
        subscription.updated_at = datetime.utcnow()
        
        # Save to cache
        await self.subscription_cache.put(
            subscription.subscription_id,
            subscription.json()
        )
        
        # In production, would also save to database
        
        return subscription
    
    async def get_subscription(
        self,
        subscription_id: str
    ) -> Optional[EventSubscription]:
        """Get subscription by ID"""
        data = await self.subscription_cache.get(subscription_id)
        if data:
            return EventSubscription(**json.loads(data))
        return None
    
    async def update_subscription(
        self,
        subscription_id: str,
        updates: Dict[str, Any]
    ) -> Optional[EventSubscription]:
        """Update existing subscription"""
        subscription = await self.get_subscription(subscription_id)
        if not subscription:
            return None
        
        # Update fields
        for key, value in updates.items():
            if hasattr(subscription, key):
                setattr(subscription, key, value)
        
        subscription.updated_at = datetime.utcnow()
        
        # Save updated subscription
        await self.subscription_cache.put(
            subscription_id,
            subscription.json()
        )
        
        return subscription
    
    async def delete_subscription(self, subscription_id: str) -> bool:
        """Delete subscription"""
        subscription = await self.get_subscription(subscription_id)
        if not subscription:
            return False
        
        # Soft delete by marking inactive
        subscription.is_active = False
        subscription.updated_at = datetime.utcnow()
        
        await self.subscription_cache.put(
            subscription_id,
            subscription.json()
        )
        
        return True 