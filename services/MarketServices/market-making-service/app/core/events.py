"""Event publishing for Market Making Service"""

import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from enum import Enum

import pulsar
from pulsar.schema import JsonSchema

from app.config import settings
from app.core.dependencies import get_pulsar_client

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    """Market making event types"""
    # Pool events
    POOL_CREATED = "pool.created"
    POOL_UPDATED = "pool.updated"
    LIQUIDITY_ADDED = "liquidity.added"
    LIQUIDITY_REMOVED = "liquidity.removed"
    SWAP_EXECUTED = "swap.executed"
    
    # Strategy events
    STRATEGY_DEPLOYED = "strategy.deployed"
    STRATEGY_UPDATED = "strategy.updated"
    STRATEGY_STOPPED = "strategy.stopped"
    ORDER_PLACED = "order.placed"
    ORDER_FILLED = "order.filled"
    ORDER_CANCELLED = "order.cancelled"
    
    # Mining events
    MINING_PROGRAM_CREATED = "mining.program_created"
    REWARDS_DISTRIBUTED = "mining.rewards_distributed"
    REWARDS_CLAIMED = "mining.rewards_claimed"
    
    # Risk events
    POSITION_LIMIT_REACHED = "risk.position_limit"
    DRAWDOWN_ALERT = "risk.drawdown_alert"
    LIQUIDATION_WARNING = "risk.liquidation_warning"


class MarketMakingEvent:
    """Base event structure"""
    
    def __init__(
        self,
        event_type: EventType,
        data: Dict[str, Any],
        user_id: Optional[str] = None,
        correlation_id: Optional[str] = None
    ):
        self.event_id = self._generate_event_id()
        self.event_type = event_type
        self.timestamp = datetime.utcnow().isoformat()
        self.service = settings.SERVICE_NAME
        self.data = data
        self.user_id = user_id
        self.correlation_id = correlation_id
    
    def _generate_event_id(self) -> str:
        """Generate unique event ID"""
        import uuid
        return str(uuid.uuid4())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp,
            "service": self.service,
            "data": self.data,
            "user_id": self.user_id,
            "correlation_id": self.correlation_id
        }


class EventPublisher:
    """Publishes events to Pulsar"""
    
    def __init__(self):
        self._producers: Dict[str, pulsar.Producer] = {}
    
    async def _get_producer(self, topic: str) -> pulsar.Producer:
        """Get or create producer for topic"""
        if topic not in self._producers:
            client = await get_pulsar_client()
            self._producers[topic] = client.create_producer(
                topic,
                schema=JsonSchema(MarketMakingEvent),
                producer_name=f"{settings.SERVICE_NAME}-{topic}",
                batching_enabled=True,
                batching_max_publish_delay_ms=100
            )
        return self._producers[topic]
    
    async def publish(
        self,
        event_type: EventType,
        data: Dict[str, Any],
        user_id: Optional[str] = None,
        correlation_id: Optional[str] = None
    ):
        """Publish event to appropriate topic"""
        try:
            # Determine topic based on event type
            topic = self._get_topic_for_event(event_type)
            
            # Create event
            event = MarketMakingEvent(
                event_type=event_type,
                data=data,
                user_id=user_id,
                correlation_id=correlation_id
            )
            
            # Get producer and send
            producer = await self._get_producer(topic)
            producer.send_async(
                event.to_dict(),
                callback=lambda res, msg: logger.debug(f"Event sent: {event.event_id}")
            )
            
            logger.info(f"Published event: {event_type.value}")
            
        except Exception as e:
            logger.error(f"Failed to publish event {event_type}: {e}")
    
    def _get_topic_for_event(self, event_type: EventType) -> str:
        """Determine topic for event type"""
        if event_type.value.startswith("pool"):
            return f"{settings.PULSAR_TOPIC_PREFIX}/pool-events"
        elif event_type.value.startswith("strategy"):
            return f"{settings.PULSAR_TOPIC_PREFIX}/strategy-events"
        elif event_type.value.startswith("mining"):
            return f"{settings.PULSAR_TOPIC_PREFIX}/mining-events"
        elif event_type.value.startswith("risk"):
            return f"{settings.PULSAR_TOPIC_PREFIX}/risk-events"
        else:
            return f"{settings.PULSAR_TOPIC_PREFIX}/general-events"
    
    async def close(self):
        """Close all producers"""
        for producer in self._producers.values():
            producer.close()
        self._producers.clear()


# Singleton instance
event_publisher = EventPublisher()


async def publish_event(
    event_type: EventType,
    data: Dict[str, Any],
    user_id: Optional[str] = None,
    correlation_id: Optional[str] = None
):
    """Convenience function to publish events"""
    await event_publisher.publish(event_type, data, user_id, correlation_id) 