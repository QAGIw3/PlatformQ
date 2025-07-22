"""
Unified Market Event Schemas for PlatformQ

Standardized event types and schemas for all market-related events.
"""

import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from decimal import Decimal
from enum import Enum
from dataclasses import dataclass, field, asdict
import uuid

from pulsar.schema import JsonSchema, String, Integer, Float, Record


class EventType(str, Enum):
    """Standard event types across MarketServices"""
    # Order events
    ORDER_CREATED = "order.created"
    ORDER_UPDATED = "order.updated"
    ORDER_FILLED = "order.filled"
    ORDER_PARTIALLY_FILLED = "order.partially_filled"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_REJECTED = "order.rejected"
    
    # Trade events
    TRADE_EXECUTED = "trade.executed"
    TRADE_SETTLED = "trade.settled"
    
    # Market data events
    MARKET_DATA_UPDATE = "market.data.update"
    ORDERBOOK_UPDATE = "market.orderbook.update"
    BEST_BID_ASK_UPDATE = "market.best_bid_ask.update"
    
    # Position events
    POSITION_OPENED = "position.opened"
    POSITION_UPDATED = "position.updated"
    POSITION_CLOSED = "position.closed"
    POSITION_LIQUIDATED = "position.liquidated"
    
    # Risk events
    RISK_ALERT = "risk.alert"
    MARGIN_CALL = "risk.margin_call"
    LIQUIDATION_TRIGGERED = "risk.liquidation_triggered"
    VAR_CALCULATED = "risk.var_calculated"
    EXPOSURE_UPDATE = "risk.exposure_update"
    
    # Market making events
    POOL_CREATED = "market_making.pool_created"
    LIQUIDITY_ADDED = "market_making.liquidity_added"
    LIQUIDITY_REMOVED = "market_making.liquidity_removed"
    STRATEGY_DEPLOYED = "market_making.strategy_deployed"
    
    # Settlement events
    SETTLEMENT_INITIATED = "settlement.initiated"
    SETTLEMENT_COMPLETED = "settlement.completed"
    SETTLEMENT_FAILED = "settlement.failed"
    
    # System events
    SERVICE_STARTED = "system.service_started"
    SERVICE_STOPPED = "system.service_stopped"
    DEPENDENCY_UNHEALTHY = "system.dependency_unhealthy"
    CIRCUIT_BREAKER_OPEN = "system.circuit_breaker_open"


class MarketEventTopics:
    """Standard Pulsar topics for market events"""
    # Base namespace
    NAMESPACE = "persistent://platform/market"
    
    # Topics by category
    ORDERS = f"{NAMESPACE}/orders"
    TRADES = f"{NAMESPACE}/trades"
    MARKET_DATA = f"{NAMESPACE}/market-data"
    POSITIONS = f"{NAMESPACE}/positions"
    RISK = f"{NAMESPACE}/risk"
    SETTLEMENTS = f"{NAMESPACE}/settlements"
    SYSTEM = f"{NAMESPACE}/system"
    
    # Aggregated topics
    ALL_EVENTS = f"{NAMESPACE}/all-events"
    CRITICAL_EVENTS = f"{NAMESPACE}/critical-events"
    
    @classmethod
    def get_topic_for_event(cls, event_type: EventType) -> str:
        """Get the appropriate topic for an event type"""
        if event_type.value.startswith("order."):
            return cls.ORDERS
        elif event_type.value.startswith("trade."):
            return cls.TRADES
        elif event_type.value.startswith("market."):
            return cls.MARKET_DATA
        elif event_type.value.startswith("position."):
            return cls.POSITIONS
        elif event_type.value.startswith("risk."):
            return cls.RISK
        elif event_type.value.startswith("settlement."):
            return cls.SETTLEMENTS
        elif event_type.value.startswith("system."):
            return cls.SYSTEM
        else:
            return cls.ALL_EVENTS


@dataclass
class MarketEvent:
    """Base market event schema"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType = EventType.ORDER_CREATED
    timestamp: datetime = field(default_factory=datetime.utcnow)
    service_name: str = ""
    service_id: str = ""
    correlation_id: Optional[str] = None
    user_id: Optional[str] = None
    tenant_id: str = "default"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        data = asdict(self)
        # Convert datetime to ISO format
        data["timestamp"] = self.timestamp.isoformat()
        # Convert enum to string
        data["event_type"] = self.event_type.value
        return data
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict())


@dataclass
class OrderEvent(MarketEvent):
    """Order-specific event"""
    order_id: str = ""
    market_id: str = ""
    side: str = ""  # buy/sell
    order_type: str = ""  # market/limit/stop
    price: Optional[Decimal] = None
    size: Decimal = Decimal("0")
    status: str = ""
    filled_size: Decimal = Decimal("0")
    remaining_size: Decimal = Decimal("0")
    average_fill_price: Optional[Decimal] = None
    
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        # Convert Decimals to strings
        if self.price:
            data["price"] = str(self.price)
        data["size"] = str(self.size)
        data["filled_size"] = str(self.filled_size)
        data["remaining_size"] = str(self.remaining_size)
        if self.average_fill_price:
            data["average_fill_price"] = str(self.average_fill_price)
        return data


@dataclass
class TradeEvent(MarketEvent):
    """Trade execution event"""
    trade_id: str = ""
    order_id: str = ""
    market_id: str = ""
    side: str = ""
    price: Decimal = Decimal("0")
    size: Decimal = Decimal("0")
    maker_user_id: Optional[str] = None
    taker_user_id: Optional[str] = None
    maker_fee: Decimal = Decimal("0")
    taker_fee: Decimal = Decimal("0")
    
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["price"] = str(self.price)
        data["size"] = str(self.size)
        data["maker_fee"] = str(self.maker_fee)
        data["taker_fee"] = str(self.taker_fee)
        return data


@dataclass
class MarketDataEvent(MarketEvent):
    """Market data update event"""
    market_id: str = ""
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    last_price: Optional[Decimal] = None
    volume_24h: Decimal = Decimal("0")
    high_24h: Optional[Decimal] = None
    low_24h: Optional[Decimal] = None
    open_interest: Optional[Decimal] = None
    
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        if self.best_bid:
            data["best_bid"] = str(self.best_bid)
        if self.best_ask:
            data["best_ask"] = str(self.best_ask)
        if self.last_price:
            data["last_price"] = str(self.last_price)
        data["volume_24h"] = str(self.volume_24h)
        if self.high_24h:
            data["high_24h"] = str(self.high_24h)
        if self.low_24h:
            data["low_24h"] = str(self.low_24h)
        if self.open_interest:
            data["open_interest"] = str(self.open_interest)
        return data


@dataclass
class PositionEvent(MarketEvent):
    """Position update event"""
    position_id: str = ""
    market_id: str = ""
    side: str = ""  # long/short
    size: Decimal = Decimal("0")
    entry_price: Decimal = Decimal("0")
    mark_price: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    margin: Decimal = Decimal("0")
    leverage: Decimal = Decimal("1")
    liquidation_price: Optional[Decimal] = None
    
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["size"] = str(self.size)
        data["entry_price"] = str(self.entry_price)
        data["mark_price"] = str(self.mark_price)
        data["unrealized_pnl"] = str(self.unrealized_pnl)
        data["realized_pnl"] = str(self.realized_pnl)
        data["margin"] = str(self.margin)
        data["leverage"] = str(self.leverage)
        if self.liquidation_price:
            data["liquidation_price"] = str(self.liquidation_price)
        return data


@dataclass
class RiskEvent(MarketEvent):
    """Risk-related event"""
    risk_type: str = ""  # margin_call, liquidation, var_breach, etc.
    severity: str = ""  # low, medium, high, critical
    position_id: Optional[str] = None
    portfolio_id: Optional[str] = None
    metric_name: str = ""
    metric_value: Decimal = Decimal("0")
    threshold_value: Optional[Decimal] = None
    action_required: bool = False
    message: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        data = super().to_dict()
        data["metric_value"] = str(self.metric_value)
        if self.threshold_value:
            data["threshold_value"] = str(self.threshold_value)
        return data


class MarketEventPublisher:
    """Unified event publisher for MarketServices"""
    
    def __init__(self, pulsar_client, service_name: str, service_id: str):
        self.pulsar_client = pulsar_client
        self.service_name = service_name
        self.service_id = service_id
        self._producers = {}
        
    def _get_producer(self, topic: str):
        """Get or create producer for topic"""
        if topic not in self._producers:
            self._producers[topic] = self.pulsar_client.create_producer(
                topic,
                producer_name=f"{self.service_name}-{topic.split('/')[-1]}",
                batching_enabled=True,
                batching_max_publish_delay_ms=10,
                compression_type="LZ4"
            )
        return self._producers[topic]
    
    async def publish_event(
        self,
        event: MarketEvent,
        additional_topics: Optional[List[str]] = None
    ):
        """Publish an event to appropriate topics"""
        # Set service info
        event.service_name = self.service_name
        event.service_id = self.service_id
        
        # Determine primary topic
        primary_topic = MarketEventTopics.get_topic_for_event(event.event_type)
        
        # Get all topics to publish to
        topics = [primary_topic, MarketEventTopics.ALL_EVENTS]
        
        # Add critical events topic if needed
        if isinstance(event, RiskEvent) and event.severity in ["high", "critical"]:
            topics.append(MarketEventTopics.CRITICAL_EVENTS)
            
        # Add any additional topics
        if additional_topics:
            topics.extend(additional_topics)
            
        # Publish to all topics
        event_data = event.to_json().encode('utf-8')
        
        for topic in set(topics):  # Remove duplicates
            try:
                producer = self._get_producer(topic)
                producer.send_async(
                    event_data,
                    callback=lambda res, msg: self._log_publish(topic, event.event_id, res)
                )
            except Exception as e:
                logger.error(f"Failed to publish event {event.event_id} to {topic}: {e}")
                
    def _log_publish(self, topic: str, event_id: str, result):
        """Log publish result"""
        if result:
            logger.debug(f"Event {event_id} published to {topic}")
        else:
            logger.error(f"Failed to publish event {event_id} to {topic}")
            
    def close(self):
        """Close all producers"""
        for producer in self._producers.values():
            producer.close()


class MarketEventConsumer:
    """Unified event consumer for MarketServices"""
    
    def __init__(
        self,
        pulsar_client,
        service_name: str,
        topics: List[str],
        subscription_name: Optional[str] = None
    ):
        self.pulsar_client = pulsar_client
        self.service_name = service_name
        self.topics = topics
        self.subscription_name = subscription_name or f"{service_name}-subscription"
        self._consumer = None
        self._handlers = {}
        
    def register_handler(self, event_type: EventType, handler):
        """Register a handler for an event type"""
        self._handlers[event_type] = handler
        
    async def start(self):
        """Start consuming events"""
        self._consumer = self.pulsar_client.subscribe(
            self.topics,
            self.subscription_name,
            consumer_type="shared"
        )
        
        # Start consumer loop
        asyncio.create_task(self._consume_loop())
        
    async def _consume_loop(self):
        """Main consumer loop"""
        while True:
            try:
                msg = self._consumer.receive(timeout_millis=1000)
                
                # Parse event
                event_data = json.loads(msg.data().decode('utf-8'))
                event_type = EventType(event_data["event_type"])
                
                # Find handler
                handler = self._handlers.get(event_type)
                if handler:
                    try:
                        await handler(event_data)
                    except Exception as e:
                        logger.error(f"Error handling event {event_type}: {e}")
                        
                # Acknowledge message
                self._consumer.acknowledge(msg)
                
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error consuming events: {e}")
                    
    def stop(self):
        """Stop consuming"""
        if self._consumer:
            self._consumer.close() 