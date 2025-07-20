"""Standardized trading event schemas for Apache Pulsar"""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, Any, List
import json
import uuid


class EventType(Enum):
    """Trading event types"""
    # Order events
    ORDER_NEW = "order.new"
    ORDER_CANCELLED = "order.cancelled"
    ORDER_UPDATED = "order.updated"
    ORDER_REJECTED = "order.rejected"
    ORDER_EXPIRED = "order.expired"
    
    # Trade events
    TRADE_EXECUTED = "trade.executed"
    TRADE_SETTLED = "trade.settled"
    TRADE_FAILED = "trade.failed"
    
    # Market events
    MARKET_OPENED = "market.opened"
    MARKET_CLOSED = "market.closed"
    MARKET_HALTED = "market.halted"
    MARKET_RESUMED = "market.resumed"
    
    # Position events
    POSITION_OPENED = "position.opened"
    POSITION_UPDATED = "position.updated"
    POSITION_CLOSED = "position.closed"
    POSITION_LIQUIDATED = "position.liquidated"
    
    # Risk events
    MARGIN_CALL = "risk.margin_call"
    RISK_LIMIT_BREACH = "risk.limit_breach"
    LIQUIDATION_WARNING = "risk.liquidation_warning"
    
    # Market data events
    ORDERBOOK_UPDATE = "market_data.orderbook_update"
    PRICE_UPDATE = "market_data.price_update"
    VOLUME_UPDATE = "market_data.volume_update"
    
    # Settlement events
    SETTLEMENT_INITIATED = "settlement.initiated"
    SETTLEMENT_COMPLETED = "settlement.completed"
    SETTLEMENT_FAILED = "settlement.failed"


@dataclass
class BaseEvent:
    """Base event structure"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType = EventType.ORDER_NEW
    timestamp: datetime = field(default_factory=datetime.utcnow)
    source_service: str = ""
    tenant_id: Optional[str] = None
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_json(self) -> str:
        """Convert event to JSON string"""
        data = asdict(self)
        # Convert datetime to ISO format
        data['timestamp'] = self.timestamp.isoformat()
        data['event_type'] = self.event_type.value
        # Convert Decimal to string
        return json.dumps(data, default=str)
    
    @classmethod
    def from_json(cls, json_str: str):
        """Create event from JSON string"""
        data = json.loads(json_str)
        # Convert ISO format to datetime
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        data['event_type'] = EventType(data['event_type'])
        return cls(**data)


@dataclass
class OrderEvent(BaseEvent):
    """Order-related event"""
    order_id: str = ""
    market_id: str = ""
    trader_id: str = ""
    side: str = ""
    order_type: str = ""
    quantity: str = ""
    price: Optional[str] = None
    filled_quantity: str = "0"
    status: str = ""
    reason: Optional[str] = None


@dataclass
class TradeEvent(BaseEvent):
    """Trade execution event"""
    trade_id: str = ""
    market_id: str = ""
    price: str = ""
    quantity: str = ""
    buyer_order_id: str = ""
    seller_order_id: str = ""
    buyer_id: str = ""
    seller_id: str = ""
    buyer_fee: str = ""
    seller_fee: str = ""
    maker_side: str = ""  # buy or sell
    
    @property
    def total_value(self) -> Decimal:
        return Decimal(self.price) * Decimal(self.quantity)


@dataclass
class MarketDataEvent(BaseEvent):
    """Market data update event"""
    market_id: str = ""
    update_type: str = ""  # snapshot, delta
    sequence_number: int = 0
    
    # Price data
    best_bid: Optional[str] = None
    best_ask: Optional[str] = None
    last_price: Optional[str] = None
    
    # Volume data
    volume_24h: Optional[str] = None
    trade_count_24h: Optional[int] = None
    
    # Order book data (for snapshots)
    bids: List[List[str]] = field(default_factory=list)  # [[price, quantity], ...]
    asks: List[List[str]] = field(default_factory=list)


@dataclass
class PositionEvent(BaseEvent):
    """Position-related event"""
    position_id: str = ""
    market_id: str = ""
    trader_id: str = ""
    side: str = ""
    size: str = ""
    entry_price: str = ""
    mark_price: str = ""
    realized_pnl: str = ""
    unrealized_pnl: str = ""
    margin_used: str = ""
    liquidation_price: Optional[str] = None


@dataclass
class RiskEvent(BaseEvent):
    """Risk-related event"""
    trader_id: str = ""
    risk_type: str = ""  # margin_call, limit_breach, etc.
    severity: str = ""  # low, medium, high, critical
    current_value: str = ""
    threshold_value: str = ""
    required_action: Optional[str] = None
    deadline: Optional[datetime] = None
    affected_positions: List[str] = field(default_factory=list)


@dataclass
class SettlementEvent(BaseEvent):
    """Settlement-related event"""
    settlement_id: str = ""
    market_id: str = ""
    settlement_type: str = ""  # cash, physical
    trades: List[str] = field(default_factory=list)  # List of trade IDs
    total_value: str = ""
    settlement_price: Optional[str] = None
    settlement_time: Optional[datetime] = None
    status: str = ""
    failure_reason: Optional[str] = None


class EventPublisher:
    """Helper class for publishing events to Pulsar"""
    
    def __init__(self, pulsar_client, topic_prefix: str = "persistent://derivatives/trading"):
        self.pulsar_client = pulsar_client
        self.topic_prefix = topic_prefix
        self.producers = {}
    
    def get_producer(self, event_type: EventType):
        """Get or create producer for event type"""
        topic = f"{self.topic_prefix}/{event_type.value.replace('.', '-')}"
        
        if topic not in self.producers:
            self.producers[topic] = self.pulsar_client.create_producer(
                topic,
                batching_enabled=True,
                batching_max_messages=1000,
                batching_max_publish_delay_ms=10
            )
        
        return self.producers[topic]
    
    async def publish_event(self, event: BaseEvent):
        """Publish event to appropriate topic"""
        producer = self.get_producer(event.event_type)
        
        # Add message properties
        properties = {
            "event_type": event.event_type.value,
            "source_service": event.source_service,
            "tenant_id": event.tenant_id or "",
            "correlation_id": event.correlation_id or ""
        }
        
        # Send message
        await producer.send_async(
            event.to_json().encode('utf-8'),
            properties=properties
        )
    
    def close(self):
        """Close all producers"""
        for producer in self.producers.values():
            producer.close()


class EventSubscriber:
    """Helper class for subscribing to events from Pulsar"""
    
    def __init__(self, pulsar_client, subscription_name: str, 
                 topic_prefix: str = "persistent://derivatives/trading"):
        self.pulsar_client = pulsar_client
        self.subscription_name = subscription_name
        self.topic_prefix = topic_prefix
        self.consumers = {}
    
    def subscribe_to_event(self, event_type: EventType, handler):
        """Subscribe to specific event type"""
        topic = f"{self.topic_prefix}/{event_type.value.replace('.', '-')}"
        
        consumer = self.pulsar_client.subscribe(
            topic,
            subscription_name=self.subscription_name,
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        self.consumers[event_type] = (consumer, handler)
        
        return consumer
    
    async def process_events(self):
        """Process events from all subscriptions"""
        while True:
            for event_type, (consumer, handler) in self.consumers.items():
                try:
                    # Receive with timeout
                    msg = consumer.receive(timeout_millis=100)
                    
                    # Parse event based on type
                    event_class = self._get_event_class(event_type)
                    event = event_class.from_json(msg.data().decode('utf-8'))
                    
                    # Call handler
                    await handler(event)
                    
                    # Acknowledge message
                    consumer.acknowledge(msg)
                    
                except Exception as e:
                    # Timeout is expected, other errors should be logged
                    if "timeout" not in str(e).lower():
                        print(f"Error processing event: {e}")
    
    def _get_event_class(self, event_type: EventType):
        """Get event class for event type"""
        # Map event types to classes
        if event_type.value.startswith("order."):
            return OrderEvent
        elif event_type.value.startswith("trade."):
            return TradeEvent
        elif event_type.value.startswith("market_data."):
            return MarketDataEvent
        elif event_type.value.startswith("position."):
            return PositionEvent
        elif event_type.value.startswith("risk."):
            return RiskEvent
        elif event_type.value.startswith("settlement."):
            return SettlementEvent
        else:
            return BaseEvent
    
    def close(self):
        """Close all consumers"""
        for consumer, _ in self.consumers.values():
            consumer.close() 