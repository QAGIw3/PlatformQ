from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum
import json


class EventStatus(str, Enum):
    """Status of event processing"""
    PENDING = "pending"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"
    WEBHOOK_PENDING = "webhook_pending"
    WEBHOOK_DELIVERED = "webhook_delivered"
    WEBHOOK_FAILED = "webhook_failed"


class EventType(str, Enum):
    """Types of blockchain events"""
    TRANSFER = "transfer"
    APPROVAL = "approval"
    MINT = "mint"
    BURN = "burn"
    SWAP = "swap"
    LIQUIDITY_ADD = "liquidity_add"
    LIQUIDITY_REMOVE = "liquidity_remove"
    CONTRACT_CREATION = "contract_creation"
    CONTRACT_CALL = "contract_call"
    CUSTOM = "custom"


class BlockchainEvent(BaseModel):
    """Blockchain event data"""
    event_id: str = Field(..., description="Unique event identifier")
    chain: str = Field(..., description="Blockchain name")
    block_number: int = Field(..., description="Block number")
    block_hash: str = Field(..., description="Block hash")
    transaction_hash: str = Field(..., description="Transaction hash")
    transaction_index: int = Field(..., description="Transaction index in block")
    log_index: int = Field(..., description="Log index in transaction")
    
    contract_address: str = Field(..., description="Contract that emitted the event")
    event_name: str = Field(..., description="Event name from ABI")
    event_type: EventType = Field(default=EventType.CUSTOM)
    
    # Event data
    topics: List[str] = Field(..., description="Event topics (indexed parameters)")
    data: str = Field(..., description="Event data (non-indexed parameters)")
    decoded_data: Optional[Dict[str, Any]] = Field(None, description="Decoded event parameters")
    
    # Metadata
    timestamp: datetime = Field(..., description="Block timestamp")
    status: EventStatus = Field(default=EventStatus.PENDING)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = None
    
    # Additional context
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class EventFilter(BaseModel):
    """Filter criteria for blockchain events"""
    chain: Optional[str] = None
    contract_address: Optional[str] = None
    event_name: Optional[str] = None
    event_type: Optional[EventType] = None
    from_block: Optional[int] = None
    to_block: Optional[int] = None
    from_timestamp: Optional[datetime] = None
    to_timestamp: Optional[datetime] = None
    status: Optional[EventStatus] = None
    transaction_hash: Optional[str] = None
    
    @validator('contract_address')
    def normalize_address(cls, v):
        if v:
            return v.lower()
        return v


class EventSubscription(BaseModel):
    """Subscription to blockchain events"""
    subscription_id: str = Field(..., description="Unique subscription identifier")
    name: str = Field(..., description="Subscription name")
    chain: str = Field(..., description="Blockchain to monitor")
    contract_address: Optional[str] = Field(None, description="Specific contract to monitor")
    event_filters: List[Dict[str, Any]] = Field(default_factory=list, description="Event filter criteria")
    
    webhook_url: Optional[str] = Field(None, description="Webhook URL for notifications")
    webhook_headers: Dict[str, str] = Field(default_factory=dict)
    webhook_secret: Optional[str] = Field(None, description="Secret for webhook signatures")
    
    is_active: bool = Field(default=True)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WebhookDelivery(BaseModel):
    """Webhook delivery record"""
    delivery_id: str = Field(..., description="Unique delivery identifier")
    subscription_id: str = Field(..., description="Related subscription")
    event_id: str = Field(..., description="Event being delivered")
    
    url: str = Field(..., description="Webhook URL")
    headers: Dict[str, str] = Field(default_factory=dict)
    payload: Dict[str, Any] = Field(..., description="Webhook payload")
    
    status: str = Field(default="pending")
    attempts: int = Field(default=0)
    max_attempts: int = Field(default=3)
    
    response_status: Optional[int] = None
    response_body: Optional[str] = None
    error_message: Optional[str] = None
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    delivered_at: Optional[datetime] = None
    next_retry_at: Optional[datetime] = None


class MonitorStatus(BaseModel):
    """Status of a blockchain monitor"""
    chain: str
    is_active: bool
    current_block: int
    target_block: int
    blocks_behind: int
    last_scan_at: Optional[datetime]
    events_processed: int
    errors: List[str] = Field(default_factory=list)
    
    @property
    def is_synced(self) -> bool:
        return self.blocks_behind < 10


class EventStatistics(BaseModel):
    """Statistics for event monitoring"""
    chain: Optional[str] = None
    period_start: datetime
    period_end: datetime
    
    total_events: int = Field(default=0)
    events_by_type: Dict[str, int] = Field(default_factory=dict)
    events_by_status: Dict[str, int] = Field(default_factory=dict)
    events_by_contract: Dict[str, int] = Field(default_factory=dict)
    
    webhooks_sent: int = Field(default=0)
    webhooks_successful: int = Field(default=0)
    webhooks_failed: int = Field(default=0)
    
    average_processing_time_ms: float = Field(default=0.0)
    blocks_processed: int = Field(default=0)


class ContractABI(BaseModel):
    """Contract ABI for event decoding"""
    contract_address: str
    chain: str
    abi: List[Dict[str, Any]]
    name: Optional[str] = None
    is_verified: bool = Field(default=False)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    
    @validator('contract_address')
    def normalize_address(cls, v):
        return v.lower()
    
    def get_event_abi(self, event_name: str) -> Optional[Dict[str, Any]]:
        """Get ABI for specific event"""
        for item in self.abi:
            if item.get('type') == 'event' and item.get('name') == event_name:
                return item
        return None


class EventBatch(BaseModel):
    """Batch of events for processing"""
    batch_id: str
    chain: str
    block_range: tuple[int, int]
    events: List[BlockchainEvent]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = None


class AlertRule(BaseModel):
    """Alert rule for event monitoring"""
    rule_id: str
    name: str
    description: Optional[str] = None
    
    # Conditions
    chain: Optional[str] = None
    contract_address: Optional[str] = None
    event_type: Optional[EventType] = None
    condition: Dict[str, Any]  # e.g., {"value": {"$gt": 1000000}}
    
    # Actions
    alert_channels: List[str] = Field(default_factory=list)  # email, slack, webhook
    alert_config: Dict[str, Any] = Field(default_factory=dict)
    
    is_active: bool = Field(default=True)
    cooldown_seconds: int = Field(default=300)
    last_triggered_at: Optional[datetime] = None
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow) 