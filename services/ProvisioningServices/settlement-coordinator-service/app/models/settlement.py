"""Settlement models for Settlement Coordinator Service"""

from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Any
from pydantic import BaseModel
import uuid


class ResourceType(str, Enum):
    """Types of compute resources"""
    CPU = "cpu"
    GPU = "gpu"
    STORAGE = "storage"
    BANDWIDTH = "bandwidth"
    MEMORY = "memory"


class ServiceTier(str, Enum):
    """Service quality tiers for resources"""
    STANDARD = "standard"       # Best effort
    PREMIUM = "premium"         # Guaranteed performance
    GUARANTEED = "guaranteed"   # Dedicated resources with SLA


class SettlementStatus(str, Enum):
    """Settlement status values"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RECONCILING = "reconciling"
    DISPUTED = "disputed"
    PENDING_RELEASE = "pending_release"


class RiskLevel(str, Enum):
    """Risk level categories"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Settlement(BaseModel):
    """Settlement model"""
    id: str = None
    trade_id: str
    buyer_id: str
    seller_id: str
    provider_id: str
    resource_type: ResourceType
    quantity: float
    unit_price: float
    total_value: float
    trade_timestamp: datetime
    delivery_start: datetime
    delivery_end: datetime
    settlement_timestamp: Optional[datetime] = None
    status: SettlementStatus = SettlementStatus.PENDING
    
    # Billing info
    billing_id: Optional[str] = None
    rated_amount: Optional[float] = None
    
    # Escrow info
    escrow_amount: float = 0.0
    escrow_released: bool = False
    escrow_release_time: Optional[datetime] = None
    
    # Metadata
    metadata: Dict[str, Any] = {}
    
    def __init__(self, **data):
        super().__init__(**data)
        if not self.id:
            self.id = str(uuid.uuid4())


class RiskAssessment(BaseModel):
    """Risk assessment result"""
    settlement_id: str
    timestamp: datetime
    risk_level: RiskLevel
    final_score: float  # 0-1 risk score
    
    # Component scores
    model_scores: Dict[str, float] = {}
    
    # Risk factors
    factors: Dict[str, float] = {}
    
    # Recommendations
    recommended_escrow_percentage: float
    
    # Metadata
    metadata: Dict[str, Any] = {}


class ProviderMetrics(BaseModel):
    """Provider performance metrics"""
    provider_id: str
    sla_uptime: float  # 0-1
    total_settlements: int
    failed_settlements: int
    average_settlement_time: float  # seconds
    total_value_settled: float
    reputation_score: float  # 0-1
    last_updated: datetime = None
    
    def __init__(self, **data):
        super().__init__(**data)
        if not self.last_updated:
            self.last_updated = datetime.utcnow() 