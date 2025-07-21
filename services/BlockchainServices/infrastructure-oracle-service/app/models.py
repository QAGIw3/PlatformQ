"""Data models for Infrastructure Oracle Service"""

from enum import Enum
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class ResourceType(str, Enum):
    """Types of infrastructure resources"""
    CPU = "cpu"
    GPU = "gpu"
    STORAGE = "storage"
    BANDWIDTH = "bandwidth"
    MEMORY = "memory"


class ServiceTier(str, Enum):
    """Service quality tiers"""
    STANDARD = "standard"
    PREMIUM = "premium"
    GUARANTEED = "guaranteed"


class ResourcePrice(BaseModel):
    """Resource price information"""
    resource_type: ResourceType
    region: str
    tier: ServiceTier
    price_per_unit: Decimal
    currency: str = "USD"
    unit: str  # e.g., "hour", "GB-hour", "TB"
    confidence: float = Field(ge=0, le=1)  # 0-1 confidence score
    sources: List[str]  # Data sources used
    timestamp: datetime
    metadata: Dict[str, Any] = {}


class PriceUpdate(BaseModel):
    """Price update request"""
    token_id: int
    resource_type: ResourceType
    region: str
    tier: ServiceTier
    price_wei: int  # Price in wei for blockchain


class ResourceMetrics(BaseModel):
    """Resource utilization and capacity metrics"""
    resource_type: ResourceType
    region: str
    utilization: float = Field(ge=0, le=1)  # 0-100% as 0-1
    available_capacity: float
    total_capacity: float
    reserved_capacity: float = 0
    average_sla_compliance: float = Field(ge=0, le=1)
    price_volatility: float  # Standard deviation of price
    timestamp: datetime
    metadata: Dict[str, Any] = {}


class OracleUpdate(BaseModel):
    """Oracle update event"""
    update_id: str
    token_id: int
    old_price: Decimal
    new_price: Decimal
    timestamp: datetime
    transaction_hash: Optional[str] = None
    block_number: Optional[int] = None


class DataSourceStatus(BaseModel):
    """Status of a data source"""
    name: str
    is_healthy: bool
    last_update: datetime
    error_count: int = 0
    latency_ms: float
    data_points: int


class PriceFeed(BaseModel):
    """Price feed from a data source"""
    source: str
    resource_type: ResourceType
    region: str
    tier: ServiceTier
    price: Decimal
    timestamp: datetime
    quality_score: float = 1.0
    metadata: Dict[str, Any] = {} 