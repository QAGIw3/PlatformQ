"""Compute resource models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
import uuid


class ResourceType(str, Enum):
    """Types of compute resources."""
    CPU = "cpu"
    GPU = "gpu"
    TPU = "tpu"
    MEMORY = "memory"
    STORAGE = "storage"
    BANDWIDTH = "bandwidth"


class ProviderStatus(str, Enum):
    """Provider status."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"
    MAINTENANCE = "maintenance"


class ResourceSpec(BaseModel):
    """Specification for a compute resource."""
    resource_type: ResourceType
    quantity: Decimal = Field(..., gt=0)
    
    # CPU specific
    cpu_model: Optional[str] = None
    cpu_cores: Optional[int] = None
    cpu_frequency_ghz: Optional[Decimal] = None
    
    # GPU specific
    gpu_model: Optional[str] = None
    gpu_memory_gb: Optional[int] = None
    cuda_cores: Optional[int] = None
    
    # Memory specific
    memory_type: Optional[str] = None  # DDR4, DDR5, etc.
    memory_speed_mhz: Optional[int] = None
    
    # Storage specific
    storage_type: Optional[str] = None  # SSD, HDD, NVMe
    iops: Optional[int] = None
    
    # Network specific
    network_speed_gbps: Optional[Decimal] = None
    
    # General specs
    region: str = "us-east-1"
    availability_zone: Optional[str] = None
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str
        }


class ComputeResource(BaseModel):
    """A compute resource available in the market."""
    resource_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    provider_id: str
    
    # Resource details
    spec: ResourceSpec
    total_capacity: Decimal
    available_capacity: Decimal
    reserved_capacity: Decimal = Decimal("0")
    
    # Quality of Service
    qos_level: str = "bronze"  # bronze, silver, gold, platinum
    availability_sla: Decimal = Field(default=Decimal("0.99"), ge=0, le=1)
    
    # Pricing
    base_price_per_hour: Decimal
    spot_price_per_hour: Optional[Decimal] = None
    reserved_discount: Decimal = Field(default=Decimal("0.2"), ge=0, le=1)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    # Performance metrics
    utilization_percent: Decimal = Field(default=Decimal("0"), ge=0, le=100)
    avg_response_time_ms: Optional[Decimal] = None
    uptime_percent: Decimal = Field(default=Decimal("100"), ge=0, le=100)
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class ComputeProvider(BaseModel):
    """Compute resource provider."""
    provider_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    organization: Optional[str] = None
    
    # Status
    status: ProviderStatus = ProviderStatus.ACTIVE
    
    # Staking
    stake_amount: Decimal = Field(default=Decimal("0"), ge=0)
    stake_locked_until: Optional[datetime] = None
    
    # Reputation
    reputation_score: Decimal = Field(default=Decimal("5.0"), ge=0, le=10)
    total_allocations: int = Field(default=0, ge=0)
    successful_allocations: int = Field(default=0, ge=0)
    slashing_events: int = Field(default=0, ge=0)
    
    # Resources
    total_resources: int = Field(default=0, ge=0)
    active_resources: int = Field(default=0, ge=0)
    
    # Regions
    supported_regions: List[str] = Field(default_factory=list)
    
    # Pricing
    pricing_strategy: str = "market"  # market, fixed, dynamic
    price_multiplier: Decimal = Field(default=Decimal("1.0"), gt=0)
    
    # Contact
    contact_email: Optional[str] = None
    webhook_url: Optional[str] = None
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    verified: bool = False
    certifications: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        } 