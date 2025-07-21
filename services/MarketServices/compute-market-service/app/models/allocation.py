"""Resource allocation models."""

from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, validator
import uuid

from .compute_resource import ResourceSpec


class AllocationStatus(str, Enum):
    """Allocation status."""
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    TERMINATED = "terminated"
    FAILED = "failed"


class AllocationRequest(BaseModel):
    """Request for resource allocation."""
    user_id: str
    
    # Resource requirements
    resource_specs: List[ResourceSpec]
    
    # Duration
    start_time: datetime
    end_time: datetime
    
    # Allocation type
    allocation_type: str = "spot"  # spot, reserved, burst
    
    # Constraints
    max_price_per_hour: Optional[Decimal] = None
    preferred_providers: List[str] = Field(default_factory=list)
    excluded_providers: List[str] = Field(default_factory=list)
    required_qos_level: str = "bronze"
    
    # Options
    auto_renew: bool = False
    preemptible: bool = True
    
    # Metadata
    purpose: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @validator('end_time')
    def validate_duration(cls, v, values):
        """Validate allocation duration."""
        if 'start_time' in values and v <= values['start_time']:
            raise ValueError("End time must be after start time")
        return v
    
    class Config:
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class ResourceAllocation(BaseModel):
    """Base resource allocation."""
    allocation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    resource_id: str
    provider_id: str
    
    # Allocation details
    status: AllocationStatus = AllocationStatus.PENDING
    allocation_type: str
    
    # Resource spec
    allocated_spec: ResourceSpec
    
    # Time
    start_time: datetime
    end_time: datetime
    actual_start_time: Optional[datetime] = None
    actual_end_time: Optional[datetime] = None
    
    # Pricing
    price_per_hour: Decimal
    total_cost: Decimal = Decimal("0")
    paid_amount: Decimal = Decimal("0")
    
    # Performance
    qos_level: str
    sla_violations: int = Field(default=0, ge=0)
    uptime_percent: Decimal = Field(default=Decimal("100"), ge=0, le=100)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    tags: List[str] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    def calculate_duration_hours(self) -> Decimal:
        """Calculate allocation duration in hours."""
        if self.actual_end_time and self.actual_start_time:
            duration = self.actual_end_time - self.actual_start_time
        else:
            duration = self.end_time - self.start_time
        
        return Decimal(str(duration.total_seconds() / 3600))
    
    def calculate_total_cost(self) -> Decimal:
        """Calculate total cost of allocation."""
        hours = self.calculate_duration_hours()
        self.total_cost = hours * self.price_per_hour
        return self.total_cost
    
    class Config:
        use_enum_values = True
        json_encoders = {
            Decimal: str,
            datetime: lambda v: v.isoformat()
        }


class SpotAllocation(ResourceAllocation):
    """Spot market allocation."""
    allocation_type: str = Field(default="spot", const=True)
    
    # Spot specific
    bid_price: Decimal
    market_price: Decimal
    preemptible: bool = True
    preemption_notice_seconds: int = 120
    
    # Preemption handling
    was_preempted: bool = False
    preempted_at: Optional[datetime] = None
    fallback_allocation_id: Optional[str] = None


class ReservedAllocation(ResourceAllocation):
    """Reserved capacity allocation."""
    allocation_type: str = Field(default="reserved", const=True)
    
    # Reservation details
    reservation_term_days: int = Field(..., ge=1, le=365)
    upfront_payment: Decimal = Decimal("0")
    hourly_rate: Decimal
    
    # Capacity guarantee
    guaranteed_capacity: bool = True
    capacity_modification_allowed: bool = True
    
    # Usage
    usage_hours: Decimal = Field(default=Decimal("0"), ge=0)
    remaining_hours: Decimal = Field(default=Decimal("0"), ge=0)
    
    def calculate_savings(self, spot_price: Decimal) -> Decimal:
        """Calculate savings compared to spot price."""
        spot_cost = self.usage_hours * spot_price
        reserved_cost = self.upfront_payment + (self.usage_hours * self.hourly_rate)
        return max(spot_cost - reserved_cost, Decimal("0")) 