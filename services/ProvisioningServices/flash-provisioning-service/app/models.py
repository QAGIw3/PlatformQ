"""
Models for Flash Provisioning Service
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field

from platformq_shared.models import ResourceType, ServiceTier


class ProvisioningStatus(str, Enum):
    """Status of resource provisioning"""
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"


class FlashProvisionRequest(BaseModel):
    """Request for flash resource provisioning"""
    resource_type: ResourceType
    amount: int = Field(..., gt=0, description="Amount of resources needed")
    tier: ServiceTier = Field(default=ServiceTier.STANDARD)
    duration: int = Field(..., gt=0, description="Duration in seconds")
    region: str = Field(..., description="Target region (e.g., us-east-1)")
    receiver_address: str = Field(..., description="Address to receive resources")
    callback_data: bytes = Field(default=b"", description="Data to pass to receiver")
    max_price: Optional[float] = Field(None, description="Maximum price per unit")
    priority: int = Field(default=1, ge=1, le=10, description="Priority level")


class ResourceAllocation(BaseModel):
    """Resource allocation details"""
    allocation_id: str
    resource_type: ResourceType
    amount: int
    provider: str
    consumer: str
    start_time: datetime
    end_time: datetime
    status: ProvisioningStatus
    token_id: Optional[int] = None
    price_per_unit: Optional[float] = None
    total_cost: Optional[float] = None


class FlashSwapRequest(BaseModel):
    """Request for atomic resource swap"""
    from_token_id: int
    from_amount: int = Field(..., gt=0)
    to_resource_type: ResourceType
    to_amount: int = Field(..., gt=0)
    max_slippage: float = Field(default=0.03, ge=0, le=0.1)


class BurstProvisionRequest(BaseModel):
    """Request for burst capacity provisioning"""
    resource_type: ResourceType
    burst_amount: int = Field(..., gt=0)
    duration: int = Field(..., gt=0, description="Duration in seconds")
    max_price: Optional[float] = None
    auto_scale: bool = Field(default=True, description="Enable auto-scaling")


class JITScalingConfig(BaseModel):
    """Just-in-time scaling configuration"""
    resource_type: ResourceType
    enabled: bool = True
    min_capacity: int = Field(100, gt=0)
    max_capacity: int = Field(10000, gt=0)
    scale_up_threshold: float = Field(0.8, gt=0, le=1)
    scale_down_threshold: float = Field(0.2, gt=0, le=1)
    cooldown_period: int = Field(300, gt=0, description="Cooldown in seconds")


class FlashProvisionResponse(BaseModel):
    """Response for flash provisioning request"""
    provision_id: str
    allocation: ResourceAllocation
    fee: float
    tx_hash: str
    estimated_cost: float
    resources: Dict[str, Any]


class FlashSwapResponse(BaseModel):
    """Response for flash swap request"""
    swap_id: str
    from_token: int
    to_token: int
    from_amount: int
    to_amount: int
    slippage: float
    tx_hash: str


class BurstProvisionResponse(BaseModel):
    """Response for burst provisioning request"""
    burst_id: str
    total_amount: int
    provisions: List[FlashProvisionResponse]
    total_cost: float
    duration: int


class ProvisionStatusResponse(BaseModel):
    """Status of a provision"""
    provision_id: str
    status: str
    resource_type: str
    amount: int
    start_time: str
    end_time: str
    remaining_time: float


class FlashStatistics(BaseModel):
    """Flash provisioning statistics"""
    active_provisions: int
    total_resources: int
    by_resource_type: Dict[str, Dict[str, int]]
    jit_scaling_enabled: List[str]


class FlashLiquidityDeposit(BaseModel):
    """Request to deposit liquidity for flash loans"""
    token_id: int
    amount: int = Field(..., gt=0)


class FlashFeeUpdate(BaseModel):
    """Request to update flash loan fees"""
    resource_type: ResourceType
    fee_basis_points: int = Field(..., ge=0, le=100)


class TrustedReceiverUpdate(BaseModel):
    """Request to update trusted receiver status"""
    receiver_address: str
    trusted: bool


# Flash loan receiver callback response
FLASH_LOAN_CALLBACK_SUCCESS = "0x439148f0bbc682ca079e46d6e2c2f0c1e3b820f1a291b069d8882abf8cf18dd9"  # keccak256("ERC3156FlashBorrower.onFlashLoan") 