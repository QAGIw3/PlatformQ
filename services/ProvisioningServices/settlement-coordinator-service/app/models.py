"""
Models for Settlement Coordinator Service
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field

from platformq_shared.models import ResourceType, ServiceTier


class SettlementStatus(str, Enum):
    """Status of a settlement"""
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    DISPUTED = "disputed"


class SettlementRequest(BaseModel):
    """Request to create a new settlement"""
    resource_type: ResourceType
    amount: int = Field(..., gt=0, description="Amount of resources")
    provider: str = Field(..., description="Provider address")
    consumer: str = Field(..., description="Consumer address")
    duration: int = Field(..., gt=0, description="Duration in seconds")
    price_per_unit: float = Field(..., gt=0, description="Price per resource unit")
    metadata: Optional[Dict[str, Any]] = None


class Settlement(BaseModel):
    """Settlement record"""
    settlement_id: str
    resource_type: ResourceType
    amount: int
    provider: str
    consumer: str
    start_time: datetime
    end_time: datetime
    price_per_unit: float
    total_cost: float
    status: SettlementStatus
    tx_hash: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    dispute_reason: Optional[str] = None
    resolution: Optional[str] = None
    
    # Resource token fields
    token_id: Optional[int] = None
    token_amount: Optional[int] = None
    tokens_minted: Optional[int] = None
    tokens_burned: Optional[int] = None
    tokens_slashed: Optional[int] = None
    
    # Flash provisioning fields
    is_flash: bool = Field(default=False, description="Whether this is a flash provision")
    flash_fee: Optional[int] = Field(None, description="Flash loan fee amount")
    fee_paid: Optional[bool] = Field(None, description="Whether flash fee was paid")
    parent_settlement: Optional[str] = Field(None, description="Parent settlement ID for swaps")
    completion_type: Optional[str] = Field(None, description="How settlement was completed")
    
    # Additional fields for tracking
    tier: ServiceTier = Field(default=ServiceTier.STANDARD)
    region: str = Field(default="us-east-1")
    usage_data: Optional[Dict[str, Any]] = None


class SettlementUpdate(BaseModel):
    """Update to a settlement"""
    status: Optional[SettlementStatus] = None
    tx_hash: Optional[str] = None
    dispute_reason: Optional[str] = None
    resolution: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class DisputeRequest(BaseModel):
    """Request to dispute a settlement"""
    settlement_id: str
    reason: str = Field(..., min_length=10, description="Reason for dispute")
    evidence: Optional[Dict[str, Any]] = None


class DisputeResolution(BaseModel):
    """Resolution for a disputed settlement"""
    settlement_id: str
    resolution: str = Field(..., description="Resolution decision")
    refund_amount: Optional[float] = None
    penalty_amount: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None


# Flash provisioning specific models
class FlashProvisioningRequest(BaseModel):
    """Request for flash provisioning through settlement"""
    resource_type: ResourceType
    amount: int = Field(..., gt=0)
    tier: ServiceTier = Field(default=ServiceTier.STANDARD)
    region: str = Field(default="us-east-1")
    duration: int = Field(..., gt=0, description="Duration in seconds")
    provider: str
    consumer: str
    callback_data: bytes = Field(default=b"")


class FlashSwapRequest(BaseModel):
    """Request for atomic resource swap"""
    settlement_id: str = Field(..., description="Settlement to swap from")
    to_resource_type: ResourceType
    to_amount: int = Field(..., gt=0)
    pool_id: int = Field(..., description="AMM pool ID")


class FlashSettlementResponse(BaseModel):
    """Response for flash settlement operations"""
    success: bool
    settlement_id: Optional[str] = None
    tx_hash: Optional[str] = None
    status: Optional[str] = None
    error: Optional[str] = None
    new_settlement_id: Optional[str] = None  # For swaps
    from_token: Optional[int] = None  # For swaps
    to_token: Optional[int] = None  # For swaps 