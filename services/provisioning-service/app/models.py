"""
Models for the provisioning service
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pydantic import BaseModel, Field
import uuid


class ProviderType(str, Enum):
    """Types of compute providers"""
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"
    RACKSPACE = "rackspace"
    ON_PREMISE = "on_premise"
    EDGE = "edge"
    PARTNER = "partner"


class ResourceType(str, Enum):
    """Types of compute resources"""
    CPU = "cpu"
    GPU = "gpu"
    TPU = "tpu"
    MEMORY = "memory"
    STORAGE = "storage"
    BANDWIDTH = "bandwidth"


class AllocationStatus(str, Enum):
    """Status of resource allocation"""
    PENDING = "pending"
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    FAILED = "failed"
    TERMINATED = "terminated"


class ResourceSpec(BaseModel):
    """Specification for compute resources"""
    resource_type: ResourceType
    quantity: Decimal
    specifications: Dict[str, Any] = Field(default_factory=dict)
    location_preferences: List[str] = Field(default_factory=list)
    required_features: List[str] = Field(default_factory=list)


class AllocationRequest(BaseModel):
    """Request for resource allocation"""
    settlement_id: str
    buyer_id: str
    provider_id: str
    resources: List[ResourceSpec]
    start_time: datetime
    duration_hours: int
    sla_requirements: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ResourceAllocation(BaseModel):
    """Allocated compute resources"""
    allocation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    settlement_id: str
    provider: ProviderType
    resources: List[ResourceSpec]
    status: AllocationStatus = AllocationStatus.PENDING
    access_details: Dict[str, Any] = Field(default_factory=dict)
    monitoring_endpoints: Dict[str, str] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    activated_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    cost_per_hour: Decimal = Decimal("0")
    total_cost: Decimal = Decimal("0") 