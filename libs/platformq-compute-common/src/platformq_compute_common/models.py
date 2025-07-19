"""Unified compute resource models for PlatformQ

This module provides common data models used across compute-related services.
"""

from enum import Enum
from typing import Dict, Any, Optional, List
from datetime import datetime
from decimal import Decimal
from dataclasses import dataclass, field
from pydantic import BaseModel, Field
import uuid


class ComputeResourceType(str, Enum):
    """Types of compute resources"""
    CPU = "cpu"
    GPU = "gpu"
    TPU = "tpu"
    MEMORY = "memory"
    STORAGE = "storage"
    NETWORK = "network"
    QUANTUM = "quantum"


class ProviderType(str, Enum):
    """Compute infrastructure providers"""
    AWS = "aws"
    AZURE = "azure"
    GCP = "gcp"
    RACKSPACE = "rackspace"
    ON_PREMISE = "on_premise"
    EDGE = "edge"
    PARTNER = "partner"
    CLOUDSTACK = "cloudstack"
    KUBERNETES = "kubernetes"


class AllocationStatus(str, Enum):
    """Status of resource allocation"""
    PENDING = "pending"
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    SCALING = "scaling"
    FAILED = "failed"
    TERMINATED = "terminated"
    SUSPENDED = "suspended"


class PricingModel(str, Enum):
    """Pricing models for resources"""
    ON_DEMAND = "on_demand"
    SPOT = "spot"
    RESERVED = "reserved"
    COMMITTED_USE = "committed_use"
    PREEMPTIBLE = "preemptible"


class AllocationStrategy(str, Enum):
    """Resource allocation strategies"""
    COST_OPTIMIZED = "cost_optimized"
    PERFORMANCE_OPTIMIZED = "performance_optimized"
    BALANCED = "balanced"
    SPOT_PREFERRED = "spot_preferred"
    RESERVED_ONLY = "reserved_only"
    LATENCY_OPTIMIZED = "latency_optimized"
    AVAILABILITY_OPTIMIZED = "availability_optimized"


@dataclass
class ResourceRequirements:
    """Unified resource requirements specification"""
    # Core resources
    cpu_cores: float = 1.0
    memory_gb: float = 2.0
    storage_gb: float = 10.0
    
    # GPU/Accelerator requirements
    gpu_count: int = 0
    gpu_type: Optional[str] = None
    gpu_memory_gb: Optional[float] = None
    tpu_count: int = 0
    tpu_type: Optional[str] = None
    
    # Network requirements
    network_bandwidth_gbps: float = 1.0
    public_ip_required: bool = False
    
    # Platform requirements
    os_type: str = "linux"
    os_version: Optional[str] = None
    kernel_version: Optional[str] = None
    
    # Location preferences
    regions: List[str] = field(default_factory=list)
    availability_zones: List[str] = field(default_factory=list)
    data_locality_requirements: List[str] = field(default_factory=list)
    
    # Hardware requirements
    specialized_hardware: List[str] = field(default_factory=list)
    cpu_architecture: str = "x86_64"
    min_cpu_generation: Optional[str] = None
    
    # Constraints
    max_cost_per_hour: Optional[float] = None
    min_availability_sla: float = 0.99
    max_latency_ms: Optional[int] = None
    compliance_requirements: List[str] = field(default_factory=list)
    
    # Scheduling preferences
    spot_instance_acceptable: bool = True
    preemptible_acceptable: bool = True
    dedicated_host_required: bool = False
    
    def validate(self) -> List[str]:
        """Validate requirements and return list of errors"""
        errors = []
        
        if self.cpu_cores <= 0:
            errors.append("CPU cores must be positive")
        if self.memory_gb <= 0:
            errors.append("Memory must be positive")
        if self.gpu_count < 0:
            errors.append("GPU count cannot be negative")
        if self.gpu_count > 0 and not self.gpu_type:
            errors.append("GPU type must be specified when GPU count > 0")
        if self.min_availability_sla < 0 or self.min_availability_sla > 1:
            errors.append("Availability SLA must be between 0 and 1")
            
        return errors


@dataclass
class ResourceAllocation:
    """Unified resource allocation record"""
    # Identifiers
    allocation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str = ""
    workload_id: str = ""
    workload_type: str = ""
    
    # Provider details
    provider: ProviderType = ProviderType.ON_PREMISE
    region: str = ""
    availability_zone: Optional[str] = None
    instance_type: Optional[str] = None
    instance_id: Optional[str] = None
    
    # Allocated resources
    cpu_cores: float = 0.0
    memory_gb: float = 0.0
    storage_gb: float = 0.0
    gpu_count: int = 0
    gpu_type: Optional[str] = None
    network_bandwidth_gbps: float = 0.0
    
    # Status
    status: AllocationStatus = AllocationStatus.PENDING
    health_status: str = "unknown"
    
    # Timing
    created_at: datetime = field(default_factory=datetime.utcnow)
    activated_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    last_modified_at: datetime = field(default_factory=datetime.utcnow)
    
    # Cost
    pricing_model: PricingModel = PricingModel.ON_DEMAND
    cost_per_hour: Decimal = Decimal("0.0")
    total_cost: Decimal = Decimal("0.0")
    currency: str = "USD"
    
    # Access details
    access_details: Dict[str, Any] = field(default_factory=dict)
    monitoring_endpoints: Dict[str, str] = field(default_factory=dict)
    
    # Metadata
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_active(self) -> bool:
        """Check if allocation is currently active"""
        return self.status in [AllocationStatus.ACTIVE, AllocationStatus.SCALING]
    
    def is_expired(self) -> bool:
        """Check if allocation has expired"""
        if self.expires_at:
            return datetime.utcnow() > self.expires_at
        return False
    
    def calculate_runtime_hours(self) -> float:
        """Calculate total runtime in hours"""
        if not self.activated_at:
            return 0.0
            
        end_time = self.expires_at or datetime.utcnow()
        if self.status == AllocationStatus.TERMINATED and self.last_modified_at:
            end_time = self.last_modified_at
            
        runtime = end_time - self.activated_at
        return runtime.total_seconds() / 3600.0
    
    def calculate_cost(self) -> Decimal:
        """Calculate total cost based on runtime"""
        runtime_hours = self.calculate_runtime_hours()
        return self.cost_per_hour * Decimal(str(runtime_hours))


class AllocationRequest(BaseModel):
    """Request for resource allocation"""
    tenant_id: str
    workload_id: str
    workload_type: str
    requirements: ResourceRequirements
    strategy: AllocationStrategy = AllocationStrategy.BALANCED
    duration_hours: float = 1.0
    start_time: Optional[datetime] = None
    pricing_preferences: List[PricingModel] = Field(
        default_factory=lambda: [PricingModel.ON_DEMAND]
    )
    tags: Dict[str, str] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        arbitrary_types_allowed = True


class AllocationResponse(BaseModel):
    """Response for allocation request"""
    success: bool
    allocation: Optional[ResourceAllocation] = None
    message: str = ""
    estimated_wait_time_seconds: Optional[int] = None
    alternative_options: List[Dict[str, Any]] = Field(default_factory=list)
    
    class Config:
        arbitrary_types_allowed = True 