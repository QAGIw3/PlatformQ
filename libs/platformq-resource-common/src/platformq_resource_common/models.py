"""
Common resource models for the Platform Q ecosystem.
"""
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, validator


# Enums
class ResourceType(str, Enum):
    """Types of resources in the platform"""
    # Compute
    CPU = "cpu"
    MEMORY = "memory"
    GPU = "gpu"
    STORAGE = "storage"
    NETWORK = "network"
    
    # Infrastructure
    COMPUTE_INSTANCE = "compute_instance"
    KUBERNETES_POD = "kubernetes_pod"
    KUBERNETES_SERVICE = "kubernetes_service"
    KUBERNETES_NAMESPACE = "kubernetes_namespace"
    
    # Data
    DATABASE = "database"
    CACHE = "cache"
    MESSAGE_QUEUE = "message_queue"
    OBJECT_STORAGE = "object_storage"
    
    # Services
    CASSANDRA = "cassandra"
    ELASTICSEARCH = "elasticsearch"
    IGNITE = "ignite"
    MINIO = "minio"
    PULSAR = "pulsar"
    CONSUL = "consul"
    VAULT = "vault"
    OPENPROJECT = "openproject"
    NEXTCLOUD = "nextcloud"
    JANUSGRAPH = "janusgraph"


class ComputeResourceType(str, Enum):
    """Types of compute resources"""
    VM = "vm"
    CONTAINER = "container"
    BARE_METAL = "bare_metal"
    SERVERLESS = "serverless"


class ProviderCapabilities(BaseModel):
    """Provider capabilities and features"""
    provider_type: ProviderType
    supported_regions: List[str]
    supported_instance_types: List[str]
    supported_os: List[str]
    pricing_models: List[PricingModel]
    features: Dict[str, bool] = Field(default_factory=dict)  # e.g., {"gpu": True, "spot": True}
    max_resources: Dict[str, float] = Field(default_factory=dict)
    min_resources: Dict[str, float] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ProviderType(str, Enum):
    """Cloud/infrastructure providers"""
    AWS = "aws"
    CLOUDSTACK = "cloudstack"
    OPENSTACK = "openstack"
    KUBERNETES = "kubernetes"
    RACKSPACE = "rackspace"
    ON_PREMISE = "on_premise"
    CROSSPLANE = "crossplane"


class ResourceStatus(str, Enum):
    """Status of a resource"""
    PENDING = "pending"
    PROVISIONING = "provisioning"
    ACTIVE = "active"
    UPDATING = "updating"
    DELETING = "deleting"
    DELETED = "deleted"
    FAILED = "failed"
    SUSPENDED = "suspended"


class AllocationStrategy(str, Enum):
    """Resource allocation strategies"""
    COST_OPTIMIZED = "cost_optimized"
    PERFORMANCE_OPTIMIZED = "performance_optimized"
    BALANCED = "balanced"
    SPOT_PREFERRED = "spot_preferred"
    RESERVED_PREFERRED = "reserved_preferred"


class PricingModel(str, Enum):
    """Pricing models for resources"""
    ON_DEMAND = "on_demand"
    SPOT = "spot"
    RESERVED = "reserved"
    SAVINGS_PLAN = "savings_plan"


class TenantTier(str, Enum):
    """Tenant subscription tiers"""
    FREE = "free"
    STARTER = "starter"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"
    CUSTOM = "custom"


class QuotaStatus(str, Enum):
    """Quota status"""
    OK = "ok"
    WARNING = "warning"
    EXCEEDED = "exceeded"
    CRITICAL = "critical"


class ScalingAction(str, Enum):
    """Types of scaling actions"""
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    SCALE_OUT = "scale_out"
    SCALE_IN = "scale_in"
    VERTICAL_SCALE = "vertical_scale"
    NO_ACTION = "no_action"


# Base Models
class ResourceSpec(BaseModel):
    """Specification for a resource requirement"""
    resource_type: ResourceType
    quantity: float = Field(gt=0)
    unit: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class ResourceRequirements(BaseModel):
    """Requirements for compute resources"""
    cpu_cores: float = Field(gt=0)
    memory_gb: float = Field(gt=0)
    storage_gb: Optional[float] = Field(default=None, ge=0)
    gpu_count: Optional[int] = Field(default=None, ge=0)
    network_bandwidth_gbps: Optional[float] = Field(default=None, ge=0)
    iops: Optional[int] = Field(default=None, ge=0)
    
    # Additional requirements
    os_type: Optional[str] = None
    os_version: Optional[str] = None
    software_requirements: List[str] = Field(default_factory=list)
    
    @validator('cpu_cores', 'memory_gb')
    def validate_positive(cls, v):
        if v <= 0:
            raise ValueError("Must be positive")
        return v


class ResourceAllocation(BaseModel):
    """Represents an allocated resource"""
    allocation_id: str
    tenant_id: str
    workload_type: str
    workload_id: str
    provider: ProviderType
    region: Optional[str] = None
    resources: ResourceRequirements
    strategy: AllocationStrategy
    pricing_model: Optional[PricingModel] = None
    status: ResourceStatus = ResourceStatus.PENDING
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    tags: Dict[str, str] = Field(default_factory=dict)
    cost_per_hour: Optional[Decimal] = None
    provider_resource_id: Optional[str] = None


class ResourceMetrics(BaseModel):
    """Container for resource metrics"""
    service_name: str
    namespace: str
    timestamp: datetime
    cpu_usage: float  # Percentage
    memory_usage: float  # Percentage
    memory_bytes: int
    network_in_bytes: int
    network_out_bytes: int
    request_rate: float  # Requests per second
    error_rate: float  # Errors per second
    response_time_p99: float  # 99th percentile response time in ms
    active_connections: int
    pod_count: int
    gpu_usage: Optional[float] = None  # For ML workloads
    storage_usage_bytes: Optional[int] = None


class ClusterMetrics(BaseModel):
    """Container for cluster-wide metrics"""
    timestamp: datetime
    total_cpu_cores: int
    used_cpu_cores: float
    total_memory_bytes: int
    used_memory_bytes: int
    total_gpu_count: int = 0
    used_gpu_count: int = 0
    node_count: int
    pod_count: int
    namespace_count: int


class ResourceQuota(BaseModel):
    """Resource quota definition"""
    tenant_id: str
    resource_type: ResourceType
    limit: float = Field(gt=0)
    used: float = Field(ge=0, default=0)
    period: str = Field(default="monthly", pattern="^(hourly|daily|weekly|monthly|yearly)$")
    soft_limit: Optional[float] = None
    hard_limit: Optional[float] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    status: QuotaStatus = QuotaStatus.OK
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def percentage_used(self) -> float:
        """Calculate percentage of quota used"""
        return (self.used / self.limit * 100) if self.limit > 0 else 0
    
    @property
    def remaining(self) -> float:
        """Calculate remaining quota"""
        return max(0, self.limit - self.used)


class ScalingPolicy(BaseModel):
    """Scaling policy for a service"""
    service_name: str
    namespace: str = "platformq"
    min_replicas: int = Field(ge=1, default=1)
    max_replicas: int = Field(ge=1, default=10)
    target_cpu_utilization: float = Field(ge=0, le=100, default=70.0)
    target_memory_utilization: float = Field(ge=0, le=100, default=80.0)
    scale_up_threshold: float = Field(ge=0, le=100, default=80.0)
    scale_down_threshold: float = Field(ge=0, le=100, default=30.0)
    scale_up_rate: float = Field(ge=1.0, default=1.5)
    scale_down_rate: float = Field(gt=0, le=1.0, default=0.8)
    cooldown_seconds: int = Field(ge=0, default=300)
    enable_vertical_scaling: bool = True
    enable_predictive_scaling: bool = True
    cost_aware: bool = True
    business_hours_only: bool = False
    priority: int = Field(ge=1, default=1)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ScalingDecision(BaseModel):
    """Container for scaling decisions"""
    decision_id: Optional[str] = None
    service_name: str
    namespace: str
    action: ScalingAction
    current_replicas: int
    target_replicas: Optional[int] = None
    current_cpu_limit: Optional[str] = None
    target_cpu_limit: Optional[str] = None
    current_memory_limit: Optional[str] = None
    target_memory_limit: Optional[str] = None
    reason: str = ""
    confidence: float = Field(ge=0, le=1, default=1.0)
    estimated_cost_impact: float = 0.0
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    applied: bool = False
    applied_at: Optional[datetime] = None


class InfrastructureResource(BaseModel):
    """Represents a provisioned infrastructure resource"""
    resource_id: str
    resource_type: ResourceType
    resource_name: str
    tenant_id: str
    status: ResourceStatus
    endpoint: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None
    configuration: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ProvisioningRequest(BaseModel):
    """Request to provision resources for a tenant"""
    request_id: Optional[str] = None
    tenant_id: str
    tenant_name: str
    tier: TenantTier = TenantTier.STARTER
    resources: List[ResourceType] = Field(default_factory=list)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    requested_by: str
    requested_at: datetime = Field(default_factory=datetime.utcnow)


class ProvisioningStatus(str, Enum):
    """Status of provisioning request"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIALLY_COMPLETED = "partially_completed"
    ROLLED_BACK = "rolled_back"


class ProvisioningResult(BaseModel):
    """Result of a provisioning operation"""
    request_id: str
    status: ProvisioningStatus
    provisioned_resources: List[InfrastructureResource] = Field(default_factory=list)
    failed_resources: List[Dict[str, Any]] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# Event Models
class BaseEvent(BaseModel):
    """Base class for all events"""
    event_id: str = Field(default_factory=lambda: str(datetime.utcnow().timestamp()))
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source_service: str
    tenant_id: Optional[str] = None
    correlation_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ResourceAnomalyEvent(BaseEvent):
    """Event for resource anomalies"""
    service_name: str
    namespace: str
    anomaly_type: str
    severity: float = Field(ge=0, le=1)
    details: Dict[str, Any]


class ScalingEvent(BaseEvent):
    """Event for scaling actions"""
    service_name: str
    namespace: str
    action: str
    old_replicas: Optional[int] = None
    new_replicas: Optional[int] = None
    old_cpu_limit: Optional[str] = None
    new_cpu_limit: Optional[str] = None
    old_memory_limit: Optional[str] = None
    new_memory_limit: Optional[str] = None
    reason: str


class QuotaExceededEvent(BaseEvent):
    """Event when quota is exceeded"""
    resource_type: ResourceType
    current_usage: float
    quota_limit: float
    requested_amount: float
    action_taken: str


class AllocationEvent(BaseEvent):
    """Event for resource allocation"""
    allocation_id: str
    event_type: str  # created, updated, deleted
    allocation: Optional[ResourceAllocation] = None
    previous_state: Optional[ResourceAllocation] = None


class TenantCreatedEvent(BaseEvent):
    """Event when a new tenant is created"""
    tenant_name: str
    tier: TenantTier
    initial_quotas: Dict[str, float] = Field(default_factory=dict)


class TenantDeletedEvent(BaseEvent):
    """Event when a tenant is deleted"""
    reason: Optional[str] = None
    deleted_by: str


class TenantUpgradedEvent(BaseEvent):
    """Event when a tenant upgrades their tier"""
    old_tier: TenantTier
    new_tier: TenantTier
    upgraded_by: str


class UserCreatedEvent(BaseEvent):
    """Event when a user is created"""
    user_id: str
    username: str
    email: str
    roles: List[str] = Field(default_factory=list)
    groups: List[str] = Field(default_factory=list)


# Request/Response Models
class AllocationRequest(BaseModel):
    """Request for allocating compute resources"""
    tenant_id: str
    workload_type: str
    workload_id: str
    requirements: ResourceRequirements
    strategy: AllocationStrategy = AllocationStrategy.BALANCED
    preferred_providers: List[ProviderType] = Field(default_factory=list)
    preferred_regions: List[str] = Field(default_factory=list)
    duration_hours: float = Field(gt=0, default=1.0)
    pricing_preferences: List[PricingModel] = Field(default_factory=list)
    tags: Dict[str, str] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class AllocationResponse(BaseModel):
    """Response for allocation request"""
    allocation: Optional[ResourceAllocation] = None
    success: bool
    message: Optional[str] = None
    alternatives: List[Dict[str, Any]] = Field(default_factory=list)
    estimated_cost: Optional[Decimal] = None 