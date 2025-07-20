"""Common resource management models"""

from enum import Enum
from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field
import uuid


class ScalingAction(str, Enum):
    """Types of scaling actions"""
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    SCALE_OUT = "scale_out"
    SCALE_IN = "scale_in"
    VERTICAL_SCALE = "vertical_scale"
    NO_ACTION = "no_action"


class ResourceMetrics(BaseModel):
    """Container for resource metrics"""
    service_name: str
    namespace: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    cpu_usage: float  # Percentage
    memory_usage: float  # Percentage
    memory_bytes: int
    network_in_bytes: int = 0
    network_out_bytes: int = 0
    request_rate: float = 0.0  # Requests per second
    error_rate: float = 0.0  # Errors per second
    response_time_p99: float = 0.0  # 99th percentile response time in ms
    active_connections: int = 0
    pod_count: int = 1
    gpu_usage: Optional[float] = None  # For ML workloads
    storage_usage_bytes: Optional[int] = None
    
    class Config:
        use_enum_values = True


class ClusterMetrics(BaseModel):
    """Container for cluster-wide metrics"""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    total_cpu_cores: int
    used_cpu_cores: float
    total_memory_bytes: int
    used_memory_bytes: int
    total_gpu_count: int = 0
    used_gpu_count: int = 0
    node_count: int
    pod_count: int
    namespace_count: int
    
    @property
    def cpu_utilization(self) -> float:
        """Calculate CPU utilization percentage"""
        if self.total_cpu_cores == 0:
            return 0.0
        return (self.used_cpu_cores / self.total_cpu_cores) * 100
    
    @property
    def memory_utilization(self) -> float:
        """Calculate memory utilization percentage"""
        if self.total_memory_bytes == 0:
            return 0.0
        return (self.used_memory_bytes / self.total_memory_bytes) * 100


class ScalingDecision(BaseModel):
    """Container for scaling decisions"""
    decision_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
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
    confidence: float = 1.0
    estimated_cost_impact: float = 0.0
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    applied: bool = False
    applied_at: Optional[datetime] = None
    
    class Config:
        use_enum_values = True


class ScalingPolicy(BaseModel):
    """Scaling policy for a service"""
    service_name: str
    min_replicas: int = 1
    max_replicas: int = 10
    target_cpu_utilization: float = 70.0
    target_memory_utilization: float = 80.0
    scale_up_threshold: float = 80.0
    scale_down_threshold: float = 30.0
    scale_up_rate: float = 1.5  # Multiply replicas by this
    scale_down_rate: float = 0.8  # Multiply replicas by this
    cooldown_seconds: int = 300  # 5 minutes
    enable_vertical_scaling: bool = True
    enable_predictive_scaling: bool = True
    cost_aware: bool = True
    business_hours_only: bool = False  # Scale down after hours
    priority: int = 1  # Higher priority services scale first
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class ResourceQuota(BaseModel):
    """Resource quota for a tenant"""
    quota_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str
    tier: str
    max_cpu_cores: int
    max_memory_gb: int
    max_storage_gb: int
    max_pods: int
    max_services: int
    max_gpu_count: int = 0
    max_monthly_cost: float = 0.0  # 0 means unlimited
    priority: int = 1  # Higher priority gets resources first
    
    # Burst limits
    burst_cpu_cores: Optional[int] = None
    burst_memory_gb: Optional[int] = None
    burst_duration_hours: int = 24
    burst_enabled_until: Optional[datetime] = None
    
    # Fair share weights
    cpu_weight: float = 1.0
    memory_weight: float = 1.0
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class ResourceUsage(BaseModel):
    """Current resource usage for a tenant"""
    tenant_id: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    cpu_cores_used: float
    memory_gb_used: float
    storage_gb_used: float
    pod_count: int
    service_count: int
    gpu_count_used: int = 0
    monthly_cost: float = 0.0
    
    # Usage trends
    cpu_usage_trend: float = 0.0  # Percentage change over last hour
    memory_usage_trend: float = 0.0
    cost_trend: float = 0.0
    
    class Config:
        use_enum_values = True


class ResourceAllocation(BaseModel):
    """Represents an allocated resource"""
    allocation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str
    service_name: str
    resource_type: str  # cpu, memory, storage, gpu
    allocated_amount: float
    unit: str  # cores, GB, etc
    allocated_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    cost_per_hour: float = 0.0
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True


class ResourceAnomalyEvent(BaseModel):
    """Event for resource anomalies"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    service_name: str
    namespace: str
    anomaly_type: str  # high_cpu, high_memory, high_error_rate, slow_response
    severity: float  # 0.0 to 1.0
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    current_value: float
    threshold_value: float
    details: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True 