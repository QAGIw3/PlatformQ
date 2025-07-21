"""
Network Bandwidth Resource Models
"""
from datetime import datetime
from typing import Dict, List, Optional
from enum import Enum
from pydantic import BaseModel, Field, validator


# Enums
class BandwidthClass(str, Enum):
    """QoS classes for bandwidth allocation"""
    BEST_EFFORT = "best_effort"
    BRONZE = "bronze" 
    SILVER = "silver"
    GOLD = "gold"
    PLATINUM = "platinum"


class PathStatus(str, Enum):
    """Network path operational status"""
    ACTIVE = "active"
    DEGRADED = "degraded"
    CONGESTED = "congested"
    MAINTENANCE = "maintenance"
    OFFLINE = "offline"


class CircuitType(str, Enum):
    """Types of dedicated circuits"""
    POINT_TO_POINT = "point_to_point"
    MULTIPOINT = "multipoint"
    MESH = "mesh"
    HUB_SPOKE = "hub_spoke"


class AllocationStatus(str, Enum):
    """Bandwidth allocation status"""
    PENDING = "pending"
    ACTIVE = "active"
    SUSPENDED = "suspended"
    EXPIRED = "expired"
    TERMINATED = "terminated"


class CongestionLevel(str, Enum):
    """Network congestion levels"""
    NONE = "none"
    LOW = "low"
    MODERATE = "moderate"
    HIGH = "high"
    SEVERE = "severe"


# Base Models
class NetworkNode(BaseModel):
    """Network node/endpoint representation"""
    node_id: str
    name: str
    location: str
    provider: str
    capabilities: List[str]
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class QoSParameters(BaseModel):
    """Quality of Service parameters"""
    bandwidth_mbps: int = Field(..., ge=1)
    latency_ms: float = Field(..., ge=0)
    jitter_ms: float = Field(..., ge=0)
    packet_loss_rate: float = Field(..., ge=0, le=1)
    priority: int = Field(..., ge=0, le=255)
    
    @validator('packet_loss_rate')
    def validate_packet_loss(cls, v):
        if v > 0.1:  # More than 10% loss is unusual
            raise ValueError("Packet loss rate unusually high")
        return v


class NetworkPath(BaseModel):
    """Network path between endpoints"""
    path_id: str
    source: NetworkNode
    destination: NetworkNode
    hops: List[NetworkNode]
    total_distance_km: float
    latency_ms: float
    available_bandwidth_mbps: int
    max_bandwidth_mbps: int
    reliability_score: float = Field(..., ge=0, le=1)
    status: PathStatus
    provider_path_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    
    @validator('hops')
    def validate_hops(cls, v):
        if len(v) > 10:
            raise ValueError("Path has too many hops")
        return v


class BandwidthAllocation(BaseModel):
    """Bandwidth allocation record"""
    allocation_id: str
    user_address: str
    path_id: str
    bandwidth_mbps: int
    qos_class: BandwidthClass
    qos_parameters: QoSParameters
    start_time: datetime
    end_time: datetime
    status: AllocationStatus
    price_per_hour: float
    total_cost: float
    burst_allowed: bool = False
    burst_limit_mbps: Optional[int] = None
    token_id: Optional[int] = None
    created_at: datetime
    
    @validator('end_time')
    def validate_end_time(cls, v, values):
        if 'start_time' in values and v <= values['start_time']:
            raise ValueError("End time must be after start time")
        return v


class BurstRequest(BaseModel):
    """Burst bandwidth request"""
    burst_id: str
    allocation_id: str
    requested_bandwidth_mbps: int
    duration_seconds: int
    urgency_factor: float = Field(1.0, ge=1.0, le=5.0)
    approved: bool = False
    actual_bandwidth_mbps: Optional[int] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    price: Optional[float] = None
    created_at: datetime


class DedicatedCircuit(BaseModel):
    """Dedicated network circuit"""
    circuit_id: str
    user_address: str
    circuit_type: CircuitType
    endpoints: List[NetworkNode]
    bandwidth_mbps: int
    guaranteed_latency_ms: float
    redundancy_enabled: bool
    redundant_paths: Optional[List[str]] = None
    start_date: datetime
    end_date: datetime
    monthly_cost: float
    sla_parameters: Dict[str, float]
    status: AllocationStatus
    token_id: Optional[int] = None
    created_at: datetime
    
    @validator('endpoints')
    def validate_endpoints(cls, v, values):
        if 'circuit_type' in values:
            if values['circuit_type'] == CircuitType.POINT_TO_POINT and len(v) != 2:
                raise ValueError("Point-to-point circuit must have exactly 2 endpoints")
            elif len(v) < 2:
                raise ValueError("Circuit must have at least 2 endpoints")
        return v


class LatencyFuture(BaseModel):
    """Latency guarantee future contract"""
    contract_id: str
    user_address: str
    source: str
    destination: str
    guaranteed_latency_ms: float
    measurement_interval_seconds: int = 60
    contract_duration_hours: int
    penalty_rate: float  # Percentage penalty per ms over guarantee
    premium_paid: float
    status: AllocationStatus
    measurements: List[float] = []
    violations_count: int = 0
    total_penalties: float = 0
    start_time: datetime
    end_time: datetime
    created_at: datetime


class CongestionMetrics(BaseModel):
    """Network congestion metrics"""
    path_id: str
    timestamp: datetime
    utilization_percent: float = Field(..., ge=0, le=100)
    congestion_level: CongestionLevel
    available_bandwidth_mbps: int
    queue_depth: int
    packet_loss_rate: float
    average_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    predicted_congestion_1h: Optional[float] = None
    
    @validator('congestion_level')
    def validate_congestion_level(cls, v, values):
        if 'utilization_percent' in values:
            util = values['utilization_percent']
            expected_level = (
                CongestionLevel.NONE if util < 50 else
                CongestionLevel.LOW if util < 70 else
                CongestionLevel.MODERATE if util < 85 else
                CongestionLevel.HIGH if util < 95 else
                CongestionLevel.SEVERE
            )
            if v != expected_level:
                return expected_level
        return v


class PathPricing(BaseModel):
    """Dynamic pricing for network paths"""
    path_id: str
    timestamp: datetime
    base_price_per_mbps_hour: float
    congestion_multiplier: float
    time_of_day_multiplier: float
    qos_multipliers: Dict[BandwidthClass, float]
    burst_multiplier: float
    current_utilization: float
    spot_price_per_mbps_hour: float
    
    @validator('spot_price_per_mbps_hour')
    def calculate_spot_price(cls, v, values):
        if v == 0 and all(k in values for k in ['base_price_per_mbps_hour', 'congestion_multiplier', 'time_of_day_multiplier']):
            return (values['base_price_per_mbps_hour'] * 
                   values['congestion_multiplier'] * 
                   values['time_of_day_multiplier'])
        return v


# API Request/Response Models
class PathRegistrationRequest(BaseModel):
    """Request to register a new network path"""
    source: NetworkNode
    destination: NetworkNode
    hops: List[NetworkNode]
    max_bandwidth_mbps: int
    base_latency_ms: float
    provider_path_id: Optional[str] = None


class BandwidthAllocationRequest(BaseModel):
    """Request to allocate bandwidth"""
    path_id: str
    bandwidth_mbps: int = Field(..., ge=10, le=10000)
    qos_class: BandwidthClass
    duration_hours: int = Field(..., ge=1, le=720)  # Max 30 days
    start_time: Optional[datetime] = None
    burst_allowed: bool = False
    burst_limit_mbps: Optional[int] = None
    
    @validator('burst_limit_mbps')
    def validate_burst_limit(cls, v, values):
        if values.get('burst_allowed') and v is None:
            raise ValueError("Burst limit required when burst is allowed")
        if v and values.get('bandwidth_mbps') and v <= values['bandwidth_mbps']:
            raise ValueError("Burst limit must exceed allocated bandwidth")
        return v


class BurstCapacityRequest(BaseModel):
    """Request for burst capacity"""
    allocation_id: str
    additional_bandwidth_mbps: int = Field(..., ge=10)
    duration_seconds: int = Field(..., ge=60, le=3600)
    urgency_factor: float = Field(1.0, ge=1.0, le=5.0)


class CircuitProvisionRequest(BaseModel):
    """Request to provision dedicated circuit"""
    circuit_type: CircuitType
    endpoints: List[NetworkNode]
    bandwidth_mbps: int = Field(..., ge=100)
    latency_requirement_ms: Optional[float] = None
    redundancy: bool = False
    duration_days: int = Field(..., ge=1, le=365)
    start_date: Optional[datetime] = None


class LatencyFutureRequest(BaseModel):
    """Request to create latency future contract"""
    source: str
    destination: str
    guaranteed_latency_ms: float = Field(..., ge=1)
    duration_hours: int = Field(..., ge=1, le=720)
    penalty_rate: float = Field(0.1, ge=0.01, le=1.0)  # 1-100% penalty


class PathSearchRequest(BaseModel):
    """Request to search for network paths"""
    source: Optional[str] = None
    destination: Optional[str] = None
    min_bandwidth_mbps: Optional[int] = None
    max_latency_ms: Optional[float] = None
    max_hops: Optional[int] = None
    providers: Optional[List[str]] = None
    status: Optional[List[PathStatus]] = None


# Response Models
class PathResponse(BaseModel):
    """Network path response"""
    path: NetworkPath
    current_pricing: PathPricing
    congestion_metrics: CongestionMetrics
    available_qos_classes: List[BandwidthClass]


class AllocationResponse(BaseModel):
    """Bandwidth allocation response"""
    allocation: BandwidthAllocation
    path_details: NetworkPath
    estimated_performance: QoSParameters
    blockchain_tx_hash: Optional[str] = None


class BurstResponse(BaseModel):
    """Burst request response"""
    burst_request: BurstRequest
    approved: bool
    reason: Optional[str] = None
    alternative_options: Optional[List[Dict]] = None


class CircuitResponse(BaseModel):
    """Circuit provision response"""
    circuit: DedicatedCircuit
    selected_paths: List[NetworkPath]
    estimated_setup_time: int  # seconds
    blockchain_tx_hash: Optional[str] = None


class PricingResponse(BaseModel):
    """Pricing information response"""
    path_id: str
    pricing: PathPricing
    qos_prices: Dict[BandwidthClass, float]
    burst_price_per_gb: float
    volume_discounts: Optional[Dict[str, float]] = None


class CongestionResponse(BaseModel):
    """Congestion status response"""
    metrics: List[CongestionMetrics]
    predictions: Dict[str, float]  # path_id -> predicted utilization
    recommended_alternatives: Optional[List[str]] = None


# Events for Pulsar
class BandwidthEvent(BaseModel):
    """Base bandwidth event"""
    event_id: str
    event_type: str
    timestamp: datetime
    path_id: Optional[str] = None
    user_address: Optional[str] = None
    details: Dict


class CongestionEvent(BaseModel):
    """Congestion event notification"""
    event_id: str
    path_id: str
    timestamp: datetime
    congestion_level: CongestionLevel
    utilization_percent: float
    affected_allocations: List[str]
    estimated_duration_minutes: Optional[int] = None


class CircuitEvent(BaseModel):
    """Circuit lifecycle event"""
    event_id: str
    circuit_id: str
    event_type: str  # provisioned, modified, failed, decommissioned
    timestamp: datetime
    details: Dict
    affected_users: List[str] 