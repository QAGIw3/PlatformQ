"""
Market Aggregation Models
"""
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum
from pydantic import BaseModel, Field, validator


# Enums
class ResourceType(str, Enum):
    """Types of compute resources"""
    QUANTUM = "quantum"
    AI = "ai"
    NETWORK = "network"


class BundleStatus(str, Enum):
    """Bundle allocation status"""
    PENDING = "pending"
    ACTIVE = "active"
    PARTIALLY_FULFILLED = "partially_fulfilled"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


class OptimizationObjective(str, Enum):
    """Optimization objectives for resource allocation"""
    MINIMIZE_COST = "minimize_cost"
    MAXIMIZE_PERFORMANCE = "maximize_performance"
    MINIMIZE_LATENCY = "minimize_latency"
    BALANCE_COST_PERFORMANCE = "balance_cost_performance"


class ArbitrageType(str, Enum):
    """Types of arbitrage opportunities"""
    PRICE_DIFFERENTIAL = "price_differential"
    QUALITY_ARBITRAGE = "quality_arbitrage"
    TIME_ARBITRAGE = "time_arbitrage"
    CROSS_MARKET = "cross_market"


# Base Models
class ResourceRequirement(BaseModel):
    """Base resource requirement"""
    resource_type: ResourceType
    specifications: Dict[str, Any]
    constraints: Optional[Dict[str, Any]] = {}
    priority: int = Field(1, ge=1, le=10)
    
    class Config:
        use_enum_values = True


class QuantumRequirement(ResourceRequirement):
    """Quantum resource requirement"""
    qpu_id: Optional[str] = None
    min_qubit_count: int = Field(..., ge=1)
    min_coherence_minutes: float = Field(..., ge=0.1)
    max_error_rate: Optional[float] = Field(None, le=0.1)
    gate_types: Optional[List[str]] = None
    algorithm_id: Optional[str] = None
    
    def __init__(self, **data):
        data['resource_type'] = ResourceType.QUANTUM
        super().__init__(**data)


class AIRequirement(ResourceRequirement):
    """AI accelerator requirement"""
    accelerator_id: Optional[str] = None
    accelerator_type: str  # TPU, GPU, NPU, ASIC
    min_tflops: float = Field(..., ge=1)
    duration_hours: float = Field(..., ge=0.1)
    memory_gb: Optional[int] = None
    precision: Optional[str] = "fp16"
    model_type: Optional[str] = None
    
    def __init__(self, **data):
        data['resource_type'] = ResourceType.AI
        super().__init__(**data)


class NetworkRequirement(ResourceRequirement):
    """Network resource requirement"""
    path_id: Optional[str] = None
    source_node: str
    destination_node: str
    min_bandwidth_mbps: int = Field(..., ge=1)
    max_latency_ms: Optional[float] = None
    qos_class: Optional[str] = "best_effort"
    duration_hours: float = Field(..., ge=0.1)
    burst_allowed: bool = False
    
    def __init__(self, **data):
        data['resource_type'] = ResourceType.NETWORK
        super().__init__(**data)


# Bundle Models
class ResourceBundle(BaseModel):
    """Bundle of multiple compute resources"""
    bundle_id: str
    name: Optional[str] = None
    description: Optional[str] = None
    requirements: List[ResourceRequirement]
    optimization_objective: OptimizationObjective = OptimizationObjective.MINIMIZE_COST
    constraints: Dict[str, Any] = {}
    created_at: datetime
    user_address: str
    
    @validator('requirements')
    def validate_requirements(cls, v):
        if not v:
            raise ValueError("Bundle must contain at least one requirement")
        if len(v) > 10:  # Reasonable limit
            raise ValueError("Bundle cannot contain more than 10 requirements")
        return v


class ResourceAllocation(BaseModel):
    """Allocated resource details"""
    resource_type: ResourceType
    resource_id: str
    allocation_id: str
    specifications: Dict[str, Any]
    price_per_hour: float
    total_cost: float
    start_time: datetime
    end_time: datetime
    quality_score: Optional[float] = None
    provider_info: Optional[Dict[str, Any]] = {}


class BundleAllocation(BaseModel):
    """Allocated resource bundle"""
    bundle_id: str
    allocation_id: str
    status: BundleStatus
    allocations: List[ResourceAllocation]
    total_cost: float
    bundle_discount: float
    final_cost: float
    optimization_score: float
    created_at: datetime
    expires_at: datetime
    blockchain_tx_hash: Optional[str] = None
    
    @validator('final_cost')
    def validate_final_cost(cls, v, values):
        if 'total_cost' in values and 'bundle_discount' in values:
            expected = values['total_cost'] * (1 - values['bundle_discount'])
            if abs(v - expected) > 0.01:  # Allow small floating point differences
                raise ValueError("Final cost doesn't match total cost minus discount")
        return v


# Arbitrage Models
class ArbitrageOpportunity(BaseModel):
    """Identified arbitrage opportunity"""
    opportunity_id: str
    arbitrage_type: ArbitrageType
    resource_type: ResourceType
    resource_id: str
    market_a: str  # e.g., "spot"
    market_b: str  # e.g., "futures"
    price_a: float
    price_b: float
    quantity: float
    potential_profit: float
    profit_margin: float
    expires_at: datetime
    confidence: float = Field(..., ge=0, le=1)
    execution_time_estimate: float  # seconds
    risk_score: float = Field(..., ge=0, le=1)
    
    @validator('profit_margin')
    def validate_profit_margin(cls, v, values):
        if 'price_a' in values and 'price_b' in values and values['price_a'] > 0:
            expected = abs(values['price_b'] - values['price_a']) / values['price_a']
            if abs(v - expected) > 0.001:
                raise ValueError("Profit margin calculation incorrect")
        return v


class ArbitrageExecution(BaseModel):
    """Arbitrage execution record"""
    execution_id: str
    opportunity_id: str
    executed_at: datetime
    buy_market: str
    sell_market: str
    quantity_executed: float
    buy_price: float
    sell_price: float
    actual_profit: float
    fees: float
    net_profit: float
    execution_time_ms: float
    success: bool
    error_message: Optional[str] = None
    blockchain_tx_hashes: List[str] = []


# Optimization Models
class OptimizationRequest(BaseModel):
    """Request for resource optimization"""
    bundle: ResourceBundle
    budget_limit: Optional[float] = None
    time_constraints: Optional[Dict[str, datetime]] = None
    quality_thresholds: Optional[Dict[ResourceType, float]] = None
    preferred_providers: Optional[Dict[ResourceType, List[str]]] = None


class OptimizationResult(BaseModel):
    """Result of optimization algorithm"""
    request_id: str
    bundle_id: str
    optimal_allocations: List[ResourceAllocation]
    total_cost: float
    performance_score: float
    optimization_time_ms: float
    algorithm_used: str
    iterations_performed: int
    constraints_satisfied: bool
    warnings: List[str] = []
    alternative_options: Optional[List[Dict]] = None


# Workload Models
class WorkloadTemplate(BaseModel):
    """Predefined workload template"""
    template_id: str
    name: str
    description: str
    resource_requirements: Dict[ResourceType, Dict[str, Any]]
    typical_duration_hours: float
    estimated_cost_range: tuple[float, float]
    use_cases: List[str]
    performance_metrics: Dict[str, Any]


class WorkloadExecution(BaseModel):
    """Workload execution tracking"""
    execution_id: str
    template_id: Optional[str] = None
    bundle_allocation_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    status: str  # running, completed, failed
    performance_metrics: Dict[str, float] = {}
    resource_utilization: Dict[str, float] = {}
    total_cost: float
    quality_scores: Dict[ResourceType, float] = {}


# API Request/Response Models
class BundleCreateRequest(BaseModel):
    """Request to create a resource bundle"""
    name: Optional[str] = None
    description: Optional[str] = None
    requirements: List[Dict[str, Any]]  # Will be parsed into specific requirement types
    optimization_objective: OptimizationObjective = OptimizationObjective.MINIMIZE_COST
    constraints: Dict[str, Any] = {}


class BundleAllocationRequest(BaseModel):
    """Request to allocate a resource bundle"""
    bundle_id: str
    start_time: Optional[datetime] = None
    duration_hours: float = Field(..., ge=0.1)
    budget_limit: Optional[float] = None
    quality_thresholds: Optional[Dict[str, float]] = None


class ArbitrageSearchRequest(BaseModel):
    """Request to search for arbitrage opportunities"""
    resource_types: Optional[List[ResourceType]] = None
    min_profit_margin: float = Field(0.02, ge=0)
    max_risk_score: float = Field(0.5, le=1)
    time_horizon_minutes: int = Field(60, ge=1)


class MarketComparisonRequest(BaseModel):
    """Request to compare prices across markets"""
    resource_type: ResourceType
    specifications: Dict[str, Any]
    duration_hours: float = Field(1, ge=0.1)
    include_quality_adjusted: bool = True


# Response Models
class BundleResponse(BaseModel):
    """Response containing bundle information"""
    bundle: ResourceBundle
    estimated_cost: float
    availability_status: Dict[ResourceType, bool]
    optimization_suggestions: List[str] = []


class AllocationResponse(BaseModel):
    """Response containing allocation details"""
    allocation: BundleAllocation
    resource_details: List[ResourceAllocation]
    optimization_report: OptimizationResult
    execution_plan: Dict[str, Any]


class ArbitrageResponse(BaseModel):
    """Response containing arbitrage opportunities"""
    opportunities: List[ArbitrageOpportunity]
    total_potential_profit: float
    recommended_executions: List[Dict[str, Any]]
    market_analysis: Dict[str, Any]


class MarketComparisonResponse(BaseModel):
    """Response containing market comparison"""
    resource_type: ResourceType
    specifications: Dict[str, Any]
    market_prices: Dict[str, float]
    quality_adjusted_prices: Dict[str, float]
    best_option: str
    savings_potential: float
    recommendations: List[str]


# Events
class BundleEvent(BaseModel):
    """Bundle lifecycle event"""
    event_id: str
    bundle_id: str
    event_type: str  # created, allocated, expired, cancelled
    timestamp: datetime
    details: Dict[str, Any]
    user_address: str


class ArbitrageEvent(BaseModel):
    """Arbitrage event"""
    event_id: str
    opportunity_id: str
    event_type: str  # discovered, executed, expired
    timestamp: datetime
    details: Dict[str, Any]
    profit_impact: Optional[float] = None 