"""Settlement and risk data models"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from enum import Enum
from pydantic import BaseModel, Field, validator
import uuid


class SettlementStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    DISPUTED = "disputed"


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RiskModel(str, Enum):
    PROBABILISTIC = "probabilistic"
    SA_CCR = "sa_ccr"
    MONTE_CARLO = "monte_carlo"


class ResourceType(str, Enum):
    CPU = "cpu"
    GPU = "gpu"
    MEMORY = "memory"
    STORAGE = "storage"
    NETWORK = "network"
    COMPOSITE = "composite"


class Settlement(BaseModel):
    """Settlement record for compute resource transactions"""
    
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    trade_id: str
    buyer_id: str
    seller_id: str
    provider_id: str
    
    # Resource details
    resource_type: ResourceType
    quantity: float
    unit_price: float
    total_value: float
    
    # Timing
    trade_timestamp: datetime
    delivery_start: datetime
    delivery_end: datetime
    settlement_timestamp: Optional[datetime] = None
    
    # Status
    status: SettlementStatus = SettlementStatus.PENDING
    
    # Risk assessment
    risk_score: Optional[float] = None
    risk_level: Optional[RiskLevel] = None
    risk_model_used: Optional[RiskModel] = None
    risk_factors: Optional[Dict[str, Any]] = None
    
    # Billing
    billing_id: Optional[str] = None
    metering_id: Optional[str] = None
    
    # Escrow
    escrow_amount: Optional[float] = None
    escrow_released: bool = False
    
    # Additional metadata
    metadata: Dict[str, Any] = Field(default_factory=dict)


class RiskAssessment(BaseModel):
    """Risk assessment result"""
    
    settlement_id: str
    assessment_timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Risk scores by model
    probabilistic_score: Optional[float] = None
    sa_ccr_exposure: Optional[float] = None
    monte_carlo_var: Optional[float] = None  # Value at Risk
    monte_carlo_cvar: Optional[float] = None  # Conditional VaR
    
    # Combined assessment
    final_score: float
    risk_level: RiskLevel
    confidence_level: float
    
    # Risk factors
    sla_uptime: Optional[float] = None
    historical_volatility: Optional[float] = None
    provider_reliability_score: Optional[float] = None
    replacement_cost: Optional[float] = None
    potential_future_exposure: Optional[float] = None
    
    # Recommendations
    require_escrow: bool = False
    escrow_percentage: float = 0.0
    risk_premium: float = 0.0
    diversification_needed: bool = False
    
    # Detailed breakdown
    risk_breakdown: Dict[str, Any] = Field(default_factory=dict)
    mitigation_strategies: List[str] = Field(default_factory=list)


class ProviderMetrics(BaseModel):
    """Provider reliability metrics"""
    
    provider_id: str
    measurement_period_days: int
    
    # SLA metrics
    uptime_percentage: float
    average_response_time_ms: float
    total_incidents: int
    critical_incidents: int
    
    # Capacity metrics
    total_capacity: Dict[str, float]  # by resource type
    utilized_capacity: Dict[str, float]
    overcommit_ratio: float
    
    # Historical performance
    completed_settlements: int
    failed_settlements: int
    disputed_settlements: int
    average_settlement_time_hours: float
    
    # Financial metrics
    total_value_settled: float
    average_transaction_value: float
    payment_default_rate: float
    
    last_updated: datetime = Field(default_factory=datetime.utcnow)


class SettlementBatch(BaseModel):
    """Batch of settlements for processing"""
    
    batch_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    settlements: List[Settlement]
    created_at: datetime = Field(default_factory=datetime.utcnow)
    processing_started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    total_value: float = 0.0
    total_risk_exposure: float = 0.0
    
    @validator('total_value', always=True)
    def calculate_total_value(cls, v, values):
        if 'settlements' in values:
            return sum(s.total_value for s in values['settlements'])
        return v


class MonteCarloSimulation(BaseModel):
    """Monte Carlo simulation parameters and results"""
    
    settlement_id: str
    num_simulations: int
    confidence_level: float
    
    # Input parameters
    expected_uptime: float
    uptime_volatility: float
    capacity_value: float
    downtime_penalty_factor: float
    
    # Results
    simulated_losses: List[float] = Field(default_factory=list)
    value_at_risk: float  # VaR at confidence level
    conditional_value_at_risk: float  # CVaR
    expected_loss: float
    worst_case_loss: float
    
    # Statistics
    loss_distribution_stats: Dict[str, float] = Field(default_factory=dict)
    scenario_probabilities: Dict[str, float] = Field(default_factory=dict) 