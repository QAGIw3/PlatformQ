"""Common cost management models"""

from enum import Enum
from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, Field
import uuid


class PricingModel(str, Enum):
    """Pricing models for resources"""
    ON_DEMAND = "on_demand"
    SPOT = "spot"
    RESERVED = "reserved"
    COMMITMENT = "commitment"


class CostRecommendationType(str, Enum):
    """Types of cost optimization recommendations"""
    DOWNSIZE = "downsize"
    UPSIZE = "upsize"
    SPOT_INSTANCES = "spot_instances"
    RESERVED_INSTANCES = "reserved_instances"
    CONSOLIDATION = "consolidation"
    SCHEDULE_BASED = "schedule_based"
    IDLE_RESOURCE = "idle_resource"


class ResourceCost(BaseModel):
    """Container for resource cost information"""
    cpu_core_hour: Decimal = Decimal("0.05")  # Cost per CPU core per hour
    memory_gb_hour: Decimal = Decimal("0.01")  # Cost per GB memory per hour
    storage_gb_month: Decimal = Decimal("0.10")  # Cost per GB storage per month
    network_gb: Decimal = Decimal("0.09")  # Cost per GB network transfer
    gpu_hour: Decimal = Decimal("0.90")  # Cost per GPU per hour
    
    # Spot/preemptible pricing
    spot_discount: Decimal = Decimal("0.3")  # 30% of regular price
    
    # Reserved instance discount
    reserved_discount: Decimal = Decimal("0.6")  # 60% of regular price
    
    # Multi-region costs
    cross_region_transfer_gb: Decimal = Decimal("0.02")
    
    # Provider-specific multipliers
    provider_multipliers: Dict[str, Decimal] = Field(default_factory=lambda: {
        "aws": Decimal("1.0"),
        "cloudstack": Decimal("0.8"),
        "kubernetes": Decimal("0.5")
    })
    
    class Config:
        json_encoders = {Decimal: str}


class CostAnalysis(BaseModel):
    """Container for cost analysis results"""
    analysis_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    service_name: str
    tenant_id: Optional[str] = None
    current_monthly_cost: Decimal
    projected_monthly_cost: Decimal
    cost_savings: Decimal
    savings_percentage: float
    optimization_recommendations: List['CostRecommendation'] = Field(default_factory=list)
    confidence: float = 0.85
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    analysis_period_days: int = 30
    
    class Config:
        json_encoders = {Decimal: str}


class CostRecommendation(BaseModel):
    """Cost optimization recommendation"""
    recommendation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: CostRecommendationType
    description: str
    estimated_monthly_savings: Decimal
    implementation_effort: str  # low, medium, high
    risk_level: str  # low, medium, high
    priority: int = Field(ge=1, le=10, default=5)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True
        json_encoders = {Decimal: str}


class BudgetAlert(BaseModel):
    """Budget alert notification"""
    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str
    service_name: Optional[str] = None
    alert_type: str  # threshold_exceeded, projection_exceeded, anomaly
    current_spend: Decimal
    budget_limit: Decimal
    threshold_percentage: float
    projected_overage: Optional[Decimal] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    message: str
    
    class Config:
        json_encoders = {Decimal: str}


class ResourcePricing(BaseModel):
    """Pricing information for a specific resource"""
    resource_type: str  # cpu, memory, storage, gpu
    provider: str
    region: str
    pricing_model: PricingModel
    price_per_unit: Decimal
    unit: str  # hour, GB-month, etc
    minimum_commitment: Optional[int] = None  # For reserved instances
    effective_date: datetime
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True
        json_encoders = {Decimal: str}


class CostReport(BaseModel):
    """Detailed cost report"""
    report_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: Optional[str] = None
    start_date: datetime
    end_date: datetime
    total_cost: Decimal
    services: Dict[str, 'ServiceCost'] = Field(default_factory=dict)
    cost_by_resource_type: Dict[str, Decimal] = Field(default_factory=dict)
    cost_by_provider: Dict[str, Decimal] = Field(default_factory=dict)
    recommendations: List[CostRecommendation] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {Decimal: str}


class ServiceCost(BaseModel):
    """Cost breakdown for a service"""
    service_name: str
    total_cost: Decimal
    cpu_cost: Decimal = Decimal("0")
    memory_cost: Decimal = Decimal("0")
    storage_cost: Decimal = Decimal("0")
    network_cost: Decimal = Decimal("0")
    gpu_cost: Decimal = Decimal("0")
    other_costs: Decimal = Decimal("0")
    daily_costs: Dict[str, Decimal] = Field(default_factory=dict)
    
    class Config:
        json_encoders = {Decimal: str}


class PredictedCost(BaseModel):
    """Predicted future costs"""
    prediction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    service_name: Optional[str] = None
    tenant_id: Optional[str] = None
    prediction_date: datetime
    horizon_days: int
    predicted_cost: Decimal
    confidence_interval_low: Decimal
    confidence_interval_high: Decimal
    confidence_level: float = 0.95
    factors: Dict[str, float] = Field(default_factory=dict)  # Contributing factors
    generated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        json_encoders = {Decimal: str}


# Update forward references
CostAnalysis.model_rebuild()
CostReport.model_rebuild() 