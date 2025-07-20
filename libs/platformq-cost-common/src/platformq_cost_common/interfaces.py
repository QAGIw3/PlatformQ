"""Common cost management interfaces"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from decimal import Decimal
from .models import (
    CostAnalysis,
    CostRecommendation,
    BudgetAlert,
    ResourcePricing,
    CostReport,
    PredictedCost,
    PricingModel
)


class ICostCalculator(ABC):
    """Interface for cost calculations"""
    
    @abstractmethod
    async def calculate_service_cost(self, service_name: str, 
                                   resource_usage: Dict[str, float],
                                   duration_hours: float) -> Decimal:
        """Calculate cost for a service based on resource usage"""
        pass
    
    @abstractmethod
    async def calculate_tenant_cost(self, tenant_id: str,
                                  start_date: datetime,
                                  end_date: datetime) -> Decimal:
        """Calculate total cost for a tenant over a period"""
        pass
    
    @abstractmethod
    async def get_pricing(self, resource_type: str, provider: str,
                        region: str, pricing_model: PricingModel) -> Optional[ResourcePricing]:
        """Get pricing information for a resource"""
        pass
    
    @abstractmethod
    async def update_pricing(self, pricing: ResourcePricing) -> bool:
        """Update pricing information"""
        pass


class IBudgetManager(ABC):
    """Interface for budget management"""
    
    @abstractmethod
    async def set_budget(self, tenant_id: str, monthly_limit: Decimal,
                       service_budgets: Optional[Dict[str, Decimal]] = None) -> bool:
        """Set budget for a tenant"""
        pass
    
    @abstractmethod
    async def get_budget(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        """Get budget configuration for a tenant"""
        pass
    
    @abstractmethod
    async def check_budget(self, tenant_id: str, additional_cost: Decimal) -> Tuple[bool, Optional[str]]:
        """Check if additional cost is within budget"""
        pass
    
    @abstractmethod
    async def get_budget_status(self, tenant_id: str) -> Dict[str, Any]:
        """Get current budget utilization status"""
        pass
    
    @abstractmethod
    async def create_budget_alert(self, alert: BudgetAlert) -> bool:
        """Create a budget alert"""
        pass


class ICostOptimizer(ABC):
    """Interface for cost optimization"""
    
    @abstractmethod
    async def analyze_costs(self, service_name: Optional[str] = None,
                          tenant_id: Optional[str] = None) -> CostAnalysis:
        """Analyze costs and generate recommendations"""
        pass
    
    @abstractmethod
    async def get_recommendations(self, service_name: Optional[str] = None,
                                tenant_id: Optional[str] = None) -> List[CostRecommendation]:
        """Get cost optimization recommendations"""
        pass
    
    @abstractmethod
    async def predict_costs(self, service_name: Optional[str] = None,
                          tenant_id: Optional[str] = None,
                          horizon_days: int = 30) -> PredictedCost:
        """Predict future costs"""
        pass
    
    @abstractmethod
    async def apply_recommendation(self, recommendation_id: str) -> bool:
        """Apply a cost optimization recommendation"""
        pass


class ICostRepository(ABC):
    """Interface for cost data persistence"""
    
    @abstractmethod
    async def store_cost_analysis(self, analysis: CostAnalysis) -> str:
        """Store cost analysis results"""
        pass
    
    @abstractmethod
    async def get_cost_analysis(self, analysis_id: str) -> Optional[CostAnalysis]:
        """Get cost analysis by ID"""
        pass
    
    @abstractmethod
    async def store_cost_metrics(self, service_name: str, tenant_id: str,
                               timestamp: datetime, costs: Dict[str, Decimal]) -> bool:
        """Store cost metrics"""
        pass
    
    @abstractmethod
    async def generate_cost_report(self, tenant_id: Optional[str],
                                 start_date: datetime,
                                 end_date: datetime) -> CostReport:
        """Generate cost report for a period"""
        pass
    
    @abstractmethod
    async def get_historical_costs(self, service_name: Optional[str],
                                 tenant_id: Optional[str],
                                 days: int = 30) -> List[Dict[str, Any]]:
        """Get historical cost data"""
        pass 