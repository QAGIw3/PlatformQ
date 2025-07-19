"""Unified cost management for compute resources

This module provides cost calculation, optimization, and budget management.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
import logging

from .models import (
    ResourceRequirements,
    ResourceAllocation,
    ProviderType,
    PricingModel,
    ComputeResourceType
)

logger = logging.getLogger(__name__)


@dataclass
class ResourceCost:
    """Cost configuration for different resource types"""
    # Base costs (per hour unless specified)
    cpu_core_hour: Decimal = Decimal("0.05")
    memory_gb_hour: Decimal = Decimal("0.01")
    storage_gb_month: Decimal = Decimal("0.10")
    network_gb: Decimal = Decimal("0.09")
    gpu_hour: Dict[str, Decimal] = None  # GPU type to cost mapping
    
    # Pricing model multipliers
    spot_discount: Decimal = Decimal("0.3")  # 70% discount
    reserved_discount: Decimal = Decimal("0.6")  # 40% discount
    preemptible_discount: Decimal = Decimal("0.25")  # 75% discount
    
    # Regional cost multipliers
    region_multipliers: Dict[str, Decimal] = None
    
    # Provider-specific multipliers
    provider_multipliers: Dict[ProviderType, Decimal] = None
    
    def __post_init__(self):
        if self.gpu_hour is None:
            self.gpu_hour = {
                "nvidia-t4": Decimal("0.526"),
                "nvidia-v100": Decimal("2.48"),
                "nvidia-a100": Decimal("3.06"),
                "nvidia-h100": Decimal("4.50"),
                "amd-mi100": Decimal("2.10"),
            }
        
        if self.region_multipliers is None:
            self.region_multipliers = {
                "us-east-1": Decimal("1.0"),
                "us-west-2": Decimal("1.05"),
                "eu-west-1": Decimal("1.1"),
                "ap-southeast-1": Decimal("1.15"),
                "ap-northeast-1": Decimal("1.2"),
            }
        
        if self.provider_multipliers is None:
            self.provider_multipliers = {
                ProviderType.AWS: Decimal("1.0"),
                ProviderType.AZURE: Decimal("0.95"),
                ProviderType.GCP: Decimal("0.98"),
                ProviderType.ON_PREMISE: Decimal("0.7"),
                ProviderType.EDGE: Decimal("1.3"),
            }


@dataclass
class CostAnalysis:
    """Result of cost analysis"""
    resource_costs: Dict[ComputeResourceType, Decimal]
    total_hourly_cost: Decimal
    total_monthly_cost: Decimal
    effective_pricing_model: PricingModel
    discount_applied: Decimal
    cost_breakdown: Dict[str, Decimal]
    recommendations: List[str]
    savings_opportunities: List[Dict[str, Any]]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "resource_costs": {k.value: str(v) for k, v in self.resource_costs.items()},
            "total_hourly_cost": str(self.total_hourly_cost),
            "total_monthly_cost": str(self.total_monthly_cost),
            "effective_pricing_model": self.effective_pricing_model.value,
            "discount_applied": str(self.discount_applied),
            "cost_breakdown": {k: str(v) for k, v in self.cost_breakdown.items()},
            "recommendations": self.recommendations,
            "savings_opportunities": self.savings_opportunities
        }


class CostCalculator:
    """Calculate and optimize costs for compute resources"""
    
    def __init__(self, cost_config: Optional[ResourceCost] = None):
        self.cost_config = cost_config or ResourceCost()
        
    def calculate_requirements_cost(
        self,
        requirements: ResourceRequirements,
        provider: ProviderType,
        region: str,
        pricing_model: PricingModel = PricingModel.ON_DEMAND,
        duration_hours: float = 1.0
    ) -> CostAnalysis:
        """Calculate cost for resource requirements"""
        
        # Calculate base costs
        resource_costs = {}
        
        # CPU cost
        cpu_cost = (
            self.cost_config.cpu_core_hour * 
            Decimal(str(requirements.cpu_cores)) * 
            Decimal(str(duration_hours))
        )
        resource_costs[ComputeResourceType.CPU] = cpu_cost
        
        # Memory cost
        memory_cost = (
            self.cost_config.memory_gb_hour * 
            Decimal(str(requirements.memory_gb)) * 
            Decimal(str(duration_hours))
        )
        resource_costs[ComputeResourceType.MEMORY] = memory_cost
        
        # Storage cost (monthly rate, so convert)
        storage_cost = (
            self.cost_config.storage_gb_month * 
            Decimal(str(requirements.storage_gb)) * 
            Decimal(str(duration_hours / 730.0))  # Average hours per month
        )
        resource_costs[ComputeResourceType.STORAGE] = storage_cost
        
        # GPU cost
        gpu_cost = Decimal("0")
        if requirements.gpu_count > 0 and requirements.gpu_type:
            gpu_rate = self.cost_config.gpu_hour.get(
                requirements.gpu_type,
                Decimal("2.0")  # Default GPU cost
            )
            gpu_cost = (
                gpu_rate * 
                Decimal(str(requirements.gpu_count)) * 
                Decimal(str(duration_hours))
            )
            resource_costs[ComputeResourceType.GPU] = gpu_cost
        
        # Calculate base total
        base_cost = sum(resource_costs.values())
        
        # Apply regional multiplier
        region_multiplier = self.cost_config.region_multipliers.get(
            region,
            Decimal("1.0")
        )
        regional_cost = base_cost * region_multiplier
        
        # Apply provider multiplier
        provider_multiplier = self.cost_config.provider_multipliers.get(
            provider,
            Decimal("1.0")
        )
        provider_cost = regional_cost * provider_multiplier
        
        # Apply pricing model discount
        discount = Decimal("0")
        if pricing_model == PricingModel.SPOT:
            discount = self.cost_config.spot_discount
        elif pricing_model == PricingModel.RESERVED:
            discount = self.cost_config.reserved_discount
        elif pricing_model == PricingModel.PREEMPTIBLE:
            discount = self.cost_config.preemptible_discount
        
        final_cost = provider_cost * (Decimal("1") - discount)
        
        # Generate recommendations
        recommendations = self._generate_recommendations(
            requirements,
            pricing_model,
            provider,
            region
        )
        
        # Find savings opportunities
        savings_opportunities = self._find_savings_opportunities(
            requirements,
            pricing_model,
            provider,
            region,
            final_cost
        )
        
        return CostAnalysis(
            resource_costs=resource_costs,
            total_hourly_cost=final_cost / Decimal(str(duration_hours)),
            total_monthly_cost=final_cost * Decimal("730") / Decimal(str(duration_hours)),
            effective_pricing_model=pricing_model,
            discount_applied=discount,
            cost_breakdown={
                "base_cost": base_cost,
                "regional_adjustment": regional_cost - base_cost,
                "provider_adjustment": provider_cost - regional_cost,
                "discount_savings": provider_cost - final_cost,
                "final_cost": final_cost
            },
            recommendations=recommendations,
            savings_opportunities=savings_opportunities
        )
    
    def calculate_allocation_cost(
        self,
        allocation: ResourceAllocation
    ) -> Decimal:
        """Calculate cost for an existing allocation"""
        runtime_hours = allocation.calculate_runtime_hours()
        return allocation.cost_per_hour * Decimal(str(runtime_hours))
    
    def compare_pricing_models(
        self,
        requirements: ResourceRequirements,
        provider: ProviderType,
        region: str,
        duration_hours: float = 730.0  # 1 month
    ) -> Dict[PricingModel, CostAnalysis]:
        """Compare costs across different pricing models"""
        results = {}
        
        for pricing_model in PricingModel:
            results[pricing_model] = self.calculate_requirements_cost(
                requirements,
                provider,
                region,
                pricing_model,
                duration_hours
            )
        
        return results
    
    def _generate_recommendations(
        self,
        requirements: ResourceRequirements,
        pricing_model: PricingModel,
        provider: ProviderType,
        region: str
    ) -> List[str]:
        """Generate cost optimization recommendations"""
        recommendations = []
        
        # Pricing model recommendations
        if pricing_model == PricingModel.ON_DEMAND:
            if requirements.spot_instance_acceptable:
                recommendations.append(
                    "Consider using spot instances for 70% cost savings"
                )
            elif requirements.preemptible_acceptable:
                recommendations.append(
                    "Consider using preemptible instances for 75% cost savings"
                )
        
        # Regional recommendations
        cheapest_region = min(
            self.cost_config.region_multipliers.items(),
            key=lambda x: x[1]
        )[0]
        if region != cheapest_region:
            current_mult = self.cost_config.region_multipliers.get(region, Decimal("1"))
            cheapest_mult = self.cost_config.region_multipliers[cheapest_region]
            savings_pct = (1 - cheapest_mult / current_mult) * 100
            recommendations.append(
                f"Consider {cheapest_region} region for {savings_pct:.1f}% cost savings"
            )
        
        # Provider recommendations
        if provider != ProviderType.ON_PREMISE:
            on_prem_mult = self.cost_config.provider_multipliers[ProviderType.ON_PREMISE]
            current_mult = self.cost_config.provider_multipliers.get(provider, Decimal("1"))
            if on_prem_mult < current_mult:
                savings_pct = (1 - on_prem_mult / current_mult) * 100
                recommendations.append(
                    f"Consider on-premise deployment for {savings_pct:.1f}% cost savings"
                )
        
        # Resource optimization
        if requirements.gpu_count > 0:
            recommendations.append(
                "GPU resources are expensive - ensure they're fully utilized"
            )
        
        if requirements.memory_gb > requirements.cpu_cores * 8:
            recommendations.append(
                "High memory-to-CPU ratio detected - consider memory-optimized instances"
            )
        
        return recommendations
    
    def _find_savings_opportunities(
        self,
        requirements: ResourceRequirements,
        current_pricing: PricingModel,
        provider: ProviderType,
        region: str,
        current_cost: Decimal
    ) -> List[Dict[str, Any]]:
        """Find specific cost saving opportunities"""
        opportunities = []
        
        # Check spot instance savings
        if current_pricing != PricingModel.SPOT and requirements.spot_instance_acceptable:
            spot_cost = self.calculate_requirements_cost(
                requirements,
                provider,
                region,
                PricingModel.SPOT,
                1.0
            ).total_hourly_cost
            
            savings = current_cost - spot_cost
            opportunities.append({
                "type": "pricing_model",
                "action": "Switch to spot instances",
                "current_cost": float(current_cost),
                "new_cost": float(spot_cost),
                "savings": float(savings),
                "savings_percentage": float((savings / current_cost) * 100),
                "risk": "medium",
                "implementation_effort": "low"
            })
        
        # Check reserved instance savings for long-term workloads
        if current_pricing == PricingModel.ON_DEMAND:
            reserved_cost = self.calculate_requirements_cost(
                requirements,
                provider,
                region,
                PricingModel.RESERVED,
                1.0
            ).total_hourly_cost
            
            savings = current_cost - reserved_cost
            opportunities.append({
                "type": "pricing_model",
                "action": "Purchase reserved instances (1-year commitment)",
                "current_cost": float(current_cost),
                "new_cost": float(reserved_cost),
                "savings": float(savings),
                "savings_percentage": float((savings / current_cost) * 100),
                "risk": "low",
                "implementation_effort": "medium"
            })
        
        return opportunities


class BudgetManager:
    """Manage budgets and cost controls"""
    
    def __init__(self):
        self.budgets: Dict[str, Dict[str, Any]] = {}
        self.alerts: List[Dict[str, Any]] = []
        
    def set_budget(
        self,
        tenant_id: str,
        monthly_limit: Decimal,
        alert_thresholds: List[float] = None
    ):
        """Set budget for a tenant"""
        if alert_thresholds is None:
            alert_thresholds = [0.5, 0.75, 0.9, 1.0]  # 50%, 75%, 90%, 100%
            
        self.budgets[tenant_id] = {
            "monthly_limit": monthly_limit,
            "alert_thresholds": alert_thresholds,
            "current_spend": Decimal("0"),
            "period_start": datetime.utcnow().replace(day=1),
            "alerts_sent": set()
        }
    
    def check_budget(
        self,
        tenant_id: str,
        additional_cost: Decimal
    ) -> Tuple[bool, Optional[str]]:
        """Check if additional cost fits within budget"""
        if tenant_id not in self.budgets:
            return True, None
            
        budget = self.budgets[tenant_id]
        new_spend = budget["current_spend"] + additional_cost
        
        if new_spend > budget["monthly_limit"]:
            return False, f"Budget exceeded: ${new_spend} > ${budget['monthly_limit']}"
            
        # Check alert thresholds
        for threshold in budget["alert_thresholds"]:
            threshold_amount = budget["monthly_limit"] * Decimal(str(threshold))
            if (new_spend >= threshold_amount and 
                threshold not in budget["alerts_sent"]):
                
                self.alerts.append({
                    "tenant_id": tenant_id,
                    "timestamp": datetime.utcnow(),
                    "threshold": threshold,
                    "current_spend": new_spend,
                    "monthly_limit": budget["monthly_limit"],
                    "message": f"Budget alert: {threshold*100}% of monthly limit reached"
                })
                budget["alerts_sent"].add(threshold)
        
        budget["current_spend"] = new_spend
        return True, None
    
    def get_budget_status(self, tenant_id: str) -> Optional[Dict[str, Any]]:
        """Get current budget status for a tenant"""
        if tenant_id not in self.budgets:
            return None
            
        budget = self.budgets[tenant_id]
        return {
            "tenant_id": tenant_id,
            "monthly_limit": float(budget["monthly_limit"]),
            "current_spend": float(budget["current_spend"]),
            "percentage_used": float(
                (budget["current_spend"] / budget["monthly_limit"]) * 100
            ),
            "period_start": budget["period_start"].isoformat(),
            "alerts_sent": list(budget["alerts_sent"])
        }