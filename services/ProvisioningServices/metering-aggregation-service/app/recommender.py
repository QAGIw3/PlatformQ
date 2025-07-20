"""Cost Recommender

Generates cost optimization recommendations based on usage patterns.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
from decimal import Decimal

from platformq_cost_common.models import (
    CostRecommendation,
    CostRecommendationType
)

from .config import Settings

logger = logging.getLogger(__name__)


class CostRecommender:
    """Generates cost optimization recommendations"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        
    async def generate_recommendations(
        self,
        tenant_id: str,
        usage_data: Dict[str, Any],
        min_savings_percent: float = 5.0
    ) -> List[CostRecommendation]:
        """Generate cost optimization recommendations"""
        
        recommendations = []
        
        # Analyze compute usage patterns
        if "compute_hours" in usage_data:
            compute_recs = await self._analyze_compute_usage(
                tenant_id, usage_data["compute_hours"], min_savings_percent
            )
            recommendations.extend(compute_recs)
        
        # Analyze storage usage
        if "storage_gb_hours" in usage_data:
            storage_recs = await self._analyze_storage_usage(
                tenant_id, usage_data["storage_gb_hours"], min_savings_percent
            )
            recommendations.extend(storage_recs)
        
        # Analyze idle resources
        idle_recs = await self._analyze_idle_resources(
            tenant_id, usage_data, min_savings_percent
        )
        recommendations.extend(idle_recs)
        
        # Sort by potential savings
        recommendations.sort(key=lambda x: x.estimated_monthly_savings, reverse=True)
        
        return recommendations
    
    async def _analyze_compute_usage(
        self,
        tenant_id: str,
        compute_pattern: Dict[str, Any],
        min_savings_percent: float
    ) -> List[CostRecommendation]:
        """Analyze compute usage and generate recommendations"""
        
        recommendations = []
        
        # Check for downsizing opportunities
        if compute_pattern.get("average", 0) < compute_pattern.get("maximum", 0) * 0.5:
            # Average usage is less than 50% of max, suggest downsizing
            current_cost = compute_pattern.get("average", 0) * 24 * 30 * 0.10  # Assuming $0.10/hour
            new_cost = current_cost * 0.5  # Smaller instance ~50% cost
            savings = current_cost - new_cost
            
            if (savings / current_cost) * 100 >= min_savings_percent:
                rec = CostRecommendation(
                    recommendation_id=f"rec-{tenant_id}-downsize-{datetime.utcnow().timestamp()}",
                    tenant_id=tenant_id,
                    type=CostRecommendationType.DOWNSIZE,
                    title="Downsize underutilized compute instances",
                    description=(
                        f"Your average compute usage ({compute_pattern['average']:.1f} hours) is less than "
                        f"50% of your maximum usage ({compute_pattern['maximum']:.1f} hours). "
                        "Consider downsizing to smaller instance types."
                    ),
                    resource_type="compute",
                    affected_resources=["compute_instances"],
                    estimated_monthly_savings=Decimal(str(savings)),
                    implementation_effort="medium",
                    risk_level="low",
                    priority="high" if savings > 100 else "medium",
                    metadata={
                        "current_average": compute_pattern["average"],
                        "current_maximum": compute_pattern["maximum"],
                        "utilization_percent": (compute_pattern["average"] / compute_pattern["maximum"]) * 100
                    }
                )
                recommendations.append(rec)
        
        # Check for spot instance opportunities
        if compute_pattern.get("variance", 0) < 10:  # Low variance, predictable workload
            current_cost = compute_pattern.get("average", 0) * 24 * 30 * 0.10
            spot_cost = current_cost * 0.3  # Spot instances ~70% cheaper
            savings = current_cost - spot_cost
            
            if (savings / current_cost) * 100 >= min_savings_percent:
                rec = CostRecommendation(
                    recommendation_id=f"rec-{tenant_id}-spot-{datetime.utcnow().timestamp()}",
                    tenant_id=tenant_id,
                    type=CostRecommendationType.SPOT_INSTANCES,
                    title="Use spot instances for predictable workloads",
                    description=(
                        "Your compute usage shows low variance, indicating predictable workloads. "
                        "Consider using spot instances for up to 70% cost savings."
                    ),
                    resource_type="compute",
                    affected_resources=["compute_instances"],
                    estimated_monthly_savings=Decimal(str(savings)),
                    implementation_effort="medium",
                    risk_level="medium",
                    priority="high",
                    metadata={
                        "variance": compute_pattern["variance"],
                        "current_pricing_model": "on_demand",
                        "recommended_pricing_model": "spot"
                    }
                )
                recommendations.append(rec)
        
        # Check for reserved instance opportunities
        if compute_pattern.get("average", 0) > 0.7 * compute_pattern.get("maximum", 0):
            # High sustained usage, good candidate for reserved instances
            current_cost = compute_pattern.get("average", 0) * 24 * 30 * 0.10
            reserved_cost = current_cost * 0.6  # Reserved instances ~40% cheaper
            savings = current_cost - reserved_cost
            
            if (savings / current_cost) * 100 >= min_savings_percent:
                rec = CostRecommendation(
                    recommendation_id=f"rec-{tenant_id}-reserved-{datetime.utcnow().timestamp()}",
                    tenant_id=tenant_id,
                    type=CostRecommendationType.RESERVED_INSTANCES,
                    title="Purchase reserved instances for sustained workloads",
                    description=(
                        "Your compute usage shows high sustained utilization. "
                        "Consider reserved instances for up to 40% cost savings."
                    ),
                    resource_type="compute",
                    affected_resources=["compute_instances"],
                    estimated_monthly_savings=Decimal(str(savings)),
                    implementation_effort="low",
                    risk_level="low",
                    priority="high",
                    metadata={
                        "average_utilization": compute_pattern["average"],
                        "utilization_ratio": compute_pattern["average"] / compute_pattern["maximum"]
                    }
                )
                recommendations.append(rec)
        
        return recommendations
    
    async def _analyze_storage_usage(
        self,
        tenant_id: str,
        storage_pattern: Dict[str, Any],
        min_savings_percent: float
    ) -> List[CostRecommendation]:
        """Analyze storage usage and generate recommendations"""
        
        recommendations = []
        
        # Check for unused storage
        if storage_pattern.get("minimum", 0) > 0 and storage_pattern.get("variance", 0) < 0.1:
            # Storage is allocated but not changing, might be unused
            current_cost = storage_pattern.get("average", 0) * 0.10  # $0.10 per GB-month
            potential_reduction = storage_pattern.get("minimum", 0) * 0.5  # Could reduce by 50%
            savings = potential_reduction * 0.10
            
            if (savings / current_cost) * 100 >= min_savings_percent:
                rec = CostRecommendation(
                    recommendation_id=f"rec-{tenant_id}-storage-{datetime.utcnow().timestamp()}",
                    tenant_id=tenant_id,
                    type=CostRecommendationType.CONSOLIDATION,
                    title="Remove or consolidate unused storage volumes",
                    description=(
                        "You have storage volumes with very low variance in usage, "
                        "indicating they might be unused or over-provisioned."
                    ),
                    resource_type="storage",
                    affected_resources=["storage_volumes"],
                    estimated_monthly_savings=Decimal(str(savings)),
                    implementation_effort="low",
                    risk_level="low",
                    priority="medium",
                    metadata={
                        "current_usage_gb": storage_pattern["average"],
                        "variance": storage_pattern["variance"]
                    }
                )
                recommendations.append(rec)
        
        return recommendations
    
    async def _analyze_idle_resources(
        self,
        tenant_id: str,
        usage_data: Dict[str, Any],
        min_savings_percent: float
    ) -> List[CostRecommendation]:
        """Analyze idle resources across all services"""
        
        recommendations = []
        
        # Check compute idle percentage
        compute_pattern = usage_data.get("compute_hours", {})
        if compute_pattern.get("idle_percentage", 0) > 30:
            # More than 30% idle time
            current_cost = compute_pattern.get("average", 0) * 24 * 30 * 0.10
            idle_cost = current_cost * (compute_pattern["idle_percentage"] / 100)
            
            if (idle_cost / current_cost) * 100 >= min_savings_percent:
                rec = CostRecommendation(
                    recommendation_id=f"rec-{tenant_id}-idle-{datetime.utcnow().timestamp()}",
                    tenant_id=tenant_id,
                    type=CostRecommendationType.IDLE_RESOURCE,
                    title="Terminate or schedule idle compute resources",
                    description=(
                        f"Your compute resources are idle {compute_pattern['idle_percentage']:.1f}% of the time. "
                        "Consider implementing auto-scaling or scheduled shutdowns."
                    ),
                    resource_type="compute",
                    affected_resources=["compute_instances"],
                    estimated_monthly_savings=Decimal(str(idle_cost)),
                    implementation_effort="medium",
                    risk_level="low",
                    priority="high" if compute_pattern["idle_percentage"] > 50 else "medium",
                    metadata={
                        "idle_percentage": compute_pattern["idle_percentage"],
                        "idle_hours_per_day": (compute_pattern["idle_percentage"] / 100) * 24
                    }
                )
                recommendations.append(rec)
        
        # Check for schedule-based optimization opportunities
        if self._has_time_pattern(usage_data):
            # Usage follows a time pattern, can implement scheduling
            estimated_savings = self._calculate_schedule_savings(usage_data)
            
            if estimated_savings > 0:
                rec = CostRecommendation(
                    recommendation_id=f"rec-{tenant_id}-schedule-{datetime.utcnow().timestamp()}",
                    tenant_id=tenant_id,
                    type=CostRecommendationType.SCHEDULE_BASED,
                    title="Implement schedule-based resource management",
                    description=(
                        "Your usage follows predictable time patterns. "
                        "Implement automated scheduling to turn off resources during off-hours."
                    ),
                    resource_type="all",
                    affected_resources=["compute_instances", "development_environments"],
                    estimated_monthly_savings=Decimal(str(estimated_savings)),
                    implementation_effort="medium",
                    risk_level="low",
                    priority="medium",
                    metadata={
                        "recommended_schedule": "Weekdays 8am-6pm",
                        "potential_off_hours": 14 * 30  # 14 hours/day * 30 days
                    }
                )
                recommendations.append(rec)
        
        return recommendations
    
    def _has_time_pattern(self, usage_data: Dict[str, Any]) -> bool:
        """Check if usage follows a time-based pattern"""
        # Simplified check - in production, use time series analysis
        compute_pattern = usage_data.get("compute_hours", {})
        
        # If variance is high but idle percentage is also high,
        # likely has on/off pattern
        return (
            compute_pattern.get("variance", 0) > 20 and
            compute_pattern.get("idle_percentage", 0) > 20
        )
    
    def _calculate_schedule_savings(self, usage_data: Dict[str, Any]) -> float:
        """Calculate potential savings from scheduling"""
        # Simplified calculation
        # Assume 14 hours/day off-time, 30% of resources can be scheduled
        
        compute_pattern = usage_data.get("compute_hours", {})
        hourly_cost = compute_pattern.get("average", 0) * 0.10  # $0.10/hour
        
        # 14 hours/day * 30 days * 30% of resources
        schedulable_hours = 14 * 30 * 0.3
        
        return hourly_cost * schedulable_hours 