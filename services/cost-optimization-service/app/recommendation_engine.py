"""Recommendation Engine for cost optimization"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any
import asyncio
from statistics import mean, stdev

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler

from platformq_cost_common import (
    CostRecommendation,
    CostRecommendationType,
    RecommendationPriority,
    ResourceCost,
    CostAnalysis
)
from platformq_resource_common import (
    ResourceType,
    ResourceMetrics,
    ResourceUsagePattern
)

from .config import settings
from .repository import CostRepository

logger = logging.getLogger(__name__)


class RecommendationEngine:
    """Generates cost optimization recommendations"""
    
    def __init__(self, repository: CostRepository):
        self.repository = repository
        self.scaler = StandardScaler()
        
    async def generate_recommendations(
        self,
        tenant_id: str,
        cost_analysis: CostAnalysis,
        resource_metrics: List[ResourceMetrics]
    ) -> List[CostRecommendation]:
        """Generate cost optimization recommendations"""
        logger.info(f"Generating recommendations for tenant {tenant_id}")
        
        recommendations = []
        
        # Rightsizing recommendations
        rightsizing_recs = await self._generate_rightsizing_recommendations(
            tenant_id, cost_analysis, resource_metrics
        )
        recommendations.extend(rightsizing_recs)
        
        # Reserved instance recommendations
        ri_recs = await self._generate_reserved_instance_recommendations(
            tenant_id, cost_analysis
        )
        recommendations.extend(ri_recs)
        
        # Unused resource recommendations
        unused_recs = await self._identify_unused_resources(
            tenant_id, cost_analysis, resource_metrics
        )
        recommendations.extend(unused_recs)
        
        # Scheduling recommendations
        scheduling_recs = await self._generate_scheduling_recommendations(
            tenant_id, resource_metrics
        )
        recommendations.extend(scheduling_recs)
        
        # Storage optimization recommendations
        storage_recs = await self._generate_storage_recommendations(
            tenant_id, cost_analysis
        )
        recommendations.extend(storage_recs)
        
        # Sort by priority and potential savings
        recommendations.sort(
            key=lambda r: (r.priority.value, -r.estimated_monthly_savings),
            reverse=True
        )
        
        # Save recommendations
        for rec in recommendations:
            await self.repository.save_recommendation(rec)
            
        return recommendations
        
    async def _generate_rightsizing_recommendations(
        self,
        tenant_id: str,
        cost_analysis: CostAnalysis,
        resource_metrics: List[ResourceMetrics]
    ) -> List[CostRecommendation]:
        """Generate rightsizing recommendations based on resource utilization"""
        recommendations = []
        
        for metrics in resource_metrics:
            if metrics.resource_type != ResourceType.COMPUTE:
                continue
                
            # Check CPU utilization
            avg_cpu = mean(metrics.cpu_utilization) if metrics.cpu_utilization else 0
            max_cpu = max(metrics.cpu_utilization) if metrics.cpu_utilization else 0
            
            # Check memory utilization
            avg_memory = mean(metrics.memory_utilization) if metrics.memory_utilization else 0
            max_memory = max(metrics.memory_utilization) if metrics.memory_utilization else 0
            
            # Find the associated cost
            resource_cost = next(
                (rc for rc in cost_analysis.resource_costs if rc.resource_id == metrics.resource_id),
                None
            )
            
            if not resource_cost:
                continue
                
            # Downsize recommendation
            if (avg_cpu < settings.cpu_utilization_low_threshold and 
                max_cpu < settings.cpu_utilization_low_threshold * 1.5 and
                avg_memory < settings.memory_utilization_low_threshold):
                
                # Calculate potential savings (estimate 30-50% cost reduction)
                savings_percentage = 0.4
                estimated_savings = resource_cost.amount * savings_percentage * 30  # Monthly
                
                if estimated_savings >= settings.cost_optimization_min_savings_percent * resource_cost.amount * 30 / 100:
                    recommendations.append(CostRecommendation(
                        recommendation_id=f"rightsize-down-{metrics.resource_id}",
                        tenant_id=tenant_id,
                        resource_id=metrics.resource_id,
                        recommendation_type=CostRecommendationType.RIGHTSIZING,
                        title=f"Downsize underutilized {metrics.resource_type}",
                        description=(
                            f"Resource {metrics.resource_id} has low utilization "
                            f"(CPU: {avg_cpu:.1f}%, Memory: {avg_memory:.1f}%). "
                            f"Consider downsizing to a smaller instance type."
                        ),
                        estimated_monthly_savings=estimated_savings,
                        implementation_effort="medium",
                        risk_level="low",
                        priority=RecommendationPriority.HIGH,
                        confidence_score=0.85,
                        action_items=[
                            "Review historical usage patterns",
                            "Identify appropriate smaller instance type",
                            "Test performance with smaller instance",
                            "Schedule downsize during maintenance window"
                        ],
                        metadata={
                            "current_cpu_avg": avg_cpu,
                            "current_memory_avg": avg_memory,
                            "suggested_reduction": "40%"
                        },
                        generated_at=datetime.now(timezone.utc)
                    ))
                    
            # Upsize recommendation (for performance)
            elif (avg_cpu > settings.cpu_utilization_high_threshold or
                  max_cpu > 95 or
                  avg_memory > settings.memory_utilization_high_threshold):
                
                # This might increase costs but improve performance
                cost_increase = resource_cost.amount * 0.5 * 30  # Monthly
                
                recommendations.append(CostRecommendation(
                    recommendation_id=f"rightsize-up-{metrics.resource_id}",
                    tenant_id=tenant_id,
                    resource_id=metrics.resource_id,
                    recommendation_type=CostRecommendationType.RIGHTSIZING,
                    title=f"Upsize overutilized {metrics.resource_type}",
                    description=(
                        f"Resource {metrics.resource_id} has high utilization "
                        f"(CPU: {avg_cpu:.1f}%, Memory: {avg_memory:.1f}%). "
                        f"Consider upsizing for better performance."
                    ),
                    estimated_monthly_savings=-cost_increase,  # Negative because it's a cost increase
                    implementation_effort="medium",
                    risk_level="medium",
                    priority=RecommendationPriority.MEDIUM,
                    confidence_score=0.8,
                    action_items=[
                        "Monitor performance metrics",
                        "Identify performance bottlenecks",
                        "Select appropriate larger instance type",
                        "Plan capacity upgrade"
                    ],
                    metadata={
                        "current_cpu_avg": avg_cpu,
                        "current_memory_avg": avg_memory,
                        "performance_risk": "high"
                    },
                    generated_at=datetime.now(timezone.utc)
                ))
                
        return recommendations
        
    async def _generate_reserved_instance_recommendations(
        self,
        tenant_id: str,
        cost_analysis: CostAnalysis
    ) -> List[CostRecommendation]:
        """Generate reserved instance recommendations based on usage patterns"""
        recommendations = []
        
        # Get usage history
        usage_history = await self.repository.get_resource_usage_history(
            tenant_id=tenant_id,
            days=settings.ri_recommendation_min_usage_days
        )
        
        # Group by resource type and analyze consistency
        resource_usage = {}
        for usage in usage_history:
            if usage.resource_type not in resource_usage:
                resource_usage[usage.resource_type] = []
            resource_usage[usage.resource_type].append(usage)
            
        for resource_type, usages in resource_usage.items():
            if len(usages) < settings.ri_recommendation_min_usage_days:
                continue
                
            # Check if resource has been consistently used
            daily_usage = {}
            for usage in usages:
                date = usage.timestamp.date()
                if date not in daily_usage:
                    daily_usage[date] = 0
                daily_usage[date] += usage.usage_hours
                
            if len(daily_usage) >= settings.ri_recommendation_min_usage_days:
                # Calculate average daily usage
                avg_daily_hours = mean(daily_usage.values())
                
                # If consistently used (e.g., >20 hours per day)
                if avg_daily_hours >= 20:
                    # Find on-demand costs for this resource type
                    on_demand_costs = [
                        rc for rc in cost_analysis.resource_costs
                        if rc.resource_type == resource_type and "on-demand" in rc.tags.get("pricing_model", "on-demand")
                    ]
                    
                    if on_demand_costs:
                        monthly_on_demand_cost = sum(rc.amount for rc in on_demand_costs) * 30
                        
                        # Estimate RI savings (typically 30-70% depending on term)
                        ri_savings_percentage = 0.4  # 40% savings for 1-year term
                        estimated_savings = monthly_on_demand_cost * ri_savings_percentage
                        
                        if estimated_savings >= settings.ri_recommendation_min_savings:
                            recommendations.append(CostRecommendation(
                                recommendation_id=f"ri-{resource_type}-{tenant_id}",
                                tenant_id=tenant_id,
                                resource_id=f"all-{resource_type}",
                                recommendation_type=CostRecommendationType.RESERVED_INSTANCES,
                                title=f"Purchase Reserved Instances for {resource_type}",
                                description=(
                                    f"You have been consistently using {resource_type} resources "
                                    f"for {len(daily_usage)} days with average {avg_daily_hours:.1f} hours/day. "
                                    f"Consider purchasing reserved instances for cost savings."
                                ),
                                estimated_monthly_savings=estimated_savings,
                                implementation_effort="low",
                                risk_level="low",
                                priority=RecommendationPriority.HIGH,
                                confidence_score=0.9,
                                action_items=[
                                    "Review usage patterns for stability",
                                    "Calculate break-even point",
                                    "Choose appropriate RI term (1 or 3 years)",
                                    "Purchase reserved instances"
                                ],
                                metadata={
                                    "avg_daily_hours": avg_daily_hours,
                                    "days_analyzed": len(daily_usage),
                                    "current_monthly_cost": monthly_on_demand_cost,
                                    "ri_savings_percentage": ri_savings_percentage * 100
                                },
                                generated_at=datetime.now(timezone.utc)
                            ))
                            
        return recommendations
        
    async def _identify_unused_resources(
        self,
        tenant_id: str,
        cost_analysis: CostAnalysis,
        resource_metrics: List[ResourceMetrics]
    ) -> List[CostRecommendation]:
        """Identify and recommend removal of unused resources"""
        recommendations = []
        
        # Check for resources with costs but no recent usage
        for resource_cost in cost_analysis.resource_costs:
            # Find metrics for this resource
            metrics = next(
                (m for m in resource_metrics if m.resource_id == resource_cost.resource_id),
                None
            )
            
            # If no metrics or all metrics are zero
            if not metrics or (
                all(cpu == 0 for cpu in (metrics.cpu_utilization or [])) and
                all(mem == 0 for mem in (metrics.memory_utilization or [])) and
                all(net == 0 for net in (metrics.network_io or []))
            ):
                # Check how long it's been unused
                last_used = await self.repository.get_last_resource_usage(
                    tenant_id=tenant_id,
                    resource_id=resource_cost.resource_id
                )
                
                days_unused = 0
                if last_used:
                    days_unused = (datetime.now(timezone.utc) - last_used).days
                    
                if days_unused > 7 or not last_used:
                    recommendations.append(CostRecommendation(
                        recommendation_id=f"remove-unused-{resource_cost.resource_id}",
                        tenant_id=tenant_id,
                        resource_id=resource_cost.resource_id,
                        recommendation_type=CostRecommendationType.UNUSED_RESOURCES,
                        title=f"Remove unused {resource_cost.resource_type}",
                        description=(
                            f"Resource {resource_cost.resource_id} has been unused for "
                            f"{days_unused if last_used else 'unknown'} days but is still incurring costs. "
                            f"Consider removing or stopping this resource."
                        ),
                        estimated_monthly_savings=resource_cost.amount * 30,
                        implementation_effort="low",
                        risk_level="low",
                        priority=RecommendationPriority.HIGH,
                        confidence_score=0.95,
                        action_items=[
                            "Verify resource is truly unused",
                            "Check for any dependencies",
                            "Create backup if needed",
                            "Remove or stop the resource"
                        ],
                        metadata={
                            "days_unused": days_unused,
                            "resource_type": resource_cost.resource_type,
                            "daily_cost": resource_cost.amount
                        },
                        generated_at=datetime.now(timezone.utc)
                    ))
                    
        return recommendations
        
    async def _generate_scheduling_recommendations(
        self,
        tenant_id: str,
        resource_metrics: List[ResourceMetrics]
    ) -> List[CostRecommendation]:
        """Generate scheduling recommendations for resources with predictable usage patterns"""
        recommendations = []
        
        for metrics in resource_metrics:
            if not metrics.usage_pattern:
                continue
                
            # Analyze usage patterns
            usage_by_hour = await self.repository.get_hourly_usage_pattern(
                tenant_id=tenant_id,
                resource_id=metrics.resource_id,
                days=7
            )
            
            if not usage_by_hour:
                continue
                
            # Check if resource has predictable off-hours
            off_hours = []
            for hour, usage in usage_by_hour.items():
                if usage < 10:  # Less than 10% usage
                    off_hours.append(hour)
                    
            if len(off_hours) >= 8:  # At least 8 hours of low usage
                # Calculate potential savings
                hours_per_month = len(off_hours) * 30
                resource_cost = await self.repository.get_resource_cost(
                    tenant_id=tenant_id,
                    resource_id=metrics.resource_id
                )
                
                if resource_cost:
                    hourly_cost = resource_cost.amount / 24
                    estimated_savings = hourly_cost * hours_per_month
                    
                    recommendations.append(CostRecommendation(
                        recommendation_id=f"schedule-{metrics.resource_id}",
                        tenant_id=tenant_id,
                        resource_id=metrics.resource_id,
                        recommendation_type=CostRecommendationType.SCHEDULING,
                        title=f"Implement scheduling for {metrics.resource_type}",
                        description=(
                            f"Resource {metrics.resource_id} has predictable low-usage periods. "
                            f"Implement auto-scheduling to stop/start during off-hours "
                            f"({len(off_hours)} hours daily)."
                        ),
                        estimated_monthly_savings=estimated_savings,
                        implementation_effort="medium",
                        risk_level="low",
                        priority=RecommendationPriority.MEDIUM,
                        confidence_score=0.8,
                        action_items=[
                            "Set up auto-scaling schedules",
                            "Configure start/stop automation",
                            "Test scheduling logic",
                            "Monitor for any issues"
                        ],
                        metadata={
                            "off_hours": off_hours,
                            "potential_hours_saved": hours_per_month,
                            "usage_pattern": metrics.usage_pattern
                        },
                        generated_at=datetime.now(timezone.utc)
                    ))
                    
        return recommendations
        
    async def _generate_storage_recommendations(
        self,
        tenant_id: str,
        cost_analysis: CostAnalysis
    ) -> List[CostRecommendation]:
        """Generate storage optimization recommendations"""
        recommendations = []
        
        # Find storage resources
        storage_costs = [
            rc for rc in cost_analysis.resource_costs
            if rc.resource_type == ResourceType.STORAGE
        ]
        
        for storage_cost in storage_costs:
            # Get storage metrics
            storage_metrics = await self.repository.get_storage_metrics(
                tenant_id=tenant_id,
                resource_id=storage_cost.resource_id
            )
            
            if not storage_metrics:
                continue
                
            # Check for lifecycle opportunities
            if storage_metrics.get("last_accessed_days", 0) > 30:
                # Recommend archival storage
                current_monthly_cost = storage_cost.amount * 30
                archive_cost = current_monthly_cost * 0.1  # Archive storage is ~90% cheaper
                estimated_savings = current_monthly_cost - archive_cost
                
                recommendations.append(CostRecommendation(
                    recommendation_id=f"archive-{storage_cost.resource_id}",
                    tenant_id=tenant_id,
                    resource_id=storage_cost.resource_id,
                    recommendation_type=CostRecommendationType.STORAGE_OPTIMIZATION,
                    title="Move infrequently accessed data to archive storage",
                    description=(
                        f"Storage {storage_cost.resource_id} hasn't been accessed in "
                        f"{storage_metrics['last_accessed_days']} days. "
                        f"Consider moving to cheaper archive storage."
                    ),
                    estimated_monthly_savings=estimated_savings,
                    implementation_effort="medium",
                    risk_level="low",
                    priority=RecommendationPriority.MEDIUM,
                    confidence_score=0.85,
                    action_items=[
                        "Identify data access patterns",
                        "Set up lifecycle policies",
                        "Configure archival rules",
                        "Test data retrieval process"
                    ],
                    metadata={
                        "last_accessed_days": storage_metrics["last_accessed_days"],
                        "storage_size_gb": storage_metrics.get("size_gb", 0),
                        "current_storage_class": storage_metrics.get("storage_class", "standard")
                    },
                    generated_at=datetime.now(timezone.utc)
                ))
                
        return recommendations 