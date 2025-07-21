"""
Capacity Monitor Service

Monitors resource capacity and utilization for JIT scaling.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import logging
import asyncio
from collections import defaultdict

from platformq_shared.models import ResourceType, ServiceTier
from platformq_shared.metrics import MetricsCollector

logger = logging.getLogger(__name__)


class CapacityMonitor:
    """Service for monitoring resource capacity and utilization"""
    
    def __init__(self, metrics_collector: Optional[MetricsCollector] = None):
        self.metrics = metrics_collector
        
        # Capacity tracking by resource type and region
        self._capacity = defaultdict(lambda: defaultdict(dict))
        
        # Utilization tracking
        self._utilization = defaultdict(lambda: defaultdict(float))
        
        # Historical data for trend analysis
        self._history = defaultdict(list)
        
        # Thresholds for alerting
        self._thresholds = {
            "high_utilization": 0.85,
            "low_utilization": 0.15,
            "critical_utilization": 0.95
        }
        
    async def initialize(self):
        """Initialize the capacity monitor"""
        # Start monitoring tasks
        asyncio.create_task(self._monitor_loop())
        asyncio.create_task(self._aggregate_metrics())
        
        logger.info("Capacity Monitor initialized")
        
    async def update_capacity(
        self,
        resource_type: ResourceType,
        region: str,
        provider: str,
        total: int,
        used: int,
        tier: ServiceTier = ServiceTier.STANDARD
    ):
        """
        Update capacity information for a resource
        
        Args:
            resource_type: Type of resource
            region: Region identifier
            provider: Provider identifier
            total: Total capacity
            used: Used capacity
            tier: Service tier
        """
        key = f"{tier.value}:{provider}"
        
        self._capacity[resource_type.value][region][key] = {
            "total": total,
            "used": used,
            "available": total - used,
            "utilization": used / total if total > 0 else 0,
            "updated_at": datetime.utcnow()
        }
        
        # Update aggregated utilization
        await self._update_utilization(resource_type, region)
        
        # Emit metrics if available
        if self.metrics:
            self.metrics.gauge(
                "flash_provisioning.capacity.total",
                total,
                tags={
                    "resource_type": resource_type.value,
                    "region": region,
                    "provider": provider,
                    "tier": tier.value
                }
            )
            
            self.metrics.gauge(
                "flash_provisioning.capacity.utilization",
                used / total if total > 0 else 0,
                tags={
                    "resource_type": resource_type.value,
                    "region": region,
                    "provider": provider,
                    "tier": tier.value
                }
            )
            
    async def get_utilization(
        self,
        resource_type: ResourceType,
        region: Optional[str] = None
    ) -> float:
        """
        Get current utilization for a resource type
        
        Args:
            resource_type: Type of resource
            region: Optional region filter
            
        Returns:
            Utilization percentage (0-1)
        """
        if region:
            return self._utilization[resource_type.value].get(region, 0.0)
            
        # Calculate average across all regions
        utilizations = self._utilization[resource_type.value].values()
        if not utilizations:
            return 0.0
            
        return sum(utilizations) / len(utilizations)
        
    async def get_total_capacity(
        self,
        resource_type: ResourceType,
        region: Optional[str] = None,
        tier: Optional[ServiceTier] = None
    ) -> int:
        """
        Get total capacity for a resource type
        
        Args:
            resource_type: Type of resource
            region: Optional region filter
            tier: Optional tier filter
            
        Returns:
            Total capacity units
        """
        total = 0
        
        regions = [region] if region else self._capacity[resource_type.value].keys()
        
        for r in regions:
            for key, data in self._capacity[resource_type.value][r].items():
                if tier and not key.startswith(tier.value):
                    continue
                total += data["total"]
                
        return total
        
    async def get_available_capacity(
        self,
        resource_type: ResourceType,
        region: str,
        tier: ServiceTier = ServiceTier.STANDARD
    ) -> int:
        """
        Get available capacity for immediate provisioning
        
        Args:
            resource_type: Type of resource
            region: Region identifier
            tier: Service tier
            
        Returns:
            Available capacity units
        """
        available = 0
        
        for key, data in self._capacity[resource_type.value][region].items():
            if key.startswith(tier.value):
                available += data["available"]
                
        return available
        
    async def predict_capacity_needs(
        self,
        resource_type: ResourceType,
        duration_hours: int = 1
    ) -> Dict[str, Any]:
        """
        Predict capacity needs based on historical trends
        
        Args:
            resource_type: Type of resource
            duration_hours: Prediction horizon in hours
            
        Returns:
            Predicted capacity requirements
        """
        history = self._history[resource_type.value]
        
        if len(history) < 10:
            # Not enough data for prediction
            return {
                "predicted_utilization": await self.get_utilization(resource_type),
                "confidence": "low",
                "recommendation": "maintain"
            }
            
        # Simple trend analysis
        recent = history[-10:]
        utilizations = [h["utilization"] for h in recent]
        
        # Calculate trend
        trend = (utilizations[-1] - utilizations[0]) / len(utilizations)
        
        # Predict future utilization
        predicted = utilizations[-1] + (trend * duration_hours * 6)  # 6 samples per hour
        predicted = max(0, min(1, predicted))  # Clamp to 0-1
        
        # Generate recommendation
        if predicted > self._thresholds["high_utilization"]:
            recommendation = "scale_up"
        elif predicted < self._thresholds["low_utilization"]:
            recommendation = "scale_down"
        else:
            recommendation = "maintain"
            
        return {
            "predicted_utilization": predicted,
            "current_utilization": utilizations[-1],
            "trend": "increasing" if trend > 0 else "decreasing",
            "confidence": "medium" if len(history) > 50 else "low",
            "recommendation": recommendation
        }
        
    async def get_capacity_report(
        self,
        resource_type: Optional[ResourceType] = None
    ) -> Dict[str, Any]:
        """
        Get comprehensive capacity report
        
        Args:
            resource_type: Optional filter by resource type
            
        Returns:
            Capacity report with utilization, trends, and recommendations
        """
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "resources": {}
        }
        
        resource_types = [resource_type] if resource_type else list(ResourceType)
        
        for rt in resource_types:
            rt_data = {
                "total_capacity": await self.get_total_capacity(rt),
                "utilization": await self.get_utilization(rt),
                "regions": {}
            }
            
            # Add per-region data
            for region in self._capacity[rt.value].keys():
                region_data = {
                    "total": 0,
                    "used": 0,
                    "available": 0,
                    "providers": 0
                }
                
                for data in self._capacity[rt.value][region].values():
                    region_data["total"] += data["total"]
                    region_data["used"] += data["used"]
                    region_data["available"] += data["available"]
                    region_data["providers"] += 1
                    
                region_data["utilization"] = (
                    region_data["used"] / region_data["total"]
                    if region_data["total"] > 0 else 0
                )
                
                rt_data["regions"][region] = region_data
                
            # Add prediction
            rt_data["prediction"] = await self.predict_capacity_needs(rt)
            
            report["resources"][rt.value] = rt_data
            
        return report
        
    async def _update_utilization(
        self,
        resource_type: ResourceType,
        region: str
    ):
        """Update aggregated utilization for a resource type and region"""
        total_capacity = 0
        total_used = 0
        
        for data in self._capacity[resource_type.value][region].values():
            total_capacity += data["total"]
            total_used += data["used"]
            
        utilization = total_used / total_capacity if total_capacity > 0 else 0
        self._utilization[resource_type.value][region] = utilization
        
        # Add to history
        self._history[resource_type.value].append({
            "timestamp": datetime.utcnow(),
            "region": region,
            "utilization": utilization,
            "total_capacity": total_capacity,
            "total_used": total_used
        })
        
        # Keep only last 24 hours of history (assuming 10 min intervals = 144 samples)
        if len(self._history[resource_type.value]) > 144:
            self._history[resource_type.value] = self._history[resource_type.value][-144:]
            
    async def _monitor_loop(self):
        """Main monitoring loop"""
        while True:
            try:
                # Check for critical conditions
                for resource_type in ResourceType:
                    utilization = await self.get_utilization(resource_type)
                    
                    if utilization > self._thresholds["critical_utilization"]:
                        logger.warning(
                            f"Critical utilization for {resource_type.value}: {utilization:.2%}"
                        )
                        
                        if self.metrics:
                            self.metrics.increment(
                                "flash_provisioning.alerts.critical_utilization",
                                tags={"resource_type": resource_type.value}
                            )
                            
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in monitor loop: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error
                
    async def _aggregate_metrics(self):
        """Aggregate metrics periodically"""
        while True:
            try:
                # Generate and emit aggregate metrics
                for resource_type in ResourceType:
                    total = await self.get_total_capacity(resource_type)
                    utilization = await self.get_utilization(resource_type)
                    
                    if self.metrics:
                        self.metrics.gauge(
                            "flash_provisioning.aggregate.total_capacity",
                            total,
                            tags={"resource_type": resource_type.value}
                        )
                        
                        self.metrics.gauge(
                            "flash_provisioning.aggregate.utilization",
                            utilization,
                            tags={"resource_type": resource_type.value}
                        )
                        
                await asyncio.sleep(600)  # Every 10 minutes
                
            except Exception as e:
                logger.error(f"Error aggregating metrics: {e}")
                await asyncio.sleep(600) 