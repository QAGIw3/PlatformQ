"""
Pricing Engine Service
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import math
from pyignite import Client
import numpy as np

from ..models import (
    BandwidthClass, PathPricing, CongestionMetrics,
    CongestionLevel, NetworkPath, PricingResponse
)
from ..config import settings


logger = logging.getLogger(__name__)


class PricingEngineService:
    """Service for dynamic bandwidth pricing calculations"""
    
    def __init__(self):
        self.ignite_client = None
        self.pricing_cache = None
        self.congestion_cache = None
        
    async def initialize(self):
        """Initialize connections"""
        try:
            # Connect to Ignite
            self.ignite_client = Client()
            self.ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
            self.pricing_cache = self.ignite_client.get_or_create_cache(
                "bandwidth_pricing"
            )
            self.congestion_cache = self.ignite_client.get_or_create_cache(
                settings.IGNITE_CACHE_CONGESTION
            )
            
            logger.info("Pricing Engine Service initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Pricing Engine Service: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup connections"""
        if self.ignite_client:
            self.ignite_client.close()
    
    async def calculate_path_pricing(
        self,
        path: NetworkPath,
        congestion_metrics: Optional[CongestionMetrics] = None
    ) -> PathPricing:
        """Calculate current pricing for a network path"""
        try:
            # Get or create congestion metrics
            if not congestion_metrics:
                congestion_metrics = await self._get_congestion_metrics(path.path_id)
            
            # Calculate congestion multiplier
            congestion_multiplier = self._calculate_congestion_multiplier(
                congestion_metrics.utilization_percent if congestion_metrics else 0
            )
            
            # Calculate time of day multiplier
            tod_multiplier = self._get_time_of_day_multiplier()
            
            # Get QoS multipliers
            qos_multipliers = settings.QOS_CLASS_MULTIPLIERS
            
            # Calculate spot price
            base_price = settings.BASE_BANDWIDTH_RATE
            spot_price = base_price * congestion_multiplier * tod_multiplier
            
            # Create pricing object
            pricing = PathPricing(
                path_id=path.path_id,
                timestamp=datetime.utcnow(),
                base_price_per_mbps_hour=base_price,
                congestion_multiplier=congestion_multiplier,
                time_of_day_multiplier=tod_multiplier,
                qos_multipliers=qos_multipliers,
                burst_multiplier=settings.BURST_RATE_MULTIPLIER,
                current_utilization=congestion_metrics.utilization_percent if congestion_metrics else 0,
                spot_price_per_mbps_hour=spot_price
            )
            
            # Cache pricing
            self.pricing_cache.put(
                f"{path.path_id}_pricing",
                pricing.dict(),
                ttl=settings.CACHE_TTL_PRICING
            )
            
            return pricing
            
        except Exception as e:
            logger.error(f"Failed to calculate path pricing: {e}")
            # Return default pricing on error
            return PathPricing(
                path_id=path.path_id,
                timestamp=datetime.utcnow(),
                base_price_per_mbps_hour=settings.BASE_BANDWIDTH_RATE,
                congestion_multiplier=1.0,
                time_of_day_multiplier=1.0,
                qos_multipliers=settings.QOS_CLASS_MULTIPLIERS,
                burst_multiplier=settings.BURST_RATE_MULTIPLIER,
                current_utilization=0,
                spot_price_per_mbps_hour=settings.BASE_BANDWIDTH_RATE
            )
    
    async def get_bandwidth_price_estimate(
        self,
        path_id: str,
        bandwidth_mbps: int,
        qos_class: BandwidthClass,
        duration_hours: int
    ) -> Dict[str, float]:
        """Get price estimate for bandwidth allocation"""
        try:
            # Get cached pricing or calculate new
            cached_pricing = self.pricing_cache.get(f"{path_id}_pricing")
            if cached_pricing:
                pricing = PathPricing(**cached_pricing)
            else:
                # Would need path object in production
                pricing = await self.calculate_path_pricing(None)
            
            # Calculate base cost
            qos_multiplier = pricing.qos_multipliers.get(qos_class.value, 1.0)
            hourly_cost = (
                pricing.spot_price_per_mbps_hour * 
                bandwidth_mbps * 
                qos_multiplier
            )
            
            total_cost = hourly_cost * duration_hours
            
            # Calculate volume discount if applicable
            volume_discount = self._calculate_volume_discount(
                bandwidth_mbps,
                duration_hours
            )
            
            discounted_cost = total_cost * (1 - volume_discount)
            
            return {
                "hourly_cost": hourly_cost,
                "total_cost": total_cost,
                "volume_discount_percent": volume_discount * 100,
                "final_cost": discounted_cost,
                "spot_price_per_mbps_hour": pricing.spot_price_per_mbps_hour,
                "qos_multiplier": qos_multiplier,
                "congestion_multiplier": pricing.congestion_multiplier
            }
            
        except Exception as e:
            logger.error(f"Failed to get bandwidth price estimate: {e}")
            return {
                "error": str(e),
                "hourly_cost": 0,
                "total_cost": 0
            }
    
    async def get_burst_price_estimate(
        self,
        path_id: str,
        burst_bandwidth_mbps: int,
        duration_seconds: int,
        urgency_factor: float,
        qos_class: BandwidthClass
    ) -> float:
        """Calculate burst bandwidth price"""
        try:
            # Get base pricing
            cached_pricing = self.pricing_cache.get(f"{path_id}_pricing")
            if cached_pricing:
                pricing = PathPricing(**cached_pricing)
                base_price = pricing.spot_price_per_mbps_hour
            else:
                base_price = settings.BASE_BANDWIDTH_RATE
            
            # Convert duration to hours
            duration_hours = duration_seconds / 3600
            
            # Get multipliers
            burst_multiplier = settings.BURST_RATE_MULTIPLIER
            qos_multiplier = settings.QOS_CLASS_MULTIPLIERS.get(qos_class.value, 1.0)
            
            # Calculate burst price
            burst_price = (
                base_price * burst_bandwidth_mbps * duration_hours *
                burst_multiplier * urgency_factor * qos_multiplier
            )
            
            return burst_price
            
        except Exception as e:
            logger.error(f"Failed to calculate burst price: {e}")
            return 0
    
    async def get_circuit_price_estimate(
        self,
        paths: List[NetworkPath],
        bandwidth_mbps: int,
        redundancy: bool,
        duration_days: int
    ) -> Dict[str, float]:
        """Calculate dedicated circuit pricing"""
        try:
            # Base monthly rate
            monthly_hours = 720
            base_monthly_rate = settings.BASE_BANDWIDTH_RATE * monthly_hours
            
            # Circuit premium for dedicated resources
            circuit_premium = 10.0
            
            # Redundancy premium
            redundancy_multiplier = 1.5 if redundancy else 1.0
            
            # Path quality factor (use lowest reliability)
            quality_scores = [p.reliability_score for p in paths]
            quality_factor = min(quality_scores) if quality_scores else 0.99
            
            # Calculate monthly cost
            monthly_cost = (
                base_monthly_rate * bandwidth_mbps * circuit_premium *
                redundancy_multiplier * quality_factor * len(paths)
            )
            
            # Calculate total cost
            duration_months = duration_days / 30
            total_cost = monthly_cost * duration_months
            
            # Long-term discount
            if duration_days >= 365:
                discount = 0.20  # 20% annual discount
            elif duration_days >= 180:
                discount = 0.10  # 10% semi-annual discount
            elif duration_days >= 90:
                discount = 0.05  # 5% quarterly discount
            else:
                discount = 0
            
            discounted_cost = total_cost * (1 - discount)
            
            return {
                "monthly_cost": monthly_cost,
                "total_cost": total_cost,
                "discount_percent": discount * 100,
                "final_cost": discounted_cost,
                "cost_per_mbps_month": monthly_cost / bandwidth_mbps,
                "setup_fee": settings.CIRCUIT_SETUP_TIME * settings.BASE_BANDWIDTH_RATE
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate circuit price: {e}")
            return {
                "error": str(e),
                "monthly_cost": 0,
                "total_cost": 0
            }
    
    async def get_latency_future_price(
        self,
        guaranteed_latency_ms: float,
        current_latency_ms: float,
        duration_hours: int,
        penalty_rate: float
    ) -> Dict[str, float]:
        """Calculate latency future contract pricing"""
        try:
            # Base premium calculation
            latency_ratio = guaranteed_latency_ms / current_latency_ms
            
            # Premium increases as guaranteed latency approaches current
            if latency_ratio >= 1:
                # No premium if guaranteed is worse than current
                premium_multiplier = 0.1
            else:
                # Exponential premium as ratio decreases
                premium_multiplier = math.exp(-2 * latency_ratio) + 0.5
            
            # Base hourly premium
            base_premium = settings.BASE_BANDWIDTH_RATE * 100  # Base unit
            hourly_premium = base_premium * premium_multiplier
            
            # Total premium
            total_premium = hourly_premium * duration_hours
            
            # Risk adjustment based on penalty rate
            risk_multiplier = 1 + (penalty_rate * 2)  # Higher penalty = higher premium
            
            adjusted_premium = total_premium * risk_multiplier
            
            return {
                "hourly_premium": hourly_premium,
                "total_premium": total_premium,
                "risk_multiplier": risk_multiplier,
                "final_premium": adjusted_premium,
                "latency_ratio": latency_ratio,
                "break_even_violations": adjusted_premium / (penalty_rate * base_premium)
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate latency future price: {e}")
            return {
                "error": str(e),
                "final_premium": 0
            }
    
    def _calculate_congestion_multiplier(self, utilization_percent: float) -> float:
        """Calculate congestion-based price multiplier"""
        if utilization_percent < settings.CONGESTION_THRESHOLD * 100:
            # Linear increase up to threshold
            return 1 + (utilization_percent / 100) * 0.5
        else:
            # Exponential increase after threshold
            excess = (utilization_percent / 100) - settings.CONGESTION_THRESHOLD
            return 1.5 + math.exp(excess * 2)
    
    def _get_time_of_day_multiplier(self) -> float:
        """Get time of day pricing multiplier"""
        current_hour = datetime.utcnow().hour
        
        if 9 <= current_hour < 17:  # Business hours (UTC)
            return settings.TIME_OF_DAY_MULTIPLIERS["peak"]
        elif 17 <= current_hour < 24:  # Evening
            return settings.TIME_OF_DAY_MULTIPLIERS["standard"]
        else:  # Night/early morning
            return settings.TIME_OF_DAY_MULTIPLIERS["off_peak"]
    
    def _calculate_volume_discount(
        self,
        bandwidth_mbps: int,
        duration_hours: int
    ) -> float:
        """Calculate volume-based discount"""
        # Bandwidth volume discount
        if bandwidth_mbps >= 10000:
            bandwidth_discount = 0.15
        elif bandwidth_mbps >= 5000:
            bandwidth_discount = 0.10
        elif bandwidth_mbps >= 1000:
            bandwidth_discount = 0.05
        else:
            bandwidth_discount = 0
        
        # Duration discount
        if duration_hours >= 720:  # 30 days
            duration_discount = 0.10
        elif duration_hours >= 168:  # 7 days
            duration_discount = 0.05
        elif duration_hours >= 24:  # 1 day
            duration_discount = 0.02
        else:
            duration_discount = 0
        
        # Combined discount (not additive)
        total_discount = bandwidth_discount + duration_discount
        return min(total_discount, 0.20)  # Cap at 20%
    
    async def _get_congestion_metrics(
        self,
        path_id: str
    ) -> Optional[CongestionMetrics]:
        """Get current congestion metrics for a path"""
        try:
            # Check cache
            metrics_data = self.congestion_cache.get(f"{path_id}_metrics")
            if metrics_data:
                return CongestionMetrics(**metrics_data)
            
            # Return default metrics if not found
            return CongestionMetrics(
                path_id=path_id,
                timestamp=datetime.utcnow(),
                utilization_percent=30.0,  # Default utilization
                congestion_level=CongestionLevel.LOW,
                available_bandwidth_mbps=1000,
                queue_depth=0,
                packet_loss_rate=0.0001,
                average_latency_ms=10,
                p95_latency_ms=15,
                p99_latency_ms=20
            )
            
        except Exception as e:
            logger.error(f"Failed to get congestion metrics: {e}")
            return None
    
    async def update_congestion_pricing(
        self,
        congestion_event: Dict[str, any]
    ):
        """Update pricing based on congestion event"""
        try:
            path_id = congestion_event.get("path_id")
            if not path_id:
                return
            
            # Get current metrics
            metrics = CongestionMetrics(
                path_id=path_id,
                timestamp=datetime.utcnow(),
                utilization_percent=congestion_event.get("utilization_percent", 0),
                congestion_level=CongestionLevel(
                    congestion_event.get("congestion_level", "none")
                ),
                available_bandwidth_mbps=congestion_event.get("available_bandwidth", 0),
                queue_depth=congestion_event.get("queue_depth", 0),
                packet_loss_rate=congestion_event.get("packet_loss_rate", 0),
                average_latency_ms=congestion_event.get("average_latency", 0),
                p95_latency_ms=congestion_event.get("p95_latency", 0),
                p99_latency_ms=congestion_event.get("p99_latency", 0)
            )
            
            # Cache metrics
            self.congestion_cache.put(
                f"{path_id}_metrics",
                metrics.dict(),
                ttl=300  # 5 minutes
            )
            
            # Recalculate pricing
            # In production, would get actual path object
            await self.calculate_path_pricing(None, metrics)
            
        except Exception as e:
            logger.error(f"Failed to update congestion pricing: {e}")
    
    async def get_pricing_trends(
        self,
        path_id: str,
        hours: int = 24
    ) -> Dict[str, any]:
        """Get historical pricing trends"""
        # In production, this would query time-series data
        # For now, return simulated trends
        
        try:
            current_pricing = self.pricing_cache.get(f"{path_id}_pricing")
            if current_pricing:
                pricing = PathPricing(**current_pricing)
                current_price = pricing.spot_price_per_mbps_hour
            else:
                current_price = settings.BASE_BANDWIDTH_RATE
            
            # Simulate historical data
            timestamps = []
            prices = []
            utilizations = []
            
            for i in range(hours):
                timestamp = datetime.utcnow() - timedelta(hours=hours-i)
                timestamps.append(timestamp.isoformat())
                
                # Simulate price variations
                hour_of_day = timestamp.hour
                if 9 <= hour_of_day < 17:
                    base_multiplier = 1.5
                elif 17 <= hour_of_day < 24:
                    base_multiplier = 1.0
                else:
                    base_multiplier = 0.7
                
                # Add some randomness
                random_factor = 0.9 + (np.random.random() * 0.2)
                price = current_price * base_multiplier * random_factor
                prices.append(price)
                
                # Simulate utilization
                utilization = 30 + (base_multiplier * 30) + (np.random.random() * 20)
                utilizations.append(min(utilization, 100))
            
            return {
                "path_id": path_id,
                "period_hours": hours,
                "timestamps": timestamps,
                "prices": prices,
                "utilizations": utilizations,
                "average_price": sum(prices) / len(prices),
                "max_price": max(prices),
                "min_price": min(prices),
                "current_price": current_price
            }
            
        except Exception as e:
            logger.error(f"Failed to get pricing trends: {e}")
            return {"error": str(e)} 