"""
Compute Provisioning for MLOps

Integrates with derivatives engine for automatic compute procurement,
cost optimization, and hedging strategies.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum

import httpx
from platformq_shared.cache import CacheManager
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class ComputeRequirementType(Enum):
    TRAINING = "training"
    INFERENCE = "inference"
    HYPERPARAMETER_TUNING = "hyperparameter_tuning"
    DISTRIBUTED_TRAINING = "distributed_training"
    FEDERATED_LEARNING = "federated_learning"


class ResourceProcurementStrategy(Enum):
    SPOT = "spot"  # Use spot market
    FUTURES = "futures"  # Use futures contracts
    OPTIONS = "options"  # Use options for hedging
    MIXED = "mixed"  # Optimize across all markets


@dataclass
class ComputeRequirement:
    """ML training compute requirements"""
    requirement_type: ComputeRequirementType
    gpu_hours: float
    gpu_type: str = "NVIDIA_A100"
    cpu_cores: int = 16
    memory_gb: int = 64
    storage_gb: int = 100
    duration_hours: float = 24
    deadline: Optional[datetime] = None
    budget_limit: Optional[Decimal] = None
    priority: str = "normal"  # low, normal, high, critical
    
    def to_resource_spec(self) -> Dict[str, Any]:
        """Convert to resource specification"""
        return {
            "resourceType": "GPU",
            "quantity": self.gpu_hours,
            "specs": {
                "gpu_type": self.gpu_type,
                "cpu_cores": self.cpu_cores,
                "memory_gb": self.memory_gb,
                "storage_gb": self.storage_gb
            },
            "duration": self.duration_hours * 3600,  # Convert to seconds
            "priority": self.priority
        }


@dataclass
class ProcurementResult:
    """Result of compute procurement"""
    success: bool
    strategy_used: ResourceProcurementStrategy
    total_cost: Decimal
    resources_allocated: Dict[str, Any]
    futures_contracts: List[str] = field(default_factory=list)
    options_contracts: List[str] = field(default_factory=list)
    spot_orders: List[str] = field(default_factory=list)
    estimated_savings: Decimal = Decimal("0")
    hedging_coverage: float = 0.0  # Percentage hedged
    access_details: Optional[Dict[str, Any]] = None


class MLOpsComputeProvisioner:
    """Handles compute provisioning for ML workloads"""
    
    def __init__(
        self,
        derivatives_engine_url: str = "http://derivatives-engine-service:8000",
        provisioning_url: str = "http://provisioning-service:8000",
        event_publisher: Optional[EventPublisher] = None,
        cache_manager: Optional[CacheManager] = None
    ):
        self.derivatives_url = derivatives_engine_url
        self.provisioning_url = provisioning_url
        self.event_publisher = event_publisher
        self.cache = cache_manager or CacheManager()
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
    async def provision_training_compute(
        self,
        model_name: str,
        requirement: ComputeRequirement,
        tenant_id: str,
        user_id: str
    ) -> ProcurementResult:
        """Provision compute for model training"""
        try:
            # Determine optimal procurement strategy
            strategy = await self._determine_strategy(requirement)
            
            if strategy == ResourceProcurementStrategy.SPOT:
                return await self._procure_spot(model_name, requirement, tenant_id, user_id)
            elif strategy == ResourceProcurementStrategy.FUTURES:
                return await self._procure_futures(model_name, requirement, tenant_id, user_id)
            elif strategy == ResourceProcurementStrategy.OPTIONS:
                return await self._procure_with_options(model_name, requirement, tenant_id, user_id)
            else:  # MIXED
                return await self._procure_mixed(model_name, requirement, tenant_id, user_id)
                
        except Exception as e:
            logger.error(f"Failed to provision compute: {e}")
            return ProcurementResult(
                success=False,
                strategy_used=strategy,
                total_cost=Decimal("0"),
                resources_allocated={}
            )
            
    async def _determine_strategy(self, requirement: ComputeRequirement) -> ResourceProcurementStrategy:
        """Determine optimal procurement strategy based on requirements"""
        # Get current market conditions
        market_data = await self._get_market_data()
        
        # Short duration + urgent = spot market
        if requirement.duration_hours < 4 and requirement.priority in ["high", "critical"]:
            return ResourceProcurementStrategy.SPOT
            
        # Long duration + predictable = futures
        if requirement.duration_hours > 24 and requirement.deadline:
            return ResourceProcurementStrategy.FUTURES
            
        # Large budget + risk aversion = options hedging
        if requirement.budget_limit and requirement.budget_limit > Decimal("10000"):
            return ResourceProcurementStrategy.OPTIONS
            
        # Default to mixed strategy for optimization
        return ResourceProcurementStrategy.MIXED
        
    async def _get_market_data(self) -> Dict[str, Any]:
        """Get current market conditions"""
        try:
            response = await self.http_client.get(
                f"{self.derivatives_url}/api/v1/compute/market-data"
            )
            if response.status_code == 200:
                return response.json()
            return {}
        except Exception as e:
            logger.error(f"Failed to get market data: {e}")
            return {}
            
    async def _procure_spot(
        self,
        model_name: str,
        requirement: ComputeRequirement,
        tenant_id: str,
        user_id: str
    ) -> ProcurementResult:
        """Procure compute from spot market"""
        try:
            # Place spot market order
            order_request = {
                "resourceType": "GPU",
                "quantity": requirement.gpu_hours,
                "orderType": "MARKET",
                "side": "BUY",
                "specs": requirement.to_resource_spec()["specs"]
            }
            
            response = await self.http_client.post(
                f"{self.derivatives_url}/api/v1/compute/spot/orders",
                json=order_request,
                headers={
                    "X-Tenant-ID": tenant_id,
                    "X-User-ID": user_id
                }
            )
            
            if response.status_code == 201:
                order = response.json()
                
                # Get provisioning details
                provision_response = await self.http_client.post(
                    f"{self.provisioning_url}/api/v1/compute/provision",
                    json={
                        "order_id": order["order_id"],
                        "model_name": model_name,
                        "resource_type": "GPU",
                        "duration_hours": requirement.duration_hours
                    },
                    headers={
                        "X-Tenant-ID": tenant_id,
                        "X-User-ID": user_id
                    }
                )
                
                if provision_response.status_code == 200:
                    provision_data = provision_response.json()
                    
                    return ProcurementResult(
                        success=True,
                        strategy_used=ResourceProcurementStrategy.SPOT,
                        total_cost=Decimal(str(order["total_cost"])),
                        resources_allocated=order["allocated_resources"],
                        spot_orders=[order["order_id"]],
                        access_details=provision_data["access_details"]
                    )
                    
            return ProcurementResult(
                success=False,
                strategy_used=ResourceProcurementStrategy.SPOT,
                total_cost=Decimal("0"),
                resources_allocated={}
            )
            
        except Exception as e:
            logger.error(f"Spot procurement failed: {e}")
            raise
            
    async def _procure_futures(
        self,
        model_name: str,
        requirement: ComputeRequirement,
        tenant_id: str,
        user_id: str
    ) -> ProcurementResult:
        """Procure compute using futures contracts"""
        try:
            # Calculate delivery time
            delivery_time = requirement.deadline or (datetime.utcnow() + timedelta(hours=24))
            
            # Create futures contract
            futures_request = {
                "resourceType": "GPU",
                "quantity": requirement.gpu_hours,
                "deliveryTime": delivery_time.isoformat(),
                "duration": requirement.duration_hours * 3600,
                "specs": requirement.to_resource_spec()["specs"]
            }
            
            response = await self.http_client.post(
                f"{self.derivatives_url}/api/v1/compute/futures/contracts",
                json=futures_request,
                headers={
                    "X-Tenant-ID": tenant_id,
                    "X-User-ID": user_id
                }
            )
            
            if response.status_code == 201:
                contract = response.json()
                
                return ProcurementResult(
                    success=True,
                    strategy_used=ResourceProcurementStrategy.FUTURES,
                    total_cost=Decimal(str(contract["total_price"])),
                    resources_allocated={
                        "contract_id": contract["contract_id"],
                        "delivery_time": contract["delivery_time"],
                        "guaranteed_resources": contract["resources"]
                    },
                    futures_contracts=[contract["contract_id"]],
                    hedging_coverage=100.0  # Fully hedged with futures
                )
                
            return ProcurementResult(
                success=False,
                strategy_used=ResourceProcurementStrategy.FUTURES,
                total_cost=Decimal("0"),
                resources_allocated={}
            )
            
        except Exception as e:
            logger.error(f"Futures procurement failed: {e}")
            raise
            
    async def _procure_with_options(
        self,
        model_name: str,
        requirement: ComputeRequirement,
        tenant_id: str,
        user_id: str
    ) -> ProcurementResult:
        """Procure compute with options hedging"""
        try:
            # First get spot allocation
            spot_result = await self._procure_spot(model_name, requirement, tenant_id, user_id)
            
            if not spot_result.success:
                return spot_result
                
            # Buy protective put options
            strike_price = float(spot_result.total_cost) * 1.2  # 20% above current price
            
            options_request = {
                "optionType": "PUT",
                "resourceType": "GPU",
                "quantity": requirement.gpu_hours,
                "strikePrice": strike_price,
                "expiry": (datetime.utcnow() + timedelta(days=30)).isoformat(),
                "exerciseStyle": "AMERICAN"
            }
            
            response = await self.http_client.post(
                f"{self.derivatives_url}/api/v1/compute/options/trade",
                json=options_request,
                headers={
                    "X-Tenant-ID": tenant_id,
                    "X-User-ID": user_id
                }
            )
            
            if response.status_code == 201:
                option = response.json()
                
                spot_result.strategy_used = ResourceProcurementStrategy.OPTIONS
                spot_result.options_contracts = [option["option_id"]]
                spot_result.total_cost += Decimal(str(option["premium"]))
                spot_result.hedging_coverage = 80.0  # Hedged against price increases
                
            return spot_result
            
        except Exception as e:
            logger.error(f"Options procurement failed: {e}")
            raise
            
    async def _procure_mixed(
        self,
        model_name: str,
        requirement: ComputeRequirement,
        tenant_id: str,
        user_id: str
    ) -> ProcurementResult:
        """Use mixed strategy for optimal cost and risk"""
        try:
            total_hours = requirement.gpu_hours
            
            # Allocate across strategies
            spot_allocation = total_hours * 0.5  # 50% spot
            futures_allocation = total_hours * 0.3  # 30% futures
            options_allocation = total_hours * 0.2  # 20% with options
            
            results = []
            total_cost = Decimal("0")
            all_resources = {}
            
            # Procure spot portion
            if spot_allocation > 0:
                spot_req = requirement
                spot_req.gpu_hours = spot_allocation
                spot_result = await self._procure_spot(model_name, spot_req, tenant_id, user_id)
                if spot_result.success:
                    results.append(spot_result)
                    total_cost += spot_result.total_cost
                    all_resources.update(spot_result.resources_allocated)
                    
            # Procure futures portion
            if futures_allocation > 0:
                futures_req = requirement
                futures_req.gpu_hours = futures_allocation
                futures_result = await self._procure_futures(model_name, futures_req, tenant_id, user_id)
                if futures_result.success:
                    results.append(futures_result)
                    total_cost += futures_result.total_cost
                    all_resources.update(futures_result.resources_allocated)
                    
            # Procure with options portion
            if options_allocation > 0:
                options_req = requirement
                options_req.gpu_hours = options_allocation
                options_result = await self._procure_with_options(model_name, options_req, tenant_id, user_id)
                if options_result.success:
                    results.append(options_result)
                    total_cost += options_result.total_cost
                    all_resources.update(options_result.resources_allocated)
                    
            # Calculate weighted average hedging
            total_hedging = sum(r.hedging_coverage * r.resources_allocated.get("gpu_hours", 0) 
                              for r in results if r.success)
            avg_hedging = total_hedging / total_hours if total_hours > 0 else 0
            
            # Combine all contracts
            all_futures = []
            all_options = []
            all_spot = []
            
            for r in results:
                all_futures.extend(r.futures_contracts)
                all_options.extend(r.options_contracts)
                all_spot.extend(r.spot_orders)
                
            return ProcurementResult(
                success=len(results) > 0,
                strategy_used=ResourceProcurementStrategy.MIXED,
                total_cost=total_cost,
                resources_allocated=all_resources,
                futures_contracts=all_futures,
                options_contracts=all_options,
                spot_orders=all_spot,
                hedging_coverage=avg_hedging,
                estimated_savings=self._calculate_savings(total_cost, requirement)
            )
            
        except Exception as e:
            logger.error(f"Mixed procurement failed: {e}")
            raise
            
    def _calculate_savings(self, actual_cost: Decimal, requirement: ComputeRequirement) -> Decimal:
        """Calculate estimated savings vs standard pricing"""
        # Assume standard GPU hour price
        standard_price = Decimal("50.0")  # $50/GPU hour
        standard_total = standard_price * Decimal(str(requirement.gpu_hours))
        
        savings = standard_total - actual_cost
        return max(savings, Decimal("0"))
        
    async def monitor_costs(self, model_name: str, tenant_id: str) -> Dict[str, Any]:
        """Monitor ongoing training costs"""
        try:
            # Get active contracts
            response = await self.http_client.get(
                f"{self.derivatives_url}/api/v1/compute/positions",
                headers={
                    "X-Tenant-ID": tenant_id
                }
            )
            
            if response.status_code == 200:
                positions = response.json()
                
                # Filter for this model
                model_positions = [
                    p for p in positions 
                    if p.get("metadata", {}).get("model_name") == model_name
                ]
                
                return {
                    "model_name": model_name,
                    "active_contracts": len(model_positions),
                    "total_allocated_hours": sum(p.get("quantity", 0) for p in model_positions),
                    "total_cost": sum(Decimal(str(p.get("cost", 0))) for p in model_positions),
                    "positions": model_positions
                }
                
            return {"model_name": model_name, "active_contracts": 0}
            
        except Exception as e:
            logger.error(f"Failed to monitor costs: {e}")
            return {"error": str(e)}
            
    async def optimize_training_schedule(
        self,
        training_jobs: List[Dict[str, Any]],
        tenant_id: str
    ) -> List[Dict[str, Any]]:
        """Optimize training schedule based on market conditions"""
        try:
            # Get price predictions
            response = await self.http_client.get(
                f"{self.derivatives_url}/api/v1/compute/price-forecast",
                params={"horizon_days": 7}
            )
            
            if response.status_code == 200:
                forecast = response.json()
                
                # Sort jobs by priority and cost sensitivity
                optimized_schedule = []
                
                for job in training_jobs:
                    best_time = self._find_optimal_time(job, forecast)
                    
                    optimized_schedule.append({
                        **job,
                        "recommended_start_time": best_time,
                        "estimated_cost": self._estimate_cost(job, forecast, best_time)
                    })
                    
                return sorted(optimized_schedule, key=lambda x: x["recommended_start_time"])
                
            return training_jobs
            
        except Exception as e:
            logger.error(f"Failed to optimize schedule: {e}")
            return training_jobs
            
    def _find_optimal_time(self, job: Dict[str, Any], forecast: Dict[str, Any]) -> datetime:
        """Find optimal time to start training based on price forecast"""
        # Simple optimization: find lowest price period
        price_series = forecast.get("price_forecast", [])
        
        if not price_series:
            return datetime.utcnow()
            
        min_price_point = min(price_series, key=lambda x: x["price"])
        return datetime.fromisoformat(min_price_point["timestamp"])
        
    def _estimate_cost(
        self,
        job: Dict[str, Any],
        forecast: Dict[str, Any],
        start_time: datetime
    ) -> Decimal:
        """Estimate cost for job starting at given time"""
        # Simplified estimation
        base_price = Decimal("50.0")
        
        # Find price multiplier from forecast
        for point in forecast.get("price_forecast", []):
            if datetime.fromisoformat(point["timestamp"]) >= start_time:
                base_price = Decimal(str(point["price"]))
                break
                
        hours = job.get("estimated_hours", 1)
        return base_price * Decimal(str(hours))
        
    async def close(self):
        """Cleanup resources"""
        await self.http_client.aclose() 