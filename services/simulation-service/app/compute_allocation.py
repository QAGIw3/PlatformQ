"""
Simulation Compute Allocation

Dynamic compute resource allocation for simulations using derivatives engine.
Handles multi-physics, federated learning, and large-scale agent simulations.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import numpy as np

import httpx
from platformq_shared.cache import CacheManager
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class SimulationType(Enum):
    AGENT_BASED = "agent_based"
    MULTI_PHYSICS = "multi_physics"
    FEDERATED_ML = "federated_ml"
    HYBRID = "hybrid"
    MONTE_CARLO = "monte_carlo"


class ResourceIntensity(Enum):
    LOW = "low"  # < 10 GPU hours
    MEDIUM = "medium"  # 10-100 GPU hours
    HIGH = "high"  # 100-1000 GPU hours
    EXTREME = "extreme"  # > 1000 GPU hours


@dataclass
class SimulationResourceRequirements:
    """Resource requirements for simulation"""
    simulation_id: str
    simulation_type: SimulationType
    agent_count: int = 1000
    timesteps: int = 1000
    physics_engines: List[str] = field(default_factory=list)
    ml_models: List[str] = field(default_factory=list)
    participant_count: int = 1  # For federated simulations
    data_size_gb: float = 1.0
    expected_duration_hours: float = 1.0
    deadline: Optional[datetime] = None
    priority: str = "normal"
    
    def calculate_gpu_hours(self) -> float:
        """Calculate estimated GPU hours needed"""
        base_hours = 0.0
        
        # Agent-based simulation scaling
        if self.simulation_type in [SimulationType.AGENT_BASED, SimulationType.HYBRID]:
            # O(n * t) complexity for agents and timesteps
            agent_factor = self.agent_count / 1000  # Normalized to 1k agents
            time_factor = self.timesteps / 1000  # Normalized to 1k steps
            base_hours += agent_factor * time_factor * 2  # 2 GPU hours per normalized unit
            
        # Multi-physics adds significant compute
        if self.simulation_type in [SimulationType.MULTI_PHYSICS, SimulationType.HYBRID]:
            physics_hours = len(self.physics_engines) * 5  # 5 hours per physics engine
            base_hours += physics_hours
            
        # Federated ML scales with participants
        if self.simulation_type == SimulationType.FEDERATED_ML:
            fl_hours = self.participant_count * len(self.ml_models) * 3
            base_hours += fl_hours
            
        # Monte Carlo simulations
        if self.simulation_type == SimulationType.MONTE_CARLO:
            # Assume 1000 iterations as baseline
            mc_hours = (self.timesteps / 1000) * 10
            base_hours += mc_hours
            
        # Add overhead for data transfer and coordination
        overhead = base_hours * 0.2
        
        return base_hours + overhead
        
    def get_resource_intensity(self) -> ResourceIntensity:
        """Determine resource intensity level"""
        gpu_hours = self.calculate_gpu_hours()
        
        if gpu_hours < 10:
            return ResourceIntensity.LOW
        elif gpu_hours < 100:
            return ResourceIntensity.MEDIUM
        elif gpu_hours < 1000:
            return ResourceIntensity.HIGH
        else:
            return ResourceIntensity.EXTREME


@dataclass
class SimulationAllocationResult:
    """Result of compute allocation for simulation"""
    success: bool
    simulation_id: str
    allocated_resources: Dict[str, Any]
    total_cost: Decimal
    allocation_strategy: str
    contracts: Dict[str, List[str]] = field(default_factory=dict)
    burst_capacity_available: bool = False
    estimated_start_time: Optional[datetime] = None
    estimated_completion_time: Optional[datetime] = None
    optimization_applied: List[str] = field(default_factory=list)


class SimulationComputeAllocator:
    """Handles compute allocation for simulations"""
    
    def __init__(
        self,
        derivatives_engine_url: str = "http://derivatives-engine-service:8000",
        ignite_cache: Optional[CacheManager] = None,
        event_publisher: Optional[EventPublisher] = None
    ):
        self.derivatives_url = derivatives_engine_url
        self.cache = ignite_cache or CacheManager()
        self.event_publisher = event_publisher
        self.http_client = httpx.AsyncClient(timeout=60.0)
        
    async def allocate_simulation_resources(
        self,
        requirements: SimulationResourceRequirements,
        tenant_id: str,
        user_id: str
    ) -> SimulationAllocationResult:
        """Allocate compute resources for simulation"""
        try:
            gpu_hours = requirements.calculate_gpu_hours()
            intensity = requirements.get_resource_intensity()
            
            # Choose allocation strategy based on intensity
            if intensity == ResourceIntensity.LOW:
                return await self._allocate_spot_resources(requirements, gpu_hours, tenant_id, user_id)
            elif intensity == ResourceIntensity.MEDIUM:
                return await self._allocate_mixed_resources(requirements, gpu_hours, tenant_id, user_id)
            elif intensity == ResourceIntensity.HIGH:
                return await self._allocate_futures_resources(requirements, gpu_hours, tenant_id, user_id)
            else:  # EXTREME
                return await self._allocate_burst_resources(requirements, gpu_hours, tenant_id, user_id)
                
        except Exception as e:
            logger.error(f"Failed to allocate simulation resources: {e}")
            return SimulationAllocationResult(
                success=False,
                simulation_id=requirements.simulation_id,
                allocated_resources={},
                total_cost=Decimal("0"),
                allocation_strategy="failed"
            )
            
    async def _allocate_spot_resources(
        self,
        requirements: SimulationResourceRequirements,
        gpu_hours: float,
        tenant_id: str,
        user_id: str
    ) -> SimulationAllocationResult:
        """Allocate from spot market for small simulations"""
        try:
            # Place spot order
            order_request = {
                "resourceType": "GPU",
                "quantity": gpu_hours,
                "orderType": "LIMIT",
                "side": "BUY",
                "price": 45.0,  # Slightly below market for better fill
                "specs": {
                    "gpu_type": "NVIDIA_V100",  # V100 sufficient for small sims
                    "cpu_cores": 8,
                    "memory_gb": 32,
                    "storage_gb": 100
                },
                "metadata": {
                    "simulation_id": requirements.simulation_id,
                    "simulation_type": requirements.simulation_type.value
                }
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
                
                return SimulationAllocationResult(
                    success=True,
                    simulation_id=requirements.simulation_id,
                    allocated_resources=order["allocated_resources"],
                    total_cost=Decimal(str(order["total_cost"])),
                    allocation_strategy="spot",
                    contracts={"spot": [order["order_id"]]},
                    estimated_start_time=datetime.utcnow(),
                    estimated_completion_time=datetime.utcnow() + timedelta(hours=requirements.expected_duration_hours)
                )
                
        except Exception as e:
            logger.error(f"Spot allocation failed: {e}")
            
        return SimulationAllocationResult(
            success=False,
            simulation_id=requirements.simulation_id,
            allocated_resources={},
            total_cost=Decimal("0"),
            allocation_strategy="spot"
        )
        
    async def _allocate_mixed_resources(
        self,
        requirements: SimulationResourceRequirements,
        gpu_hours: float,
        tenant_id: str,
        user_id: str
    ) -> SimulationAllocationResult:
        """Use mix of spot and futures for medium simulations"""
        try:
            # Split allocation: 60% spot, 40% futures
            spot_hours = gpu_hours * 0.6
            futures_hours = gpu_hours * 0.4
            
            total_cost = Decimal("0")
            all_contracts = {"spot": [], "futures": []}
            allocated_resources = {}
            
            # Allocate spot portion
            spot_result = await self._allocate_spot_resources(
                requirements,
                spot_hours,
                tenant_id,
                user_id
            )
            
            if spot_result.success:
                total_cost += spot_result.total_cost
                all_contracts["spot"].extend(spot_result.contracts.get("spot", []))
                allocated_resources.update(spot_result.allocated_resources)
                
            # Allocate futures portion
            futures_request = {
                "resourceType": "GPU",
                "quantity": futures_hours,
                "deliveryTime": (datetime.utcnow() + timedelta(hours=2)).isoformat(),
                "duration": requirements.expected_duration_hours * 3600,
                "specs": {
                    "gpu_type": "NVIDIA_A100",
                    "cpu_cores": 16,
                    "memory_gb": 64,
                    "storage_gb": 200
                }
            }
            
            futures_response = await self.http_client.post(
                f"{self.derivatives_url}/api/v1/compute/futures/contracts",
                json=futures_request,
                headers={
                    "X-Tenant-ID": tenant_id,
                    "X-User-ID": user_id
                }
            )
            
            if futures_response.status_code == 201:
                futures_contract = futures_response.json()
                total_cost += Decimal(str(futures_contract["total_price"]))
                all_contracts["futures"].append(futures_contract["contract_id"])
                allocated_resources.update(futures_contract["resources"])
                
            return SimulationAllocationResult(
                success=len(all_contracts["spot"]) > 0 or len(all_contracts["futures"]) > 0,
                simulation_id=requirements.simulation_id,
                allocated_resources=allocated_resources,
                total_cost=total_cost,
                allocation_strategy="mixed",
                contracts=all_contracts,
                estimated_start_time=datetime.utcnow() + timedelta(hours=2),
                estimated_completion_time=datetime.utcnow() + timedelta(hours=2 + requirements.expected_duration_hours),
                optimization_applied=["cost_optimization", "resource_diversity"]
            )
            
        except Exception as e:
            logger.error(f"Mixed allocation failed: {e}")
            return SimulationAllocationResult(
                success=False,
                simulation_id=requirements.simulation_id,
                allocated_resources={},
                total_cost=Decimal("0"),
                allocation_strategy="mixed"
            )
            
    async def _allocate_futures_resources(
        self,
        requirements: SimulationResourceRequirements,
        gpu_hours: float,
        tenant_id: str,
        user_id: str
    ) -> SimulationAllocationResult:
        """Use futures contracts for large, predictable simulations"""
        try:
            # Calculate optimal delivery time
            delivery_time = requirements.deadline or (datetime.utcnow() + timedelta(hours=6))
            
            # Create futures contract with volume discount
            futures_request = {
                "resourceType": "GPU",
                "quantity": gpu_hours,
                "deliveryTime": delivery_time.isoformat(),
                "duration": requirements.expected_duration_hours * 3600,
                "specs": {
                    "gpu_type": "NVIDIA_A100_80GB",  # High-end for large sims
                    "cpu_cores": 32,
                    "memory_gb": 128,
                    "storage_gb": 1000
                },
                "slaLevel": 99.9  # High SLA for critical simulations
            }
            
            response = await self.http_client.post(
                f"{self.derivatives_url}/api/v1/compute/futures/day-ahead",
                json=futures_request,
                headers={
                    "X-Tenant-ID": tenant_id,
                    "X-User-ID": user_id
                }
            )
            
            if response.status_code == 201:
                contract = response.json()
                
                # Apply volume discount for large contracts
                discount = Decimal("0.1") if gpu_hours > 500 else Decimal("0")
                final_cost = Decimal(str(contract["total_price"])) * (1 - discount)
                
                return SimulationAllocationResult(
                    success=True,
                    simulation_id=requirements.simulation_id,
                    allocated_resources=contract["resources"],
                    total_cost=final_cost,
                    allocation_strategy="futures",
                    contracts={"futures": [contract["contract_id"]]},
                    estimated_start_time=delivery_time,
                    estimated_completion_time=delivery_time + timedelta(hours=requirements.expected_duration_hours),
                    optimization_applied=["volume_discount", "guaranteed_capacity"]
                )
                
        except Exception as e:
            logger.error(f"Futures allocation failed: {e}")
            
        return SimulationAllocationResult(
            success=False,
            simulation_id=requirements.simulation_id,
            allocated_resources={},
            total_cost=Decimal("0"),
            allocation_strategy="futures"
        )
        
    async def _allocate_burst_resources(
        self,
        requirements: SimulationResourceRequirements,
        gpu_hours: float,
        tenant_id: str,
        user_id: str
    ) -> SimulationAllocationResult:
        """Use burst compute derivatives for extreme workloads"""
        try:
            # Check if burst capacity is available
            burst_check = await self.http_client.get(
                f"{self.derivatives_url}/api/v1/compute/burst/availability",
                params={"required_hours": gpu_hours}
            )
            
            if burst_check.status_code == 200:
                availability = burst_check.json()
                
                if availability["available"]:
                    # Create surge swap for burst capacity
                    surge_request = {
                        "baseCapacity": gpu_hours * 0.3,  # 30% base
                        "surgeMultiplier": 3.0,  # Up to 3x surge
                        "duration": requirements.expected_duration_hours * 3600,
                        "maxPremium": 50.0  # Max 50% premium for surge
                    }
                    
                    surge_response = await self.http_client.post(
                        f"{self.derivatives_url}/api/v1/compute/burst/surge-swap",
                        json=surge_request,
                        headers={
                            "X-Tenant-ID": tenant_id,
                            "X-User-ID": user_id
                        }
                    )
                    
                    if surge_response.status_code == 201:
                        surge_swap = surge_response.json()
                        
                        # Also buy spike options for additional protection
                        spike_request = {
                            "triggerUtilization": 80.0,
                            "additionalCapacity": gpu_hours * 0.2,
                            "duration": requirements.expected_duration_hours * 3600,
                            "maxSpikeDuration": 3600  # 1 hour spikes
                        }
                        
                        spike_response = await self.http_client.post(
                            f"{self.derivatives_url}/api/v1/compute/burst/spike-option",
                            json=spike_request,
                            headers={
                                "X-Tenant-ID": tenant_id,
                                "X-User-ID": user_id
                            }
                        )
                        
                        total_cost = Decimal(str(surge_swap["total_cost"]))
                        contracts = {
                            "surge_swaps": [surge_swap["swap_id"]],
                            "spike_options": []
                        }
                        
                        if spike_response.status_code == 201:
                            spike_option = spike_response.json()
                            total_cost += Decimal(str(spike_option["premium"]))
                            contracts["spike_options"].append(spike_option["option_id"])
                            
                        return SimulationAllocationResult(
                            success=True,
                            simulation_id=requirements.simulation_id,
                            allocated_resources=surge_swap["allocated_resources"],
                            total_cost=total_cost,
                            allocation_strategy="burst",
                            contracts=contracts,
                            burst_capacity_available=True,
                            estimated_start_time=datetime.utcnow(),
                            estimated_completion_time=datetime.utcnow() + timedelta(hours=requirements.expected_duration_hours),
                            optimization_applied=["burst_capacity", "spike_protection", "elastic_scaling"]
                        )
                        
        except Exception as e:
            logger.error(f"Burst allocation failed: {e}")
            
        # Fallback to standard futures if burst not available
        return await self._allocate_futures_resources(requirements, gpu_hours, tenant_id, user_id)
        
    async def optimize_federated_allocation(
        self,
        requirements: SimulationResourceRequirements,
        participant_locations: List[str],
        tenant_id: str
    ) -> Dict[str, SimulationAllocationResult]:
        """Optimize allocation for federated learning across regions"""
        try:
            allocations = {}
            
            # Get regional pricing
            pricing_response = await self.http_client.get(
                f"{self.derivatives_url}/api/v1/compute/regional-pricing"
            )
            
            regional_pricing = {}
            if pricing_response.status_code == 200:
                regional_pricing = pricing_response.json()
                
            # Allocate resources per participant
            for location in participant_locations:
                # Adjust requirements for single participant
                participant_req = SimulationResourceRequirements(
                    simulation_id=f"{requirements.simulation_id}_{location}",
                    simulation_type=SimulationType.FEDERATED_ML,
                    agent_count=requirements.agent_count // len(participant_locations),
                    timesteps=requirements.timesteps,
                    ml_models=requirements.ml_models,
                    participant_count=1,
                    data_size_gb=requirements.data_size_gb / len(participant_locations),
                    expected_duration_hours=requirements.expected_duration_hours,
                    deadline=requirements.deadline,
                    priority=requirements.priority
                )
                
                # Allocate with regional optimization
                allocation = await self.allocate_simulation_resources(
                    participant_req,
                    tenant_id,
                    f"participant_{location}"
                )
                
                # Apply regional pricing adjustment
                if location in regional_pricing:
                    adjustment = Decimal(str(regional_pricing[location]["multiplier"]))
                    allocation.total_cost *= adjustment
                    
                allocations[location] = allocation
                
            return allocations
            
        except Exception as e:
            logger.error(f"Federated allocation optimization failed: {e}")
            return {}
            
    async def estimate_simulation_cost(
        self,
        requirements: SimulationResourceRequirements
    ) -> Dict[str, Any]:
        """Estimate cost for simulation without allocating"""
        try:
            gpu_hours = requirements.calculate_gpu_hours()
            intensity = requirements.get_resource_intensity()
            
            # Get current market prices
            market_response = await self.http_client.get(
                f"{self.derivatives_url}/api/v1/compute/market-data"
            )
            
            if market_response.status_code == 200:
                market_data = market_response.json()
                
                spot_price = Decimal(str(market_data.get("spot_price", 45.0)))
                futures_price = Decimal(str(market_data.get("futures_price", 48.0)))
                burst_premium = Decimal(str(market_data.get("burst_premium", 1.5)))
                
                estimates = {
                    "simulation_id": requirements.simulation_id,
                    "estimated_gpu_hours": gpu_hours,
                    "resource_intensity": intensity.value,
                    "cost_estimates": {
                        "spot_only": float(gpu_hours * spot_price),
                        "futures_only": float(gpu_hours * futures_price),
                        "mixed_strategy": float(gpu_hours * (spot_price * Decimal("0.6") + futures_price * Decimal("0.4"))),
                        "burst_worst_case": float(gpu_hours * spot_price * burst_premium)
                    },
                    "recommended_strategy": self._recommend_strategy(intensity),
                    "potential_optimizations": self._suggest_optimizations(requirements)
                }
                
                return estimates
                
        except Exception as e:
            logger.error(f"Cost estimation failed: {e}")
            
        return {
            "error": "Failed to estimate costs",
            "simulation_id": requirements.simulation_id
        }
        
    def _recommend_strategy(self, intensity: ResourceIntensity) -> str:
        """Recommend allocation strategy based on intensity"""
        recommendations = {
            ResourceIntensity.LOW: "spot",
            ResourceIntensity.MEDIUM: "mixed",
            ResourceIntensity.HIGH: "futures",
            ResourceIntensity.EXTREME: "burst"
        }
        return recommendations.get(intensity, "mixed")
        
    def _suggest_optimizations(self, requirements: SimulationResourceRequirements) -> List[str]:
        """Suggest optimizations to reduce costs"""
        suggestions = []
        
        if requirements.agent_count > 10000:
            suggestions.append("Consider hierarchical agent modeling to reduce agent count")
            
        if requirements.timesteps > 10000:
            suggestions.append("Use adaptive timestepping to reduce computation")
            
        if len(requirements.physics_engines) > 2:
            suggestions.append("Evaluate if all physics engines are necessary")
            
        if requirements.simulation_type == SimulationType.MONTE_CARLO:
            suggestions.append("Use variance reduction techniques")
            
        if requirements.participant_count > 10:
            suggestions.append("Consider hierarchical federated learning")
            
        return suggestions
        
    async def close(self):
        """Cleanup resources"""
        await self.http_client.aclose() 