"""
Compute Allocation Engine

Manages compute resource allocation across multiple providers.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import uuid
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


class AllocationStrategy(str, Enum):
    """Resource allocation strategies"""
    COST_OPTIMIZED = "COST_OPTIMIZED"
    PERFORMANCE_OPTIMIZED = "PERFORMANCE_OPTIMIZED"
    BALANCED = "BALANCED"
    SPOT_PREFERRED = "SPOT_PREFERRED"
    RESERVED_ONLY = "RESERVED_ONLY"


class ResourceType(str, Enum):
    """Types of compute resources"""
    CPU = "CPU"
    GPU = "GPU"
    MEMORY = "MEMORY"
    STORAGE = "STORAGE"
    NETWORK = "NETWORK"


class ProviderType(str, Enum):
    """Compute providers"""
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"
    ON_PREMISE = "ON_PREMISE"
    EDGE = "EDGE"
    QUANTUM = "QUANTUM"


@dataclass
class ResourceRequirements:
    """Resource requirements for a workload"""
    cpu_cores: int = 1
    gpu_count: int = 0
    gpu_type: Optional[str] = None
    memory_gb: float = 2.0
    storage_gb: float = 10.0
    network_bandwidth_gbps: float = 1.0
    
    # Additional requirements
    os_type: str = "linux"
    region_preferences: List[str] = field(default_factory=list)
    availability_zone: Optional[str] = None
    specialized_hardware: List[str] = field(default_factory=list)
    
    # Constraints
    max_cost_per_hour: Optional[float] = None
    min_availability: float = 0.99
    max_latency_ms: Optional[int] = None
    data_locality_requirements: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "cpu_cores": self.cpu_cores,
            "gpu_count": self.gpu_count,
            "gpu_type": self.gpu_type,
            "memory_gb": self.memory_gb,
            "storage_gb": self.storage_gb,
            "network_bandwidth_gbps": self.network_bandwidth_gbps,
            "os_type": self.os_type,
            "region_preferences": self.region_preferences,
            "availability_zone": self.availability_zone,
            "specialized_hardware": self.specialized_hardware,
            "max_cost_per_hour": self.max_cost_per_hour,
            "min_availability": self.min_availability,
            "max_latency_ms": self.max_latency_ms,
            "data_locality_requirements": self.data_locality_requirements
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ResourceRequirements":
        return cls(**data)


@dataclass
class ResourceAllocation:
    """Represents an allocated resource"""
    allocation_id: str
    workload_id: str
    workload_type: str
    provider: ProviderType
    region: str
    instance_type: str
    instance_id: Optional[str] = None
    
    # Resources
    cpu_cores: int = 0
    gpu_count: int = 0
    gpu_type: Optional[str] = None
    memory_gb: float = 0.0
    storage_gb: float = 0.0
    network_bandwidth_gbps: float = 0.0
    
    # Timing
    created_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    status: str = "PENDING"
    
    # Cost
    cost_per_hour: float = 0.0
    total_cost: float = 0.0
    pricing_model: str = "ON_DEMAND"
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def is_active(self) -> bool:
        return self.status in ["ACTIVE", "PENDING"]
    
    def is_expired(self) -> bool:
        if self.expires_at:
            return datetime.utcnow() > self.expires_at
        return False
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "allocation_id": self.allocation_id,
            "workload_id": self.workload_id,
            "workload_type": self.workload_type,
            "provider": self.provider,
            "region": self.region,
            "instance_type": self.instance_type,
            "instance_id": self.instance_id,
            "cpu_cores": self.cpu_cores,
            "gpu_count": self.gpu_count,
            "gpu_type": self.gpu_type,
            "memory_gb": self.memory_gb,
            "storage_gb": self.storage_gb,
            "network_bandwidth_gbps": self.network_bandwidth_gbps,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "status": self.status,
            "cost_per_hour": self.cost_per_hour,
            "total_cost": self.total_cost,
            "pricing_model": self.pricing_model,
            "metadata": self.metadata
        }


class ResourceProvider:
    """Base class for compute resource providers"""
    
    def __init__(self, provider_type: ProviderType, config: Dict[str, Any]):
        self.provider_type = provider_type
        self.config = config
        self.available_regions = config.get("regions", [])
        self.instance_types = config.get("instance_types", {})
    
    async def check_availability(self, 
                               requirements: ResourceRequirements,
                               region: str) -> Tuple[bool, Optional[str]]:
        """Check if resources are available"""
        raise NotImplementedError
    
    async def get_pricing(self,
                         requirements: ResourceRequirements,
                         region: str,
                         pricing_model: str = "ON_DEMAND") -> float:
        """Get pricing for resources"""
        raise NotImplementedError
    
    async def allocate(self,
                      allocation: ResourceAllocation) -> bool:
        """Allocate resources"""
        raise NotImplementedError
    
    async def deallocate(self,
                        allocation: ResourceAllocation) -> bool:
        """Deallocate resources"""
        raise NotImplementedError
    
    async def get_status(self,
                        allocation: ResourceAllocation) -> str:
        """Get allocation status"""
        raise NotImplementedError


class MockCloudProvider(ResourceProvider):
    """Mock cloud provider for testing"""
    
    def __init__(self, provider_type: ProviderType, config: Dict[str, Any]):
        super().__init__(provider_type, config)
        self.allocations = {}
        
        # Mock pricing
        self.base_prices = {
            "cpu_per_core": 0.05,
            "gpu_per_unit": 0.50,
            "memory_per_gb": 0.01,
            "storage_per_gb": 0.001,
            "network_per_gbps": 0.02
        }
    
    async def check_availability(self, 
                               requirements: ResourceRequirements,
                               region: str) -> Tuple[bool, Optional[str]]:
        """Check if resources are available"""
        # Mock availability check
        if region not in self.available_regions:
            return False, None
        
        # Simple capacity check
        if requirements.gpu_count > 8:
            return False, None
        
        # Find suitable instance type
        for instance_type, specs in self.instance_types.items():
            if (specs.get("cpu_cores", 0) >= requirements.cpu_cores and
                specs.get("gpu_count", 0) >= requirements.gpu_count and
                specs.get("memory_gb", 0) >= requirements.memory_gb):
                return True, instance_type
        
        return False, None
    
    async def get_pricing(self,
                         requirements: ResourceRequirements,
                         region: str,
                         pricing_model: str = "ON_DEMAND") -> float:
        """Get pricing for resources"""
        base_cost = (
            requirements.cpu_cores * self.base_prices["cpu_per_core"] +
            requirements.gpu_count * self.base_prices["gpu_per_unit"] +
            requirements.memory_gb * self.base_prices["memory_per_gb"] +
            requirements.storage_gb * self.base_prices["storage_per_gb"] +
            requirements.network_bandwidth_gbps * self.base_prices["network_per_gbps"]
        )
        
        # Apply pricing model multiplier
        if pricing_model == "SPOT":
            base_cost *= 0.3  # 70% discount
        elif pricing_model == "RESERVED":
            base_cost *= 0.6  # 40% discount
        
        # Apply region multiplier
        region_multipliers = {
            "us-east-1": 1.0,
            "us-west-2": 1.1,
            "eu-central-1": 1.2,
            "ap-southeast-1": 1.15
        }
        base_cost *= region_multipliers.get(region, 1.0)
        
        return round(base_cost, 4)
    
    async def allocate(self,
                      allocation: ResourceAllocation) -> bool:
        """Allocate resources"""
        try:
            # Simulate allocation delay
            await asyncio.sleep(0.5)
            
            # Generate instance ID
            allocation.instance_id = f"{self.provider_type.lower()}-{uuid.uuid4().hex[:8]}"
            allocation.status = "ACTIVE"
            
            # Store allocation
            self.allocations[allocation.allocation_id] = allocation
            
            logger.info(f"Allocated resources: {allocation.allocation_id} on {self.provider_type}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to allocate resources: {e}")
            return False
    
    async def deallocate(self,
                        allocation: ResourceAllocation) -> bool:
        """Deallocate resources"""
        try:
            # Simulate deallocation delay
            await asyncio.sleep(0.3)
            
            if allocation.allocation_id in self.allocations:
                del self.allocations[allocation.allocation_id]
                allocation.status = "TERMINATED"
                
                logger.info(f"Deallocated resources: {allocation.allocation_id}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to deallocate resources: {e}")
            return False
    
    async def get_status(self,
                        allocation: ResourceAllocation) -> str:
        """Get allocation status"""
        if allocation.allocation_id in self.allocations:
            return "ACTIVE"
        return "TERMINATED"


class AllocationEngine:
    """Core compute allocation engine"""
    
    def __init__(self):
        self.providers: Dict[ProviderType, ResourceProvider] = {}
        self.allocations: Dict[str, ResourceAllocation] = {}
        self.allocation_history: List[ResourceAllocation] = []
        self._monitor_task = None
        
        # Initialize mock providers
        self._initialize_mock_providers()
    
    def _initialize_mock_providers(self):
        """Initialize mock providers for testing"""
        # AWS mock
        aws_config = {
            "regions": ["us-east-1", "us-west-2", "eu-central-1"],
            "instance_types": {
                "t3.micro": {"cpu_cores": 2, "memory_gb": 1},
                "t3.medium": {"cpu_cores": 2, "memory_gb": 4},
                "m5.large": {"cpu_cores": 2, "memory_gb": 8},
                "m5.xlarge": {"cpu_cores": 4, "memory_gb": 16},
                "p3.2xlarge": {"cpu_cores": 8, "gpu_count": 1, "gpu_type": "V100", "memory_gb": 61},
                "p3.8xlarge": {"cpu_cores": 32, "gpu_count": 4, "gpu_type": "V100", "memory_gb": 244}
            }
        }
        self.providers[ProviderType.AWS] = MockCloudProvider(ProviderType.AWS, aws_config)
        
        # Azure mock
        azure_config = {
            "regions": ["eastus", "westus2", "westeurope"],
            "instance_types": {
                "B1s": {"cpu_cores": 1, "memory_gb": 1},
                "B2s": {"cpu_cores": 2, "memory_gb": 4},
                "D2s_v3": {"cpu_cores": 2, "memory_gb": 8},
                "D4s_v3": {"cpu_cores": 4, "memory_gb": 16},
                "NC6": {"cpu_cores": 6, "gpu_count": 1, "gpu_type": "K80", "memory_gb": 56},
                "NC12": {"cpu_cores": 12, "gpu_count": 2, "gpu_type": "K80", "memory_gb": 112}
            }
        }
        self.providers[ProviderType.AZURE] = MockCloudProvider(ProviderType.AZURE, azure_config)
        
        # On-premise mock
        onprem_config = {
            "regions": ["datacenter-1"],
            "instance_types": {
                "server-small": {"cpu_cores": 8, "memory_gb": 32},
                "server-medium": {"cpu_cores": 16, "memory_gb": 64},
                "server-large": {"cpu_cores": 32, "memory_gb": 128},
                "gpu-server": {"cpu_cores": 24, "gpu_count": 2, "gpu_type": "RTX3090", "memory_gb": 128}
            }
        }
        self.providers[ProviderType.ON_PREMISE] = MockCloudProvider(ProviderType.ON_PREMISE, onprem_config)
    
    async def start(self):
        """Start the allocation engine"""
        self._monitor_task = asyncio.create_task(self._monitor_allocations())
        logger.info("Allocation engine started")
    
    async def stop(self):
        """Stop the allocation engine"""
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info("Allocation engine stopped")
    
    async def allocate_resources(self,
                               workload_type: str,
                               workload_id: str,
                               requirements: ResourceRequirements,
                               strategy: AllocationStrategy = AllocationStrategy.BALANCED,
                               duration_hours: float = 1.0) -> Optional[ResourceAllocation]:
        """Allocate compute resources"""
        try:
            # Find best provider and region
            provider, region, instance_type, cost = await self._find_best_allocation(
                requirements, strategy
            )
            
            if not provider:
                logger.error("No suitable provider found for requirements")
                return None
            
            # Create allocation
            allocation = ResourceAllocation(
                allocation_id=str(uuid.uuid4()),
                workload_id=workload_id,
                workload_type=workload_type,
                provider=provider.provider_type,
                region=region,
                instance_type=instance_type,
                cpu_cores=requirements.cpu_cores,
                gpu_count=requirements.gpu_count,
                gpu_type=requirements.gpu_type,
                memory_gb=requirements.memory_gb,
                storage_gb=requirements.storage_gb,
                network_bandwidth_gbps=requirements.network_bandwidth_gbps,
                expires_at=datetime.utcnow() + timedelta(hours=duration_hours),
                cost_per_hour=cost,
                total_cost=cost * duration_hours,
                pricing_model="ON_DEMAND" if strategy != AllocationStrategy.SPOT_PREFERRED else "SPOT"
            )
            
            # Allocate with provider
            success = await provider.allocate(allocation)
            
            if success:
                self.allocations[allocation.allocation_id] = allocation
                self.allocation_history.append(allocation)
                logger.info(f"Successfully allocated resources: {allocation.allocation_id}")
                return allocation
            else:
                logger.error("Provider failed to allocate resources")
                return None
                
        except Exception as e:
            logger.error(f"Failed to allocate resources: {e}")
            return None
    
    async def _find_best_allocation(self,
                                  requirements: ResourceRequirements,
                                  strategy: AllocationStrategy) -> Tuple[Optional[ResourceProvider], 
                                                                        Optional[str], 
                                                                        Optional[str], 
                                                                        float]:
        """Find best provider and region for allocation"""
        candidates = []
        
        # Check all providers and regions
        for provider in self.providers.values():
            for region in provider.available_regions:
                # Skip if region not in preferences (if specified)
                if requirements.region_preferences and region not in requirements.region_preferences:
                    continue
                
                # Check availability
                available, instance_type = await provider.check_availability(requirements, region)
                if not available:
                    continue
                
                # Get pricing
                pricing_model = "SPOT" if strategy == AllocationStrategy.SPOT_PREFERRED else "ON_DEMAND"
                cost = await provider.get_pricing(requirements, region, pricing_model)
                
                # Check cost constraint
                if requirements.max_cost_per_hour and cost > requirements.max_cost_per_hour:
                    continue
                
                candidates.append((provider, region, instance_type, cost))
        
        if not candidates:
            return None, None, None, 0.0
        
        # Select based on strategy
        if strategy == AllocationStrategy.COST_OPTIMIZED:
            # Sort by cost
            candidates.sort(key=lambda x: x[3])
        elif strategy == AllocationStrategy.PERFORMANCE_OPTIMIZED:
            # Prefer certain providers/regions for performance
            # For now, just prefer on-premise and certain regions
            def performance_score(candidate):
                provider, region, _, cost = candidate
                score = cost  # Base score is cost
                if provider.provider_type == ProviderType.ON_PREMISE:
                    score *= 0.5  # Prefer on-premise
                if region in ["us-east-1", "datacenter-1"]:
                    score *= 0.8  # Prefer certain regions
                return score
            
            candidates.sort(key=performance_score)
        else:
            # BALANCED - consider both cost and performance
            # Simple weighted scoring
            candidates.sort(key=lambda x: x[3])  # Sort by cost for now
        
        return candidates[0]
    
    async def get_allocation(self, allocation_id: str) -> Optional[ResourceAllocation]:
        """Get allocation by ID"""
        return self.allocations.get(allocation_id)
    
    async def modify_allocation(self, 
                              allocation_id: str,
                              modifications: Dict[str, Any]) -> bool:
        """Modify an existing allocation"""
        allocation = self.allocations.get(allocation_id)
        if not allocation:
            return False
        
        try:
            # Handle duration extension
            if "extend_hours" in modifications:
                hours = modifications["extend_hours"]
                allocation.expires_at = allocation.expires_at + timedelta(hours=hours)
                allocation.total_cost += allocation.cost_per_hour * hours
            
            # Handle scaling (would need provider support in real implementation)
            if "scale_to" in modifications:
                # This is simplified - real implementation would need to
                # deallocate old and allocate new resources
                logger.warning("Scaling not fully implemented in mock provider")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to modify allocation: {e}")
            return False
    
    async def deallocate_resources(self, allocation_id: str) -> bool:
        """Deallocate resources"""
        allocation = self.allocations.get(allocation_id)
        if not allocation:
            return False
        
        try:
            provider = self.providers.get(allocation.provider)
            if provider:
                success = await provider.deallocate(allocation)
                if success:
                    allocation.status = "TERMINATED"
                    del self.allocations[allocation_id]
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Failed to deallocate resources: {e}")
            return False
    
    async def get_current_pricing(self) -> Dict[str, Any]:
        """Get current pricing across providers"""
        pricing = {}
        
        # Sample requirements for pricing
        sample_requirements = ResourceRequirements(
            cpu_cores=4,
            memory_gb=16,
            storage_gb=100
        )
        
        for provider_type, provider in self.providers.items():
            provider_pricing = {}
            for region in provider.available_regions[:2]:  # Sample first 2 regions
                costs = {}
                for model in ["ON_DEMAND", "SPOT", "RESERVED"]:
                    cost = await provider.get_pricing(sample_requirements, region, model)
                    costs[model.lower()] = cost
                provider_pricing[region] = costs
            
            pricing[provider_type.value] = provider_pricing
        
        return pricing
    
    async def get_available_resources(self, resource_type: Optional[str] = None) -> Dict[str, Any]:
        """Get available resources across providers"""
        resources = {}
        
        for provider_type, provider in self.providers.items():
            provider_resources = {
                "regions": provider.available_regions,
                "instance_types": provider.instance_types
            }
            resources[provider_type.value] = provider_resources
        
        return resources
    
    async def _monitor_allocations(self):
        """Monitor allocations and handle expirations"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Check for expired allocations
                expired = []
                for allocation_id, allocation in self.allocations.items():
                    if allocation.is_expired():
                        expired.append(allocation_id)
                
                # Deallocate expired resources
                for allocation_id in expired:
                    logger.info(f"Deallocating expired allocation: {allocation_id}")
                    await self.deallocate_resources(allocation_id)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in allocation monitor: {e}")
    
    def get_allocation_metrics(self) -> Dict[str, Any]:
        """Get allocation metrics"""
        active_allocations = [a for a in self.allocations.values() if a.is_active()]
        
        metrics = {
            "total_allocations": len(self.allocation_history),
            "active_allocations": len(active_allocations),
            "allocations_by_provider": {},
            "allocations_by_workload_type": {},
            "total_cost_per_hour": 0.0,
            "total_resources": {
                "cpu_cores": 0,
                "gpu_count": 0,
                "memory_gb": 0.0,
                "storage_gb": 0.0
            }
        }
        
        for allocation in active_allocations:
            # By provider
            provider = allocation.provider.value
            metrics["allocations_by_provider"][provider] = \
                metrics["allocations_by_provider"].get(provider, 0) + 1
            
            # By workload type
            workload = allocation.workload_type
            metrics["allocations_by_workload_type"][workload] = \
                metrics["allocations_by_workload_type"].get(workload, 0) + 1
            
            # Total cost
            metrics["total_cost_per_hour"] += allocation.cost_per_hour
            
            # Total resources
            metrics["total_resources"]["cpu_cores"] += allocation.cpu_cores
            metrics["total_resources"]["gpu_count"] += allocation.gpu_count
            metrics["total_resources"]["memory_gb"] += allocation.memory_gb
            metrics["total_resources"]["storage_gb"] += allocation.storage_gb
        
        return metrics 