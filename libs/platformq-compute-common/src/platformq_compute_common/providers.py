"""Provider abstraction layer for compute resources

This module provides a unified interface for different compute providers.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import asyncio
import logging

from .models import (
    ResourceRequirements,
    ResourceAllocation,
    ProviderType,
    AllocationStatus,
    PricingModel
)

logger = logging.getLogger(__name__)


@dataclass
class ProviderCapabilities:
    """Capabilities of a compute provider"""
    provider_type: ProviderType
    supported_regions: List[str]
    supported_instance_types: Dict[str, Dict[str, Any]]
    supported_gpu_types: List[str]
    supported_pricing_models: List[PricingModel]
    max_instances: int
    features: Dict[str, bool]  # e.g., {"spot_instances": True, "dedicated_hosts": True}
    sla_guarantees: Dict[str, float]  # e.g., {"availability": 0.999, "network": 0.995}
    compliance_certifications: List[str]  # e.g., ["SOC2", "HIPAA", "PCI-DSS"]


class ResourceProvider(ABC):
    """Abstract base class for compute resource providers"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.provider_type = config.get("provider_type", ProviderType.ON_PREMISE)
        self._capabilities: Optional[ProviderCapabilities] = None
        self._health_status = {"healthy": True, "last_check": datetime.utcnow()}
        
    @abstractmethod
    async def get_capabilities(self) -> ProviderCapabilities:
        """Get provider capabilities"""
        pass
        
    @abstractmethod
    async def check_availability(
        self,
        requirements: ResourceRequirements,
        region: Optional[str] = None
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """Check if resources are available
        
        Returns:
            Tuple of (available, instance_type, availability_details)
        """
        pass
        
    @abstractmethod
    async def get_pricing(
        self,
        requirements: ResourceRequirements,
        region: str,
        instance_type: str,
        pricing_model: PricingModel = PricingModel.ON_DEMAND
    ) -> Dict[str, Any]:
        """Get pricing information
        
        Returns:
            Dict with pricing details including hourly_cost, setup_fee, etc.
        """
        pass
        
    @abstractmethod
    async def allocate(
        self,
        allocation: ResourceAllocation
    ) -> Tuple[bool, Dict[str, Any]]:
        """Allocate resources
        
        Returns:
            Tuple of (success, allocation_details)
        """
        pass
        
    @abstractmethod
    async def deallocate(
        self,
        allocation: ResourceAllocation
    ) -> Tuple[bool, str]:
        """Deallocate resources
        
        Returns:
            Tuple of (success, message)
        """
        pass
        
    @abstractmethod
    async def get_status(
        self,
        allocation: ResourceAllocation
    ) -> Dict[str, Any]:
        """Get allocation status and metrics"""
        pass
        
    @abstractmethod
    async def resize(
        self,
        allocation: ResourceAllocation,
        new_requirements: ResourceRequirements
    ) -> Tuple[bool, Dict[str, Any]]:
        """Resize an existing allocation"""
        pass
        
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on provider"""
        try:
            # Basic implementation - providers can override
            capabilities = await self.get_capabilities()
            self._health_status = {
                "healthy": True,
                "last_check": datetime.utcnow(),
                "capabilities": capabilities is not None
            }
        except Exception as e:
            logger.error(f"Health check failed for {self.provider_type}: {e}")
            self._health_status = {
                "healthy": False,
                "last_check": datetime.utcnow(),
                "error": str(e)
            }
        
        return self._health_status
        
    def supports_pricing_model(self, model: PricingModel) -> bool:
        """Check if provider supports a pricing model"""
        if not self._capabilities:
            return False
        return model in self._capabilities.supported_pricing_models
        
    def get_sla_guarantee(self, metric: str) -> Optional[float]:
        """Get SLA guarantee for a specific metric"""
        if not self._capabilities:
            return None
        return self._capabilities.sla_guarantees.get(metric)


class ProviderRegistry:
    """Registry for managing compute providers"""
    
    def __init__(self):
        self._providers: Dict[str, ResourceProvider] = {}
        self._provider_health: Dict[str, Dict[str, Any]] = {}
        self._health_check_interval = 60  # seconds
        self._health_check_task: Optional[asyncio.Task] = None
        
    def register(self, name: str, provider: ResourceProvider):
        """Register a provider"""
        self._providers[name] = provider
        logger.info(f"Registered provider: {name} ({provider.provider_type})")
        
    def unregister(self, name: str):
        """Unregister a provider"""
        if name in self._providers:
            del self._providers[name]
            logger.info(f"Unregistered provider: {name}")
            
    def get_provider(self, name: str) -> Optional[ResourceProvider]:
        """Get a specific provider"""
        return self._providers.get(name)
        
    def get_providers_by_type(self, provider_type: ProviderType) -> List[ResourceProvider]:
        """Get all providers of a specific type"""
        return [
            provider for provider in self._providers.values()
            if provider.provider_type == provider_type
        ]
        
    def get_all_providers(self) -> Dict[str, ResourceProvider]:
        """Get all registered providers"""
        return self._providers.copy()
        
    async def start_health_monitoring(self):
        """Start background health monitoring"""
        if self._health_check_task:
            return
            
        async def monitor():
            while True:
                await self._check_all_providers_health()
                await asyncio.sleep(self._health_check_interval)
                
        self._health_check_task = asyncio.create_task(monitor())
        
    async def stop_health_monitoring(self):
        """Stop health monitoring"""
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
            self._health_check_task = None
            
    async def _check_all_providers_health(self):
        """Check health of all providers"""
        for name, provider in self._providers.items():
            try:
                health = await provider.health_check()
                self._provider_health[name] = health
            except Exception as e:
                logger.error(f"Failed to check health for provider {name}: {e}")
                self._provider_health[name] = {
                    "healthy": False,
                    "error": str(e),
                    "last_check": datetime.utcnow()
                }
                
    def get_healthy_providers(self) -> Dict[str, ResourceProvider]:
        """Get only healthy providers"""
        healthy = {}
        for name, provider in self._providers.items():
            if self._provider_health.get(name, {}).get("healthy", True):
                healthy[name] = provider
        return healthy
        
    async def find_best_provider(
        self,
        requirements: ResourceRequirements,
        strategy: str = "balanced"
    ) -> Optional[Tuple[str, ResourceProvider]]:
        """Find the best provider for given requirements"""
        candidates = []
        
        for name, provider in self.get_healthy_providers().items():
            try:
                available, instance_type, details = await provider.check_availability(
                    requirements
                )
                if available:
                    pricing = await provider.get_pricing(
                        requirements,
                        details.get("region", "us-east-1"),
                        instance_type,
                        PricingModel.ON_DEMAND
                    )
                    
                    candidates.append({
                        "name": name,
                        "provider": provider,
                        "instance_type": instance_type,
                        "cost": pricing.get("hourly_cost", float('inf')),
                        "details": details,
                        "pricing": pricing
                    })
            except Exception as e:
                logger.error(f"Error checking provider {name}: {e}")
                continue
                
        if not candidates:
            return None
            
        # Select based on strategy
        if strategy == "cost_optimized":
            best = min(candidates, key=lambda x: x["cost"])
        elif strategy == "performance_optimized":
            # Prefer providers with better SLA guarantees
            best = max(
                candidates,
                key=lambda x: x["provider"].get_sla_guarantee("availability") or 0
            )
        else:  # balanced
            # Simple scoring based on cost and availability
            best = min(
                candidates,
                key=lambda x: x["cost"] / (x["provider"].get_sla_guarantee("availability") or 0.95)
            )
            
        return best["name"], best["provider"] 