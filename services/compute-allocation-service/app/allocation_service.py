"""Compute Allocation Service

Handles resource allocation requests using the shared compute framework.
"""

import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
import asyncio

from platformq_compute_common.models import (
    ResourceRequirements,
    ResourceAllocation,
    AllocationRequest,
    AllocationResponse,
    AllocationStatus,
    ComputeResourceType,
    ProviderType,
    PricingModel,
    AllocationStrategy
)
from platformq_compute_common.providers import ProviderRegistry, ResourceProvider
from platformq_compute_common.cost import CostCalculator, BudgetManager
from .providers import (
    AWSProvider,
    CloudStackProvider, 
    KubernetesProvider,
    RackspaceProvider
)
from .config_manager import ConfigManager

logger = logging.getLogger(__name__)


class AllocationService:
    """Main service for managing compute allocations"""
    
    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.provider_registry = ProviderRegistry()
        self.cost_calculator = CostCalculator()
        self.budget_manager = BudgetManager()
        
        # Track allocations
        self.allocations: Dict[str, ResourceAllocation] = {}
        
        # Background tasks
        self._monitor_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize the allocation service"""
        # Load provider configurations from Consul
        providers_config = await self.config_manager.get_config("compute_providers", {})
        
        # Register providers
        for provider_name, config in providers_config.items():
            if config.get("enabled", False):
                # Get credentials from Vault
                credentials = await self.config_manager.get_provider_credentials(provider_name)
                config.update(credentials)
                
                # Create provider instance based on type
                provider = self._create_provider(provider_name, config)
                if provider:
                    self.provider_registry.register(provider_name, provider)
                    logger.info(f"Registered provider: {provider_name}")
        
        # Load budgets from Consul
        budgets = await self.config_manager.get_config("tenant_budgets", {})
        for tenant_id, budget in budgets.items():
            self.budget_manager.set_budget(
                tenant_id,
                budget["monthly_limit"],
                budget.get("alert_thresholds")
            )
        
        # Start background tasks
        await self.provider_registry.start_health_monitoring()
        self._monitor_task = asyncio.create_task(self._monitor_allocations())
        self._cleanup_task = asyncio.create_task(self._cleanup_expired())
        
        # Watch for configuration changes
        await self.config_manager.watch_config(
            "compute_providers",
            self._handle_provider_config_change
        )
        
        await self.config_manager.watch_config(
            "tenant_budgets",
            self._handle_budget_config_change
        )
        
    def _create_provider(self, name: str, config: Dict[str, Any]) -> Optional[ResourceProvider]:
        """Create provider instance based on configuration"""
        provider_type = config.get("type", name).lower()
        
        if provider_type == "aws":
            return AWSProvider(config)
        elif provider_type == "cloudstack":
            return CloudStackProvider(config)
        elif provider_type == "kubernetes":
            return KubernetesProvider(config)
        elif provider_type == "rackspace":
            return RackspaceProvider(config)
        else:
            logger.warning(f"Unknown provider type: {provider_type}")
            return None
    
    async def _handle_provider_config_change(self, new_config: Dict[str, Any]):
        """Handle provider configuration changes"""
        logger.info("Provider configuration changed, updating...")
        
        # Update existing providers or add new ones
        for provider_name, config in new_config.items():
            if config.get("enabled", False):
                # Get updated credentials
                credentials = await self.config_manager.get_provider_credentials(provider_name)
                config.update(credentials)
                
                # Update or create provider
                existing = self.provider_registry.get_provider(provider_name)
                if existing:
                    # Update existing provider config
                    existing.config.update(config)
                else:
                    # Create new provider
                    provider = self._create_provider(provider_name, config)
                    if provider:
                        self.provider_registry.register(provider_name, provider)
            else:
                # Remove disabled provider
                self.provider_registry.unregister(provider_name)
    
    async def _handle_budget_config_change(self, new_budgets: Dict[str, Any]):
        """Handle budget configuration changes"""
        logger.info("Budget configuration changed, updating...")
        
        for tenant_id, budget in new_budgets.items():
            self.budget_manager.set_budget(
                tenant_id,
                budget["monthly_limit"],
                budget.get("alert_thresholds")
            )
    
    async def allocate_resources(
        self,
        request: AllocationRequest
    ) -> AllocationResponse:
        """Allocate compute resources"""
        try:
            # Validate request
            errors = request.requirements.validate()
            if errors:
                return AllocationResponse(
                    success=False,
                    message=f"Invalid requirements: {', '.join(errors)}"
                )
            
            # Check budget
            cost_estimate = await self._estimate_cost(request)
            budget_ok, budget_msg = self.budget_manager.check_budget(
                request.tenant_id,
                cost_estimate
            )
            
            if not budget_ok:
                return AllocationResponse(
                    success=False,
                    message=budget_msg
                )
            
            # Find best provider
            result = await self.provider_registry.find_best_provider(
                request.requirements,
                request.strategy.value.lower()
            )
            
            if not result:
                # No single provider available, check for multi-provider solution
                alternative_options = await self._find_alternative_options(request)
                
                return AllocationResponse(
                    success=False,
                    message="No single provider can fulfill the request",
                    alternative_options=alternative_options
                )
            
            provider_name, provider = result
            
            # Get pricing
            pricing = await provider.get_pricing(
                request.requirements,
                request.requirements.regions[0] if request.requirements.regions else "us-east-1",
                "m5.large",  # Example instance type
                request.pricing_preferences[0] if request.pricing_preferences else PricingModel.ON_DEMAND
            )
            
            # Create allocation
            allocation = ResourceAllocation(
                tenant_id=request.tenant_id,
                workload_id=request.workload_id,
                workload_type=request.workload_type,
                provider=provider.provider_type,
                region=request.requirements.regions[0] if request.requirements.regions else "us-east-1",
                cpu_cores=request.requirements.cpu_cores,
                memory_gb=request.requirements.memory_gb,
                storage_gb=request.requirements.storage_gb,
                gpu_count=request.requirements.gpu_count,
                gpu_type=request.requirements.gpu_type,
                network_bandwidth_gbps=request.requirements.network_bandwidth_gbps,
                status=AllocationStatus.PROVISIONING,
                pricing_model=request.pricing_preferences[0] if request.pricing_preferences else PricingModel.ON_DEMAND,
                cost_per_hour=pricing.get("hourly_cost", 0),
                tags=request.tags,
                metadata=request.metadata
            )
            
            # Allocate through provider
            success, details = await provider.allocate(allocation)
            
            if success:
                allocation.status = AllocationStatus.ACTIVE
                allocation.activated_at = datetime.utcnow()
                allocation.access_details = details
                
                # Store allocation
                self.allocations[allocation.allocation_id] = allocation
                
                # Store in Consul for persistence
                await self.config_manager.set_config(
                    f"allocations/{allocation.allocation_id}",
                    allocation.to_dict()
                )
                
                return AllocationResponse(
                    success=True,
                    allocation=allocation,
                    message="Resources allocated successfully"
                )
            else:
                allocation.status = AllocationStatus.FAILED
                return AllocationResponse(
                    success=False,
                    message="Failed to allocate resources"
                )
                
        except Exception as e:
            logger.error(f"Error allocating resources: {e}")
            return AllocationResponse(
                success=False,
                message=str(e)
            )
    
    async def get_allocation(self, allocation_id: str) -> Optional[ResourceAllocation]:
        """Get allocation details"""
        # Check local cache first
        if allocation_id in self.allocations:
            return self.allocations[allocation_id]
        
        # Check Consul
        data = await self.config_manager.get_config(f"allocations/{allocation_id}")
        if data:
            allocation = ResourceAllocation(**data)
            self.allocations[allocation_id] = allocation
            return allocation
        
        return None
    
    async def modify_allocation(
        self,
        allocation_id: str,
        modifications: Dict[str, Any]
    ) -> bool:
        """Modify an existing allocation"""
        allocation = await self.get_allocation(allocation_id)
        if not allocation:
            return False
        
        # Get provider
        provider = None
        for name, p in self.provider_registry.get_all_providers().items():
            if p.provider_type == allocation.provider:
                provider = p
                break
        
        if not provider:
            logger.error(f"Provider {allocation.provider} not found")
            return False
        
        # Handle different modification types
        if "extend_hours" in modifications:
            # Extend allocation duration
            extension_hours = modifications["extend_hours"]
            new_expiry = allocation.expires_at + timedelta(hours=extension_hours)
            allocation.expires_at = new_expiry
            
            # Update cost
            additional_cost = allocation.cost_per_hour * extension_hours
            budget_ok, _ = self.budget_manager.check_budget(
                allocation.tenant_id,
                additional_cost
            )
            
            if not budget_ok:
                return False
        
        if "scale_to" in modifications:
            # Resize allocation
            new_requirements = ResourceRequirements(**modifications["scale_to"])
            success, details = await provider.resize(allocation, new_requirements)
            
            if success:
                # Update allocation with new resources
                allocation.cpu_cores = new_requirements.cpu_cores
                allocation.memory_gb = new_requirements.memory_gb
                allocation.storage_gb = new_requirements.storage_gb
                allocation.gpu_count = new_requirements.gpu_count
                allocation.status = AllocationStatus.ACTIVE
            else:
                return False
        
        # Save updated allocation
        allocation.last_modified_at = datetime.utcnow()
        await self.config_manager.set_config(
            f"allocations/{allocation_id}",
            allocation.to_dict()
        )
        
        return True
    
    async def deallocate_resources(self, allocation_id: str) -> bool:
        """Deallocate resources"""
        allocation = await self.get_allocation(allocation_id)
        if not allocation:
            return False
        
        # Get provider
        provider = None
        for name, p in self.provider_registry.get_all_providers().items():
            if p.provider_type == allocation.provider:
                provider = p
                break
        
        if not provider:
            logger.error(f"Provider {allocation.provider} not found")
            return False
        
        # Deallocate through provider
        success, message = await provider.deallocate(allocation)
        
        if success:
            allocation.status = AllocationStatus.TERMINATED
            allocation.last_modified_at = datetime.utcnow()
            
            # Update in Consul
            await self.config_manager.set_config(
                f"allocations/{allocation_id}",
                allocation.to_dict()
            )
            
            # Remove from local cache
            self.allocations.pop(allocation_id, None)
            
            return True
        else:
            logger.error(f"Failed to deallocate: {message}")
            return False
    
    async def get_allocation_metrics(self) -> Dict[str, Any]:
        """Get metrics for all allocations"""
        active_count = sum(1 for a in self.allocations.values() if a.is_active())
        total_cost = sum(
            float(a.calculate_cost()) 
            for a in self.allocations.values() 
            if a.is_active()
        )
        
        by_provider = {}
        for allocation in self.allocations.values():
            if allocation.is_active():
                provider = allocation.provider.value
                if provider not in by_provider:
                    by_provider[provider] = {
                        "count": 0,
                        "cost": 0,
                        "cpu_cores": 0,
                        "memory_gb": 0,
                        "gpu_count": 0
                    }
                
                by_provider[provider]["count"] += 1
                by_provider[provider]["cost"] += float(allocation.calculate_cost())
                by_provider[provider]["cpu_cores"] += allocation.cpu_cores
                by_provider[provider]["memory_gb"] += allocation.memory_gb
                by_provider[provider]["gpu_count"] += allocation.gpu_count
        
        return {
            "total_allocations": len(self.allocations),
            "active_allocations": active_count,
            "total_cost_usd": total_cost,
            "by_provider": by_provider,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    async def _estimate_cost(self, request: AllocationRequest) -> float:
        """Estimate cost for allocation request"""
        # Use cost calculator with average provider rates
        cost_analysis = self.cost_calculator.calculate_requirements_cost(
            request.requirements,
            ProviderType.AWS,  # Use AWS as baseline
            request.requirements.regions[0] if request.requirements.regions else "us-east-1",
            request.pricing_preferences[0] if request.pricing_preferences else PricingModel.ON_DEMAND,
            request.duration_hours
        )
        
        return float(cost_analysis.total_hourly_cost * request.duration_hours)
    
    async def _find_alternative_options(
        self,
        request: AllocationRequest
    ) -> List[Dict[str, Any]]:
        """Find alternative allocation options"""
        alternatives = []
        
        # Option 1: Split across multiple providers
        # Option 2: Use different regions
        # Option 3: Use spot/preemptible instances
        # Option 4: Reduce requirements slightly
        
        # This would be implemented based on specific business logic
        
        return alternatives
    
    async def _monitor_allocations(self):
        """Monitor active allocations"""
        while True:
            try:
                for allocation in list(self.allocations.values()):
                    if allocation.is_active():
                        # Get provider
                        provider = None
                        for name, p in self.provider_registry.get_all_providers().items():
                            if p.provider_type == allocation.provider:
                                provider = p
                                break
                        
                        if provider:
                            # Check allocation status
                            status = await provider.get_status(allocation)
                            
                            # Update health status
                            allocation.health_status = status.get("health", "unknown")
                            
                            # Handle unhealthy allocations
                            if status.get("health") == "unhealthy":
                                logger.warning(
                                    f"Allocation {allocation.allocation_id} is unhealthy"
                                )
                
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring allocations: {e}")
                await asyncio.sleep(60)
    
    async def _cleanup_expired(self):
        """Clean up expired allocations"""
        while True:
            try:
                for allocation_id, allocation in list(self.allocations.items()):
                    if allocation.is_expired() and allocation.status != AllocationStatus.TERMINATED:
                        logger.info(f"Cleaning up expired allocation: {allocation_id}")
                        await self.deallocate_resources(allocation_id)
                
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error cleaning up allocations: {e}")
                await asyncio.sleep(300)
    
    async def shutdown(self):
        """Shutdown the service"""
        # Cancel background tasks
        if self._monitor_task:
            self._monitor_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
        
        # Stop provider health monitoring
        await self.provider_registry.stop_health_monitoring()
        
        # Wait for tasks to complete
        if self._monitor_task:
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        if self._cleanup_task:
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass 