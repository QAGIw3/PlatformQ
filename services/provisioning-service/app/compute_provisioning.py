"""
Compute Provisioning Module

Handles compute resource provisioning requests from services and integrates
with the derivatives engine for partner capacity allocation.
"""

import logging
import httpx
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from decimal import Decimal
import uuid
import asyncio

from platformq_compute_common.models import (
    ComputeResourceType,
    ProviderType,
    AllocationStatus,
    ResourceRequirements,
    ResourceAllocation,
    AllocationRequest,
    AllocationResponse,
    PricingModel
)
from platformq_compute_common.providers import ProviderRegistry
from platformq_compute_common.cost import CostCalculator, BudgetManager

from .core.config_manager import ConfigManager

logger = logging.getLogger(__name__)


class ComputeProvisioningManager:
    """Manages compute resource provisioning using shared compute framework"""
    
    def __init__(
        self,
        config_manager: ConfigManager,
        provider_registry: ProviderRegistry,
        derivatives_engine_url: str = "http://derivatives-engine-service:8000",
        ignite_client = None,
        pulsar_publisher = None
    ):
        self.config_manager = config_manager
        self.provider_registry = provider_registry
        self.derivatives_engine_url = derivatives_engine_url
        self.ignite_client = ignite_client
        self.pulsar_publisher = pulsar_publisher
        
        # Cost management
        self.cost_calculator = CostCalculator()
        self.budget_manager = BudgetManager()
        
        # HTTP client for derivatives engine
        self.http_client = httpx.AsyncClient(
            base_url=derivatives_engine_url,
            timeout=30.0
        )
        
        # Track active allocations
        self.active_allocations: Dict[str, ResourceAllocation] = {}
        
    async def initialize(self):
        """Initialize the provisioning manager"""
        # Load provider configurations from Consul
        providers_config = await self.config_manager.get_config("compute_providers", {})
        
        # Initialize providers with credentials from Vault
        for provider_name, config in providers_config.items():
            if config.get("enabled", False):
                # Get credentials from Vault
                credentials = await self.config_manager.get_provider_credentials(provider_name)
                config.update(credentials)
                
                # Create and register provider
                # This would instantiate actual provider implementations
                logger.info(f"Initialized provider: {provider_name}")
        
        # Start provider health monitoring
        await self.provider_registry.start_health_monitoring()
        
        # Watch for configuration changes
        await self.config_manager.watch_config(
            "compute_providers",
            self._handle_provider_config_change
        )
        
    async def _handle_provider_config_change(self, new_config: Dict[str, Any]):
        """Handle provider configuration changes"""
        logger.info("Provider configuration changed, reloading...")
        # Re-initialize providers with new config
        # This would be implemented based on specific requirements
        
    async def provision_compute(
        self,
        request: AllocationRequest
    ) -> AllocationResponse:
        """Provision compute resources through derivatives engine"""
        try:
            # Validate request
            errors = request.requirements.validate()
            if errors:
                return AllocationResponse(
                    success=False,
                    message=f"Invalid requirements: {', '.join(errors)}"
                )
            
            # Check budget
            cost_analysis = self.cost_calculator.calculate_requirements_cost(
                request.requirements,
                ProviderType.AWS,  # Default for estimation
                "us-east-1",
                request.pricing_preferences[0] if request.pricing_preferences else PricingModel.ON_DEMAND,
                request.duration_hours
            )
            
            budget_ok, budget_msg = self.budget_manager.check_budget(
                request.tenant_id,
                cost_analysis.total_hourly_cost * Decimal(str(request.duration_hours))
            )
            
            if not budget_ok:
                return AllocationResponse(
                    success=False,
                    message=budget_msg
                )
            
            # First, request capacity from cross-service coordinator
            capacity_response = await self._request_capacity_allocation(request)
            
            if not capacity_response or capacity_response.get("status") != "allocated":
                return AllocationResponse(
                    success=False,
                    message="Failed to allocate capacity from derivatives engine"
                )
                
            allocation_id = capacity_response["allocation_id"]
            provider_name = capacity_response.get("provider")
            
            # Find best provider if not specified
            if not provider_name:
                result = await self.provider_registry.find_best_provider(
                    request.requirements,
                    request.strategy.value.lower()
                )
                
                if not result:
                    return AllocationResponse(
                        success=False,
                        message="No suitable provider found"
                    )
                
                provider_name, provider = result
            else:
                provider = self.provider_registry.get_provider(provider_name)
                
            if not provider:
                return AllocationResponse(
                    success=False,
                    message=f"Provider {provider_name} not available"
                )
            
            # Create allocation record
            allocation = ResourceAllocation(
                allocation_id=allocation_id,
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
                status=AllocationStatus.PROVISIONING,
                cost_per_hour=cost_analysis.total_hourly_cost,
                pricing_model=request.pricing_preferences[0] if request.pricing_preferences else PricingModel.ON_DEMAND,
                expires_at=datetime.utcnow() + timedelta(hours=request.duration_hours),
                tags=request.tags,
                metadata=request.metadata
            )
            
            # Provision through provider
            success, details = await provider.allocate(allocation)
            
            if success:
                allocation.status = AllocationStatus.ACTIVE
                allocation.activated_at = datetime.utcnow()
                allocation.access_details = details
                
                # Store active allocation
                self.active_allocations[allocation.allocation_id] = allocation
                
                # Publish provisioning event
                if self.pulsar_publisher:
                    await self._publish_provisioning_event(allocation, "provisioned")
                    
                return AllocationResponse(
                    success=True,
                    allocation=allocation,
                    message="Resources provisioned successfully"
                )
            else:
                allocation.status = AllocationStatus.FAILED
                return AllocationResponse(
                    success=False,
                    message="Failed to provision resources"
                )
                
        except Exception as e:
            logger.error(f"Error provisioning compute: {e}")
            return AllocationResponse(
                success=False,
                message=str(e)
            )
            
    async def get_provisioning_status(
        self,
        allocation_id: str
    ) -> Dict[str, Any]:
        """Get status of provisioning request"""
        if allocation_id not in self.active_allocations:
            return {"status": "not_found"}
            
        allocation = self.active_allocations[allocation_id]
        
        # Get provider
        provider = None
        for name, p in self.provider_registry.get_all_providers().items():
            if p.provider_type == allocation.provider:
                provider = p
                break
                
        if provider:
            status = await provider.get_status(allocation)
            return {
                "allocation": allocation.to_dict(),
                "provider_status": status
            }
        else:
            return {
                "allocation": allocation.to_dict(),
                "provider_status": {"error": "Provider not found"}
            }
            
    async def terminate_provision(
        self,
        allocation_id: str
    ) -> bool:
        """Terminate provisioned resources"""
        if allocation_id not in self.active_allocations:
            logger.warning(f"Allocation {allocation_id} not found")
            return False
            
        allocation = self.active_allocations[allocation_id]
        
        try:
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
                
                # Remove from active allocations
                del self.active_allocations[allocation_id]
                
                # Publish termination event
                if self.pulsar_publisher:
                    await self._publish_provisioning_event(allocation, "terminated")
                    
                return True
            else:
                logger.error(f"Failed to terminate allocation: {message}")
                return False
                
        except Exception as e:
            logger.error(f"Error terminating provision: {e}")
            return False
            
    async def get_available_capacity(
        self,
        resource_type: ComputeResourceType,
        region: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get available capacity from all providers"""
        capacity = {
            "total_available": 0,
            "by_provider": {},
            "by_region": {}
        }
        
        # Query each healthy provider
        for name, provider in self.provider_registry.get_healthy_providers().items():
            try:
                # Create sample requirements
                requirements = ResourceRequirements()
                if resource_type == ComputeResourceType.GPU:
                    requirements.gpu_count = 1
                elif resource_type == ComputeResourceType.CPU:
                    requirements.cpu_cores = 1
                
                available, instance_type, details = await provider.check_availability(
                    requirements,
                    region
                )
                
                if available:
                    capacity["by_provider"][name] = details
                    
                    # Aggregate by region
                    provider_region = details.get("region", "unknown")
                    if provider_region not in capacity["by_region"]:
                        capacity["by_region"][provider_region] = 0
                    capacity["by_region"][provider_region] += details.get("available_count", 0)
                    capacity["total_available"] += details.get("available_count", 0)
                    
            except Exception as e:
                logger.error(f"Error checking capacity for provider {name}: {e}")
                
        return capacity
            
    # Private methods
    async def _request_capacity_allocation(
        self,
        request: AllocationRequest
    ) -> Optional[Dict[str, Any]]:
        """Request capacity allocation from derivatives engine"""
        try:
            # Prepare allocation request for derivatives engine
            derivatives_request = {
                "service_type": request.workload_type,
                "tenant_id": request.tenant_id,
                "resource_type": ComputeResourceType.GPU.value if request.requirements.gpu_count > 0 else ComputeResourceType.CPU.value,
                "quantity": str(request.requirements.gpu_count or request.requirements.cpu_cores),
                "duration_hours": request.duration_hours,
                "start_time": (request.start_time or datetime.utcnow()).isoformat(),
                "priority": 5,  # Default medium priority
                "flexibility_hours": 2,  # Allow 2 hour flexibility
                "metadata": request.metadata or {}
            }
            
            # Call cross-service capacity coordinator
            response = await self.http_client.post(
                "/api/v1/capacity/request",
                json=derivatives_request
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # Wait for allocation if pending
                if result.get("status") == "pending":
                    # Poll for allocation result
                    for _ in range(30):  # Wait up to 5 minutes
                        await asyncio.sleep(10)
                        
                        status_response = await self.http_client.get(
                            f"/api/v1/capacity/allocation/{result['request_id']}"
                        )
                        
                        if status_response.status_code == 200:
                            status_data = status_response.json()
                            if status_data.get("status") == "allocated":
                                return status_data
                                
                return result
            else:
                logger.error(f"Capacity allocation failed: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error requesting capacity allocation: {e}")
            return None
            
    async def _publish_provisioning_event(self, allocation: ResourceAllocation, event_type: str):
        """Publish provisioning event"""
        try:
            event = {
                "event_type": f"compute_{event_type}",
                "allocation_id": allocation.allocation_id,
                "tenant_id": allocation.tenant_id,
                "workload_id": allocation.workload_id,
                "workload_type": allocation.workload_type,
                "provider": allocation.provider.value,
                "region": allocation.region,
                "status": allocation.status.value,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            await self.pulsar_publisher.publish(
                "persistent://platformq/provisioning/compute-events",
                event
            )
        except Exception as e:
            logger.error(f"Failed to publish provisioning event: {e}")
            
    async def close(self):
        """Close HTTP client and clean up"""
        await self.http_client.aclose()
        await self.provider_registry.stop_health_monitoring() 