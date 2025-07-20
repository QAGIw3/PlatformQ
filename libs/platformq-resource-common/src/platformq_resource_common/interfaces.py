"""
Common interfaces for the Platform Q resource management ecosystem.
"""
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from .models import (
    ResourceType, ComputeResourceType, ProviderType, ResourceStatus,
    AllocationStrategy, PricingModel, TenantTier, ScalingAction,
    ResourceSpec, ResourceRequirements, ResourceAllocation,
    ResourceMetrics, ClusterMetrics, ResourceQuota, ScalingPolicy,
    ScalingDecision, InfrastructureResource, ProvisioningRequest,
    ProvisioningResult, ProvisioningStatus, AllocationRequest,
    AllocationResponse, ProviderCapabilities
)


# Resource Provider Interfaces
class IResourceProvider(ABC):
    """Interface for cloud/infrastructure resource providers"""
    
    @abstractmethod
    async def get_capabilities(self) -> ProviderCapabilities:
        """Get provider capabilities and supported features"""
        pass
    
    @abstractmethod
    async def check_availability(
        self,
        requirements: ResourceRequirements,
        region: Optional[str] = None
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """Check if resources are available"""
        pass
    
    @abstractmethod
    async def get_pricing(
        self,
        requirements: ResourceRequirements,
        region: Optional[str] = None,
        instance_type: Optional[str] = None,
        pricing_model: PricingModel = PricingModel.ON_DEMAND
    ) -> Dict[str, Any]:
        """Get pricing information for resources"""
        pass
    
    @abstractmethod
    async def allocate(
        self,
        allocation: ResourceAllocation
    ) -> Tuple[bool, Dict[str, Any]]:
        """Allocate resources"""
        pass
    
    @abstractmethod
    async def deallocate(
        self,
        allocation: ResourceAllocation
    ) -> Tuple[bool, str]:
        """Deallocate resources"""
        pass
    
    @abstractmethod
    async def get_status(
        self,
        allocation: ResourceAllocation
    ) -> Dict[str, Any]:
        """Get allocation status"""
        pass
    
    @abstractmethod
    async def resize(
        self,
        allocation: ResourceAllocation,
        new_requirements: ResourceRequirements
    ) -> Tuple[bool, Dict[str, Any]]:
        """Resize allocated resources"""
        pass


# Resource Allocation Interfaces
class IAllocationService(ABC):
    """Interface for compute allocation service"""
    
    @abstractmethod
    async def allocate_resources(
        self,
        request: AllocationRequest
    ) -> AllocationResponse:
        """Allocate compute resources"""
        pass
    
    @abstractmethod
    async def get_allocation(self, allocation_id: str) -> Optional[ResourceAllocation]:
        """Get allocation details"""
        pass
    
    @abstractmethod
    async def modify_allocation(
        self,
        allocation_id: str,
        modifications: Dict[str, Any]
    ) -> bool:
        """Modify existing allocation"""
        pass
    
    @abstractmethod
    async def deallocate_resources(self, allocation_id: str) -> bool:
        """Deallocate resources"""
        pass
    
    @abstractmethod
    async def get_allocation_metrics(self) -> Dict[str, Any]:
        """Get allocation metrics"""
        pass


# Resource Monitoring Interfaces
class IResourceMonitor(ABC):
    """Interface for resource monitoring"""
    
    @abstractmethod
    async def get_service_metrics(
        self,
        service_name: str,
        namespace: str = "platformq"
    ) -> Optional[ResourceMetrics]:
        """Get current metrics for a service"""
        pass
    
    @abstractmethod
    async def get_cluster_metrics(self) -> Optional[ClusterMetrics]:
        """Get cluster-wide metrics"""
        pass
    
    @abstractmethod
    async def get_historical_metrics(
        self,
        service_name: str,
        namespace: str,
        start_time: datetime,
        end_time: datetime
    ) -> List[ResourceMetrics]:
        """Get historical metrics"""
        pass
    
    @abstractmethod
    async def detect_anomalies(
        self,
        metrics: ResourceMetrics
    ) -> List[Dict[str, Any]]:
        """Detect anomalies in metrics"""
        pass


# Scaling Interfaces
class IScalingEngine(ABC):
    """Interface for resource scaling engine"""
    
    @abstractmethod
    async def evaluate_scaling(
        self,
        service_name: str,
        policy: ScalingPolicy,
        metrics: ResourceMetrics
    ) -> Optional[ScalingDecision]:
        """Evaluate if scaling is needed"""
        pass
    
    @abstractmethod
    async def apply_scaling_decision(self, decision: ScalingDecision) -> bool:
        """Apply a scaling decision"""
        pass
    
    @abstractmethod
    async def get_scaling_policy(self, service_name: str) -> Optional[ScalingPolicy]:
        """Get scaling policy for service"""
        pass
    
    @abstractmethod
    async def update_scaling_policy(self, policy: ScalingPolicy) -> bool:
        """Update scaling policy"""
        pass
    
    @abstractmethod
    async def get_recent_decisions(
        self,
        service_name: Optional[str] = None,
        hours: int = 24
    ) -> List[ScalingDecision]:
        """Get recent scaling decisions"""
        pass


class IPredictiveScaler(ABC):
    """Interface for predictive scaling"""
    
    @abstractmethod
    async def predict_load(
        self,
        service_name: str,
        horizon_minutes: int = 30
    ) -> Optional[float]:
        """Predict future load"""
        pass
    
    @abstractmethod
    async def train_models(self) -> None:
        """Train predictive models"""
        pass


# Quota Management Interfaces
class IQuotaManager(ABC):
    """Interface for quota management"""
    
    @abstractmethod
    async def check_quota(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        requested_amount: float
    ) -> Tuple[bool, Optional[str]]:
        """Check if quota allows resource usage"""
        pass
    
    @abstractmethod
    async def get_current_usage(
        self,
        tenant_id: str,
        resource_type: ResourceType
    ) -> float:
        """Get current resource usage"""
        pass
    
    @abstractmethod
    async def update_usage(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        delta: float
    ) -> None:
        """Update resource usage"""
        pass
    
    @abstractmethod
    async def set_quota(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        limit: float,
        period: Optional[str] = None
    ) -> ResourceQuota:
        """Set resource quota"""
        pass
    
    @abstractmethod
    async def get_quota_status(
        self,
        tenant_id: str
    ) -> Dict[str, Any]:
        """Get quota status for all resources"""
        pass


# Provisioning Interfaces
class IResourceProvisioner(ABC):
    """Interface for infrastructure resource provisioners"""
    
    @abstractmethod
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision a resource"""
        pass
    
    @abstractmethod
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision a resource"""
        pass
    
    @abstractmethod
    async def validate(self, tenant_id: str) -> bool:
        """Validate provisioned resource"""
        pass
    
    @abstractmethod
    def get_resource_type(self) -> ResourceType:
        """Get the type of resource this provisioner handles"""
        pass


class IProvisioningOrchestrator(ABC):
    """Interface for provisioning orchestration"""
    
    @abstractmethod
    async def provision_tenant(
        self,
        request: ProvisioningRequest
    ) -> ProvisioningResult:
        """Provision all resources for a tenant"""
        pass
    
    @abstractmethod
    async def deprovision_tenant(self, tenant_id: str) -> ProvisioningResult:
        """Deprovision all tenant resources"""
        pass
    
    @abstractmethod
    async def get_provisioning_status(
        self,
        request_id: str
    ) -> Optional[ProvisioningResult]:
        """Get provisioning status"""
        pass
    
    @abstractmethod
    async def retry_failed_resources(
        self,
        request_id: str
    ) -> ProvisioningResult:
        """Retry failed resource provisioning"""
        pass


# Repository Interfaces
class IProvisioningRepository(ABC):
    """Interface for provisioning data persistence"""
    
    @abstractmethod
    async def create_request(self, request: ProvisioningRequest) -> str:
        """Create provisioning request"""
        pass
    
    @abstractmethod
    async def update_request_status(
        self,
        request_id: str,
        status: ProvisioningStatus
    ) -> bool:
        """Update request status"""
        pass
    
    @abstractmethod
    async def add_provisioned_resource(
        self,
        request_id: str,
        resource: InfrastructureResource
    ) -> bool:
        """Add provisioned resource to request"""
        pass
    
    @abstractmethod
    async def get_request(self, request_id: str) -> Optional[ProvisioningRequest]:
        """Get provisioning request"""
        pass
    
    @abstractmethod
    async def get_tenant_resources(
        self,
        tenant_id: str
    ) -> List[InfrastructureResource]:
        """Get all resources for a tenant"""
        pass


class IResourceRepository(ABC):
    """Interface for resource data persistence"""
    
    @abstractmethod
    async def save_allocation(self, allocation: ResourceAllocation) -> str:
        """Save resource allocation"""
        pass
    
    @abstractmethod
    async def get_allocation(self, allocation_id: str) -> Optional[ResourceAllocation]:
        """Get resource allocation"""
        pass
    
    @abstractmethod
    async def update_allocation_status(
        self,
        allocation_id: str,
        status: ResourceStatus
    ) -> bool:
        """Update allocation status"""
        pass
    
    @abstractmethod
    async def get_tenant_allocations(
        self,
        tenant_id: str,
        active_only: bool = True
    ) -> List[ResourceAllocation]:
        """Get all allocations for a tenant"""
        pass
    
    @abstractmethod
    async def save_metrics(
        self,
        metrics: ResourceMetrics
    ) -> bool:
        """Save resource metrics"""
        pass
    
    @abstractmethod
    async def get_metrics_history(
        self,
        service_name: str,
        namespace: str,
        hours: int = 24
    ) -> List[ResourceMetrics]:
        """Get metrics history"""
        pass


# Service Client Interfaces
class IServiceClient(ABC):
    """Base interface for service clients"""
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check if service is healthy"""
        pass
    
    @abstractmethod
    async def close(self) -> None:
        """Close client connections"""
        pass


class IComputeAllocationClient(IServiceClient):
    """Client interface for compute allocation service"""
    
    @abstractmethod
    async def allocate_resources(
        self,
        request: AllocationRequest
    ) -> AllocationResponse:
        """Allocate compute resources"""
        pass
    
    @abstractmethod
    async def get_allocation(self, allocation_id: str) -> Optional[ResourceAllocation]:
        """Get allocation details"""
        pass
    
    @abstractmethod
    async def deallocate_resources(self, allocation_id: str) -> bool:
        """Deallocate resources"""
        pass


class IQuotaServiceClient(IServiceClient):
    """Client interface for quota management service"""
    
    @abstractmethod
    async def check_quota(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        requested_amount: float
    ) -> Tuple[bool, Optional[str]]:
        """Check quota availability"""
        pass
    
    @abstractmethod
    async def update_usage(
        self,
        tenant_id: str,
        resource_type: ResourceType,
        delta: float,
        operation: str = "increment"
    ) -> bool:
        """Update resource usage"""
        pass


class IMonitoringServiceClient(IServiceClient):
    """Client interface for resource monitoring service"""
    
    @abstractmethod
    async def get_service_metrics(
        self,
        service_name: str,
        namespace: str = "platformq"
    ) -> Optional[ResourceMetrics]:
        """Get service metrics"""
        pass
    
    @abstractmethod
    async def get_cluster_metrics(self) -> Optional[ClusterMetrics]:
        """Get cluster metrics"""
        pass


class IScalingServiceClient(IServiceClient):
    """Client interface for resource scaling service"""
    
    @abstractmethod
    async def trigger_scaling_evaluation(
        self,
        service_name: str,
        namespace: str = "platformq"
    ) -> bool:
        """Trigger scaling evaluation"""
        pass
    
    @abstractmethod
    async def get_scaling_policy(self, service_name: str) -> Optional[ScalingPolicy]:
        """Get scaling policy"""
        pass 