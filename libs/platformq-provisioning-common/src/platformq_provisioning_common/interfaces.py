"""Common provisioning interfaces"""

from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any
from .models import (
    ProvisioningRequest,
    ProvisioningResult,
    InfrastructureResource,
    ResourceType,
    ProvisioningStatus
)


class IResourceProvisioner(ABC):
    """Interface for resource-specific provisioners"""
    
    @abstractmethod
    async def provision(self, tenant_id: str, tenant_name: str, 
                       metadata: Dict[str, Any]) -> InfrastructureResource:
        """Provision a specific resource for a tenant"""
        pass
    
    @abstractmethod
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision a specific resource"""
        pass
    
    @abstractmethod
    async def validate(self, tenant_id: str) -> bool:
        """Validate if provisioning is possible"""
        pass
    
    @abstractmethod
    def get_resource_type(self) -> ResourceType:
        """Get the type of resource this provisioner handles"""
        pass


class IProvisioningOrchestrator(ABC):
    """Interface for provisioning orchestration"""
    
    @abstractmethod
    async def provision_tenant(self, request: ProvisioningRequest) -> ProvisioningResult:
        """Orchestrate provisioning of all resources for a tenant"""
        pass
    
    @abstractmethod
    async def deprovision_tenant(self, tenant_id: str) -> ProvisioningResult:
        """Orchestrate deprovisioning of all resources for a tenant"""
        pass
    
    @abstractmethod
    async def get_provisioning_status(self, request_id: str) -> Optional[ProvisioningResult]:
        """Get the status of a provisioning request"""
        pass
    
    @abstractmethod
    async def retry_failed_resources(self, request_id: str) -> ProvisioningResult:
        """Retry provisioning of failed resources"""
        pass


class IProvisioningRepository(ABC):
    """Interface for provisioning data persistence"""
    
    @abstractmethod
    async def create_request(self, request: ProvisioningRequest) -> str:
        """Store a new provisioning request"""
        pass
    
    @abstractmethod
    async def update_request_status(self, request_id: str, status: ProvisioningStatus) -> bool:
        """Update the status of a provisioning request"""
        pass
    
    @abstractmethod
    async def add_provisioned_resource(self, request_id: str, 
                                     resource: InfrastructureResource) -> bool:
        """Add a provisioned resource to a request"""
        pass
    
    @abstractmethod
    async def get_request(self, request_id: str) -> Optional[ProvisioningRequest]:
        """Get a provisioning request by ID"""
        pass
    
    @abstractmethod
    async def get_tenant_resources(self, tenant_id: str) -> List[InfrastructureResource]:
        """Get all resources for a tenant"""
        pass 