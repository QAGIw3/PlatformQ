"""OpenProject Provisioner"""

import logging
from typing import Dict, Any
from datetime import datetime

from platformq_provisioning_common import (
    IResourceProvisioner,
    InfrastructureResource,
    ResourceType,
    ProvisioningStatus
)

from ..config import Settings

logger = logging.getLogger(__name__)


class OpenProjectProvisioner(IResourceProvisioner):
    """Provisions OpenProject projects for tenants"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
    
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision OpenProject project for tenant"""
        resource_name = f"tenant-{tenant_id}-project"
        
        resource = InfrastructureResource(
            resource_type=ResourceType.OPENPROJECT_PROJECT,
            resource_name=resource_name,
            tenant_id=tenant_id,
            status=ProvisioningStatus.COMPLETED,
            provisioned_at=datetime.utcnow(),
            provisioned_by="openproject-provisioner"
        )
        
        # TODO: Implement actual OpenProject project creation
        logger.info(f"Would provision OpenProject project: {resource_name}")
        
        return resource
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision OpenProject project"""
        logger.info(f"Would deprovision OpenProject project: {resource_name}")
        return True
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate if provisioning is possible"""
        return True
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.OPENPROJECT_PROJECT 