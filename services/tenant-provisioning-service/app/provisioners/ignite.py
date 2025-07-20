"""Ignite Provisioner"""

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


class IgniteProvisioner(IResourceProvisioner):
    """Provisions Ignite caches for tenants"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
    
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision Ignite caches for tenant"""
        resource_name = f"ignite-{tenant_id}"
        
        resource = InfrastructureResource(
            resource_type=ResourceType.IGNITE_CACHE,
            resource_name=resource_name,
            tenant_id=tenant_id,
            status=ProvisioningStatus.COMPLETED,
            provisioned_at=datetime.utcnow(),
            provisioned_by="ignite-provisioner"
        )
        
        # TODO: Implement actual Ignite cache creation
        logger.info(f"Would provision Ignite caches: {resource_name}")
        
        return resource
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision Ignite caches"""
        logger.info(f"Would deprovision Ignite caches: {resource_name}")
        return True
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate if provisioning is possible"""
        return True
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.IGNITE_CACHE 