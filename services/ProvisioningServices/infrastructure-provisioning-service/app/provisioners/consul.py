"""
Consul Provisioner

Provisions Consul KV stores and configurations for tenants.
"""
import logging
from typing import Dict, Any
import uuid
from datetime import datetime

import consul

from platformq_resource_common import (
    ResourceType, InfrastructureResource, ResourceStatus,
    IResourceProvisioner
)
from ..core.config import Settings

logger = logging.getLogger(__name__)


class ConsulProvisioner(IResourceProvisioner):
    """Provisions Consul resources"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.consul_client = None
    
    async def initialize(self):
        """Initialize Consul connection"""
        try:
            self.consul_client = consul.Consul(
                host=self.settings.consul_host,
                port=self.settings.consul_port,
                token=self.settings.consul_token if self.settings.consul_token else None
            )
            
            # Test connection
            self.consul_client.agent.self()
            
            logger.info("Consul provisioner initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Consul provisioner: {e}")
            raise
    
    async def shutdown(self):
        """Shutdown Consul connection"""
        # Consul client doesn't need explicit shutdown
        pass
    
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision Consul KV store for tenant"""
        kv_prefix = f"tenants/{tenant_id}"
        
        try:
            # Create tenant KV structure
            await self._create_kv_structure(kv_prefix, tenant_name, metadata)
            
            # Set up ACL policies if enabled
            policy_id = None
            if metadata.get('enable_acl', False):
                policy_id = await self._create_acl_policy(tenant_id, kv_prefix)
            
            # Create resource object
            resource = InfrastructureResource(
                resource_id=str(uuid.uuid4()),
                resource_type=ResourceType.CONSUL,
                resource_name=kv_prefix,
                tenant_id=tenant_id,
                status=ResourceStatus.ACTIVE,
                endpoint=f"{self.settings.consul_host}:{self.settings.consul_port}",
                configuration={
                    "kv_prefix": kv_prefix,
                    "acl_policy_id": policy_id,
                    "datacenter": self.consul_client.agent.self()['Config']['Datacenter']
                },
                created_at=datetime.utcnow()
            )
            
            logger.info(f"Successfully provisioned Consul for tenant {tenant_id}")
            return resource
            
        except Exception as e:
            logger.error(f"Failed to provision Consul for tenant {tenant_id}: {e}")
            raise
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision Consul KV store"""
        try:
            # Delete all keys under the tenant prefix
            index, keys = self.consul_client.kv.get(resource_name, recurse=True)
            if keys:
                for key in keys:
                    self.consul_client.kv.delete(key['Key'])
                    logger.info(f"Deleted key: {key['Key']}")
            
            # Delete ACL policy if exists
            # Note: This would require policy ID tracking
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to deprovision Consul KV store {resource_name}: {e}")
            return False
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate Consul provisioning"""
        kv_prefix = f"tenants/{tenant_id}"
        
        try:
            # Check if tenant config exists
            index, data = self.consul_client.kv.get(f"{kv_prefix}/config")
            return data is not None
            
        except Exception as e:
            logger.error(f"Failed to validate Consul for tenant {tenant_id}: {e}")
            return False
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.CONSUL
    
    async def _create_kv_structure(self, kv_prefix: str, tenant_name: str, metadata: Dict[str, Any]):
        """Create KV structure for tenant"""
        # Create base configuration
        config = {
            "tenant_name": tenant_name,
            "created_at": datetime.utcnow().isoformat(),
            "tier": metadata.get('tier', 'starter'),
            "features": metadata.get('features', {})
        }
        
        # Store tenant configuration
        self.consul_client.kv.put(f"{kv_prefix}/config", str(config))
        
        # Create service discovery paths
        self.consul_client.kv.put(f"{kv_prefix}/services/", "")
        
        # Create feature flags path
        self.consul_client.kv.put(f"{kv_prefix}/features/", "")
        
        # Create settings path
        self.consul_client.kv.put(f"{kv_prefix}/settings/", "")
        
        logger.info(f"Created KV structure for tenant: {kv_prefix}")
    
    async def _create_acl_policy(self, tenant_id: str, kv_prefix: str) -> str:
        """Create ACL policy for tenant"""
        policy_name = f"tenant-{tenant_id}-policy"
        
        policy_rules = f'''
            key_prefix "{kv_prefix}/" {{
                policy = "write"
            }}
            
            service_prefix "" {{
                policy = "read"
            }}
            
            node_prefix "" {{
                policy = "read"
            }}
        '''
        
        # Note: ACL policy creation would require admin token
        # This is a placeholder for the functionality
        logger.info(f"Would create ACL policy: {policy_name}")
        
        return policy_name 