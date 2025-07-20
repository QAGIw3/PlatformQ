"""
HashiCorp Vault Provisioner

Provisions Vault secret engines and policies for tenants.
"""
import logging
from typing import Dict, Any
import uuid
from datetime import datetime

import hvac

from platformq_resource_common import (
    ResourceType, InfrastructureResource, ResourceStatus,
    IResourceProvisioner
)
from ..core.config import Settings

logger = logging.getLogger(__name__)


class VaultProvisioner(IResourceProvisioner):
    """Provisions Vault resources"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.vault_client = None
    
    async def initialize(self):
        """Initialize Vault connection"""
        try:
            self.vault_client = hvac.Client(
                url=self.settings.vault_addr,
                token=self.settings.vault_token
            )
            
            # Test connection
            if not self.vault_client.is_authenticated():
                raise Exception("Vault authentication failed")
            
            logger.info("Vault provisioner initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Vault provisioner: {e}")
            raise
    
    async def shutdown(self):
        """Shutdown Vault connection"""
        # Vault client doesn't need explicit shutdown
        pass
    
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision Vault secret engine and policies for tenant"""
        secret_path = f"tenants/{tenant_id}"
        
        try:
            # Enable KV secret engine for tenant
            await self._enable_secret_engine(tenant_id)
            
            # Create tenant policy
            policy_name = await self._create_tenant_policy(tenant_id, secret_path)
            
            # Create initial secrets structure
            await self._create_initial_secrets(secret_path, tenant_name, metadata)
            
            # Create app role for tenant if requested
            app_role_id = None
            if metadata.get('create_app_role', True):
                app_role_id = await self._create_app_role(tenant_id, policy_name)
            
            # Create resource object
            resource = InfrastructureResource(
                resource_id=str(uuid.uuid4()),
                resource_type=ResourceType.VAULT,
                resource_name=secret_path,
                tenant_id=tenant_id,
                status=ResourceStatus.ACTIVE,
                endpoint=self.settings.vault_addr,
                configuration={
                    "secret_path": secret_path,
                    "mount_path": f"tenant-{tenant_id}",
                    "policy_name": policy_name,
                    "app_role_id": app_role_id
                },
                created_at=datetime.utcnow()
            )
            
            logger.info(f"Successfully provisioned Vault for tenant {tenant_id}")
            return resource
            
        except Exception as e:
            logger.error(f"Failed to provision Vault for tenant {tenant_id}: {e}")
            raise
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision Vault resources"""
        try:
            mount_path = f"tenant-{tenant_id}"
            policy_name = f"tenant-{tenant_id}-policy"
            
            # Disable secret engine
            try:
                self.vault_client.sys.disable_secrets_engine(mount_path)
                logger.info(f"Disabled secret engine: {mount_path}")
            except:
                pass
            
            # Delete policy
            try:
                self.vault_client.sys.delete_policy(policy_name)
                logger.info(f"Deleted policy: {policy_name}")
            except:
                pass
            
            # Delete app role
            try:
                self.vault_client.auth.approle.delete_role(f"tenant-{tenant_id}")
                logger.info(f"Deleted app role: tenant-{tenant_id}")
            except:
                pass
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to deprovision Vault resources {resource_name}: {e}")
            return False
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate Vault provisioning"""
        mount_path = f"tenant-{tenant_id}"
        
        try:
            # Check if secret engine is mounted
            mounts = self.vault_client.sys.list_mounted_secrets_engines()
            return f"{mount_path}/" in mounts['data']
            
        except Exception as e:
            logger.error(f"Failed to validate Vault for tenant {tenant_id}: {e}")
            return False
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.VAULT
    
    async def _enable_secret_engine(self, tenant_id: str):
        """Enable KV v2 secret engine for tenant"""
        mount_path = f"tenant-{tenant_id}"
        
        try:
            self.vault_client.sys.enable_secrets_engine(
                backend_type='kv-v2',
                path=mount_path,
                config={
                    'default_lease_ttl': '24h',
                    'max_lease_ttl': '720h',
                    'force_no_cache': False
                }
            )
            logger.info(f"Enabled KV v2 secret engine at: {mount_path}")
        except Exception as e:
            if "already in use" in str(e):
                logger.info(f"Secret engine already exists at: {mount_path}")
            else:
                raise
    
    async def _create_tenant_policy(self, tenant_id: str, secret_path: str) -> str:
        """Create Vault policy for tenant"""
        policy_name = f"tenant-{tenant_id}-policy"
        mount_path = f"tenant-{tenant_id}"
        
        policy = f'''
            # Allow full access to tenant's own secrets
            path "{mount_path}/*" {{
                capabilities = ["create", "read", "update", "delete", "list"]
            }}
            
            # Allow reading tenant's own metadata
            path "{mount_path}/metadata/*" {{
                capabilities = ["read", "list"]
            }}
            
            # Allow managing versions
            path "{mount_path}/data/*" {{
                capabilities = ["create", "read", "update", "delete"]
            }}
            
            # Allow deleting versions
            path "{mount_path}/delete/*" {{
                capabilities = ["update"]
            }}
            
            # Allow destroying versions
            path "{mount_path}/destroy/*" {{
                capabilities = ["update"]
            }}
            
            # Allow listing
            path "{mount_path}/" {{
                capabilities = ["list"]
            }}
        '''
        
        self.vault_client.sys.create_or_update_policy(
            name=policy_name,
            policy=policy
        )
        
        logger.info(f"Created policy: {policy_name}")
        return policy_name
    
    async def _create_initial_secrets(self, secret_path: str, tenant_name: str, metadata: Dict[str, Any]):
        """Create initial secrets structure"""
        mount_path = f"tenant-{secret_path.split('/')[1]}"
        
        # Store tenant metadata
        self.vault_client.secrets.kv.v2.create_or_update_secret(
            mount_point=mount_path,
            path='metadata',
            secret={
                'tenant_name': tenant_name,
                'created_at': datetime.utcnow().isoformat(),
                'tier': metadata.get('tier', 'starter')
            }
        )
        
        # Create paths for different secret types
        paths = ['api-keys', 'database', 'certificates', 'encryption-keys']
        for path in paths:
            self.vault_client.secrets.kv.v2.create_or_update_secret(
                mount_point=mount_path,
                path=f'{path}/.placeholder',
                secret={'placeholder': 'true'}
            )
        
        logger.info(f"Created initial secrets structure for tenant")
    
    async def _create_app_role(self, tenant_id: str, policy_name: str) -> str:
        """Create app role for programmatic access"""
        role_name = f"tenant-{tenant_id}"
        
        # Enable AppRole auth if not already enabled
        try:
            self.vault_client.sys.enable_auth_method(
                method_type='approle',
                path='approle'
            )
        except:
            pass  # Already enabled
        
        # Create role
        self.vault_client.auth.approle.create_or_update_approle(
            role_name=role_name,
            token_policies=[policy_name],
            token_ttl='24h',
            token_max_ttl='720h',
            secret_id_ttl='720h',
            secret_id_num_uses=0
        )
        
        # Get role ID
        role_id = self.vault_client.auth.approle.read_role_id(role_name)['data']['role_id']
        
        logger.info(f"Created app role: {role_name}")
        return role_id 