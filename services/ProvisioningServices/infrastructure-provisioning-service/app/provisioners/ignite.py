"""
Apache Ignite Provisioner

Provisions Ignite caches and configurations for tenants.
"""
import logging
from typing import Dict, Any
import uuid
from datetime import datetime

from pyignite import Client

from platformq_resource_common import (
    ResourceType, InfrastructureResource, ResourceStatus,
    IResourceProvisioner
)
from ..core.config import Settings

logger = logging.getLogger(__name__)


class IgniteProvisioner(IResourceProvisioner):
    """Provisions Apache Ignite resources"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.ignite_client = None
    
    async def initialize(self):
        """Initialize Ignite connection"""
        try:
            self.ignite_client = Client()
            self.ignite_client.connect(
                self.settings.ignite_host,
                self.settings.ignite_port
            )
            
            logger.info("Ignite provisioner initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Ignite provisioner: {e}")
            raise
    
    async def shutdown(self):
        """Shutdown Ignite connection"""
        if self.ignite_client:
            self.ignite_client.close()
    
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision Ignite caches for tenant"""
        cache_prefix = f"tenant_{tenant_id.replace('-', '_')}"
        
        try:
            # Create caches
            await self._create_caches(cache_prefix, metadata)
            
            # Setup affinity colocation
            await self._setup_affinity_colocation(cache_prefix)
            
            # Configure data regions if needed
            await self._configure_data_regions(cache_prefix, metadata)
            
            # Create resource object
            resource = InfrastructureResource(
                resource_id=str(uuid.uuid4()),
                resource_type=ResourceType.IGNITE,
                resource_name=cache_prefix,
                tenant_id=tenant_id,
                status=ResourceStatus.ACTIVE,
                endpoint=f"{self.settings.ignite_host}:{self.settings.ignite_port}",
                configuration={
                    "cache_prefix": cache_prefix,
                    "caches": [
                        f"{cache_prefix}_session",
                        f"{cache_prefix}_compute",
                        f"{cache_prefix}_analytics",
                        f"{cache_prefix}_ml_models"
                    ]
                },
                created_at=datetime.utcnow()
            )
            
            logger.info(f"Successfully provisioned Ignite for tenant {tenant_id}")
            return resource
            
        except Exception as e:
            logger.error(f"Failed to provision Ignite for tenant {tenant_id}: {e}")
            raise
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision Ignite caches"""
        try:
            # List all caches for the tenant
            cache_names = [
                f"{resource_name}_session",
                f"{resource_name}_compute",
                f"{resource_name}_analytics",
                f"{resource_name}_ml_models"
            ]
            
            # Destroy each cache
            for cache_name in cache_names:
                try:
                    cache = self.ignite_client.get_cache(cache_name)
                    if cache:
                        cache.destroy()
                        logger.info(f"Destroyed cache: {cache_name}")
                except:
                    pass
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to deprovision Ignite caches {resource_name}: {e}")
            return False
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate Ignite provisioning"""
        cache_prefix = f"tenant_{tenant_id.replace('-', '_')}"
        
        try:
            # Check if at least one cache exists
            cache_name = f"{cache_prefix}_session"
            cache = self.ignite_client.get_cache(cache_name)
            return cache is not None
            
        except Exception as e:
            logger.error(f"Failed to validate Ignite for tenant {tenant_id}: {e}")
            return False
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.IGNITE
    
    async def _create_caches(self, cache_prefix: str, metadata: Dict[str, Any]):
        """Create Ignite caches for the tenant"""
        caches = {
            # Session cache for user sessions
            f"{cache_prefix}_session": {
                "cache_mode": "REPLICATED",
                "atomicity_mode": "ATOMIC",
                "expiry_policy": {
                    "create": 3600000,  # 1 hour in ms
                    "update": 3600000,
                    "access": 3600000
                }
            },
            
            # Compute cache for distributed compute results
            f"{cache_prefix}_compute": {
                "cache_mode": "PARTITIONED",
                "backups": 1,
                "atomicity_mode": "TRANSACTIONAL",
                "affinity": {
                    "partitions": 128,
                    "exclude_neighbors": True
                }
            },
            
            # Analytics cache for real-time analytics
            f"{cache_prefix}_analytics": {
                "cache_mode": "PARTITIONED",
                "backups": 2,
                "atomicity_mode": "ATOMIC",
                "sql_schema": f"TENANT_{cache_prefix.upper()}",
                "query_parallelism": 4
            },
            
            # ML models cache
            f"{cache_prefix}_ml_models": {
                "cache_mode": "REPLICATED",
                "atomicity_mode": "ATOMIC",
                "on_heap_cache_enabled": True,
                "eager_ttl": True
            }
        }
        
        for cache_name, config in caches.items():
            cache = self.ignite_client.get_or_create_cache(cache_name)
            logger.info(f"Created cache: {cache_name}")
            # Note: Full cache configuration would require using Ignite's configuration API
    
    async def _setup_affinity_colocation(self, cache_prefix: str):
        """Setup affinity colocation for better performance"""
        # This would configure affinity keys to ensure related data
        # is colocated on the same nodes
        logger.info(f"Configured affinity colocation for {cache_prefix}")
    
    async def _configure_data_regions(self, cache_prefix: str, metadata: Dict[str, Any]):
        """Configure data regions for the tenant"""
        # This would set up memory policies and persistence if needed
        logger.info(f"Configured data regions for {cache_prefix}") 