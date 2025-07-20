"""Blueprint Manager for Platform Q Services

Manages Cloudify blueprints for provisioning Platform Q services.
"""

import logging
import os
import yaml
import tempfile
import shutil
from typing import Dict, Any, Optional, List
from pathlib import Path

from .models import BlueprintMetadata, PlatformServiceDeployment
from .cloudify_client import CloudifyClient
from platformq_provisioning_common import ResourceType, TenantTier

logger = logging.getLogger(__name__)


class BlueprintManager:
    """Manages Platform Q service blueprints"""
    
    def __init__(
        self,
        cloudify_client: CloudifyClient,
        blueprints_dir: str = "/app/blueprints"
    ):
        self.cloudify_client = cloudify_client
        self.blueprints_dir = Path(blueprints_dir)
        self.blueprint_metadata: Dict[str, BlueprintMetadata] = {}
        
    async def initialize(self):
        """Initialize blueprint manager and load blueprint metadata"""
        # Ensure blueprints directory exists
        self.blueprints_dir.mkdir(parents=True, exist_ok=True)
        
        # Load blueprint metadata
        await self._load_blueprint_metadata()
        
        # Upload blueprints to Cloudify
        await self._upload_blueprints()
    
    async def _load_blueprint_metadata(self):
        """Load metadata for all available blueprints"""
        for service_dir in self.blueprints_dir.iterdir():
            if not service_dir.is_dir():
                continue
            
            metadata_file = service_dir / "metadata.yaml"
            if metadata_file.exists():
                with open(metadata_file, 'r') as f:
                    metadata_dict = yaml.safe_load(f)
                    metadata = BlueprintMetadata(**metadata_dict)
                    self.blueprint_metadata[metadata.service_type] = metadata
    
    async def _upload_blueprints(self):
        """Upload all blueprints to Cloudify Manager"""
        for service_type, metadata in self.blueprint_metadata.items():
            blueprint_id = f"platformq-{service_type}-{metadata.version}"
            
            # Check if blueprint already exists
            existing = await self.cloudify_client.get_blueprint(blueprint_id)
            if existing:
                logger.info(f"Blueprint {blueprint_id} already exists")
                continue
            
            # Create blueprint archive
            archive_path = await self._create_blueprint_archive(service_type)
            
            try:
                # Upload blueprint
                await self.cloudify_client.upload_blueprint(
                    blueprint_id=blueprint_id,
                    blueprint_path=archive_path,
                    blueprint_filename="blueprint.yaml"
                )
                logger.info(f"Uploaded blueprint {blueprint_id}")
            finally:
                # Clean up archive
                if os.path.exists(archive_path):
                    os.remove(archive_path)
    
    async def _create_blueprint_archive(self, service_type: str) -> str:
        """Create a tar.gz archive of the blueprint"""
        service_dir = self.blueprints_dir / service_type
        
        # Create temporary archive
        with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as tmp:
            archive_path = tmp.name
        
        # Create archive
        shutil.make_archive(
            archive_path.replace('.tar.gz', ''),
            'gztar',
            service_dir
        )
        
        return archive_path
    
    def get_blueprint_id(
        self,
        service_type: str,
        tier: Optional[TenantTier] = None
    ) -> str:
        """Get blueprint ID for a service type and tier"""
        metadata = self.blueprint_metadata.get(service_type)
        if not metadata:
            raise ValueError(f"No blueprint found for service type: {service_type}")
        
        # For now, use the same blueprint for all tiers
        # In future, we might have tier-specific blueprints
        return f"platformq-{service_type}-{metadata.version}"
    
    def get_blueprint_inputs(
        self,
        service_type: str,
        tenant_id: str,
        tenant_name: str,
        tier: TenantTier,
        additional_inputs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get blueprint inputs for a service deployment"""
        metadata = self.blueprint_metadata.get(service_type)
        if not metadata:
            raise ValueError(f"No blueprint found for service type: {service_type}")
        
        # Base inputs
        inputs = {
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "tier": tier.value
        }
        
        # Add tier-specific defaults
        tier_defaults = self._get_tier_defaults(service_type, tier)
        inputs.update(tier_defaults)
        
        # Add any additional inputs
        if additional_inputs:
            inputs.update(additional_inputs)
        
        # Validate required inputs
        for required in metadata.required_inputs:
            if required not in inputs:
                raise ValueError(f"Missing required input: {required}")
        
        return inputs
    
    def _get_tier_defaults(
        self,
        service_type: str,
        tier: TenantTier
    ) -> Dict[str, Any]:
        """Get default values for a service tier"""
        # Define tier-based defaults for each service
        tier_configs = {
            ResourceType.CASSANDRA_KEYSPACE: {
                TenantTier.STARTER: {
                    "replication_factor": 1,
                    "gc_grace_seconds": 864000,
                    "compaction_strategy": "SizeTieredCompactionStrategy"
                },
                TenantTier.PROFESSIONAL: {
                    "replication_factor": 3,
                    "gc_grace_seconds": 864000,
                    "compaction_strategy": "LeveledCompactionStrategy"
                },
                TenantTier.ENTERPRISE: {
                    "replication_factor": 3,
                    "gc_grace_seconds": 432000,
                    "compaction_strategy": "LeveledCompactionStrategy",
                    "enable_row_cache": True
                }
            },
            ResourceType.PULSAR_NAMESPACE: {
                TenantTier.STARTER: {
                    "ensemble_size": 1,
                    "write_quorum": 1,
                    "ack_quorum": 1,
                    "retention_policies": {
                        "retention_time_minutes": 60,
                        "retention_size_mb": 100
                    }
                },
                TenantTier.PROFESSIONAL: {
                    "ensemble_size": 2,
                    "write_quorum": 2,
                    "ack_quorum": 2,
                    "retention_policies": {
                        "retention_time_minutes": 1440,  # 24 hours
                        "retention_size_mb": 1000
                    }
                },
                TenantTier.ENTERPRISE: {
                    "ensemble_size": 3,
                    "write_quorum": 3,
                    "ack_quorum": 2,
                    "retention_policies": {
                        "retention_time_minutes": 10080,  # 7 days
                        "retention_size_mb": 10000
                    },
                    "enable_deduplication": True
                }
            },
            ResourceType.IGNITE_CACHE: {
                TenantTier.STARTER: {
                    "cache_mode": "REPLICATED",
                    "backups": 0,
                    "max_memory_mb": 256
                },
                TenantTier.PROFESSIONAL: {
                    "cache_mode": "PARTITIONED",
                    "backups": 1,
                    "max_memory_mb": 1024
                },
                TenantTier.ENTERPRISE: {
                    "cache_mode": "PARTITIONED",
                    "backups": 2,
                    "max_memory_mb": 4096,
                    "enable_persistence": True
                }
            },
            ResourceType.MINIO_BUCKET: {
                TenantTier.STARTER: {
                    "versioning": False,
                    "quota_gb": 10,
                    "lifecycle_days": 30
                },
                TenantTier.PROFESSIONAL: {
                    "versioning": True,
                    "quota_gb": 100,
                    "lifecycle_days": 90
                },
                TenantTier.ENTERPRISE: {
                    "versioning": True,
                    "quota_gb": 1000,
                    "lifecycle_days": 365,
                    "enable_encryption": True,
                    "enable_replication": True
                }
            },
            ResourceType.ELASTICSEARCH_INDEX: {
                TenantTier.STARTER: {
                    "number_of_shards": 1,
                    "number_of_replicas": 0,
                    "max_result_window": 10000
                },
                TenantTier.PROFESSIONAL: {
                    "number_of_shards": 2,
                    "number_of_replicas": 1,
                    "max_result_window": 50000
                },
                TenantTier.ENTERPRISE: {
                    "number_of_shards": 5,
                    "number_of_replicas": 2,
                    "max_result_window": 100000,
                    "enable_slow_log": True
                }
            },
            ResourceType.JANUSGRAPH_SCHEMA: {
                TenantTier.STARTER: {
                    "cache_db_cache": False,
                    "tx_cache_size": 1000
                },
                TenantTier.PROFESSIONAL: {
                    "cache_db_cache": True,
                    "tx_cache_size": 5000,
                    "enable_metrics": True
                },
                TenantTier.ENTERPRISE: {
                    "cache_db_cache": True,
                    "tx_cache_size": 10000,
                    "enable_metrics": True,
                    "enable_geo_indexing": True
                }
            }
        }
        
        # Convert ResourceType to string for lookup
        service_key = ResourceType(service_type) if isinstance(service_type, str) else service_type
        
        return tier_configs.get(service_key, {}).get(tier, {})
    
    async def deploy_service(
        self,
        service_type: str,
        tenant_id: str,
        tenant_name: str,
        tier: TenantTier,
        reseller_id: Optional[str] = None,
        customer_id: Optional[str] = None,
        additional_inputs: Optional[Dict[str, Any]] = None
    ) -> PlatformServiceDeployment:
        """Deploy a Platform Q service using Cloudify"""
        # Get blueprint ID
        blueprint_id = self.get_blueprint_id(service_type, tier)
        
        # Prepare inputs
        inputs = self.get_blueprint_inputs(
            service_type=service_type,
            tenant_id=tenant_id,
            tenant_name=tenant_name,
            tier=tier,
            additional_inputs=additional_inputs
        )
        
        # Create deployment ID
        deployment_id = f"{service_type}-{tenant_id}"
        
        # Create deployment labels
        labels = [
            {"key": "tenant_id", "value": tenant_id},
            {"key": "service_type", "value": service_type},
            {"key": "tier", "value": tier.value}
        ]
        
        if reseller_id:
            labels.append({"key": "reseller_id", "value": reseller_id})
        if customer_id:
            labels.append({"key": "customer_id", "value": customer_id})
        
        # Create deployment
        deployment = await self.cloudify_client.create_deployment(
            deployment_id=deployment_id,
            blueprint_id=blueprint_id,
            inputs=inputs,
            labels=labels
        )
        
        # Install deployment
        execution = await self.cloudify_client.install_deployment(deployment_id)
        
        # Wait for installation to complete
        final_execution = await self.cloudify_client.wait_for_execution(
            execution.id,
            timeout=600
        )
        
        if final_execution.status != "terminated":
            raise Exception(f"Deployment failed: {final_execution.error}")
        
        # Get outputs
        outputs = await self.cloudify_client.get_deployment_outputs(deployment_id)
        
        # Create deployment record
        return PlatformServiceDeployment(
            tenant_id=tenant_id,
            service_type=service_type,
            deployment_id=deployment_id,
            blueprint_id=blueprint_id,
            status="active",
            created_at=deployment.created_at,
            inputs=inputs,
            outputs=outputs,
            reseller_id=reseller_id,
            customer_id=customer_id,
            resource_limits=self._get_tier_defaults(service_type, tier)
        )
    
    async def undeploy_service(
        self,
        deployment_id: str
    ) -> bool:
        """Undeploy a Platform Q service"""
        # Uninstall deployment
        execution = await self.cloudify_client.uninstall_deployment(deployment_id)
        
        # Wait for uninstallation
        final_execution = await self.cloudify_client.wait_for_execution(
            execution.id,
            timeout=600
        )
        
        if final_execution.status != "terminated":
            logger.error(f"Uninstall failed: {final_execution.error}")
            return False
        
        # Delete deployment
        return await self.cloudify_client.delete_deployment(deployment_id) 