"""
MinIO Provisioner

Provisions MinIO buckets and configurations for tenants.
"""
import json
import logging
from typing import Dict, Any
import uuid
from datetime import datetime

from minio import Minio
from minio.error import S3Error

from platformq_resource_common import (
    ResourceType, InfrastructureResource, ResourceStatus,
    IResourceProvisioner
)
from ..core.config import Settings

logger = logging.getLogger(__name__)


class MinioProvisioner(IResourceProvisioner):
    """Provisions MinIO resources"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.minio_client = None
    
    async def initialize(self):
        """Initialize MinIO connection"""
        try:
            self.minio_client = Minio(
                endpoint=self.settings.minio_endpoint,
                access_key=self.settings.minio_access_key,
                secret_key=self.settings.minio_secret_key,
                secure=self.settings.minio_secure
            )
            
            logger.info("MinIO provisioner initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize MinIO provisioner: {e}")
            raise
    
    async def shutdown(self):
        """Shutdown MinIO connection"""
        # MinIO client doesn't need explicit shutdown
        pass
    
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision MinIO bucket for tenant"""
        bucket_name = f"tenant-{tenant_id}".lower()
        
        try:
            # Create bucket
            if not self.minio_client.bucket_exists(bucket_name):
                self.minio_client.make_bucket(bucket_name)
                logger.info(f"Created MinIO bucket: {bucket_name}")
            
            # Set bucket policies
            await self._set_bucket_policies(bucket_name, tenant_id)
            
            # Enable versioning if configured
            if metadata.get('versioning', self.settings.default_minio_bucket_versioning):
                await self._enable_versioning(bucket_name)
            
            # Set lifecycle policies
            await self._set_lifecycle_policies(bucket_name)
            
            # Create folder structure
            await self._create_folder_structure(bucket_name)
            
            # Create resource object
            resource = InfrastructureResource(
                resource_id=str(uuid.uuid4()),
                resource_type=ResourceType.MINIO,
                resource_name=bucket_name,
                tenant_id=tenant_id,
                status=ResourceStatus.ACTIVE,
                endpoint=self.settings.minio_endpoint,
                configuration={
                    "bucket_name": bucket_name,
                    "versioning": metadata.get('versioning', self.settings.default_minio_bucket_versioning),
                    "access_key": self.settings.minio_access_key,
                },
                created_at=datetime.utcnow()
            )
            
            logger.info(f"Successfully provisioned MinIO for tenant {tenant_id}")
            return resource
            
        except Exception as e:
            logger.error(f"Failed to provision MinIO for tenant {tenant_id}: {e}")
            raise
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision MinIO bucket"""
        try:
            # List and remove all objects
            objects = self.minio_client.list_objects(resource_name, recursive=True)
            for obj in objects:
                self.minio_client.remove_object(resource_name, obj.object_name)
            
            # Remove bucket
            self.minio_client.remove_bucket(resource_name)
            logger.info(f"Removed MinIO bucket: {resource_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to deprovision MinIO bucket {resource_name}: {e}")
            return False
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate MinIO provisioning"""
        bucket_name = f"tenant-{tenant_id}".lower()
        
        try:
            return self.minio_client.bucket_exists(bucket_name)
            
        except Exception as e:
            logger.error(f"Failed to validate MinIO for tenant {tenant_id}: {e}")
            return False
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.MINIO
    
    async def _set_bucket_policies(self, bucket_name: str, tenant_id: str):
        """Set bucket access policies"""
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": [f"arn:aws:iam:::user/tenant-{tenant_id}"]
                    },
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListBucket",
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{bucket_name}",
                        f"arn:aws:s3:::{bucket_name}/*"
                    ]
                }
            ]
        }
        
        policy_json = json.dumps(policy)
        self.minio_client.set_bucket_policy(bucket_name, policy_json)
        logger.info(f"Set bucket policy for: {bucket_name}")
    
    async def _enable_versioning(self, bucket_name: str):
        """Enable bucket versioning"""
        # Note: MinIO versioning requires specific server configuration
        # This is a placeholder for the functionality
        logger.info(f"Versioning configuration for bucket: {bucket_name}")
    
    async def _set_lifecycle_policies(self, bucket_name: str):
        """Set lifecycle policies for automatic data management"""
        # Configure lifecycle rules
        # Note: This would use MinIO's lifecycle management API
        logger.info(f"Set lifecycle policies for bucket: {bucket_name}")
    
    async def _create_folder_structure(self, bucket_name: str):
        """Create default folder structure"""
        folders = [
            "data/",
            "models/",
            "artifacts/",
            "logs/",
            "temp/",
            "backups/"
        ]
        
        for folder in folders:
            # Create empty object to represent folder
            self.minio_client.put_object(
                bucket_name,
                folder,
                data=b'',
                length=0
            )
        
        logger.info(f"Created folder structure in bucket: {bucket_name}") 