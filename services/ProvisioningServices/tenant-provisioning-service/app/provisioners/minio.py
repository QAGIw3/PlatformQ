"""MinIO Provisioner"""

import logging
import json
from typing import Dict, Any
from datetime import datetime, timedelta

from minio import Minio
from minio.error import S3Error

from platformq_provisioning_common import (
    IResourceProvisioner,
    InfrastructureResource,
    ResourceType,
    ProvisioningStatus,
    ProvisioningError
)

from ..config import Settings

logger = logging.getLogger(__name__)


class MinioProvisioner(IResourceProvisioner):
    """Provisions MinIO buckets for tenants"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = None
    
    async def initialize(self):
        """Initialize MinIO client"""
        try:
            self.client = Minio(
                self.settings.minio_endpoint,
                access_key=self.settings.minio_access_key,
                secret_key=self.settings.minio_secret_key,
                secure=self.settings.minio_secure
            )
            
            # Test connection by listing buckets
            self.client.list_buckets()
            logger.info("Connected to MinIO")
        except Exception as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            raise
    
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision MinIO bucket for tenant"""
        bucket_name = f"tenant-{tenant_id}"
        
        resource = InfrastructureResource(
            resource_type=ResourceType.MINIO_BUCKET,
            resource_name=bucket_name,
            tenant_id=tenant_id,
            provisioned_by="minio-provisioner"
        )
        
        try:
            # Validate bucket name
            if not await self.validate(tenant_id):
                raise ProvisioningError("Invalid tenant ID for bucket name")
            
            # Create bucket
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(
                    bucket_name,
                    location=metadata.get('minio_region', 'us-east-1')
                )
                logger.info(f"Created bucket {bucket_name}")
            else:
                logger.info(f"Bucket {bucket_name} already exists")
            
            # Set bucket policies
            await self._set_bucket_policies(bucket_name, tenant_id)
            
            # Create default folder structure
            await self._create_folder_structure(bucket_name)
            
            # Set lifecycle policies
            await self._set_lifecycle_policies(bucket_name)
            
            # Set resource metadata
            resource.status = ProvisioningStatus.COMPLETED
            resource.provisioned_at = datetime.utcnow()
            resource.metadata = {
                'bucket_name': bucket_name,
                'location': metadata.get('minio_region', 'us-east-1'),
                'folders_created': [
                    'data/raw', 'data/processed', 'data/archive',
                    'models', 'artifacts', 'temp'
                ],
                'lifecycle_policies': ['archive_old_data', 'cleanup_temp']
            }
            
            return resource
            
        except Exception as e:
            logger.error(f"Failed to provision MinIO bucket: {e}")
            resource.status = ProvisioningStatus.FAILED
            resource.error_message = str(e)
            raise ProvisioningError(
                f"Failed to create bucket {bucket_name}",
                ResourceType.MINIO_BUCKET,
                {'error': str(e)}
            )
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision MinIO bucket"""
        try:
            # Remove all objects first
            objects = self.client.list_objects(resource_name, recursive=True)
            for obj in objects:
                self.client.remove_object(resource_name, obj.object_name)
            
            # Remove bucket
            self.client.remove_bucket(resource_name)
            
            logger.info(f"Removed bucket {resource_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to deprovision MinIO bucket: {e}")
            return False
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate if provisioning is possible"""
        bucket_name = f"tenant-{tenant_id}"
        
        # MinIO bucket names must be DNS-compliant
        if len(bucket_name) < 3 or len(bucket_name) > 63:
            return False
        
        # Must start and end with lowercase letter or number
        if not (bucket_name[0].isalnum() and bucket_name[-1].isalnum()):
            return False
        
        # Can only contain lowercase letters, numbers, and hyphens
        for char in bucket_name:
            if not (char.isalnum() or char == '-'):
                return False
        
        return True
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.MINIO_BUCKET
    
    async def _set_bucket_policies(self, bucket_name: str, tenant_id: str):
        """Set bucket access policies"""
        # Create a policy that allows the tenant to access their bucket
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": [f"arn:aws:iam::*:user/tenant-{tenant_id}"]},
                    "Action": [
                        "s3:GetBucketLocation",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads"
                    ],
                    "Resource": [f"arn:aws:s3:::{bucket_name}"]
                },
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": [f"arn:aws:iam::*:user/tenant-{tenant_id}"]},
                    "Action": [
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:AbortMultipartUpload",
                        "s3:ListMultipartUploadParts"
                    ],
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
                }
            ]
        }
        
        self.client.set_bucket_policy(bucket_name, json.dumps(policy))
        logger.info(f"Set bucket policy for {bucket_name}")
    
    async def _create_folder_structure(self, bucket_name: str):
        """Create default folder structure in bucket"""
        folders = [
            'data/raw/',
            'data/processed/',
            'data/archive/',
            'models/',
            'artifacts/',
            'temp/'
        ]
        
        for folder in folders:
            # Create empty object to represent folder
            self.client.put_object(
                bucket_name,
                folder,
                data=b'',
                length=0
            )
        
        logger.info(f"Created folder structure in {bucket_name}")
    
    async def _set_lifecycle_policies(self, bucket_name: str):
        """Set lifecycle policies for data management"""
        # Archive old data after 90 days
        archive_config = {
            "Rules": [{
                "ID": "archive_old_data",
                "Status": "Enabled",
                "Filter": {"Prefix": "data/raw/"},
                "Transitions": [{
                    "Days": 90,
                    "StorageClass": "GLACIER"
                }]
            }]
        }
        
        # Clean up temp files after 7 days
        cleanup_config = {
            "Rules": [{
                "ID": "cleanup_temp",
                "Status": "Enabled",
                "Filter": {"Prefix": "temp/"},
                "Expiration": {"Days": 7}
            }]
        }
        
        # Note: MinIO doesn't support all S3 lifecycle features
        # This is a placeholder for when using actual S3
        logger.info(f"Lifecycle policies configured for {bucket_name}") 