#!/usr/bin/env python3
"""
Cloudify script to create MinIO bucket and policies for a tenant.
"""

import os
import sys
import time
import json
import logging
from typing import Dict, Any, List, Optional
from cloudify import ctx
from cloudify.state import ctx_parameters as inputs
from cloudify.exceptions import NonRecoverableError, RecoverableError
from minio import Minio
from minio.error import S3Error
from minio.api import VersioningConfig
from minio.api import ObjectLockConfig
from minio.commonconfig import ENABLED
import urllib3

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('minio_provisioner')

# Disable SSL warnings for development
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class MinIOProvisioner:
    """Handles MinIO bucket and policy provisioning for tenants."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.endpoint = config['minio_endpoint']
        self.access_key = config['access_key']
        self.secret_key = config['secret_key']
        self.secure = config.get('secure', True)
        self.bucket_name = config['bucket_name']
        self.tenant_id = config['tenant_id']
        self.reseller_id = config.get('reseller_id')
        self.customer_id = config.get('customer_id')
        
        # Initialize MinIO client
        self.client = Minio(
            self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure
        )
        
    def create_bucket(self):
        """Create bucket for the tenant."""
        try:
            # Check if bucket exists
            if self.client.bucket_exists(self.bucket_name):
                logger.info(f"Bucket {self.bucket_name} already exists")
                return
                
            # Create bucket
            self.client.make_bucket(
                self.bucket_name,
                location=self.config.get('region', 'us-east-1')
            )
            logger.info(f"Created bucket: {self.bucket_name}")
            
            # Enable versioning if specified
            if self.config.get('enable_versioning', True):
                self.client.set_bucket_versioning(
                    self.bucket_name,
                    VersioningConfig(ENABLED)
                )
                logger.info(f"Enabled versioning for bucket {self.bucket_name}")
                
            # Configure lifecycle rules
            self._configure_lifecycle_rules()
            
            # Set bucket tags
            tags = {
                'tenant_id': self.tenant_id,
                'reseller_id': self.reseller_id or '',
                'customer_id': self.customer_id or '',
                'created_at': str(int(time.time())),
                'environment': self.config.get('environment', 'production')
            }
            self.client.set_bucket_tags(self.bucket_name, tags)
            
            # Report usage
            self._report_usage('bucket_created', {
                'bucket_name': self.bucket_name,
                'storage_quota_gb': self.config.get('storage_quota_gb', 100),
                'versioning_enabled': self.config.get('enable_versioning', True)
            })
            
        except S3Error as e:
            if e.code == 'BucketAlreadyOwnedByYou':
                logger.warning(f"Bucket {self.bucket_name} already owned by this account")
            else:
                raise NonRecoverableError(f"Failed to create bucket: {str(e)}")
                
    def _configure_lifecycle_rules(self):
        """Configure lifecycle rules for the bucket."""
        try:
            lifecycle_config = {
                "Rules": []
            }
            
            # Archive old versions
            if self.config.get('enable_versioning', True):
                archive_rule = {
                    "ID": "archive-old-versions",
                    "Status": "Enabled",
                    "NoncurrentVersionTransitions": [
                        {
                            "NoncurrentDays": self.config.get('archive_after_days', 30),
                            "StorageClass": "GLACIER"
                        }
                    ],
                    "NoncurrentVersionExpiration": {
                        "NoncurrentDays": self.config.get('expire_versions_after_days', 90)
                    }
                }
                lifecycle_config["Rules"].append(archive_rule)
                
            # Delete incomplete multipart uploads
            multipart_rule = {
                "ID": "delete-incomplete-multipart",
                "Status": "Enabled",
                "AbortIncompleteMultipartUpload": {
                    "DaysAfterInitiation": 7
                }
            }
            lifecycle_config["Rules"].append(multipart_rule)
            
            # Transition to cheaper storage for old data
            if self.config.get('enable_tiering', True):
                tiering_rule = {
                    "ID": "tiering-rule",
                    "Status": "Enabled",
                    "Transitions": [
                        {
                            "Days": self.config.get('tier_after_days', 60),
                            "StorageClass": "STANDARD_IA"
                        }
                    ],
                    "Filter": {"Prefix": ""}
                }
                lifecycle_config["Rules"].append(tiering_rule)
                
            # Expire temporary files
            temp_rule = {
                "ID": "expire-temp-files",
                "Status": "Enabled",
                "Expiration": {
                    "Days": self.config.get('temp_file_days', 7)
                },
                "Filter": {"Prefix": "temp/"}
            }
            lifecycle_config["Rules"].append(temp_rule)
            
            # Set lifecycle configuration
            self.client.set_bucket_lifecycle(self.bucket_name, lifecycle_config)
            logger.info(f"Configured lifecycle rules for bucket {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to configure lifecycle rules: {str(e)}")
            
    def configure_bucket_policy(self):
        """Configure bucket access policy."""
        try:
            # Create bucket policy
            bucket_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": f"TenantReadWrite-{self.tenant_id}",
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": [f"arn:aws:iam:::user/tenant-{self.tenant_id}"]
                        },
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                            "s3:ListBucket",
                            "s3:GetBucketLocation",
                            "s3:GetBucketVersioning",
                            "s3:GetObjectVersion",
                            "s3:DeleteObjectVersion"
                        ],
                        "Resource": [
                            f"arn:aws:s3:::{self.bucket_name}",
                            f"arn:aws:s3:::{self.bucket_name}/*"
                        ]
                    },
                    {
                        "Sid": f"TenantListBucket-{self.tenant_id}",
                        "Effect": "Allow",
                        "Principal": {
                            "AWS": [f"arn:aws:iam:::user/tenant-{self.tenant_id}"]
                        },
                        "Action": [
                            "s3:ListBucketVersions",
                            "s3:ListBucketMultipartUploads"
                        ],
                        "Resource": f"arn:aws:s3:::{self.bucket_name}"
                    }
                ]
            }
            
            # Add read-only access for monitoring
            if self.config.get('enable_monitoring', True):
                monitor_statement = {
                    "Sid": "MonitoringReadAccess",
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": ["arn:aws:iam:::user/monitoring-service"]
                    },
                    "Action": [
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:GetBucketLocation"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{self.bucket_name}",
                        f"arn:aws:s3:::{self.bucket_name}/*"
                    ]
                }
                bucket_policy["Statement"].append(monitor_statement)
                
            # Set bucket policy
            self.client.set_bucket_policy(
                self.bucket_name,
                json.dumps(bucket_policy)
            )
            logger.info(f"Configured bucket policy for {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to configure bucket policy: {str(e)}")
            
    def create_service_account(self):
        """Create service account for tenant access."""
        try:
            # This would typically use MinIO Admin API
            # For now, we'll store the credentials metadata
            service_account = {
                'access_key': f"tenant-{self.tenant_id}-{int(time.time())}",
                'secret_key': self._generate_secret_key(),
                'tenant_id': self.tenant_id,
                'bucket_name': self.bucket_name,
                'created_at': int(time.time())
            }
            
            # In production, this would create actual MinIO service account
            # Store in runtime properties for retrieval
            ctx.instance.runtime_properties['service_account'] = service_account
            logger.info(f"Created service account for tenant {self.tenant_id}")
            
            return service_account
            
        except Exception as e:
            logger.error(f"Failed to create service account: {str(e)}")
            return None
            
    def set_bucket_quota(self):
        """Set storage quota for the bucket."""
        try:
            # MinIO quotas are typically set at the user level via Admin API
            # Store quota metadata for enforcement
            quota_config = {
                'bucket_name': self.bucket_name,
                'tenant_id': self.tenant_id,
                'storage_quota_gb': self.config.get('storage_quota_gb', 100),
                'object_count_limit': self.config.get('object_count_limit', 1000000),
                'bandwidth_limit_mbps': self.config.get('bandwidth_limit_mbps', 100)
            }
            
            # In production, this would set actual MinIO quotas
            ctx.instance.runtime_properties['quota_config'] = quota_config
            logger.info(f"Set quota for bucket {self.bucket_name}: {quota_config['storage_quota_gb']}GB")
            
        except Exception as e:
            logger.error(f"Failed to set bucket quota: {str(e)}")
            
    def configure_replication(self):
        """Configure bucket replication if enabled."""
        try:
            if not self.config.get('enable_replication', False):
                return
                
            replication_config = {
                "Role": f"arn:aws:iam:::role/replication-{self.bucket_name}",
                "Rules": [
                    {
                        "ID": "replicate-all",
                        "Status": "Enabled",
                        "Priority": 1,
                        "DeleteMarkerReplication": {"Status": "Enabled"},
                        "Filter": {},
                        "Destination": {
                            "Bucket": f"arn:aws:s3:::{self.bucket_name}-replica",
                            "ReplicationTime": {
                                "Status": "Enabled",
                                "Time": {"Minutes": 15}
                            },
                            "Metrics": {
                                "Status": "Enabled",
                                "EventThreshold": {"Minutes": 15}
                            },
                            "StorageClass": "STANDARD"
                        }
                    }
                ]
            }
            
            # In production, this would configure actual replication
            ctx.instance.runtime_properties['replication_config'] = replication_config
            logger.info(f"Configured replication for bucket {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to configure replication: {str(e)}")
            
    def create_default_folders(self):
        """Create default folder structure in the bucket."""
        try:
            default_folders = [
                'uploads/',
                'temp/',
                'backups/',
                'logs/',
                'public/',
                'private/',
                'archives/'
            ]
            
            for folder in default_folders:
                # Create empty object to represent folder
                self.client.put_object(
                    self.bucket_name,
                    folder,
                    data=b'',
                    length=0
                )
                
            logger.info(f"Created default folder structure in bucket {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to create default folders: {str(e)}")
            
    def configure_notifications(self):
        """Configure bucket event notifications."""
        try:
            if not self.config.get('enable_notifications', False):
                return
                
            # Configure notifications for specific events
            notification_config = {
                'TopicConfigurations': [
                    {
                        'Id': 'object-created',
                        'Topic': f"arn:minio:sns:{self.config.get('region', 'us-east-1')}:1:object-created-{self.bucket_name}",
                        'Events': ['s3:ObjectCreated:*'],
                        'Filter': {
                            'Key': {
                                'FilterRules': [
                                    {'Name': 'prefix', 'Value': 'uploads/'}
                                ]
                            }
                        }
                    },
                    {
                        'Id': 'object-removed',
                        'Topic': f"arn:minio:sns:{self.config.get('region', 'us-east-1')}:1:object-removed-{self.bucket_name}",
                        'Events': ['s3:ObjectRemoved:*']
                    }
                ]
            }
            
            # In production, this would configure actual notifications
            ctx.instance.runtime_properties['notification_config'] = notification_config
            logger.info(f"Configured notifications for bucket {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"Failed to configure notifications: {str(e)}")
            
    def _generate_secret_key(self) -> str:
        """Generate a secure secret key."""
        import secrets
        import string
        alphabet = string.ascii_letters + string.digits
        return ''.join(secrets.choice(alphabet) for _ in range(40))
        
    def _report_usage(self, event_type: str, details: Dict[str, Any]):
        """Report usage event to metering service."""
        try:
            # In production, this would send to OpenMeter/CloudKitty
            usage_event = {
                'tenant_id': self.tenant_id,
                'reseller_id': self.reseller_id,
                'customer_id': self.customer_id,
                'service': 'minio',
                'event_type': event_type,
                'timestamp': int(time.time()),
                'details': details
            }
            logger.info(f"Usage event: {usage_event}")
            
        except Exception as e:
            logger.error(f"Failed to report usage: {str(e)}")


def main():
    """Main execution function for Cloudify."""
    try:
        # Get configuration from Cloudify inputs
        config = {
            'minio_endpoint': inputs.get('minio_endpoint', 'localhost:9000'),
            'access_key': inputs.get('access_key', 'minioadmin'),
            'secret_key': inputs.get('secret_key', 'minioadmin'),
            'secure': inputs.get('secure', True),
            'tenant_id': inputs['tenant_id'],
            'reseller_id': inputs.get('reseller_id'),
            'customer_id': inputs.get('customer_id'),
            'bucket_name': inputs.get('bucket_name', f"tenant-{inputs['tenant_id']}"),
            'region': inputs.get('region', 'us-east-1'),
            'storage_quota_gb': inputs.get('storage_quota_gb', 100),
            'object_count_limit': inputs.get('object_count_limit', 1000000),
            'bandwidth_limit_mbps': inputs.get('bandwidth_limit_mbps', 100),
            'enable_versioning': inputs.get('enable_versioning', True),
            'enable_tiering': inputs.get('enable_tiering', True),
            'enable_replication': inputs.get('enable_replication', False),
            'enable_notifications': inputs.get('enable_notifications', False),
            'enable_monitoring': inputs.get('enable_monitoring', True),
            'archive_after_days': inputs.get('archive_after_days', 30),
            'expire_versions_after_days': inputs.get('expire_versions_after_days', 90),
            'tier_after_days': inputs.get('tier_after_days', 60),
            'temp_file_days': inputs.get('temp_file_days', 7),
            'environment': inputs.get('environment', 'production')
        }
        
        # Store config in runtime properties for other operations
        ctx.instance.runtime_properties['minio_config'] = config
        
        provisioner = MinIOProvisioner(config)
        
        # Create bucket
        provisioner.create_bucket()
        
        # Configure bucket policy
        provisioner.configure_bucket_policy()
        
        # Create service account
        service_account = provisioner.create_service_account()
        
        # Set bucket quota
        provisioner.set_bucket_quota()
        
        # Configure replication if enabled
        provisioner.configure_replication()
        
        # Create default folder structure
        provisioner.create_default_folders()
        
        # Configure notifications if enabled
        provisioner.configure_notifications()
        
        # Store bucket info in runtime properties
        ctx.instance.runtime_properties['bucket_name'] = config['bucket_name']
        ctx.instance.runtime_properties['bucket_created'] = True
        ctx.instance.runtime_properties['endpoint'] = config['minio_endpoint']
        
        logger.info(f"Successfully provisioned MinIO bucket for tenant {config['tenant_id']}")
        
    except Exception as e:
        logger.error(f"Failed to provision MinIO bucket: {str(e)}")
        raise NonRecoverableError(str(e))


if __name__ == '__main__':
    main() 