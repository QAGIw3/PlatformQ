"""
Apache Pulsar Provisioner

Provisions Pulsar namespaces and topics for tenants.
"""
import logging
from typing import Dict, Any, List
import uuid
from datetime import datetime

from pulsar import Client as PulsarClient
import requests

from platformq_resource_common import (
    ResourceType, InfrastructureResource, ResourceStatus,
    IResourceProvisioner
)
from ..core.config import Settings

logger = logging.getLogger(__name__)


class PulsarProvisioner(IResourceProvisioner):
    """Provisions Apache Pulsar resources"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.admin_url = settings.pulsar_admin_url
        self.pulsar_client = None
    
    async def initialize(self):
        """Initialize Pulsar connection"""
        try:
            self.pulsar_client = PulsarClient(self.settings.pulsar_url)
            logger.info("Pulsar provisioner initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Pulsar provisioner: {e}")
            raise
    
    async def shutdown(self):
        """Shutdown Pulsar connection"""
        if self.pulsar_client:
            self.pulsar_client.close()
    
    async def provision(
        self,
        tenant_id: str,
        tenant_name: str,
        metadata: Dict[str, Any]
    ) -> InfrastructureResource:
        """Provision Pulsar namespace and topics for tenant"""
        namespace = f"public/tenant-{tenant_id}"
        
        try:
            # Create namespace
            await self._create_namespace(namespace, metadata)
            
            # Create default topics
            topics = await self._create_default_topics(namespace, metadata)
            
            # Set namespace policies
            await self._set_namespace_policies(namespace, metadata)
            
            # Create resource object
            resource = InfrastructureResource(
                resource_id=str(uuid.uuid4()),
                resource_type=ResourceType.PULSAR,
                resource_name=namespace,
                tenant_id=tenant_id,
                status=ResourceStatus.ACTIVE,
                endpoint=self.settings.pulsar_url,
                configuration={
                    "namespace": namespace,
                    "topics": topics,
                    "admin_url": self.admin_url,
                    "partitions": metadata.get('partitions', self.settings.default_pulsar_partitions)
                },
                created_at=datetime.utcnow()
            )
            
            logger.info(f"Successfully provisioned Pulsar for tenant {tenant_id}")
            return resource
            
        except Exception as e:
            logger.error(f"Failed to provision Pulsar for tenant {tenant_id}: {e}")
            raise
    
    async def deprovision(self, tenant_id: str, resource_name: str) -> bool:
        """Deprovision Pulsar namespace"""
        try:
            # Delete namespace (this will delete all topics within it)
            response = requests.delete(
                f"{self.admin_url}/admin/v2/namespaces/{resource_name}"
            )
            
            if response.status_code in [204, 404]:
                logger.info(f"Deleted Pulsar namespace: {resource_name}")
                return True
            else:
                logger.error(f"Failed to delete namespace: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to deprovision Pulsar namespace {resource_name}: {e}")
            return False
    
    async def validate(self, tenant_id: str) -> bool:
        """Validate Pulsar provisioning"""
        namespace = f"public/tenant-{tenant_id}"
        
        try:
            # Check if namespace exists
            response = requests.get(
                f"{self.admin_url}/admin/v2/namespaces/{namespace}"
            )
            return response.status_code == 200
            
        except Exception as e:
            logger.error(f"Failed to validate Pulsar for tenant {tenant_id}: {e}")
            return False
    
    def get_resource_type(self) -> ResourceType:
        """Get the resource type this provisioner handles"""
        return ResourceType.PULSAR
    
    async def _create_namespace(self, namespace: str, metadata: Dict[str, Any]):
        """Create Pulsar namespace"""
        # Create namespace
        response = requests.put(
            f"{self.admin_url}/admin/v2/namespaces/{namespace}"
        )
        
        if response.status_code not in [204, 409]:  # 409 = already exists
            raise Exception(f"Failed to create namespace: {response.text}")
        
        logger.info(f"Created Pulsar namespace: {namespace}")
    
    async def _create_default_topics(self, namespace: str, metadata: Dict[str, Any]) -> List[str]:
        """Create default topics for the tenant"""
        partitions = metadata.get('partitions', self.settings.default_pulsar_partitions)
        
        topics = [
            "events",
            "commands",
            "queries",
            "notifications",
            "audit-log",
            "metrics",
            "ml-requests",
            "ml-results"
        ]
        
        created_topics = []
        
        for topic in topics:
            topic_name = f"persistent://{namespace}/{topic}"
            
            # Create partitioned topic
            response = requests.put(
                f"{self.admin_url}/admin/v2/persistent/{namespace}/{topic}/partitions",
                json=partitions
            )
            
            if response.status_code in [204, 409]:  # 409 = already exists
                created_topics.append(topic_name)
                logger.info(f"Created topic: {topic_name} with {partitions} partitions")
            else:
                logger.error(f"Failed to create topic {topic_name}: {response.text}")
        
        return created_topics
    
    async def _set_namespace_policies(self, namespace: str, metadata: Dict[str, Any]):
        """Set namespace policies"""
        policies = {
            # Retention policy
            "retention": {
                "retentionTimeInMinutes": metadata.get('retention_minutes', 10080),  # 7 days
                "retentionSizeInMB": metadata.get('retention_size_mb', 10240)  # 10 GB
            },
            
            # Message TTL
            "message_ttl": metadata.get('message_ttl_seconds', 0),  # 0 = no TTL
            
            # Backlog quota
            "backlog_quota": {
                "limit": metadata.get('backlog_limit_bytes', 1073741824),  # 1 GB
                "policy": "producer_request_hold"
            }
        }
        
        # Set retention policy
        response = requests.post(
            f"{self.admin_url}/admin/v2/namespaces/{namespace}/retention",
            json=policies["retention"]
        )
        if response.status_code == 204:
            logger.info(f"Set retention policy for namespace: {namespace}")
        
        # Set message TTL if specified
        if policies["message_ttl"] > 0:
            response = requests.post(
                f"{self.admin_url}/admin/v2/namespaces/{namespace}/messageTTL",
                json=policies["message_ttl"]
            )
            if response.status_code == 204:
                logger.info(f"Set message TTL for namespace: {namespace}")
        
        # Set backlog quota
        response = requests.post(
            f"{self.admin_url}/admin/v2/namespaces/{namespace}/backlogQuota",
            json=policies["backlog_quota"]
        )
        if response.status_code == 204:
            logger.info(f"Set backlog quota for namespace: {namespace}") 