"""Common provisioning models"""

from enum import Enum
from typing import Dict, List, Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field, UUID4
import uuid


class ProvisioningStatus(str, Enum):
    """Status of provisioning operation"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIALLY_COMPLETED = "partially_completed"
    ROLLED_BACK = "rolled_back"


class ResourceType(str, Enum):
    """Types of infrastructure resources"""
    CASSANDRA_KEYSPACE = "cassandra_keyspace"
    MINIO_BUCKET = "minio_bucket"
    PULSAR_NAMESPACE = "pulsar_namespace"
    IGNITE_CACHE = "ignite_cache"
    ELASTICSEARCH_INDEX = "elasticsearch_index"
    JANUSGRAPH_SCHEMA = "janusgraph_schema"
    KUBERNETES_NAMESPACE = "kubernetes_namespace"
    OPENPROJECT_PROJECT = "openproject_project"
    NEXTCLOUD_USER = "nextcloud_user"
    VAULT_SECRETS = "vault_secrets"
    CONSUL_CONFIG = "consul_config"


class TenantTier(str, Enum):
    """Tenant subscription tiers"""
    FREE = "free"
    STARTER = "starter"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"
    CUSTOM = "custom"


class InfrastructureResource(BaseModel):
    """Represents a provisioned infrastructure resource"""
    resource_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    resource_type: ResourceType
    resource_name: str
    tenant_id: str
    status: ProvisioningStatus = ProvisioningStatus.PENDING
    provisioned_at: Optional[datetime] = None
    provisioned_by: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    error_message: Optional[str] = None
    
    class Config:
        use_enum_values = True


class ProvisioningRequest(BaseModel):
    """Request to provision resources for a tenant"""
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str
    tenant_name: str
    tier: TenantTier
    requested_by: str
    requested_at: datetime = Field(default_factory=datetime.utcnow)
    resources_to_provision: List[ResourceType]
    metadata: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=1, le=10)
    dry_run: bool = False
    
    class Config:
        use_enum_values = True


class ProvisioningResult(BaseModel):
    """Result of a provisioning operation"""
    request_id: str
    tenant_id: str
    status: ProvisioningStatus
    started_at: datetime
    completed_at: Optional[datetime] = None
    provisioned_resources: List[InfrastructureResource] = Field(default_factory=list)
    failed_resources: List[InfrastructureResource] = Field(default_factory=list)
    rollback_performed: bool = False
    total_duration_seconds: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        use_enum_values = True


class ProvisioningEvent(BaseModel):
    """Event emitted during provisioning"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    tenant_id: str
    resource_type: Optional[ResourceType] = None
    resource_name: Optional[str] = None
    status: ProvisioningStatus
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    details: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None
    
    class Config:
        use_enum_values = True


class ProvisioningError(Exception):
    """Custom exception for provisioning errors"""
    def __init__(self, message: str, resource_type: Optional[ResourceType] = None, 
                 details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.resource_type = resource_type
        self.details = details or {}
        super().__init__(self.message) 