"""Data models for Cloudify integration"""

from datetime import datetime
from typing import Dict, Any, Optional, List
from enum import Enum
from pydantic import BaseModel, Field


class CloudifyWorkflowState(str, Enum):
    """Cloudify workflow execution states"""
    PENDING = "pending"
    STARTED = "started"
    CANCELLING = "cancelling"
    FORCE_CANCELLING = "force_cancelling"
    CANCELLED = "cancelled"
    TERMINATED = "terminated"
    FAILED = "failed"
    QUEUED = "queued"
    SCHEDULED = "scheduled"


class CloudifyBlueprint(BaseModel):
    """Cloudify blueprint model"""
    id: str
    created_at: datetime
    main_file_name: str
    description: str = ""
    tenant_name: str


class CloudifyDeployment(BaseModel):
    """Cloudify deployment model"""
    id: str
    blueprint_id: str
    created_at: datetime
    tenant_name: str
    inputs: Dict[str, Any] = Field(default_factory=dict)
    outputs: Dict[str, Any] = Field(default_factory=dict)
    capabilities: Dict[str, Any] = Field(default_factory=dict)
    labels: List[Dict[str, str]] = Field(default_factory=list)


class CloudifyExecution(BaseModel):
    """Cloudify execution model"""
    id: str
    workflow_id: str
    deployment_id: str
    status: CloudifyWorkflowState
    created_at: datetime
    ended_at: Optional[datetime] = None
    error: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)


class BlueprintMetadata(BaseModel):
    """Metadata for Platform Q blueprints"""
    service_type: str  # e.g., "cassandra", "pulsar", "ignite"
    version: str
    tier: str  # "starter", "professional", "enterprise"
    description: str
    required_inputs: List[str] = Field(default_factory=list)
    optional_inputs: List[str] = Field(default_factory=list)
    outputs: List[str] = Field(default_factory=list)


class PlatformServiceDeployment(BaseModel):
    """Platform Q service deployment details"""
    tenant_id: str
    service_type: str
    deployment_id: str
    blueprint_id: str
    status: str
    created_at: datetime
    inputs: Dict[str, Any] = Field(default_factory=dict)
    outputs: Dict[str, Any] = Field(default_factory=dict)
    
    # Hierarchical tenant info
    reseller_id: Optional[str] = None
    customer_id: Optional[str] = None
    
    # Resource quotas and limits
    resource_limits: Dict[str, Any] = Field(default_factory=dict)
    
    # Billing metadata
    billing_metadata: Dict[str, Any] = Field(default_factory=dict) 