"""Open Service Broker API Models

Based on OSB API v2.16 specification
https://github.com/openservicebrokerapi/servicebroker/blob/v2.16/spec.md
"""

from enum import Enum
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator
import uuid


class ServicePlanFeatures(BaseModel):
    """Features supported by a service plan"""
    bindable: Optional[bool] = None
    plan_updateable: Optional[bool] = None


class ServicePlanMetadata(BaseModel):
    """Service plan metadata"""
    displayName: Optional[str] = None
    bullets: Optional[List[str]] = None
    costs: Optional[List[Dict[str, Any]]] = None
    attributes: Optional[Dict[str, Any]] = None
    

class ServicePlanSchemas(BaseModel):
    """Schemas for service plan operations"""
    service_instance: Optional[Dict[str, Any]] = None
    service_binding: Optional[Dict[str, Any]] = None


class ServicePlan(BaseModel):
    """Service plan definition"""
    id: str = Field(..., description="Globally unique ID")
    name: str = Field(..., description="CLI-friendly name")
    description: str
    metadata: Optional[ServicePlanMetadata] = None
    free: bool = True
    bindable: Optional[bool] = None
    plan_updateable: Optional[bool] = None
    schemas: Optional[ServicePlanSchemas] = None
    maximum_polling_duration: Optional[int] = None
    maintenance_info: Optional[Dict[str, Any]] = None


class ServiceMetadata(BaseModel):
    """Service metadata"""
    displayName: Optional[str] = None
    imageUrl: Optional[str] = None
    longDescription: Optional[str] = None
    providerDisplayName: Optional[str] = None
    documentationUrl: Optional[str] = None
    supportUrl: Optional[str] = None


class ServiceDashboardClient(BaseModel):
    """OAuth2 client for service dashboard"""
    id: str
    secret: str
    redirect_uri: str


class Service(BaseModel):
    """Service offering definition"""
    id: str = Field(..., description="Globally unique ID")
    name: str = Field(..., description="CLI-friendly name")
    description: str
    tags: Optional[List[str]] = None
    requires: Optional[List[str]] = None
    bindable: bool
    instances_retrievable: bool = False
    bindings_retrievable: bool = False
    allow_context_updates: bool = False
    metadata: Optional[ServiceMetadata] = None
    dashboard_client: Optional[ServiceDashboardClient] = None
    plan_updateable: bool = False
    plans: List[ServicePlan]


class CatalogResponse(BaseModel):
    """Service broker catalog response"""
    services: List[Service]


class Context(BaseModel):
    """Platform-specific context"""
    platform: str
    organization_guid: Optional[str] = None
    space_guid: Optional[str] = None
    organization_name: Optional[str] = None
    space_name: Optional[str] = None
    # Additional fields for hierarchical multi-tenancy
    reseller_id: Optional[str] = None
    customer_id: Optional[str] = None
    tenant_id: Optional[str] = None


class PreviousValues(BaseModel):
    """Previous values for updates"""
    service_id: Optional[str] = None
    plan_id: Optional[str] = None
    organization_id: Optional[str] = None
    space_id: Optional[str] = None


class MaintenanceInfo(BaseModel):
    """Maintenance information"""
    version: str
    description: Optional[str] = None


class ProvisionRequest(BaseModel):
    """Service instance provisioning request"""
    service_id: str
    plan_id: str
    context: Optional[Context] = None
    organization_guid: str
    space_guid: str
    parameters: Optional[Dict[str, Any]] = None
    maintenance_info: Optional[MaintenanceInfo] = None


class ProvisionResponse(BaseModel):
    """Service instance provisioning response"""
    dashboard_url: Optional[str] = None
    operation: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class UpdateRequest(BaseModel):
    """Service instance update request"""
    context: Optional[Context] = None
    service_id: str
    plan_id: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    previous_values: Optional[PreviousValues] = None
    maintenance_info: Optional[MaintenanceInfo] = None


class UpdateResponse(BaseModel):
    """Service instance update response"""
    operation: Optional[str] = None
    dashboard_url: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class BindRequest(BaseModel):
    """Service binding request"""
    context: Optional[Context] = None
    service_id: str
    plan_id: str
    app_guid: Optional[str] = None
    bind_resource: Optional[Dict[str, Any]] = None
    parameters: Optional[Dict[str, Any]] = None


class BindResponse(BaseModel):
    """Service binding response"""
    operation: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None
    syslog_drain_url: Optional[str] = None
    route_service_url: Optional[str] = None
    volume_mounts: Optional[List[Dict[str, Any]]] = None
    endpoints: Optional[List[Dict[str, Any]]] = None


class UnbindResponse(BaseModel):
    """Service unbinding response"""
    operation: Optional[str] = None


class DeprovisionResponse(BaseModel):
    """Service instance deprovisioning response"""
    operation: Optional[str] = None


class LastOperationState(str, Enum):
    """State of last operation"""
    IN_PROGRESS = "in progress"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class LastOperationResponse(BaseModel):
    """Last operation response"""
    state: LastOperationState
    description: Optional[str] = None
    instance_usable: bool = True
    update_repeatable: bool = True


class ServiceInstanceResponse(BaseModel):
    """Service instance fetch response"""
    service_id: str
    plan_id: str
    dashboard_url: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None


class ServiceBindingResponse(BaseModel):
    """Service binding fetch response"""
    credentials: Optional[Dict[str, Any]] = None
    syslog_drain_url: Optional[str] = None
    route_service_url: Optional[str] = None
    volume_mounts: Optional[List[Dict[str, Any]]] = None
    endpoints: Optional[List[Dict[str, Any]]] = None
    parameters: Optional[Dict[str, Any]] = None


class ErrorResponse(BaseModel):
    """Error response"""
    error: str
    description: Optional[str] = None
    instance_usable: Optional[bool] = None
    update_repeatable: Optional[bool] = None


# Platform Q specific extensions
class HierarchicalTenant(BaseModel):
    """Hierarchical tenant structure for cloud brokerage"""
    reseller_id: str
    reseller_name: str
    customer_id: str
    customer_name: str
    tenant_id: str
    tenant_name: str
    quotas: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ResourceOffering(BaseModel):
    """Cloud resource offering"""
    offering_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: str
    resource_type: str  # compute, storage, network, platform_service
    provider: str  # openstack, kubernetes, crossplane
    specifications: Dict[str, Any]
    pricing: Dict[str, Any]
    availability: Dict[str, Any]
    constraints: Dict[str, Any] = Field(default_factory=dict) 