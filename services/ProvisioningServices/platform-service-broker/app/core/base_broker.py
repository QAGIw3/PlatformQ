"""Base Service Broker Interface

Implements the Open Service Broker API specification
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Tuple
import logging
from datetime import datetime

from ..models.osb_models import (
    CatalogResponse,
    ProvisionRequest, ProvisionResponse,
    UpdateRequest, UpdateResponse,
    BindRequest, BindResponse,
    UnbindResponse,
    DeprovisionResponse,
    LastOperationResponse,
    ServiceInstanceResponse,
    ServiceBindingResponse,
    HierarchicalTenant
)
from ..integrations.cloudkitty.client import CloudKittyClient
from ..integrations.openmeter.client import OpenMeterClient, EventType

logger = logging.getLogger(__name__)


class ServiceBrokerInterface(ABC):
    """Abstract base class for OSB-compliant service brokers"""
    
    @abstractmethod
    async def catalog(self) -> CatalogResponse:
        """Return the service catalog"""
        pass
    
    @abstractmethod
    async def provision(
        self,
        instance_id: str,
        request: ProvisionRequest,
        accepts_incomplete: bool = False
    ) -> Tuple[ProvisionResponse, int]:
        """Provision a service instance
        
        Returns:
            Tuple of (response, http_status_code)
            - 201: Created
            - 202: Accepted (async)
            - 200: OK (instance already exists with same params)
            - 409: Conflict (instance exists with different params)
        """
        pass
    
    @abstractmethod
    async def update(
        self,
        instance_id: str,
        request: UpdateRequest,
        accepts_incomplete: bool = False
    ) -> Tuple[UpdateResponse, int]:
        """Update a service instance
        
        Returns:
            Tuple of (response, http_status_code)
            - 200: OK
            - 202: Accepted (async)
        """
        pass
    
    @abstractmethod
    async def deprovision(
        self,
        instance_id: str,
        service_id: str,
        plan_id: str,
        accepts_incomplete: bool = False
    ) -> Tuple[DeprovisionResponse, int]:
        """Deprovision a service instance
        
        Returns:
            Tuple of (response, http_status_code)
            - 200: OK
            - 202: Accepted (async)
            - 410: Gone (instance doesn't exist)
        """
        pass
    
    @abstractmethod
    async def bind(
        self,
        instance_id: str,
        binding_id: str,
        request: BindRequest,
        accepts_incomplete: bool = False
    ) -> Tuple[BindResponse, int]:
        """Create a service binding
        
        Returns:
            Tuple of (response, http_status_code)
            - 201: Created
            - 202: Accepted (async)
            - 200: OK (binding already exists with same params)
            - 409: Conflict (binding exists with different params)
        """
        pass
    
    @abstractmethod
    async def unbind(
        self,
        instance_id: str,
        binding_id: str,
        service_id: str,
        plan_id: str,
        accepts_incomplete: bool = False
    ) -> Tuple[UnbindResponse, int]:
        """Remove a service binding
        
        Returns:
            Tuple of (response, http_status_code)
            - 200: OK
            - 202: Accepted (async)
            - 410: Gone (binding doesn't exist)
        """
        pass
    
    @abstractmethod
    async def last_operation(
        self,
        instance_id: str,
        service_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        operation: Optional[str] = None
    ) -> Tuple[LastOperationResponse, int]:
        """Get the last operation status
        
        Returns:
            Tuple of (response, http_status_code)
            - 200: OK
            - 410: Gone (instance doesn't exist)
        """
        pass
    
    async def get_instance(
        self,
        instance_id: str,
        service_id: Optional[str] = None,
        plan_id: Optional[str] = None
    ) -> Tuple[ServiceInstanceResponse, int]:
        """Get a service instance (optional OSB feature)
        
        Returns:
            Tuple of (response, http_status_code)
            - 200: OK
            - 404: Not Found
        """
        # Default implementation returns not implemented
        return ServiceInstanceResponse(
            service_id="",
            plan_id=""
        ), 501
    
    async def get_binding(
        self,
        instance_id: str,
        binding_id: str,
        service_id: Optional[str] = None,
        plan_id: Optional[str] = None
    ) -> Tuple[ServiceBindingResponse, int]:
        """Get a service binding (optional OSB feature)
        
        Returns:
            Tuple of (response, http_status_code)
            - 200: OK
            - 404: Not Found
        """
        # Default implementation returns not implemented
        return ServiceBindingResponse(), 501


class BasePlatformBroker(ServiceBrokerInterface):
    """Base implementation with common Platform Q functionality"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._instances: Dict[str, Dict[str, Any]] = {}
        self._bindings: Dict[str, Dict[str, Any]] = {}
        self._operations: Dict[str, Dict[str, Any]] = {}
        
        # Initialize metering clients
        self.cloudkitty_client = CloudKittyClient(config)
        self.openmeter_client = OpenMeterClient(config)
    
    def _extract_tenant_hierarchy(self, context: Optional[Dict[str, Any]]) -> HierarchicalTenant:
        """Extract hierarchical tenant information from context"""
        if not context:
            raise ValueError("Context required for multi-tenant operations")
        
        return HierarchicalTenant(
            reseller_id=context.get("reseller_id", "default"),
            reseller_name=context.get("reseller_name", "Default Reseller"),
            customer_id=context.get("customer_id", context.get("organization_guid", "default")),
            customer_name=context.get("customer_name", context.get("organization_name", "Default Customer")),
            tenant_id=context.get("tenant_id", context.get("space_guid", "default")),
            tenant_name=context.get("tenant_name", context.get("space_name", "Default Tenant")),
            quotas=context.get("quotas", {}),
            metadata=context.get("metadata", {})
        )
    
    def _generate_operation_id(self, instance_id: str, operation_type: str) -> str:
        """Generate unique operation ID"""
        import uuid
        return f"{instance_id}-{operation_type}-{uuid.uuid4()}"
    
    async def _store_operation(
        self,
        operation_id: str,
        instance_id: str,
        operation_type: str,
        state: str = "in progress"
    ) -> None:
        """Store operation state"""
        self._operations[operation_id] = {
            "instance_id": instance_id,
            "type": operation_type,
            "state": state,
            "created_at": datetime.utcnow()
        }
    
    async def _validate_quota(
        self,
        tenant: HierarchicalTenant,
        resource_requirements: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """Validate tenant has sufficient quota
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # This will be implemented by specific brokers
        return True, None
    
    async def _report_usage(
        self,
        tenant: HierarchicalTenant,
        instance_id: str,
        usage_data: Dict[str, Any]
    ) -> None:
        """Report usage to CloudKitty/OpenMeter"""
        try:
            # Report to CloudKitty for billing
            await self.cloudkitty_client.report_usage(
                tenant=tenant,
                service_id=usage_data.get("service_id", "unknown"),
                instance_id=instance_id,
                usage_data=usage_data
            )
            
            # Report to OpenMeter for real-time analytics
            event_type = EventType.PROVISION if usage_data.get("action") == "provision" else EventType.USAGE
            await self.openmeter_client.ingest_event(
                event_type=event_type,
                tenant=tenant,
                service_id=usage_data.get("service_id", "unknown"),
                instance_id=instance_id,
                data=usage_data
            )
        except Exception as e:
            logger.error(f"Error reporting usage for instance {instance_id}: {e}")
    
    async def _create_cloudify_deployment(
        self,
        blueprint_id: str,
        deployment_id: str,
        inputs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create deployment in Cloudify"""
        # This will integrate with Cloudify API
        logger.info(f"Creating Cloudify deployment {deployment_id} from blueprint {blueprint_id}")
        return {
            "deployment_id": deployment_id,
            "status": "created"
        } 