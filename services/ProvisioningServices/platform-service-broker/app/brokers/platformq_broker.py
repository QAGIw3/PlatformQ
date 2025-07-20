"""
Platform Q Services Broker

Handles provisioning of Platform Q native services (Cassandra, Ignite, Pulsar, etc.)
through Cloudify orchestration.
"""

import uuid
import logging
from typing import Dict, Any, Tuple, Optional
from datetime import datetime

from ..models.osb_models import (
    CatalogResponse, Service, ServicePlan, ServiceMetadata, PlanMetadata,
    ProvisionResponse, UpdateResponse, DeprovisionResponse,
    BindResponse, UnbindResponse, LastOperationResponse,
    ProvisionRequest, UpdateRequest, BindRequest,
    ProvisionState, Schemas, ServiceInstanceSchema, InputParametersSchema,
    ServiceInstanceResponse, ServiceBindingResponse
)
from ..catalog import get_catalog, get_service, get_plan
from ..integrations.cloudify.client import CloudifyClient
from ..repository import ServiceInstanceRepository, ServiceBindingRepository
from ..config import BrokerConfig

logger = logging.getLogger(__name__)


class PlatformQBroker:
    """Platform Q services broker implementation"""
    
    def __init__(self, config: BrokerConfig):
        self.config = config
        self.cloudify = CloudifyClient(config.cloudify)
        self.instance_repo = ServiceInstanceRepository(config.database_url)
        self.binding_repo = ServiceBindingRepository(config.database_url)
        
    async def catalog(self) -> CatalogResponse:
        """Get service catalog"""
        catalog_data = get_catalog()
        
        services = []
        for service_data in catalog_data["services"]:
            # Convert catalog format to OSB models
            plans = []
            for plan_data in service_data["plans"]:
                plan = ServicePlan(
                    id=plan_data["id"],
                    name=plan_data["name"],
                    description=plan_data["description"],
                    metadata=PlanMetadata(**plan_data["metadata"]),
                    free=plan_data.get("free", False),
                    bindable=plan_data.get("bindable", True),
                    schemas=Schemas(**plan_data.get("schemas", {})) if plan_data.get("schemas") else None
                )
                plans.append(plan)
                
            service = Service(
                id=service_data["id"],
                name=service_data["name"],
                description=service_data["description"],
                tags=service_data.get("tags", []),
                requires=service_data.get("requires", []),
                bindable=service_data.get("bindable", True),
                metadata=ServiceMetadata(**service_data["metadata"]),
                dashboard_client=service_data.get("dashboard_client"),
                plan_updateable=service_data.get("plan_updateable", True),
                plans=plans
            )
            services.append(service)
            
        return CatalogResponse(services=services)
        
    async def provision(
        self,
        instance_id: str,
        request: ProvisionRequest,
        accepts_incomplete: bool
    ) -> Tuple[ProvisionResponse, int]:
        """Provision a service instance"""
        logger.info(f"Provisioning instance {instance_id} for service {request.service_id}")
        
        # Validate service and plan exist
        service = get_service(request.service_id)
        if not service:
            return ProvisionResponse(
                error="InvalidServiceId",
                description=f"Service {request.service_id} not found"
            ), 400
            
        plan = get_plan(request.service_id, request.plan_id)
        if not plan:
            return ProvisionResponse(
                error="InvalidPlanId",
                description=f"Plan {request.plan_id} not found"
            ), 400
            
        # Check if instance already exists
        existing = await self.instance_repo.get(instance_id)
        if existing:
            if existing.service_id == request.service_id and existing.plan_id == request.plan_id:
                # Instance already exists with same parameters
                return ProvisionResponse(), 200
            else:
                # Instance exists with different parameters
                return ProvisionResponse(
                    error="Conflict",
                    description="Instance already exists with different parameters"
                ), 409
                
        # Extract tenant information from context
        tenant_id = request.context.get("tenant_id")
        reseller_id = request.context.get("reseller_id")
        customer_id = request.context.get("customer_id")
        
        if not tenant_id:
            return ProvisionResponse(
                error="InvalidRequest",
                description="tenant_id is required in context"
            ), 400
            
        # Prepare Cloudify deployment inputs
        deployment_inputs = {
            "tenant_id": tenant_id,
            "reseller_id": reseller_id,
            "customer_id": customer_id,
            "service_id": request.service_id,
            "plan_id": request.plan_id,
            "instance_id": instance_id,
            "region": request.context.get("region", "default"),
            "environment": request.context.get("environment", "production")
        }
        
        # Add service-specific parameters
        if request.parameters:
            deployment_inputs.update(request.parameters)
            
        # Determine blueprint based on service
        blueprint_id = self._get_blueprint_id(request.service_id)
        deployment_id = f"service-{instance_id}"
        
        try:
            # Create Cloudify deployment
            if request.service_id == "platformq-bundle":
                # For bundle, provision all services
                await self._provision_bundle(
                    deployment_id,
                    deployment_inputs,
                    accepts_incomplete
                )
            else:
                # For individual service, use specific blueprint
                await self.cloudify.create_deployment(
                    deployment_id=deployment_id,
                    blueprint_id=blueprint_id,
                    inputs=deployment_inputs
                )
                
                if accepts_incomplete:
                    # Start installation workflow asynchronously
                    execution_id = await self.cloudify.execute_workflow(
                        deployment_id=deployment_id,
                        workflow_id="install"
                    )
                    
                    # Store instance in repository
                    await self.instance_repo.create(
                        instance_id=instance_id,
                        service_id=request.service_id,
                        plan_id=request.plan_id,
                        context=request.context,
                        parameters=request.parameters,
                        deployment_id=deployment_id,
                        operation="provision",
                        operation_data={"execution_id": execution_id}
                    )
                    
                    return ProvisionResponse(
                        operation=f"provision:{execution_id}"
                    ), 202
                else:
                    # Execute installation workflow synchronously
                    execution_id = await self.cloudify.execute_workflow(
                        deployment_id=deployment_id,
                        workflow_id="install",
                        wait=True
                    )
                    
                    # Get deployment outputs
                    outputs = await self.cloudify.get_deployment_outputs(deployment_id)
                    
                    # Store instance in repository
                    await self.instance_repo.create(
                        instance_id=instance_id,
                        service_id=request.service_id,
                        plan_id=request.plan_id,
                        context=request.context,
                        parameters=request.parameters,
                        deployment_id=deployment_id,
                        outputs=outputs,
                        state=ProvisionState.SUCCEEDED
                    )
                    
                    # Prepare dashboard URL
                    dashboard_url = self._get_dashboard_url(
                        request.service_id,
                        instance_id,
                        tenant_id
                    )
                    
                    return ProvisionResponse(
                        dashboard_url=dashboard_url
                    ), 201
                    
        except Exception as e:
            logger.error(f"Failed to provision instance {instance_id}: {str(e)}")
            
            # Store failed state
            await self.instance_repo.create(
                instance_id=instance_id,
                service_id=request.service_id,
                plan_id=request.plan_id,
                context=request.context,
                parameters=request.parameters,
                deployment_id=deployment_id,
                state=ProvisionState.FAILED,
                state_description=str(e)
            )
            
            return ProvisionResponse(
                error="ProvisioningFailed",
                description=str(e)
            ), 500
            
    async def update(
        self,
        instance_id: str,
        request: UpdateRequest,
        accepts_incomplete: bool
    ) -> Tuple[UpdateResponse, int]:
        """Update a service instance"""
        logger.info(f"Updating instance {instance_id}")
        
        # Get existing instance
        instance = await self.instance_repo.get(instance_id)
        if not instance:
            return UpdateResponse(
                error="InstanceNotFound",
                description=f"Instance {instance_id} not found"
            ), 404
            
        # Validate service and plan
        if request.service_id != instance.service_id:
            return UpdateResponse(
                error="InvalidRequest",
                description="Cannot change service_id"
            ), 400
            
        # Check if plan update is allowed
        service = get_service(request.service_id)
        if not service.get("plan_updateable", False):
            return UpdateResponse(
                error="PlanChangeNotSupported",
                description="Plan changes not supported for this service"
            ), 422
            
        # If plan is changing, validate new plan exists
        if request.plan_id and request.plan_id != instance.plan_id:
            new_plan = get_plan(request.service_id, request.plan_id)
            if not new_plan:
                return UpdateResponse(
                    error="InvalidPlanId",
                    description=f"Plan {request.plan_id} not found"
                ), 400
                
        deployment_id = instance.deployment_id
        
        try:
            # Prepare update inputs
            update_inputs = {}
            if request.parameters:
                update_inputs.update(request.parameters)
            if request.plan_id and request.plan_id != instance.plan_id:
                update_inputs["plan_id"] = request.plan_id
                
            if accepts_incomplete:
                # Update deployment asynchronously
                execution_id = await self.cloudify.update_deployment(
                    deployment_id=deployment_id,
                    inputs=update_inputs
                )
                
                # Update instance in repository
                await self.instance_repo.update_operation(
                    instance_id=instance_id,
                    operation="update",
                    operation_data={"execution_id": execution_id},
                    parameters=request.parameters,
                    plan_id=request.plan_id
                )
                
                return UpdateResponse(
                    operation=f"update:{execution_id}"
                ), 202
            else:
                # Update deployment synchronously
                execution_id = await self.cloudify.update_deployment(
                    deployment_id=deployment_id,
                    inputs=update_inputs,
                    wait=True
                )
                
                # Update instance in repository
                await self.instance_repo.update(
                    instance_id=instance_id,
                    plan_id=request.plan_id,
                    parameters=request.parameters,
                    state=ProvisionState.SUCCEEDED
                )
                
                return UpdateResponse(), 200
                
        except Exception as e:
            logger.error(f"Failed to update instance {instance_id}: {str(e)}")
            
            # Update failed state
            await self.instance_repo.update_state(
                instance_id=instance_id,
                state=ProvisionState.FAILED,
                state_description=str(e)
            )
            
            return UpdateResponse(
                error="UpdateFailed",
                description=str(e)
            ), 500
            
    async def deprovision(
        self,
        instance_id: str,
        service_id: str,
        plan_id: str,
        accepts_incomplete: bool
    ) -> Tuple[Optional[DeprovisionResponse], int]:
        """Deprovision a service instance"""
        logger.info(f"Deprovisioning instance {instance_id}")
        
        # Get instance from repository
        instance = await self.instance_repo.get(instance_id)
        if not instance:
            return DeprovisionResponse(), 410  # Gone
            
        # Check for existing bindings
        bindings = await self.binding_repo.list_by_instance(instance_id)
        if bindings:
            return DeprovisionResponse(
                error="ConcurrencyError",
                description="Instance has active bindings"
            ), 422
            
        deployment_id = instance.deployment_id
        
        try:
            if accepts_incomplete:
                # Execute uninstall workflow asynchronously
                execution_id = await self.cloudify.execute_workflow(
                    deployment_id=deployment_id,
                    workflow_id="uninstall"
                )
                
                # Update instance state
                await self.instance_repo.update_operation(
                    instance_id=instance_id,
                    operation="deprovision",
                    operation_data={"execution_id": execution_id}
                )
                
                return DeprovisionResponse(
                    operation=f"deprovision:{execution_id}"
                ), 202
            else:
                # Execute uninstall workflow synchronously
                await self.cloudify.execute_workflow(
                    deployment_id=deployment_id,
                    workflow_id="uninstall",
                    wait=True
                )
                
                # Delete deployment
                await self.cloudify.delete_deployment(deployment_id)
                
                # Delete from repository
                await self.instance_repo.delete(instance_id)
                
                return DeprovisionResponse(), 200
                
        except Exception as e:
            logger.error(f"Failed to deprovision instance {instance_id}: {str(e)}")
            
            # Update failed state
            await self.instance_repo.update_state(
                instance_id=instance_id,
                state=ProvisionState.FAILED,
                state_description=str(e)
            )
            
            return DeprovisionResponse(
                error="DeprovisioningFailed",
                description=str(e)
            ), 500
            
    async def bind(
        self,
        instance_id: str,
        binding_id: str,
        request: BindRequest,
        accepts_incomplete: bool
    ) -> Tuple[BindResponse, int]:
        """Create service binding"""
        logger.info(f"Creating binding {binding_id} for instance {instance_id}")
        
        # Get instance
        instance = await self.instance_repo.get(instance_id)
        if not instance:
            return BindResponse(
                error="InstanceNotFound",
                description=f"Instance {instance_id} not found"
            ), 404
            
        # Check if binding already exists
        existing = await self.binding_repo.get(binding_id)
        if existing:
            if existing.instance_id == instance_id:
                # Binding already exists
                return BindResponse(
                    credentials=existing.credentials
                ), 200
            else:
                # Binding exists for different instance
                return BindResponse(
                    error="Conflict",
                    description="Binding already exists for different instance"
                ), 409
                
        # Get service outputs from deployment
        outputs = await self.cloudify.get_deployment_outputs(instance.deployment_id)
        
        # Generate credentials based on service type
        credentials = self._generate_credentials(
            instance.service_id,
            instance_id,
            outputs
        )
        
        # Store binding
        await self.binding_repo.create(
            binding_id=binding_id,
            instance_id=instance_id,
            service_id=request.service_id,
            plan_id=request.plan_id,
            context=request.context,
            parameters=request.parameters,
            credentials=credentials
        )
        
        return BindResponse(credentials=credentials), 201
        
    async def unbind(
        self,
        instance_id: str,
        binding_id: str,
        service_id: str,
        plan_id: str,
        accepts_incomplete: bool
    ) -> Tuple[Optional[UnbindResponse], int]:
        """Remove service binding"""
        logger.info(f"Removing binding {binding_id} for instance {instance_id}")
        
        # Get binding
        binding = await self.binding_repo.get(binding_id)
        if not binding:
            return UnbindResponse(), 410  # Gone
            
        # Delete binding
        await self.binding_repo.delete(binding_id)
        
        return UnbindResponse(), 200
        
    async def last_operation(
        self,
        instance_id: str,
        service_id: Optional[str],
        plan_id: Optional[str],
        operation: Optional[str]
    ) -> Tuple[LastOperationResponse, int]:
        """Get last operation status"""
        logger.info(f"Getting last operation for instance {instance_id}")
        
        # Get instance
        instance = await self.instance_repo.get(instance_id)
        if not instance:
            return LastOperationResponse(
                state=ProvisionState.FAILED,
                description="Instance not found"
            ), 404
            
        # Check if operation is complete
        if instance.operation and instance.operation_data:
            execution_id = instance.operation_data.get("execution_id")
            if execution_id:
                execution = await self.cloudify.get_execution(execution_id)
                
                if execution["status"] == "terminated":
                    # Operation succeeded
                    await self.instance_repo.update_state(
                        instance_id=instance_id,
                        state=ProvisionState.SUCCEEDED
                    )
                    
                    return LastOperationResponse(
                        state=ProvisionState.SUCCEEDED,
                        description="Operation completed successfully"
                    ), 200
                    
                elif execution["status"] == "failed":
                    # Operation failed
                    error_msg = execution.get("error", "Unknown error")
                    await self.instance_repo.update_state(
                        instance_id=instance_id,
                        state=ProvisionState.FAILED,
                        state_description=error_msg
                    )
                    
                    return LastOperationResponse(
                        state=ProvisionState.FAILED,
                        description=error_msg
                    ), 200
                    
                else:
                    # Still in progress
                    return LastOperationResponse(
                        state=ProvisionState.IN_PROGRESS,
                        description=f"Execution status: {execution['status']}"
                    ), 200
                    
        # Return current state
        return LastOperationResponse(
            state=instance.state,
            description=instance.state_description
        ), 200
        
    async def get_instance(
        self,
        instance_id: str,
        service_id: Optional[str],
        plan_id: Optional[str]
    ) -> Tuple[ServiceInstanceResponse, int]:
        """Get service instance details (optional OSB endpoint)"""
        logger.info(f"Getting instance {instance_id}")
        
        # Get instance from repository
        instance = await self.instance_repo.get(instance_id)
        if not instance:
            return ServiceInstanceResponse(
                error="InstanceNotFound",
                description=f"Instance {instance_id} not found"
            ), 404
            
        # Get dashboard URL
        dashboard_url = self._get_dashboard_url(
            instance.service_id,
            instance_id,
            instance.context.get("tenant_id")
        )
        
        return ServiceInstanceResponse(
            service_id=instance.service_id,
            plan_id=instance.plan_id,
            dashboard_url=dashboard_url,
            parameters=instance.parameters
        ), 200
        
    async def get_binding(
        self,
        instance_id: str,
        binding_id: str,
        service_id: Optional[str],
        plan_id: Optional[str]
    ) -> Tuple[ServiceBindingResponse, int]:
        """Get service binding details (optional OSB endpoint)"""
        logger.info(f"Getting binding {binding_id} for instance {instance_id}")
        
        # Get binding from repository
        binding = await self.binding_repo.get(binding_id)
        if not binding:
            return ServiceBindingResponse(
                error="BindingNotFound",
                description=f"Binding {binding_id} not found"
            ), 404
            
        # Verify binding belongs to instance
        if binding.instance_id != instance_id:
            return ServiceBindingResponse(
                error="BindingNotFound",
                description=f"Binding {binding_id} not found for instance {instance_id}"
            ), 404
            
        return ServiceBindingResponse(
            credentials=binding.credentials,
            parameters=binding.parameters
        ), 200
        
    def _get_blueprint_id(self, service_id: str) -> str:
        """Get Cloudify blueprint ID for service"""
        blueprint_mapping = {
            "cassandra-service": "cassandra-cluster",
            "ignite-service": "ignite-cluster",
            "pulsar-service": "pulsar-cluster",
            "minio-service": "minio-cluster",
            "elasticsearch-service": "elasticsearch-cluster",
            "janusgraph-service": "janusgraph-cluster",
            "platformq-bundle": "tenant-infrastructure"
        }
        
        return blueprint_mapping.get(service_id, "generic-service")
        
    async def _provision_bundle(
        self,
        deployment_id: str,
        inputs: Dict[str, Any],
        accepts_incomplete: bool
    ):
        """Provision complete Platform Q bundle"""
        # Use the tenant-infrastructure blueprint we created
        await self.cloudify.create_deployment(
            deployment_id=deployment_id,
            blueprint_id="tenant-infrastructure",
            inputs=inputs
        )
        
    def _get_dashboard_url(
        self,
        service_id: str,
        instance_id: str,
        tenant_id: str
    ) -> str:
        """Generate dashboard URL for service"""
        base_url = self.config.dashboard_base_url
        
        service_dashboards = {
            "cassandra-service": f"{base_url}/cassandra/{tenant_id}",
            "ignite-service": f"{base_url}/ignite/{tenant_id}",
            "pulsar-service": f"{base_url}/pulsar/{tenant_id}",
            "minio-service": f"{base_url}/minio/{tenant_id}",
            "elasticsearch-service": f"{base_url}/kibana/{tenant_id}",
            "janusgraph-service": f"{base_url}/graphexp/{tenant_id}",
            "platformq-bundle": f"{base_url}/platform/{tenant_id}"
        }
        
        return service_dashboards.get(service_id, f"{base_url}/services/{instance_id}")
        
    def _generate_credentials(
        self,
        service_id: str,
        instance_id: str,
        outputs: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate service-specific credentials"""
        base_creds = {
            "instance_id": instance_id,
            "tenant_id": outputs.get("tenant_id")
        }
        
        if service_id == "cassandra-service":
            base_creds.update({
                "hosts": outputs.get("cassandra_hosts", []),
                "keyspace": outputs.get("keyspace"),
                "username": outputs.get("username"),
                "password": outputs.get("password"),
                "port": 9042
            })
        elif service_id == "ignite-service":
            base_creds.update({
                "hosts": outputs.get("ignite_hosts", []),
                "cache_name": outputs.get("cache_name"),
                "port": 10800,
                "thin_client_port": 10800,
                "rest_port": 8080
            })
        elif service_id == "pulsar-service":
            base_creds.update({
                "service_url": outputs.get("pulsar_service_url"),
                "admin_url": outputs.get("pulsar_admin_url"),
                "tenant": outputs.get("pulsar_tenant"),
                "namespace": outputs.get("namespace"),
                "auth_token": outputs.get("auth_token")
            })
        elif service_id == "minio-service":
            base_creds.update({
                "endpoint": outputs.get("minio_endpoint"),
                "bucket": outputs.get("bucket_name"),
                "access_key": outputs.get("access_key"),
                "secret_key": outputs.get("secret_key"),
                "secure": outputs.get("secure", False)
            })
        elif service_id == "elasticsearch-service":
            base_creds.update({
                "hosts": outputs.get("elasticsearch_hosts", []),
                "index_prefix": outputs.get("index_prefix"),
                "username": outputs.get("username"),
                "password": outputs.get("password"),
                "api_key": outputs.get("api_key")
            })
        elif service_id == "janusgraph-service":
            base_creds.update({
                "gremlin_endpoint": outputs.get("gremlin_endpoint"),
                "graph_name": outputs.get("graph_name"),
                "username": outputs.get("username"),
                "password": outputs.get("password")
            })
        elif service_id == "platformq-bundle":
            # For bundle, include all service credentials
            base_creds.update({
                "services": {
                    "cassandra": {
                        "keyspace": outputs.get("cassandra_keyspace"),
                        "hosts": outputs.get("cassandra_hosts", [])
                    },
                    "ignite": {
                        "cache": outputs.get("ignite_cache"),
                        "hosts": outputs.get("ignite_hosts", [])
                    },
                    "pulsar": {
                        "namespace": outputs.get("pulsar_namespace"),
                        "service_url": outputs.get("pulsar_service_url")
                    },
                    "minio": {
                        "bucket": outputs.get("minio_bucket"),
                        "endpoint": outputs.get("minio_endpoint")
                    },
                    "elasticsearch": {
                        "index_prefix": outputs.get("elasticsearch_index_prefix"),
                        "hosts": outputs.get("elasticsearch_hosts", [])
                    },
                    "janusgraph": {
                        "graph": outputs.get("janusgraph_graph"),
                        "gremlin": outputs.get("janusgraph_gremlin")
                    }
                },
                "vault_path": outputs.get("vault_path"),
                "consul_path": outputs.get("consul_path"),
                "kubernetes_namespace": outputs.get("kubernetes_namespace")
            })
            
        return base_creds 