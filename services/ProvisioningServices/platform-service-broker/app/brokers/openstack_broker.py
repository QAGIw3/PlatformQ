"""OpenStack Service Broker

Implements OSB API for OpenStack compute, storage, and network resources.
Integrates with Cloudify for orchestration and CloudKitty for metering.
"""

import logging
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
import asyncio
import uuid

from openstack import connection
from openstack.exceptions import ResourceNotFound, ConflictException

from ..core.base_broker import BasePlatformBroker
from ..models.osb_models import (
    Service, ServicePlan, ServiceMetadata, ServicePlanMetadata,
    CatalogResponse, ProvisionRequest, ProvisionResponse,
    UpdateRequest, UpdateResponse, BindRequest, BindResponse,
    UnbindResponse, DeprovisionResponse, LastOperationResponse,
    LastOperationState, ErrorResponse
)
from ..integrations.cloudify.client import CloudifyClient as CloudifyIntegrationClient

logger = logging.getLogger(__name__)


class OpenStackBroker(BasePlatformBroker):
    """OpenStack resource broker implementation"""
    
    # Service and plan IDs
    COMPUTE_SERVICE_ID = "openstack-compute"
    STORAGE_SERVICE_ID = "openstack-storage"
    NETWORK_SERVICE_ID = "openstack-network"
    
    # Flavor mappings (similar to CloudStack instance types)
    FLAVOR_MAPPINGS = {
        "small": {"vcpus": 1, "ram": 2048, "disk": 20},
        "medium": {"vcpus": 2, "ram": 4096, "disk": 40},
        "large": {"vcpus": 4, "ram": 8192, "disk": 80},
        "xlarge": {"vcpus": 8, "ram": 16384, "disk": 160},
        "2xlarge": {"vcpus": 16, "ram": 32768, "disk": 320},
        "gpu.large": {"vcpus": 8, "ram": 32768, "disk": 200, "extra_specs": {"pci_passthrough:alias": "gpu:1"}},
        "gpu.xlarge": {"vcpus": 16, "ram": 65536, "disk": 400, "extra_specs": {"pci_passthrough:alias": "gpu:2"}}
    }
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        
        # OpenStack connection parameters
        self.auth_url = config["openstack"]["auth_url"]
        self.region_name = config["openstack"].get("region_name", "RegionOne")
        self.interface = config["openstack"].get("interface", "public")
        
        # Service account credentials
        self.service_project = config["openstack"]["service_project"]
        self.service_user = config["openstack"]["service_user"]
        self.service_password = config["openstack"]["service_password"]
        self.service_domain = config["openstack"].get("service_domain", "default")
        
        # Cloudify integration
        self.cloudify_url = config.get("cloudify", {}).get("url")
        self.cloudify_tenant = config.get("cloudify", {}).get("tenant", "default_tenant")
        self.cloudify_client = None
        if config.get("cloudify", {}).get("enabled", False):
            self.cloudify_client = CloudifyIntegrationClient(config)
        
        # CloudKitty integration
        self.cloudkitty_enabled = config.get("cloudkitty", {}).get("enabled", True)
        
        # Initialize OpenStack connection
        self.conn = None
        self._init_connection()
    
    def _init_connection(self):
        """Initialize OpenStack connection"""
        try:
            self.conn = connection.Connection(
                auth_url=self.auth_url,
                project_name=self.service_project,
                username=self.service_user,
                password=self.service_password,
                project_domain_name=self.service_domain,
                user_domain_name=self.service_domain,
                region_name=self.region_name,
                interface=self.interface
            )
            logger.info("Successfully connected to OpenStack")
        except Exception as e:
            logger.error(f"Failed to connect to OpenStack: {e}")
            raise
    
    async def catalog(self) -> CatalogResponse:
        """Return the OpenStack service catalog"""
        services = []
        
        # Compute Service
        compute_plans = []
        for flavor_name, specs in self.FLAVOR_MAPPINGS.items():
            plan = ServicePlan(
                id=f"compute-{flavor_name}",
                name=flavor_name,
                description=f"Virtual machine with {specs['vcpus']} vCPUs, {specs['ram']/1024}GB RAM, {specs['disk']}GB disk",
                metadata=ServicePlanMetadata(
                    displayName=f"Compute {flavor_name.upper()}",
                    bullets=[
                        f"{specs['vcpus']} vCPUs",
                        f"{specs['ram']/1024}GB RAM",
                        f"{specs['disk']}GB disk",
                        "99.9% availability SLA"
                    ],
                    costs=[{
                        "amount": {"usd": self._calculate_hourly_cost(specs)},
                        "unit": "HOURLY"
                    }]
                ),
                free=False
            )
            compute_plans.append(plan)
        
        compute_service = Service(
            id=self.COMPUTE_SERVICE_ID,
            name="openstack-compute",
            description="OpenStack compute instances with various configurations",
            bindable=True,
            instances_retrievable=True,
            bindings_retrievable=True,
            tags=["compute", "vm", "openstack", "iaas"],
            metadata=ServiceMetadata(
                displayName="OpenStack Compute",
                longDescription="Provision virtual machines on OpenStack infrastructure with flexible configurations",
                providerDisplayName="Platform Q Cloud Broker",
                documentationUrl="https://docs.platformq.io/openstack-compute"
            ),
            plans=compute_plans
        )
        services.append(compute_service)
        
        # Storage Service
        storage_plans = [
            ServicePlan(
                id="storage-standard",
                name="standard",
                description="Standard block storage with 3x replication",
                metadata=ServicePlanMetadata(
                    displayName="Standard Storage",
                    bullets=["3x replication", "99.99% durability", "Standard IOPS"]
                ),
                free=False
            ),
            ServicePlan(
                id="storage-premium",
                name="premium",
                description="Premium SSD storage with high IOPS",
                metadata=ServicePlanMetadata(
                    displayName="Premium Storage",
                    bullets=["SSD backend", "High IOPS", "Low latency", "99.99% durability"]
                ),
                free=False
            )
        ]
        
        storage_service = Service(
            id=self.STORAGE_SERVICE_ID,
            name="openstack-storage",
            description="OpenStack block storage volumes",
            bindable=True,
            tags=["storage", "volume", "openstack", "block-storage"],
            metadata=ServiceMetadata(
                displayName="OpenStack Storage",
                longDescription="Provision persistent block storage volumes"
            ),
            plans=storage_plans
        )
        services.append(storage_service)
        
        return CatalogResponse(services=services)
    
    async def provision(
        self,
        instance_id: str,
        request: ProvisionRequest,
        accepts_incomplete: bool = False
    ) -> Tuple[ProvisionResponse, int]:
        """Provision OpenStack resources"""
        
        # Extract tenant hierarchy
        tenant = self._extract_tenant_hierarchy(request.context.dict() if request.context else {})
        
        # Validate quota
        is_valid, error_msg = await self._validate_quota(tenant, request.parameters or {})
        if not is_valid:
            return ProvisionResponse(), 403  # Forbidden due to quota
        
        # Check if instance already exists
        if instance_id in self._instances:
            existing = self._instances[instance_id]
            if (existing["service_id"] == request.service_id and 
                existing["plan_id"] == request.plan_id):
                return ProvisionResponse(), 200  # Already exists, idempotent
            else:
                return ProvisionResponse(), 409  # Conflict
        
        try:
            if request.service_id == self.COMPUTE_SERVICE_ID:
                response = await self._provision_compute(instance_id, request, tenant)
            elif request.service_id == self.STORAGE_SERVICE_ID:
                response = await self._provision_storage(instance_id, request, tenant)
            elif request.service_id == self.NETWORK_SERVICE_ID:
                response = await self._provision_network(instance_id, request, tenant)
            else:
                return ProvisionResponse(), 400  # Bad request
            
            # Store instance metadata
            self._instances[instance_id] = {
                "service_id": request.service_id,
                "plan_id": request.plan_id,
                "tenant": tenant.dict(),
                "parameters": request.parameters,
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Report initial usage
            await self._report_usage(tenant, instance_id, {
                "service_id": request.service_id,
                "plan_id": request.plan_id,
                "action": "provision",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            if accepts_incomplete and response.operation:
                return response, 202  # Accepted, async operation
            else:
                return response, 201  # Created
                
        except Exception as e:
            logger.error(f"Failed to provision instance {instance_id}: {e}")
            return ProvisionResponse(), 500
    
    async def _provision_compute(
        self,
        instance_id: str,
        request: ProvisionRequest,
        tenant: Any
    ) -> ProvisionResponse:
        """Provision compute instance"""
        
        # Extract flavor from plan_id (e.g., "compute-medium" -> "medium")
        flavor_name = request.plan_id.replace("compute-", "")
        flavor_specs = self.FLAVOR_MAPPINGS.get(flavor_name)
        
        if not flavor_specs:
            raise ValueError(f"Invalid flavor: {flavor_name}")
        
        # Get or create OpenStack project for tenant
        project = await self._ensure_tenant_project(tenant)
        
        # Create instance in tenant's project
        params = request.parameters or {}
        
        # Find or create flavor
        flavor = self._find_or_create_flavor(flavor_name, flavor_specs)
        
        # Get image
        image_name = params.get("image", "ubuntu-22.04")
        image = self.conn.compute.find_image(image_name)
        
        if not image:
            raise ValueError(f"Image not found: {image_name}")
        
        # Get or create network
        network = await self._ensure_tenant_network(project)
        
        # Create server
        server_name = f"platformq-{instance_id}"
        
        # If using Cloudify, create via blueprint
        if self.cloudify_url and params.get("use_cloudify", True):
            operation_id = await self._provision_via_cloudify(
                instance_id, "compute", {
                    "server_name": server_name,
                    "flavor_id": flavor.id,
                    "image_id": image.id,
                    "network_id": network.id,
                    "project_id": project.id,
                    "metadata": {
                        "platformq_instance_id": instance_id,
                        "tenant_id": tenant.tenant_id,
                        "service_id": request.service_id,
                        "plan_id": request.plan_id
                    }
                }
            )
            return ProvisionResponse(operation=operation_id)
        else:
            # Direct OpenStack provisioning
            server = self.conn.compute.create_server(
                name=server_name,
                image_id=image.id,
                flavor_id=flavor.id,
                networks=[{"uuid": network.id}],
                metadata={
                    "platformq_instance_id": instance_id,
                    "tenant_id": tenant.tenant_id,
                    "service_id": request.service_id,
                    "plan_id": request.plan_id
                }
            )
            
            # Wait for active state if synchronous
            if params.get("wait_for_active", False):
                self.conn.compute.wait_for_server(server)
            
            return ProvisionResponse(
                dashboard_url=f"https://horizon.platformq.io/project/instances/{server.id}/"
            )
    
    async def _provision_storage(
        self,
        instance_id: str,
        request: ProvisionRequest,
        tenant: Any
    ) -> ProvisionResponse:
        """Provision storage volume"""
        
        params = request.parameters or {}
        size_gb = params.get("size_gb", 100)
        volume_type = "premium" if "premium" in request.plan_id else "standard"
        
        # Get tenant project
        project = await self._ensure_tenant_project(tenant)
        
        # Create volume
        volume_name = f"platformq-vol-{instance_id}"
        
        volume = self.conn.block_storage.create_volume(
            name=volume_name,
            size=size_gb,
            volume_type=volume_type,
            metadata={
                "platformq_instance_id": instance_id,
                "tenant_id": tenant.tenant_id,
                "service_id": request.service_id,
                "plan_id": request.plan_id
            }
        )
        
        return ProvisionResponse(
            dashboard_url=f"https://horizon.platformq.io/project/volumes/{volume.id}/"
        )
    
    async def deprovision(
        self,
        instance_id: str,
        service_id: str,
        plan_id: str,
        accepts_incomplete: bool = False
    ) -> Tuple[DeprovisionResponse, int]:
        """Deprovision OpenStack resources"""
        
        if instance_id not in self._instances:
            return DeprovisionResponse(), 410  # Gone
        
        instance_data = self._instances[instance_id]
        
        try:
            if service_id == self.COMPUTE_SERVICE_ID:
                await self._deprovision_compute(instance_id)
            elif service_id == self.STORAGE_SERVICE_ID:
                await self._deprovision_storage(instance_id)
            elif service_id == self.NETWORK_SERVICE_ID:
                await self._deprovision_network(instance_id)
            
            # Remove from tracking
            del self._instances[instance_id]
            
            # Report usage
            tenant = self._extract_tenant_hierarchy(instance_data["tenant"])
            await self._report_usage(tenant, instance_id, {
                "service_id": service_id,
                "plan_id": plan_id,
                "action": "deprovision",
                "timestamp": datetime.utcnow().isoformat()
            })
            
            return DeprovisionResponse(), 200
            
        except Exception as e:
            logger.error(f"Failed to deprovision instance {instance_id}: {e}")
            return DeprovisionResponse(), 500
    
    async def _deprovision_compute(self, instance_id: str):
        """Delete compute instance"""
        server_name = f"platformq-{instance_id}"
        servers = list(self.conn.compute.servers(name=server_name))
        
        for server in servers:
            self.conn.compute.delete_server(server, force=True)
    
    async def _deprovision_storage(self, instance_id: str):
        """Delete storage volume"""
        volume_name = f"platformq-vol-{instance_id}"
        volumes = list(self.conn.block_storage.volumes(name=volume_name))
        
        for volume in volumes:
            # Detach if attached
            if volume.attachments:
                for attachment in volume.attachments:
                    self.conn.compute.detach_volume(
                        attachment["server_id"],
                        volume
                    )
            self.conn.block_storage.delete_volume(volume)
    
    async def bind(
        self,
        instance_id: str,
        binding_id: str,
        request: BindRequest,
        accepts_incomplete: bool = False
    ) -> Tuple[BindResponse, int]:
        """Create service binding"""
        
        if instance_id not in self._instances:
            return BindResponse(), 404
        
        instance_data = self._instances[instance_id]
        
        # Check if binding already exists
        binding_key = f"{instance_id}:{binding_id}"
        if binding_key in self._bindings:
            return BindResponse(), 200  # Already exists
        
        try:
            if instance_data["service_id"] == self.COMPUTE_SERVICE_ID:
                credentials = await self._bind_compute(instance_id, binding_id, request)
            elif instance_data["service_id"] == self.STORAGE_SERVICE_ID:
                credentials = await self._bind_storage(instance_id, binding_id, request)
            else:
                credentials = {}
            
            self._bindings[binding_key] = {
                "instance_id": instance_id,
                "binding_id": binding_id,
                "credentials": credentials,
                "created_at": datetime.utcnow().isoformat()
            }
            
            return BindResponse(credentials=credentials), 201
            
        except Exception as e:
            logger.error(f"Failed to bind {binding_id} to instance {instance_id}: {e}")
            return BindResponse(), 500
    
    async def _bind_compute(
        self,
        instance_id: str,
        binding_id: str,
        request: BindRequest
    ) -> Dict[str, Any]:
        """Create compute instance binding (SSH credentials)"""
        
        server_name = f"platformq-{instance_id}"
        servers = list(self.conn.compute.servers(name=server_name))
        
        if not servers:
            raise ResourceNotFound(f"Server not found: {server_name}")
        
        server = servers[0]
        
        # Get server IPs
        addresses = server.addresses
        private_ip = None
        public_ip = None
        
        for network_name, addrs in addresses.items():
            for addr in addrs:
                if addr["OS-EXT-IPS:type"] == "fixed":
                    private_ip = addr["addr"]
                elif addr["OS-EXT-IPS:type"] == "floating":
                    public_ip = addr["addr"]
        
        # Generate or retrieve SSH key
        key_name = f"platformq-{instance_id}-{binding_id}"
        keypair = self.conn.compute.create_keypair(name=key_name)
        
        return {
            "hostname": public_ip or private_ip,
            "private_ip": private_ip,
            "public_ip": public_ip,
            "ssh_user": "ubuntu",  # Default for Ubuntu images
            "ssh_private_key": keypair.private_key,
            "server_id": server.id
        }
    
    async def unbind(
        self,
        instance_id: str,
        binding_id: str,
        service_id: str,
        plan_id: str,
        accepts_incomplete: bool = False
    ) -> Tuple[UnbindResponse, int]:
        """Remove service binding"""
        
        binding_key = f"{instance_id}:{binding_id}"
        
        if binding_key not in self._bindings:
            return UnbindResponse(), 410  # Gone
        
        try:
            # Clean up binding-specific resources
            if service_id == self.COMPUTE_SERVICE_ID:
                # Delete SSH keypair
                key_name = f"platformq-{instance_id}-{binding_id}"
                try:
                    keypair = self.conn.compute.find_keypair(key_name)
                    if keypair:
                        self.conn.compute.delete_keypair(keypair)
                except:
                    pass
            
            del self._bindings[binding_key]
            
            return UnbindResponse(), 200
            
        except Exception as e:
            logger.error(f"Failed to unbind {binding_id} from instance {instance_id}: {e}")
            return UnbindResponse(), 500
    
    async def update(
        self,
        instance_id: str,
        request: UpdateRequest,
        accepts_incomplete: bool = False
    ) -> Tuple[UpdateResponse, int]:
        """Update service instance (resize, change plan, etc.)"""
        
        if instance_id not in self._instances:
            return UpdateResponse(), 404
        
        instance_data = self._instances[instance_id]
        
        # For compute instances, this could mean resizing
        if instance_data["service_id"] == self.COMPUTE_SERVICE_ID:
            if request.plan_id and request.plan_id != instance_data["plan_id"]:
                # Resize operation
                operation_id = await self._resize_compute(
                    instance_id,
                    instance_data["plan_id"],
                    request.plan_id
                )
                
                # Update stored plan
                instance_data["plan_id"] = request.plan_id
                
                if accepts_incomplete:
                    return UpdateResponse(operation=operation_id), 202
        
        return UpdateResponse(), 200
    
    async def last_operation(
        self,
        instance_id: str,
        service_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        operation: Optional[str] = None
    ) -> Tuple[LastOperationResponse, int]:
        """Get last operation status"""
        
        if operation and operation in self._operations:
            op_data = self._operations[operation]
            
            # Check actual status from Cloudify or OpenStack
            if op_data.get("cloudify_execution_id"):
                state = await self._check_cloudify_execution(
                    op_data["cloudify_execution_id"]
                )
            else:
                state = LastOperationState.SUCCEEDED
            
            return LastOperationResponse(
                state=state,
                description=f"Operation {op_data['type']} is {state}"
            ), 200
        
        return LastOperationResponse(
            state=LastOperationState.SUCCEEDED,
            description="No active operations"
        ), 200
    
    # Helper methods
    
    async def _ensure_tenant_project(self, tenant: Any) -> Any:
        """Ensure OpenStack project exists for tenant"""
        project_name = f"platformq-{tenant.tenant_id}"
        
        # Check if project exists
        project = self.conn.identity.find_project(project_name)
        
        if not project:
            # Create project with hierarchical structure
            parent_project = None
            if tenant.customer_id != "default":
                parent_name = f"platformq-customer-{tenant.customer_id}"
                parent_project = self.conn.identity.find_project(parent_name)
                if not parent_project:
                    parent_project = self.conn.identity.create_project(
                        name=parent_name,
                        description=f"Customer: {tenant.customer_name}",
                        domain_id="default"
                    )
            
            project = self.conn.identity.create_project(
                name=project_name,
                description=f"Tenant: {tenant.tenant_name}",
                domain_id="default",
                parent_id=parent_project.id if parent_project else None
            )
            
            # Set quotas based on tenant tier
            await self._set_project_quotas(project, tenant.quotas)
        
        return project
    
    async def _ensure_tenant_network(self, project: Any) -> Any:
        """Ensure network exists for tenant project"""
        network_name = f"{project.name}-network"
        
        # Switch to project context
        networks = list(self.conn.network.networks(
            name=network_name,
            project_id=project.id
        ))
        
        if not networks:
            # Create network and subnet
            network = self.conn.network.create_network(
                name=network_name,
                project_id=project.id
            )
            
            subnet = self.conn.network.create_subnet(
                name=f"{network_name}-subnet",
                network_id=network.id,
                project_id=project.id,
                cidr="10.0.0.0/24",
                ip_version=4,
                enable_dhcp=True
            )
            
            # Create router and attach
            router = self.conn.network.create_router(
                name=f"{project.name}-router",
                project_id=project.id,
                external_gateway_info={
                    "network_id": self._get_external_network().id
                }
            )
            
            self.conn.network.add_interface_to_router(
                router,
                subnet_id=subnet.id
            )
            
            return network
        
        return networks[0]
    
    def _find_or_create_flavor(self, name: str, specs: Dict[str, Any]) -> Any:
        """Find or create compute flavor"""
        flavor_name = f"platformq.{name}"
        
        flavor = self.conn.compute.find_flavor(flavor_name)
        
        if not flavor:
            flavor = self.conn.compute.create_flavor(
                name=flavor_name,
                ram=specs["ram"],
                vcpus=specs["vcpus"],
                disk=specs["disk"],
                is_public=True
            )
            
            # Set extra specs if needed (e.g., for GPU)
            if "extra_specs" in specs:
                self.conn.compute.set_flavor_specs(
                    flavor,
                    specs["extra_specs"]
                )
        
        return flavor
    
    def _calculate_hourly_cost(self, specs: Dict[str, Any]) -> float:
        """Calculate hourly cost based on resource specs"""
        # Base costs per unit
        cpu_cost = 0.02  # per vCPU per hour
        ram_cost = 0.005  # per GB per hour
        disk_cost = 0.0001  # per GB per hour
        gpu_cost = 0.50  # per GPU per hour
        
        cost = (
            specs["vcpus"] * cpu_cost +
            (specs["ram"] / 1024) * ram_cost +
            specs["disk"] * disk_cost
        )
        
        # Add GPU cost if applicable
        if "extra_specs" in specs and "gpu" in str(specs.get("extra_specs", {})):
            gpu_count = int(specs["extra_specs"].get("pci_passthrough:alias", "gpu:1").split(":")[1])
            cost += gpu_count * gpu_cost
        
        return round(cost, 3)
    
    async def _provision_via_cloudify(
        self,
        instance_id: str,
        resource_type: str,
        inputs: Dict[str, Any]
    ) -> str:
        """Provision resources using Cloudify blueprints"""
        
        deployment_id = f"platformq-{instance_id}"
        blueprint_id = f"platformq-{resource_type}-blueprint"
        
        # This would integrate with Cloudify API
        deployment = await self._create_cloudify_deployment(
            blueprint_id,
            deployment_id,
            inputs
        )
        
        # Store operation for tracking
        operation_id = self._generate_operation_id(instance_id, "provision")
        await self._store_operation(
            operation_id,
            instance_id,
            "provision",
            "in progress"
        )
        
        return operation_id
    
    def _get_external_network(self) -> Any:
        """Get external network for router gateway"""
        # Find network marked as external
        networks = self.conn.network.networks(is_router_external=True)
        for network in networks:
            return network
        
        raise ValueError("No external network found")
    
    async def _set_project_quotas(self, project: Any, quotas: Dict[str, Any]):
        """Set OpenStack quotas for project"""
        
        # Compute quotas
        compute_quotas = {
            "instances": quotas.get("max_instances", 10),
            "cores": quotas.get("max_vcpus", 20),
            "ram": quotas.get("max_ram_mb", 40960),
        }
        self.conn.compute.update_quota_set(project.id, **compute_quotas)
        
        # Storage quotas
        storage_quotas = {
            "volumes": quotas.get("max_volumes", 10),
            "gigabytes": quotas.get("max_storage_gb", 1000),
        }
        self.conn.block_storage.update_quota_set(project.id, **storage_quotas)
        
        # Network quotas
        network_quotas = {
            "network": quotas.get("max_networks", 5),
            "subnet": quotas.get("max_subnets", 10),
            "port": quotas.get("max_ports", 50),
            "router": quotas.get("max_routers", 2),
            "floatingip": quotas.get("max_floating_ips", 5),
        }
        self.conn.network.update_quota(project.id, **network_quotas) 