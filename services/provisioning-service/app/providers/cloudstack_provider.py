"""
CloudStack Provider for Unified Compute Management

Manages compute resources across multiple CloudStack zones including
partner infrastructure, on-premise resources, and edge locations.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from decimal import Decimal
import uuid
import hashlib
import hmac
import base64
from urllib.parse import quote

import httpx
from pydantic import BaseModel, Field

from app.providers.base import ComputeProvider
from app.models import ResourceSpec, ResourceAllocation, AllocationStatus, ResourceType

logger = logging.getLogger(__name__)


class CloudStackConfig(BaseModel):
    """CloudStack connection configuration"""
    api_url: str
    api_key: str
    secret_key: str
    zones: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    default_zone_id: Optional[str] = None
    verify_ssl: bool = True
    timeout: int = 60


class ServiceOffering(BaseModel):
    """CloudStack service offering"""
    id: str
    name: str
    displaytext: str
    cpunumber: int
    cpuspeed: int  # MHz
    memory: int  # MB
    resource_type: ResourceType
    tags: Dict[str, str] = Field(default_factory=dict)


class CloudStackZone(BaseModel):
    """CloudStack zone information"""
    id: str
    name: str
    description: str
    capacity: Dict[str, float]
    location: str
    provider_type: str  # partner, on_premise, edge
    partner_id: Optional[str] = None


class CloudStackProvider(ComputeProvider):
    """
    CloudStack provider implementation for unified compute management
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.cloudstack_config = CloudStackConfig(**config.get("cloudstack", {}))
        self.client = httpx.AsyncClient(
            base_url=self.cloudstack_config.api_url,
            verify=self.cloudstack_config.verify_ssl,
            timeout=self.cloudstack_config.timeout
        )
        
        # Cache for service offerings and zones
        self.service_offerings: Dict[str, ServiceOffering] = {}
        self.zones: Dict[str, CloudStackZone] = {}
        self._cache_timestamp = None
        self._cache_ttl = 300  # 5 minutes
        
        # Resource mapping
        self.resource_mappings = {
            ResourceType.CPU: {"tags": ["cpu-optimized"], "min_cpu": 4},
            ResourceType.GPU: {"tags": ["gpu-enabled"], "min_gpu": 1},
            ResourceType.TPU: {"tags": ["tpu-enabled"], "min_tpu": 1},
            ResourceType.MEMORY: {"tags": ["memory-optimized"], "min_memory": 32768},
            ResourceType.STORAGE: {"tags": ["storage-optimized"], "min_storage": 1000}
        }
        
    async def _sign_request(self, params: Dict[str, str]) -> str:
        """Sign CloudStack API request"""
        # Add API key
        params["apiKey"] = self.cloudstack_config.api_key
        params["response"] = "json"
        
        # Sort parameters
        sorted_params = sorted(params.items(), key=lambda x: x[0].lower())
        
        # Create query string
        query_string = "&".join([f"{k}={quote(str(v))}" for k, v in sorted_params])
        
        # Generate signature
        signature = hmac.new(
            self.cloudstack_config.secret_key.encode('utf-8'),
            query_string.lower().encode('utf-8'),
            hashlib.sha1
        ).digest()
        
        # Base64 encode and URL encode
        signature_b64 = base64.b64encode(signature).decode('utf-8')
        params["signature"] = signature_b64
        
        return query_string + f"&signature={quote(signature_b64)}"
        
    async def _api_call(self, command: str, params: Dict[str, Any] = None) -> Dict:
        """Make CloudStack API call"""
        if params is None:
            params = {}
            
        params["command"] = command
        
        # Sign request
        signed_query = await self._sign_request(params.copy())
        
        # Make request
        response = await self.client.get("", params=signed_query)
        
        if response.status_code != 200:
            raise Exception(f"CloudStack API error: {response.status_code} - {response.text}")
            
        result = response.json()
        
        # Check for API error
        if "errorresponse" in result:
            error = result["errorresponse"]
            raise Exception(f"CloudStack error: {error.get('errortext', 'Unknown error')}")
            
        return result
        
    async def _refresh_cache(self):
        """Refresh service offerings and zones cache"""
        now = datetime.utcnow()
        
        if (self._cache_timestamp and 
            (now - self._cache_timestamp).total_seconds() < self._cache_ttl):
            return
            
        # Get zones
        zones_response = await self._api_call("listZones")
        for zone_data in zones_response.get("listzonesresponse", {}).get("zone", []):
            zone = CloudStackZone(
                id=zone_data["id"],
                name=zone_data["name"],
                description=zone_data.get("description", ""),
                capacity=await self._get_zone_capacity(zone_data["id"]),
                location=zone_data.get("tags", {}).get("location", zone_data["name"]),
                provider_type=zone_data.get("tags", {}).get("provider_type", "on_premise"),
                partner_id=zone_data.get("tags", {}).get("partner_id")
            )
            self.zones[zone.id] = zone
            
        # Get service offerings
        offerings_response = await self._api_call("listServiceOfferings")
        for offering_data in offerings_response.get("listserviceofferingsresponse", {}).get("serviceoffering", []):
            
            # Determine resource type from tags
            resource_type = ResourceType.CPU  # default
            tags = offering_data.get("tags", "").split(",") if offering_data.get("tags") else []
            
            if "gpu-enabled" in tags:
                resource_type = ResourceType.GPU
            elif "tpu-enabled" in tags:
                resource_type = ResourceType.TPU
            elif "memory-optimized" in tags:
                resource_type = ResourceType.MEMORY
            elif "storage-optimized" in tags:
                resource_type = ResourceType.STORAGE
                
            offering = ServiceOffering(
                id=offering_data["id"],
                name=offering_data["name"],
                displaytext=offering_data["displaytext"],
                cpunumber=offering_data["cpunumber"],
                cpuspeed=offering_data["cpuspeed"],
                memory=offering_data["memory"],
                resource_type=resource_type,
                tags={tag.split(":")[0]: tag.split(":")[1] if ":" in tag else "true" 
                      for tag in tags}
            )
            self.service_offerings[offering.id] = offering
            
        self._cache_timestamp = now
        
    async def _get_zone_capacity(self, zone_id: str) -> Dict[str, float]:
        """Get zone capacity metrics"""
        capacity_response = await self._api_call(
            "listCapacity",
            {"zoneid": zone_id}
        )
        
        capacity = {}
        for cap in capacity_response.get("listcapacityresponse", {}).get("capacity", []):
            # Map capacity types to our metrics
            if cap["type"] == 0:  # Memory
                capacity["memory_gb"] = (cap["capacitytotal"] - cap["capacityused"]) / 1024
            elif cap["type"] == 1:  # CPU
                capacity["cpu_cores"] = cap["capacitytotal"] - cap["capacityused"]
            elif cap["type"] == 3:  # Storage
                capacity["storage_gb"] = (cap["capacitytotal"] - cap["capacityused"]) / 1024
                
        return capacity
        
    async def check_availability(
        self,
        resources: List[ResourceSpec],
        location: Optional[str] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """Check if resources are available in CloudStack"""
        await self._refresh_cache()
        
        # Find suitable zone
        suitable_zones = []
        
        for zone in self.zones.values():
            if location and zone.location != location:
                continue
                
            # Check capacity
            has_capacity = True
            for resource in resources:
                if resource.resource_type == ResourceType.CPU:
                    if zone.capacity.get("cpu_cores", 0) < float(resource.quantity):
                        has_capacity = False
                        break
                elif resource.resource_type == ResourceType.GPU:
                    # Check GPU availability via tags
                    if "gpu_count" not in zone.capacity or \
                       zone.capacity["gpu_count"] < float(resource.quantity):
                        has_capacity = False
                        break
                elif resource.resource_type == ResourceType.MEMORY:
                    if zone.capacity.get("memory_gb", 0) < float(resource.quantity):
                        has_capacity = False
                        break
                        
            if has_capacity:
                suitable_zones.append(zone)
                
        if not suitable_zones:
            return False, {
                "reason": "No zones with sufficient capacity",
                "requested_location": location
            }
            
        # Select best zone (prefer partner zones for cost optimization)
        best_zone = sorted(
            suitable_zones,
            key=lambda z: (
                0 if z.provider_type == "partner" else 1,
                -z.capacity.get("cpu_cores", 0)
            )
        )[0]
        
        return True, {
            "zone_id": best_zone.id,
            "zone_name": best_zone.name,
            "provider_type": best_zone.provider_type,
            "partner_id": best_zone.partner_id,
            "available_capacity": best_zone.capacity
        }
        
    async def allocate_resources(
        self,
        allocation: ResourceAllocation
    ) -> Dict[str, Any]:
        """Allocate resources in CloudStack"""
        await self._refresh_cache()
        
        # Select service offering based on requirements
        service_offering = await self._select_service_offering(allocation.resources)
        if not service_offering:
            raise Exception("No suitable service offering found")
            
        # Check availability and get zone
        available, zone_info = await self.check_availability(
            allocation.resources,
            allocation.resources[0].location_preferences[0] 
            if allocation.resources[0].location_preferences else None
        )
        
        if not available:
            raise Exception(f"Resources not available: {zone_info.get('reason')}")
            
        zone_id = zone_info["zone_id"]
        
        # Generate unique name for the deployment
        deployment_name = f"compute-{allocation.allocation_id[:8]}"
        
        # Deploy virtual machine
        deploy_params = {
            "serviceofferingid": service_offering.id,
            "templateid": await self._get_template_id(zone_id, allocation.resources),
            "zoneid": zone_id,
            "name": deployment_name,
            "displayname": f"Settlement {allocation.settlement_id}",
            "keypair": "platformq-compute",  # Pre-created keypair
            "affinitygroupnames": "compute-futures",  # For anti-affinity
        }
        
        # Add network configuration
        network_id = await self._get_network_id(zone_id, allocation.settlement_id)
        if network_id:
            deploy_params["networkids"] = network_id
            
        # Add tags for tracking
        deploy_params["tags[0].key"] = "settlement_id"
        deploy_params["tags[0].value"] = allocation.settlement_id
        deploy_params["tags[1].key"] = "allocation_id"
        deploy_params["tags[1].value"] = allocation.allocation_id
        deploy_params["tags[2].key"] = "buyer_id"
        deploy_params["tags[2].value"] = allocation.settlement_id.split("_")[1]  # Extract from settlement
        
        # Deploy VM
        deploy_response = await self._api_call("deployVirtualMachine", deploy_params)
        vm = deploy_response.get("deployvirtualmachineresponse", {}).get("virtualmachine", {})
        
        if not vm:
            raise Exception("Failed to deploy virtual machine")
            
        vm_id = vm["id"]
        
        # Wait for VM to be ready
        await self._wait_for_vm_ready(vm_id)
        
        # Get VM details
        vm_details = await self._get_vm_details(vm_id)
        
        # Setup access
        access_details = {
            "vm_id": vm_id,
            "instance_name": deployment_name,
            "zone_id": zone_id,
            "zone_name": zone_info["zone_name"],
            "provider_type": zone_info["provider_type"],
            "partner_id": zone_info.get("partner_id"),
            "public_ip": vm_details.get("public_ip"),
            "private_ip": vm_details.get("private_ip"),
            "ssh_port": 22,
            "api_endpoint": f"https://compute-{allocation.allocation_id}.{zone_info['zone_name']}.platformq.io",
            "monitoring_endpoint": f"https://monitor-{allocation.allocation_id}.{zone_info['zone_name']}.platformq.io",
            "credentials": {
                "username": "compute",
                "key_name": "platformq-compute",
                "access_method": "ssh-key"
            }
        }
        
        # If GPU allocation, ensure GPU passthrough is configured
        if any(r.resource_type == ResourceType.GPU for r in allocation.resources):
            await self._configure_gpu_passthrough(vm_id)
            
        return access_details
        
    async def deallocate_resources(
        self,
        allocation_id: str
    ) -> bool:
        """Deallocate CloudStack resources"""
        try:
            # Find VM by allocation tag
            vms_response = await self._api_call(
                "listVirtualMachines",
                {
                    "tags[0].key": "allocation_id",
                    "tags[0].value": allocation_id
                }
            )
            
            vms = vms_response.get("listvirtualmachinesresponse", {}).get("virtualmachine", [])
            
            if not vms:
                logger.warning(f"No VMs found for allocation {allocation_id}")
                return True
                
            # Destroy all VMs for this allocation
            for vm in vms:
                await self._api_call(
                    "destroyVirtualMachine",
                    {
                        "id": vm["id"],
                        "expunge": "true"  # Immediately expunge
                    }
                )
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to deallocate resources: {e}")
            return False
            
    async def get_metrics(
        self,
        allocation_id: str
    ) -> Dict[str, Any]:
        """Get resource metrics from CloudStack"""
        # Find VM
        vms_response = await self._api_call(
            "listVirtualMachines",
            {
                "tags[0].key": "allocation_id",
                "tags[0].value": allocation_id
            }
        )
        
        vms = vms_response.get("listvirtualmachinesresponse", {}).get("virtualmachine", [])
        
        if not vms:
            return {
                "uptime": 0.0,
                "cpu_utilization": 0.0,
                "memory_utilization": 0.0,
                "network_throughput_gbps": 0.0,
                "error_count": 0
            }
            
        vm = vms[0]
        vm_id = vm["id"]
        
        # Get metrics
        metrics_response = await self._api_call(
            "listVirtualMachinesMetrics",
            {"id": vm_id}
        )
        
        vm_metrics = metrics_response.get("listvirtualmachinesmetricsresponse", {}).get("virtualmachine", [{}])[0]
        
        # Calculate uptime
        created = datetime.fromisoformat(vm["created"].replace("Z", "+00:00"))
        uptime_hours = (datetime.utcnow() - created).total_seconds() / 3600
        
        # Get performance metrics
        cpu_response = await self._api_call(
            "listHostsMetrics",
            {"virtualmachineid": vm_id}
        )
        
        host_metrics = cpu_response.get("listhostsmetricsresponse", {}).get("host", [{}])[0]
        
        return {
            "uptime": 100.0 if vm["state"] == "Running" else 0.0,
            "cpu_utilization": float(vm_metrics.get("cpuused", "0%").strip("%")),
            "memory_utilization": float(vm_metrics.get("memoryused", "0%").strip("%")),
            "network_throughput_gbps": float(vm_metrics.get("networkkbsread", 0)) / 1024 / 1024,
            "disk_iops": float(vm_metrics.get("diskkbsread", 0)) + float(vm_metrics.get("diskkbswrite", 0)),
            "latency_ms": float(host_metrics.get("averageload", 1)) * 10,  # Approximate
            "error_count": 0 if vm["state"] == "Running" else 1,
            "uptime_hours": uptime_hours
        }
        
    async def _select_service_offering(
        self,
        resources: List[ResourceSpec]
    ) -> Optional[ServiceOffering]:
        """Select appropriate service offering"""
        # Find primary resource requirement
        primary_resource = max(resources, key=lambda r: r.quantity)
        
        suitable_offerings = []
        
        for offering in self.service_offerings.values():
            # Check resource type match
            if offering.resource_type != primary_resource.resource_type:
                continue
                
            # Check minimum requirements
            suitable = True
            
            if primary_resource.resource_type == ResourceType.CPU:
                if offering.cpunumber < int(primary_resource.quantity):
                    suitable = False
            elif primary_resource.resource_type == ResourceType.GPU:
                if "gpu_count" not in offering.tags or \
                   int(offering.tags["gpu_count"]) < int(primary_resource.quantity):
                    suitable = False
            elif primary_resource.resource_type == ResourceType.MEMORY:
                if offering.memory < int(primary_resource.quantity) * 1024:  # Convert GB to MB
                    suitable = False
                    
            if suitable:
                suitable_offerings.append(offering)
                
        if not suitable_offerings:
            return None
            
        # Select best offering (closest match to requirements)
        return min(
            suitable_offerings,
            key=lambda o: abs(o.cpunumber - int(primary_resource.quantity))
        )
        
    async def _get_template_id(
        self,
        zone_id: str,
        resources: List[ResourceSpec]
    ) -> str:
        """Get appropriate template ID"""
        # Determine OS based on requirements
        requires_gpu = any(r.resource_type == ResourceType.GPU for r in resources)
        
        template_filter = {
            "zoneid": zone_id,
            "templatefilter": "featured",
            "keyword": "gpu" if requires_gpu else "compute"
        }
        
        templates_response = await self._api_call("listTemplates", template_filter)
        templates = templates_response.get("listtemplatesresponse", {}).get("template", [])
        
        if not templates:
            # Use default template
            template_filter = {
                "zoneid": zone_id,
                "templatefilter": "featured",
                "keyword": "ubuntu-22"
            }
            templates_response = await self._api_call("listTemplates", template_filter)
            templates = templates_response.get("listtemplatesresponse", {}).get("template", [])
            
        if templates:
            return templates[0]["id"]
            
        raise Exception("No suitable template found")
        
    async def _get_network_id(
        self,
        zone_id: str,
        settlement_id: str
    ) -> Optional[str]:
        """Get or create isolated network for settlement"""
        # Check if network exists
        networks_response = await self._api_call(
            "listNetworks",
            {
                "zoneid": zone_id,
                "tags[0].key": "settlement_id",
                "tags[0].value": settlement_id
            }
        )
        
        networks = networks_response.get("listnetworksresponse", {}).get("network", [])
        
        if networks:
            return networks[0]["id"]
            
        # Create new isolated network
        network_offering_response = await self._api_call(
            "listNetworkOfferings",
            {
                "name": "DefaultIsolatedNetworkOfferingForVpcNetworks",
                "state": "Enabled"
            }
        )
        
        offerings = network_offering_response.get("listnetworkofferingsresponse", {}).get("networkoffering", [])
        
        if not offerings:
            return None  # Use default network
            
        # Create network
        create_response = await self._api_call(
            "createNetwork",
            {
                "name": f"settlement-{settlement_id[:8]}",
                "displaytext": f"Network for settlement {settlement_id}",
                "networkofferingid": offerings[0]["id"],
                "zoneid": zone_id,
                "tags[0].key": "settlement_id",
                "tags[0].value": settlement_id
            }
        )
        
        network = create_response.get("createnetworkresponse", {}).get("network", {})
        
        return network.get("id")
        
    async def _wait_for_vm_ready(
        self,
        vm_id: str,
        timeout: int = 300
    ) -> None:
        """Wait for VM to be in running state"""
        start_time = datetime.utcnow()
        
        while (datetime.utcnow() - start_time).total_seconds() < timeout:
            vm_response = await self._api_call(
                "listVirtualMachines",
                {"id": vm_id}
            )
            
            vms = vm_response.get("listvirtualmachinesresponse", {}).get("virtualmachine", [])
            
            if vms and vms[0]["state"] == "Running":
                return
                
            await asyncio.sleep(5)
            
        raise Exception(f"VM {vm_id} did not become ready within {timeout} seconds")
        
    async def _get_vm_details(self, vm_id: str) -> Dict[str, Any]:
        """Get VM details including IPs"""
        vm_response = await self._api_call(
            "listVirtualMachines",
            {"id": vm_id}
        )
        
        vms = vm_response.get("listvirtualmachinesresponse", {}).get("virtualmachine", [])
        
        if not vms:
            return {}
            
        vm = vms[0]
        
        # Get public IP if available
        public_ip = None
        for nic in vm.get("nic", []):
            if nic.get("type") == "Virtual" and nic.get("isdefault"):
                public_ip = nic.get("ipaddress")
                break
                
        return {
            "public_ip": public_ip,
            "private_ip": vm.get("ipaddress"),
            "state": vm.get("state"),
            "created": vm.get("created")
        }
        
    async def _configure_gpu_passthrough(self, vm_id: str) -> None:
        """Configure GPU passthrough for VM"""
        # This would involve CloudStack GPU passthrough APIs
        # Implementation depends on hypervisor (KVM, VMware, etc.)
        logger.info(f"Configuring GPU passthrough for VM {vm_id}")
        
        # Example for KVM with NVIDIA GPUs
        await self._api_call(
            "addGpuToVm",  # Custom API extension
            {
                "vmid": vm_id,
                "gpugroup": "nvidia-a100",
                "count": 1
            }
        ) 