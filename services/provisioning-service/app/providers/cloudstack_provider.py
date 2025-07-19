"""
CloudStack Provider for PlatformQ Provisioning Service

Unified cloud management across multiple CloudStack installations including
partner clouds (Rackspace, etc.) and on-premise infrastructure.
"""

import logging
import httpx
import hmac
import hashlib
import base64
import urllib.parse
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import asyncio
import uuid

from app.models import (
    ResourceSpec, ResourceAllocation, ResourceType,
    AllocationStatus, ProviderType
)
from .base import ComputeProvider

logger = logging.getLogger(__name__)


class CloudStackProvider(ComputeProvider):
    """CloudStack compute provider for multi-cloud management"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_key = config.get("api_key", "")
        self.secret_key = config.get("secret_key", "")
        self.zone_id = config.get("zone_id")
        self.network_id = config.get("network_id")
        self.template_mappings = config.get("template_mappings", {})
        self.service_offering_mappings = config.get("service_offering_mappings", {})
        
        # HTTP client for CloudStack API
        self.client = httpx.AsyncClient(
            base_url=self.api_endpoint,
            timeout=30.0
        )
        
    def _sign_request(self, params: Dict[str, str]) -> str:
        """Sign CloudStack API request"""
        # Sort parameters
        sorted_params = sorted(params.items())
        
        # Create query string
        query_string = urllib.parse.urlencode(sorted_params).lower()
        
        # Create signature
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha1
        ).digest()
        
        # Base64 encode and URL encode
        return urllib.parse.quote(base64.b64encode(signature).decode('utf-8'))
        
    async def _make_request(self, command: str, **kwargs) -> Dict[str, Any]:
        """Make authenticated CloudStack API request"""
        params = {
            "command": command,
            "apiKey": self.api_key,
            "response": "json",
            **kwargs
        }
        
        # Add signature
        params["signature"] = self._sign_request(params)
        
        try:
            response = await self.client.get("", params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for error response
            if "errorresponse" in data:
                error = data["errorresponse"]
                raise Exception(f"CloudStack API error: {error.get('errortext', 'Unknown error')}")
                
            return data
            
        except Exception as e:
            logger.error(f"CloudStack API request failed: {e}")
            raise
            
    async def check_availability(
        self,
        resources: List[ResourceSpec],
        location: Optional[str] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """Check resource availability in CloudStack"""
        try:
            # Get zone capacity
            zone_id = location or self.zone_id
            capacity_response = await self._make_request(
                "listCapacity",
                zoneid=zone_id
            )
            
            capacities = capacity_response.get("listcapacityresponse", {}).get("capacity", [])
            
            # Parse capacity by type
            available_resources = {}
            for cap in capacities:
                cap_type = cap["type"]
                if cap_type == 0:  # Memory
                    available_resources["memory_gb"] = (cap["capacitytotal"] - cap["capacityused"]) / 1024
                elif cap_type == 1:  # CPU
                    available_resources["cpu_cores"] = cap["capacitytotal"] - cap["capacityused"]
                elif cap_type == 2:  # Storage
                    available_resources["storage_gb"] = (cap["capacitytotal"] - cap["capacityused"]) / 1024
                elif cap_type == 4:  # Public IPs
                    available_resources["public_ips"] = cap["capacitytotal"] - cap["capacityused"]
                    
            # Check if requested resources are available
            for spec in resources:
                if spec.resource_type == ResourceType.CPU:
                    if available_resources.get("cpu_cores", 0) < spec.quantity:
                        return False, {"reason": "Insufficient CPU cores", "available": available_resources}
                elif spec.resource_type == ResourceType.GPU:
                    # Check GPU-enabled service offerings
                    gpu_offerings = await self._get_gpu_offerings()
                    if len(gpu_offerings) == 0:
                        return False, {"reason": "No GPU offerings available", "available": available_resources}
                elif spec.resource_type == ResourceType.MEMORY:
                    if available_resources.get("memory_gb", 0) < spec.quantity:
                        return False, {"reason": "Insufficient memory", "available": available_resources}
                elif spec.resource_type == ResourceType.STORAGE:
                    if available_resources.get("storage_gb", 0) < spec.quantity:
                        return False, {"reason": "Insufficient storage", "available": available_resources}
                        
            return True, {
                "zone_id": zone_id,
                "available_resources": available_resources,
                "network_id": self.network_id
            }
            
        except Exception as e:
            logger.error(f"Error checking CloudStack availability: {e}")
            return False, {"error": str(e)}
            
    async def _get_gpu_offerings(self) -> List[Dict[str, Any]]:
        """Get GPU-enabled service offerings"""
        response = await self._make_request(
            "listServiceOfferings",
            keyword="gpu"
        )
        
        offerings = response.get("listserviceofferingsresponse", {}).get("serviceoffering", [])
        return [o for o in offerings if "gpu" in o.get("name", "").lower()]
        
    async def allocate_resources(
        self,
        allocation: ResourceAllocation
    ) -> Dict[str, Any]:
        """Allocate resources in CloudStack"""
        try:
            instances = []
            
            for spec in allocation.resources:
                # Select appropriate service offering
                service_offering_id = await self._select_service_offering(spec)
                
                # Select template
                template_id = await self._select_template(spec)
                
                # Deploy virtual machine
                deploy_params = {
                    "serviceofferingid": service_offering_id,
                    "templateid": template_id,
                    "zoneid": self.zone_id,
                    "name": f"platformq-{allocation.allocation_id}-{uuid.uuid4().hex[:8]}",
                    "displayname": f"PlatformQ Compute - {allocation.settlement_id}"
                }
                
                if self.network_id:
                    deploy_params["networkids"] = self.network_id
                    
                # Add user data for initialization
                user_data = self._generate_user_data(allocation, spec)
                if user_data:
                    deploy_params["userdata"] = base64.b64encode(user_data.encode()).decode()
                    
                response = await self._make_request("deployVirtualMachine", **deploy_params)
                
                vm_info = response.get("deployvirtualmachineresponse", {})
                vm_id = vm_info.get("id")
                
                if not vm_id:
                    raise Exception("Failed to deploy VM - no ID returned")
                    
                # Wait for VM to be ready
                vm_details = await self._wait_for_vm(vm_id)
                
                instances.append({
                    "vm_id": vm_id,
                    "name": vm_details["name"],
                    "ip_address": vm_details.get("nic", [{}])[0].get("ipaddress"),
                    "public_ip": vm_details.get("publicip"),
                    "state": vm_details["state"],
                    "spec": spec.dict()
                })
                
            # Get access details
            access_details = self._generate_access_details(instances, allocation)
            
            return access_details
            
        except Exception as e:
            logger.error(f"Error allocating CloudStack resources: {e}")
            raise
            
    async def _select_service_offering(self, spec: ResourceSpec) -> str:
        """Select appropriate service offering based on resource spec"""
        # Check mappings first
        if spec.resource_type == ResourceType.GPU:
            gpu_type = spec.specifications.get("gpu_type", "nvidia-a100")
            if gpu_type in self.service_offering_mappings:
                return self.service_offering_mappings[gpu_type]
                
        # List available offerings
        response = await self._make_request("listServiceOfferings")
        offerings = response.get("listserviceofferingsresponse", {}).get("serviceoffering", [])
        
        # Select based on requirements
        for offering in offerings:
            if spec.resource_type == ResourceType.CPU:
                if offering.get("cpunumber", 0) >= spec.quantity:
                    return offering["id"]
            elif spec.resource_type == ResourceType.GPU:
                if "gpu" in offering.get("name", "").lower():
                    return offering["id"]
            elif spec.resource_type == ResourceType.MEMORY:
                if offering.get("memory", 0) >= spec.quantity * 1024:  # Convert GB to MB
                    return offering["id"]
                    
        # Default to first available
        if offerings:
            return offerings[0]["id"]
            
        raise Exception("No suitable service offering found")
        
    async def _select_template(self, spec: ResourceSpec) -> str:
        """Select appropriate template based on resource spec"""
        # Check mappings
        template_type = spec.specifications.get("os", "ubuntu-22.04")
        if template_type in self.template_mappings:
            return self.template_mappings[template_type]
            
        # List available templates
        response = await self._make_request(
            "listTemplates",
            templatefilter="featured"
        )
        templates = response.get("listtemplatesresponse", {}).get("template", [])
        
        # Find Ubuntu or CentOS template
        for template in templates:
            name = template.get("name", "").lower()
            if "ubuntu" in name or "centos" in name:
                return template["id"]
                
        # Default to first available
        if templates:
            return templates[0]["id"]
            
        raise Exception("No suitable template found")
        
    def _generate_user_data(self, allocation: ResourceAllocation, spec: ResourceSpec) -> str:
        """Generate cloud-init user data for VM initialization"""
        user_data = f"""#!/bin/bash
# PlatformQ Compute Node Initialization
echo "Initializing PlatformQ compute node..."

# Set hostname
hostnamectl set-hostname platformq-{allocation.allocation_id}

# Install monitoring agent
curl -sSL https://platformq.io/install-agent.sh | bash -s -- \
    --allocation-id {allocation.allocation_id} \
    --settlement-id {allocation.settlement_id}

# Configure GPU if needed
"""
        
        if spec.resource_type == ResourceType.GPU:
            user_data += """
# Install NVIDIA drivers
apt-get update && apt-get install -y nvidia-driver-525
modprobe nvidia

# Install CUDA toolkit
wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.0-1_all.deb
dpkg -i cuda-keyring_1.0-1_all.deb
apt-get update && apt-get -y install cuda

# Install Docker with NVIDIA runtime
curl -fsSL https://get.docker.com | bash
apt-get install -y nvidia-docker2
systemctl restart docker
"""
        
        user_data += """
# Start monitoring
systemctl enable platformq-monitor
systemctl start platformq-monitor

echo "Initialization complete"
"""
        
        return user_data
        
    async def _wait_for_vm(self, vm_id: str, timeout: int = 300) -> Dict[str, Any]:
        """Wait for VM to be in running state"""
        start_time = datetime.utcnow()
        
        while (datetime.utcnow() - start_time).seconds < timeout:
            response = await self._make_request(
                "listVirtualMachines",
                id=vm_id
            )
            
            vms = response.get("listvirtualmachinesresponse", {}).get("virtualmachine", [])
            if not vms:
                raise Exception(f"VM {vm_id} not found")
                
            vm = vms[0]
            if vm["state"] == "Running":
                return vm
            elif vm["state"] == "Error":
                raise Exception(f"VM {vm_id} failed to start")
                
            await asyncio.sleep(5)
            
        raise Exception(f"Timeout waiting for VM {vm_id} to start")
        
    def _generate_access_details(
        self,
        instances: List[Dict[str, Any]],
        allocation: ResourceAllocation
    ) -> Dict[str, Any]:
        """Generate access details for allocated resources"""
        access_details = {
            "provider": "cloudstack",
            "zone_id": self.zone_id,
            "instances": instances,
            "allocation_id": allocation.allocation_id,
            "api_endpoint": f"{self.api_endpoint}/client/console",
            "monitoring_endpoints": {}
        }
        
        # Add SSH access for each instance
        for idx, instance in enumerate(instances):
            instance_id = instance["vm_id"]
            
            # Generate SSH key if needed
            ssh_key = self._generate_ssh_key(instance_id)
            
            instance["ssh_access"] = {
                "host": instance.get("public_ip") or instance.get("ip_address"),
                "port": 22,
                "username": "ubuntu",
                "key_name": f"platformq-{instance_id}"
            }
            
            # Add monitoring endpoint
            access_details["monitoring_endpoints"][instance_id] = {
                "metrics": f"https://monitoring.platformq.io/instances/{instance_id}",
                "logs": f"https://logs.platformq.io/instances/{instance_id}",
                "console": f"{self.api_endpoint}/client/console?cmd=access&vm={instance_id}"
            }
            
        # Add Jupyter/notebook access for GPU instances
        gpu_instances = [i for i in instances if i["spec"]["resource_type"] == "gpu"]
        if gpu_instances:
            access_details["jupyter_endpoints"] = {
                i["vm_id"]: f"https://jupyter-{i['vm_id']}.compute.platformq.io"
                for i in gpu_instances
            }
            
        return access_details
        
    def _generate_ssh_key(self, instance_id: str) -> str:
        """Generate or retrieve SSH key for instance"""
        # In production, this would integrate with a key management service
        # For now, return a placeholder
        return f"ssh-key-{instance_id}"
        
    async def deallocate_resources(self, allocation_id: str) -> bool:
        """Deallocate CloudStack resources"""
        try:
            # List VMs with allocation tag
            response = await self._make_request(
                "listVirtualMachines",
                keyword=f"platformq-{allocation_id}"
            )
            
            vms = response.get("listvirtualmachinesresponse", {}).get("virtualmachine", [])
            
            for vm in vms:
                # Stop VM first
                await self._make_request(
                    "stopVirtualMachine",
                    id=vm["id"]
                )
                
                # Wait for stop
                await self._wait_for_vm_state(vm["id"], "Stopped")
                
                # Destroy VM
                await self._make_request(
                    "destroyVirtualMachine",
                    id=vm["id"],
                    expunge=True
                )
                
                logger.info(f"Deallocated VM {vm['id']} for allocation {allocation_id}")
                
            return True
            
        except Exception as e:
            logger.error(f"Error deallocating CloudStack resources: {e}")
            return False
            
    async def _wait_for_vm_state(self, vm_id: str, target_state: str, timeout: int = 60):
        """Wait for VM to reach target state"""
        start_time = datetime.utcnow()
        
        while (datetime.utcnow() - start_time).seconds < timeout:
            response = await self._make_request(
                "listVirtualMachines",
                id=vm_id
            )
            
            vms = response.get("listvirtualmachinesresponse", {}).get("virtualmachine", [])
            if vms and vms[0]["state"] == target_state:
                return
                
            await asyncio.sleep(2)
            
    async def get_metrics(self, allocation_id: str) -> Dict[str, Any]:
        """Get resource metrics from CloudStack"""
        try:
            # List VMs for allocation
            response = await self._make_request(
                "listVirtualMachines",
                keyword=f"platformq-{allocation_id}"
            )
            
            vms = response.get("listvirtualmachinesresponse", {}).get("virtualmachine", [])
            
            if not vms:
                return {
                    "cpu_utilization": 0,
                    "memory_utilization": 0,
                    "network_throughput_gbps": 0,
                    "disk_iops": 0,
                    "uptime": 0,
                    "latency_ms": 9999,
                    "error_count": 1
                }
                
            # Aggregate metrics
            total_cpu = 0
            total_memory = 0
            total_network = 0
            vm_count = len(vms)
            
            for vm in vms:
                # Get VM metrics
                metrics_response = await self._make_request(
                    "listVirtualMachinesMetrics",
                    id=vm["id"]
                )
                
                vm_metrics = metrics_response.get("listvirtualmachinesmetricsresponse", {}).get("virtualmachine", [{}])[0]
                
                # Parse metrics
                total_cpu += float(vm_metrics.get("cpuused", "0").rstrip("%"))
                total_memory += float(vm_metrics.get("memoryused", "0").rstrip("%"))
                total_network += float(vm_metrics.get("networkkbsread", 0)) + float(vm_metrics.get("networkkbswrite", 0))
                
            # Calculate averages and convert units
            return {
                "cpu_utilization": total_cpu / vm_count if vm_count > 0 else 0,
                "memory_utilization": total_memory / vm_count if vm_count > 0 else 0,
                "network_throughput_gbps": (total_network / vm_count / 1024 / 1024) if vm_count > 0 else 0,  # KB/s to Gbps
                "disk_iops": 10000,  # Placeholder - would need storage metrics
                "uptime": 99.9,  # Calculate from VM state history
                "latency_ms": 10,  # Would need external monitoring
                "error_count": 0
            }
            
        except Exception as e:
            logger.error(f"Error getting CloudStack metrics: {e}")
            return {
                "cpu_utilization": 0,
                "memory_utilization": 0,
                "network_throughput_gbps": 0,
                "disk_iops": 0,
                "uptime": 0,
                "latency_ms": 9999,
                "error_count": 1
            } 