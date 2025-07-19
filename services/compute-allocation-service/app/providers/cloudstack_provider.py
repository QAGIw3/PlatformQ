"""CloudStack compute provider implementation"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from datetime import datetime

import httpx
import hmac
import hashlib
import base64
from urllib.parse import quote, urlencode

from platformq_compute_common.providers import ResourceProvider, ProviderCapabilities
from platformq_compute_common.models import (
    ResourceRequirements,
    ResourceAllocation,
    ProviderType,
    PricingModel,
    AllocationStatus
)

logger = logging.getLogger(__name__)


class CloudStackProvider(ResourceProvider):
    """CloudStack compute provider implementation"""
    
    # Instance type mappings
    INSTANCE_TYPES = {
        "small": {"cpu": 1, "memory": 2, "storage": 20},
        "medium": {"cpu": 2, "memory": 4, "storage": 40},
        "large": {"cpu": 4, "memory": 8, "storage": 80},
        "xlarge": {"cpu": 8, "memory": 16, "storage": 160},
        "2xlarge": {"cpu": 16, "memory": 32, "storage": 320},
        "gpu.large": {"cpu": 8, "memory": 32, "storage": 200, "gpu": 1},
        "gpu.xlarge": {"cpu": 16, "memory": 64, "storage": 400, "gpu": 2}
    }
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize CloudStack provider
        
        Args:
            config: Provider configuration including:
                - api_url: CloudStack API endpoint
                - api_key: API key
                - secret_key: Secret key
                - zone_id: Default zone ID
                - network_id: Default network ID
                - template_mappings: OS template mappings
        """
        super().__init__(config)
        self.api_url = config["api_url"].rstrip('/')
        self.api_key = config["api_key"]
        self.secret_key = config["secret_key"]
        self.zone_id = config.get("zone_id")
        self.network_id = config.get("network_id")
        self.template_mappings = config.get("template_mappings", {})
        
        # HTTP client for API calls
        self.client = httpx.AsyncClient(timeout=30.0)
        
        # Cache for service offerings
        self._service_offerings_cache = {}
        self._cache_expires = None
    
    async def get_capabilities(self) -> ProviderCapabilities:
        """Get provider capabilities"""
        try:
            # Get available service offerings
            offerings = await self._get_service_offerings()
            
            # Get available zones
            zones = await self._list_zones()
            
            return ProviderCapabilities(
                provider_type=ProviderType.CLOUDSTACK,
                supported_regions=[zone["name"] for zone in zones],
                supported_instance_types=list(self.INSTANCE_TYPES.keys()),
                supported_gpu_types=["nvidia-v100"],  # If GPU support is available
                supported_pricing_models=[PricingModel.ON_DEMAND],
                max_instances=100,  # Can be configured
                features={
                    "spot_instances": False,
                    "dedicated_hosts": True,
                    "auto_scaling": True,
                    "load_balancing": True,
                    "custom_images": True,
                    "snapshots": True,
                    "live_migration": True
                },
                sla_guarantees={
                    "availability": 0.99,
                    "network": 0.999,
                    "storage": 0.999
                }
            )
        except Exception as e:
            logger.error(f"Failed to get CloudStack capabilities: {e}")
            # Return minimal capabilities on error
            return ProviderCapabilities(
                provider_type=ProviderType.CLOUDSTACK,
                supported_regions=["default"],
                supported_instance_types=list(self.INSTANCE_TYPES.keys()),
                supported_gpu_types=[],
                supported_pricing_models=[PricingModel.ON_DEMAND],
                max_instances=50,
                features={"dedicated_hosts": True},
                sla_guarantees={"availability": 0.95}
            )
    
    async def check_availability(
        self,
        requirements: ResourceRequirements,
        region: Optional[str] = None
    ) -> Tuple[bool, Optional[str], Dict[str, Any]]:
        """Check resource availability
        
        Returns:
            Tuple of (available, instance_type, details)
        """
        try:
            # Find suitable instance type
            instance_type = self._find_suitable_instance_type(requirements)
            if not instance_type:
                return False, None, {"reason": "No suitable instance type found"}
            
            # Get service offering for this instance type
            offering = await self._get_service_offering_for_type(instance_type)
            if not offering:
                return False, None, {"reason": f"Service offering not found for {instance_type}"}
            
            # Check capacity (simplified - real implementation would check actual capacity)
            # In production, you'd call listCapacity API
            capacity_available = True  # Placeholder
            
            if not capacity_available:
                return False, None, {"reason": "Insufficient capacity"}
            
            return True, instance_type, {
                "service_offering_id": offering["id"],
                "available_count": 10,  # Placeholder
                "zone_id": self.zone_id or (await self._get_default_zone())["id"]
            }
            
        except Exception as e:
            logger.error(f"Failed to check availability: {e}")
            return False, None, {"error": str(e)}
    
    async def get_pricing(
        self,
        requirements: ResourceRequirements,
        region: Optional[str] = None,
        instance_type: Optional[str] = None,
        pricing_model: PricingModel = PricingModel.ON_DEMAND
    ) -> Dict[str, Any]:
        """Get pricing information"""
        if not instance_type:
            instance_type = self._find_suitable_instance_type(requirements)
        
        if not instance_type:
            return {"error": "No suitable instance type"}
        
        # CloudStack typically has fixed pricing
        # These are example rates - replace with actual pricing
        base_rates = {
            "small": 0.05,
            "medium": 0.10,
            "large": 0.20,
            "xlarge": 0.40,
            "2xlarge": 0.80,
            "gpu.large": 1.50,
            "gpu.xlarge": 3.00
        }
        
        hourly_cost = base_rates.get(instance_type, 0.10)
        
        return {
            "hourly_cost": hourly_cost,
            "currency": "USD",
            "instance_type": instance_type,
            "pricing_model": pricing_model.value,
            "minimum_hours": 1
        }
    
    async def allocate(
        self,
        allocation: ResourceAllocation
    ) -> Tuple[bool, Dict[str, Any]]:
        """Allocate compute resources"""
        try:
            # Get service offering
            requirements = ResourceRequirements(
                cpu_cores=allocation.cpu_cores,
                memory_gb=allocation.memory_gb,
                storage_gb=allocation.storage_gb,
                gpu_count=allocation.gpu_count,
                gpu_type=allocation.gpu_type
            )
            
            instance_type = self._find_suitable_instance_type(requirements)
            if not instance_type:
                return False, {"error": "No suitable instance type found"}
            
            offering = await self._get_service_offering_for_type(instance_type)
            if not offering:
                return False, {"error": f"Service offering not found for {instance_type}"}
            
            # Get template
            template = await self._get_template(allocation.tags.get("os", "ubuntu"))
            if not template:
                return False, {"error": "Template not found"}
            
            # Prepare VM deployment parameters
            params = {
                "command": "deployVirtualMachine",
                "serviceofferingid": offering["id"],
                "templateid": template["id"],
                "zoneid": self.zone_id or (await self._get_default_zone())["id"],
                "name": f"platformq-{allocation.workload_id}",
                "displayname": f"PlatformQ {allocation.workload_type} - {allocation.workload_id}"
            }
            
            # Add network if specified
            if self.network_id:
                params["networkids"] = self.network_id
            
            # Add user data if provided
            if allocation.tags.get("user_data"):
                params["userdata"] = base64.b64encode(
                    allocation.tags["user_data"].encode()
                ).decode()
            
            # Deploy VM
            response = await self._api_request("deployVirtualMachine", params)
            
            if "errorcode" in response:
                return False, {
                    "error": response.get("errortext", "Deployment failed"),
                    "error_code": response["errorcode"]
                }
            
            vm = response.get("virtualmachine", {})
            
            # Wait for VM to be ready (async job)
            if "jobid" in response:
                vm = await self._wait_for_job(response["jobid"])
                if not vm or "virtualmachine" not in vm:
                    return False, {"error": "VM deployment job failed"}
                vm = vm["virtualmachine"]
            
            # Store instance details
            allocation.instance_id = vm["id"]
            allocation.instance_type = instance_type
            
            return True, {
                "instance_id": vm["id"],
                "instance_name": vm["name"],
                "private_ip": vm.get("nic", [{}])[0].get("ipaddress"),
                "public_ip": vm.get("publicip"),
                "state": vm["state"],
                "zone": vm.get("zonename"),
                "instance_type": instance_type
            }
            
        except Exception as e:
            logger.error(f"Failed to allocate resources: {e}")
            return False, {"error": str(e)}
    
    async def deallocate(
        self,
        allocation: ResourceAllocation
    ) -> Tuple[bool, str]:
        """Deallocate compute resources"""
        try:
            if not allocation.instance_id:
                return False, "No instance ID found"
            
            # Destroy VM
            params = {
                "command": "destroyVirtualMachine",
                "id": allocation.instance_id,
                "expunge": "true"  # Immediately expunge
            }
            
            response = await self._api_request("destroyVirtualMachine", params)
            
            if "errorcode" in response:
                return False, response.get("errortext", "Deallocation failed")
            
            # Wait for destruction if async
            if "jobid" in response:
                result = await self._wait_for_job(response["jobid"])
                if not result or not result.get("success"):
                    return False, "VM destruction job failed"
            
            return True, "VM destroyed successfully"
            
        except Exception as e:
            logger.error(f"Failed to deallocate resources: {e}")
            return False, str(e)
    
    async def get_status(
        self,
        allocation: ResourceAllocation
    ) -> Dict[str, Any]:
        """Get allocation status"""
        try:
            if not allocation.instance_id:
                return {"status": "unknown", "error": "No instance ID"}
            
            # Get VM details
            params = {
                "command": "listVirtualMachines",
                "id": allocation.instance_id
            }
            
            response = await self._api_request("listVirtualMachines", params)
            
            if "errorcode" in response:
                return {
                    "status": "error",
                    "error": response.get("errortext", "Failed to get status")
                }
            
            vms = response.get("listvirtualmachinesresponse", {}).get("virtualmachine", [])
            if not vms:
                return {"status": "not_found"}
            
            vm = vms[0]
            
            # Map CloudStack states to our states
            state_mapping = {
                "Running": "running",
                "Stopped": "stopped",
                "Stopping": "stopping",
                "Starting": "starting",
                "Destroyed": "terminated",
                "Expunging": "terminating",
                "Error": "error"
            }
            
            return {
                "status": state_mapping.get(vm["state"], "unknown"),
                "health": "healthy" if vm["state"] == "Running" else "degraded",
                "instance_state": vm["state"],
                "cpu_used": vm.get("cpuused", "0%"),
                "memory_used": vm.get("memoryused"),
                "uptime": vm.get("uptime"),
                "created": vm.get("created"),
                "private_ip": vm.get("nic", [{}])[0].get("ipaddress"),
                "public_ip": vm.get("publicip")
            }
            
        except Exception as e:
            logger.error(f"Failed to get status: {e}")
            return {"status": "error", "error": str(e)}
    
    async def resize(
        self,
        allocation: ResourceAllocation,
        new_requirements: ResourceRequirements
    ) -> Tuple[bool, Dict[str, Any]]:
        """Resize allocation"""
        try:
            if not allocation.instance_id:
                return False, {"error": "No instance ID"}
            
            # Find new instance type
            new_instance_type = self._find_suitable_instance_type(new_requirements)
            if not new_instance_type:
                return False, {"error": "No suitable instance type for new requirements"}
            
            # Get new service offering
            new_offering = await self._get_service_offering_for_type(new_instance_type)
            if not new_offering:
                return False, {"error": "Service offering not found"}
            
            # Stop VM first (required for resize in most cases)
            stop_params = {
                "command": "stopVirtualMachine",
                "id": allocation.instance_id
            }
            
            stop_response = await self._api_request("stopVirtualMachine", stop_params)
            if "jobid" in stop_response:
                await self._wait_for_job(stop_response["jobid"])
            
            # Change service offering
            resize_params = {
                "command": "changeServiceForVirtualMachine",
                "id": allocation.instance_id,
                "serviceofferingid": new_offering["id"]
            }
            
            resize_response = await self._api_request("changeServiceForVirtualMachine", resize_params)
            
            if "errorcode" in resize_response:
                # Try to restart VM even if resize failed
                await self._api_request("startVirtualMachine", {"command": "startVirtualMachine", "id": allocation.instance_id})
                return False, {"error": resize_response.get("errortext", "Resize failed")}
            
            # Start VM again
            start_params = {
                "command": "startVirtualMachine",
                "id": allocation.instance_id
            }
            
            start_response = await self._api_request("startVirtualMachine", start_params)
            if "jobid" in start_response:
                await self._wait_for_job(start_response["jobid"])
            
            # Update allocation
            allocation.cpu_cores = new_requirements.cpu_cores
            allocation.memory_gb = new_requirements.memory_gb
            allocation.instance_type = new_instance_type
            
            return True, {
                "resized": True,
                "new_instance_type": new_instance_type,
                "new_offering_id": new_offering["id"]
            }
            
        except Exception as e:
            logger.error(f"Failed to resize: {e}")
            return False, {"error": str(e)}
    
    def _find_suitable_instance_type(self, requirements: ResourceRequirements) -> Optional[str]:
        """Find suitable instance type for requirements"""
        suitable_types = []
        
        for instance_type, specs in self.INSTANCE_TYPES.items():
            if (specs["cpu"] >= requirements.cpu_cores and
                specs["memory"] >= requirements.memory_gb and
                specs.get("storage", 0) >= requirements.storage_gb):
                
                # Check GPU requirements
                if requirements.gpu_count > 0:
                    if specs.get("gpu", 0) < requirements.gpu_count:
                        continue
                
                suitable_types.append((instance_type, specs))
        
        if not suitable_types:
            return None
        
        # Sort by resource efficiency (minimize waste)
        suitable_types.sort(key=lambda x: (
            x[1]["cpu"] - requirements.cpu_cores +
            x[1]["memory"] - requirements.memory_gb
        ))
        
        return suitable_types[0][0]
    
    async def _get_service_offerings(self) -> List[Dict[str, Any]]:
        """Get cached service offerings"""
        if self._service_offerings_cache and self._cache_expires and datetime.utcnow() < self._cache_expires:
            return self._service_offerings_cache
        
        params = {"command": "listServiceOfferings"}
        response = await self._api_request("listServiceOfferings", params)
        
        offerings = response.get("listserviceofferingsresponse", {}).get("serviceoffering", [])
        
        # Cache for 1 hour
        from datetime import timedelta
        self._service_offerings_cache = offerings
        self._cache_expires = datetime.utcnow() + timedelta(hours=1)
        
        return offerings
    
    async def _get_service_offering_for_type(self, instance_type: str) -> Optional[Dict[str, Any]]:
        """Get service offering for instance type"""
        offerings = await self._get_service_offerings()
        
        # Map instance type to offering name pattern
        # This is simplified - real implementation would have proper mapping
        for offering in offerings:
            if instance_type.lower() in offering["name"].lower():
                return offering
        
        return None
    
    async def _list_zones(self) -> List[Dict[str, Any]]:
        """List available zones"""
        params = {"command": "listZones"}
        response = await self._api_request("listZones", params)
        return response.get("listzonesresponse", {}).get("zone", [])
    
    async def _get_default_zone(self) -> Dict[str, Any]:
        """Get default zone"""
        zones = await self._list_zones()
        if not zones:
            raise Exception("No zones available")
        return zones[0]
    
    async def _get_template(self, os_name: str = "ubuntu") -> Optional[Dict[str, Any]]:
        """Get template by OS name"""
        # Check template mappings first
        template_id = self.template_mappings.get(os_name)
        
        if template_id:
            params = {
                "command": "listTemplates",
                "templatefilter": "all",
                "id": template_id
            }
        else:
            params = {
                "command": "listTemplates",
                "templatefilter": "featured",
                "keyword": os_name
            }
        
        response = await self._api_request("listTemplates", params)
        templates = response.get("listtemplatesresponse", {}).get("template", [])
        
        if templates:
            return templates[0]
        
        return None
    
    async def _wait_for_job(self, job_id: str, timeout: int = 300) -> Optional[Dict[str, Any]]:
        """Wait for async job to complete"""
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            params = {
                "command": "queryAsyncJobResult",
                "jobid": job_id
            }
            
            response = await self._api_request("queryAsyncJobResult", params)
            result = response.get("queryasyncjobresultresponse", {})
            
            job_status = result.get("jobstatus", 0)
            
            # Job status: 0=pending, 1=success, 2=failed
            if job_status == 1:
                return result.get("jobresult", {})
            elif job_status == 2:
                logger.error(f"Job {job_id} failed: {result.get('jobresult', {})}")
                return None
            
            # Still pending, wait a bit
            await asyncio.sleep(2)
        
        logger.error(f"Job {job_id} timed out after {timeout} seconds")
        return None
    
    async def _api_request(self, command: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Make CloudStack API request"""
        # Add API key and response format
        params["apikey"] = self.api_key
        params["response"] = "json"
        
        # Create signature
        signature = self._create_signature(params)
        params["signature"] = signature
        
        # Make request
        url = f"{self.api_url}/client/api"
        
        try:
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPError as e:
            logger.error(f"CloudStack API request failed: {e}")
            raise
    
    def _create_signature(self, params: Dict[str, Any]) -> str:
        """Create CloudStack API signature"""
        # Sort parameters and create query string
        sorted_params = sorted(params.items(), key=lambda x: x[0].lower())
        query_string = urlencode([(k.lower(), v) for k, v in sorted_params])
        
        # Create signature
        signature = hmac.new(
            self.secret_key.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha1
        ).digest()
        
        return base64.b64encode(signature).decode('utf-8')
    
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose() 