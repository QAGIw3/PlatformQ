"""
Compute Provisioning Module

Handles compute resource provisioning requests from services and integrates
with the derivatives engine for partner capacity allocation.
"""

import logging
import httpx
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from decimal import Decimal
import uuid
from enum import Enum
from dataclasses import dataclass
import asyncio

logger = logging.getLogger(__name__)


class ComputeResourceType(Enum):
    """Types of compute resources"""
    GPU = "gpu"
    CPU = "cpu"
    MEMORY = "memory"
    STORAGE = "storage"


class ProvisioningStatus(Enum):
    """Status of provisioning request"""
    PENDING = "pending"
    ALLOCATING = "allocating"
    PROVISIONED = "provisioned"
    ACTIVE = "active"
    FAILED = "failed"
    TERMINATED = "terminated"


@dataclass
class ComputeProvisioningRequest:
    """Compute provisioning request"""
    request_id: str
    tenant_id: str
    service_type: str
    resource_type: ComputeResourceType
    quantity: int
    duration_hours: int
    start_time: datetime
    provider_id: Optional[str] = None
    metadata: Dict[str, Any] = None


@dataclass
class ComputeProvisioningResult:
    """Result of compute provisioning"""
    request_id: str
    allocation_id: str
    status: ProvisioningStatus
    provider: Optional[str]
    access_details: Dict[str, Any]
    cost: Optional[Decimal]
    message: Optional[str] = None


class ComputeProvisioningManager:
    """Manages compute resource provisioning"""
    
    def __init__(
        self,
        derivatives_engine_url: str = "http://derivatives-engine-service:8000",
        ignite_client = None,
        pulsar_publisher = None
    ):
        self.derivatives_engine_url = derivatives_engine_url
        self.ignite_client = ignite_client
        self.pulsar_publisher = pulsar_publisher
        
        # HTTP client for derivatives engine
        self.http_client = httpx.AsyncClient(
            base_url=derivatives_engine_url,
            timeout=30.0
        )
        
        # Track provisioning requests
        self.active_provisions: Dict[str, ComputeProvisioningRequest] = {}
        
    async def provision_compute(
        self,
        request: ComputeProvisioningRequest
    ) -> ComputeProvisioningResult:
        """Provision compute resources through derivatives engine"""
        try:
            # First, request capacity from cross-service coordinator
            capacity_response = await self._request_capacity_allocation(request)
            
            if not capacity_response or capacity_response.get("status") != "allocated":
                return ComputeProvisioningResult(
                    request_id=request.request_id,
                    allocation_id="",
                    status=ProvisioningStatus.FAILED,
                    provider=None,
                    access_details={},
                    cost=None,
                    message="Failed to allocate capacity"
                )
                
            allocation_id = capacity_response["allocation_id"]
            provider = capacity_response.get("provider")
            cost = Decimal(capacity_response.get("cost", "0"))
            
            # Now provision the actual resources
            access_details = await self._provision_resources(
                allocation_id,
                request,
                provider
            )
            
            if not access_details:
                return ComputeProvisioningResult(
                    request_id=request.request_id,
                    allocation_id=allocation_id,
                    status=ProvisioningStatus.FAILED,
                    provider=provider,
                    access_details={},
                    cost=cost,
                    message="Failed to provision resources"
                )
                
            # Store active provision
            self.active_provisions[request.request_id] = request
            
            # Publish provisioning event
            if self.pulsar_publisher:
                await self.pulsar_publisher.publish(
                    "persistent://platformq/provisioning/compute-provisioned",
                    {
                        "request_id": request.request_id,
                        "tenant_id": request.tenant_id,
                        "service_type": request.service_type,
                        "resource_type": request.resource_type.value,
                        "quantity": request.quantity,
                        "provider": provider,
                        "allocation_id": allocation_id,
                        "status": "provisioned",
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
            return ComputeProvisioningResult(
                request_id=request.request_id,
                allocation_id=allocation_id,
                status=ProvisioningStatus.PROVISIONED,
                provider=provider,
                access_details=access_details,
                cost=cost
            )
            
        except Exception as e:
            logger.error(f"Error provisioning compute: {e}")
            return ComputeProvisioningResult(
                request_id=request.request_id,
                allocation_id="",
                status=ProvisioningStatus.FAILED,
                provider=None,
                access_details={},
                cost=None,
                message=str(e)
            )
            
    async def get_provisioning_status(
        self,
        request_id: str
    ) -> Dict[str, Any]:
        """Get status of provisioning request"""
        if request_id not in self.active_provisions:
            return {"status": "not_found"}
            
        request = self.active_provisions[request_id]
        
        # Check with derivatives engine for current status
        try:
            response = await self.http_client.get(
                f"/api/v1/provisioning/status/{request_id}"
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "status": "unknown",
                    "request": request.__dict__
                }
                
        except Exception as e:
            logger.error(f"Error getting provisioning status: {e}")
            return {
                "status": "error",
                "message": str(e)
            }
            
    async def terminate_provision(
        self,
        request_id: str
    ) -> bool:
        """Terminate provisioned resources"""
        if request_id not in self.active_provisions:
            logger.warning(f"Provision {request_id} not found")
            return False
            
        try:
            # Call derivatives engine to release resources
            response = await self.http_client.post(
                f"/api/v1/provisioning/terminate/{request_id}"
            )
            
            if response.status_code == 200:
                # Remove from active provisions
                del self.active_provisions[request_id]
                
                # Publish termination event
                if self.pulsar_publisher:
                    await self.pulsar_publisher.publish(
                        "persistent://platformq/provisioning/compute-terminated",
                        {
                            "request_id": request_id,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
                    
                return True
            else:
                logger.error(f"Failed to terminate provision: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error terminating provision: {e}")
            return False
            
    async def get_available_capacity(
        self,
        resource_type: ComputeResourceType,
        region: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get available capacity from derivatives engine"""
        try:
            params = {
                "resource_type": resource_type.value
            }
            if region:
                params["region"] = region
                
            response = await self.http_client.get(
                "/api/v1/partners/inventory",
                params=params
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                return {
                    "total_inventory": 0,
                    "inventory": []
                }
                
        except Exception as e:
            logger.error(f"Error getting available capacity: {e}")
            return {
                "total_inventory": 0,
                "inventory": [],
                "error": str(e)
            }
            
    # Private methods
    async def _request_capacity_allocation(
        self,
        request: ComputeProvisioningRequest
    ) -> Optional[Dict[str, Any]]:
        """Request capacity allocation from derivatives engine"""
        try:
            # Prepare allocation request
            allocation_request = {
                "service_type": request.service_type,
                "tenant_id": request.tenant_id,
                "resource_type": request.resource_type.value,
                "quantity": str(request.quantity),
                "duration_hours": request.duration_hours,
                "start_time": request.start_time.isoformat(),
                "priority": 5,  # Default medium priority
                "flexibility_hours": 2,  # Allow 2 hour flexibility
                "metadata": request.metadata or {}
            }
            
            # Call cross-service capacity coordinator
            response = await self.http_client.post(
                "/api/v1/capacity/request",
                json=allocation_request
            )
            
            if response.status_code == 200:
                result = response.json()
                
                # Wait for allocation if pending
                if result.get("status") == "pending":
                    # Poll for allocation result
                    for _ in range(30):  # Wait up to 5 minutes
                        await asyncio.sleep(10)
                        
                        status_response = await self.http_client.get(
                            f"/api/v1/capacity/allocation/{result['request_id']}"
                        )
                        
                        if status_response.status_code == 200:
                            status_data = status_response.json()
                            if status_data.get("status") == "allocated":
                                return status_data
                                
                return result
            else:
                logger.error(f"Capacity allocation failed: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error requesting capacity allocation: {e}")
            return None
            
    async def _provision_resources(
        self,
        allocation_id: str,
        request: ComputeProvisioningRequest,
        provider: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """Provision actual compute resources"""
        # This would integrate with specific providers
        # For now, return mock access details
        
        if request.resource_type == ComputeResourceType.GPU:
            # GPU provisioning
            return {
                "type": "gpu_instance",
                "provider": provider or "platform",
                "instance_id": f"gpu-{allocation_id}",
                "ssh_host": f"gpu-{allocation_id}.compute.platformq.io",
                "ssh_port": 22,
                "ssh_user": "platformq",
                "gpu_type": "nvidia-a100",
                "gpu_count": request.quantity,
                "jupyter_url": f"https://jupyter-{allocation_id}.compute.platformq.io",
                "monitoring_url": f"https://grafana.platformq.io/d/{allocation_id}"
            }
        elif request.resource_type == ComputeResourceType.CPU:
            # CPU provisioning
            return {
                "type": "cpu_instance",
                "provider": provider or "platform",
                "instance_id": f"cpu-{allocation_id}",
                "ssh_host": f"cpu-{allocation_id}.compute.platformq.io",
                "ssh_port": 22,
                "ssh_user": "platformq",
                "cpu_cores": request.quantity,
                "memory_gb": request.quantity * 4,  # 4GB per core
                "monitoring_url": f"https://grafana.platformq.io/d/{allocation_id}"
            }
        else:
            # Storage or memory provisioning
            return {
                "type": request.resource_type.value,
                "provider": provider or "platform",
                "allocation_id": allocation_id,
                "access_endpoint": f"https://storage.platformq.io/{allocation_id}",
                "capacity": request.quantity,
                "unit": "GB" if request.resource_type == ComputeResourceType.STORAGE else "GB"
            }
            
    async def close(self):
        """Close HTTP client"""
        await self.http_client.aclose() 