"""
Provisioning Service

Automated compute resource provisioning for physical settlement of futures contracts.
Integrates with multiple cloud providers and on-premise resources.
"""

import asyncio
import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
from contextlib import asynccontextmanager
from enum import Enum
import uuid
import os

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, Field
import httpx

from platformq_shared import (
    create_base_app,
    EventProcessor,
    event_handler,
    ProcessingResult,
    ProcessingStatus
)
from platformq_shared.event_publisher import EventPublisher

from app.models import (
    ProviderType, ResourceType, AllocationStatus,
    ResourceSpec, AllocationRequest, ResourceAllocation
)
from app.providers.base import ComputeProvider

logger = logging.getLogger(__name__)


# ============= Additional Models =============


# ============= Provider Implementations =============


class AWSProvider(ComputeProvider):
    """AWS EC2/EKS provider"""
    
    async def check_availability(
        self,
        resources: List[ResourceSpec],
        location: Optional[str] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """Check AWS capacity"""
        # In production, use boto3 to check capacity
        # For now, simulate availability
        total_cpus = sum(r.quantity for r in resources if r.resource_type == ResourceType.CPU)
        total_gpus = sum(r.quantity for r in resources if r.resource_type == ResourceType.GPU)
        
        # Simulate capacity limits
        available = total_cpus <= 1000 and total_gpus <= 100
        
        return available, {
            "region": location or "us-east-1",
            "available_cpus": 1000 - total_cpus if available else 0,
            "available_gpus": 100 - total_gpus if available else 0,
            "instance_types": ["m5.xlarge", "p3.2xlarge"] if available else []
        }
        
    async def allocate_resources(
        self,
        allocation: ResourceAllocation
    ) -> Dict[str, Any]:
        """Allocate AWS resources"""
        # In production, use boto3 to create instances/containers
        # For now, simulate allocation
        instance_ids = [f"i-{uuid.uuid4().hex[:8]}" for _ in range(3)]
        
        return {
            "instance_ids": instance_ids,
            "region": "us-east-1",
            "vpc_id": f"vpc-{uuid.uuid4().hex[:8]}",
            "security_group_id": f"sg-{uuid.uuid4().hex[:8]}",
            "ssh_key": "generated-key",
            "api_endpoint": f"https://compute-{allocation.allocation_id}.us-east-1.elb.amazonaws.com"
        }


class RackspaceProvider(ComputeProvider):
    """Rackspace bare metal provider"""
    
    async def check_availability(
        self,
        resources: List[ResourceSpec],
        location: Optional[str] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """Check Rackspace capacity"""
        # Integrate with Rackspace API
        return True, {
            "datacenter": location or "dfw",
            "available_servers": 50,
            "server_types": ["gpu-accelerated", "high-memory"]
        }
        
    async def allocate_resources(
        self,
        allocation: ResourceAllocation
    ) -> Dict[str, Any]:
        """Allocate Rackspace bare metal servers"""
        server_ids = [f"srv-{uuid.uuid4().hex[:8]}" for _ in range(2)]
        
        return {
            "server_ids": server_ids,
            "datacenter": "dfw",
            "ipmi_access": "enabled",
            "api_endpoint": f"https://{allocation.allocation_id}.rackspace.platformq.io"
        }


class KubernetesProvider(ComputeProvider):
    """Kubernetes cluster provider for on-premise/edge"""
    
    async def check_availability(
        self,
        resources: List[ResourceSpec],
        location: Optional[str] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """Check Kubernetes cluster capacity"""
        # In production, query Kubernetes metrics
        return True, {
            "cluster": location or "main-cluster",
            "available_nodes": 20,
            "gpu_nodes": 5
        }
        
    async def allocate_resources(
        self,
        allocation: ResourceAllocation
    ) -> Dict[str, Any]:
        """Create Kubernetes namespace and resources"""
        namespace = f"compute-{allocation.allocation_id[:8]}"
        
        # In production, create actual K8s resources
        return {
            "namespace": namespace,
            "kubeconfig": "generated-kubeconfig",
            "ingress_url": f"https://{namespace}.compute.platformq.io",
            "monitoring_url": f"https://grafana.platformq.io/d/{namespace}"
        }


# ============= Provisioning Engine =============

class ProvisioningEngine:
    """Main provisioning engine for automated resource allocation"""
    
    def __init__(self):
        self.providers: Dict[ProviderType, ComputeProvider] = {}
        self.allocations: Dict[str, ResourceAllocation] = {}
        self.settlement_allocations: Dict[str, str] = {}  # settlement_id -> allocation_id
        
        # Initialize providers
        self._initialize_providers()
        
        # Background tasks
        self._monitoring_task = None
        self._cleanup_task = None
        
    def _initialize_providers(self):
        """Initialize compute providers"""
        # AWS Provider
        self.providers[ProviderType.AWS] = AWSProvider({
            "name": "AWS",
            "api_endpoint": "https://ec2.amazonaws.com",
            "credentials": {
                # Load from environment/vault
            }
        })
        
        # Rackspace Provider
        self.providers[ProviderType.RACKSPACE] = RackspaceProvider({
            "name": "Rackspace",
            "api_endpoint": "https://api.rackspace.com",
            "credentials": {
                # Load from environment/vault
            }
        })
        
        # Kubernetes Provider
        self.providers[ProviderType.ON_PREMISE] = KubernetesProvider({
            "name": "On-Premise K8s",
            "api_endpoint": "https://k8s.platformq.internal",
            "credentials": {
                # Load from environment/vault
            }
        })
        
        # CloudStack Provider - Unified management for partners and on-premise
        from app.providers.cloudstack_provider import CloudStackProvider
        self.providers[ProviderType.PARTNER] = CloudStackProvider({
            "name": "CloudStack Unified",
            "cloudstack": {
                "api_url": os.getenv("CLOUDSTACK_API_URL", "https://cloudstack.platformq.io/client/api"),
                "api_key": os.getenv("CLOUDSTACK_API_KEY"),
                "secret_key": os.getenv("CLOUDSTACK_SECRET_KEY"),
                "zones": {
                    "partner-rackspace": {
                        "description": "Rackspace wholesale capacity",
                        "location": "us-central",
                        "provider_type": "partner",
                        "partner_id": "rackspace"
                    },
                    "partner-equinix": {
                        "description": "Equinix Metal capacity",
                        "location": "us-east",
                        "provider_type": "partner",
                        "partner_id": "equinix"
                    },
                    "edge-nyc": {
                        "description": "NYC edge location",
                        "location": "us-east-edge",
                        "provider_type": "edge"
                    },
                    "onprem-main": {
                        "description": "Main datacenter",
                        "location": "us-west",
                        "provider_type": "on_premise"
                    }
                }
            }
        })
        
        # Edge Provider (also CloudStack managed)
        self.providers[ProviderType.EDGE] = CloudStackProvider({
            "name": "Edge Locations",
            "cloudstack": {
                "api_url": os.getenv("CLOUDSTACK_API_URL", "https://cloudstack.platformq.io/client/api"),
                "api_key": os.getenv("CLOUDSTACK_API_KEY"),
                "secret_key": os.getenv("CLOUDSTACK_SECRET_KEY"),
                "default_zone_id": "edge-nyc"
            }
        })
        
    async def start(self):
        """Start background tasks"""
        self._monitoring_task = asyncio.create_task(self._monitor_allocations())
        self._cleanup_task = asyncio.create_task(self._cleanup_expired())
        
    async def stop(self):
        """Stop background tasks"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
            
    async def allocate_resources(
        self,
        request: AllocationRequest
    ) -> ResourceAllocation:
        """Allocate resources for settlement"""
        # Create allocation record
        allocation = ResourceAllocation(
            settlement_id=request.settlement_id,
            provider=ProviderType.AWS,  # Default, will be selected based on requirements
            resources=request.resources,
            expires_at=request.start_time + timedelta(hours=request.duration_hours)
        )
        
        # Select best provider
        provider_type = await self._select_provider(request.resources, request.metadata.get("location"))
        allocation.provider = provider_type
        provider = self.providers[provider_type]
        
        # Check availability
        available, details = await provider.check_availability(
            request.resources,
            request.metadata.get("location")
        )
        
        if not available:
            allocation.status = AllocationStatus.FAILED
            raise HTTPException(
                status_code=503,
                detail=f"Resources not available from {provider_type}"
            )
            
        # Allocate resources
        allocation.status = AllocationStatus.PROVISIONING
        self.allocations[allocation.allocation_id] = allocation
        self.settlement_allocations[request.settlement_id] = allocation.allocation_id
        
        try:
            access_details = await provider.allocate_resources(allocation)
            allocation.access_details = access_details
            allocation.status = AllocationStatus.ACTIVE
            allocation.activated_at = datetime.utcnow()
            
            # Calculate costs
            allocation.cost_per_hour = await self._calculate_cost(
                provider_type,
                request.resources
            )
            allocation.total_cost = allocation.cost_per_hour * request.duration_hours
            
            # Set monitoring endpoints
            allocation.monitoring_endpoints = {
                "metrics": f"https://metrics.platformq.io/allocation/{allocation.allocation_id}",
                "logs": f"https://logs.platformq.io/allocation/{allocation.allocation_id}",
                "traces": f"https://traces.platformq.io/allocation/{allocation.allocation_id}"
            }
            
        except Exception as e:
            logger.error(f"Failed to allocate resources: {e}")
            allocation.status = AllocationStatus.FAILED
            raise HTTPException(status_code=500, detail=str(e))
            
        return allocation
        
    async def deallocate_resources(
        self,
        allocation_id: str
    ) -> bool:
        """Deallocate resources"""
        allocation = self.allocations.get(allocation_id)
        if not allocation:
            raise HTTPException(status_code=404, detail="Allocation not found")
            
        provider = self.providers[allocation.provider]
        success = await provider.deallocate_resources(allocation_id)
        
        if success:
            allocation.status = AllocationStatus.TERMINATED
            
        return success
        
    async def get_allocation_metrics(
        self,
        allocation_id: str
    ) -> Dict[str, Any]:
        """Get metrics for allocation"""
        allocation = self.allocations.get(allocation_id)
        if not allocation:
            raise HTTPException(status_code=404, detail="Allocation not found")
            
        provider = self.providers[allocation.provider]
        metrics = await provider.get_metrics(allocation_id)
        
        # Add SLA compliance
        sla_compliance = self._calculate_sla_compliance(metrics)
        
        return {
            "allocation_id": allocation_id,
            "status": allocation.status,
            "metrics": metrics,
            "sla_compliance": sla_compliance,
            "uptime_percentage": metrics.get("uptime", 100.0),
            "performance_score": metrics.get("performance_score", 1.0)
        }
        
    async def _select_provider(
        self,
        resources: List[ResourceSpec],
        location: Optional[str] = None
    ) -> ProviderType:
        """Select best provider based on requirements"""
        # Check GPU requirements
        has_gpu = any(r.resource_type == ResourceType.GPU for r in resources)
        
        if has_gpu:
            # Prefer providers with GPU capacity
            return ProviderType.AWS
        elif location and location.startswith("edge"):
            # Edge locations
            return ProviderType.ON_PREMISE
        else:
            # Default to Rackspace for bare metal
            return ProviderType.RACKSPACE
            
    async def _calculate_cost(
        self,
        provider: ProviderType,
        resources: List[ResourceSpec]
    ) -> Decimal:
        """Calculate hourly cost"""
        # Simplified pricing model
        costs = {
            ResourceType.CPU: Decimal("0.10"),  # per core per hour
            ResourceType.GPU: Decimal("2.50"),  # per GPU per hour
            ResourceType.MEMORY: Decimal("0.01"),  # per GB per hour
            ResourceType.STORAGE: Decimal("0.0001"),  # per GB per hour
        }
        
        total_cost = Decimal("0")
        for resource in resources:
            unit_cost = costs.get(resource.resource_type, Decimal("0"))
            total_cost += unit_cost * resource.quantity
            
        # Provider markup
        provider_markups = {
            ProviderType.AWS: Decimal("1.2"),
            ProviderType.AZURE: Decimal("1.15"),
            ProviderType.RACKSPACE: Decimal("1.0"),
            ProviderType.ON_PREMISE: Decimal("0.8")
        }
        
        markup = provider_markups.get(provider, Decimal("1.0"))
        return total_cost * markup
        
    def _calculate_sla_compliance(
        self,
        metrics: Dict[str, Any]
    ) -> Dict[str, bool]:
        """Calculate SLA compliance"""
        return {
            "uptime": metrics.get("uptime", 100.0) >= 99.9,
            "latency": metrics.get("latency_ms", 0) <= 10,
            "throughput": metrics.get("throughput_gbps", 0) >= 1.0,
            "availability": metrics.get("availability", 100.0) >= 99.5
        }
        
    async def _monitor_allocations(self):
        """Monitor active allocations"""
        while True:
            try:
                for allocation in self.allocations.values():
                    if allocation.status == AllocationStatus.ACTIVE:
                        # Get metrics
                        metrics = await self.get_allocation_metrics(allocation.allocation_id)
                        
                        # Check SLA compliance
                        if not all(metrics["sla_compliance"].values()):
                            logger.warning(
                                f"SLA violation for allocation {allocation.allocation_id}: "
                                f"{metrics['sla_compliance']}"
                            )
                            
                        # Check expiration
                        if datetime.utcnow() >= allocation.expires_at:
                            await self.deallocate_resources(allocation.allocation_id)
                            
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring: {e}")
                await asyncio.sleep(60)
                
    async def _cleanup_expired(self):
        """Clean up expired allocations"""
        while True:
            try:
                cutoff = datetime.utcnow() - timedelta(days=7)
                
                for allocation_id, allocation in list(self.allocations.items()):
                    if (allocation.status == AllocationStatus.TERMINATED and
                        allocation.created_at < cutoff):
                        del self.allocations[allocation_id]
                        
                await asyncio.sleep(3600)  # Run hourly
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in cleanup: {e}")
                await asyncio.sleep(3600)


# ============= Global Services =============

provisioning_engine = None
event_publisher = None


# ============= Lifespan Management =============

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global provisioning_engine, event_publisher
    
    # Startup
    logger.info("Starting Provisioning Service...")
    
    # Initialize services
    provisioning_engine = ProvisioningEngine()
    await provisioning_engine.start()
    
    event_publisher = EventPublisher("pulsar://pulsar:6650")
    event_publisher.connect()
    
    logger.info("Provisioning Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Provisioning Service...")
    
    await provisioning_engine.stop()
    event_publisher.close()
    
    logger.info("Provisioning Service shut down")


# ============= Create FastAPI App =============

app = create_base_app(
    service_name="provisioning-service",
    lifespan=lifespan
)


# ============= API Endpoints =============

@app.post("/api/v1/allocate", response_model=ResourceAllocation)
async def allocate_resources(
    request: AllocationRequest,
    background_tasks: BackgroundTasks
) -> ResourceAllocation:
    """Allocate compute resources for settlement"""
    allocation = await provisioning_engine.allocate_resources(request)
    
    # Publish allocation event
    background_tasks.add_task(
        event_publisher.publish,
        "compute.allocation.created",
        {
            "allocation_id": allocation.allocation_id,
            "settlement_id": allocation.settlement_id,
            "provider": allocation.provider,
            "status": allocation.status,
            "cost_per_hour": str(allocation.cost_per_hour)
        }
    )
    
    return allocation


@app.delete("/api/v1/allocate/{allocation_id}")
async def deallocate_resources(
    allocation_id: str,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """Deallocate compute resources"""
    success = await provisioning_engine.deallocate_resources(allocation_id)
    
    # Publish deallocation event
    background_tasks.add_task(
        event_publisher.publish,
        "compute.allocation.terminated",
        {
            "allocation_id": allocation_id,
            "success": success
        }
    )
    
    return {"success": success, "allocation_id": allocation_id}


@app.get("/api/v1/allocate/{allocation_id}")
async def get_allocation(allocation_id: str) -> ResourceAllocation:
    """Get allocation details"""
    allocation = provisioning_engine.allocations.get(allocation_id)
    if not allocation:
        raise HTTPException(status_code=404, detail="Allocation not found")
    return allocation


@app.get("/api/v1/allocate/{allocation_id}/metrics")
async def get_allocation_metrics(allocation_id: str) -> Dict[str, Any]:
    """Get allocation metrics and SLA compliance"""
    return await provisioning_engine.get_allocation_metrics(allocation_id)


@app.get("/api/v1/settlement/{settlement_id}/allocation")
async def get_settlement_allocation(settlement_id: str) -> ResourceAllocation:
    """Get allocation for a settlement"""
    allocation_id = provisioning_engine.settlement_allocations.get(settlement_id)
    if not allocation_id:
        raise HTTPException(status_code=404, detail="No allocation found for settlement")
        
    allocation = provisioning_engine.allocations.get(allocation_id)
    if not allocation:
        raise HTTPException(status_code=404, detail="Allocation not found")
        
    return allocation


@app.get("/api/v1/providers")
async def list_providers() -> Dict[str, List[str]]:
    """List available compute providers"""
    return {
        "providers": list(provisioning_engine.providers.keys()),
        "capabilities": {
            ProviderType.AWS: ["cpu", "gpu", "auto-scaling"],
            ProviderType.RACKSPACE: ["bare-metal", "dedicated", "high-performance"],
            ProviderType.ON_PREMISE: ["kubernetes", "edge", "custom"]
        }
    }


@app.get("/api/v1/health")
async def health_check() -> Dict[str, Any]:
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "provisioning-service",
        "providers": len(provisioning_engine.providers),
        "active_allocations": sum(
            1 for a in provisioning_engine.allocations.values()
            if a.status == AllocationStatus.ACTIVE
        )
    }


# ============= Compute Futures Integration Endpoints =============

class ComputeProvisionRequest(BaseModel):
    """Request model for compute futures provisioning"""
    settlement_id: str
    resource_type: str
    quantity: str  # Decimal as string
    duration_hours: int
    start_time: str  # ISO format datetime
    provider_id: str
    buyer_id: str
    is_failover: bool = False


@app.post("/api/v1/compute/provision")
async def provision_compute_futures(
    request: ComputeProvisionRequest,
    background_tasks: BackgroundTasks
) -> Dict[str, Any]:
    """
    Provision compute resources for futures contract settlement.
    This endpoint is called by the compute futures engine.
    """
    try:
        # Convert request to AllocationRequest format
        resources = []
        
        # Map resource type to ResourceSpec
        if request.resource_type.lower() == "gpu":
            resources.append(ResourceSpec(
                resource_type=ResourceType.GPU,
                quantity=Decimal(request.quantity),
                specifications={"gpu_type": "nvidia-a100"},
                required_features=["cuda", "tensor-cores"]
            ))
        elif request.resource_type.lower() == "cpu":
            resources.append(ResourceSpec(
                resource_type=ResourceType.CPU,
                quantity=Decimal(request.quantity),
                specifications={"cpu_type": "intel-xeon"},
                required_features=["avx512"]
            ))
        else:
            # Generic compute resource
            resources.append(ResourceSpec(
                resource_type=ResourceType.CPU,
                quantity=Decimal(request.quantity),
                specifications={"type": request.resource_type}
            ))
        
        # Create allocation request
        allocation_request = AllocationRequest(
            settlement_id=request.settlement_id,
            buyer_id=request.buyer_id,
            provider_id=request.provider_id,
            resources=resources,
            start_time=datetime.fromisoformat(request.start_time.replace('Z', '+00:00')),
            duration_hours=request.duration_hours,
            metadata={
                "is_failover": request.is_failover,
                "original_provider": request.provider_id
            }
        )
        
        # Allocate resources
        allocation = await provisioning_engine.allocate_resources(allocation_request)
        
        # Publish provisioning event
        background_tasks.add_task(
            event_publisher.publish,
            "compute.futures.provisioned",
            {
                "settlement_id": request.settlement_id,
                "allocation_id": allocation.allocation_id,
                "provider": allocation.provider,
                "status": allocation.status,
                "is_failover": request.is_failover
            }
        )
        
        return {
            "success": True,
            "allocation_id": allocation.allocation_id,
            "status": allocation.status,
            "provider": allocation.provider,
            "access_details": allocation.access_details,
            "monitoring_endpoints": allocation.monitoring_endpoints
        }
        
    except Exception as e:
        logger.error(f"Failed to provision compute: {e}")
        
        # Publish failure event
        background_tasks.add_task(
            event_publisher.publish,
            "compute.futures.provisioning_failed",
            {
                "settlement_id": request.settlement_id,
                "error": str(e),
                "is_failover": request.is_failover
            }
        )
        
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/metrics/compute/{settlement_id}")
async def get_compute_metrics(settlement_id: str) -> Dict[str, Any]:
    """
    Get compute metrics for SLA monitoring.
    This endpoint is called by the compute futures engine for SLA compliance checks.
    """
    # Get allocation for settlement
    allocation_id = provisioning_engine.settlement_allocations.get(settlement_id)
    if not allocation_id:
        # Return default metrics if allocation not found
        return {
            "uptime_percent": 100.0,
            "latency_ms": 5,
            "performance_score": 1.0,
            "error_rate": 0.0,
            "throughput_gbps": 10.0
        }
    
    # Get actual metrics
    allocation = provisioning_engine.allocations.get(allocation_id)
    if not allocation or allocation.status != AllocationStatus.ACTIVE:
        return {
            "uptime_percent": 0.0,
            "latency_ms": 9999,
            "performance_score": 0.0,
            "error_rate": 1.0,
            "throughput_gbps": 0.0
        }
    
    # Get provider metrics
    provider = provisioning_engine.providers.get(allocation.provider)
    if provider:
        metrics = await provider.get_metrics(allocation_id)
    else:
        # Mock metrics
        metrics = {
            "cpu_utilization": 45.2,
            "memory_utilization": 62.1,
            "network_throughput_gbps": 8.5,
            "disk_iops": 15000,
            "uptime": 99.95,
            "latency_ms": 12,
            "error_count": 2
        }
    
    # Calculate SLA-relevant metrics
    total_time = (datetime.utcnow() - allocation.activated_at).total_seconds() / 3600
    uptime_hours = total_time * (metrics.get("uptime", 100) / 100)
    
    return {
        "uptime_percent": metrics.get("uptime", 100.0),
        "latency_ms": metrics.get("latency_ms", 10),
        "performance_score": min(1.0, metrics.get("cpu_utilization", 50) / 100 + 0.5),
        "error_rate": metrics.get("error_count", 0) / max(1, total_time * 3600),
        "throughput_gbps": metrics.get("network_throughput_gbps", 10.0),
        "cpu_utilization": metrics.get("cpu_utilization", 50),
        "memory_utilization": metrics.get("memory_utilization", 60),
        "total_uptime_hours": uptime_hours,
        "total_runtime_hours": total_time
    }


@app.post("/api/v1/compute/register-failover")
async def register_failover_provider(
    provider_id: str,
    capabilities: List[str],
    priority: int = 100
) -> Dict[str, Any]:
    """Register a provider as failover option for compute futures"""
    # Store failover provider information
    # In practice, this would be stored in a database
    
    return {
        "success": True,
        "provider_id": provider_id,
        "capabilities": capabilities,
        "priority": priority,
        "registered_at": datetime.utcnow().isoformat()
    } 