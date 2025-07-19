"""Provisioning Service API

Handles tenant provisioning, resource scaling, and compute provisioning.
"""

from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
import os
import logging

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from prometheus_client.core import CollectorRegistry
from pydantic import BaseModel, Field

from platformq_compute_common.models import (
    AllocationRequest,
    AllocationResponse,
    ComputeResourceType,
    AllocationStrategy
)
from platformq_compute_common.providers import ProviderRegistry
from platformq_shared.security import get_current_user_from_trusted_header as get_current_user

from .core.config import settings
from .core.config_manager import ConfigManager
from .compute_provisioning import ComputeProvisioningManager
from .dynamic_provisioning import (
    ResourceMonitor,
    ScalingEngine,
    TenantResourceManager
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
registry = CollectorRegistry()
provisioning_counter = Counter(
    'provisioning_requests_total',
    'Total number of provisioning requests',
    ['type', 'status'],
    registry=registry
)
scaling_counter = Counter(
    'scaling_actions_total',
    'Total number of scaling actions',
    ['service', 'action'],
    registry=registry
)

# Global instances
config_manager = ConfigManager(settings)
compute_manager = None
resource_monitor = None
scaling_engine = None
tenant_manager = None


class TenantProvisionRequest(BaseModel):
    tenant_id: str
    tenant_name: str
    tier: str = "starter"
    metadata: Optional[Dict[str, Any]] = None


class ScalingPolicyRequest(BaseModel):
    service_name: str
    min_replicas: int = 1
    max_replicas: int = 10
    target_cpu_utilization: float = 70.0
    target_memory_utilization: float = 80.0
    enable_predictive_scaling: bool = True
    enable_vertical_scaling: bool = True


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global compute_manager, resource_monitor, scaling_engine, tenant_manager
    
    # Initialize configuration manager
    await config_manager.initialize()
    
    # Register service with Consul
    await config_manager.register_service()
    
    # Initialize Ignite client
    from pyignite import Client as IgniteClient
    ignite_client = IgniteClient()
    ignite_client.connect(
        [(settings.ignite_config["host"], settings.ignite_config["port"])]
    )
    
    # Initialize Pulsar client
    import pulsar
    pulsar_client = pulsar.Client(settings.pulsar_config["service_url"])
    
    # Initialize components
    provider_registry = ProviderRegistry()
    
    compute_manager = ComputeProvisioningManager(
        config_manager=config_manager,
        provider_registry=provider_registry,
        derivatives_engine_url=settings.derivatives_engine_url,
        ignite_client=ignite_client,
        pulsar_publisher=pulsar_client
    )
    await compute_manager.initialize()
    
    resource_monitor = ResourceMonitor(
        prometheus_url=settings.prometheus_url,
        ignite_client=ignite_client,
        pulsar_client=pulsar_client,
        kubernetes_api_url="https://kubernetes.default.svc"
    )
    await resource_monitor.start()
    
    scaling_engine = ScalingEngine(
        resource_monitor=resource_monitor,
        ignite_client=ignite_client,
        pulsar_client=pulsar_client,
        kubernetes_namespace=settings.kubernetes_namespace
    )
    await scaling_engine.start()
    
    tenant_manager = TenantResourceManager(ignite_client)
    await tenant_manager.start()
    
    logger.info("Provisioning Service started")
    
    yield
    
    # Cleanup
    await scaling_engine.stop()
    await resource_monitor.stop()
    await compute_manager.close()
    await tenant_manager.stop()
    
    ignite_client.close()
    pulsar_client.close()
    
    await config_manager.deregister_service()
    await config_manager.close()
    
    logger.info("Provisioning Service stopped")


app = FastAPI(
    title="Provisioning Service",
    description="Manages tenant provisioning, resource scaling, and compute provisioning",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "provisioning-service",
        "version": "1.0.0"
    }


# Tenant provisioning endpoints
@app.post("/api/v1/tenants/provision")
async def provision_tenant(
    request: TenantProvisionRequest,
    current_user=Depends(get_current_user)
):
    """Provision resources for a new tenant"""
    try:
        # Create tenant quota
        quota = tenant_manager.create_tenant_quota(
            request.tenant_id,
            request.tier
        )
        
        # Provision infrastructure resources
        # This would integrate with the full workflow
        
        provisioning_counter.labels(type="tenant", status="success").inc()
        
        return {
            "status": "provisioned",
            "tenant_id": request.tenant_id,
            "quota": {
                "cpu_cores": quota.max_cpu_cores,
                "memory_gb": quota.max_memory_gb,
                "storage_gb": quota.max_storage_gb,
                "gpu_count": quota.max_gpu_count
            }
        }
        
    except Exception as e:
        provisioning_counter.labels(type="tenant", status="failure").inc()
        logger.error(f"Failed to provision tenant: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/tenants/{tenant_id}/quota")
async def get_tenant_quota(
    tenant_id: str,
    current_user=Depends(get_current_user)
):
    """Get tenant resource quota"""
    quota = tenant_manager.get_tenant_quota(tenant_id)
    
    if not quota:
        raise HTTPException(status_code=404, detail="Tenant not found")
    
    return {
        "tenant_id": tenant_id,
        "tier": quota.tier.value,
        "max_cpu_cores": quota.max_cpu_cores,
        "max_memory_gb": quota.max_memory_gb,
        "max_storage_gb": quota.max_storage_gb,
        "max_gpu_count": quota.max_gpu_count,
        "max_monthly_cost": float(quota.max_monthly_cost)
    }


@app.get("/api/v1/tenants/{tenant_id}/usage")
async def get_tenant_usage(
    tenant_id: str,
    current_user=Depends(get_current_user)
):
    """Get current resource usage for a tenant"""
    usage = await tenant_manager.get_tenant_usage(tenant_id)
    
    if not usage:
        raise HTTPException(status_code=404, detail="Usage data not found")
    
    return {
        "tenant_id": tenant_id,
        "cpu_cores_used": usage.cpu_cores_used,
        "memory_gb_used": usage.memory_gb_used,
        "storage_gb_used": usage.storage_gb_used,
        "gpu_count_used": usage.gpu_count_used,
        "monthly_cost": float(usage.monthly_cost),
        "timestamp": usage.timestamp.isoformat()
    }


# Compute provisioning endpoints
@app.post("/api/v1/compute/provision", response_model=AllocationResponse)
async def provision_compute(
    request: AllocationRequest,
    current_user=Depends(get_current_user)
):
    """Provision compute resources"""
    try:
        # Override tenant_id from auth
        request.tenant_id = current_user["tenant_id"]
        
        response = await compute_manager.provision_compute(request)
        
        if response.success:
            provisioning_counter.labels(type="compute", status="success").inc()
        else:
            provisioning_counter.labels(type="compute", status="failure").inc()
            
        return response
        
    except Exception as e:
        provisioning_counter.labels(type="compute", status="error").inc()
        logger.error(f"Failed to provision compute: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/compute/provision/{allocation_id}")
async def get_provisioning_status(
    allocation_id: str,
    current_user=Depends(get_current_user)
):
    """Get compute provisioning status"""
    status = await compute_manager.get_provisioning_status(allocation_id)
    
    if status["status"] == "not_found":
        raise HTTPException(status_code=404, detail="Allocation not found")
    
    return status


@app.delete("/api/v1/compute/provision/{allocation_id}")
async def terminate_compute(
    allocation_id: str,
    current_user=Depends(get_current_user)
):
    """Terminate compute resources"""
    success = await compute_manager.terminate_provision(allocation_id)
    
    if not success:
        raise HTTPException(status_code=400, detail="Failed to terminate resources")
    
    return {"status": "terminated", "allocation_id": allocation_id}


@app.get("/api/v1/compute/capacity")
async def get_available_capacity(
    resource_type: Optional[ComputeResourceType] = None,
    region: Optional[str] = None
):
    """Get available compute capacity"""
    capacity = await compute_manager.get_available_capacity(
        resource_type=resource_type,
        region=region
    )
    
    return capacity


# Scaling endpoints
@app.get("/api/v1/scaling/policies")
async def list_scaling_policies(
    service_name: Optional[str] = None,
    current_user=Depends(get_current_user)
):
    """List scaling policies"""
    policies = []
    
    # Get all policies from cache
    for key in scaling_engine.policies_cache.keys():
        policy = scaling_engine.policies_cache.get(key)
        
        if service_name and policy.service_name != service_name:
            continue
            
        policies.append({
            "service_name": policy.service_name,
            "min_replicas": policy.min_replicas,
            "max_replicas": policy.max_replicas,
            "target_cpu_utilization": policy.target_cpu_utilization,
            "target_memory_utilization": policy.target_memory_utilization,
            "enable_predictive_scaling": policy.enable_predictive_scaling,
            "enable_vertical_scaling": policy.enable_vertical_scaling
        })
    
    return {"policies": policies}


@app.post("/api/v1/scaling/policies")
async def create_scaling_policy(
    request: ScalingPolicyRequest,
    current_user=Depends(get_current_user)
):
    """Create or update scaling policy"""
    from .dynamic_provisioning.scaling_engine import ScalingPolicy
    
    policy = ScalingPolicy(
        service_name=request.service_name,
        min_replicas=request.min_replicas,
        max_replicas=request.max_replicas,
        target_cpu_utilization=request.target_cpu_utilization,
        target_memory_utilization=request.target_memory_utilization,
        enable_predictive_scaling=request.enable_predictive_scaling,
        enable_vertical_scaling=request.enable_vertical_scaling
    )
    
    scaling_engine.update_policy(policy)
    
    return {"status": "created", "service_name": request.service_name}


@app.get("/api/v1/scaling/decisions")
async def get_recent_scaling_decisions(
    service_name: Optional[str] = None,
    hours: int = Query(default=24, ge=1, le=168)
):
    """Get recent scaling decisions"""
    decisions = scaling_engine.get_recent_decisions(
        service_name=service_name,
        hours=hours
    )
    
    return {
        "decisions": [
            {
                "service_name": d.service_name,
                "action": d.action.value,
                "timestamp": d.timestamp.isoformat(),
                "reason": d.reason,
                "confidence": d.confidence,
                "current_replicas": d.current_replicas,
                "target_replicas": d.target_replicas
            }
            for d in decisions
        ]
    }


# Metrics endpoints
@app.get("/api/v1/metrics/resources")
async def get_resource_metrics(
    service_name: Optional[str] = None
):
    """Get current resource metrics"""
    if service_name:
        metrics = resource_monitor.get_current_metrics(service_name)
        if not metrics:
            raise HTTPException(status_code=404, detail="Metrics not found")
        
        return {
            "service_name": service_name,
            "cpu_usage": metrics.cpu_usage,
            "memory_usage": metrics.memory_usage,
            "request_rate": metrics.request_rate,
            "error_rate": metrics.error_rate,
            "response_time_p99": metrics.response_time_p99,
            "pod_count": metrics.pod_count,
            "timestamp": metrics.timestamp.isoformat()
        }
    else:
        # Get cluster metrics
        cluster_metrics = resource_monitor.get_cluster_metrics()
        if not cluster_metrics:
            raise HTTPException(status_code=404, detail="Cluster metrics not found")
        
        return {
            "total_cpu_cores": cluster_metrics.total_cpu_cores,
            "used_cpu_cores": cluster_metrics.used_cpu_cores,
            "total_memory_bytes": cluster_metrics.total_memory_bytes,
            "used_memory_bytes": cluster_metrics.used_memory_bytes,
            "node_count": cluster_metrics.node_count,
            "pod_count": cluster_metrics.pod_count,
            "timestamp": cluster_metrics.timestamp.isoformat()
        }


@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    return generate_latest(registry)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 