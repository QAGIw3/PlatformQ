"""
Provisioning Service

Dynamic tenant and resource provisioning with auto-scaling.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from cassandra.cluster import Session
from minio import Minio
from pulsar.admin import PulsarAdmin
from openproject import OpenProject
from typing import Optional, Dict, List, Any
from datetime import datetime
import logging
import asyncio
import uuid

from platformq_shared import (
    create_base_app,
    EventProcessor,
    ServiceClients,
    add_error_handlers
)
from platformq_shared.config import ConfigLoader
from platformq_shared.event_publisher import EventPublisher

from .api.deps import (
    get_current_tenant_and_user,
    get_db_session,
    get_event_publisher,
    get_api_key_crud,
    get_user_crud,
    get_password_verifier
)
from .repository import (
    TenantProvisioningRepository,
    ResourceQuotaRepository,
    ScalingPolicyRepository,
    ScalingEventRepository,
    ResourceMetricsRepository
)
from .event_processors import (
    TenantProvisioningProcessor,
    ResourceScalingProcessor,
    QuotaManagementProcessor,
    ResourceCleanupProcessor,
    UserProvisioningProcessor
)
from .compute_provisioning import (
    ComputeProvisioningManager,
    ComputeProvisioningRequest,
    ComputeResourceType,
    ProvisioningStatus
)
from .dynamic_provisioning import (
    ResourceMonitor,
    ScalingEngine,
    TenantResourceManager,
    TenantTier
)
from .dependencies import (
    get_cassandra_session,
    get_minio_client,
    get_pulsar_admin,
    get_openproject_client,
    get_ignite_client,
    get_elasticsearch_client
)

logger = logging.getLogger(__name__)

# Service components
tenant_processor = None
scaling_processor = None
quota_processor = None
cleanup_processor = None
user_processor = None
resource_monitor = None
scaling_engine = None
tenant_manager = None
service_clients = None
compute_provisioning_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global tenant_processor, scaling_processor, quota_processor, cleanup_processor, user_processor
    global resource_monitor, scaling_engine, tenant_manager, service_clients, compute_provisioning_manager
    
    # Startup
    logger.info("Starting Provisioning Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    
    # Initialize service clients
    service_clients = ServiceClients(base_timeout=30.0, max_retries=3)
    app.state.service_clients = service_clients
    
    # Initialize repositories
    app.state.provisioning_repo = TenantProvisioningRepository(
        get_cassandra_session,
        event_publisher=app.state.event_publisher
    )
    app.state.quota_repo = ResourceQuotaRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    app.state.policy_repo = ScalingPolicyRepository(get_db_session)
    app.state.scaling_event_repo = ScalingEventRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    app.state.metrics_repo = ResourceMetricsRepository(get_db_session)
    
    # Initialize dynamic provisioning components
    k8s_config = {
        'in_cluster': settings.get('k8s_in_cluster', 'false').lower() == 'true',
        'namespace': settings.get('k8s_namespace', 'default')
    }
    
    resource_monitor = ResourceMonitor(
        prometheus_url=settings.get('prometheus_url', 'http://prometheus:9090'),
        k8s_config=k8s_config
    )
    await resource_monitor.start()
    app.state.resource_monitor = resource_monitor
    
    scaling_engine = ScalingEngine(
        k8s_config=k8s_config,
        prometheus_url=settings.get('prometheus_url', 'http://prometheus:9090')
    )
    await scaling_engine.initialize()
    app.state.scaling_engine = scaling_engine
    
    ignite_host = settings.get('ignite_host', 'ignite')
    ignite_port = int(settings.get('ignite_port', 10800))
    
    tenant_manager = TenantResourceManager(
        ignite_host=ignite_host,
        ignite_port=ignite_port,
        cache_name='tenant_resources'
    )
    await tenant_manager.initialize()
    app.state.tenant_manager = tenant_manager
    
    # Initialize compute provisioning manager
    compute_provisioning_manager = ComputeProvisioningManager(
        derivatives_engine_url=settings.get('derivatives_engine_url', 'http://derivatives-engine-service:8000'),
        ignite_client=get_ignite_client(),
        pulsar_publisher=app.state.event_publisher
    )
    app.state.compute_provisioning_manager = compute_provisioning_manager
    
    # Initialize event processors
    tenant_processor = TenantProvisioningProcessor(
        service_name="provisioning-service",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        provisioning_repo=app.state.provisioning_repo,
        quota_repo=app.state.quota_repo,
        resource_manager=tenant_manager,
        service_clients=service_clients
    )
    
    scaling_processor = ResourceScalingProcessor(
        service_name="provisioning-scaling",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        scaling_engine=scaling_engine,
        policy_repo=app.state.policy_repo,
        event_repo=app.state.scaling_event_repo,
        metrics_repo=app.state.metrics_repo,
        service_clients=service_clients
    )
    
    quota_processor = QuotaManagementProcessor(
        service_name="provisioning-quota",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        quota_repo=app.state.quota_repo,
        resource_manager=tenant_manager,
        service_clients=service_clients
    )
    
    cleanup_processor = ResourceCleanupProcessor(
        service_name="provisioning-cleanup",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        service_clients=service_clients
    )
    
    user_processor = UserProvisioningProcessor(
        service_name="provisioning-user",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        service_clients=service_clients
    )
    
    # Start event processors
    await asyncio.gather(
        tenant_processor.start(),
        scaling_processor.start(),
        quota_processor.start(),
        cleanup_processor.start(),
        user_processor.start()
    )
    
    # Start monitoring tasks
    app.state.monitor_task = asyncio.create_task(
        resource_monitor.monitor_loop()
    )
    app.state.scaling_task = asyncio.create_task(
        scaling_engine.auto_scaling_loop()
    )
    
    logger.info("Provisioning Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Provisioning Service...")
    
    # Cancel monitoring tasks
    if hasattr(app.state, "monitor_task"):
        app.state.monitor_task.cancel()
    if hasattr(app.state, "scaling_task"):
        app.state.scaling_task.cancel()
        
    # Stop event processors
    await asyncio.gather(
        tenant_processor.stop() if tenant_processor else asyncio.sleep(0),
        scaling_processor.stop() if scaling_processor else asyncio.sleep(0),
        quota_processor.stop() if quota_processor else asyncio.sleep(0),
        cleanup_processor.stop() if cleanup_processor else asyncio.sleep(0),
        user_processor.stop() if user_processor else asyncio.sleep(0)
    )
    
    # Cleanup resources
    if resource_monitor:
        await resource_monitor.stop()
    if scaling_engine:
        await scaling_engine.cleanup()
    if tenant_manager:
        await tenant_manager.cleanup()
    if compute_provisioning_manager:
        await compute_provisioning_manager.close()
        
    logger.info("Provisioning Service shutdown complete")


# Create app with enhanced patterns
app = create_base_app(
    service_name="provisioning-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud,
    user_crud_dependency=get_user_crud,
    password_verifier_dependency=get_password_verifier,
    event_processors=[
        tenant_processor, scaling_processor, quota_processor,
        cleanup_processor, user_processor
    ] if all([tenant_processor, scaling_processor, quota_processor, cleanup_processor, user_processor]) else []
)

# Set lifespan
app.router.lifespan_context = lifespan

# Service root endpoint
@app.get("/")
def read_root():
    return {
        "service": "provisioning-service",
        "version": "2.0",
        "features": [
            "tenant-provisioning",
            "dynamic-scaling",
            "resource-quotas",
            "auto-scaling",
            "multi-tier-support",
            "event-driven"
        ]
    }


# Request Models
class TenantProvisioningRequest(BaseModel):
    tenant_id: str
    tenant_name: str
    tier: str = "STARTER"

class ScalingPolicyUpdate(BaseModel):
    service_name: str
    min_replicas: Optional[int] = None
    max_replicas: Optional[int] = None
    target_cpu_percent: Optional[float] = None
    target_memory_percent: Optional[float] = None
    scale_up_cooldown_seconds: Optional[int] = None
    scale_down_cooldown_seconds: Optional[int] = None
    enabled: Optional[bool] = None


# API Endpoints using new patterns
@app.post("/api/v1/provision")
async def provision_tenant_manual(
    request: TenantProvisioningRequest,
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """Manual tenant provisioning endpoint"""
    
    # Check permissions
    user = context["user"]
    if "admin" not in user.get("roles", []):
        raise HTTPException(status_code=403, detail="Admin role required")
        
    # Publish tenant created event to trigger provisioning
    publisher.publish_event(
        topic="persistent://platformq/system/tenant-created-events",
        event={
            "tenant_id": request.tenant_id,
            "tenant_name": request.tenant_name,
            "tier": request.tier,
            "created_by": user["id"],
            "created_at": datetime.utcnow().isoformat()
        }
    )
    
    return {
        "status": "provisioning_initiated",
        "tenant_id": request.tenant_id,
        "message": "Tenant provisioning has been initiated"
    }


@app.post("/api/v1/compute/provision")
async def provision_compute_resources(
    request: ComputeProvisioningRequest,
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """Provision compute resources"""
    
    # Use tenant from context
    request.tenant_id = context["tenant_id"]
    
    # Get compute provisioning manager
    compute_manager = app.state.compute_provisioning_manager
    if not compute_manager:
        raise HTTPException(status_code=503, detail="Compute provisioning not available")
    
    # Provision resources
    result = await compute_manager.provision_compute(request)
    
    if result.status == ProvisioningStatus.FAILED:
        raise HTTPException(status_code=400, detail=result.message or "Provisioning failed")
    
    return {
        "request_id": result.request_id,
        "allocation_id": result.allocation_id,
        "status": result.status.value,
        "provider": result.provider,
        "access_details": result.access_details,
        "cost": str(result.cost) if result.cost else None
    }


@app.get("/api/v1/compute/provision/{request_id}")
async def get_compute_provision_status(
    request_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get status of compute provisioning request"""
    
    compute_manager = app.state.compute_provisioning_manager
    if not compute_manager:
        raise HTTPException(status_code=503, detail="Compute provisioning not available")
    
    status = await compute_manager.get_provisioning_status(request_id)
    
    if status.get("status") == "not_found":
        raise HTTPException(status_code=404, detail="Provisioning request not found")
    
    return status


@app.delete("/api/v1/compute/provision/{request_id}")
async def terminate_compute_provision(
    request_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Terminate provisioned compute resources"""
    
    compute_manager = app.state.compute_provisioning_manager
    if not compute_manager:
        raise HTTPException(status_code=503, detail="Compute provisioning not available")
    
    success = await compute_manager.terminate_provision(request_id)
    
    if not success:
        raise HTTPException(status_code=400, detail="Failed to terminate provision")
    
    return {
        "status": "terminated",
        "request_id": request_id
    }


@app.get("/api/v1/compute/capacity")
async def get_available_compute_capacity(
    resource_type: str,
    region: Optional[str] = None,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get available compute capacity"""
    
    compute_manager = app.state.compute_provisioning_manager
    if not compute_manager:
        raise HTTPException(status_code=503, detail="Compute provisioning not available")
    
    try:
        resource_type_enum = ComputeResourceType(resource_type)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid resource type: {resource_type}")
    
    capacity = await compute_manager.get_available_capacity(resource_type_enum, region)
    
    return capacity


@app.get("/api/v1/tenants/{tenant_id}/provisioning-status")
async def get_provisioning_status(
    tenant_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get provisioning status for a tenant"""
    
    # Check permissions
    if tenant_id != context["tenant_id"] and "admin" not in context["user"].get("roles", []):
        raise HTTPException(status_code=403, detail="Access denied")
        
    provisioning_repo = app.state.provisioning_repo
    history = provisioning_repo.get_tenant_provisioning_history(
        uuid.UUID(tenant_id),
        limit=1
    )
    
    if not history:
        raise HTTPException(status_code=404, detail="No provisioning record found")
        
    return history[0]


@app.get("/api/v1/tenants/{tenant_id}/quota")
async def get_tenant_quota(
    tenant_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get resource quota for a tenant"""
    
    # Check permissions
    if tenant_id != context["tenant_id"] and "admin" not in context["user"].get("roles", []):
        raise HTTPException(status_code=403, detail="Access denied")
        
    quota_repo = app.state.quota_repo
    quota_info = quota_repo.get_quota_usage(tenant_id)
    
    if not quota_info:
        raise HTTPException(status_code=404, detail="No quota found for tenant")
        
    return quota_info


@app.put("/api/v1/tenants/{tenant_id}/quota")
async def update_tenant_quota(
    tenant_id: str,
    updates: Dict[str, Any],
    context: dict = Depends(get_current_tenant_and_user)
):
    """Update resource quota for a tenant"""
    
    # Check admin permissions
    if "admin" not in context["user"].get("roles", []):
        raise HTTPException(status_code=403, detail="Admin role required")
        
    quota_repo = app.state.quota_repo
    updated = quota_repo.update_quota(tenant_id, updates)
    
    if not updated:
        raise HTTPException(status_code=404, detail="Failed to update quota")
        
    return updated


# Dynamic Provisioning API Endpoints
@app.get("/api/v1/metrics/{service_name}")
async def get_service_metrics(
    service_name: str,
    tenant_id: Optional[str] = None,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get current metrics for a service"""
    
    # Check permissions
    if tenant_id and tenant_id != context["tenant_id"] and "admin" not in context["user"].get("roles", []):
        raise HTTPException(status_code=403, detail="Access denied")
        
    if not resource_monitor:
        raise HTTPException(status_code=503, detail="Resource monitor not initialized")
        
    metrics = resource_monitor.get_current_metrics(service_name)
    if not metrics:
        raise HTTPException(status_code=404, detail=f"No metrics found for service {service_name}")
        
    return {
        "service_name": metrics.service_name,
        "timestamp": metrics.timestamp.isoformat(),
        "cpu_usage": metrics.cpu_usage,
        "memory_usage": metrics.memory_usage,
        "request_rate": metrics.request_rate,
        "error_rate": metrics.error_rate,
        "response_time_p99": metrics.response_time_p99,
        "pod_count": metrics.pod_count
    }


@app.get("/api/v1/metrics/cluster/current")
async def get_cluster_metrics(
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get current cluster-wide metrics"""
    
    # Check admin permissions
    if "admin" not in context["user"].get("roles", []):
        raise HTTPException(status_code=403, detail="Admin role required")
        
    if not resource_monitor:
        raise HTTPException(status_code=503, detail="Resource monitor not initialized")
        
    metrics = resource_monitor.get_cluster_metrics()
    if not metrics:
        raise HTTPException(status_code=404, detail="No cluster metrics available")
        
    return {
        "timestamp": metrics.timestamp.isoformat(),
        "total_cpu_cores": metrics.total_cpu_cores,
        "used_cpu_cores": metrics.used_cpu_cores,
        "cpu_utilization": (metrics.used_cpu_cores / metrics.total_cpu_cores * 100) if metrics.total_cpu_cores > 0 else 0,
        "total_memory_bytes": metrics.total_memory_bytes,
        "used_memory_bytes": metrics.used_memory_bytes,
        "memory_utilization": (metrics.used_memory_bytes / metrics.total_memory_bytes * 100) if metrics.total_memory_bytes > 0 else 0,
        "node_count": metrics.node_count,
        "pod_count": metrics.pod_count
    }


@app.get("/api/v1/scaling/policies")
async def list_scaling_policies(
    enabled_only: bool = True,
    context: dict = Depends(get_current_tenant_and_user)
):
    """List all scaling policies"""
    
    # Check admin permissions
    if "admin" not in context["user"].get("roles", []):
        raise HTTPException(status_code=403, detail="Admin role required")
        
    policy_repo = app.state.policy_repo
    policies = policy_repo.get_all_policies(enabled_only=enabled_only)
    
    return {
        "policies": policies,
        "total": len(policies)
    }


@app.get("/api/v1/scaling/policies/{service_name}")
async def get_scaling_policy(
    service_name: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get scaling policy for a service"""
    
    policy_repo = app.state.policy_repo
    policy = policy_repo.get_policy(service_name)
    
    if not policy:
        raise HTTPException(status_code=404, detail=f"No policy found for service {service_name}")
        
    return policy


@app.put("/api/v1/scaling/policies/{service_name}")
async def update_scaling_policy(
    service_name: str,
    policy_update: ScalingPolicyUpdate,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Update scaling policy for a service"""
    
    # Check admin permissions
    if "admin" not in context["user"].get("roles", []):
        raise HTTPException(status_code=403, detail="Admin role required")
        
    policy_repo = app.state.policy_repo
    
    # Get existing policy
    existing = policy_repo.get_policy(service_name)
    if not existing:
        # Create new policy
        policy = policy_repo.create_policy(
            service_name=service_name,
            min_replicas=policy_update.min_replicas or 1,
            max_replicas=policy_update.max_replicas or 10,
            target_cpu=policy_update.target_cpu_percent or 70.0,
            target_memory=policy_update.target_memory_percent or 80.0,
            scale_up_cooldown=policy_update.scale_up_cooldown_seconds or 300,
            scale_down_cooldown=policy_update.scale_down_cooldown_seconds or 600
        )
    else:
        # Update existing
        policy = policy_repo.update_policy(
            service_name,
            policy_update.dict(exclude_unset=True)
        )
        
    if not policy:
        raise HTTPException(status_code=500, detail="Failed to update policy")
        
    return policy


@app.get("/api/v1/scaling/history")
async def get_scaling_history(
    service_name: Optional[str] = None,
    tenant_id: Optional[str] = None,
    limit: int = 100,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get scaling event history"""
    
    # Check permissions
    if tenant_id and tenant_id != context["tenant_id"] and "admin" not in context["user"].get("roles", []):
        raise HTTPException(status_code=403, detail="Access denied")
        
    event_repo = app.state.scaling_event_repo
    events = event_repo.get_scaling_history(
        service_name=service_name,
        tenant_id=tenant_id or (None if "admin" in context["user"].get("roles", []) else context["tenant_id"]),
        limit=limit
    )
    
    return {
        "events": events,
        "total": len(events)
    }


@app.post("/api/v1/scaling/manual")
async def manual_scale(
    service_name: str,
    target_replicas: int,
    reason: str = "Manual scaling request",
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """Manually scale a service"""
    
    # Check admin permissions
    if "admin" not in context["user"].get("roles", []):
        raise HTTPException(status_code=403, detail="Admin role required")
        
    # Publish scaling request event
    publisher.publish_event(
        topic="persistent://platformq/system/scaling-request-events",
        event={
            "service_name": service_name,
            "target_replicas": target_replicas,
            "reason": reason,
            "requested_by": context["user"]["id"],
            "tenant_id": context["tenant_id"],
            "requested_at": datetime.utcnow().isoformat()
        }
    )
    
    return {
        "status": "scaling_requested",
        "service_name": service_name,
        "target_replicas": target_replicas
    }


# Health check with resource monitoring
@app.get("/health/detailed")
async def detailed_health_check():
    """Detailed health check including resource monitoring"""
    health = {
        "status": "healthy",
        "checks": {}
    }
    
    # Check resource monitor
    if hasattr(app.state, "resource_monitor"):
        health["checks"]["resource_monitor"] = {
            "status": "active" if app.state.resource_monitor.is_monitoring else "inactive"
        }
        
    # Check scaling engine
    if hasattr(app.state, "scaling_engine"):
        health["checks"]["scaling_engine"] = {
            "status": "active" if app.state.scaling_engine.is_running else "inactive"
        }
        
    # Check tenant manager
    if hasattr(app.state, "tenant_manager"):
        health["checks"]["tenant_manager"] = {
            "status": "connected" if app.state.tenant_manager.is_connected else "disconnected"
        }
        
    return health 