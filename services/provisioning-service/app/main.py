import pulsar
import avro.schema
import avro.io
import io
import logging
import os
import sys
import re
import signal
import threading
import httpx
import asyncio

from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from cassandra.cluster import Cluster, Session
from minio import Minio
from pulsar.admin import PulsarAdmin
from openproject import OpenProject
from typing import Optional, Dict, List
from datetime import datetime, timedelta

from .workflow import provision_tenant
from .core.config import settings

# Add project root to path to allow imports from shared_lib
# This is a hack for local development; in a container, this is handled by PYTHONPATH
if "/app" not in sys.path:
    sys.path.append("/app")
    
from platformq_shared.event_publisher import EventPublisher # We use it for creating a client
from platformq_shared.events import UserCreatedEvent
from pulsar.schema import AvroSchema
from .nextcloud_provisioner import NextcloudProvisioner
from pyignite import Client as IgniteClient
from .scaling import AdaptiveScaler
from .events import ResourceAnomalyEvent
from elasticsearch import Elasticsearch

# Import dynamic provisioning components
from .dynamic_provisioning import (
    ResourceMonitor,
    ScalingEngine,
    TenantResourceManager,
    TenantTier
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Provisioning Service with Dynamic Resource Management")

class TenantProvisioningRequest(BaseModel):
    tenant_id: str
    tenant_name: str
    tier: Optional[str] = "starter"  # Default tier

class ScalingPolicyUpdate(BaseModel):
    service_name: str
    min_replicas: Optional[int] = None
    max_replicas: Optional[int] = None
    target_cpu_utilization: Optional[float] = None
    target_memory_utilization: Optional[float] = None
    enable_predictive_scaling: Optional[bool] = None
    enable_vertical_scaling: Optional[bool] = None
    cost_aware: Optional[bool] = None

class ResourceBudget(BaseModel):
    service_name: str
    monthly_limit: float

# Initialize dynamic provisioning components
resource_monitor = None
scaling_engine = None
tenant_manager = None

def get_cassandra_session() -> Session:
    cluster = Cluster([settings.cassandra_host], port=settings.cassandra_port)
    return cluster.connect()

def get_minio_client() -> Minio:
    return Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure
    )

def get_pulsar_admin() -> PulsarAdmin:
    return PulsarAdmin(settings.pulsar_admin_url)

def get_openproject_client() -> OpenProject:
    return OpenProject(url=settings.openproject_url, api_key=settings.openproject_api_key)

def get_elasticsearch_client():
    return Elasticsearch(
        [f"{settings.elasticsearch_host}:{settings.elasticsearch_port}"],
        scheme=settings.elasticsearch_scheme,
        http_auth=(settings.elasticsearch_username, settings.elasticsearch_password) if settings.elasticsearch_username else None
    )

@app.post("/provision")
def provision_tenant_endpoint(
    request: TenantProvisioningRequest,
    cassandra_session: Session = Depends(get_cassandra_session),
    minio_client: Minio = Depends(get_minio_client),
    pulsar_admin: PulsarAdmin = Depends(get_pulsar_admin),
    openproject_client: OpenProject = Depends(get_openproject_client),
):
    """
    Provision a new tenant with dynamic resource management.
    """
    scaler = AdaptiveScaler(ignite_client, resource_cache)
    scaler.initialize_tenant_resources(request.tenant_id)
    
    # Get Elasticsearch client
    es_client = get_elasticsearch_client()
    
    # Create tenant resource quota
    if tenant_manager:
        try:
            tier = TenantTier(request.tier.lower())
            tenant_manager.create_tenant_quota(request.tenant_id, tier)
        except ValueError:
            logger.warning(f"Invalid tier {request.tier}, using STARTER")
            tenant_manager.create_tenant_quota(request.tenant_id, TenantTier.STARTER)
    
    provision_tenant(
        tenant_id=request.tenant_id,
        tenant_name=request.tenant_name,
        cassandra_session=cassandra_session,
        minio_client=minio_client,
        pulsar_admin=pulsar_admin,
        openproject_client=openproject_client,
        ignite_client=ignite_client,
        es_client=es_client,
    )
    return {"status": "success", "message": f"Tenant {request.tenant_id} provisioned with {request.tier} tier"}

# Dynamic Provisioning API Endpoints

@app.get("/api/v1/metrics/{service_name}")
async def get_service_metrics(service_name: str):
    """Get current metrics for a service"""
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
async def get_cluster_metrics():
    """Get current cluster-wide metrics"""
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

@app.put("/api/v1/scaling/policy")
async def update_scaling_policy(policy_update: ScalingPolicyUpdate):
    """Update scaling policy for a service"""
    if not scaling_engine:
        raise HTTPException(status_code=503, detail="Scaling engine not initialized")
    
    # Get existing policy
    policy = scaling_engine.get_policy(policy_update.service_name)
    if not policy:
        raise HTTPException(status_code=404, detail=f"No policy found for service {policy_update.service_name}")
    
    # Update fields
    if policy_update.min_replicas is not None:
        policy.min_replicas = policy_update.min_replicas
    if policy_update.max_replicas is not None:
        policy.max_replicas = policy_update.max_replicas
    if policy_update.target_cpu_utilization is not None:
        policy.target_cpu_utilization = policy_update.target_cpu_utilization
    if policy_update.target_memory_utilization is not None:
        policy.target_memory_utilization = policy_update.target_memory_utilization
    if policy_update.enable_predictive_scaling is not None:
        policy.enable_predictive_scaling = policy_update.enable_predictive_scaling
    if policy_update.enable_vertical_scaling is not None:
        policy.enable_vertical_scaling = policy_update.enable_vertical_scaling
    if policy_update.cost_aware is not None:
        policy.cost_aware = policy_update.cost_aware
    
    # Save updated policy
    scaling_engine.update_policy(policy)
    
    return {"status": "success", "message": f"Updated scaling policy for {policy_update.service_name}"}

@app.get("/api/v1/scaling/decisions")
async def get_scaling_decisions(service_name: Optional[str] = None, hours: int = 24):
    """Get recent scaling decisions"""
    if not scaling_engine:
        raise HTTPException(status_code=503, detail="Scaling engine not initialized")
    
    decisions = scaling_engine.get_recent_decisions(service_name, hours)
    
    return {
        "decisions": [
            {
                "service_name": d.service_name,
                "timestamp": d.timestamp.isoformat(),
                "action": d.action.value,
                "current_replicas": d.current_replicas,
                "target_replicas": d.target_replicas,
                "reason": d.reason,
                "confidence": d.confidence,
                "estimated_cost_impact": d.estimated_cost_impact
            }
            for d in decisions
        ]
    }

@app.post("/api/v1/cost/budget")
async def set_service_budget(budget: ResourceBudget):
    """Set monthly budget for a service"""
    if not scaling_engine:
        raise HTTPException(status_code=503, detail="Scaling engine not initialized")
    
    scaling_engine.cost_optimizer.set_budget(budget.service_name, budget.monthly_limit)
    
    return {"status": "success", "message": f"Set budget ${budget.monthly_limit}/month for {budget.service_name}"}

@app.get("/api/v1/cost/report")
async def get_cost_report(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    service_name: Optional[str] = None
):
    """Get cost report"""
    if not scaling_engine:
        raise HTTPException(status_code=503, detail="Scaling engine not initialized")
    
    # Default to last 30 days
    if not end_date:
        end = datetime.utcnow()
    else:
        end = datetime.fromisoformat(end_date)
    
    if not start_date:
        start = end - timedelta(days=30)
    else:
        start = datetime.fromisoformat(start_date)
    
    report = scaling_engine.cost_optimizer.get_cost_report(start, end, service_name)
    
    return report

@app.get("/api/v1/tenants/{tenant_id}/usage")
async def get_tenant_usage(tenant_id: str):
    """Get resource usage for a tenant"""
    if not tenant_manager:
        raise HTTPException(status_code=503, detail="Tenant manager not initialized")
    
    usage = await tenant_manager.get_tenant_usage(tenant_id)
    if not usage:
        raise HTTPException(status_code=404, detail=f"No usage data found for tenant {tenant_id}")
    
    return {
        "tenant_id": usage.tenant_id,
        "timestamp": usage.timestamp.isoformat(),
        "cpu_cores_used": usage.cpu_cores_used,
        "memory_gb_used": usage.memory_gb_used,
        "storage_gb_used": usage.storage_gb_used,
        "pod_count": usage.pod_count,
        "service_count": usage.service_count,
        "gpu_count_used": usage.gpu_count_used,
        "monthly_cost": usage.monthly_cost,
        "trends": {
            "cpu_usage_trend": usage.cpu_usage_trend,
            "memory_usage_trend": usage.memory_usage_trend,
            "cost_trend": usage.cost_trend
        }
    }

@app.get("/api/v1/tenants/{tenant_id}/quota")
async def get_tenant_quota(tenant_id: str):
    """Get resource quota for a tenant"""
    if not tenant_manager:
        raise HTTPException(status_code=503, detail="Tenant manager not initialized")
    
    quota = tenant_manager.get_tenant_quota(tenant_id)
    if not quota:
        raise HTTPException(status_code=404, detail=f"No quota found for tenant {tenant_id}")
    
    return {
        "tenant_id": quota.tenant_id,
        "tier": quota.tier.value,
        "max_cpu_cores": quota.max_cpu_cores,
        "max_memory_gb": quota.max_memory_gb,
        "max_storage_gb": quota.max_storage_gb,
        "max_pods": quota.max_pods,
        "max_services": quota.max_services,
        "max_gpu_count": quota.max_gpu_count,
        "max_monthly_cost": quota.max_monthly_cost,
        "priority": quota.priority,
        "burst_cpu_cores": quota.burst_cpu_cores,
        "burst_memory_gb": quota.burst_memory_gb
    }

@app.post("/api/v1/tenants/{tenant_id}/burst")
async def enable_tenant_burst(tenant_id: str, duration_hours: Optional[int] = None):
    """Enable burst mode for a tenant"""
    if not tenant_manager:
        raise HTTPException(status_code=503, detail="Tenant manager not initialized")
    
    tenant_manager.enable_burst_mode(tenant_id, duration_hours)
    
    return {"status": "success", "message": f"Burst mode enabled for tenant {tenant_id}"}

@app.put("/api/v1/tenants/{tenant_id}/tier")
async def update_tenant_tier(tenant_id: str, tier: str):
    """Update tenant tier"""
    if not tenant_manager:
        raise HTTPException(status_code=503, detail="Tenant manager not initialized")
    
    try:
        new_tier = TenantTier(tier.lower())
        tenant_manager.update_tenant_tier(tenant_id, new_tier)
        return {"status": "success", "message": f"Updated tenant {tenant_id} to {tier} tier"}
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid tier: {tier}")

# --- Configuration ---
PULSAR_URL = os.environ.get("PULSAR_URL", "pulsar://pulsar:6650")
SUBSCRIPTION_NAME = "provisioning-service-sub"
# Subscribe to all 'user-events' topics across all tenants
TOPIC_PATTERN = "persistent://platformq/.*/user-events"
SCHEMA_PATH = "schemas/user_created.avsc"

NEXTCLOUD_URL = os.environ.get("NEXTCLOUD_URL", "http://nextcloud-web") # K8s service name
NEXTCLOUD_ADMIN_USER = os.environ.get("NEXTCLOUD_ADMIN_USER", "nc-admin")
NEXTCLOUD_ADMIN_PASS = os.environ.get("NEXTCLOUD_ADMIN_PASS", "strongpassword")

def get_tenant_from_topic(topic: str) -> str:
    """Extracts tenant ID from a topic name."""
    match = re.search(r'platformq/(.*?)/user-events', topic)
    if match:
        return match.group(1)
    return None

# Initialize Ignite client globally
ignite_client = IgniteClient()
ignite_client.connect('ignite:10800')
resource_cache = ignite_client.get_or_create_cache('resource_states')

scaler = AdaptiveScaler(ignite_client, resource_cache)

async def init_dynamic_provisioning():
    """Initialize dynamic provisioning components"""
    global resource_monitor, scaling_engine, tenant_manager
    
    try:
        # Create Pulsar client
        pulsar_client = pulsar.Client(PULSAR_URL)
        
        # Initialize resource monitor
        prometheus_url = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
        resource_monitor = ResourceMonitor(
            prometheus_url=prometheus_url,
            ignite_client=ignite_client,
            pulsar_client=pulsar_client
        )
        await resource_monitor.start()
        logger.info("Resource monitor started")
        
        # Initialize scaling engine
        scaling_engine = ScalingEngine(
            resource_monitor=resource_monitor,
            ignite_client=ignite_client,
            pulsar_client=pulsar_client
        )
        await scaling_engine.start()
        logger.info("Scaling engine started")
        
        # Initialize tenant manager
        tenant_manager = TenantResourceManager(ignite_client)
        await tenant_manager.start()
        logger.info("Tenant resource manager started")
        
    except Exception as e:
        logger.error(f"Failed to initialize dynamic provisioning: {e}")

async def stop_dynamic_provisioning():
    """Stop dynamic provisioning components"""
    if resource_monitor:
        await resource_monitor.stop()
    if scaling_engine:
        await scaling_engine.stop()
    if tenant_manager:
        await tenant_manager.stop()

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    await init_dynamic_provisioning()

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await stop_dynamic_provisioning()

def anomaly_consumer_loop():
    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe('persistent://public/default/resource-anomalies', 'provisioning-sub', schema=AvroSchema(ResourceAnomalyEvent))
    while True:
        msg = consumer.receive()
        anomaly = msg.value()
        scaler.handle_anomaly(anomaly)
        # Trigger Airflow DAG via HTTP to workflow-service
        httpx.post('http://workflow-service:8000/api/v1/workflows/scaling_workflow/trigger', json={'anomaly': anomaly.dict()})
        consumer.acknowledge(msg)

thread = threading.Thread(target=anomaly_consumer_loop)
thread.start()

def main():
    logger.info("Starting Tenant-Aware Provisioning Service with Dynamic Resource Management...")

    # Initialize and start the background provisioner for Nextcloud
    provisioner = NextcloudProvisioner(
        admin_user=NEXTCLOUD_ADMIN_USER,
        admin_pass=NEXTCLOUD_ADMIN_PASS,
        nextcloud_url=NEXTCLOUD_URL,
    )
    provisioner_thread = provisioner.start()

    client = pulsar.Client(PULSAR_URL)
    consumer = client.subscribe(
        topic_pattern=TOPIC_PATTERN,
        subscription_name=SUBSCRIPTION_NAME,
        consumer_type=pulsar.ConsumerType.Shared,
        schema=AvroSchema(UserCreatedEvent)
    )

    def shutdown(signum, frame):
        logger.info("Shutdown signal received. Stopping consumer and provisioner...")
        provisioner.stop()
        provisioner_thread.join() # Wait for the worker to finish
        consumer.close()
        client.close()
        # Stop dynamic provisioning
        asyncio.run(stop_dynamic_provisioning())
        logger.info("Provisioning Service shut down gracefully.")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    
    try:
        while True:
            msg = consumer.receive(timeout_millis=1000)
            if msg is None:
                continue
            try:
                tenant_id = get_tenant_from_topic(msg.topic_name())
                if not tenant_id:
                    logger.warning(f"Could not extract tenant from topic: {msg.topic_name()}")
                    consumer.acknowledge(msg)
                    continue

                user_data = msg.value() # Deserialization is automatic
                logger.info(f"Received user_created event for tenant {tenant_id}: {user_data.email}")
                
                # Add provisioning task to the non-blocking queue
                provisioner.add_user_to_queue(tenant_id, user_data)
                
                consumer.acknowledge(msg)
            except Exception as e:
                consumer.negative_acknowledge(msg)
                logger.error(f"Failed to process message: {e}")

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
    finally:
        shutdown(None, None)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 