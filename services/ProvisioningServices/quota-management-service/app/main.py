"""Quota Management Service

Manages resource quotas for tenants.
"""

from contextlib import asynccontextmanager
import logging
import asyncio
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest, Counter, Histogram, Gauge
import consul.aio
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from platformq_resource_common import (
    ResourceQuota,
    ResourceUsage,
    ResourceType,
    QuotaStatus
)

from .config import settings
from .api import router
from .quota_manager import QuotaManager
from .repository import QuotaRepository

# Configure logging
logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

# Prometheus metrics
quota_check_counter = Counter(
    'quota_checks_total',
    'Total number of quota checks',
    ['tenant_id', 'resource_type', 'action']
)
quota_exceeded_counter = Counter(
    'quota_exceeded_total',
    'Total number of quota exceeded events',
    ['tenant_id', 'resource_type']
)
quota_alert_counter = Counter(
    'quota_alerts_total',
    'Total number of quota alerts generated',
    ['tenant_id', 'resource_type', 'threshold']
)
resource_usage_gauge = Gauge(
    'resource_usage_current',
    'Current resource usage',
    ['tenant_id', 'resource_type']
)
quota_utilization_gauge = Gauge(
    'quota_utilization_percentage',
    'Quota utilization percentage',
    ['tenant_id', 'resource_type']
)

# Global instances
repository = None
quota_manager = None
consul_client = None
scheduler = None
event_processor_task = None


async def register_with_consul():
    """Register service with Consul"""
    global consul_client
    
    try:
        consul_client = consul.aio.Consul(
            host=settings.consul_host,
            port=settings.consul_port
        )
        
        # Register service
        await consul_client.agent.service.register(
            name=settings.consul_service_name,
            service_id=settings.consul_service_id,
            address=settings.service_name,
            port=settings.service_port,
            tags=[
                "quota-management",
                "resource-control",
                "tenant-management"
            ],
            check=consul.Check.http(
                f"http://{settings.service_name}:{settings.service_port}/health",
                interval=settings.consul_health_check_interval,
                deregister=settings.consul_deregister_critical_after
            )
        )
        
        logger.info(f"Registered with Consul as {settings.consul_service_id}")
        
    except Exception as e:
        logger.error(f"Failed to register with Consul: {e}")


async def deregister_from_consul():
    """Deregister service from Consul"""
    if consul_client:
        try:
            await consul_client.agent.service.deregister(
                service_id=settings.consul_service_id
            )
            logger.info("Deregistered from Consul")
        except Exception as e:
            logger.error(f"Failed to deregister from Consul: {e}")
        finally:
            await consul_client.close()


async def check_all_quotas():
    """Scheduled task to check quotas for all tenants"""
    logger.info("Running scheduled quota check")
    
    try:
        # In production, this would get list of all active tenants
        # For now, using mock tenant IDs
        tenant_ids = ["tenant-001", "tenant-002", "tenant-003"]
        
        for tenant_id in tenant_ids:
            try:
                # Get all quotas for tenant
                quotas = await repository.get_all_quotas(tenant_id)
                
                for quota in quotas:
                    # Get current usage
                    usage = await quota_manager.get_current_usage(
                        tenant_id,
                        quota.resource_type
                    )
                    
                    # Update metrics
                    resource_usage_gauge.labels(
                        tenant_id=tenant_id,
                        resource_type=quota.resource_type.value
                    ).set(usage)
                    
                    # Calculate utilization
                    utilization = (usage / quota.limit * 100) if quota.limit > 0 else 0
                    quota_utilization_gauge.labels(
                        tenant_id=tenant_id,
                        resource_type=quota.resource_type.value
                    ).set(utilization)
                    
                    # Check if quota status needs update
                    old_status = quota.status
                    if utilization >= 100:
                        new_status = QuotaStatus.EXCEEDED
                    elif utilization >= settings.quota_soft_limit_threshold * 100:
                        new_status = QuotaStatus.WARNING
                    else:
                        new_status = QuotaStatus.OK
                        
                    if new_status != old_status:
                        await repository.update_quota_status(
                            tenant_id,
                            quota.resource_type,
                            new_status
                        )
                        
                        # Update exceeded counter
                        if new_status == QuotaStatus.EXCEEDED:
                            quota_exceeded_counter.labels(
                                tenant_id=tenant_id,
                                resource_type=quota.resource_type.value
                            ).inc()
                            
            except Exception as e:
                logger.error(f"Error checking quotas for tenant {tenant_id}: {e}")
                
    except Exception as e:
        logger.error(f"Error in scheduled quota check: {e}")


async def cleanup_old_data():
    """Scheduled task to clean up old usage history"""
    logger.info("Running scheduled cleanup")
    
    try:
        await quota_manager.cleanup_old_usage_data()
        logger.info("Cleanup completed successfully")
        
    except Exception as e:
        logger.error(f"Error in scheduled cleanup: {e}")


async def start_event_processor():
    """Start the resource event processor"""
    logger.info("Starting resource event processor")
    
    try:
        await quota_manager.process_resource_events()
    except asyncio.CancelledError:
        logger.info("Event processor cancelled")
        raise
    except Exception as e:
        logger.error(f"Error in event processor: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global repository, quota_manager, scheduler, event_processor_task
    
    logger.info("Quota Management Service starting up...")
    
    # Initialize components
    repository = QuotaRepository()
    quota_manager = QuotaManager(repository)
    
    # Register with Consul
    await register_with_consul()
    
    # Initialize scheduler
    scheduler = AsyncIOScheduler()
    
    # Schedule periodic tasks
    scheduler.add_job(
        check_all_quotas,
        'interval',
        seconds=settings.quota_check_interval_seconds,
        id='quota_check',
        max_instances=1
    )
    
    scheduler.add_job(
        cleanup_old_data,
        'cron',
        hour=2,  # Run at 2 AM daily
        id='cleanup',
        max_instances=1
    )
    
    scheduler.start()
    logger.info("Scheduler started")
    
    # Start event processor
    event_processor_task = asyncio.create_task(start_event_processor())
    logger.info("Event processor started")
    
    logger.info("Quota Management Service started")
    
    yield
    
    # Shutdown
    logger.info("Quota Management Service shutting down...")
    
    # Cancel event processor
    if event_processor_task:
        event_processor_task.cancel()
        try:
            await event_processor_task
        except asyncio.CancelledError:
            pass
    
    # Stop scheduler
    if scheduler:
        scheduler.shutdown()
    
    # Close connections
    if quota_manager:
        await quota_manager.close()
    if repository:
        await repository.close()
    
    # Deregister from Consul
    await deregister_from_consul()
    
    logger.info("Quota Management Service stopped")


# Create FastAPI app
app = FastAPI(
    title="Quota Management Service",
    description="Manages resource quotas for tenants",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(router)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": settings.service_name,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    return generate_latest()


@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint"""
    # Check if all components are initialized
    if not all([repository, quota_manager]):
        raise HTTPException(status_code=503, detail="Service not ready")
        
    # Check database connections
    try:
        # Simple query to verify connections
        await repository.get_all_quotas("test")
    except Exception as e:
        logger.error(f"Readiness check failed: {e}")
        raise HTTPException(status_code=503, detail="Database not ready")
        
    return {
        "status": "ready",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.service_port,
        reload=False
    ) 