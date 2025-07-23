"""Health API router

Handles health checks, readiness probes, and service status.
"""

import logging
from typing import Dict, Any
from datetime import datetime
import psutil

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.core.config import settings


logger = logging.getLogger(__name__)
router = APIRouter()


class HealthStatus(BaseModel):
    """Health status response model"""
    status: str
    timestamp: str
    service: str
    version: str
    environment: str
    checks: Dict[str, Dict[str, Any]]


class ReadinessStatus(BaseModel):
    """Readiness status response model"""
    ready: bool
    timestamp: str
    checks: Dict[str, bool]
    message: str


@router.get("/health", response_model=HealthStatus)
async def health_check() -> HealthStatus:
    """Health check endpoint"""
    from app.main import spark_manager, job_scheduler, resource_manager
    
    # Perform health checks
    checks = {}
    overall_status = "healthy"
    
    # Check Spark manager
    try:
        if spark_manager and spark_manager.initialized:
            spark_ui_url = spark_manager.get_spark_ui_url()
            checks["spark_manager"] = {
                "status": "healthy",
                "initialized": True,
                "spark_ui": spark_ui_url
            }
        else:
            checks["spark_manager"] = {"status": "unhealthy", "error": "Not initialized"}
            overall_status = "unhealthy"
    except Exception as e:
        checks["spark_manager"] = {"status": "unhealthy", "error": str(e)}
        overall_status = "unhealthy"
    
    # Check job scheduler
    try:
        if job_scheduler:
            job_count = len(job_scheduler.jobs)
            running_count = len(job_scheduler._running_jobs)
            checks["job_scheduler"] = {
                "status": "healthy",
                "total_jobs": job_count,
                "running_jobs": running_count,
                "scheduled_jobs": len(job_scheduler.scheduled_jobs)
            }
        else:
            checks["job_scheduler"] = {"status": "unhealthy", "error": "Not initialized"}
            overall_status = "unhealthy"
    except Exception as e:
        checks["job_scheduler"] = {"status": "unhealthy", "error": str(e)}
        overall_status = "unhealthy"
    
    # Check resource manager
    try:
        if resource_manager:
            cluster_status = await resource_manager.get_cluster_status()
            checks["resource_manager"] = {
                "status": "healthy",
                "cluster_health": cluster_status["cluster_health"],
                "allocated_jobs": cluster_status["allocated_jobs"]
            }
        else:
            checks["resource_manager"] = {"status": "unhealthy", "error": "Not initialized"}
            overall_status = "degraded"
    except Exception as e:
        checks["resource_manager"] = {"status": "unhealthy", "error": str(e)}
        overall_status = "degraded"
    
    return HealthStatus(
        status=overall_status,
        timestamp=datetime.utcnow().isoformat(),
        service=settings.service_name,
        version=settings.service_version,
        environment=settings.environment,
        checks=checks
    )


@router.get("/ready", response_model=ReadinessStatus)
async def readiness_check() -> ReadinessStatus:
    """Readiness probe endpoint"""
    from app.main import spark_manager, job_scheduler, resource_manager
    
    checks = {}
    
    # Check if core components are ready
    checks["spark_manager"] = spark_manager is not None and spark_manager.initialized
    checks["job_scheduler"] = job_scheduler is not None
    checks["resource_manager"] = resource_manager is not None
    
    # All must be true for service to be ready
    all_ready = all(checks.values())
    
    message = "Service is ready" if all_ready else "Service is not ready"
    if not all_ready:
        failed = [k for k, v in checks.items() if not v]
        message += f". Failed checks: {', '.join(failed)}"
    
    return ReadinessStatus(
        ready=all_ready,
        timestamp=datetime.utcnow().isoformat(),
        checks=checks,
        message=message
    )


@router.get("/liveness")
async def liveness_check() -> Dict[str, str]:
    """Liveness probe endpoint"""
    return {
        "status": "alive",
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/startup")
async def startup_check() -> Dict[str, Any]:
    """Startup probe endpoint"""
    return {
        "status": "started",
        "timestamp": datetime.utcnow().isoformat(),
        "service": settings.service_name,
        "version": settings.service_version,
        "api_port": settings.api_port,
        "spark_master": settings.spark_master
    } 