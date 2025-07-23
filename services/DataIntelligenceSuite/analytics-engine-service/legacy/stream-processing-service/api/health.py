"""Health API router

Handles health checks, readiness probes, and service status.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime
import psutil
import platform

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel


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


class SystemInfo(BaseModel):
    """System information model"""
    hostname: str
    platform: str
    cpu_count: int
    memory_total_gb: float
    memory_used_gb: float
    memory_percent: float
    disk_usage_percent: float


@router.get("/health", response_model=HealthStatus)
async def health_check() -> HealthStatus:
    """Health check endpoint"""
    from app.main import job_manager, pattern_library, state_manager
    from app.core.config import settings
    
    # Perform health checks
    checks = {}
    overall_status = "healthy"
    
    # Check job manager
    try:
        if job_manager:
            job_count = len(job_manager.jobs)
            checks["job_manager"] = {
                "status": "healthy",
                "job_count": job_count,
                "connected": True
            }
        else:
            checks["job_manager"] = {"status": "unhealthy", "error": "Not initialized"}
            overall_status = "unhealthy"
    except Exception as e:
        checks["job_manager"] = {"status": "unhealthy", "error": str(e)}
        overall_status = "unhealthy"
    
    # Check pattern library
    try:
        if pattern_library:
            pattern_count = len(pattern_library.patterns)
            checks["pattern_library"] = {
                "status": "healthy",
                "pattern_count": pattern_count,
                "loaded": True
            }
        else:
            checks["pattern_library"] = {"status": "unhealthy", "error": "Not initialized"}
            overall_status = "unhealthy"
    except Exception as e:
        checks["pattern_library"] = {"status": "unhealthy", "error": str(e)}
        overall_status = "unhealthy"
    
    # Check state manager (Ignite)
    try:
        if state_manager and state_manager.connected:
            checks["state_manager"] = {
                "status": "healthy",
                "connected": True,
                "cache": settings.ignite_cache_name,
                "metrics": state_manager.get_metrics()
            }
        else:
            checks["state_manager"] = {"status": "unhealthy", "error": "Not connected"}
            overall_status = "degraded"
    except Exception as e:
        checks["state_manager"] = {"status": "unhealthy", "error": str(e)}
        overall_status = "degraded"
    
    # Check Flink connection
    try:
        # In production, would actually check Flink cluster
        checks["flink"] = {
            "status": "healthy",
            "master": settings.flink_master,
            "parallelism": settings.flink_parallelism
        }
    except Exception as e:
        checks["flink"] = {"status": "unhealthy", "error": str(e)}
        overall_status = "unhealthy"
    
    # Check Pulsar connection
    try:
        # In production, would actually check Pulsar
        checks["pulsar"] = {
            "status": "healthy",
            "url": settings.pulsar_url,
            "namespace": settings.pulsar_namespace
        }
    except Exception as e:
        checks["pulsar"] = {"status": "unhealthy", "error": str(e)}
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
    from app.main import job_manager, pattern_library, state_manager
    
    checks = {}
    all_ready = True
    
    # Check if core components are initialized
    checks["job_manager"] = job_manager is not None
    checks["pattern_library"] = pattern_library is not None
    checks["state_manager"] = state_manager is not None and state_manager.connected
    
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
    from app.core.config import settings
    
    return {
        "status": "started",
        "timestamp": datetime.utcnow().isoformat(),
        "service": settings.service_name,
        "version": settings.service_version,
        "api_port": settings.api_port
    }


@router.get("/system", response_model=SystemInfo)
async def system_info() -> SystemInfo:
    """Get system information"""
    try:
        # Get system metrics
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        return SystemInfo(
            hostname=platform.node(),
            platform=platform.platform(),
            cpu_count=psutil.cpu_count(),
            memory_total_gb=round(memory.total / (1024**3), 2),
            memory_used_gb=round(memory.used / (1024**3), 2),
            memory_percent=memory.percent,
            disk_usage_percent=disk.percent
        )
    except Exception as e:
        logger.error(f"Failed to get system info: {e}")
        raise HTTPException(500, f"Failed to get system info: {str(e)}")


@router.get("/dependencies")
async def check_dependencies() -> Dict[str, Any]:
    """Check external dependencies"""
    from app.core.config import settings
    
    dependencies = {
        "flink": {
            "url": f"http://{settings.flink_master}",
            "status": "unknown"
        },
        "pulsar": {
            "url": settings.pulsar_url,
            "admin_url": settings.pulsar_admin_url,
            "status": "unknown"
        },
        "cassandra": {
            "hosts": settings.cassandra_hosts,
            "keyspace": settings.cassandra_keyspace,
            "status": "unknown"
        },
        "ignite": {
            "host": f"{settings.ignite_host}:{settings.ignite_port}",
            "cache": settings.ignite_cache_name,
            "status": "unknown"
        },
        "elasticsearch": {
            "hosts": settings.elasticsearch_hosts,
            "status": "unknown"
        },
        "minio": {
            "endpoint": settings.minio_endpoint,
            "bucket_prefix": settings.minio_bucket_prefix,
            "status": "unknown"
        }
    }
    
    # In production, would actually check each dependency
    # For now, returning the configuration
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "dependencies": dependencies
    } 