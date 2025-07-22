"""
Health Check API endpoints
"""

from typing import Dict, Any, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from platformq_shared.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["health"])

# Global dependencies  
components: Optional[Dict[str, Any]] = None


def set_dependencies(health_components: Dict[str, Any]):
    """Set the global dependencies for this router"""
    global components
    components = health_components


# Response Models
class HealthStatus(BaseModel):
    """Health check response"""
    status: str = Field(..., pattern="^(healthy|degraded|unhealthy)$")
    timestamp: datetime
    uptime_seconds: float
    version: str = "1.0.0"


class ComponentHealth(BaseModel):
    """Individual component health"""
    name: str
    status: str
    message: Optional[str] = None
    response_time_ms: Optional[float] = None


class DetailedHealth(BaseModel):
    """Detailed health check response"""
    status: str
    timestamp: datetime
    uptime_seconds: float
    version: str
    components: Dict[str, ComponentHealth]
    checks_passed: int
    checks_total: int


# Health check tracking
service_start_time = datetime.utcnow()


async def check_component_health(name: str, component: Any) -> ComponentHealth:
    """Check health of a single component"""
    start = datetime.utcnow()
    try:
        if hasattr(component, 'check_health'):
            is_healthy = await component.check_health()
            status = "healthy" if is_healthy else "unhealthy"
            message = None
        else:
            # Simple connectivity check
            status = "healthy"
            message = "No health check method available"
            
        response_time = (datetime.utcnow() - start).total_seconds() * 1000
        
        return ComponentHealth(
            name=name,
            status=status,
            message=message,
            response_time_ms=response_time
        )
    except Exception as e:
        response_time = (datetime.utcnow() - start).total_seconds() * 1000
        return ComponentHealth(
            name=name,
            status="unhealthy",
            message=str(e),
            response_time_ms=response_time
        )


# API Endpoints
@router.get("/health", response_model=HealthStatus)
async def health_check():
    """
    Basic health check endpoint
    
    Returns 200 if service is operational
    Used for Kubernetes liveness probe
    """
    uptime = (datetime.utcnow() - service_start_time).total_seconds()
    
    return HealthStatus(
        status="healthy",
        timestamp=datetime.utcnow(),
        uptime_seconds=uptime,
        version="1.0.0"
    )


@router.get("/health/ready", response_model=DetailedHealth)
async def readiness_check():
    """
    Readiness check endpoint
    
    Checks all dependencies and returns detailed status
    Used for Kubernetes readiness probe
    """
    if not components:
        raise HTTPException(
            status_code=503,
            detail="Service not fully initialized"
        )
    
    uptime = (datetime.utcnow() - service_start_time).total_seconds()
    
    # Check all components
    component_checks = {}
    for name, component in components.items():
        component_checks[name] = await check_component_health(name, component)
    
    # Determine overall status
    unhealthy_count = sum(1 for c in component_checks.values() if c.status == "unhealthy")
    degraded_count = sum(1 for c in component_checks.values() if c.status == "degraded")
    
    if unhealthy_count > 0:
        overall_status = "unhealthy"
    elif degraded_count > 0:
        overall_status = "degraded"
    else:
        overall_status = "healthy"
    
    # Return 503 if unhealthy
    if overall_status == "unhealthy":
        raise HTTPException(
            status_code=503,
            detail=DetailedHealth(
                status=overall_status,
                timestamp=datetime.utcnow(),
                uptime_seconds=uptime,
                version="1.0.0",
                components=component_checks,
                checks_passed=len(component_checks) - unhealthy_count - degraded_count,
                checks_total=len(component_checks)
            ).dict()
        )
    
    return DetailedHealth(
        status=overall_status,
        timestamp=datetime.utcnow(),
        uptime_seconds=uptime,
        version="1.0.0",
        components=component_checks,
        checks_passed=len(component_checks) - unhealthy_count - degraded_count,
        checks_total=len(component_checks)
    )


@router.get("/health/live")
async def liveness_check():
    """
    Liveness check endpoint
    
    Simple check that the service is running
    Returns 200 if alive, regardless of dependency status
    """
    return {
        "status": "alive",
        "timestamp": datetime.utcnow().isoformat()
    } 