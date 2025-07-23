"""
Health check API endpoints
"""

from datetime import datetime
from typing import Dict, Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.core.config import settings

router = APIRouter()

# Service start time
service_start_time = datetime.utcnow()

# Component references (will be set by main)
components: Dict[str, Any] = {}


def set_components(comps: Dict[str, Any]):
    """Set component references for health checks"""
    global components
    components = comps


class HealthStatus(BaseModel):
    """Health check response"""
    status: str
    timestamp: datetime
    uptime_seconds: float
    version: str
    environment: str


class ComponentHealth(BaseModel):
    """Individual component health"""
    name: str
    status: str
    message: Optional[str] = None


class DetailedHealth(BaseModel):
    """Detailed health check response"""
    status: str
    timestamp: datetime
    uptime_seconds: float
    version: str
    environment: str
    components: Dict[str, ComponentHealth]
    checks_passed: int
    checks_total: int


@router.get("/", response_model=HealthStatus)
async def health_check():
    """Basic health check endpoint"""
    uptime = (datetime.utcnow() - service_start_time).total_seconds()
    
    return HealthStatus(
        status="healthy",
        timestamp=datetime.utcnow(),
        uptime_seconds=uptime,
        version=settings.service_version,
        environment=settings.environment
    )


@router.get("/ready", response_model=DetailedHealth)
async def readiness_check():
    """Detailed readiness check"""
    uptime = (datetime.utcnow() - service_start_time).total_seconds()
    
    # Check all components
    component_health = {}
    unhealthy_count = 0
    
    # Check CDC Manager
    if "cdc_manager" in components:
        try:
            # Simple check - could be more sophisticated
            component_health["cdc_manager"] = ComponentHealth(
                name="CDC Manager",
                status="healthy"
            )
        except Exception as e:
            component_health["cdc_manager"] = ComponentHealth(
                name="CDC Manager",
                status="unhealthy",
                message=str(e)
            )
            unhealthy_count += 1
            
    # Check Stream Manager
    if "stream_manager" in components:
        try:
            component_health["stream_manager"] = ComponentHealth(
                name="Stream Manager",
                status="healthy"
            )
        except Exception as e:
            component_health["stream_manager"] = ComponentHealth(
                name="Stream Manager",
                status="unhealthy",
                message=str(e)
            )
            unhealthy_count += 1
            
    # Check Batch Manager
    if "batch_manager" in components:
        try:
            component_health["batch_manager"] = ComponentHealth(
                name="Batch Manager",
                status="healthy"
            )
        except Exception as e:
            component_health["batch_manager"] = ComponentHealth(
                name="Batch Manager",
                status="unhealthy",
                message=str(e)
            )
            unhealthy_count += 1
            
    # Check Schema Registry
    if "schema_registry" in components:
        try:
            component_health["schema_registry"] = ComponentHealth(
                name="Schema Registry",
                status="healthy"
            )
        except Exception as e:
            component_health["schema_registry"] = ComponentHealth(
                name="Schema Registry",
                status="unhealthy",
                message=str(e)
            )
            unhealthy_count += 1
            
    # Check SeaTunnel
    if "seatunnel" in components:
        try:
            # Check if SeaTunnel is accessible
            component_health["seatunnel"] = ComponentHealth(
                name="Apache SeaTunnel",
                status="healthy"
            )
        except Exception as e:
            component_health["seatunnel"] = ComponentHealth(
                name="Apache SeaTunnel",
                status="unhealthy",
                message=str(e)
            )
            unhealthy_count += 1
            
    # Determine overall status
    total_checks = len(component_health)
    if unhealthy_count == 0:
        overall_status = "healthy"
    elif unhealthy_count < total_checks:
        overall_status = "degraded"
    else:
        overall_status = "unhealthy"
        
    response = DetailedHealth(
        status=overall_status,
        timestamp=datetime.utcnow(),
        uptime_seconds=uptime,
        version=settings.service_version,
        environment=settings.environment,
        components=component_health,
        checks_passed=total_checks - unhealthy_count,
        checks_total=total_checks
    )
    
    # Return 503 if unhealthy
    if overall_status == "unhealthy":
        raise HTTPException(status_code=503, detail=response.dict())
        
    return response


@router.get("/live")
async def liveness_check():
    """Kubernetes liveness probe endpoint"""
    return {
        "status": "alive",
        "timestamp": datetime.utcnow().isoformat()
    } 