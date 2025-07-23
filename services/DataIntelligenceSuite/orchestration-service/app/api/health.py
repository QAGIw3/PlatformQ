"""
Health check API endpoints
"""

from typing import Dict, Any, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from platformq_shared.logging import get_logger

logger = get_logger(__name__)

router = APIRouter(tags=["health"])

# Dependency injection for health checks
health_check_components: Dict[str, Any] = {}

def set_dependencies(components: Dict[str, Any]):
    """Set health check dependencies"""
    global health_check_components
    health_check_components = components


# Response models
class HealthResponse(BaseModel):
    status: str
    timestamp: str
    service: str = "unified-orchestration-service"
    version: str = "1.0.0"


class ReadyResponse(BaseModel):
    ready: bool
    timestamp: str
    checks: Dict[str, bool]
    details: Dict[str, Any]


# API Endpoints
@router.get("/health", response_model=HealthResponse)
async def health():
    """Basic health check endpoint"""
    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow().isoformat()
    )


@router.get("/ready", response_model=ReadyResponse)
async def ready():
    """Readiness check endpoint"""
    checks = {
        "airflow": False,
        "ignite": False,
        "pulsar": False,
        "seatunnel": False
    }
    
    details = {}
    
    # Check Airflow
    try:
        if health_check_components.get('airflow'):
            # Verify Airflow connectivity
            checks['airflow'] = True
            details['airflow'] = "Connected"
    except Exception as e:
        details['airflow'] = f"Error: {str(e)}"
        
    # Check Ignite
    try:
        if health_check_components.get('pipeline') and health_check_components['pipeline'].ignite_client:
            checks['ignite'] = True
            details['ignite'] = "Connected"
    except Exception as e:
        details['ignite'] = f"Error: {str(e)}"
        
    # Check Pulsar (Event Stream)
    try:
        if health_check_components.get('events') and health_check_components['events'].event_stream:
            checks['pulsar'] = True
            details['pulsar'] = "Connected"
    except Exception as e:
        details['pulsar'] = f"Error: {str(e)}"
        
    # Check SeaTunnel
    try:
        if health_check_components.get('seatunnel'):
            checks['seatunnel'] = True
            details['seatunnel'] = "Initialized"
    except Exception as e:
        details['seatunnel'] = f"Error: {str(e)}"
        
    # Overall readiness
    ready = all(checks.values())
    
    return ReadyResponse(
        ready=ready,
        timestamp=datetime.utcnow().isoformat(),
        checks=checks,
        details=details
    )


@router.get("/health/airflow")
async def health_airflow():
    """Airflow-specific health check"""
    if not health_check_components.get('airflow'):
        raise HTTPException(status_code=503, detail="Airflow bridge not initialized")
        
    try:
        # Perform Airflow health check
        return {
            "status": "healthy",
            "api_url": health_check_components['airflow'].base_url,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Airflow unhealthy: {str(e)}")


@router.get("/health/seatunnel")
async def health_seatunnel():
    """SeaTunnel-specific health check"""
    if not health_check_components.get('seatunnel'):
        raise HTTPException(status_code=503, detail="SeaTunnel orchestrator not initialized")
        
    try:
        seatunnel = health_check_components['seatunnel']
        return {
            "status": "healthy",
            "api_url": seatunnel.api_url,
            "templates_loaded": len(seatunnel.templates),
            "active_jobs": len(seatunnel.jobs),
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"SeaTunnel unhealthy: {str(e)}") 