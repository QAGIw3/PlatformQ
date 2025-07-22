"""Health and monitoring API endpoints."""

import logging
from typing import Dict, Any
from datetime import datetime
import psutil
import os

from fastapi import APIRouter, Depends
from prometheus_client import CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

from ..dependencies import get_state_manager

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Health & Monitoring"])


@router.get("/health")
async def health_check(
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Health check endpoint."""
    try:
        # Check core dependencies
        ignite_status = await state_manager.check_ignite_health()
        pulsar_status = await state_manager.check_pulsar_health()
        
        # Get service metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        
        # Determine overall health
        all_healthy = ignite_status and pulsar_status
        
        return {
            "status": "healthy" if all_healthy else "degraded",
            "timestamp": datetime.utcnow().isoformat(),
            "service": "risk-engine-service",
            "version": "1.0.0",
            "dependencies": {
                "ignite": "healthy" if ignite_status else "unhealthy",
                "pulsar": "healthy" if pulsar_status else "unhealthy"
            },
            "metrics": {
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "memory_available_mb": memory.available / 1024 / 1024
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "service": "risk-engine-service",
            "error": str(e)
        }


@router.get("/ready")
async def readiness_check(
    state_manager = Depends(get_state_manager)
) -> Dict[str, str]:
    """Readiness check endpoint."""
    try:
        # Check if service is ready to accept requests
        ready = await state_manager.is_ready()
        
        if ready:
            return {"status": "ready"}
        else:
            return {"status": "not_ready"}
    except Exception:
        return {"status": "not_ready"}


@router.get("/metrics", response_class=Response)
async def get_metrics() -> Response:
    """Prometheus metrics endpoint."""
    registry = CollectorRegistry()
    
    # Collect metrics
    # Note: In production, you'd register your custom metrics
    data = generate_latest(registry)
    
    return Response(content=data, media_type=CONTENT_TYPE_LATEST)


@router.get("/stats")
async def get_service_stats(
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Get service statistics."""
    stats = await state_manager.get_service_stats()
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "uptime_seconds": (datetime.utcnow() - stats.get("start_time", datetime.utcnow())).total_seconds(),
        "calculations": {
            "total_risk_calculations": stats.get("risk_calculations", 0),
            "total_var_calculations": stats.get("var_calculations", 0),
            "total_stress_tests": stats.get("stress_tests", 0),
            "total_margin_calls": stats.get("margin_calls", 0),
            "total_liquidations": stats.get("liquidations", 0)
        },
        "performance": {
            "avg_risk_calc_time_ms": stats.get("avg_risk_calc_time", 0),
            "avg_var_calc_time_ms": stats.get("avg_var_calc_time", 0),
            "avg_stress_test_time_ms": stats.get("avg_stress_test_time", 0)
        },
        "cache": {
            "hit_rate": stats.get("cache_hit_rate", 0),
            "size_mb": stats.get("cache_size_mb", 0)
        }
    }


@router.get("/config")
async def get_config() -> Dict[str, Any]:
    """Get non-sensitive configuration."""
    return {
        "service_name": "risk-engine-service",
        "version": "1.0.0",
        "environment": os.getenv("ENVIRONMENT", "development"),
        "features": {
            "real_time_risk": True,
            "portfolio_var": True,
            "stress_testing": True,
            "ml_models": True,
            "automated_liquidation": True
        },
        "limits": {
            "max_portfolio_size": 1000,
            "max_stress_scenarios": 50,
            "cache_ttl_seconds": 60
        }
    } 