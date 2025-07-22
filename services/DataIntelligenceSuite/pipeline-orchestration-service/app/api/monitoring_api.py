"""
Pipeline Monitoring API endpoints

Provides API for pipeline monitoring and metrics.
"""

from typing import Dict, List, Optional, Any
from fastapi import APIRouter, HTTPException, Query, Path

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter(prefix="/api/v1/monitoring", tags=["monitoring"])


# API Endpoints
@router.get("/metrics/{pipeline_id}")
async def get_pipeline_metrics(pipeline_id: str = Path(..., description="Pipeline ID")):
    """Get metrics for a specific pipeline"""
    try:
        logger.info("get_pipeline_metrics_requested", pipeline_id=pipeline_id)
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get metrics
        metrics = await service.monitor.get_pipeline_metrics(pipeline_id)
        if not metrics:
            raise HTTPException(status_code=404, detail="Pipeline not found")
        
        return metrics
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("get_pipeline_metrics_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics")
async def get_all_metrics():
    """Get metrics for all pipelines"""
    try:
        logger.info("get_all_metrics_requested")
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get all metrics
        metrics = await service.monitor.get_all_metrics()
        
        return metrics
        
    except Exception as e:
        logger.error("get_all_metrics_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
async def get_alerts(
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID"),
    acknowledged: Optional[bool] = Query(None, description="Filter by acknowledged status"),
    limit: int = Query(100, description="Maximum results")
):
    """Get pipeline alerts"""
    try:
        logger.info("get_alerts_requested")
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get alerts
        alerts = await service.monitor.get_alerts(
            pipeline_id=pipeline_id,
            acknowledged=acknowledged,
            limit=limit
        )
        
        return {"alerts": alerts}
        
    except Exception as e:
        logger.error("get_alerts_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(alert_id: str = Path(..., description="Alert ID")):
    """Acknowledge an alert"""
    try:
        logger.info("acknowledge_alert_requested", alert_id=alert_id)
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Acknowledge alert
        success = await service.monitor.acknowledge_alert(alert_id)
        if not success:
            raise HTTPException(status_code=404, detail="Alert not found")
        
        return {"message": "Alert acknowledged successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("acknowledge_alert_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/schedules")
async def get_scheduled_tasks(
    pipeline_id: Optional[str] = Query(None, description="Filter by pipeline ID"),
    enabled_only: bool = Query(True, description="Show only enabled schedules")
):
    """Get scheduled pipeline tasks"""
    try:
        logger.info("get_scheduled_tasks_requested")
        
        # Get service instance
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get scheduled tasks
        tasks = await service.scheduler.get_scheduled_tasks(
            pipeline_id=pipeline_id,
            enabled_only=enabled_only
        )
        
        return {"scheduled_tasks": tasks}
        
    except Exception as e:
        logger.error("get_scheduled_tasks_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) 