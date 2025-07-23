"""
Monitoring API endpoints
"""
from typing import Dict, Any
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException
from dependency_injector.wiring import inject, Provide

from ...core.container import Container
from ...core.monitoring_manager import MonitoringManager

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


@router.get("/deployments/{deployment_id}/drift")
@inject
async def check_drift(
    deployment_id: UUID,
    monitoring_manager: MonitoringManager = Depends(Provide[Container.monitoring_manager])
) -> Dict[str, Any]:
    """Check for model drift"""
    try:
        drift_status = await monitoring_manager.check_drift(deployment_id)
        return drift_status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deployments/{deployment_id}/performance")
@inject
async def get_performance_metrics(
    deployment_id: UUID,
    monitoring_manager: MonitoringManager = Depends(Provide[Container.monitoring_manager])
) -> Dict[str, float]:
    """Get model performance metrics"""
    try:
        metrics = await monitoring_manager.get_performance_metrics(deployment_id)
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/deployments/{deployment_id}/alerts")
@inject
async def create_alert(
    deployment_id: UUID,
    alert_type: str,
    message: str,
    monitoring_manager: MonitoringManager = Depends(Provide[Container.monitoring_manager])
) -> dict:
    """Create monitoring alert"""
    try:
        await monitoring_manager.create_alert(deployment_id, alert_type, message)
        return {"message": "Alert created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/deployments/{deployment_id}/report")
@inject
async def get_monitoring_report(
    deployment_id: UUID,
    monitoring_manager: MonitoringManager = Depends(Provide[Container.monitoring_manager])
) -> Dict[str, Any]:
    """Get comprehensive monitoring report"""
    try:
        report = await monitoring_manager.get_monitoring_report(deployment_id)
        return report
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 