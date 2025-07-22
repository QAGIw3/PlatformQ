"""
Monitoring API endpoints

Provides API for data quality monitoring and alerting.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Query, Path
from pydantic import BaseModel, Field
import json

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter(prefix="/api/v1/monitoring", tags=["monitoring"])


# Request/Response Models
class AlertConfigRequest(BaseModel):
    """Alert configuration request"""
    dataset: str = Field(..., description="Dataset to monitor")
    metric: str = Field(..., description="Metric to monitor")
    threshold_warning: float = Field(..., description="Warning threshold")
    threshold_error: float = Field(..., description="Error threshold")
    threshold_critical: float = Field(..., description="Critical threshold")
    enabled: bool = Field(default=True, description="Enable alerting")


class MonitoringConfigRequest(BaseModel):
    """Monitoring configuration request"""
    datasets: List[str] = Field(..., description="Datasets to monitor")
    check_interval_seconds: int = Field(default=300, description="Check interval")
    alert_cooldown_minutes: int = Field(default=30, description="Alert cooldown period")
    trend_window_hours: int = Field(default=24, description="Trend analysis window")


# API Endpoints
@router.get("/metrics/{dataset}")
async def get_dataset_metrics(
    dataset: str = Path(..., description="Dataset identifier"),
    hours: int = Query(24, description="Hours of history to retrieve")
):
    """
    Get quality metrics for a dataset
    """
    try:
        logger.info("get_metrics_requested", dataset=dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get metrics
        metrics = await service.quality_monitor.get_dataset_metrics(dataset)
        
        return metrics
        
    except Exception as e:
        logger.error("get_metrics_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trends")
async def get_quality_trends(
    dataset: Optional[str] = Query(None, description="Filter by dataset"),
    hours: int = Query(24, description="Hours of trend data"),
    metric: Optional[str] = Query(None, description="Specific metric")
):
    """
    Get quality trends
    """
    try:
        logger.info("get_trends_requested")
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get trends
        trends = await service.quality_monitor.get_quality_trends(
            dataset=dataset,
            hours=hours
        )
        
        # Filter by metric if specified
        if metric:
            trends = [t for t in trends if t.metric.value == metric]
        
        return {
            "period_hours": hours,
            "trends": [
                {
                    "dataset": t.dataset,
                    "metric": t.metric.value,
                    "trend_direction": t.trend_direction,
                    "change_rate": t.change_rate,
                    "data_points": len(t.values),
                    "latest_value": t.values[-1] if t.values else None,
                    "timestamps": [ts.isoformat() for ts in t.timestamps]
                }
                for t in trends
            ]
        }
        
    except Exception as e:
        logger.error("get_trends_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
async def get_active_alerts(
    dataset: Optional[str] = Query(None, description="Filter by dataset"),
    severity: Optional[str] = Query(None, description="Filter by severity"),
    metric: Optional[str] = Query(None, description="Filter by metric")
):
    """
    Get active quality alerts
    """
    try:
        logger.info("get_alerts_requested")
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get active alerts
        alerts = list(service.quality_monitor.active_alerts.values())
        
        # Apply filters
        if dataset:
            alerts = [a for a in alerts if a.dataset == dataset]
        if severity:
            alerts = [a for a in alerts if a.severity.value == severity]
        if metric:
            alerts = [a for a in alerts if a.metric.value == metric]
        
        return {
            "total_alerts": len(alerts),
            "alerts": [
                {
                    "id": a.id,
                    "dataset": a.dataset,
                    "metric": a.metric.value,
                    "severity": a.severity.value,
                    "current_value": a.current_value,
                    "threshold": a.threshold,
                    "message": a.message,
                    "timestamp": a.timestamp.isoformat(),
                    "metadata": a.metadata
                }
                for a in alerts
            ]
        }
        
    except Exception as e:
        logger.error("get_alerts_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts/history")
async def get_alert_history(
    dataset: Optional[str] = Query(None, description="Filter by dataset"),
    days: int = Query(7, description="Days of history"),
    limit: int = Query(100, description="Maximum results")
):
    """
    Get alert history
    """
    try:
        logger.info("get_alert_history_requested")
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Get alert history
        cutoff = datetime.utcnow() - timedelta(days=days)
        history = [
            a for a in service.quality_monitor.alert_history
            if a.timestamp >= cutoff
        ]
        
        # Apply filters
        if dataset:
            history = [a for a in history if a.dataset == dataset]
        
        # Sort by timestamp descending
        history.sort(key=lambda a: a.timestamp, reverse=True)
        
        # Limit results
        history = history[:limit]
        
        return {
            "period_days": days,
            "total_alerts": len(history),
            "alerts": [
                {
                    "id": a.id,
                    "dataset": a.dataset,
                    "metric": a.metric.value,
                    "severity": a.severity.value,
                    "current_value": a.current_value,
                    "threshold": a.threshold,
                    "message": a.message,
                    "timestamp": a.timestamp.isoformat(),
                    "metadata": a.metadata
                }
                for a in history
            ]
        }
        
    except Exception as e:
        logger.error("get_alert_history_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/config/alerts")
async def configure_alerts(request: AlertConfigRequest):
    """
    Configure quality alerts
    """
    try:
        logger.info("configure_alerts_requested", dataset=request.dataset)
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Update alert configuration
        # This would update the monitoring configuration in Consul
        config_key = f"data-quality/alerts/{request.dataset}/{request.metric}"
        config_data = {
            "thresholds": {
                "warning": request.threshold_warning,
                "error": request.threshold_error,
                "critical": request.threshold_critical
            },
            "enabled": request.enabled
        }
        
        await service.vault_consul.consul.kv.put(config_key, json.dumps(config_data))
        
        return {"message": "Alert configuration updated"}
        
    except Exception as e:
        logger.error("configure_alerts_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/config/monitoring")
async def configure_monitoring(request: MonitoringConfigRequest):
    """
    Configure monitoring settings
    """
    try:
        logger.info("configure_monitoring_requested")
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Update monitoring configuration
        config_data = {
            "datasets": request.datasets,
            "check_interval_seconds": request.check_interval_seconds,
            "alert_cooldown_minutes": request.alert_cooldown_minutes,
            "trend_window_hours": request.trend_window_hours
        }
        
        await service.vault_consul.consul.kv.put(
            "data-quality/monitoring-config",
            json.dumps(config_data)
        )
        
        # Update monitored datasets
        await service.vault_consul.consul.kv.put(
            "data-quality/monitored-datasets",
            json.dumps(request.datasets)
        )
        
        return {"message": "Monitoring configuration updated"}
        
    except Exception as e:
        logger.error("configure_monitoring_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health/components")
async def get_component_health():
    """
    Get health status of monitoring components
    """
    try:
        logger.info("get_component_health_requested")
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Check component health
        health = {
            "monitor": {
                "status": "healthy" if service.quality_monitor.is_running else "stopped",
                "active_alerts": len(service.quality_monitor.active_alerts),
                "monitored_datasets": len(service.quality_monitor.metric_history)
            },
            "rule_engine": {
                "status": "healthy",
                "total_rules": len(service.rule_engine.rules),
                "enabled_rules": sum(1 for r in service.rule_engine.rules.values() if r.enabled)
            },
            "quality_engine": {
                "status": "healthy" if hasattr(service.quality_engine, 'is_running') and service.quality_engine.is_running else "unknown"
            }
        }
        
        return health
        
    except Exception as e:
        logger.error("get_component_health_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_monitoring_statistics():
    """
    Get monitoring statistics
    """
    try:
        logger.info("get_stats_requested")
        
        # Get service instance
        from fastapi import Request
        from starlette.requests import Request as StarletteRequest
        service = StarletteRequest.app.state.service
        
        # Calculate statistics
        now = datetime.utcnow()
        last_24h = now - timedelta(hours=24)
        last_7d = now - timedelta(days=7)
        
        stats = {
            "monitoring": {
                "datasets_monitored": len(service.quality_monitor.metric_history),
                "total_checks_24h": sum(
                    len([e for e in history if e["timestamp"] >= last_24h])
                    for history in service.quality_monitor.metric_history.values()
                ),
                "active_alerts": len(service.quality_monitor.active_alerts)
            },
            "alerts": {
                "total_24h": len([
                    a for a in service.quality_monitor.alert_history
                    if a.timestamp >= last_24h
                ]),
                "total_7d": len([
                    a for a in service.quality_monitor.alert_history
                    if a.timestamp >= last_7d
                ]),
                "by_severity": {}
            },
            "trends": {
                "improving": 0,
                "declining": 0,
                "stable": 0
            }
        }
        
        # Count alerts by severity
        for alert in service.quality_monitor.alert_history:
            if alert.timestamp >= last_7d:
                severity = alert.severity.value
                stats["alerts"]["by_severity"][severity] = \
                    stats["alerts"]["by_severity"].get(severity, 0) + 1
        
        # Count trends
        trends = await service.quality_monitor.get_quality_trends(hours=24)
        for trend in trends:
            direction = trend.trend_direction
            if direction in stats["trends"]:
                stats["trends"][direction] += 1
        
        return stats
        
    except Exception as e:
        logger.error("get_stats_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e)) 