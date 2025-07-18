"""
Monitoring Dashboard API

Endpoints for real-time monitoring and analytics.
"""

from fastapi import APIRouter, HTTPException, Depends, WebSocket, WebSocketDisconnect, Query
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.monitoring.dashboard import MonitoringDashboard, DashboardMetric

router = APIRouter(
    prefix="/api/v1/monitoring",
    tags=["monitoring"]
)

# Initialize dashboard (would be injected in practice)
monitoring_dashboard = None


class AddMetricRequest(BaseModel):
    """Request to add a custom metric"""
    metric_id: str = Field(..., min_length=3, max_length=50)
    name: str = Field(..., min_length=3, max_length=100)
    category: str = Field(..., pattern="^(volume|liquidity|risk|performance|custom)$")
    aggregation: str = Field(..., pattern="^(sum|avg|max|min|last)$")
    time_window_minutes: int = Field(..., ge=1, le=1440)  # Max 24 hours
    alert_thresholds: Optional[Dict[str, Decimal]] = None


class UpdateMetricRequest(BaseModel):
    """Request to update a metric value"""
    value: Decimal
    metadata: Optional[Dict[str, Any]] = None


@router.get("/dashboard")
async def get_dashboard(
    categories: Optional[List[str]] = Query(None, description="Filter by categories"),
    lookback_hours: int = Query(default=24, ge=1, le=168),  # Max 7 days
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get main monitoring dashboard data
    """
    lookback = timedelta(hours=lookback_hours)
    dashboard_data = await monitoring_dashboard.get_dashboard_data(
        categories=categories,
        lookback=lookback
    )
    
    return dashboard_data


@router.get("/market-overview")
async def get_market_overview(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get comprehensive market overview
    """
    return await monitoring_dashboard.get_market_overview()


@router.get("/performance")
async def get_performance_metrics(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get system performance metrics
    """
    return await monitoring_dashboard.get_performance_metrics()


@router.get("/alerts")
async def get_alerts(
    active_only: bool = Query(default=True),
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get monitoring alerts
    """
    return await monitoring_dashboard.get_alert_dashboard()


@router.get("/metrics")
async def list_metrics(
    category: Optional[str] = None,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    List all available metrics
    """
    metrics = []
    
    for metric_id, metric in monitoring_dashboard.metrics.items():
        if category and metric.category != category:
            continue
            
        metrics.append({
            "metric_id": metric_id,
            "name": metric.name,
            "category": metric.category,
            "current_value": str(metric.current_value),
            "aggregation": metric.aggregation,
            "time_window": str(metric.time_window),
            "is_alerting": metric.is_alerting
        })
        
    return {
        "metrics": metrics,
        "total": len(metrics),
        "categories": list(set(m.category for m in monitoring_dashboard.metrics.values()))
    }


@router.get("/metric/{metric_id}")
async def get_metric_details(
    metric_id: str,
    lookback_hours: int = Query(default=24, ge=1, le=168),
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get detailed data for a specific metric
    """
    if metric_id not in monitoring_dashboard.metrics:
        raise HTTPException(status_code=404, detail=f"Metric not found: {metric_id}")
        
    metric = monitoring_dashboard.metrics[metric_id]
    lookback = timedelta(hours=lookback_hours)
    
    return {
        "metric_id": metric_id,
        "name": metric.name,
        "category": metric.category,
        "current_value": str(metric.current_value),
        "aggregation": metric.aggregation,
        "time_window": str(metric.time_window),
        "is_alerting": metric.is_alerting,
        "alert_thresholds": {k: str(v) for k, v in metric.alert_thresholds.items()},
        "time_series": metric.get_time_series(lookback),
        "statistics": await _calculate_metric_statistics(metric, lookback)
    }


@router.post("/metrics")
async def add_custom_metric(
    request: AddMetricRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Add a custom metric (admin only)
    """
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
        
    try:
        metric = monitoring_dashboard.add_metric(
            metric_id=request.metric_id,
            name=request.name,
            category=request.category,
            aggregation=request.aggregation,
            time_window=timedelta(minutes=request.time_window_minutes),
            alert_thresholds=request.alert_thresholds
        )
        
        return {
            "success": True,
            "metric_id": metric.metric_id,
            "name": metric.name,
            "category": metric.category
        }
        
    except Exception as e:
        logger.error(f"Failed to add metric: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/metric/{metric_id}")
async def update_metric_value(
    metric_id: str,
    request: UpdateMetricRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Update a metric value (internal use)
    """
    # This would typically be called by internal services
    if current_user.get("role") != "admin" and current_user.get("service") != "internal":
        raise HTTPException(status_code=403, detail="Unauthorized")
        
    try:
        await monitoring_dashboard.update_metric(
            metric_id=metric_id,
            value=request.value,
            metadata=request.metadata
        )
        
        return {
            "success": True,
            "metric_id": metric_id,
            "new_value": str(request.value),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to update metric: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health-score")
async def get_platform_health_score(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get overall platform health score
    """
    health_score = await monitoring_dashboard._calculate_health_score()
    
    # Determine health status
    if health_score >= 90:
        status = "excellent"
    elif health_score >= 75:
        status = "good"
    elif health_score >= 60:
        status = "fair"
    else:
        status = "poor"
        
    return {
        "health_score": health_score,
        "status": status,
        "factors": {
            "active_alerts": len(monitoring_dashboard.active_alerts),
            "risk_level": "medium",  # Would calculate from metrics
            "performance": "good",
            "liquidity": "high"
        },
        "recommendations": await _generate_health_recommendations(health_score)
    }


@router.get("/trends")
async def get_metric_trends(
    period: str = Query(default="24h", pattern="^(1h|24h|7d|30d)$"),
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get trending metrics and insights
    """
    # Convert period to timedelta
    period_map = {
        "1h": timedelta(hours=1),
        "24h": timedelta(hours=24),
        "7d": timedelta(days=7),
        "30d": timedelta(days=30)
    }
    
    lookback = period_map[period]
    
    trends = {
        "period": period,
        "timestamp": datetime.utcnow().isoformat(),
        "volume_trend": await _calculate_trend("total_volume_24h", lookback),
        "liquidity_trend": await _calculate_trend("total_liquidity", lookback),
        "user_trend": await _calculate_trend("active_users", lookback),
        "risk_trend": await _calculate_trend("platform_var_95", lookback),
        "insights": []
    }
    
    # Generate insights
    if trends["volume_trend"]["change_percent"] > 20:
        trends["insights"].append("Significant volume increase detected")
    
    if trends["risk_trend"]["change_percent"] > 10:
        trends["insights"].append("Risk levels increasing - monitor closely")
        
    return trends


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(...)  # Auth token
):
    """
    WebSocket endpoint for real-time monitoring updates
    """
    # Validate token (simplified)
    if not token:
        await websocket.close(code=1008, reason="Unauthorized")
        return
        
    await websocket.accept()
    
    try:
        # Add connection
        await monitoring_dashboard.add_websocket_connection(websocket)
        
        # Keep connection alive
        while True:
            # Wait for any message (ping/pong handled by framework)
            await websocket.receive_text()
            
    except WebSocketDisconnect:
        # Remove connection
        await monitoring_dashboard.remove_websocket_connection(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        await monitoring_dashboard.remove_websocket_connection(websocket)


@router.get("/export")
async def export_metrics(
    format: str = Query(default="json", pattern="^(json|csv)$"),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    current_user=Depends(get_current_user)
) -> Any:
    """
    Export metrics data
    """
    if current_user.get("role") not in ["admin", "analyst"]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
        
    # Collect data
    export_data = {
        "export_time": datetime.utcnow().isoformat(),
        "period": {
            "start": start_time.isoformat() if start_time else "all",
            "end": end_time.isoformat() if end_time else "now"
        },
        "metrics": {}
    }
    
    for metric_id, metric in monitoring_dashboard.metrics.items():
        time_series = metric.get_time_series()
        
        # Filter by time if specified
        if start_time or end_time:
            filtered = []
            for point in time_series:
                ts = datetime.fromisoformat(point["timestamp"])
                if start_time and ts < start_time:
                    continue
                if end_time and ts > end_time:
                    continue
                filtered.append(point)
            time_series = filtered
            
        export_data["metrics"][metric_id] = {
            "name": metric.name,
            "category": metric.category,
            "data": time_series
        }
        
    if format == "csv":
        # Convert to CSV format (simplified)
        csv_data = "metric_id,metric_name,timestamp,value\n"
        for metric_id, metric_data in export_data["metrics"].items():
            for point in metric_data["data"]:
                csv_data += f"{metric_id},{metric_data['name']},{point['timestamp']},{point['value']}\n"
        
        return csv_data
    else:
        return export_data


async def _calculate_metric_statistics(
    metric: DashboardMetric,
    lookback: timedelta
) -> Dict[str, Any]:
    """Calculate statistics for a metric"""
    time_series = metric.get_time_series(lookback)
    
    if not time_series:
        return {
            "min": "0",
            "max": "0",
            "avg": "0",
            "std_dev": "0",
            "count": 0
        }
        
    values = [Decimal(point["value"]) for point in time_series]
    
    return {
        "min": str(min(values)),
        "max": str(max(values)),
        "avg": str(sum(values) / len(values)),
        "std_dev": "0",  # Would calculate standard deviation
        "count": len(values)
    }


async def _generate_health_recommendations(health_score: float) -> List[str]:
    """Generate recommendations based on health score"""
    recommendations = []
    
    if health_score < 90:
        recommendations.append("Monitor active alerts and resolve critical issues")
        
    if health_score < 75:
        recommendations.append("Review risk metrics and consider reducing exposure")
        recommendations.append("Check system performance metrics")
        
    if health_score < 60:
        recommendations.append("URGENT: Platform health degraded - immediate action required")
        recommendations.append("Consider enabling reduce-only mode for high-risk users")
        
    return recommendations


async def _calculate_trend(
    metric_id: str,
    lookback: timedelta
) -> Dict[str, Any]:
    """Calculate trend for a metric"""
    if metric_id not in monitoring_dashboard.metrics:
        return {"error": "Metric not found"}
        
    metric = monitoring_dashboard.metrics[metric_id]
    time_series = metric.get_time_series(lookback)
    
    if len(time_series) < 2:
        return {
            "direction": "stable",
            "change_percent": 0,
            "current": str(metric.current_value)
        }
        
    # Compare first and last values
    first_value = Decimal(time_series[0]["value"])
    last_value = Decimal(time_series[-1]["value"])
    
    if first_value == 0:
        change_percent = 0
    else:
        change_percent = float((last_value - first_value) / first_value * 100)
        
    direction = "up" if change_percent > 5 else "down" if change_percent < -5 else "stable"
    
    return {
        "direction": direction,
        "change_percent": round(change_percent, 2),
        "current": str(last_value),
        "previous": str(first_value)
    }


import logging

logger = logging.getLogger(__name__) 