"""Monitoring API endpoints"""

from typing import Dict, Any, List, Optional
from fastapi import APIRouter, HTTPException, Request, Query
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger()
router = APIRouter()


@router.get("/metrics/current")
async def get_current_metrics(
    request: Request
) -> Dict[str, Any]:
    """Get current system metrics"""
    try:
        system_monitor = request.app.state.system_monitor
        
        metrics = await system_monitor.get_current_metrics()
        
        return {
            "metrics": metrics,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get current metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/historical")
async def get_historical_metrics(
    request: Request,
    hours: int = Query(24, ge=1, le=168, description="Hours of history")
) -> Dict[str, Any]:
    """Get historical system metrics"""
    try:
        system_monitor = request.app.state.system_monitor
        
        metrics = await system_monitor.get_historical_metrics(hours)
        
        return {
            "metrics": [
                {
                    "timestamp": m.timestamp.isoformat(),
                    "cpu_usage": m.cpu_usage,
                    "memory_usage": m.memory_usage,
                    "disk_usage": m.disk_usage,
                    "network_io": m.network_io,
                    "active_processes": m.active_processes,
                    "service_metrics": m.service_metrics
                }
                for m in metrics
            ],
            "count": len(metrics),
            "time_range": {
                "start": (datetime.utcnow() - timedelta(hours=hours)).isoformat(),
                "end": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to get historical metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/anomalies")
async def get_anomalies(
    request: Request,
    hours: int = Query(24, ge=1, le=168, description="Hours to check")
) -> Dict[str, Any]:
    """Get detected anomalies"""
    try:
        ml_optimizer = request.app.state.ml_optimizer
        system_monitor = request.app.state.system_monitor
        
        # Get recent metrics
        recent_metrics = await system_monitor.get_historical_metrics(hours)
        
        # Detect anomalies
        anomalies = []
        for metric in recent_metrics:
            metric_dict = {
                "cpu_usage": metric.cpu_usage,
                "memory_usage": metric.memory_usage,
                "disk_usage": metric.disk_usage,
                "active_processes": metric.active_processes
            }
            
            detected = ml_optimizer.detect_anomalies(metric_dict)
            if detected:
                anomalies.extend(detected)
                
        return {
            "anomalies": anomalies,
            "count": len(anomalies),
            "time_range": {
                "start": (datetime.utcnow() - timedelta(hours=hours)).isoformat(),
                "end": datetime.utcnow().isoformat()
            }
        }
        
    except Exception as e:
        logger.error(f"Anomaly detection failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health/services")
async def get_service_health(
    request: Request
) -> Dict[str, Any]:
    """Get health status of integrated services"""
    try:
        health_status = {}
        
        # Check orchestrator
        orchestrator = request.app.state.orchestrator
        health_status["orchestrator"] = {
            "status": "healthy" if orchestrator._running else "stopped",
            "active_workflows": len(orchestrator.active_workflows),
            "workflow_history_size": len(orchestrator.workflow_history)
        }
        
        # Check ML optimizer
        ml_optimizer = request.app.state.ml_optimizer
        health_status["ml_optimizer"] = {
            "status": "healthy",
            "models_loaded": {
                "performance_model": ml_optimizer.performance_model is not None,
                "resource_predictor": ml_optimizer.resource_predictor is not None,
                "anomaly_detector": ml_optimizer.anomaly_detector is not None
            }
        }
        
        # Check system monitor
        system_monitor = request.app.state.system_monitor
        health_status["system_monitor"] = {
            "status": "healthy" if system_monitor._running else "stopped",
            "metrics_history_size": len(system_monitor._metrics_history)
        }
        
        # Overall status
        all_healthy = all(
            service.get("status") == "healthy"
            for service in health_status.values()
        )
        
        return {
            "overall_status": "healthy" if all_healthy else "degraded",
            "services": health_status,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/performance/summary")
async def get_performance_summary(
    request: Request,
    time_range: str = Query("24h", description="Time range: 1h, 24h, 7d, 30d")
) -> Dict[str, Any]:
    """Get performance summary statistics"""
    try:
        orchestrator = request.app.state.orchestrator
        
        # Parse time range
        time_map = {
            "1h": timedelta(hours=1),
            "24h": timedelta(hours=24),
            "7d": timedelta(days=7),
            "30d": timedelta(days=30)
        }
        
        delta = time_map.get(time_range, timedelta(hours=24))
        cutoff = datetime.utcnow() - delta
        
        # Filter recent executions
        recent_executions = [
            ex for ex in orchestrator.workflow_history
            if ex.start_time >= cutoff
        ]
        
        if not recent_executions:
            return {
                "summary": {
                    "total_executions": 0,
                    "success_rate": 0,
                    "average_duration": 0,
                    "total_optimizations": 0
                },
                "time_range": time_range
            }
            
        # Calculate statistics
        total = len(recent_executions)
        successful = len([ex for ex in recent_executions if ex.status.value == "completed"])
        
        durations = [
            (ex.end_time - ex.start_time).total_seconds()
            for ex in recent_executions
            if ex.end_time
        ]
        
        optimized = len([
            ex for ex in recent_executions
            if ex.optimizations_applied
        ])
        
        return {
            "summary": {
                "total_executions": total,
                "success_rate": successful / total if total > 0 else 0,
                "average_duration": sum(durations) / len(durations) if durations else 0,
                "total_optimizations": optimized,
                "optimization_rate": optimized / total if total > 0 else 0
            },
            "time_range": time_range,
            "analyzed_from": cutoff.isoformat(),
            "analyzed_to": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Failed to get performance summary: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 