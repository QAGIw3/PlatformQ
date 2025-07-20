"""API endpoints for Resource Monitoring Service"""

from typing import List, Optional
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Query

from platformq_resource_common import (
    ResourceMetrics,
    ClusterMetrics,
    ResourceAnomalyEvent
)

from .main import monitor

router = APIRouter()


@router.get("/metrics/service/{service_name}", response_model=ResourceMetrics)
async def get_service_metrics(
    service_name: str,
    namespace: str = Query(default="platformq", description="Kubernetes namespace")
):
    """Get current metrics for a service"""
    if not monitor:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    metrics = await monitor.get_service_metrics(service_name, namespace)
    if not metrics:
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for service {service_name} in namespace {namespace}"
        )
    
    return metrics


@router.get("/metrics/cluster", response_model=ClusterMetrics)
async def get_cluster_metrics():
    """Get current cluster-wide metrics"""
    if not monitor:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    metrics = await monitor.get_cluster_metrics()
    if not metrics:
        raise HTTPException(status_code=404, detail="No cluster metrics available")
    
    return metrics


@router.get("/metrics/service/{service_name}/history", response_model=List[ResourceMetrics])
async def get_service_metrics_history(
    service_name: str,
    namespace: str = Query(default="platformq", description="Kubernetes namespace"),
    hours: int = Query(default=24, ge=1, le=168, description="Hours of history to retrieve")
):
    """Get historical metrics for a service"""
    if not monitor:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=hours)
    
    metrics = await monitor.get_historical_metrics(
        service_name, namespace, start_time, end_time
    )
    
    return metrics


@router.get("/metrics/services", response_model=List[ResourceMetrics])
async def get_all_services_metrics(
    namespace: Optional[str] = Query(default=None, description="Filter by namespace")
):
    """Get current metrics for all services"""
    if not monitor:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    all_metrics = []
    
    # Get all metrics from cache
    for key in monitor.metrics_cache.keys():
        if namespace and not key.startswith(f"{namespace}/"):
            continue
        
        metrics_dict = monitor.metrics_cache.get(key)
        metrics = ResourceMetrics(**metrics_dict)
        all_metrics.append(metrics)
    
    return all_metrics


@router.get("/anomalies/service/{service_name}", response_model=List[ResourceAnomalyEvent])
async def get_service_anomalies(
    service_name: str,
    namespace: str = Query(default="platformq", description="Kubernetes namespace")
):
    """Get current anomalies for a service"""
    if not monitor:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    # Get current metrics
    metrics = await monitor.get_service_metrics(service_name, namespace)
    if not metrics:
        raise HTTPException(
            status_code=404,
            detail=f"No metrics found for service {service_name}"
        )
    
    # Detect anomalies
    anomalies = await monitor.detect_anomalies(metrics)
    
    return anomalies


@router.get("/anomalies", response_model=List[ResourceAnomalyEvent])
async def get_all_anomalies(
    severity_threshold: float = Query(
        default=0.5, ge=0.0, le=1.0,
        description="Minimum severity threshold"
    )
):
    """Get all current anomalies across the platform"""
    if not monitor:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    all_anomalies = []
    
    # Check all services for anomalies
    for key in monitor.metrics_cache.keys():
        metrics_dict = monitor.metrics_cache.get(key)
        metrics = ResourceMetrics(**metrics_dict)
        
        # Detect anomalies
        anomalies = await monitor.detect_anomalies(metrics)
        
        # Filter by severity
        for anomaly in anomalies:
            if anomaly.severity >= severity_threshold:
                all_anomalies.append(anomaly)
    
    return all_anomalies


@router.get("/cache/size")
async def get_cache_sizes():
    """Get the size of various caches"""
    if not monitor:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    return {
        "metrics_cache": len(list(monitor.metrics_cache.keys())),
        "cluster_metrics_cache": len(list(monitor.cluster_metrics_cache.keys())),
        "historical_cache": len(list(monitor.historical_cache.keys()))
    }


@router.delete("/cache/historical")
async def clear_historical_cache(
    older_than_days: int = Query(
        default=7, ge=1,
        description="Clear metrics older than this many days"
    )
):
    """Clear historical metrics older than specified days"""
    if not monitor:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    cutoff = datetime.utcnow() - timedelta(days=older_than_days)
    cleared_count = 0
    
    for key in list(monitor.historical_cache.keys()):
        if ':' in key:
            timestamp_str = key.split(':')[-1]
            try:
                timestamp = datetime.fromisoformat(timestamp_str)
                if timestamp < cutoff:
                    monitor.historical_cache.remove(key)
                    cleared_count += 1
            except:
                pass
    
    return {
        "cleared_entries": cleared_count,
        "cutoff_date": cutoff.isoformat()
    } 