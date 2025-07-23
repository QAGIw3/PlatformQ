"""Metrics API router

Handles metrics collection and exposure for Prometheus.
"""

import logging
from typing import Dict, Any
from datetime import datetime

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from prometheus_client import Counter, Histogram, Gauge, generate_latest, REGISTRY


logger = logging.getLogger(__name__)
router = APIRouter()


# Define Prometheus metrics
job_submissions = Counter(
    'stream_processing_job_submissions_total',
    'Total number of job submissions',
    ['job_type', 'status']
)

job_processing_time = Histogram(
    'stream_processing_job_duration_seconds',
    'Job processing duration in seconds',
    ['job_type']
)

active_jobs = Gauge(
    'stream_processing_active_jobs',
    'Number of currently active jobs',
    ['job_type']
)

pattern_matches = Counter(
    'stream_processing_pattern_matches_total',
    'Total number of pattern matches',
    ['pattern_type', 'pattern_id']
)

state_operations = Counter(
    'stream_processing_state_operations_total',
    'Total number of state operations',
    ['operation', 'status']
)

state_cache_hits = Gauge(
    'stream_processing_state_cache_hits_ratio',
    'State cache hit ratio'
)

flink_job_status = Gauge(
    'stream_processing_flink_job_status',
    'Flink job status (1=running, 0=not running)',
    ['job_id', 'job_name']
)

system_memory_usage = Gauge(
    'stream_processing_memory_usage_bytes',
    'Memory usage in bytes',
    ['type']
)

api_requests = Counter(
    'stream_processing_api_requests_total',
    'Total number of API requests',
    ['method', 'endpoint', 'status_code']
)

api_request_duration = Histogram(
    'stream_processing_api_request_duration_seconds',
    'API request duration in seconds',
    ['method', 'endpoint']
)


@router.get("/metrics", response_class=PlainTextResponse)
async def get_metrics() -> str:
    """Expose Prometheus metrics"""
    from app.main import job_manager, state_manager
    
    try:
        # Update job metrics
        if job_manager:
            job_type_counts = {}
            for job in job_manager.jobs.values():
                job_type = job.type
                status = job.status.value
                
                if job_type not in job_type_counts:
                    job_type_counts[job_type] = 0
                    
                if status == "running":
                    job_type_counts[job_type] += 1
                    flink_job_status.labels(
                        job_id=job.id,
                        job_name=job.name
                    ).set(1)
                else:
                    flink_job_status.labels(
                        job_id=job.id,
                        job_name=job.name
                    ).set(0)
            
            # Update active jobs gauge
            for job_type, count in job_type_counts.items():
                active_jobs.labels(job_type=job_type).set(count)
        
        # Update state manager metrics
        if state_manager and state_manager.connected:
            metrics = state_manager.get_metrics()
            
            # Calculate cache hit ratio
            total_reads = metrics.get("reads", 0)
            hits = metrics.get("hits", 0)
            if total_reads > 0:
                hit_ratio = hits / total_reads
                state_cache_hits.set(hit_ratio)
            
            # Update state operation counters
            state_operations.labels(operation="read", status="success").inc(metrics.get("reads", 0))
            state_operations.labels(operation="write", status="success").inc(metrics.get("writes", 0))
            state_operations.labels(operation="delete", status="success").inc(metrics.get("deletes", 0))
        
        # Update system metrics
        try:
            import psutil
            memory = psutil.virtual_memory()
            system_memory_usage.labels(type="total").set(memory.total)
            system_memory_usage.labels(type="used").set(memory.used)
            system_memory_usage.labels(type="available").set(memory.available)
        except Exception as e:
            logger.warning(f"Failed to collect system metrics: {e}")
        
        # Generate Prometheus format metrics
        return generate_latest(REGISTRY)
        
    except Exception as e:
        logger.error(f"Failed to generate metrics: {e}")
        return f"# Error generating metrics: {str(e)}"


@router.get("/metrics/jobs")
async def get_job_metrics() -> Dict[str, Any]:
    """Get detailed job metrics"""
    from app.main import job_manager
    
    if not job_manager:
        return {"error": "Job manager not initialized"}
    
    metrics = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_jobs": len(job_manager.jobs),
        "jobs_by_status": {},
        "jobs_by_type": {},
        "recent_jobs": []
    }
    
    # Count by status and type
    for job in job_manager.jobs.values():
        status = job.status.value
        job_type = job.type
        
        metrics["jobs_by_status"][status] = metrics["jobs_by_status"].get(status, 0) + 1
        metrics["jobs_by_type"][job_type] = metrics["jobs_by_type"].get(job_type, 0) + 1
        
        # Add to recent jobs (last 10)
        if len(metrics["recent_jobs"]) < 10:
            metrics["recent_jobs"].append({
                "id": job.id,
                "name": job.name,
                "type": job.type,
                "status": status,
                "created_at": job.created_at.isoformat()
            })
    
    return metrics


@router.get("/metrics/patterns")
async def get_pattern_metrics() -> Dict[str, Any]:
    """Get pattern matching metrics"""
    from app.main import pattern_library
    
    if not pattern_library:
        return {"error": "Pattern library not initialized"}
    
    metrics = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_patterns": len(pattern_library.patterns),
        "patterns_by_type": {},
        "enabled_patterns": 0
    }
    
    # Count by type and status
    for pattern in pattern_library.patterns.values():
        pattern_type = pattern.type
        metrics["patterns_by_type"][pattern_type] = metrics["patterns_by_type"].get(pattern_type, 0) + 1
        
        if pattern.enabled:
            metrics["enabled_patterns"] += 1
    
    return metrics


@router.get("/metrics/state")
async def get_state_metrics() -> Dict[str, Any]:
    """Get state manager metrics"""
    from app.main import state_manager
    
    if not state_manager:
        return {"error": "State manager not initialized"}
    
    metrics = state_manager.get_metrics()
    
    # Calculate additional metrics
    total_reads = metrics.get("reads", 0)
    hits = metrics.get("hits", 0)
    hit_ratio = hits / total_reads if total_reads > 0 else 0
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "connected": state_manager.connected,
        "operations": metrics,
        "cache_hit_ratio": hit_ratio,
        "total_operations": sum(metrics.values())
    }


# Helper function to record API metrics
def record_api_metric(method: str, endpoint: str, status_code: int, duration: float):
    """Record API request metrics"""
    api_requests.labels(
        method=method,
        endpoint=endpoint,
        status_code=str(status_code)
    ).inc()
    
    api_request_duration.labels(
        method=method,
        endpoint=endpoint
    ).observe(duration) 