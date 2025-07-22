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
    'batch_processing_job_submissions_total',
    'Total number of job submissions',
    ['job_type', 'resource_profile']
)

job_completions = Counter(
    'batch_processing_job_completions_total',
    'Total number of job completions',
    ['job_type', 'status']
)

job_duration = Histogram(
    'batch_processing_job_duration_seconds',
    'Job execution duration in seconds',
    ['job_type'],
    buckets=(60, 300, 600, 1800, 3600, 7200, 14400)  # 1m, 5m, 10m, 30m, 1h, 2h, 4h
)

active_jobs = Gauge(
    'batch_processing_active_jobs',
    'Number of currently active jobs',
    ['job_type']
)

scheduled_jobs = Gauge(
    'batch_processing_scheduled_jobs',
    'Number of scheduled jobs'
)

spark_executors = Gauge(
    'batch_processing_spark_executors',
    'Number of Spark executors',
    ['status']
)

resource_utilization = Gauge(
    'batch_processing_resource_utilization_percent',
    'Resource utilization percentage',
    ['resource_type']
)

cluster_health = Gauge(
    'batch_processing_cluster_health',
    'Cluster health status (0=critical, 1=warning, 2=healthy)'
)


@router.get("/metrics", response_class=PlainTextResponse)
async def get_metrics() -> str:
    """Expose Prometheus metrics"""
    from app.main import job_scheduler, resource_manager
    
    try:
        # Update job metrics
        if job_scheduler:
            # Count jobs by type and status
            job_type_counts = {}
            for job in job_scheduler.jobs.values():
                job_type = job.type
                status = job.status.value
                
                if job_type not in job_type_counts:
                    job_type_counts[job_type] = {"running": 0, "completed": 0, "failed": 0}
                
                if status == "running":
                    job_type_counts[job_type]["running"] += 1
                elif status == "completed":
                    job_type_counts[job_type]["completed"] += 1
                elif status == "failed":
                    job_type_counts[job_type]["failed"] += 1
            
            # Update active jobs gauge
            for job_type, counts in job_type_counts.items():
                active_jobs.labels(job_type=job_type).set(counts["running"])
            
            # Update scheduled jobs
            scheduled_jobs.set(len(job_scheduler.scheduled_jobs))
        
        # Update resource metrics
        if resource_manager:
            utilization = resource_manager.get_resource_utilization()
            
            # Update utilization gauges
            resource_utilization.labels(resource_type="cpu").set(utilization["cpu"])
            resource_utilization.labels(resource_type="memory").set(utilization["memory"])
            resource_utilization.labels(resource_type="disk").set(utilization["disk"])
            resource_utilization.labels(resource_type="executors").set(utilization["executors"])
            
            # Update executor metrics
            spark_executors.labels(status="active").set(resource_manager.current_metrics.executors_active)
            spark_executors.labels(status="total").set(resource_manager.current_metrics.executors_total)
            
            # Update cluster health
            health_status = resource_manager._calculate_health_status()
            health_value = {"critical": 0, "warning": 1, "healthy": 2}.get(health_status, 0)
            cluster_health.set(health_value)
        
        # Generate Prometheus format metrics
        return generate_latest(REGISTRY)
        
    except Exception as e:
        logger.error(f"Failed to generate metrics: {e}")
        return f"# Error generating metrics: {str(e)}"


@router.get("/metrics/jobs")
async def get_job_metrics() -> Dict[str, Any]:
    """Get detailed job metrics"""
    from app.main import job_scheduler
    
    if not job_scheduler:
        return {"error": "Job scheduler not initialized"}
    
    metrics = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_jobs": len(job_scheduler.jobs),
        "jobs_by_status": {},
        "jobs_by_type": {},
        "average_duration_by_type": {}
    }
    
    # Count and calculate metrics
    for job in job_scheduler.jobs.values():
        status = job.status.value
        job_type = job.type
        
        # Count by status
        metrics["jobs_by_status"][status] = metrics["jobs_by_status"].get(status, 0) + 1
        
        # Count by type
        if job_type not in metrics["jobs_by_type"]:
            metrics["jobs_by_type"][job_type] = {"count": 0, "total_duration": 0}
        
        metrics["jobs_by_type"][job_type]["count"] += 1
        
        # Calculate duration for completed jobs
        if job.completed_at and job.started_at:
            duration = (job.completed_at - job.started_at).total_seconds()
            metrics["jobs_by_type"][job_type]["total_duration"] += duration
    
    # Calculate averages
    for job_type, data in metrics["jobs_by_type"].items():
        if data["count"] > 0:
            metrics["average_duration_by_type"][job_type] = data["total_duration"] / data["count"]
    
    return metrics


@router.get("/metrics/resources")
async def get_resource_metrics() -> Dict[str, Any]:
    """Get detailed resource metrics"""
    from app.main import resource_manager
    
    if not resource_manager:
        return {"error": "Resource manager not initialized"}
    
    cluster_status = await resource_manager.get_cluster_status()
    available = resource_manager.get_available_resources()
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "cluster_status": cluster_status,
        "available_resources": available,
        "resource_history": [
            metrics.to_dict() for metrics in resource_manager.resource_history[-10:]
        ]
    }


# Helper function to record job metrics
def record_job_submission(job_type: str, resource_profile: str):
    """Record job submission metrics"""
    job_submissions.labels(
        job_type=job_type,
        resource_profile=resource_profile
    ).inc()


def record_job_completion(job_type: str, status: str, duration_seconds: float):
    """Record job completion metrics"""
    job_completions.labels(
        job_type=job_type,
        status=status
    ).inc()
    
    if status == "completed":
        job_duration.labels(job_type=job_type).observe(duration_seconds) 