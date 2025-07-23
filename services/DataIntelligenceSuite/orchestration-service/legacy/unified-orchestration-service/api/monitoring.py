"""
Monitoring API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from platformq_shared.logging import get_logger
from ..core import (
    AirflowBridge, PipelineManager, MLPipelineOptimizer,
    SeaTunnelOrchestrator, EventOrchestrator, CredentialAttestor
)

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/monitoring", tags=["monitoring"])

# Dependency injection
components: Dict[str, Any] = {}

def set_dependencies(
    airflow: AirflowBridge,
    pipeline: PipelineManager,
    ml_optimizer: MLPipelineOptimizer,
    seatunnel: SeaTunnelOrchestrator,
    events: EventOrchestrator,
    credentials: CredentialAttestor
):
    """Set API dependencies"""
    global components
    components = {
        'airflow': airflow,
        'pipeline': pipeline,
        'ml_optimizer': ml_optimizer,
        'seatunnel': seatunnel,
        'events': events,
        'credentials': credentials
    }


# Response models
class HealthCheckResponse(BaseModel):
    status: str
    timestamp: str
    components: Dict[str, Dict[str, Any]]


class MetricsResponse(BaseModel):
    orchestration_metrics: Dict[str, Any]
    component_metrics: Dict[str, Dict[str, Any]]
    timestamp: str


class ActiveWorkflowsResponse(BaseModel):
    total_active: int
    by_type: Dict[str, int]
    workflows: List[Dict[str, Any]]


# API Endpoints
@router.get("/health", response_model=HealthCheckResponse)
async def health_check():
    """Get service health status"""
    try:
        health_status = {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "components": {}
        }
        
        # Check Airflow connectivity
        try:
            if components.get('airflow'):
                # Simple connectivity check
                health_status['components']['airflow'] = {
                    "status": "healthy",
                    "api_url": components['airflow'].base_url
                }
        except Exception as e:
            health_status['components']['airflow'] = {
                "status": "unhealthy",
                "error": str(e)
            }
            health_status['status'] = "degraded"
            
        # Check Pipeline Manager
        if components.get('pipeline'):
            health_status['components']['pipeline_manager'] = {
                "status": "healthy",
                "active_pipelines": len(components['pipeline'].pipelines),
                "active_executions": len([
                    e for e in components['pipeline'].executions.values()
                    if e['status'] == 'running'
                ])
            }
            
        # Check ML Optimizer
        if components.get('ml_optimizer'):
            health_status['components']['ml_optimizer'] = {
                "status": "healthy",
                "models_loaded": len(components['ml_optimizer'].models),
                "optimization_history": len(components['ml_optimizer'].optimization_history)
            }
            
        # Check SeaTunnel
        if components.get('seatunnel'):
            health_status['components']['seatunnel'] = {
                "status": "healthy",
                "active_jobs": len([
                    j for j in components['seatunnel'].jobs.values()
                    if j['status'] in ['running', 'submitted']
                ]),
                "templates_loaded": len(components['seatunnel'].templates)
            }
            
        # Check Event Orchestrator
        if components.get('events'):
            health_status['components']['event_orchestrator'] = {
                "status": "healthy",
                "active_mappings": len(components['events'].event_mappings),
                "active_correlations": len(components['events'].active_correlations)
            }
            
        # Check Credential Attestor
        if components.get('credentials'):
            health_status['components']['credential_attestor'] = {
                "status": "healthy",
                "issued_credentials": len(components['credentials'].issued_credentials),
                "key_initialized": components['credentials'].private_key is not None
            }
            
        return health_status
        
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics")
async def get_metrics():
    """Get Prometheus metrics"""
    try:
        # Return Prometheus metrics in text format
        metrics = generate_latest()
        return metrics.decode('utf-8')
        
    except Exception as e:
        logger.error(f"Failed to generate metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics/json", response_model=MetricsResponse)
async def get_metrics_json():
    """Get metrics in JSON format"""
    try:
        metrics = {
            "orchestration_metrics": {},
            "component_metrics": {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Orchestration-level metrics
        if components.get('pipeline'):
            pipeline_mgr = components['pipeline']
            metrics['orchestration_metrics']['pipelines'] = {
                "total": len(pipeline_mgr.pipelines),
                "executions": {
                    "total": len(pipeline_mgr.executions),
                    "running": len([
                        e for e in pipeline_mgr.executions.values()
                        if e['status'] == 'running'
                    ]),
                    "completed": len([
                        e for e in pipeline_mgr.executions.values()
                        if e['status'] == 'success'
                    ]),
                    "failed": len([
                        e for e in pipeline_mgr.executions.values()
                        if e['status'] == 'failed'
                    ])
                }
            }
            
        # Component-specific metrics
        if components.get('seatunnel'):
            seatunnel = components['seatunnel']
            metrics['component_metrics']['seatunnel'] = {
                "jobs": {
                    "total": len(seatunnel.jobs),
                    "running": len([
                        j for j in seatunnel.jobs.values()
                        if j['status'] == 'running'
                    ]),
                    "completed": len([
                        j for j in seatunnel.jobs.values()
                        if j['status'] == 'finished'
                    ])
                }
            }
            
        if components.get('events'):
            events = components['events']
            event_stats = await events.get_event_statistics()
            metrics['component_metrics']['events'] = event_stats
            
        if components.get('ml_optimizer'):
            optimizer = components['ml_optimizer']
            metrics['component_metrics']['ml_optimizer'] = {
                "models": len(optimizer.models),
                "optimizations_performed": len(optimizer.optimization_history),
                "average_confidence": sum(
                    h.get('confidence', 0) for h in optimizer.optimization_history
                ) / max(len(optimizer.optimization_history), 1)
            }
            
        return metrics
        
    except Exception as e:
        logger.error(f"Failed to get metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/active-workflows", response_model=ActiveWorkflowsResponse)
async def get_active_workflows(
    include_details: bool = Query(False)
):
    """Get active workflows across all orchestration systems"""
    try:
        active_workflows = {
            "total_active": 0,
            "by_type": {},
            "workflows": []
        }
        
        # Get active pipelines
        if components.get('pipeline'):
            pipeline_mgr = components['pipeline']
            active_pipelines = [
                e for e in pipeline_mgr.executions.values()
                if e['status'] == 'running'
            ]
            
            active_workflows['total_active'] += len(active_pipelines)
            active_workflows['by_type']['pipeline'] = len(active_pipelines)
            
            if include_details:
                for execution in active_pipelines:
                    active_workflows['workflows'].append({
                        "type": "pipeline",
                        "id": execution['id'],
                        "name": execution['pipeline_name'],
                        "started_at": execution['started_at'],
                        "current_step": execution.get('current_step')
                    })
                    
        # Get active SeaTunnel jobs
        if components.get('seatunnel'):
            seatunnel = components['seatunnel']
            active_jobs = [
                j for j in seatunnel.jobs.values()
                if j['status'] in ['running', 'submitted']
            ]
            
            active_workflows['total_active'] += len(active_jobs)
            active_workflows['by_type']['seatunnel'] = len(active_jobs)
            
            if include_details:
                for job in active_jobs:
                    active_workflows['workflows'].append({
                        "type": "seatunnel",
                        "id": job['id'],
                        "name": job['name'],
                        "job_type": job['job_type'],
                        "status": job['status'],
                        "created_at": job['created_at']
                    })
                    
        # Get active event correlations
        if components.get('events'):
            events = components['events']
            active_correlations = len(events.active_correlations)
            
            active_workflows['total_active'] += active_correlations
            active_workflows['by_type']['event_correlation'] = active_correlations
            
        return active_workflows
        
    except Exception as e:
        logger.error(f"Failed to get active workflows: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/performance")
async def get_performance_metrics(
    time_range: str = Query("1h", regex="^[0-9]+[hdm]$")
):
    """Get performance metrics for the specified time range"""
    try:
        # Parse time range
        unit = time_range[-1]
        value = int(time_range[:-1])
        
        if unit == 'h':
            delta = timedelta(hours=value)
        elif unit == 'd':
            delta = timedelta(days=value)
        else:  # 'm'
            delta = timedelta(minutes=value)
            
        cutoff_time = datetime.utcnow() - delta
        
        performance = {
            "time_range": time_range,
            "period_start": cutoff_time.isoformat(),
            "period_end": datetime.utcnow().isoformat(),
            "metrics": {}
        }
        
        # Pipeline performance
        if components.get('pipeline'):
            pipeline_mgr = components['pipeline']
            recent_executions = [
                e for e in pipeline_mgr.executions.values()
                if 'completed_at' in e and
                datetime.fromisoformat(e['completed_at']) > cutoff_time
            ]
            
            if recent_executions:
                durations = []
                for exec in recent_executions:
                    start = datetime.fromisoformat(exec['started_at'])
                    end = datetime.fromisoformat(exec['completed_at'])
                    duration = (end - start).total_seconds()
                    durations.append(duration)
                    
                performance['metrics']['pipeline'] = {
                    "executions": len(recent_executions),
                    "average_duration_seconds": sum(durations) / len(durations),
                    "min_duration_seconds": min(durations),
                    "max_duration_seconds": max(durations),
                    "success_rate": len([
                        e for e in recent_executions if e['status'] == 'success'
                    ]) / len(recent_executions)
                }
                
        # Event processing performance
        if components.get('events'):
            events = components['events']
            recent_mappings = [
                m for m in events.event_mappings.values()
                if m.get('last_triggered') and
                datetime.fromisoformat(m['last_triggered']) > cutoff_time
            ]
            
            if recent_mappings:
                total_executions = sum(m.get('execution_count', 0) for m in recent_mappings)
                performance['metrics']['events'] = {
                    "triggered_mappings": len(recent_mappings),
                    "total_executions": total_executions
                }
                
        return performance
        
    except Exception as e:
        logger.error(f"Failed to get performance metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/resource-usage")
async def get_resource_usage():
    """Get current resource usage"""
    try:
        usage = {
            "timestamp": datetime.utcnow().isoformat(),
            "resources": {}
        }
        
        # Pipeline resources
        if components.get('pipeline'):
            pipeline_mgr = components['pipeline']
            active_pipelines = len([
                e for e in pipeline_mgr.executions.values()
                if e['status'] == 'running'
            ])
            
            usage['resources']['pipeline'] = {
                "active_pipelines": active_pipelines,
                "max_concurrent": pipeline_mgr.resource_pool['concurrent'],
                "utilization": active_pipelines / pipeline_mgr.resource_pool['concurrent']
            }
            
        # Memory usage (if available)
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            
            usage['resources']['memory'] = {
                "rss_mb": memory_info.rss / 1024 / 1024,
                "vms_mb": memory_info.vms / 1024 / 1024,
                "percent": process.memory_percent()
            }
            
            # CPU usage
            usage['resources']['cpu'] = {
                "percent": process.cpu_percent(interval=1),
                "num_threads": process.num_threads()
            }
        except ImportError:
            pass
            
        return usage
        
    except Exception as e:
        logger.error(f"Failed to get resource usage: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 