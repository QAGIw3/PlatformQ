"""
ML experiments API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum

router = APIRouter()


class ExperimentStatus(str, Enum):
    ACTIVE = "active"
    COMPLETED = "completed"
    ARCHIVED = "archived"


class RunStatus(str, Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    KILLED = "killed"


class ExperimentCreate(BaseModel):
    """Create experiment request"""
    name: str = Field(..., description="Experiment name")
    description: Optional[str] = Field(None)
    tags: Dict[str, str] = Field(default_factory=dict)
    artifact_location: Optional[str] = Field(None, description="Custom artifact storage location")


class ExperimentResponse(BaseModel):
    """Experiment response"""
    experiment_id: str
    name: str
    description: Optional[str]
    artifact_location: str
    lifecycle_stage: ExperimentStatus
    tags: Dict[str, str]
    creation_time: datetime
    last_update_time: datetime
    created_by: str


class RunCreate(BaseModel):
    """Create experiment run"""
    experiment_id: str = Field(..., description="Experiment to run under")
    run_name: Optional[str] = Field(None, description="Optional run name")
    tags: Dict[str, str] = Field(default_factory=dict)
    source_type: str = Field("LOCAL", description="Source type (LOCAL, NOTEBOOK, JOB, etc.)")
    source_name: Optional[str] = Field(None, description="Source name/path")
    entry_point: Optional[str] = Field(None, description="Entry point for code")


class RunResponse(BaseModel):
    """Experiment run response"""
    run_id: str
    experiment_id: str
    status: RunStatus
    start_time: datetime
    end_time: Optional[datetime]
    metrics: Dict[str, float]
    params: Dict[str, str]
    tags: Dict[str, str]
    artifacts: List[str]
    source_type: str
    source_name: Optional[str]
    created_by: str


class MetricRecord(BaseModel):
    """Metric log record"""
    key: str = Field(..., description="Metric name")
    value: float = Field(..., description="Metric value")
    timestamp: Optional[datetime] = Field(None)
    step: Optional[int] = Field(None, description="Training step/epoch")


class ParamRecord(BaseModel):
    """Parameter record"""
    key: str = Field(..., description="Parameter name")
    value: str = Field(..., description="Parameter value")


@router.post("/experiments", response_model=ExperimentResponse)
async def create_experiment(
    experiment: ExperimentCreate,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Create a new experiment"""
    experiment_id = f"exp_{datetime.utcnow().timestamp()}"
    
    return ExperimentResponse(
        experiment_id=experiment_id,
        name=experiment.name,
        description=experiment.description,
        artifact_location=experiment.artifact_location or f"s3://mlflow/{experiment_id}",
        lifecycle_stage=ExperimentStatus.ACTIVE,
        tags=experiment.tags,
        creation_time=datetime.utcnow(),
        last_update_time=datetime.utcnow(),
        created_by=user_id
    )


@router.get("/experiments", response_model=List[ExperimentResponse])
async def list_experiments(
    tenant_id: str = Query(..., description="Tenant ID"),
    view_type: str = Query("active_only", description="active_only, deleted_only, all"),
    search_filter: Optional[str] = Query(None, description="Search in name/tags"),
    order_by: List[str] = Query(["creation_time DESC"], description="Order by fields"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List experiments"""
    return []


@router.get("/experiments/{experiment_id}", response_model=ExperimentResponse)
async def get_experiment(
    experiment_id: str,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get experiment details"""
    raise HTTPException(status_code=404, detail="Experiment not found")


@router.patch("/experiments/{experiment_id}")
async def update_experiment(
    experiment_id: str,
    name: Optional[str] = Query(None),
    description: Optional[str] = Query(None),
    tags: Optional[Dict[str, str]] = None,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Update experiment metadata"""
    return {
        "experiment_id": experiment_id,
        "status": "updated",
        "updated_at": datetime.utcnow()
    }


@router.delete("/experiments/{experiment_id}")
async def delete_experiment(
    experiment_id: str,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Delete (archive) an experiment"""
    return {
        "experiment_id": experiment_id,
        "status": "archived",
        "archived_at": datetime.utcnow()
    }


@router.post("/runs", response_model=RunResponse)
async def create_run(
    run: RunCreate,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Create a new experiment run"""
    run_id = f"run_{datetime.utcnow().timestamp()}"
    
    return RunResponse(
        run_id=run_id,
        experiment_id=run.experiment_id,
        status=RunStatus.RUNNING,
        start_time=datetime.utcnow(),
        end_time=None,
        metrics={},
        params={},
        tags=run.tags,
        artifacts=[],
        source_type=run.source_type,
        source_name=run.source_name,
        created_by=user_id
    )


@router.get("/runs", response_model=List[RunResponse])
async def list_runs(
    tenant_id: str = Query(..., description="Tenant ID"),
    experiment_id: Optional[str] = Query(None),
    status: Optional[RunStatus] = Query(None),
    metric_filter: Optional[str] = Query(None, description="e.g., 'accuracy > 0.9'"),
    param_filter: Optional[str] = Query(None, description="e.g., 'learning_rate = 0.01'"),
    order_by: List[str] = Query(["start_time DESC"]),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List experiment runs with filtering"""
    return []


@router.get("/runs/{run_id}", response_model=RunResponse)
async def get_run(
    run_id: str,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get run details"""
    raise HTTPException(status_code=404, detail="Run not found")


@router.post("/runs/{run_id}/log-metric")
async def log_metric(
    run_id: str,
    metric: MetricRecord,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Log a metric for a run"""
    return {
        "status": "logged",
        "run_id": run_id,
        "metric": metric.key,
        "value": metric.value,
        "timestamp": metric.timestamp or datetime.utcnow()
    }


@router.post("/runs/{run_id}/log-metrics")
async def log_metrics(
    run_id: str,
    metrics: List[MetricRecord],
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Log multiple metrics for a run"""
    return {
        "status": "logged",
        "run_id": run_id,
        "metrics_count": len(metrics),
        "timestamp": datetime.utcnow()
    }


@router.post("/runs/{run_id}/log-param")
async def log_param(
    run_id: str,
    param: ParamRecord,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Log a parameter for a run"""
    return {
        "status": "logged",
        "run_id": run_id,
        "param": param.key,
        "value": param.value
    }


@router.post("/runs/{run_id}/log-params")
async def log_params(
    run_id: str,
    params: List[ParamRecord],
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Log multiple parameters for a run"""
    return {
        "status": "logged",
        "run_id": run_id,
        "params_count": len(params)
    }


@router.post("/runs/{run_id}/log-artifact")
async def log_artifact(
    run_id: str,
    artifact_path: str = Query(..., description="Path to artifact"),
    artifact_type: str = Query("file", description="Artifact type"),
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Log an artifact for a run"""
    artifact_id = f"artifact_{datetime.utcnow().timestamp()}"
    
    return {
        "status": "logged",
        "run_id": run_id,
        "artifact_id": artifact_id,
        "path": artifact_path,
        "type": artifact_type
    }


@router.patch("/runs/{run_id}/end")
async def end_run(
    run_id: str,
    status: RunStatus = Query(RunStatus.COMPLETED),
    end_time: Optional[datetime] = Query(None),
    tenant_id: str = Query(..., description="Tenant ID")
):
    """End an experiment run"""
    return {
        "run_id": run_id,
        "status": status.value,
        "end_time": end_time or datetime.utcnow()
    }


@router.post("/runs/compare")
async def compare_runs(
    run_ids: List[str] = Query(..., description="Run IDs to compare"),
    metric_keys: Optional[List[str]] = Query(None, description="Specific metrics to compare"),
    param_keys: Optional[List[str]] = Query(None, description="Specific params to compare"),
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Compare multiple experiment runs"""
    return {
        "comparison": {
            "runs": run_ids,
            "metrics": {
                "accuracy": {
                    run_ids[0]: 0.95,
                    run_ids[1]: 0.93 if len(run_ids) > 1 else None
                },
                "loss": {
                    run_ids[0]: 0.15,
                    run_ids[1]: 0.18 if len(run_ids) > 1 else None
                }
            },
            "params": {
                "learning_rate": {
                    run_ids[0]: "0.01",
                    run_ids[1]: "0.001" if len(run_ids) > 1 else None
                },
                "batch_size": {
                    run_ids[0]: "32",
                    run_ids[1]: "64" if len(run_ids) > 1 else None
                }
            }
        },
        "best_run": {
            "run_id": run_ids[0],
            "metric": "accuracy",
            "value": 0.95
        }
    }


@router.get("/experiments/{experiment_id}/best-run")
async def get_best_run(
    experiment_id: str,
    metric: str = Query(..., description="Metric to optimize"),
    mode: str = Query("max", description="Optimization mode (max/min)"),
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get best run from an experiment"""
    return {
        "experiment_id": experiment_id,
        "best_run": {
            "run_id": "run_best_123",
            "metric_value": 0.97,
            "metric_name": metric,
            "params": {
                "learning_rate": "0.001",
                "batch_size": "32",
                "epochs": "50"
            }
        }
    }


@router.post("/experiments/{experiment_id}/archive-runs")
async def archive_old_runs(
    experiment_id: str,
    older_than_days: int = Query(30, ge=1),
    keep_best_n: int = Query(5, ge=0),
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Archive old experiment runs"""
    return {
        "experiment_id": experiment_id,
        "archived_count": 15,
        "kept_count": keep_best_n,
        "archive_date": datetime.utcnow()
    } 