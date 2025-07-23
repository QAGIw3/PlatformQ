"""
Training API endpoints
"""
from typing import List, Optional
from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException, Query
from dependency_injector.wiring import inject, Provide

from ...core.container import Container
from ...core.training_manager import TrainingManager
from ...domain.models.training import (
    TrainingJob, TrainingStatus, TrainingConfig,
    DatasetConfig, TrainingMetrics, HyperparameterTuning
)
from ..dependencies import get_current_user

router = APIRouter(prefix="/training", tags=["training"])


@router.post("/jobs", response_model=TrainingJob)
@inject
async def submit_training_job(
    name: str,
    experiment_id: str,
    project_id: str,
    training_config: TrainingConfig,
    dataset_config: DatasetConfig,
    description: Optional[str] = None,
    tags: Optional[dict] = None,
    current_user: dict = Depends(get_current_user),
    training_manager: TrainingManager = Depends(Provide[Container.training_manager])
) -> TrainingJob:
    """Submit a new training job"""
    try:
        job = await training_manager.submit_job(
            name=name,
            experiment_id=experiment_id,
            user_id=current_user["user_id"],
            project_id=project_id,
            training_config=training_config,
            dataset_config=dataset_config,
            description=description,
            tags=tags
        )
        return job
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs", response_model=List[TrainingJob])
@inject
async def list_training_jobs(
    project_id: Optional[str] = Query(None),
    status: Optional[TrainingStatus] = Query(None),
    limit: int = Query(100, le=1000),
    current_user: dict = Depends(get_current_user),
    training_manager: TrainingManager = Depends(Provide[Container.training_manager])
) -> List[TrainingJob]:
    """List training jobs"""
    try:
        jobs = await training_manager.list_jobs(
            user_id=current_user["user_id"],
            project_id=project_id,
            status=status,
            limit=limit
        )
        return jobs
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}", response_model=TrainingJob)
@inject
async def get_training_job(
    job_id: UUID,
    training_manager: TrainingManager = Depends(Provide[Container.training_manager])
) -> TrainingJob:
    """Get training job details"""
    job = await training_manager.get_job_status(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Training job not found")
    return job


@router.delete("/jobs/{job_id}")
@inject
async def cancel_training_job(
    job_id: UUID,
    training_manager: TrainingManager = Depends(Provide[Container.training_manager])
) -> dict:
    """Cancel a training job"""
    success = await training_manager.cancel_job(job_id)
    if not success:
        raise HTTPException(status_code=404, detail="Training job not found or already completed")
    return {"message": "Training job cancelled successfully"}


@router.get("/jobs/{job_id}/metrics", response_model=List[TrainingMetrics])
@inject
async def get_training_metrics(
    job_id: UUID,
    training_manager: TrainingManager = Depends(Provide[Container.training_manager])
) -> List[TrainingMetrics]:
    """Get training metrics for a job"""
    metrics = await training_manager.get_job_metrics(job_id)
    return metrics


@router.post("/hyperparameter-tuning", response_model=List[TrainingJob])
@inject
async def run_hyperparameter_tuning(
    name: str,
    experiment_id: str,
    project_id: str,
    base_config: TrainingConfig,
    dataset_config: DatasetConfig,
    tuning_config: HyperparameterTuning,
    current_user: dict = Depends(get_current_user),
    training_manager: TrainingManager = Depends(Provide[Container.training_manager])
) -> List[TrainingJob]:
    """Run hyperparameter tuning"""
    try:
        jobs = await training_manager.hyperparameter_tuning(
            name=name,
            experiment_id=experiment_id,
            user_id=current_user["user_id"],
            project_id=project_id,
            base_config=base_config,
            dataset_config=dataset_config,
            tuning_config=tuning_config
        )
        return jobs
    except NotImplementedError:
        raise HTTPException(status_code=501, detail="Hyperparameter tuning not yet implemented")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 