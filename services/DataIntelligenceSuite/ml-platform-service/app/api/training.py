"""
Training API endpoints
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()


class TrainingJobRequest(BaseModel):
    """Training job request model"""
    name: str = Field(..., description="Job name")
    framework: str = Field(..., description="ML framework")
    model_type: str = Field(..., description="Model type")
    dataset: Dict[str, Any] = Field(..., description="Dataset configuration")
    hyperparameters: Dict[str, Any] = Field(..., description="Model hyperparameters")
    resources: Dict[str, Any] = Field(default={}, description="Resource requirements")
    callbacks: List[str] = Field(default=[], description="Training callbacks")


class TrainingJobResponse(BaseModel):
    """Training job response model"""
    job_id: str
    status: str
    message: str


@router.post("/jobs", response_model=TrainingJobResponse)
async def submit_training_job(request: TrainingJobRequest) -> TrainingJobResponse:
    """Submit a new training job"""
    try:
        # Get training orchestrator from app state
        from ..main import training_orchestrator
        
        if not training_orchestrator:
            raise HTTPException(status_code=503, detail="Training orchestrator not available")
        
        # Submit job
        job_id = await training_orchestrator.submit_training_job(request.dict())
        
        return TrainingJobResponse(
            job_id=job_id,
            status="submitted",
            message="Training job submitted successfully"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error submitting training job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/jobs/{job_id}")
async def get_training_job_status(job_id: str) -> Dict[str, Any]:
    """Get training job status"""
    try:
        from ..main import training_orchestrator
        
        if not training_orchestrator:
            raise HTTPException(status_code=503, detail="Training orchestrator not available")
        
        status = await training_orchestrator.get_job_status(job_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/jobs/{job_id}")
async def cancel_training_job(job_id: str) -> Dict[str, Any]:
    """Cancel a training job"""
    try:
        from ..main import training_orchestrator
        
        if not training_orchestrator:
            raise HTTPException(status_code=503, detail="Training orchestrator not available")
        
        success = await training_orchestrator.cancel_job(job_id)
        
        return {
            "job_id": job_id,
            "cancelled": success,
            "message": "Job cancelled successfully" if success else "Job could not be cancelled"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error cancelling job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/metrics")
async def get_training_metrics() -> Dict[str, Any]:
    """Get training orchestrator metrics"""
    try:
        from ..main import training_orchestrator
        
        if not training_orchestrator:
            raise HTTPException(status_code=503, detail="Training orchestrator not available")
        
        metrics = await training_orchestrator.get_training_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting training metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 