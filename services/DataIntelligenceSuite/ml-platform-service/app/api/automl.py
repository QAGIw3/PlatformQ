"""
AutoML API endpoints
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()


class AutoMLJobRequest(BaseModel):
    """AutoML job request"""
    name: str = Field(..., description="Job name")
    dataset: Dict[str, Any] = Field(..., description="Dataset configuration")
    target_column: str = Field(..., description="Target variable name")
    problem_type: str = Field(None, description="Type of ML problem (auto-detected if not provided)")
    time_limit: int = Field(60, description="Time limit in minutes")
    optimization_metric: str = Field(None, description="Metric to optimize")
    constraints: Dict[str, Any] = Field(default={}, description="Model constraints")


class AutoMLJobResponse(BaseModel):
    """AutoML job response"""
    job_id: str
    status: str
    message: str


@router.post("/jobs", response_model=AutoMLJobResponse)
async def start_automl_job(request: AutoMLJobRequest) -> AutoMLJobResponse:
    """Start an AutoML job"""
    try:
        from ..main import automl_engine
        
        if not automl_engine:
            raise HTTPException(status_code=503, detail="AutoML engine not available")
        
        job_id = await automl_engine.start_automl(request.dict())
        
        return AutoMLJobResponse(
            job_id=job_id,
            status="started",
            message="AutoML job started successfully"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error starting AutoML job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/jobs/{job_id}")
async def get_automl_job_status(job_id: str) -> Dict[str, Any]:
    """Get AutoML job status"""
    try:
        from ..main import automl_engine
        
        if not automl_engine:
            raise HTTPException(status_code=503, detail="AutoML engine not available")
        
        status = await automl_engine.get_job_status(job_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/jobs/{job_id}/best-model")
async def get_best_model(job_id: str) -> Dict[str, Any]:
    """Get the best model from AutoML job"""
    try:
        from ..main import automl_engine
        
        if not automl_engine:
            raise HTTPException(status_code=503, detail="AutoML engine not available")
        
        best_model = await automl_engine.get_best_model(job_id)
        return best_model
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting best model: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/jobs/{job_id}/leaderboard")
async def get_model_leaderboard(job_id: str, limit: int = 10) -> List[Dict[str, Any]]:
    """Get model leaderboard from AutoML job"""
    try:
        from ..main import automl_engine
        
        if not automl_engine:
            raise HTTPException(status_code=503, detail="AutoML engine not available")
        
        job = automl_engine.jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        leaderboard = job.get("leaderboard", [])
        return leaderboard[:limit]
        
    except Exception as e:
        logger.error(f"Error getting leaderboard: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/jobs/{job_id}")
async def cancel_automl_job(job_id: str) -> Dict[str, Any]:
    """Cancel an AutoML job"""
    try:
        from ..main import automl_engine
        
        if not automl_engine:
            raise HTTPException(status_code=503, detail="AutoML engine not available")
        
        success = await automl_engine.cancel_job(job_id)
        
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
async def get_automl_metrics() -> Dict[str, Any]:
    """Get AutoML engine metrics"""
    try:
        from ..main import automl_engine
        
        if not automl_engine:
            raise HTTPException(status_code=503, detail="AutoML engine not available")
        
        metrics = await automl_engine.get_automl_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting AutoML metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/search-spaces/{model_name}")
async def get_search_space(model_name: str, problem_type: str = "classification") -> Dict[str, Any]:
    """Get hyperparameter search space for a model"""
    try:
        from ..main import automl_engine
        
        if not automl_engine:
            raise HTTPException(status_code=503, detail="AutoML engine not available")
        
        search_space = await automl_engine.model_search.get_search_space(
            model_name,
            problem_type
        )
        
        return {
            "model_name": model_name,
            "problem_type": problem_type,
            "search_space": search_space
        }
        
    except Exception as e:
        logger.error(f"Error getting search space: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 