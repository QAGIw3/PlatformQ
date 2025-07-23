"""Jobs API router

Handles job submission, monitoring, and management endpoints.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from app.core.job_manager import JobManager, JobStatus


logger = logging.getLogger(__name__)
router = APIRouter()


class JobSubmitRequest(BaseModel):
    """Job submission request model"""
    name: str = Field(..., description="Job name")
    type: str = Field(..., description="Job type (streaming_sql, cep_pattern, etc.)")
    config: Dict[str, Any] = Field(..., description="Job configuration")
    parallelism: Optional[int] = Field(None, description="Job parallelism")
    checkpoint_interval: Optional[int] = Field(30000, description="Checkpoint interval in ms")
    restart_strategy: Optional[str] = Field("fixed-delay", description="Restart strategy")
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "fraud-detection-job",
                "type": "cep_pattern",
                "config": {
                    "pattern_id": "fraud_velocity",
                    "input_topic": "transactions",
                    "output_topic": "fraud_alerts"
                },
                "parallelism": 4,
                "checkpoint_interval": 30000
            }
        }


class JobResponse(BaseModel):
    """Job response model"""
    id: str
    name: str
    type: str
    status: str
    created_at: str
    updated_at: str
    flink_job_id: Optional[str] = None
    error: Optional[str] = None
    metrics: Optional[Dict[str, Any]] = None


class JobSubmitResponse(BaseModel):
    """Job submission response"""
    job_id: str
    status: str
    message: str


# Dependency to get job manager
def get_job_manager() -> JobManager:
    """Get job manager instance"""
    from app.main import job_manager
    if not job_manager:
        raise HTTPException(500, "Job manager not initialized")
    return job_manager


@router.post("/", response_model=JobSubmitResponse)
async def submit_job(
    request: JobSubmitRequest,
    job_manager: JobManager = get_job_manager()
) -> JobSubmitResponse:
    """Submit a new streaming job"""
    try:
        job_id = await job_manager.submit_job(
            name=request.name,
            job_type=request.type,
            config=request.config,
            parallelism=request.parallelism,
            checkpoint_interval=request.checkpoint_interval,
            restart_strategy=request.restart_strategy
        )
        
        return JobSubmitResponse(
            job_id=job_id,
            status="submitted",
            message=f"Job {request.name} submitted successfully"
        )
        
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.error(f"Failed to submit job: {e}")
        raise HTTPException(500, f"Failed to submit job: {str(e)}")


@router.get("/", response_model=List[JobResponse])
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by job status"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of jobs to return"),
    offset: int = Query(0, ge=0, description="Number of jobs to skip"),
    job_manager: JobManager = get_job_manager()
) -> List[JobResponse]:
    """List all jobs"""
    try:
        jobs = await job_manager.list_jobs(status=status)
        
        # Apply pagination
        paginated_jobs = jobs[offset:offset + limit]
        
        return [JobResponse(**job) for job in paginated_jobs]
        
    except Exception as e:
        logger.error(f"Failed to list jobs: {e}")
        raise HTTPException(500, f"Failed to list jobs: {str(e)}")


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str = Path(..., description="Job ID"),
    job_manager: JobManager = get_job_manager()
) -> JobResponse:
    """Get job details"""
    try:
        job = await job_manager.get_job_status(job_id)
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
            
        return JobResponse(**job)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job {job_id}: {e}")
        raise HTTPException(500, f"Failed to get job: {str(e)}")


@router.delete("/{job_id}")
async def cancel_job(
    job_id: str = Path(..., description="Job ID"),
    job_manager: JobManager = get_job_manager()
) -> Dict[str, str]:
    """Cancel a running job"""
    try:
        success = await job_manager.cancel_job(job_id)
        if not success:
            raise HTTPException(404, f"Job {job_id} not found or not running")
            
        return {"message": f"Job {job_id} cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel job {job_id}: {e}")
        raise HTTPException(500, f"Failed to cancel job: {str(e)}")


@router.post("/{job_id}/savepoint")
async def create_savepoint(
    job_id: str = Path(..., description="Job ID"),
    job_manager: JobManager = get_job_manager()
) -> Dict[str, Any]:
    """Create a savepoint for the job"""
    try:
        savepoint_path = await job_manager.create_savepoint(job_id)
        if not savepoint_path:
            raise HTTPException(404, f"Job {job_id} not found or not running")
            
        return {
            "job_id": job_id,
            "savepoint_path": savepoint_path,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create savepoint for job {job_id}: {e}")
        raise HTTPException(500, f"Failed to create savepoint: {str(e)}")


@router.get("/{job_id}/metrics")
async def get_job_metrics(
    job_id: str = Path(..., description="Job ID"),
    job_manager: JobManager = get_job_manager()
) -> Dict[str, Any]:
    """Get job metrics"""
    try:
        job = await job_manager.get_job_status(job_id)
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
            
        return {
            "job_id": job_id,
            "metrics": job.get("metrics", {}),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get metrics for job {job_id}: {e}")
        raise HTTPException(500, f"Failed to get job metrics: {str(e)}")


@router.post("/{job_id}/restart")
async def restart_job(
    job_id: str = Path(..., description="Job ID"),
    from_savepoint: Optional[str] = Body(None, description="Savepoint path to restart from"),
    job_manager: JobManager = get_job_manager()
) -> Dict[str, str]:
    """Restart a job"""
    try:
        # This would be implemented to restart a job
        # For now, returning a placeholder response
        return {
            "message": f"Job {job_id} restart initiated",
            "from_savepoint": from_savepoint
        }
        
    except Exception as e:
        logger.error(f"Failed to restart job {job_id}: {e}")
        raise HTTPException(500, f"Failed to restart job: {str(e)}") 