"""Jobs API router

Handles job submission, monitoring, and management endpoints.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Path, Body
from pydantic import BaseModel, Field

from app.core.job_scheduler import JobScheduler


logger = logging.getLogger(__name__)
router = APIRouter()


class JobSubmitRequest(BaseModel):
    """Job submission request model"""
    name: str = Field(..., description="Job name")
    type: str = Field(..., description="Job type")
    config: Dict[str, Any] = Field(..., description="Job configuration")
    resource_profile: Optional[str] = Field("medium", description="Resource profile")
    priority: Optional[int] = Field(5, ge=1, le=10, description="Job priority (1-10)")
    schedule: Optional[str] = Field(None, description="Cron schedule expression")
    
    class Config:
        json_schema_extra = {
            "example": {
                "name": "daily_aggregation",
                "type": "spark_sql",
                "config": {
                    "query": "SELECT * FROM events WHERE date = current_date()",
                    "output_path": "s3a://data-lake/aggregated/daily"
                },
                "resource_profile": "medium",
                "priority": 5
            }
        }


class JobResponse(BaseModel):
    """Job response model"""
    id: str
    name: str
    type: str
    status: str
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    resource_profile: str
    priority: int
    spark_app_id: Optional[str] = None


# Dependency to get job scheduler
def get_job_scheduler() -> JobScheduler:
    """Get job scheduler instance"""
    from app.main import job_scheduler
    if not job_scheduler:
        raise HTTPException(500, "Job scheduler not initialized")
    return job_scheduler


@router.post("/", response_model=Dict[str, str])
async def submit_job(
    request: JobSubmitRequest,
    scheduler: JobScheduler = get_job_scheduler()
) -> Dict[str, str]:
    """Submit a new batch job"""
    try:
        job_id = await scheduler.submit_job(
            name=request.name,
            job_type=request.type,
            config=request.config,
            resource_profile=request.resource_profile,
            priority=request.priority,
            schedule=request.schedule
        )
        
        return {
            "job_id": job_id,
            "message": f"Job {request.name} submitted successfully"
        }
        
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.error(f"Failed to submit job: {e}")
        raise HTTPException(500, f"Failed to submit job: {str(e)}")


@router.get("/", response_model=List[JobResponse])
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by job status"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of jobs"),
    offset: int = Query(0, ge=0, description="Number of jobs to skip"),
    scheduler: JobScheduler = get_job_scheduler()
) -> List[JobResponse]:
    """List all jobs"""
    try:
        jobs = await scheduler.list_jobs(status=status)
        
        # Apply pagination
        paginated_jobs = jobs[offset:offset + limit]
        
        return [JobResponse(**job) for job in paginated_jobs]
        
    except Exception as e:
        logger.error(f"Failed to list jobs: {e}")
        raise HTTPException(500, f"Failed to list jobs: {str(e)}")


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str = Path(..., description="Job ID"),
    scheduler: JobScheduler = get_job_scheduler()
) -> JobResponse:
    """Get job details"""
    try:
        job = await scheduler.get_job_status(job_id)
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
    scheduler: JobScheduler = get_job_scheduler()
) -> Dict[str, str]:
    """Cancel a running job"""
    try:
        success = await scheduler.cancel_job(job_id)
        if not success:
            raise HTTPException(404, f"Job {job_id} not found or not cancellable")
            
        return {"message": f"Job {job_id} cancelled successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to cancel job {job_id}: {e}")
        raise HTTPException(500, f"Failed to cancel job: {str(e)}")


@router.get("/{job_id}/logs")
async def get_job_logs(
    job_id: str = Path(..., description="Job ID"),
    lines: int = Query(100, ge=1, le=10000, description="Number of log lines"),
    scheduler: JobScheduler = get_job_scheduler()
) -> Dict[str, Any]:
    """Get job logs"""
    try:
        logs = await scheduler.get_job_logs(job_id, lines)
        if logs is None:
            raise HTTPException(404, f"Logs for job {job_id} not found")
            
        return {
            "job_id": job_id,
            "logs": logs,
            "line_count": len(logs)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get logs for job {job_id}: {e}")
        raise HTTPException(500, f"Failed to get job logs: {str(e)}")


@router.post("/{job_id}/retry")
async def retry_job(
    job_id: str = Path(..., description="Job ID"),
    scheduler: JobScheduler = get_job_scheduler()
) -> Dict[str, str]:
    """Retry a failed job"""
    try:
        # Get original job
        job = await scheduler.get_job_status(job_id)
        if not job:
            raise HTTPException(404, f"Job {job_id} not found")
            
        # Submit new job with same configuration
        new_job_id = await scheduler.submit_job(
            name=f"{job['name']}_retry",
            job_type=job["type"],
            config=job["config"],
            resource_profile=job["resource_profile"],
            priority=job["priority"]
        )
        
        return {
            "original_job_id": job_id,
            "new_job_id": new_job_id,
            "message": "Job retry submitted successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retry job {job_id}: {e}")
        raise HTTPException(500, f"Failed to retry job: {str(e)}") 