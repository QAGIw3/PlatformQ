"""
Stream Processing Service

Unified service for all real-time stream processing needs,
consolidating multiple Flink jobs into a single manageable service.
"""

import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

from app.core.config import settings
from app.core.job_manager import JobManager
from app.core.pattern_library import PatternLibrary
from app.core.state_manager import StateManager
from app.api import jobs, patterns, health, metrics
from app.middleware.error_handler import error_handler_middleware
from app.middleware.logging import logging_middleware

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
job_manager: Optional[JobManager] = None
pattern_library: Optional[PatternLibrary] = None
state_manager: Optional[StateManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global job_manager, pattern_library, state_manager
    
    # Startup
    logger.info(f"Starting {settings.service_name} v{settings.service_version}")
    
    # Initialize components
    job_manager = JobManager(settings)
    pattern_library = PatternLibrary(settings)
    state_manager = StateManager(settings)
    
    # Start background tasks
    await job_manager.start()
    await pattern_library.load_patterns()
    await state_manager.initialize()
    
    # Register with service discovery
    if settings.consul_enabled:
        from app.core.service_discovery import register_service
        await register_service(settings)
    
    logger.info("Stream Processing Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Stream Processing Service")
    
    # Stop components
    await job_manager.stop()
    await state_manager.cleanup()
    
    # Deregister from service discovery
    if settings.consul_enabled:
        from app.core.service_discovery import deregister_service
        await deregister_service(settings)
    
    logger.info("Stream Processing Service stopped")


# Create FastAPI app
app = FastAPI(
    title=settings.service_name,
    description="Unified stream processing service for real-time data processing",
    version=settings.service_version,
    lifespan=lifespan
)

# Add middleware
app.middleware("http")(error_handler_middleware)
app.middleware("http")(logging_middleware)

# Include routers
app.include_router(jobs.router, prefix="/api/v1/jobs", tags=["jobs"])
app.include_router(patterns.router, prefix="/api/v1/patterns", tags=["patterns"])
app.include_router(health.router, prefix="/api/v1", tags=["health"])
app.include_router(metrics.router, prefix="/api/v1", tags=["metrics"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": settings.service_name,
        "version": settings.service_version,
        "status": "running",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/api/v1/info")
async def service_info():
    """Get service information"""
    return {
        "service": {
            "name": settings.service_name,
            "version": settings.service_version,
            "environment": settings.environment
        },
        "capabilities": {
            "streaming": True,
            "cep": True,
            "stateful_processing": True,
            "windowing": True,
            "exactly_once": True
        },
        "job_types": [
            "streaming_sql",
            "cep_pattern",
            "stateful_processing",
            "window_aggregation",
            "async_io"
        ],
        "integrations": {
            "sources": ["pulsar", "kafka", "kinesis", "files"],
            "sinks": ["cassandra", "elasticsearch", "minio", "pulsar", "ignite"]
        }
    }


class JobSubmitRequest(BaseModel):
    """Job submission request"""
    name: str
    type: str
    config: Dict[str, Any]
    parallelism: Optional[int] = None
    checkpoint_interval: Optional[int] = 30000
    restart_strategy: Optional[str] = "fixed-delay"
    

@app.post("/api/v1/submit")
async def submit_job(request: JobSubmitRequest, background_tasks: BackgroundTasks):
    """Submit a new streaming job"""
    try:
        # Validate job type
        valid_types = ["streaming_sql", "cep_pattern", "stateful_processing", 
                      "window_aggregation", "async_io"]
        if request.type not in valid_types:
            raise HTTPException(400, f"Invalid job type. Must be one of: {valid_types}")
        
        # Submit job
        job_id = await job_manager.submit_job(
            name=request.name,
            job_type=request.type,
            config=request.config,
            parallelism=request.parallelism,
            checkpoint_interval=request.checkpoint_interval,
            restart_strategy=request.restart_strategy
        )
        
        return {
            "job_id": job_id,
            "status": "submitted",
            "message": f"Job {request.name} submitted successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to submit job: {e}")
        raise HTTPException(500, f"Failed to submit job: {str(e)}")


@app.get("/api/v1/jobs/{job_id}/status")
async def get_job_status(job_id: str):
    """Get job status"""
    try:
        status = await job_manager.get_job_status(job_id)
        if not status:
            raise HTTPException(404, f"Job {job_id} not found")
        return status
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        raise HTTPException(500, f"Failed to get job status: {str(e)}")


@app.delete("/api/v1/jobs/{job_id}")
async def cancel_job(job_id: str):
    """Cancel a running job"""
    try:
        result = await job_manager.cancel_job(job_id)
        if not result:
            raise HTTPException(404, f"Job {job_id} not found")
        return {"message": f"Job {job_id} cancelled successfully"}
    except Exception as e:
        logger.error(f"Failed to cancel job: {e}")
        raise HTTPException(500, f"Failed to cancel job: {str(e)}")


@app.post("/api/v1/jobs/{job_id}/savepoint")
async def create_savepoint(job_id: str):
    """Create a savepoint for the job"""
    try:
        savepoint_path = await job_manager.create_savepoint(job_id)
        if not savepoint_path:
            raise HTTPException(404, f"Job {job_id} not found")
        return {
            "job_id": job_id,
            "savepoint_path": savepoint_path,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Failed to create savepoint: {e}")
        raise HTTPException(500, f"Failed to create savepoint: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.api_port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    ) 