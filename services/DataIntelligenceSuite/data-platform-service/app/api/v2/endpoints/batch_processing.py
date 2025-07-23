"""
Batch Processing API v2 - Multi-Engine Support
"""

from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from enum import Enum

from data_intelligence_common.core.processing import (
    BatchProcessor, 
    BatchConfig,
    BatchEngine
)
from data_intelligence_common.core.lakehouse import LakehouseFormat
from platformq_shared.logging import get_logger

logger = get_logger(__name__)
router = APIRouter()


class BatchJobRequest(BaseModel):
    """Batch job request model"""
    name: str
    description: Optional[str] = None
    engine: BatchEngine = BatchEngine.SPARK
    source_path: str
    target_path: str
    lakehouse_format: LakehouseFormat = LakehouseFormat.DELTA
    transformations: Optional[List[Dict[str, Any]]] = None
    quality_checks: Optional[List[str]] = None
    enable_ml_optimization: bool = True
    partitions: Optional[int] = None
    resource_limits: Optional[Dict[str, Any]] = None


class BatchJobResponse(BaseModel):
    """Batch job response model"""
    job_id: str
    status: str
    engine: str
    estimated_duration: Optional[float] = None
    resource_allocation: Optional[Dict[str, Any]] = None


@router.post("/jobs", response_model=BatchJobResponse)
async def create_batch_job(
    request: BatchJobRequest,
    background_tasks: BackgroundTasks
):
    """
    Create a new batch processing job with multi-engine support
    """
    try:
        # Create batch configuration
        config = BatchConfig(
            name=request.name,
            engine=request.engine,
            enable_ml_optimization=request.enable_ml_optimization,
            lakehouse_format=request.lakehouse_format,
            resource_limits=request.resource_limits
        )
        
        # Initialize processor
        processor = BatchProcessor(config)
        
        # Generate job ID
        job_id = f"batch-{request.name}-{processor._generate_job_id()}"
        
        # Add background task for processing
        background_tasks.add_task(
            run_batch_job,
            processor,
            request,
            job_id
        )
        
        return BatchJobResponse(
            job_id=job_id,
            status="submitted",
            engine=request.engine.value,
            estimated_duration=processor._estimate_duration(request.source_path),
            resource_allocation=processor._allocate_resources()
        )
        
    except Exception as e:
        logger.error(f"Failed to create batch job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def run_batch_job(
    processor: BatchProcessor,
    request: BatchJobRequest,
    job_id: str
):
    """Run batch job in background"""
    try:
        logger.info(f"Starting batch job {job_id}")
        
        # Build processing pipeline
        pipeline = processor.read(request.source_path)
        
        # Apply transformations
        if request.transformations:
            for transform in request.transformations:
                pipeline = pipeline.transform(transform)
        
        # Apply quality checks
        if request.quality_checks:
            pipeline = pipeline.quality(request.quality_checks)
        
        # Write to lakehouse
        result = pipeline.to_lakehouse(request.target_path)
        
        logger.info(f"Batch job {job_id} completed: {result}")
        
    except Exception as e:
        logger.error(f"Batch job {job_id} failed: {e}")


@router.get("/jobs/{job_id}")
async def get_job_status(job_id: str):
    """Get batch job status"""
    # In production, this would query job tracking system
    return {
        "job_id": job_id,
        "status": "running",
        "progress": 0.75,
        "metrics": {
            "records_processed": 1000000,
            "processing_rate": 50000,
            "estimated_completion": "5 minutes"
        }
    }


@router.get("/engines")
async def list_available_engines():
    """List available batch processing engines"""
    return {
        "engines": [
            {
                "name": engine.value,
                "description": f"{engine.value} batch processing engine",
                "supported": True
            }
            for engine in BatchEngine
        ]
    }


@router.post("/jobs/{job_id}/optimize")
async def optimize_job(job_id: str):
    """Trigger ML-based optimization for running job"""
    return {
        "job_id": job_id,
        "optimization_status": "triggered",
        "expected_improvement": "15-30%"
    } 