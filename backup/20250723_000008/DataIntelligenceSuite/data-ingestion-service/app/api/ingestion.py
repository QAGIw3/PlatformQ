"""
Data Ingestion API endpoints
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, Depends, BackgroundTasks
from pydantic import BaseModel, Field

from app.core.cdc_manager import CDCManager, CDCSourceType
from app.core.stream_ingestion import StreamIngestionManager, StreamSourceType
from app.core.batch_ingestion import BatchIngestionManager, BatchSourceType

logger = logging.getLogger(__name__)

router = APIRouter()

# Global managers (will be injected)
cdc_manager: Optional[CDCManager] = None
stream_manager: Optional[StreamIngestionManager] = None
batch_manager: Optional[BatchIngestionManager] = None


def set_managers(cdc: CDCManager, stream: StreamIngestionManager, batch: BatchIngestionManager):
    """Set the global managers"""
    global cdc_manager, stream_manager, batch_manager
    cdc_manager = cdc
    stream_manager = stream
    batch_manager = batch


# Request/Response Models
class CDCSourceRequest(BaseModel):
    """CDC source creation request"""
    source_type: CDCSourceType
    connection_config: Dict[str, Any] = Field(..., description="Database connection configuration")
    tables: List[str] = Field(..., description="Tables to capture changes from")
    destination_config: Dict[str, Any] = Field(..., description="Where to send the CDC data")
    options: Optional[Dict[str, Any]] = Field(None, description="Additional CDC options")
    
    class Config:
        schema_extra = {
            "example": {
                "source_type": "postgresql",
                "connection_config": {
                    "hostname": "postgres",
                    "port": 5432,
                    "database": "mydb",
                    "username": "user",
                    "password": "pass"
                },
                "tables": ["users", "orders"],
                "destination_config": {
                    "type": "pulsar",
                    "topic": "cdc-events"
                }
            }
        }


class StreamIngestionRequest(BaseModel):
    """Stream ingestion creation request"""
    source_type: StreamSourceType
    topics: List[str] = Field(..., description="Topics/streams to consume from")
    destination_config: Dict[str, Any] = Field(..., description="Where to send the stream data")
    consumer_config: Optional[Dict[str, Any]] = Field(None, description="Consumer configuration")
    schema_config: Optional[Dict[str, Any]] = Field(None, description="Schema configuration")
    
    class Config:
        schema_extra = {
            "example": {
                "source_type": "pulsar",
                "topics": ["user-events", "system-events"],
                "destination_config": {
                    "type": "cassandra",
                    "table": "stream_data"
                },
                "consumer_config": {
                    "subscription": "ingestion-service",
                    "startup_mode": "latest"
                }
            }
        }


class BatchJobRequest(BaseModel):
    """Batch job creation request"""
    source_type: BatchSourceType
    source_config: Dict[str, Any] = Field(..., description="Source configuration")
    destination_config: Dict[str, Any] = Field(..., description="Destination configuration")
    processing_config: Optional[Dict[str, Any]] = Field(None, description="Processing/transformation config")
    schedule: Optional[str] = Field(None, description="Cron expression for scheduled jobs")
    
    class Config:
        schema_extra = {
            "example": {
                "source_type": "s3",
                "source_config": {
                    "bucket": "raw-data",
                    "path": "sales/*.csv",
                    "format": "csv"
                },
                "destination_config": {
                    "type": "minio",
                    "bucket": "processed-data",
                    "format": "parquet"
                }
            }
        }


# CDC Endpoints
@router.post("/cdc/sources", response_model=Dict[str, Any])
async def create_cdc_source(request: CDCSourceRequest):
    """Create a new CDC source"""
    try:
        source_id = await cdc_manager.create_source(
            source_type=request.source_type,
            connection_config=request.connection_config,
            tables=request.tables,
            destination_config=request.destination_config,
            options=request.options
        )
        
        return {
            "source_id": source_id,
            "status": "created",
            "message": "CDC source created and started"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create CDC source: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/cdc/sources", response_model=List[Dict[str, Any]])
async def list_cdc_sources(source_type: Optional[CDCSourceType] = None):
    """List all CDC sources"""
    try:
        sources = await cdc_manager.list_sources(source_type)
        return sources
        
    except Exception as e:
        logger.error(f"Failed to list CDC sources: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/cdc/sources/{source_id}", response_model=Dict[str, Any])
async def get_cdc_source_status(source_id: str):
    """Get CDC source status"""
    try:
        status = await cdc_manager.get_source_status(source_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to get CDC source status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/cdc/sources/{source_id}")
async def delete_cdc_source(source_id: str):
    """Delete a CDC source"""
    try:
        result = await cdc_manager.delete_source(source_id)
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete CDC source: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Stream Ingestion Endpoints
@router.post("/streams", response_model=Dict[str, Any])
async def create_stream_ingestion(request: StreamIngestionRequest):
    """Create a new stream ingestion"""
    try:
        stream_id = await stream_manager.create_stream(
            source_type=request.source_type,
            topics=request.topics,
            destination_config=request.destination_config,
            consumer_config=request.consumer_config,
            schema_config=request.schema_config
        )
        
        return {
            "stream_id": stream_id,
            "status": "created",
            "message": "Stream ingestion created and started"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create stream ingestion: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/streams", response_model=List[Dict[str, Any]])
async def list_stream_ingestions(
    source_type: Optional[StreamSourceType] = None,
    status: Optional[str] = None
):
    """List all stream ingestions"""
    try:
        streams = await stream_manager.list_streams(source_type, status)
        return streams
        
    except Exception as e:
        logger.error(f"Failed to list stream ingestions: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/streams/{stream_id}", response_model=Dict[str, Any])
async def get_stream_status(stream_id: str):
    """Get stream ingestion status"""
    try:
        status = await stream_manager.get_stream_status(stream_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to get stream status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.delete("/streams/{stream_id}")
async def delete_stream_ingestion(stream_id: str):
    """Delete a stream ingestion"""
    try:
        result = await stream_manager.delete_stream(stream_id)
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to delete stream: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/streams/{stream_id}/pause")
async def pause_stream(stream_id: str):
    """Pause a stream ingestion"""
    try:
        result = await stream_manager.pause_stream(stream_id)
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to pause stream: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/streams/{stream_id}/resume")
async def resume_stream(stream_id: str):
    """Resume a paused stream ingestion"""
    try:
        result = await stream_manager.resume_stream(stream_id)
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to resume stream: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# Batch Ingestion Endpoints
@router.post("/batch", response_model=Dict[str, Any])
async def create_batch_job(request: BatchJobRequest):
    """Create a batch ingestion job"""
    try:
        job_id = await batch_manager.create_batch_job(
            source_type=request.source_type,
            source_config=request.source_config,
            destination_config=request.destination_config,
            processing_config=request.processing_config,
            schedule=request.schedule
        )
        
        return {
            "job_id": job_id,
            "status": "created",
            "message": "Batch job created"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create batch job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/batch/upload", response_model=Dict[str, Any])
async def upload_and_process(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    destination_type: str = Form("minio"),
    destination_bucket: str = Form("processed-data"),
    destination_table: Optional[str] = Form(None),
    processing_sql: Optional[str] = Form(None)
):
    """Upload a file and process it"""
    try:
        # Prepare destination config
        destination_config = {
            "type": destination_type
        }
        
        if destination_type == "minio":
            destination_config["bucket"] = destination_bucket
        elif destination_type == "cassandra" and destination_table:
            destination_config["table"] = destination_table
            
        # Prepare processing config
        processing_config = None
        if processing_sql:
            processing_config = {"sql": processing_sql}
            
        # Upload and create job
        job_id = await batch_manager.upload_and_process(
            file=file,
            destination_config=destination_config,
            processing_config=processing_config
        )
        
        return {
            "job_id": job_id,
            "filename": file.filename,
            "status": "processing",
            "message": "File uploaded and processing started"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to upload and process file: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/batch", response_model=List[Dict[str, Any]])
async def list_batch_jobs(
    status: Optional[str] = None,
    limit: int = 100
):
    """List batch jobs"""
    try:
        jobs = await batch_manager.list_jobs(status, limit)
        return jobs
        
    except Exception as e:
        logger.error(f"Failed to list batch jobs: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/batch/{job_id}", response_model=Dict[str, Any])
async def get_batch_job_status(job_id: str):
    """Get batch job status"""
    try:
        status = await batch_manager.get_job_status(job_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to get batch job status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/batch/{job_id}/retry")
async def retry_batch_job(job_id: str):
    """Retry a failed batch job"""
    try:
        new_job_id = await batch_manager.retry_job(job_id)
        return {
            "original_job_id": job_id,
            "new_job_id": new_job_id,
            "status": "retrying"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to retry batch job: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 