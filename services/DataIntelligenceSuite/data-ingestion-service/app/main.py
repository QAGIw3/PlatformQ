"""
Data Ingestion Service

Unified service for data ingestion from multiple sources,
including CDC, streaming, batch, and schema management.
"""

import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, UploadFile, File
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

from app.core.config import settings
from app.core.cdc_manager import CDCManager
from app.core.stream_ingestion import StreamIngestionManager
from app.core.batch_ingestion import BatchIngestionManager
from app.core.schema_registry import SchemaRegistry
from app.api import ingestion, schemas, health, metrics
from app.middleware.error_handler import error_handler_middleware
from app.middleware.logging import logging_middleware

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
cdc_manager: Optional[CDCManager] = None
stream_manager: Optional[StreamIngestionManager] = None
batch_manager: Optional[BatchIngestionManager] = None
schema_registry: Optional[SchemaRegistry] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global cdc_manager, stream_manager, batch_manager, schema_registry
    
    # Startup
    logger.info(f"Starting {settings.service_name} v{settings.service_version}")
    
    # Initialize components
    cdc_manager = CDCManager(settings)
    stream_manager = StreamIngestionManager(settings)
    batch_manager = BatchIngestionManager(settings)
    schema_registry = SchemaRegistry(settings)
    
    # Start background tasks
    await cdc_manager.start()
    await stream_manager.start()
    await schema_registry.initialize()
    
    # Register with service discovery
    if settings.consul_enabled:
        from app.core.service_discovery import register_service
        await register_service(settings)
    
    logger.info("Data Ingestion Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Data Ingestion Service")
    
    # Stop components
    await cdc_manager.stop()
    await stream_manager.stop()
    await batch_manager.cleanup()
    
    # Deregister from service discovery
    if settings.consul_enabled:
        from app.core.service_discovery import deregister_service
        await deregister_service(settings)
    
    logger.info("Data Ingestion Service stopped")


# Create FastAPI app
app = FastAPI(
    title=settings.service_name,
    description="Unified data ingestion service supporting CDC, streaming, and batch",
    version=settings.service_version,
    lifespan=lifespan
)

# Add middleware
app.middleware("http")(error_handler_middleware)
app.middleware("http")(logging_middleware)

# Include routers
app.include_router(ingestion.router, prefix="/api/v1/ingestion", tags=["ingestion"])
app.include_router(schemas.router, prefix="/api/v1/schemas", tags=["schemas"])
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
            "cdc": True,
            "streaming": True,
            "batch": True,
            "schema_registry": True,
            "multi_source": True
        },
        "ingestion_types": [
            "database_cdc",
            "file_import",
            "stream_consumer",
            "api_webhook",
            "s3_sync"
        ],
        "supported_sources": {
            "databases": ["postgresql", "mysql", "mongodb", "cassandra"],
            "streams": ["pulsar", "kafka", "kinesis"],
            "files": ["csv", "json", "parquet", "avro"],
            "storage": ["s3", "minio", "gcs", "azure"]
        }
    }


class CDCSourceConfig(BaseModel):
    """CDC source configuration"""
    source_type: str  # postgresql, mysql, mongodb
    connection_string: str
    tables: Optional[List[str]] = None
    start_position: Optional[str] = None
    

@app.post("/api/v1/cdc/sources")
async def create_cdc_source(config: CDCSourceConfig):
    """Create a new CDC source"""
    try:
        source_id = await cdc_manager.create_source(
            source_type=config.source_type,
            connection_string=config.connection_string,
            tables=config.tables,
            start_position=config.start_position
        )
        
        return {
            "source_id": source_id,
            "status": "created",
            "message": f"CDC source created successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to create CDC source: {e}")
        raise HTTPException(500, f"Failed to create CDC source: {str(e)}")


class StreamIngestionConfig(BaseModel):
    """Stream ingestion configuration"""
    source_type: str  # pulsar, kafka, kinesis
    topics: List[str]
    consumer_group: Optional[str] = None
    schema_id: Optional[str] = None
    

@app.post("/api/v1/streams")
async def create_stream_ingestion(config: StreamIngestionConfig):
    """Create a new stream ingestion"""
    try:
        stream_id = await stream_manager.create_stream(
            source_type=config.source_type,
            topics=config.topics,
            consumer_group=config.consumer_group,
            schema_id=config.schema_id
        )
        
        return {
            "stream_id": stream_id,
            "status": "created",
            "message": f"Stream ingestion created successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to create stream ingestion: {e}")
        raise HTTPException(500, f"Failed to create stream ingestion: {str(e)}")


class BatchIngestionRequest(BaseModel):
    """Batch ingestion request"""
    source_type: str  # s3, file_upload, api
    source_path: str
    destination_table: str
    format: Optional[str] = "parquet"
    options: Optional[Dict[str, Any]] = None
    

@app.post("/api/v1/batch")
async def create_batch_ingestion(request: BatchIngestionRequest, background_tasks: BackgroundTasks):
    """Create a batch ingestion job"""
    try:
        job_id = await batch_manager.create_job(
            source_type=request.source_type,
            source_path=request.source_path,
            destination_table=request.destination_table,
            format=request.format,
            options=request.options
        )
        
        # Start processing in background
        background_tasks.add_task(batch_manager.process_job, job_id)
        
        return {
            "job_id": job_id,
            "status": "submitted",
            "message": f"Batch ingestion job submitted successfully"
        }
        
    except Exception as e:
        logger.error(f"Failed to create batch ingestion: {e}")
        raise HTTPException(500, f"Failed to create batch ingestion: {str(e)}")


@app.post("/api/v1/batch/upload")
async def upload_file_for_ingestion(
    file: UploadFile = File(...),
    destination_table: str = None,
    format: str = "auto"
):
    """Upload a file for batch ingestion"""
    try:
        # Save uploaded file
        file_path = await batch_manager.save_upload(file)
        
        # Create ingestion job
        job_id = await batch_manager.create_job(
            source_type="file_upload",
            source_path=file_path,
            destination_table=destination_table or file.filename.split('.')[0],
            format=format
        )
        
        return {
            "job_id": job_id,
            "file_name": file.filename,
            "file_size": file.size,
            "status": "uploaded",
            "message": "File uploaded and ingestion started"
        }
        
    except Exception as e:
        logger.error(f"Failed to upload file: {e}")
        raise HTTPException(500, f"Failed to upload file: {str(e)}")


@app.get("/api/v1/sources")
async def list_ingestion_sources():
    """List all active ingestion sources"""
    try:
        cdc_sources = await cdc_manager.list_sources()
        stream_sources = await stream_manager.list_streams()
        
        return {
            "cdc_sources": cdc_sources,
            "stream_sources": stream_sources,
            "total_sources": len(cdc_sources) + len(stream_sources)
        }
    except Exception as e:
        logger.error(f"Failed to list sources: {e}")
        raise HTTPException(500, f"Failed to list sources: {str(e)}")


@app.delete("/api/v1/sources/{source_id}")
async def delete_ingestion_source(source_id: str, source_type: str):
    """Delete an ingestion source"""
    try:
        if source_type == "cdc":
            result = await cdc_manager.delete_source(source_id)
        elif source_type == "stream":
            result = await stream_manager.delete_stream(source_id)
        else:
            raise HTTPException(400, f"Invalid source type: {source_type}")
        
        if not result:
            raise HTTPException(404, f"Source {source_id} not found")
            
        return {"message": f"Source {source_id} deleted successfully"}
    except Exception as e:
        logger.error(f"Failed to delete source: {e}")
        raise HTTPException(500, f"Failed to delete source: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.api_port,
        reload=settings.debug,
        log_level=settings.log_level.lower()
    ) 