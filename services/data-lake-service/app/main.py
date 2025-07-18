"""
Data Lake Service

Provides medallion architecture data lake with Bronze, Silver, Gold layers.
Integrates with Apache Spark, MinIO, and various data sources.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, UploadFile, Form, Query, Depends, File, HTTPException
from sqlalchemy.orm import Session
import pandas as pd

from platformq_shared import (
    create_base_app,
    ErrorCode,
    AppException,
    EventProcessor,
    get_pulsar_client
)
from platformq_shared.config import ConfigLoader
from platformq_shared.event_publisher import EventPublisher

from .dependencies import get_spark_session
from .api.endpoints import router as api_router
from .quality.data_quality_manager import DataQualityManager, DataQualityLevel
from .repository import (
    DataIngestionRepository,
    DataQualityRepository,
    DataLineageRepository,
    DataCatalogRepository,
    ProcessingJobRepository
)
from .event_processors import DataLakeEventProcessor
from .models import DataIngestion

# Configure logging
logger = logging.getLogger(__name__)

# Create event processor instance
event_processor = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global event_processor
    
    # Startup
    logger.info("Initializing Data Lake Service...")
    
    # Initialize Spark session
    get_spark_session()
    
    # Initialize event processor
    pulsar_client = get_pulsar_client()
    event_processor = DataLakeEventProcessor(
        pulsar_client=pulsar_client,
        service_name="data-lake-service"
    )
    
    # Start event processor
    await event_processor.start()
    
    logger.info("Data Lake Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Data Lake Service...")
    
    # Stop event processor
    if event_processor:
        await event_processor.stop()
    
    # Stop Spark session
    spark = get_spark_session()
    if spark:
        spark.stop()
    
    logger.info("Data Lake Service shutdown complete")

# Create FastAPI app
app = create_base_app(
    title="PlatformQ Data Lake Service",
    description="Medallion architecture data lake with quality framework",
    version="1.0.0",
    lifespan=lifespan,
    event_processors=[event_processor] if event_processor else []
)

# Include API router
app.include_router(api_router, prefix="/api/v1")

# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    spark = get_spark_session()
    return {
        "status": "healthy",
        "service": "data-lake-service",
        "timestamp": "2024-01-01T00:00:00Z",
        "spark_active": spark is not None,
    }

async def trigger_optimization(pipeline_id: str):
    event = {'pipeline_id': pipeline_id}
    # Assuming pulsar_client is available in the environment or imported
    # For demonstration, we'll just print the event
    print(f"Triggering optimization for pipeline: {pipeline_id}")
    print(f"Event data: {event}")

# Data quality manager will be initialized in endpoints as needed

@app.post("/api/v1/ingest-with-quality")
async def ingest_data_with_quality(
    file: UploadFile = File(...),
    data_type: str = Form(...),
    target_quality: str = Form("silver"),
    tenant_id: str = Depends(lambda: "default-tenant"),  # TODO: Get from auth
    db: Session = Depends(lambda: None)  # TODO: Get from database
):
    """
    Ingest data with quality validation and processing
    """
    try:
        # Map quality level
        quality_level = DataQualityLevel[target_quality.upper()]
        
        # Save uploaded file temporarily
        temp_path = f"/tmp/{file.filename}"
        with open(temp_path, "wb") as f:
            content = await file.read()
            f.write(content)
            
        # Read data based on file type
        if file.filename.endswith('.csv'):
            df = pd.read_csv(temp_path)
        elif file.filename.endswith('.parquet'):
            df = pd.read_parquet(temp_path)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")
            
        # Get dependencies
        from .dependencies import get_minio_client, get_event_publisher
        from pyignite import Client as IgniteClient
        
        # Initialize data quality manager
        minio_client = get_minio_client()
        ignite_client = IgniteClient()
        ignite_client.connect('ignite', 10800)
        event_publisher = get_event_publisher()
        
        data_quality_manager = DataQualityManager(
            minio_client=minio_client,
            ignite_client=ignite_client,
            event_publisher=event_publisher
        )
        
        # Process with quality management
        result = await data_quality_manager.process_data_with_quality(
            data=df,
            data_type=data_type,
            source_path=file.filename,
            target_quality=quality_level
        )
        
        # Clean up temp file
        os.remove(temp_path)
        
        # Create ingestion record
        if result["action"] in ["accept", "clean"]:
            ingestion = DataIngestion(
                tenant_id=tenant_id,
                source_type="file_upload",
                source_path=file.filename,
                destination_path=result.get("output_path"),
                status="completed",
                quality_score=result["validation_results"]["quality_score"],
                metadata={
                    "data_type": data_type,
                    "target_quality": target_quality,
                    "action": result["action"],
                    "violations": result["validation_results"]["violations"]
                }
            )
        else:
            ingestion = DataIngestion(
                tenant_id=tenant_id,
                source_type="file_upload",
                source_path=file.filename,
                destination_path=result.get("quarantine_path"),
                status="quarantined" if result["action"] == "quarantine" else "rejected",
                quality_score=result["validation_results"]["quality_score"],
                metadata={
                    "data_type": data_type,
                    "target_quality": target_quality,
                    "action": result["action"],
                    "violations": result["validation_results"]["violations"]
                }
            )
        
        # Only save to db if available
        if db:
            db.add(ingestion)
            db.commit()
            ingestion_id = str(ingestion.id)
        else:
            # Generate a temporary ID if no DB
            import uuid
            ingestion_id = str(uuid.uuid4())
        
        return {
            "ingestion_id": ingestion_id,
            "status": ingestion.status,
            "quality_results": result
        }
        
    except Exception as e:
        logger.error(f"Error ingesting data with quality: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/quality/report")
async def get_data_quality_report(
    data_type: Optional[str] = Query(None, description="Filter by data type"),
    time_range: str = Query("24h", description="Time range for report"),
    tenant_id: str = Depends(lambda: "default-tenant") # This line was not in the new_code, but should be changed for consistency
):
    """
    Get data quality report
    """
    try:
        # Get dependencies
        from .dependencies import get_minio_client, get_event_publisher
        from pyignite import Client as IgniteClient
        
        # Initialize data quality manager
        minio_client = get_minio_client()
        ignite_client = IgniteClient()
        ignite_client.connect('ignite', 10800)
        event_publisher = get_event_publisher()
        
        data_quality_manager = DataQualityManager(
            minio_client=minio_client,
            ignite_client=ignite_client,
            event_publisher=event_publisher
        )
        
        report = await data_quality_manager.get_quality_report(
            data_type=data_type,
            time_range=time_range
        )
        
        return report
        
    except Exception as e:
        logger.error(f"Error getting quality report: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/quality/validate")
async def validate_data_quality(
    file: UploadFile = File(...),
    data_type: str = Form(...),
    tenant_id: str = Depends(lambda: "default-tenant") # This line was not in the new_code, but should be changed for consistency
):
    """
    Validate data quality without ingesting
    """
    try:
        # Save uploaded file temporarily
        temp_path = f"/tmp/{file.filename}"
        with open(temp_path, "wb") as f:
            content = await file.read()
            f.write(content)
            
        # Read data
        if file.filename.endswith('.csv'):
            df = pd.read_csv(temp_path)
        elif file.filename.endswith('.parquet'):
            df = pd.read_parquet(temp_path)
        else:
            raise HTTPException(status_code=400, detail="Unsupported file format")
            
        # Get dependencies
        from .dependencies import get_minio_client, get_event_publisher
        from pyignite import Client as IgniteClient
        
        # Initialize data quality manager
        minio_client = get_minio_client()
        ignite_client = IgniteClient()
        ignite_client.connect('ignite', 10800)
        event_publisher = get_event_publisher()
        
        data_quality_manager = DataQualityManager(
            minio_client=minio_client,
            ignite_client=ignite_client,
            event_publisher=event_publisher
        )
        
        # Validate
        validation_results = await data_quality_manager.validate_data(
            data=df,
            data_type=data_type,
            source_path=file.filename
        )
        
        # Clean up
        os.remove(temp_path)
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error validating data quality: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/quality/rules/{data_type}")
async def get_quality_rules(
    data_type: str,
    tenant_id: str = Depends(lambda: "default-tenant") # This line was not in the new_code, but should be changed for consistency
):
    """
    Get quality rules for a data type
    """
    try:
        rules = data_quality_manager.rules.get(data_type, [])
        
        return {
            "data_type": data_type,
            "rules": [
                {
                    "rule_id": rule.rule_id,
                    "name": rule.name,
                    "severity": rule.severity
                }
                for rule in rules
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting quality rules: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Add quality monitoring background task
async def monitor_data_quality():
    """Background task to monitor data quality metrics"""
    while True:
        try:
            # Get quality metrics from cache
            cache = data_quality_manager.quality_cache
            
            # Check for quality degradation
            for data_type in data_quality_manager.rules.keys():
                key = f"quality_metrics:{data_type}"
                metrics = cache.get(key)
                
                if metrics and metrics["average_score"] < 0.7:
                    # Publish alert
                    await event_publisher.publish_event(
                        "DATA_QUALITY_DEGRADATION",
                        {
                            "data_type": data_type,
                            "average_score": metrics["average_score"],
                            "total_validations": metrics["total_validations"],
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
                    
            # Sleep for 5 minutes
            await asyncio.sleep(300)
            
        except Exception as e:
            logger.error(f"Error in quality monitoring: {e}")
            await asyncio.sleep(60)


# Start background task on startup
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(monitor_data_quality())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 