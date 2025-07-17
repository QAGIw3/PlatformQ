"""
Data Lake Service

Provides medallion architecture data lake with Bronze, Silver, Gold layers.
Integrates with Apache Spark, MinIO, and various data sources.
"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .dependencies import get_spark_session
from .api.endpoints import router as api_router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Initializing Data Lake Service...")
    get_spark_session()
    logger.info("Data Lake Service initialized successfully")
    yield
    # Shutdown
    logger.info("Shutting down Data Lake Service...")
    spark = get_spark_session()
    if spark:
        spark.stop()
    logger.info("Data Lake Service shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="PlatformQ Data Lake Service",
    description="Medallion architecture data lake with quality framework",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    ) 