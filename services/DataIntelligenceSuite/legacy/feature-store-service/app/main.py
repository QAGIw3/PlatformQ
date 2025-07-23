"""
Feature Store Service

High-performance feature management for ML pipelines
"""

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

from app.api import features
from app.core.feature_store import FeatureStore

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    # Startup
    logger.info("Starting Feature Store Service")
    
    # Initialize feature store
    ignite_host = os.getenv("IGNITE_HOST", "ignite")
    ignite_port = int(os.getenv("IGNITE_PORT", "10800"))
    pulsar_url = os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
    
    feature_store = FeatureStore(
        ignite_host=ignite_host,
        ignite_port=ignite_port,
        pulsar_url=pulsar_url
    )
    
    # Store in app state
    app.state.feature_store = feature_store
    
    logger.info("Feature Store Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Feature Store Service")
    
    # Close feature store connections
    if hasattr(app.state, 'feature_store'):
        app.state.feature_store.close()
        
    logger.info("Feature Store Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Feature Store Service",
    description="High-performance feature management for ML pipelines",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(features.router, prefix="/api/v1", tags=["features"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Feature Store Service",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "feature-store-service"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 