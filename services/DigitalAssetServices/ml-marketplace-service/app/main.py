"""
ML Marketplace Service

Decentralized marketplace for ML models and datasets
"""

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

from app.api import marketplace
from app.core.marketplace import ModelMarketplace

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
    logger.info("Starting ML Marketplace Service")
    
    # Initialize marketplace
    ignite_host = os.getenv("IGNITE_HOST", "ignite")
    ignite_port = int(os.getenv("IGNITE_PORT", "10800"))
    pulsar_url = os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
    
    marketplace_instance = ModelMarketplace(
        ignite_host=ignite_host,
        ignite_port=ignite_port,
        pulsar_url=pulsar_url
    )
    
    # Initialize marketplace
    await marketplace_instance.initialize()
    
    # Store in app state
    app.state.marketplace = marketplace_instance
    
    logger.info("ML Marketplace Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down ML Marketplace Service")
    
    # Close marketplace connections
    if hasattr(app.state, 'marketplace'):
        app.state.marketplace.close()
        
    logger.info("ML Marketplace Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="ML Marketplace Service",
    description="Decentralized marketplace for ML models and datasets",
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
app.include_router(marketplace.router, prefix="/api/v1", tags=["marketplace"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "ML Marketplace Service",
        "version": "1.0.0",
        "status": "running",
        "description": "Decentralized marketplace for ML models and datasets"
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "ml-marketplace-service"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 