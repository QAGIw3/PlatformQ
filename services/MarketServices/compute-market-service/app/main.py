"""Compute Market Service - Main application."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import Settings


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    logger.info("Starting Compute Market Service...")
    
    # Initialize components
    settings = Settings()
    
    # Initialize state manager, pricing engine, etc.
    # Simplified for now
    
    logger.info("Compute Market Service started successfully")
    
    yield
    
    logger.info("Shutting down Compute Market Service...")


# Create FastAPI application
app = FastAPI(
    title="Compute Market Service",
    description="Marketplace for compute resources",
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


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Compute Market Service",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "resources": "/api/v1/resources",
            "allocations": "/api/v1/allocations",
            "pricing": "/api/v1/pricing",
            "providers": "/api/v1/providers"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8023,
        reload=True,
        log_level="info"
    ) 