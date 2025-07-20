"""Resource Monitoring Service

Monitors resource usage across all services and infrastructure components.
"""

from contextlib import asynccontextmanager
from typing import Optional
import logging

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest

from platformq_shared.security import get_current_user_from_trusted_header as get_current_user

from .config import settings
from .monitor import ResourceMonitor
from .api import router as api_router

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
monitor: Optional[ResourceMonitor] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global monitor
    
    # Initialize monitor
    monitor = ResourceMonitor(settings)
    await monitor.initialize()
    await monitor.start()
    
    logger.info("Resource Monitoring Service started")
    
    yield
    
    # Cleanup
    await monitor.stop()
    
    logger.info("Resource Monitoring Service stopped")


# Create FastAPI app
app = FastAPI(
    title="Resource Monitoring Service",
    description="Monitors resource usage across the platform",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(api_router, prefix="/api/v1")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": settings.service_name,
        "version": "1.0.0"
    }


@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    return generate_latest()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=settings.service_host, port=settings.service_port) 