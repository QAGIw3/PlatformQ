"""Resource Scaling Service

Provides auto-scaling capabilities with predictive scaling.
"""

from contextlib import asynccontextmanager
from typing import Optional
import logging

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import generate_latest

from platformq_shared.security import get_current_user_from_trusted_header as get_current_user

from .config import settings
from .scaling_engine import ScalingEngine
from .api import router as api_router
from .event_processor import ScalingEventProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
scaling_engine: Optional[ScalingEngine] = None
event_processor: Optional[ScalingEventProcessor] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global scaling_engine, event_processor
    
    # Initialize scaling engine
    scaling_engine = ScalingEngine(settings)
    await scaling_engine.initialize()
    await scaling_engine.start()
    
    # Initialize event processor
    event_processor = ScalingEventProcessor(
        service_name=settings.service_name,
        pulsar_url=settings.pulsar_url,
        scaling_engine=scaling_engine
    )
    await event_processor.start()
    
    logger.info("Resource Scaling Service started")
    
    yield
    
    # Cleanup
    await event_processor.stop()
    await scaling_engine.stop()
    
    logger.info("Resource Scaling Service stopped")


# Create FastAPI app
app = FastAPI(
    title="Resource Scaling Service",
    description="Provides auto-scaling capabilities",
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