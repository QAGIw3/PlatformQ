"""
Neuromorphic Computing Service

Brain-inspired computing with spiking neural networks
"""

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import logging

from app.api import neuromorphic
from app.core.neuromorphic_engine import NeuromorphicEngine

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
    logger.info("Starting Neuromorphic Computing Service")
    
    # Initialize neuromorphic engine
    ignite_host = os.getenv("IGNITE_HOST", "ignite")
    ignite_port = int(os.getenv("IGNITE_PORT", "10800"))
    pulsar_url = os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
    
    engine = NeuromorphicEngine(
        ignite_host=ignite_host,
        ignite_port=ignite_port,
        pulsar_url=pulsar_url
    )
    
    # Initialize engine
    await engine.initialize()
    
    # Store in app state
    app.state.neuromorphic_engine = engine
    
    logger.info("Neuromorphic Computing Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Neuromorphic Computing Service")
    
    # Close engine connections
    if hasattr(app.state, 'neuromorphic_engine'):
        app.state.neuromorphic_engine.close()
        
    logger.info("Neuromorphic Computing Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Neuromorphic Computing Service",
    description="Brain-inspired computing with spiking neural networks for energy-efficient AI",
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
app.include_router(neuromorphic.router, prefix="/api/v1", tags=["neuromorphic"])


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Neuromorphic Computing Service",
        "version": "1.0.0",
        "status": "running",
        "description": "Brain-inspired computing with spiking neural networks"
    }


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "neuromorphic-computing-service"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 