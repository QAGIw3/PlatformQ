"""
Flash Provisioning Service

Provides instant resource provisioning using flash loans for just-in-time scaling.
"""

from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
import logging
import sys

from platformq_shared.logging import setup_logging
from platformq_shared.middleware import (
    RequestLoggingMiddleware,
    ErrorHandlerMiddleware,
    PrometheusMiddleware
)
from .api import flash

# Configure logging
setup_logging("flash-provisioning-service")
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Flash Provisioning Service",
    description="Instant resource provisioning using flash loans",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add middleware
app.add_middleware(RequestLoggingMiddleware)
app.add_middleware(ErrorHandlerMiddleware)
app.add_middleware(PrometheusMiddleware, app_name="flash_provisioning")

# Include routers
app.include_router(flash.router, prefix="/api/v1")


@app.on_event("startup")
async def startup_event():
    """Initialize service on startup"""
    logger.info("Starting Flash Provisioning Service")
    
    # Initialize dependencies
    from .dependencies import (
        get_flash_protocol,
        get_resource_matcher,
        get_capacity_monitor
    )
    
    try:
        # Pre-initialize singletons
        await get_flash_protocol()
        logger.info("Flash Provisioning Service initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize service: {e}")
        sys.exit(1)


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down Flash Provisioning Service")


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "flash-provisioning"}


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Flash Provisioning Service",
        "version": "1.0.0",
        "description": "Instant resource provisioning using flash loans",
        "endpoints": {
            "flash_provision": "/api/v1/flash/provision",
            "flash_swap": "/api/v1/flash/swap",
            "burst_provision": "/api/v1/flash/burst",
            "jit_scaling": "/api/v1/flash/jit-scaling/{resource_type}",
            "statistics": "/api/v1/flash/statistics",
            "health": "/health"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080) 