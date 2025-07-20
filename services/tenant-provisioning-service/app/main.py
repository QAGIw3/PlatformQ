"""Tenant Provisioning Service

Orchestrates the provisioning of all necessary resources for tenants.
"""

from contextlib import asynccontextmanager
from typing import Optional
import logging

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest

from platformq_shared.security import get_current_user_from_trusted_header as get_current_user
from platformq_provisioning_common import (
    ProvisioningRequest,
    ProvisioningResult,
    ProvisioningStatus,
    TenantTier
)

from .config import settings
from .orchestrator import ProvisioningOrchestrator
from .api import router as api_router
from .event_processor import TenantProvisioningProcessor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Prometheus metrics
provisioning_counter = Counter(
    'tenant_provisioning_requests_total',
    'Total number of tenant provisioning requests',
    ['status']
)
provisioning_duration = Histogram(
    'tenant_provisioning_duration_seconds',
    'Duration of tenant provisioning operations'
)

# Global instances
orchestrator: Optional[ProvisioningOrchestrator] = None
event_processor: Optional[TenantProvisioningProcessor] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global orchestrator, event_processor
    
    # Initialize orchestrator
    orchestrator = ProvisioningOrchestrator(settings)
    await orchestrator.initialize()
    
    # Initialize event processor
    event_processor = TenantProvisioningProcessor(
        service_name=settings.service_name,
        pulsar_url=settings.pulsar_url,
        orchestrator=orchestrator
    )
    await event_processor.start()
    
    logger.info("Tenant Provisioning Service started")
    
    yield
    
    # Cleanup
    await event_processor.stop()
    await orchestrator.shutdown()
    
    logger.info("Tenant Provisioning Service stopped")


# Create FastAPI app
app = FastAPI(
    title="Tenant Provisioning Service",
    description="Orchestrates tenant resource provisioning",
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