"""Platform Service Broker API

Implements Open Service Broker API v2.16 for Platform Q cloud brokerage.
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional

from fastapi import FastAPI, HTTPException, Header, Query, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from .models.osb_models import (
    CatalogResponse, ProvisionRequest, UpdateRequest,
    BindRequest, ErrorResponse
)
from .brokers.openstack_broker import OpenStackBroker
from .brokers.platformq_broker import PlatformQBroker
from .config import Settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global broker instances
openstack_broker: Optional[OpenStackBroker] = None
platformq_broker: Optional[PlatformQBroker] = None
settings: Optional[Settings] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global openstack_broker, platformq_broker, settings
    
    # Load configuration
    settings = Settings()
    
    # Initialize brokers
    config = settings.get_broker_config()
    
    if settings.enable_openstack_broker:
        openstack_broker = OpenStackBroker(config)
        logger.info("OpenStack broker initialized")
    
    if settings.enable_platform_broker:
        platformq_broker = PlatformQBroker(config)
        logger.info("Platform Q broker initialized")
    
    logger.info("Platform Service Broker started")
    
    yield
    
    # Cleanup
    logger.info("Platform Service Broker stopped")


app = FastAPI(
    title="Platform Q Service Broker",
    description="Open Service Broker API implementation for Platform Q cloud brokerage",
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


# OSB API version header
OSB_API_VERSION_HEADER = "X-Broker-Api-Version"
SUPPORTED_OSB_VERSION = "2.16"


def verify_osb_version(x_broker_api_version: Optional[str] = Header(None)):
    """Verify OSB API version header"""
    if not x_broker_api_version:
        raise HTTPException(
            status_code=400,
            detail="Missing required header: X-Broker-API-Version"
        )
    
    # Extract major.minor version
    try:
        major, minor = x_broker_api_version.split(".")[:2]
        version = float(f"{major}.{minor}")
        
        if version < 2.13:
            raise HTTPException(
                status_code=412,
                detail=f"Unsupported API version: {x_broker_api_version}. Minimum supported: 2.13"
            )
    except:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid API version format: {x_broker_api_version}"
        )


def get_broker(service_id: str):
    """Get appropriate broker based on service ID"""
    # OpenStack services
    if service_id.startswith("openstack-"):
        if not openstack_broker:
            raise HTTPException(status_code=503, detail="OpenStack broker not available")
        return openstack_broker
    # Platform Q native services
    elif service_id in [
        "cassandra-service", "ignite-service", "pulsar-service",
        "minio-service", "elasticsearch-service", "janusgraph-service",
        "platformq-bundle"
    ]:
        if not platformq_broker:
            raise HTTPException(status_code=503, detail="Platform Q broker not available")
        return platformq_broker
    else:
        raise HTTPException(status_code=400, detail=f"Unknown service type: {service_id}")


# Health check endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "platform-service-broker"}


@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint"""
    ready = True
    details = {}
    
    if settings.enable_openstack_broker:
        # Check OpenStack connectivity
        try:
            # Simplified check - in production, verify actual connection
            details["openstack"] = "ready"
        except:
            details["openstack"] = "not ready"
            ready = False
    
    if settings.enable_platform_broker:
        details["platform_services"] = "ready"
    
    return {
        "ready": ready,
        "details": details
    }


# OSB API Endpoints

@app.get("/v2/catalog", response_model=CatalogResponse)
async def get_catalog(
    x_broker_api_version: str = Header(...)
):
    """Get service broker catalog"""
    verify_osb_version(x_broker_api_version)
    
    catalogs = []
    
    if openstack_broker:
        openstack_catalog = await openstack_broker.catalog()
        catalogs.extend(openstack_catalog.services)
    
    if platformq_broker:
        platformq_catalog = await platformq_broker.catalog()
        catalogs.extend(platformq_catalog.services)
    
    return CatalogResponse(services=catalogs)


@app.put("/v2/service_instances/{instance_id}")
async def provision_service_instance(
    instance_id: str,
    request: ProvisionRequest,
    accepts_incomplete: bool = Query(default=False),
    x_broker_api_version: str = Header(...)
):
    """Provision a service instance"""
    verify_osb_version(x_broker_api_version)
    
    broker = get_broker(request.service_id)
    response, status_code = await broker.provision(
        instance_id,
        request,
        accepts_incomplete
    )
    
    return JSONResponse(
        status_code=status_code,
        content=response.dict(exclude_none=True)
    )


@app.patch("/v2/service_instances/{instance_id}")
async def update_service_instance(
    instance_id: str,
    request: UpdateRequest,
    accepts_incomplete: bool = Query(default=False),
    x_broker_api_version: str = Header(...)
):
    """Update a service instance"""
    verify_osb_version(x_broker_api_version)
    
    broker = get_broker(request.service_id)
    response, status_code = await broker.update(
        instance_id,
        request,
        accepts_incomplete
    )
    
    return JSONResponse(
        status_code=status_code,
        content=response.dict(exclude_none=True)
    )


@app.delete("/v2/service_instances/{instance_id}")
async def deprovision_service_instance(
    instance_id: str,
    service_id: str = Query(...),
    plan_id: str = Query(...),
    accepts_incomplete: bool = Query(default=False),
    x_broker_api_version: str = Header(...)
):
    """Deprovision a service instance"""
    verify_osb_version(x_broker_api_version)
    
    broker = get_broker(service_id)
    response, status_code = await broker.deprovision(
        instance_id,
        service_id,
        plan_id,
        accepts_incomplete
    )
    
    return JSONResponse(
        status_code=status_code,
        content=response.dict(exclude_none=True) if response else {}
    )


@app.put("/v2/service_instances/{instance_id}/service_bindings/{binding_id}")
async def create_service_binding(
    instance_id: str,
    binding_id: str,
    request: BindRequest,
    accepts_incomplete: bool = Query(default=False),
    x_broker_api_version: str = Header(...)
):
    """Create a service binding"""
    verify_osb_version(x_broker_api_version)
    
    broker = get_broker(request.service_id)
    response, status_code = await broker.bind(
        instance_id,
        binding_id,
        request,
        accepts_incomplete
    )
    
    return JSONResponse(
        status_code=status_code,
        content=response.dict(exclude_none=True)
    )


@app.delete("/v2/service_instances/{instance_id}/service_bindings/{binding_id}")
async def remove_service_binding(
    instance_id: str,
    binding_id: str,
    service_id: str = Query(...),
    plan_id: str = Query(...),
    accepts_incomplete: bool = Query(default=False),
    x_broker_api_version: str = Header(...)
):
    """Remove a service binding"""
    verify_osb_version(x_broker_api_version)
    
    broker = get_broker(service_id)
    response, status_code = await broker.unbind(
        instance_id,
        binding_id,
        service_id,
        plan_id,
        accepts_incomplete
    )
    
    return JSONResponse(
        status_code=status_code,
        content=response.dict(exclude_none=True) if response else {}
    )


@app.get("/v2/service_instances/{instance_id}/last_operation")
async def get_last_operation(
    instance_id: str,
    service_id: Optional[str] = Query(default=None),
    plan_id: Optional[str] = Query(default=None),
    operation: Optional[str] = Query(default=None),
    x_broker_api_version: str = Header(...)
):
    """Get last operation status"""
    verify_osb_version(x_broker_api_version)
    
    # Need to determine which broker to use
    # In practice, you'd track this in a database
    if service_id:
        broker = get_broker(service_id)
    else:
        # Try to find the broker that knows about this instance
        # This is simplified - in production, use persistent storage
        broker = openstack_broker or platformq_broker
    
    response, status_code = await broker.last_operation(
        instance_id,
        service_id,
        plan_id,
        operation
    )
    
    return JSONResponse(
        status_code=status_code,
        content=response.dict(exclude_none=True)
    )


@app.get("/v2/service_instances/{instance_id}")
async def get_service_instance(
    instance_id: str,
    service_id: Optional[str] = Query(default=None),
    plan_id: Optional[str] = Query(default=None),
    x_broker_api_version: str = Header(...)
):
    """Get service instance (optional OSB feature)"""
    verify_osb_version(x_broker_api_version)
    
    if service_id:
        broker = get_broker(service_id)
    else:
        broker = openstack_broker or platformq_broker
    
    response, status_code = await broker.get_instance(
        instance_id,
        service_id,
        plan_id
    )
    
    return JSONResponse(
        status_code=status_code,
        content=response.dict(exclude_none=True)
    )


@app.get("/v2/service_instances/{instance_id}/service_bindings/{binding_id}")
async def get_service_binding(
    instance_id: str,
    binding_id: str,
    service_id: Optional[str] = Query(default=None),
    plan_id: Optional[str] = Query(default=None),
    x_broker_api_version: str = Header(...)
):
    """Get service binding (optional OSB feature)"""
    verify_osb_version(x_broker_api_version)
    
    if service_id:
        broker = get_broker(service_id)
    else:
        broker = openstack_broker or platformq_broker
    
    response, status_code = await broker.get_binding(
        instance_id,
        binding_id,
        service_id,
        plan_id
    )
    
    return JSONResponse(
        status_code=status_code,
        content=response.dict(exclude_none=True)
    )


# Error handler
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Convert exceptions to OSB error format"""
    error_response = ErrorResponse(
        error=f"Error-{exc.status_code}",
        description=exc.detail
    )
    
    return JSONResponse(
        status_code=exc.status_code,
        content=error_response.dict(exclude_none=True)
    )


# Metrics endpoint (Prometheus format)
@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    # This would integrate with prometheus_client
    # For now, return basic metrics
    metrics = """
# HELP osb_catalog_requests_total Total catalog requests
# TYPE osb_catalog_requests_total counter
osb_catalog_requests_total 0

# HELP osb_provision_requests_total Total provision requests
# TYPE osb_provision_requests_total counter
osb_provision_requests_total 0

# HELP osb_deprovision_requests_total Total deprovision requests  
# TYPE osb_deprovision_requests_total counter
osb_deprovision_requests_total 0

# HELP osb_bind_requests_total Total bind requests
# TYPE osb_bind_requests_total counter
osb_bind_requests_total 0

# HELP osb_unbind_requests_total Total unbind requests
# TYPE osb_unbind_requests_total counter
osb_unbind_requests_total 0
"""
    return Response(content=metrics, media_type="text/plain")


if __name__ == "__main__":
    port = int(os.getenv("SERVICE_PORT", "8080"))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=True
    ) 