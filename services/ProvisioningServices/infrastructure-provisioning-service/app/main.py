"""
Infrastructure Provisioning Service

Handles provisioning of infrastructure resources like Cassandra, ElasticSearch, 
Ignite, MinIO, Pulsar, Consul, Vault, JanusGraph, etc.
"""
import asyncio
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from platformq_resource_common import (
    ResourceType, InfrastructureResource, ProvisioningRequest,
    ProvisioningResult, ProvisioningStatus
)

from .config import Settings
from .orchestrator import InfrastructureOrchestrator
from .repository import InfrastructureRepository
from .event_processor import InfrastructureEventProcessor

# Initialize settings
settings = Settings()

# Global instances
orchestrator: Optional[InfrastructureOrchestrator] = None
repository: Optional[InfrastructureRepository] = None
event_processor: Optional[InfrastructureEventProcessor] = None


class ProvisionRequest(BaseModel):
    """Request to provision infrastructure resources"""
    tenant_id: str
    tenant_name: str
    resources: List[ResourceType] = Field(
        description="List of infrastructure resources to provision"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DeprovisionRequest(BaseModel):
    """Request to deprovision infrastructure resources"""
    tenant_id: str
    resources: Optional[List[ResourceType]] = Field(
        default=None,
        description="Specific resources to deprovision. If None, deprovision all."
    )
    force: bool = Field(default=False, description="Force deprovision even if in use")


class CleanupRequest(BaseModel):
    """Request to cleanup orphaned resources"""
    tenant_id: Optional[str] = Field(
        default=None,
        description="Tenant to cleanup. If None, cleanup all tenants."
    )
    resource_types: Optional[List[ResourceType]] = Field(
        default=None,
        description="Resource types to cleanup. If None, cleanup all types."
    )
    dry_run: bool = Field(default=True, description="Only report what would be cleaned")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global orchestrator, repository, event_processor
    
    # Initialize repository
    repository = InfrastructureRepository(settings)
    await repository.initialize()
    
    # Initialize orchestrator
    orchestrator = InfrastructureOrchestrator(settings, repository)
    await orchestrator.initialize()
    
    # Initialize event processor
    event_processor = InfrastructureEventProcessor(
        service_name=settings.service_name,
        pulsar_url=settings.pulsar_url,
        orchestrator=orchestrator,
        repository=repository
    )
    await event_processor.start()
    
    yield
    
    # Cleanup
    if event_processor:
        await event_processor.stop()
    if orchestrator:
        await orchestrator.shutdown()
    if repository:
        await repository.close()


# Create FastAPI app
app = FastAPI(
    title="Infrastructure Provisioning Service",
    description="Service for provisioning infrastructure resources in Platform Q",
    version=settings.api_version,
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


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": settings.service_name,
        "version": settings.api_version
    }


@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint"""
    if not orchestrator or not repository:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not ready"
        )
    
    return {"status": "ready"}


@app.post("/api/v1/infrastructure/provision", response_model=ProvisioningResult)
async def provision_infrastructure(request: ProvisionRequest):
    """Provision infrastructure resources for a tenant"""
    try:
        provisioning_request = ProvisioningRequest(
            tenant_id=request.tenant_id,
            tenant_name=request.tenant_name,
            resources=request.resources,
            metadata=request.metadata,
            requested_by="infrastructure-service"
        )
        
        result = await orchestrator.provision_resources(provisioning_request)
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to provision infrastructure: {str(e)}"
        )


@app.post("/api/v1/infrastructure/deprovision", response_model=ProvisioningResult)
async def deprovision_infrastructure(request: DeprovisionRequest):
    """Deprovision infrastructure resources for a tenant"""
    try:
        result = await orchestrator.deprovision_resources(
            tenant_id=request.tenant_id,
            resources=request.resources,
            force=request.force
        )
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to deprovision infrastructure: {str(e)}"
        )


@app.get("/api/v1/infrastructure/{tenant_id}", response_model=List[InfrastructureResource])
async def get_tenant_infrastructure(tenant_id: str):
    """Get all infrastructure resources for a tenant"""
    try:
        resources = await repository.get_tenant_resources(tenant_id)
        return resources
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get infrastructure: {str(e)}"
        )


@app.get("/api/v1/infrastructure/{tenant_id}/{resource_type}")
async def get_infrastructure_resource(tenant_id: str, resource_type: ResourceType):
    """Get specific infrastructure resource for a tenant"""
    try:
        resource = await repository.get_resource(tenant_id, resource_type)
        if not resource:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Resource {resource_type} not found for tenant {tenant_id}"
            )
        return resource
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get resource: {str(e)}"
        )


@app.post("/api/v1/infrastructure/validate/{tenant_id}")
async def validate_infrastructure(
    tenant_id: str,
    resources: Optional[List[ResourceType]] = None
):
    """Validate infrastructure resources for a tenant"""
    try:
        validation_results = await orchestrator.validate_resources(tenant_id, resources)
        
        all_valid = all(result["valid"] for result in validation_results.values())
        
        return {
            "tenant_id": tenant_id,
            "valid": all_valid,
            "results": validation_results
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to validate infrastructure: {str(e)}"
        )


@app.post("/api/v1/infrastructure/cleanup")
async def cleanup_resources(request: CleanupRequest):
    """Cleanup orphaned infrastructure resources"""
    try:
        result = await orchestrator.cleanup_orphaned_resources(
            tenant_id=request.tenant_id,
            resource_types=request.resource_types,
            dry_run=request.dry_run
        )
        
        return {
            "dry_run": request.dry_run,
            "cleaned_resources": result["cleaned"] if not request.dry_run else [],
            "orphaned_resources": result["orphaned"],
            "errors": result["errors"]
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cleanup resources: {str(e)}"
        )


@app.get("/api/v1/provisioners")
async def list_provisioners():
    """List available infrastructure provisioners"""
    try:
        provisioners = orchestrator.get_available_provisioners()
        return {
            "provisioners": [
                {
                    "resource_type": p.get_resource_type().value,
                    "status": "active"
                }
                for p in provisioners
            ]
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list provisioners: {str(e)}"
        )


@app.get("/api/v1/infrastructure/status/{request_id}")
async def get_provisioning_status(request_id: str):
    """Get status of a provisioning request"""
    try:
        result = await repository.get_provisioning_result(request_id)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Provisioning request {request_id} not found"
            )
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get provisioning status: {str(e)}"
        )


@app.get("/metrics")
async def get_metrics():
    """Get service metrics in Prometheus format"""
    # TODO: Implement Prometheus metrics
    return "# Infrastructure Provisioning Service Metrics\n" 