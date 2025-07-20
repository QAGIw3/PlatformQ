"""
User Provisioning Service

Handles provisioning of users across various platform services.
"""
import asyncio
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException, Depends, status, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, EmailStr

from .config import Settings
from .orchestrator import UserProvisioningOrchestrator
from .event_processor import UserProvisioningEventProcessor

# Initialize settings
settings = Settings()

# Global instances
orchestrator: Optional[UserProvisioningOrchestrator] = None
event_processor: Optional[UserProvisioningEventProcessor] = None


class UserProvisionRequest(BaseModel):
    """Request to provision a user"""
    user_id: str
    username: str
    email: EmailStr
    full_name: str
    tenant_id: str
    roles: List[str] = Field(default_factory=list)
    groups: List[str] = Field(default_factory=list)
    services: List[str] = Field(
        default_factory=list,
        description="Services to provision user in. Empty means all available."
    )
    metadata: Dict[str, Any] = Field(default_factory=dict)


class BulkUserProvisionRequest(BaseModel):
    """Request to provision multiple users"""
    tenant_id: str
    users: List[UserProvisionRequest]
    batch_size: int = Field(default=10, ge=1, le=100)


class UserDeprovisionRequest(BaseModel):
    """Request to deprovision a user"""
    user_id: str
    tenant_id: str
    services: List[str] = Field(
        default_factory=list,
        description="Services to deprovision user from. Empty means all."
    )
    delete_data: bool = Field(
        default=False,
        description="Whether to delete user data or just disable access"
    )


class UserProvisioningResult(BaseModel):
    """Result of user provisioning operation"""
    user_id: str
    success: bool
    provisioned_services: List[str] = Field(default_factory=list)
    failed_services: List[Dict[str, str]] = Field(default_factory=list)
    message: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None


class BulkProvisioningResult(BaseModel):
    """Result of bulk user provisioning"""
    total_users: int
    successful: int
    failed: int
    results: List[UserProvisioningResult]


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global orchestrator, event_processor
    
    # Initialize orchestrator
    orchestrator = UserProvisioningOrchestrator(settings)
    await orchestrator.initialize()
    
    # Initialize event processor
    event_processor = UserProvisioningEventProcessor(
        service_name=settings.service_name,
        pulsar_url=settings.pulsar_url,
        orchestrator=orchestrator
    )
    await event_processor.start()
    
    yield
    
    # Cleanup
    if event_processor:
        await event_processor.stop()
    if orchestrator:
        await orchestrator.shutdown()


# Create FastAPI app
app = FastAPI(
    title="User Provisioning Service",
    description="Service for provisioning users across Platform Q services",
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
    if not orchestrator:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service not ready"
        )
    
    return {"status": "ready"}


@app.post("/api/v1/users/provision", response_model=UserProvisioningResult)
async def provision_user(request: UserProvisionRequest):
    """Provision a single user across services"""
    try:
        result = await orchestrator.provision_user(
            user_id=request.user_id,
            username=request.username,
            email=request.email,
            full_name=request.full_name,
            tenant_id=request.tenant_id,
            roles=request.roles,
            groups=request.groups,
            services=request.services,
            metadata=request.metadata
        )
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to provision user: {str(e)}"
        )


@app.post("/api/v1/users/provision/bulk", response_model=BulkProvisioningResult)
async def provision_users_bulk(
    request: BulkUserProvisionRequest,
    background_tasks: BackgroundTasks
):
    """Provision multiple users in bulk"""
    try:
        # Start bulk provisioning in background
        result = await orchestrator.provision_users_bulk(
            tenant_id=request.tenant_id,
            users=[user.dict() for user in request.users],
            batch_size=request.batch_size
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to provision users: {str(e)}"
        )


@app.post("/api/v1/users/deprovision", response_model=UserProvisioningResult)
async def deprovision_user(request: UserDeprovisionRequest):
    """Deprovision a user from services"""
    try:
        result = await orchestrator.deprovision_user(
            user_id=request.user_id,
            tenant_id=request.tenant_id,
            services=request.services,
            delete_data=request.delete_data
        )
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to deprovision user: {str(e)}"
        )


@app.get("/api/v1/users/{tenant_id}/{user_id}/status")
async def get_user_provisioning_status(tenant_id: str, user_id: str):
    """Get provisioning status for a user"""
    try:
        status = await orchestrator.get_user_status(tenant_id, user_id)
        
        if not status:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User {user_id} not found in tenant {tenant_id}"
            )
        
        return status
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get user status: {str(e)}"
        )


@app.post("/api/v1/users/{tenant_id}/{user_id}/sync")
async def sync_user(tenant_id: str, user_id: str):
    """Synchronize user across all services"""
    try:
        result = await orchestrator.sync_user(tenant_id, user_id)
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync user: {str(e)}"
        )


@app.put("/api/v1/users/{tenant_id}/{user_id}/roles")
async def update_user_roles(
    tenant_id: str,
    user_id: str,
    roles: List[str]
):
    """Update user roles across services"""
    try:
        result = await orchestrator.update_user_roles(
            tenant_id=tenant_id,
            user_id=user_id,
            roles=roles
        )
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update user roles: {str(e)}"
        )


@app.put("/api/v1/users/{tenant_id}/{user_id}/groups")
async def update_user_groups(
    tenant_id: str,
    user_id: str,
    groups: List[str]
):
    """Update user groups across services"""
    try:
        result = await orchestrator.update_user_groups(
            tenant_id=tenant_id,
            user_id=user_id,
            groups=groups
        )
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update user groups: {str(e)}"
        )


@app.post("/api/v1/users/{tenant_id}/{user_id}/reset-password")
async def reset_user_password(tenant_id: str, user_id: str):
    """Reset user password across services"""
    try:
        result = await orchestrator.reset_user_password(tenant_id, user_id)
        return result
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to reset user password: {str(e)}"
        )


@app.get("/api/v1/provisioners")
async def list_provisioners():
    """List available user provisioners"""
    try:
        provisioners = orchestrator.get_available_provisioners()
        return {"provisioners": provisioners}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list provisioners: {str(e)}"
        )


@app.get("/metrics")
async def get_metrics():
    """Get service metrics in Prometheus format"""
    # TODO: Implement Prometheus metrics
    return "# User Provisioning Service Metrics\n" 