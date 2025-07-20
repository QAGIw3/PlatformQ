"""API endpoints for Tenant Provisioning Service"""

from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query

from platformq_shared.security import get_current_user_from_trusted_header as get_current_user
from platformq_provisioning_common import (
    ProvisioningRequest,
    ProvisioningResult,
    ProvisioningStatus,
    TenantTier,
    ResourceType
)

from .main import orchestrator, provisioning_counter, provisioning_duration

router = APIRouter()


@router.post("/tenants/provision", response_model=ProvisioningResult)
async def provision_tenant(
    tenant_id: str,
    tenant_name: str,
    tier: TenantTier = TenantTier.STARTER,
    resources: Optional[List[ResourceType]] = None,
    current_user=Depends(get_current_user)
):
    """Provision resources for a new tenant"""
    if not orchestrator:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    # Create provisioning request
    request = ProvisioningRequest(
        tenant_id=tenant_id,
        tenant_name=tenant_name,
        tier=tier,
        requested_by=current_user.get("user_id", "system"),
        resources_to_provision=resources,
        metadata={
            "user_email": current_user.get("email"),
            "organization": current_user.get("organization")
        }
    )
    
    # Track metrics
    with provisioning_duration.time():
        try:
            result = await orchestrator.provision_tenant(request)
            
            if result.status == ProvisioningStatus.COMPLETED:
                provisioning_counter.labels(status="success").inc()
            elif result.status == ProvisioningStatus.PARTIALLY_COMPLETED:
                provisioning_counter.labels(status="partial").inc()
            else:
                provisioning_counter.labels(status="failed").inc()
            
            return result
            
        except Exception as e:
            provisioning_counter.labels(status="error").inc()
            raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tenants/{tenant_id}/deprovision", response_model=ProvisioningResult)
async def deprovision_tenant(
    tenant_id: str,
    current_user=Depends(get_current_user)
):
    """Deprovision all resources for a tenant"""
    if not orchestrator:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    # Check authorization
    if current_user.get("role") not in ["admin", "platform_admin"]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    try:
        result = await orchestrator.deprovision_tenant(tenant_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/provisioning/{request_id}", response_model=ProvisioningResult)
async def get_provisioning_status(
    request_id: str,
    current_user=Depends(get_current_user)
):
    """Get the status of a provisioning request"""
    if not orchestrator:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    result = await orchestrator.get_provisioning_status(request_id)
    if not result:
        raise HTTPException(status_code=404, detail="Request not found")
    
    return result


@router.post("/provisioning/{request_id}/retry", response_model=ProvisioningResult)
async def retry_provisioning(
    request_id: str,
    current_user=Depends(get_current_user)
):
    """Retry provisioning of failed resources"""
    if not orchestrator:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    try:
        result = await orchestrator.retry_failed_resources(request_id)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/provisioners")
async def list_provisioners():
    """List available resource provisioners"""
    if not orchestrator:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    provisioners = []
    for resource_type, provisioner in orchestrator.provisioners.items():
        provisioners.append({
            "resource_type": resource_type.value,
            "provisioner_class": provisioner.__class__.__name__,
            "enabled": True
        })
    
    return {"provisioners": provisioners}


@router.get("/tiers")
async def list_tiers():
    """List available tenant tiers"""
    tiers = []
    for tier in TenantTier:
        tiers.append({
            "tier": tier.value,
            "name": tier.value.title(),
            "description": f"{tier.value.title()} tier with standard resources"
        })
    
    return {"tiers": tiers} 