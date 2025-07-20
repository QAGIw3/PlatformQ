"""API endpoints for Quota Management Service"""

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, HTTPException, Query, Depends, Body
from pydantic import BaseModel, Field

from platformq_resource_common import (
    ResourceQuota,
    ResourceUsage,
    ResourceType,
    QuotaAlert,
    QuotaStatus
)

from .quota_manager import QuotaManager, QuotaAction
from .repository import QuotaRepository

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/v1", tags=["quota-management"])

# Dependency injection
repository = QuotaRepository()
quota_manager = QuotaManager(repository)


# Request/Response models
class QuotaCheckRequest(BaseModel):
    """Request for quota check"""
    tenant_id: str
    resource_type: ResourceType
    requested_amount: float = Field(gt=0)


class QuotaCheckResponse(BaseModel):
    """Response for quota check"""
    action: str
    allowed: bool
    message: Optional[str] = None
    current_usage: float
    quota_limit: float
    percentage_used: float


class QuotaSetRequest(BaseModel):
    """Request for setting quota"""
    resource_type: ResourceType
    limit: float = Field(gt=0)
    period: Optional[str] = Field(default="monthly", pattern="^(hourly|daily|weekly|monthly|yearly)$")


class UsageUpdateRequest(BaseModel):
    """Request for updating usage"""
    resource_type: ResourceType
    delta: float
    operation: str = Field(pattern="^(increment|decrement)$")


class QuotaResponse(BaseModel):
    """Response for quota operations"""
    quota: ResourceQuota
    message: str


# Quota management endpoints
@router.post("/quota/check", response_model=QuotaCheckResponse)
async def check_quota(request: QuotaCheckRequest):
    """Check if resource allocation is allowed under quota"""
    try:
        action, message = await quota_manager.check_quota(
            tenant_id=request.tenant_id,
            resource_type=request.resource_type,
            requested_amount=request.requested_amount
        )
        
        # Get current status
        current_usage = await quota_manager.get_current_usage(
            request.tenant_id,
            request.resource_type
        )
        
        quota = await repository.get_quota(
            request.tenant_id,
            request.resource_type
        )
        
        quota_limit = quota.limit if quota else 0
        percentage_used = (
            ((current_usage + request.requested_amount) / quota_limit * 100)
            if quota_limit > 0 else 0
        )
        
        return QuotaCheckResponse(
            action=action.value,
            allowed=(action != QuotaAction.BLOCK),
            message=message,
            current_usage=current_usage,
            quota_limit=quota_limit,
            percentage_used=percentage_used
        )
        
    except Exception as e:
        logger.error(f"Error checking quota: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quotas/{tenant_id}")
async def get_quotas(tenant_id: str):
    """Get all quotas for tenant"""
    try:
        quotas = await repository.get_all_quotas(tenant_id)
        
        # Enrich with current usage
        enriched_quotas = []
        for quota in quotas:
            usage = await quota_manager.get_current_usage(
                tenant_id,
                quota.resource_type
            )
            
            enriched_quota = {
                "resource_type": quota.resource_type.value,
                "limit": quota.limit,
                "used": usage,
                "available": max(0, quota.limit - usage),
                "percentage_used": (usage / quota.limit * 100) if quota.limit > 0 else 0,
                "period": quota.period,
                "status": quota.status.value,
                "created_at": quota.created_at.isoformat(),
                "updated_at": quota.updated_at.isoformat()
            }
            enriched_quotas.append(enriched_quota)
            
        return {"quotas": enriched_quotas}
        
    except Exception as e:
        logger.error(f"Error getting quotas: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/quotas/{tenant_id}/{resource_type}")
async def get_quota(tenant_id: str, resource_type: ResourceType):
    """Get specific quota for tenant"""
    try:
        quota = await repository.get_quota(tenant_id, resource_type)
        if not quota:
            raise HTTPException(status_code=404, detail="Quota not found")
            
        # Get current usage
        usage = await quota_manager.get_current_usage(tenant_id, resource_type)
        
        return {
            "quota": {
                "resource_type": quota.resource_type.value,
                "limit": quota.limit,
                "used": usage,
                "available": max(0, quota.limit - usage),
                "percentage_used": (usage / quota.limit * 100) if quota.limit > 0 else 0,
                "period": quota.period,
                "status": quota.status.value,
                "created_at": quota.created_at.isoformat(),
                "updated_at": quota.updated_at.isoformat()
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting quota: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/quotas/{tenant_id}", response_model=QuotaResponse)
async def set_quota(tenant_id: str, request: QuotaSetRequest):
    """Set quota for tenant"""
    try:
        quota = await quota_manager.set_quota(
            tenant_id=tenant_id,
            resource_type=request.resource_type,
            limit=request.limit,
            period=request.period
        )
        
        return QuotaResponse(
            quota=quota,
            message=f"Quota set successfully for {request.resource_type.value}"
        )
        
    except Exception as e:
        logger.error(f"Error setting quota: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/quotas/{tenant_id}/{resource_type}")
async def update_quota(
    tenant_id: str,
    resource_type: ResourceType,
    limit: float = Body(..., gt=0)
):
    """Update existing quota"""
    try:
        # Check if quota exists
        existing = await repository.get_quota(tenant_id, resource_type)
        if not existing:
            raise HTTPException(status_code=404, detail="Quota not found")
            
        # Update quota
        quota = await quota_manager.set_quota(
            tenant_id=tenant_id,
            resource_type=resource_type,
            limit=limit,
            period=existing.period
        )
        
        return {
            "message": "Quota updated successfully",
            "quota": quota
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating quota: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Usage tracking endpoints
@router.get("/usage/{tenant_id}")
async def get_usage(tenant_id: str):
    """Get current resource usage for tenant"""
    try:
        # Get all resource types
        usage_data = []
        for resource_type in ResourceType:
            usage = await quota_manager.get_current_usage(tenant_id, resource_type)
            if usage > 0:
                usage_data.append({
                    "resource_type": resource_type.value,
                    "current_usage": usage
                })
                
        return {"usage": usage_data}
        
    except Exception as e:
        logger.error(f"Error getting usage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/usage/{tenant_id}/{resource_type}")
async def get_resource_usage(tenant_id: str, resource_type: ResourceType):
    """Get usage for specific resource type"""
    try:
        usage = await quota_manager.get_current_usage(tenant_id, resource_type)
        
        return {
            "resource_type": resource_type.value,
            "current_usage": usage
        }
        
    except Exception as e:
        logger.error(f"Error getting resource usage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/usage/{tenant_id}")
async def update_usage(tenant_id: str, request: UsageUpdateRequest):
    """Update resource usage (for manual adjustments)"""
    try:
        delta = request.delta
        if request.operation == "decrement":
            delta = -delta
            
        await quota_manager.update_usage(
            tenant_id=tenant_id,
            resource_type=request.resource_type,
            delta=delta
        )
        
        # Get updated usage
        new_usage = await quota_manager.get_current_usage(
            tenant_id,
            request.resource_type
        )
        
        return {
            "message": "Usage updated successfully",
            "resource_type": request.resource_type.value,
            "new_usage": new_usage
        }
        
    except Exception as e:
        logger.error(f"Error updating usage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/usage-history/{tenant_id}/{resource_type}")
async def get_usage_history(
    tenant_id: str,
    resource_type: ResourceType,
    hours: int = Query(default=24, ge=1, le=168)  # Max 7 days
):
    """Get usage history for resource"""
    try:
        history = await repository.get_usage_history(
            tenant_id=tenant_id,
            resource_type=resource_type,
            hours=hours
        )
        
        return {
            "resource_type": resource_type.value,
            "hours": hours,
            "history": history
        }
        
    except Exception as e:
        logger.error(f"Error getting usage history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Alert endpoints
@router.get("/alerts/{tenant_id}")
async def get_quota_alerts(
    tenant_id: str,
    days: int = Query(default=7, ge=1, le=30)
):
    """Get quota alerts for tenant"""
    try:
        alerts = await repository.get_quota_alerts(tenant_id, days)
        
        alert_data = []
        for alert in alerts:
            alert_data.append({
                "resource_type": alert.resource_type.value,
                "threshold_percentage": alert.threshold_percentage,
                "current_usage": alert.current_usage,
                "quota_limit": alert.quota_limit,
                "alert_type": alert.alert_type,
                "message": alert.message,
                "triggered_at": alert.triggered_at.isoformat()
            })
            
        return {
            "alerts": alert_data,
            "days": days,
            "count": len(alert_data)
        }
        
    except Exception as e:
        logger.error(f"Error getting alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Status endpoints
@router.get("/status/{tenant_id}")
async def get_quota_status(tenant_id: str):
    """Get comprehensive quota status for tenant"""
    try:
        status = await quota_manager.get_quota_status(tenant_id)
        return status
        
    except Exception as e:
        logger.error(f"Error getting quota status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Summary endpoints
@router.get("/summary/{tenant_id}")
async def get_quota_summary(tenant_id: str):
    """Get quota summary for tenant"""
    try:
        quotas = await repository.get_all_quotas(tenant_id)
        
        summary = {
            "tenant_id": tenant_id,
            "total_quotas": len(quotas),
            "resource_summary": [],
            "status_summary": {
                "ok": 0,
                "warning": 0,
                "exceeded": 0
            },
            "alerts_last_24h": 0
        }
        
        for quota in quotas:
            usage = await quota_manager.get_current_usage(
                tenant_id,
                quota.resource_type
            )
            percentage = (usage / quota.limit * 100) if quota.limit > 0 else 0
            
            summary["resource_summary"].append({
                "resource_type": quota.resource_type.value,
                "percentage_used": percentage,
                "status": quota.status.value
            })
            
            # Update status counts
            if quota.status == QuotaStatus.OK:
                summary["status_summary"]["ok"] += 1
            elif quota.status == QuotaStatus.WARNING:
                summary["status_summary"]["warning"] += 1
            elif quota.status == QuotaStatus.EXCEEDED:
                summary["status_summary"]["exceeded"] += 1
                
        # Get recent alerts
        alerts = await repository.get_quota_alerts(tenant_id, days=1)
        summary["alerts_last_24h"] = len(alerts)
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting quota summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Admin endpoints
@router.post("/initialize/{tenant_id}")
async def initialize_tenant_quotas(tenant_id: str):
    """Initialize default quotas for new tenant"""
    try:
        initialized = []
        
        for resource_type in ResourceType:
            # Check if quota already exists
            existing = await repository.get_quota(tenant_id, resource_type)
            if not existing:
                quota = await quota_manager._create_default_quota(
                    tenant_id,
                    resource_type
                )
                initialized.append(resource_type.value)
                
        return {
            "message": "Tenant quotas initialized",
            "initialized_resources": initialized
        }
        
    except Exception as e:
        logger.error(f"Error initializing tenant quotas: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 