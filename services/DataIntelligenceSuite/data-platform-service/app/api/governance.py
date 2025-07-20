"""
Data governance API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Body
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from enum import Enum
import uuid
import logging

from .. import main

router = APIRouter()
logger = logging.getLogger(__name__)


class PolicyType(str, Enum):
    ACCESS = "access"
    RETENTION = "retention"
    QUALITY = "quality"
    PRIVACY = "privacy"
    COMPLIANCE = "compliance"


class PolicyAction(str, Enum):
    ALLOW = "allow"
    DENY = "deny"
    MASK = "mask"
    ENCRYPT = "encrypt"
    AUDIT = "audit"
    NOTIFY = "notify"


class ComplianceStandard(str, Enum):
    GDPR = "gdpr"
    CCPA = "ccpa"
    HIPAA = "hipaa"
    SOC2 = "soc2"
    PCI_DSS = "pci_dss"
    ISO_27001 = "iso_27001"


class PolicyRule(BaseModel):
    """Policy rule definition"""
    condition: str = Field(..., description="Rule condition expression")
    action: PolicyAction
    parameters: Optional[Dict[str, Any]] = Field(None)
    priority: int = Field(100, ge=0, le=1000)


class PolicyCreate(BaseModel):
    """Create policy request"""
    name: str = Field(..., description="Policy name")
    description: Optional[str] = Field(None)
    policy_type: PolicyType
    rules: List[PolicyRule]
    scope: Dict[str, Any] = Field(..., description="Policy scope (assets, users, etc.)")
    enabled: bool = Field(True)
    tags: List[str] = Field(default_factory=list)


class PolicyResponse(PolicyCreate):
    """Policy response"""
    policy_id: str
    created_at: datetime
    updated_at: datetime
    created_by: str
    version: int
    applied_count: int = 0


class AccessRequest(BaseModel):
    """Data access request"""
    asset_id: str = Field(..., description="Asset being requested")
    purpose: str = Field(..., description="Access purpose")
    duration_days: int = Field(30, ge=1, le=365)
    justification: str = Field(..., description="Business justification")
    data_usage: List[str] = Field(..., description="How data will be used")


class AccessRequestResponse(AccessRequest):
    """Access request response"""
    request_id: str
    status: str
    requester: str
    created_at: datetime
    approved_at: Optional[datetime]
    expires_at: Optional[datetime]
    approver: Optional[str]
    denial_reason: Optional[str]


class DataClassificationRequest(BaseModel):
    """Data classification request"""
    asset_id: str
    classification_level: str
    justification: str
    sensitivity_tags: List[str] = Field(default_factory=list)


# In-memory storage for policies (would be database in production)
policies_store: Dict[str, Dict[str, Any]] = {}
access_requests_store: Dict[str, Dict[str, Any]] = {}


@router.post("/policies", response_model=PolicyResponse)
async def create_policy(
    policy: PolicyCreate,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Create a new governance policy"""
    try:
        policy_id = str(uuid.uuid4())
        
        # Store policy
        policy_data = {
            **policy.dict(),
            "policy_id": policy_id,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
            "created_by": tenant_id,
            "version": 1,
            "applied_count": 0
        }
        
        policies_store[policy_id] = policy_data
        
        return PolicyResponse(**policy_data)
    except Exception as e:
        logger.error(f"Failed to create policy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/policies", response_model=List[PolicyResponse])
async def list_policies(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    policy_type: Optional[PolicyType] = Query(None),
    enabled: Optional[bool] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List governance policies"""
    try:
        # Filter policies
        filtered_policies = []
        for policy_data in policies_store.values():
            if policy_type and policy_data["policy_type"] != policy_type.value:
                continue
            if enabled is not None and policy_data["enabled"] != enabled:
                continue
            filtered_policies.append(PolicyResponse(**policy_data))
        
        # Apply pagination
        start = offset
        end = offset + limit
        return filtered_policies[start:end]
    except Exception as e:
        logger.error(f"Failed to list policies: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/policies/{policy_id}", response_model=PolicyResponse)
async def get_policy(
    policy_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get policy details"""
    if policy_id not in policies_store:
        raise HTTPException(status_code=404, detail="Policy not found")
    
    return PolicyResponse(**policies_store[policy_id])


@router.put("/policies/{policy_id}", response_model=PolicyResponse)
async def update_policy(
    policy_id: str,
    policy: PolicyCreate,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Update a policy"""
    if policy_id not in policies_store:
        raise HTTPException(status_code=404, detail="Policy not found")
    
    try:
        # Update policy
        existing_policy = policies_store[policy_id]
        updated_policy = {
            **policy.dict(),
            "policy_id": policy_id,
            "created_at": existing_policy["created_at"],
            "updated_at": datetime.utcnow(),
            "created_by": existing_policy["created_by"],
            "version": existing_policy["version"] + 1,
            "applied_count": existing_policy["applied_count"]
        }
        
        policies_store[policy_id] = updated_policy
        
        return PolicyResponse(**updated_policy)
    except Exception as e:
        logger.error(f"Failed to update policy: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/policies/{policy_id}")
async def delete_policy(
    policy_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Delete a policy"""
    if policy_id not in policies_store:
        raise HTTPException(status_code=404, detail="Policy not found")
    
    del policies_store[policy_id]
    
    return {"message": f"Policy {policy_id} deleted successfully"}


@router.post("/access-requests", response_model=AccessRequestResponse)
async def request_access(
    access_request: AccessRequest,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Request access to a data asset"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        # Verify asset exists
        asset = await main.catalog_manager.get_asset(access_request.asset_id)
        if not asset:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        request_id = str(uuid.uuid4())
        
        # Store access request
        request_data = {
            **access_request.dict(),
            "request_id": request_id,
            "status": "pending",
            "requester": tenant_id,
            "created_at": datetime.utcnow(),
            "approved_at": None,
            "expires_at": None,
            "approver": None,
            "denial_reason": None
        }
        
        access_requests_store[request_id] = request_data
        
        return AccessRequestResponse(**request_data)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create access request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/access-requests", response_model=List[AccessRequestResponse])
async def list_access_requests(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    status: Optional[str] = Query(None),
    requester: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List access requests"""
    try:
        # Filter requests
        filtered_requests = []
        for request_data in access_requests_store.values():
            if status and request_data["status"] != status:
                continue
            if requester and request_data["requester"] != requester:
                continue
            filtered_requests.append(AccessRequestResponse(**request_data))
        
        # Sort by created_at descending
        filtered_requests.sort(key=lambda x: x.created_at, reverse=True)
        
        # Apply pagination
        start = offset
        end = offset + limit
        return filtered_requests[start:end]
    except Exception as e:
        logger.error(f"Failed to list access requests: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/access-requests/{request_id}/approve")
async def approve_access_request(
    request_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    expiration_override_days: Optional[int] = Query(None)
):
    """Approve an access request"""
    if request_id not in access_requests_store:
        raise HTTPException(status_code=404, detail="Access request not found")
    
    try:
        request_data = access_requests_store[request_id]
        
        if request_data["status"] != "pending":
            raise HTTPException(status_code=400, detail="Request already processed")
        
        # Update request
        duration = expiration_override_days or request_data["duration_days"]
        request_data.update({
            "status": "approved",
            "approved_at": datetime.utcnow(),
            "expires_at": datetime.utcnow() + timedelta(days=duration),
            "approver": tenant_id
        })
        
        return {"message": f"Access request {request_id} approved"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to approve access request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/access-requests/{request_id}/deny")
async def deny_access_request(
    request_id: str,
    request: Request,
    reason: str = Query(..., description="Denial reason"),
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Deny an access request"""
    if request_id not in access_requests_store:
        raise HTTPException(status_code=404, detail="Access request not found")
    
    try:
        request_data = access_requests_store[request_id]
        
        if request_data["status"] != "pending":
            raise HTTPException(status_code=400, detail="Request already processed")
        
        # Update request
        request_data.update({
            "status": "denied",
            "denial_reason": reason,
            "approver": tenant_id
        })
        
        return {"message": f"Access request {request_id} denied"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to deny access request: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/assets/{asset_id}/classify")
async def classify_asset(
    asset_id: str,
    classification: DataClassificationRequest,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Classify a data asset"""
    if not main.catalog_manager:
        raise HTTPException(status_code=503, detail="Catalog manager not available")
    
    try:
        # Update asset classification
        updates = {
            "classification": classification.classification_level,
            "sensitivity_tags": classification.sensitivity_tags,
            "classification_justification": classification.justification,
            "classified_at": datetime.utcnow(),
            "classified_by": tenant_id
        }
        
        result = await main.catalog_manager.update_asset(asset_id, updates)
        if not result:
            raise HTTPException(status_code=404, detail="Asset not found")
        
        return {
            "asset_id": asset_id,
            "classification": classification.classification_level,
            "sensitivity_tags": classification.sensitivity_tags,
            "status": "classified"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to classify asset: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/compliance/status")
async def get_compliance_status(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    standard: Optional[ComplianceStandard] = Query(None)
):
    """Get compliance status"""
    # Mock compliance status
    compliance_status = {
        "overall_score": 0.85,
        "last_assessment": datetime.utcnow() - timedelta(days=7),
        "standards": {
            "gdpr": {"score": 0.90, "violations": 2, "status": "compliant"},
            "ccpa": {"score": 0.88, "violations": 1, "status": "compliant"},
            "hipaa": {"score": 0.82, "violations": 3, "status": "needs_attention"}
        },
        "pending_actions": 5,
        "recent_violations": []
    }
    
    if standard:
        return {
            "standard": standard.value,
            **compliance_status["standards"].get(standard.value, {})
        }
    
    return compliance_status


@router.get("/compliance/findings")
async def list_compliance_findings(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    standard: Optional[ComplianceStandard] = Query(None),
    severity: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List compliance findings"""
    return {
        "findings": [
            {
                "finding_id": "finding_123",
                "standard": "gdpr",
                "severity": "medium",
                "title": "Data retention policy not enforced",
                "description": "Some data assets exceed retention period",
                "affected_assets": ["asset_456", "asset_789"],
                "remediation": "Implement automated retention enforcement",
                "status": "open",
                "detected_at": datetime.utcnow() - timedelta(days=5)
            }
        ],
        "total": 1
    }


@router.post("/compliance/remediate/{finding_id}")
async def remediate_finding(
    finding_id: str,
    request: Request,
    action: str = Query(..., description="Remediation action"),
    tenant_id: str = Query(..., description="Tenant ID"),
    notes: Optional[str] = Query(None)
):
    """Remediate compliance finding"""
    return {
        "finding_id": finding_id,
        "status": "remediated",
        "remediated_by": user_id,
        "remediated_at": datetime.utcnow(),
        "action": action,
        "notes": notes
    }


@router.get("/data-privacy/subjects/{subject_id}")
async def get_data_subject(
    subject_id: str,
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get data subject information (GDPR)"""
    return {
        "subject_id": subject_id,
        "data_assets": [
            {
                "asset_id": "asset_123",
                "asset_name": "customer_orders",
                "data_types": ["name", "email", "address"],
                "purpose": "order_processing",
                "retention_days": 730
            }
        ],
        "consents": [
            {
                "purpose": "marketing",
                "granted": True,
                "granted_at": datetime.utcnow() - timedelta(days=90)
            }
        ],
        "requests": []
    }


@router.post("/data-privacy/requests")
async def create_privacy_request(
    request: Request,
    subject_id: str = Query(..., description="Data subject ID"),
    request_type: str = Query(..., description="Request type (access, deletion, portability, rectification)"),
    tenant_id: str = Query(..., description="Tenant ID"),
    details: Optional[str] = Query(None)
):
    """Create data privacy request (GDPR)"""
    privacy_request_id = str(uuid.uuid4())
    
    return {
        "request_id": privacy_request_id,
        "subject_id": subject_id,
        "request_type": request_type,
        "status": "pending",
        "submitted_at": datetime.utcnow(),
        "due_date": datetime.utcnow() + timedelta(days=30)
    }


@router.get("/audit-logs")
async def get_audit_logs(
    request: Request,
    tenant_id: str = Query(..., description="Tenant ID"),
    asset_id: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    action: Optional[str] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """Get governance audit logs"""
    return {
        "logs": [
            {
                "log_id": "log_123",
                "timestamp": datetime.utcnow(),
                "user_id": "user_456",
                "action": "data_access",
                "asset_id": "asset_789",
                "details": {
                    "query": "SELECT * FROM customers",
                    "rows_accessed": 1000
                },
                "policy_applied": "pii_access_policy",
                "result": "allowed"
            }
        ],
        "total": 1
    } 