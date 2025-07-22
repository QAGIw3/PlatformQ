"""
Credential API endpoints
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
import logging

from fastapi import APIRouter, HTTPException, Depends, Query, Request
from pydantic import BaseModel, Field, validator

from platformq_vc_common import (
    VerifiableCredentialModel,
    CredentialType,
    create_credential_id
)

logger = logging.getLogger(__name__)
router = APIRouter()


# Request/Response Models
class IssueCredentialRequest(BaseModel):
    """Request to issue a verifiable credential"""
    credential_type: str = Field(..., description="Type of credential to issue")
    subject: Dict[str, Any] = Field(..., description="Credential subject data")
    issuer_did: Optional[str] = Field(None, description="Issuer DID (uses default if not provided)")
    validity_days: Optional[int] = Field(None, description="Validity period in days")
    
    # Optional fields
    description: Optional[str] = Field(None, description="Human-readable description")
    name: Optional[str] = Field(None, description="Human-readable name")
    
    # PlatformQ extensions
    tenant_id: Optional[str] = Field(None, description="Tenant identifier")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    # Storage options
    store_on_ipfs: bool = Field(default=True, description="Store on IPFS")
    encrypt_storage: bool = Field(default=True, description="Encrypt credential in storage")
    
    # Blockchain options
    anchor_on_blockchain: bool = Field(default=True, description="Anchor on blockchain")
    blockchain_networks: Optional[List[str]] = Field(None, description="Specific blockchains to anchor on")
    
    @validator('credential_type')
    def validate_credential_type(cls, v):
        """Validate credential type"""
        # Could check against supported types
        return v


class VerifyCredentialRequest(BaseModel):
    """Request to verify a credential"""
    credential: Dict[str, Any] = Field(..., description="The credential to verify")
    
    # Verification options
    check_revocation: bool = Field(default=True, description="Check revocation status")
    check_expiration: bool = Field(default=True, description="Check if expired")
    verify_signature: bool = Field(default=True, description="Verify cryptographic signature")
    verify_issuer: bool = Field(default=True, description="Verify issuer DID")
    
    # Optional expected values
    expected_issuer: Optional[str] = Field(None, description="Expected issuer DID")
    expected_subject: Optional[str] = Field(None, description="Expected subject DID")


class BatchIssueRequest(BaseModel):
    """Request to issue multiple credentials"""
    credentials: List[IssueCredentialRequest] = Field(
        ..., 
        description="List of credentials to issue",
        max_items=100
    )
    
    # Batch options
    fail_on_error: bool = Field(default=False, description="Fail entire batch on any error")
    parallel_processing: bool = Field(default=True, description="Process in parallel")


class CredentialSearchRequest(BaseModel):
    """Search credentials with filters"""
    issuer: Optional[str] = Field(None, description="Filter by issuer DID")
    subject: Optional[str] = Field(None, description="Filter by subject DID")
    credential_type: Optional[str] = Field(None, description="Filter by credential type")
    
    # Date filters
    issued_after: Optional[datetime] = Field(None, description="Issued after date")
    issued_before: Optional[datetime] = Field(None, description="Issued before date")
    
    # Status filters
    include_revoked: bool = Field(default=False, description="Include revoked credentials")
    only_valid: bool = Field(default=True, description="Only non-expired credentials")
    
    # Pagination
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=20, ge=1, le=100)


class CredentialStatusResponse(BaseModel):
    """Credential status information"""
    credential_id: str
    status: str  # active, revoked, expired
    issued_at: datetime
    expires_at: Optional[datetime]
    revoked_at: Optional[datetime]
    revocation_reason: Optional[str]
    blockchain_anchors: Optional[List[Dict[str, Any]]]


class IssueCredentialResponse(BaseModel):
    """Response after issuing a credential"""
    credential: Dict[str, Any]
    credential_id: str
    storage_info: Optional[Dict[str, Any]]
    blockchain_info: Optional[Dict[str, Any]]


class VerifyCredentialResponse(BaseModel):
    """Response from credential verification"""
    valid: bool
    credential_id: Optional[str]
    checks: Dict[str, bool]  # Individual check results
    errors: List[str]
    warnings: List[str]


# Dependency to get credential manager
async def get_credential_manager(request: Request):
    """Get credential manager from app state"""
    return request.app.state.credential_manager


# API Endpoints

@router.post("/credentials/issue", response_model=IssueCredentialResponse)
async def issue_credential(
    request: IssueCredentialRequest,
    credential_manager = Depends(get_credential_manager)
):
    """
    Issue a new verifiable credential
    
    This endpoint creates a W3C-compliant verifiable credential with proper
    signatures and optional blockchain anchoring.
    """
    try:
        logger.info(f"Issuing {request.credential_type} credential")
        
        # Issue credential through manager
        result = await credential_manager.issue_credential(
            credential_type=request.credential_type,
            subject=request.subject,
            issuer_did=request.issuer_did,
            validity_days=request.validity_days,
            description=request.description,
            name=request.name,
            tenant_id=request.tenant_id,
            metadata=request.metadata,
            store_on_ipfs=request.store_on_ipfs,
            encrypt_storage=request.encrypt_storage,
            anchor_on_blockchain=request.anchor_on_blockchain,
            blockchain_networks=request.blockchain_networks
        )
        
        return IssueCredentialResponse(
            credential=result["credential"],
            credential_id=result["credential_id"],
            storage_info=result.get("storage_info"),
            blockchain_info=result.get("blockchain_info")
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error issuing credential: {e}")
        raise HTTPException(status_code=500, detail="Failed to issue credential")


@router.post("/credentials/verify", response_model=VerifyCredentialResponse)
async def verify_credential(
    request: VerifyCredentialRequest,
    credential_manager = Depends(get_credential_manager)
):
    """
    Verify a verifiable credential
    
    Performs comprehensive verification including signature validation,
    expiration checking, and revocation status.
    """
    try:
        result = await credential_manager.verify_credential(
            credential=request.credential,
            check_revocation=request.check_revocation,
            check_expiration=request.check_expiration,
            verify_signature=request.verify_signature,
            verify_issuer=request.verify_issuer,
            expected_issuer=request.expected_issuer,
            expected_subject=request.expected_subject
        )
        
        return VerifyCredentialResponse(**result)
        
    except Exception as e:
        logger.error(f"Error verifying credential: {e}")
        raise HTTPException(status_code=500, detail="Failed to verify credential")


@router.get("/credentials/{credential_id}")
async def get_credential(
    credential_id: str,
    credential_manager = Depends(get_credential_manager)
):
    """
    Retrieve a credential by ID
    
    Returns the full credential if found and accessible.
    """
    try:
        credential = await credential_manager.get_credential(credential_id)
        if not credential:
            raise HTTPException(status_code=404, detail="Credential not found")
        
        return credential
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving credential: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve credential")


@router.post("/credentials/{credential_id}/revoke")
async def revoke_credential(
    credential_id: str,
    reason: str = Query(..., description="Revocation reason"),
    credential_manager = Depends(get_credential_manager)
):
    """
    Revoke a credential
    
    Marks the credential as revoked with the given reason.
    """
    try:
        result = await credential_manager.revoke_credential(
            credential_id=credential_id,
            reason=reason
        )
        
        if not result:
            raise HTTPException(status_code=404, detail="Credential not found")
        
        return {"status": "revoked", "credential_id": credential_id, "reason": reason}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error revoking credential: {e}")
        raise HTTPException(status_code=500, detail="Failed to revoke credential")


@router.get("/credentials/{credential_id}/status", response_model=CredentialStatusResponse)
async def get_credential_status(
    credential_id: str,
    credential_manager = Depends(get_credential_manager)
):
    """
    Get credential status
    
    Returns detailed status information including revocation status
    and blockchain anchors.
    """
    try:
        status = await credential_manager.get_credential_status(credential_id)
        if not status:
            raise HTTPException(status_code=404, detail="Credential not found")
        
        return CredentialStatusResponse(**status)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting credential status: {e}")
        raise HTTPException(status_code=500, detail="Failed to get credential status")


@router.post("/credentials/batch/issue")
async def batch_issue_credentials(
    request: BatchIssueRequest,
    credential_manager = Depends(get_credential_manager)
):
    """
    Issue multiple credentials in batch
    
    Efficiently issues multiple credentials with optional parallel processing.
    """
    try:
        results = await credential_manager.batch_issue_credentials(
            requests=request.credentials,
            fail_on_error=request.fail_on_error,
            parallel_processing=request.parallel_processing
        )
        
        return {
            "total": len(request.credentials),
            "successful": len([r for r in results if r.get("success")]),
            "failed": len([r for r in results if not r.get("success")]),
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error in batch issue: {e}")
        raise HTTPException(status_code=500, detail="Failed to batch issue credentials")


@router.post("/credentials/batch/verify")
async def batch_verify_credentials(
    credentials: List[Dict[str, Any]],
    credential_manager = Depends(get_credential_manager)
):
    """
    Verify multiple credentials in batch
    
    Efficiently verifies multiple credentials in parallel.
    """
    try:
        results = await credential_manager.batch_verify_credentials(credentials)
        
        return {
            "total": len(credentials),
            "valid": len([r for r in results if r.get("valid")]),
            "invalid": len([r for r in results if not r.get("valid")]),
            "results": results
        }
        
    except Exception as e:
        logger.error(f"Error in batch verify: {e}")
        raise HTTPException(status_code=500, detail="Failed to batch verify credentials")


@router.post("/credentials/search")
async def search_credentials(
    request: CredentialSearchRequest,
    credential_manager = Depends(get_credential_manager)
):
    """
    Search credentials with filters
    
    Returns paginated results matching the search criteria.
    """
    try:
        results = await credential_manager.search_credentials(
            issuer=request.issuer,
            subject=request.subject,
            credential_type=request.credential_type,
            issued_after=request.issued_after,
            issued_before=request.issued_before,
            include_revoked=request.include_revoked,
            only_valid=request.only_valid,
            offset=request.offset,
            limit=request.limit
        )
        
        return results
        
    except Exception as e:
        logger.error(f"Error searching credentials: {e}")
        raise HTTPException(status_code=500, detail="Failed to search credentials") 