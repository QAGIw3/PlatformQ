"""
Verifiable credential attestation API endpoints
"""

from typing import Dict, Any, List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Body, Path
from pydantic import BaseModel
from cryptography.hazmat.primitives import serialization

from platformq_shared.logging import get_logger
from ..core import CredentialAttestor, CredentialType, CredentialStatus

logger = get_logger(__name__)

router = APIRouter(prefix="/api/v1/attestations", tags=["attestations"])

# Dependency injection
credential_attestor: Optional[CredentialAttestor] = None

def set_dependencies(attestor: CredentialAttestor):
    """Set API dependencies"""
    global credential_attestor
    credential_attestor = attestor


# Request/Response models
class WorkflowAttestationRequest(BaseModel):
    workflow_id: str
    workflow_name: str
    execution_id: str
    execution_result: Dict[str, Any]


class DataProcessingAttestationRequest(BaseModel):
    dataset_id: str
    processing_type: str
    processing_result: Dict[str, Any]


class QualityAttestationRequest(BaseModel):
    dataset_id: str
    quality_result: Dict[str, Any]


class ComplianceVerificationRequest(BaseModel):
    entity_id: str
    compliance_type: str
    verification_result: Dict[str, Any]


class VerifyCredentialRequest(BaseModel):
    credential: Dict[str, Any]


class CreatePresentationRequest(BaseModel):
    credentials: List[Dict[str, Any]]
    holder: Dict[str, Any]
    verifier: Optional[str] = None


class RevokeCredentialRequest(BaseModel):
    reason: str = ""


class CredentialResponse(BaseModel):
    id: str
    type: List[str]
    issuer: Dict[str, Any]
    issuanceDate: str
    expirationDate: str
    credentialSubject: Dict[str, Any]
    credentialStatus: Dict[str, Any]
    proof: Dict[str, Any]


class VerificationResponse(BaseModel):
    verified: bool
    checks: Dict[str, bool]
    errors: List[str]


class CredentialStatusResponse(BaseModel):
    credential_id: str
    status: CredentialStatus
    issued_at: str
    type: str
    subject_id: str
    revoked_at: Optional[str] = None
    revocation_reason: Optional[str] = None


# API Endpoints
@router.post("/workflow", response_model=Dict[str, Any])
async def create_workflow_attestation(request: WorkflowAttestationRequest = Body(...)):
    """Create attestation for completed workflow"""
    if not credential_attestor:
        raise HTTPException(status_code=503, detail="Credential attestor not initialized")
        
    try:
        credential = await credential_attestor.create_workflow_attestation(
            workflow_id=request.workflow_id,
            workflow_name=request.workflow_name,
            execution_id=request.execution_id,
            execution_result=request.execution_result
        )
        
        return credential
        
    except Exception as e:
        logger.error(f"Failed to create workflow attestation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/data-processing", response_model=Dict[str, Any])
async def create_data_processing_attestation(request: DataProcessingAttestationRequest = Body(...)):
    """Create attestation for data processing"""
    if not credential_attestor:
        raise HTTPException(status_code=503, detail="Credential attestor not initialized")
        
    try:
        credential = await credential_attestor.create_data_processing_attestation(
            dataset_id=request.dataset_id,
            processing_type=request.processing_type,
            processing_result=request.processing_result
        )
        
        return credential
        
    except Exception as e:
        logger.error(f"Failed to create data processing attestation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/quality", response_model=Dict[str, Any])
async def create_quality_attestation(request: QualityAttestationRequest = Body(...)):
    """Create attestation for data quality verification"""
    if not credential_attestor:
        raise HTTPException(status_code=503, detail="Credential attestor not initialized")
        
    try:
        credential = await credential_attestor.create_quality_attestation(
            dataset_id=request.dataset_id,
            quality_result=request.quality_result
        )
        
        return credential
        
    except Exception as e:
        logger.error(f"Failed to create quality attestation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compliance", response_model=Dict[str, Any])
async def create_compliance_verification(request: ComplianceVerificationRequest = Body(...)):
    """Create attestation for compliance verification"""
    if not credential_attestor:
        raise HTTPException(status_code=503, detail="Credential attestor not initialized")
        
    try:
        credential = await credential_attestor.create_compliance_verification(
            entity_id=request.entity_id,
            compliance_type=request.compliance_type,
            verification_result=request.verification_result
        )
        
        return credential
        
    except Exception as e:
        logger.error(f"Failed to create compliance verification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/verify", response_model=VerificationResponse)
async def verify_credential(request: VerifyCredentialRequest = Body(...)):
    """Verify a credential"""
    if not credential_attestor:
        raise HTTPException(status_code=503, detail="Credential attestor not initialized")
        
    try:
        result = await credential_attestor.verify_credential(request.credential)
        return VerificationResponse(**result)
        
    except Exception as e:
        logger.error(f"Failed to verify credential: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/presentation", response_model=Dict[str, Any])
async def create_presentation(request: CreatePresentationRequest = Body(...)):
    """Create a verifiable presentation from multiple credentials"""
    if not credential_attestor:
        raise HTTPException(status_code=503, detail="Credential attestor not initialized")
        
    try:
        presentation = await credential_attestor.create_presentation(
            credentials=request.credentials,
            holder=request.holder,
            verifier=request.verifier
        )
        
        return presentation
        
    except Exception as e:
        logger.error(f"Failed to create presentation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/credentials", response_model=List[Dict[str, Any]])
async def list_credentials(
    credential_type: Optional[CredentialType] = Query(None),
    subject_id: Optional[str] = Query(None),
    status: Optional[CredentialStatus] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """List issued credentials with filtering"""
    if not credential_attestor:
        raise HTTPException(status_code=503, detail="Credential attestor not initialized")
        
    try:
        credentials = await credential_attestor.list_credentials(
            credential_type=credential_type,
            subject_id=subject_id,
            status=status
        )
        
        # Sort by issuance date (newest first)
        credentials.sort(key=lambda x: x.get('issued_at', ''), reverse=True)
        
        # Apply pagination
        start = offset
        end = offset + limit
        paginated = credentials[start:end]
        
        return paginated
        
    except Exception as e:
        logger.error(f"Failed to list credentials: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/credentials/{credential_id}/status", response_model=CredentialStatusResponse)
async def get_credential_status(credential_id: str = Path(...)):
    """Get credential status"""
    if not credential_attestor:
        raise HTTPException(status_code=503, detail="Credential attestor not initialized")
        
    try:
        status = await credential_attestor.get_credential_status(credential_id)
        if not status:
            raise HTTPException(status_code=404, detail=f"Credential {credential_id} not found")
            
        return CredentialStatusResponse(**status)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get credential status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/credentials/{credential_id}/revoke")
async def revoke_credential(
    credential_id: str = Path(...),
    request: RevokeCredentialRequest = Body(...)
):
    """Revoke a credential"""
    if not credential_attestor:
        raise HTTPException(status_code=503, detail="Credential attestor not initialized")
        
    try:
        success = await credential_attestor.revoke_credential(
            credential_id=credential_id,
            reason=request.reason
        )
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Credential {credential_id} not found")
            
        return {"message": f"Credential {credential_id} revoked successfully"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to revoke credential: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/issuer/info")
async def get_issuer_info():
    """Get information about this credential issuer"""
    if not credential_attestor:
        raise HTTPException(status_code=503, detail="Credential attestor not initialized")
        
    try:
        from ..core.config import settings
        
        # Get public key
        public_key_pem = credential_attestor.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')
        
        return {
            "issuer": {
                "id": f"did:platformq:{settings.organization_id}",
                "name": settings.organization_name,
                "type": "OrchestrationService"
            },
            "public_key": public_key_pem,
            "supported_credential_types": [t.value for t in CredentialType],
            "credential_endpoint": "/api/v1/attestations",
            "verification_endpoint": "/api/v1/attestations/verify"
        }
        
    except Exception as e:
        logger.error(f"Failed to get issuer info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics")
async def get_attestation_statistics():
    """Get attestation statistics"""
    if not credential_attestor:
        raise HTTPException(status_code=503, detail="Credential attestor not initialized")
        
    try:
        stats = {
            "total_issued": len(credential_attestor.issued_credentials),
            "by_type": {},
            "by_status": {},
            "trusted_issuers": len(credential_attestor.trusted_issuers)
        }
        
        # Count by type and status
        for cred_data in credential_attestor.issued_credentials.values():
            cred_type = cred_data['metadata']['type']
            status = cred_data['status']
            
            stats['by_type'][cred_type] = stats['by_type'].get(cred_type, 0) + 1
            stats['by_status'][status] = stats['by_status'].get(status, 0) + 1
            
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get attestation statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 