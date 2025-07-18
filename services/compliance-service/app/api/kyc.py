"""
KYC API endpoints for Compliance Service.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from pydantic import BaseModel, Field

from platformq_shared import get_current_user
from ..core.kyc_manager import KYCManager, KYCStatus, KYCLevel
from ..models.kyc import KYCVerification, Document

logger = logging.getLogger(__name__)

router = APIRouter()


class KYCInitiationRequest(BaseModel):
    """Request to initiate KYC verification"""
    user_id: str = Field(..., description="User identifier")
    kyc_level: int = Field(..., ge=1, le=3, description="Requested KYC level")
    country: str = Field(..., description="User's country of residence")
    document_type: str = Field(..., description="Type of document (passport, id_card, driver_license)")


class DocumentUploadResponse(BaseModel):
    """Response from document upload"""
    document_id: str
    status: str
    message: str


class KYCStatusResponse(BaseModel):
    """KYC verification status"""
    user_id: str
    status: str
    level: int
    verified_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    required_documents: List[str] = Field(default_factory=list)
    completed_checks: List[str] = Field(default_factory=list)
    pending_checks: List[str] = Field(default_factory=list)


def get_kyc_manager(request) -> KYCManager:
    """Dependency to get KYC manager instance"""
    return request.app.state.kyc_manager


@router.post("/initiate")
async def initiate_kyc_verification(
    request: KYCInitiationRequest,
    current_user: Dict = Depends(get_current_user),
    kyc_manager: KYCManager = Depends(get_kyc_manager)
):
    """
    Initiate KYC verification process for a user.
    
    This starts the verification workflow based on the requested level.
    """
    try:
        # Ensure user can only initiate their own KYC
        if request.user_id != current_user["id"]:
            raise HTTPException(
                status_code=403,
                detail="Can only initiate KYC for your own account"
            )
        
        # Create verification request
        verification = await kyc_manager.initiate_verification(
            user_id=request.user_id,
            kyc_level=KYCLevel(request.kyc_level),
            country=request.country,
            document_type=request.document_type
        )
        
        return {
            "verification_id": verification.id,
            "status": verification.status.value,
            "required_documents": verification.required_documents,
            "upload_url": f"/api/v1/kyc/{verification.id}/documents",
            "expires_at": verification.expires_at.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error initiating KYC: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{verification_id}/documents")
async def upload_document(
    verification_id: str,
    document_type: str,
    file: UploadFile = File(...),
    current_user: Dict = Depends(get_current_user),
    kyc_manager: KYCManager = Depends(get_kyc_manager)
):
    """
    Upload a document for KYC verification.
    
    Accepts identity documents like passport, ID card, or driver's license.
    """
    try:
        # Validate file type
        allowed_types = ["image/jpeg", "image/png", "image/jpg", "application/pdf"]
        if file.content_type not in allowed_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file type. Allowed: {allowed_types}"
            )
        
        # Validate file size (max 10MB)
        contents = await file.read()
        if len(contents) > 10 * 1024 * 1024:
            raise HTTPException(
                status_code=400,
                detail="File size exceeds 10MB limit"
            )
        
        # Upload document
        document = await kyc_manager.upload_document(
            verification_id=verification_id,
            document_type=document_type,
            file_data=contents,
            file_name=file.filename,
            content_type=file.content_type,
            user_id=current_user["id"]
        )
        
        return DocumentUploadResponse(
            document_id=document.id,
            status="uploaded",
            message="Document uploaded successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading document: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{verification_id}/submit")
async def submit_for_verification(
    verification_id: str,
    current_user: Dict = Depends(get_current_user),
    kyc_manager: KYCManager = Depends(get_kyc_manager)
):
    """
    Submit KYC documents for verification.
    
    Triggers the verification process after all documents are uploaded.
    """
    try:
        # Submit for verification
        result = await kyc_manager.submit_for_verification(
            verification_id=verification_id,
            user_id=current_user["id"]
        )
        
        return {
            "verification_id": verification_id,
            "status": result["status"],
            "message": "Verification submitted successfully",
            "estimated_completion": result.get("estimated_completion")
        }
        
    except Exception as e:
        logger.error(f"Error submitting verification: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status", response_model=KYCStatusResponse)
async def get_kyc_status(
    current_user: Dict = Depends(get_current_user),
    kyc_manager: KYCManager = Depends(get_kyc_manager)
):
    """
    Get current KYC status for the authenticated user.
    """
    try:
        status = await kyc_manager.get_user_status(current_user["id"])
        
        return KYCStatusResponse(
            user_id=current_user["id"],
            status=status.status.value,
            level=status.level.value,
            verified_at=status.verified_at,
            expires_at=status.expires_at,
            required_documents=status.required_documents,
            completed_checks=status.completed_checks,
            pending_checks=status.pending_checks
        )
        
    except Exception as e:
        logger.error(f"Error getting KYC status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def get_kyc_history(
    limit: int = 10,
    current_user: Dict = Depends(get_current_user),
    kyc_manager: KYCManager = Depends(get_kyc_manager)
):
    """
    Get KYC verification history for the authenticated user.
    """
    try:
        history = await kyc_manager.get_verification_history(
            user_id=current_user["id"],
            limit=limit
        )
        
        return {
            "user_id": current_user["id"],
            "verifications": [
                {
                    "verification_id": v.id,
                    "status": v.status.value,
                    "level": v.level.value,
                    "created_at": v.created_at.isoformat(),
                    "completed_at": v.completed_at.isoformat() if v.completed_at else None,
                    "result": v.result
                }
                for v in history
            ]
        }
        
    except Exception as e:
        logger.error(f"Error getting KYC history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/refresh")
async def refresh_kyc_verification(
    current_user: Dict = Depends(get_current_user),
    kyc_manager: KYCManager = Depends(get_kyc_manager)
):
    """
    Refresh expired KYC verification.
    
    Initiates a streamlined re-verification process.
    """
    try:
        result = await kyc_manager.refresh_verification(current_user["id"])
        
        return {
            "verification_id": result["verification_id"],
            "status": "refresh_initiated",
            "required_documents": result["required_documents"],
            "previous_level": result["previous_level"]
        }
        
    except Exception as e:
        logger.error(f"Error refreshing KYC: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/documents/{document_id}")
async def delete_document(
    document_id: str,
    current_user: Dict = Depends(get_current_user),
    kyc_manager: KYCManager = Depends(get_kyc_manager)
):
    """
    Delete a KYC document.
    
    Only allowed for documents in pending status.
    """
    try:
        await kyc_manager.delete_document(
            document_id=document_id,
            user_id=current_user["id"]
        )
        
        return {
            "status": "deleted",
            "document_id": document_id
        }
        
    except Exception as e:
        logger.error(f"Error deleting document: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 