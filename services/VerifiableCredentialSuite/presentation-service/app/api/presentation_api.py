"""
Presentation API Endpoints
"""

from typing import Optional, List, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.core.presentation_manager import PresentationManager, PresentationStatus
from app import main

router = APIRouter()


# Request/Response models
class CreatePresentationRequest(BaseModel):
    """Request to create a presentation"""
    holder_did: str = Field(
        description="DID of the holder creating the presentation"
    )
    credential_ids: List[str] = Field(
        description="List of credential IDs to include"
    )
    verifier_did: Optional[str] = Field(
        description="Optional DID of intended verifier",
        default=None
    )
    challenge: Optional[str] = Field(
        description="Challenge nonce from verifier",
        default=None
    )
    domain: Optional[str] = Field(
        description="Domain for domain binding",
        default=None
    )
    selective_disclosure: Optional[Dict[str, List[str]]] = Field(
        description="Fields to disclose per credential",
        default=None,
        example={"cred-123": ["name", "dateOfBirth"]}
    )
    proof_options: Optional[Dict[str, Any]] = Field(
        description="Additional proof generation options",
        default=None
    )


class SubmitPresentationRequest(BaseModel):
    """Request to submit a presentation"""
    verifier_did: str = Field(
        description="DID of the verifier"
    )
    session_id: Optional[str] = Field(
        description="Optional session ID",
        default=None
    )


class RevokePresentationRequest(BaseModel):
    """Request to revoke a presentation"""
    reason: str = Field(
        description="Reason for revocation"
    )
    revoker_did: str = Field(
        description="DID of the revoker"
    )


class PresentationResponse(BaseModel):
    """Presentation response"""
    id: str
    holder: str
    verifier: Optional[str]
    presentation: Dict[str, Any]
    status: str
    created_at: str
    submitted_at: Optional[str]
    verified_at: Optional[str]
    session_id: Optional[str]


class PresentationListResponse(BaseModel):
    """List of presentations"""
    presentations: List[PresentationResponse]
    total: int
    page: int
    limit: int


# Dependency to get presentation manager
def get_presentation_manager() -> PresentationManager:
    """Get presentation manager instance"""
    if not main.presentation_manager:
        raise HTTPException(
            status_code=503,
            detail="Presentation manager not initialized"
        )
    return main.presentation_manager


# API Endpoints

@router.post("/presentations", response_model=PresentationResponse)
async def create_presentation(
    request: CreatePresentationRequest,
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Create a Verifiable Presentation
    
    Creates a new presentation from one or more credentials.
    Supports selective disclosure for privacy-preserving presentations.
    """
    try:
        result = await presentation_manager.create_presentation(
            holder_did=request.holder_did,
            credential_ids=request.credential_ids,
            verifier_did=request.verifier_did,
            challenge=request.challenge,
            domain=request.domain,
            selective_disclosure=request.selective_disclosure,
            proof_options=request.proof_options
        )
        
        stored = await presentation_manager.presentation_store.get(result["id"])
        
        return PresentationResponse(
            id=result["id"],
            holder=request.holder_did,
            verifier=request.verifier_did,
            presentation=result["presentation"],
            status=result["status"],
            created_at=result["created_at"],
            submitted_at=None,
            verified_at=None,
            session_id=None
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create presentation: {str(e)}"
        )


@router.get("/presentations/{presentation_id}", response_model=PresentationResponse)
async def get_presentation(
    presentation_id: str,
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Get presentation by ID
    
    Retrieves a specific presentation.
    """
    presentation = await presentation_manager.get_presentation(presentation_id)
    
    if not presentation:
        raise HTTPException(
            status_code=404,
            detail=f"Presentation {presentation_id} not found"
        )
    
    return PresentationResponse(**presentation)


@router.post("/presentations/{presentation_id}/submit")
async def submit_presentation(
    presentation_id: str,
    request: SubmitPresentationRequest,
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Submit a presentation to a verifier
    
    Submits a presentation for verification, optionally within a session.
    """
    try:
        result = await presentation_manager.submit_presentation(
            presentation_id=presentation_id,
            verifier_did=request.verifier_did,
            session_id=request.session_id
        )
        
        return JSONResponse(
            content=result,
            status_code=200
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit presentation: {str(e)}"
        )


@router.post("/presentations/{presentation_id}/revoke")
async def revoke_presentation(
    presentation_id: str,
    request: RevokePresentationRequest,
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Revoke a presentation
    
    Revokes a presentation, making it invalid. Only the holder can revoke.
    """
    try:
        result = await presentation_manager.revoke_presentation(
            presentation_id=presentation_id,
            reason=request.reason,
            revoker_did=request.revoker_did
        )
        
        return JSONResponse(
            content=result,
            status_code=200
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to revoke presentation: {str(e)}"
        )


@router.get("/presentations", response_model=PresentationListResponse)
async def list_presentations(
    holder_did: Optional[str] = Query(None, description="Filter by holder DID"),
    verifier_did: Optional[str] = Query(None, description="Filter by verifier DID"),
    status: Optional[PresentationStatus] = Query(None, description="Filter by status"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=100, description="Items per page"),
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    List presentations with filters
    
    Retrieves a paginated list of presentations.
    """
    offset = (page - 1) * limit
    
    presentations = await presentation_manager.list_presentations(
        holder_did=holder_did,
        verifier_did=verifier_did,
        status=status,
        limit=limit,
        offset=offset
    )
    
    # Convert to response format
    presentation_responses = []
    for pres in presentations:
        presentation_responses.append(PresentationResponse(**pres))
    
    return PresentationListResponse(
        presentations=presentation_responses,
        total=len(presentation_responses),
        page=page,
        limit=limit
    )


@router.get("/presentations/{presentation_id}/verification-history")
async def get_verification_history(
    presentation_id: str,
    limit: int = Query(100, ge=1, le=1000, description="Maximum records"),
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Get verification history for a presentation
    
    Retrieves the history of verification attempts for a presentation.
    """
    # Check presentation exists
    presentation = await presentation_manager.get_presentation(presentation_id)
    if not presentation:
        raise HTTPException(
            status_code=404,
            detail=f"Presentation {presentation_id} not found"
        )
    
    # Get verification history
    history = await presentation_manager.presentation_store.get_verification_history(
        presentation_id=presentation_id,
        limit=limit
    )
    
    # Format response
    history_data = []
    for record in history:
        history_data.append({
            "id": record.id,
            "verifier": record.verifier,
            "result": record.result,
            "timestamp": record.timestamp.isoformat(),
            "details": record.details
        })
    
    return JSONResponse(
        content={
            "presentation_id": presentation_id,
            "history": history_data,
            "total": len(history_data)
        }
    )


@router.post("/presentations/search")
async def search_presentations(
    query: str = Body(..., description="Search query"),
    filters: Optional[Dict[str, Any]] = Body(None, description="Search filters"),
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(50, ge=1, le=100, description="Items per page"),
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Search presentations
    
    Search presentations by query and filters.
    """
    offset = (page - 1) * limit
    
    results = await presentation_manager.presentation_store.search(
        query=query,
        filters=filters,
        limit=limit,
        offset=offset
    )
    
    # Format results
    presentations = []
    for record in results:
        presentations.append({
            "id": record.presentation_id,
            "holder": record.holder_did,
            "verifier": record.verifier_did,
            "status": record.status,
            "created_at": record.created_at.isoformat(),
            "session_id": record.session_id
        })
    
    return JSONResponse(
        content={
            "results": presentations,
            "total": len(presentations),
            "page": page,
            "limit": limit,
            "query": query,
            "filters": filters
        }
    )


@router.get("/stats")
async def get_statistics(
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """Get presentation service statistics"""
    stats = await presentation_manager.get_statistics()
    return JSONResponse(content=stats) 