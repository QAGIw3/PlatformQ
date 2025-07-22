"""
Session Management API Endpoints
"""

from typing import Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.core.session_manager import SessionManager
from app import main

router = APIRouter()


# Request/Response models
class CreateSessionRequest(BaseModel):
    """Request to create a session"""
    holder_did: str = Field(
        description="DID of the credential holder"
    )
    verifier_did: str = Field(
        description="DID of the verifier"
    )
    presentation_id: Optional[str] = Field(
        description="Optional presentation ID",
        default=None
    )
    challenge: Optional[str] = Field(
        description="Optional challenge nonce",
        default=None
    )
    metadata: Optional[Dict[str, Any]] = Field(
        description="Additional session metadata",
        default=None
    )


class UpdateSessionRequest(BaseModel):
    """Request to update session"""
    updates: Dict[str, Any] = Field(
        description="Fields to update"
    )


class SessionResponse(BaseModel):
    """Session response"""
    id: str
    holder_did: str
    verifier_did: str
    presentation_id: Optional[str]
    challenge: str
    created_at: str
    expires_at: str
    status: str
    metadata: Dict[str, Any]
    events: list


# Dependency to get session manager
def get_session_manager() -> SessionManager:
    """Get session manager instance"""
    if not main.session_manager:
        raise HTTPException(
            status_code=503,
            detail="Session manager not initialized"
        )
    return main.session_manager


# API Endpoints

@router.post("/sessions", response_model=SessionResponse)
async def create_session(
    request: CreateSessionRequest,
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Create a new presentation session
    
    Creates a session for managing the presentation flow between
    holder and verifier.
    """
    try:
        session = await session_manager.create_session(
            holder_did=request.holder_did,
            verifier_did=request.verifier_did,
            presentation_id=request.presentation_id,
            challenge=request.challenge,
            metadata=request.metadata
        )
        
        return SessionResponse(**session)
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to create session: {str(e)}"
        )


@router.get("/sessions/{session_id}", response_model=SessionResponse)
async def get_session(
    session_id: str,
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Get session by ID
    
    Retrieves session details by session ID.
    """
    session = await session_manager.get_session(session_id)
    
    if not session:
        raise HTTPException(
            status_code=404,
            detail=f"Session {session_id} not found or expired"
        )
    
    return SessionResponse(**session)


@router.put("/sessions/{session_id}")
async def update_session(
    session_id: str,
    request: UpdateSessionRequest,
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Update session data
    
    Updates session with new information.
    """
    try:
        session = await session_manager.update_session(
            session_id=session_id,
            updates=request.updates
        )
        
        if not session:
            raise HTTPException(
                status_code=404,
                detail=f"Session {session_id} not found or expired"
            )
        
        return SessionResponse(**session)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to update session: {str(e)}"
        )


@router.post("/sessions/{session_id}/extend")
async def extend_session(
    session_id: str,
    additional_seconds: int = Query(3600, ge=60, le=86400, description="Additional seconds"),
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Extend session expiration
    
    Extends the session TTL by the specified duration.
    """
    try:
        session = await session_manager.extend_session(
            session_id=session_id,
            additional_seconds=additional_seconds
        )
        
        if not session:
            raise HTTPException(
                status_code=404,
                detail=f"Session {session_id} not found or expired"
            )
        
        return SessionResponse(**session)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to extend session: {str(e)}"
        )


@router.delete("/sessions/{session_id}")
async def invalidate_session(
    session_id: str,
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Invalidate a session
    
    Immediately invalidates and removes a session.
    """
    try:
        await session_manager.invalidate_session(session_id)
        
        return JSONResponse(
            content={
                "message": f"Session {session_id} invalidated",
                "timestamp": datetime.now().isoformat()
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to invalidate session: {str(e)}"
        )


@router.post("/sessions/{session_id}/presentation")
async def add_presentation_to_session(
    session_id: str,
    presentation_id: str = Query(..., description="Presentation ID to add"),
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Add presentation to session
    
    Associates a presentation with an existing session.
    """
    try:
        session = await session_manager.add_presentation_to_session(
            session_id=session_id,
            presentation_id=presentation_id
        )
        
        if not session:
            raise HTTPException(
                status_code=404,
                detail=f"Session {session_id} not found or expired"
            )
        
        return SessionResponse(**session)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add presentation to session: {str(e)}"
        )


@router.post("/sessions/{session_id}/verification-result")
async def add_verification_result(
    session_id: str,
    verification_result: Dict[str, Any] = ...,
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Add verification result to session
    
    Records the verification result in the session.
    """
    try:
        session = await session_manager.add_verification_result_to_session(
            session_id=session_id,
            verification_result=verification_result
        )
        
        if not session:
            raise HTTPException(
                status_code=404,
                detail=f"Session {session_id} not found or expired"
            )
        
        return SessionResponse(**session)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to add verification result: {str(e)}"
        )


@router.get("/sessions/active")
async def get_active_sessions(
    holder_did: Optional[str] = Query(None, description="Filter by holder DID"),
    verifier_did: Optional[str] = Query(None, description="Filter by verifier DID"),
    session_manager: SessionManager = Depends(get_session_manager)
):
    """
    Get active sessions
    
    Retrieves active sessions filtered by holder or verifier.
    Note: This is a simplified implementation for demonstration.
    """
    try:
        sessions = await session_manager.get_active_sessions(
            holder_did=holder_did,
            verifier_did=verifier_did
        )
        
        return JSONResponse(
            content={
                "sessions": sessions,
                "total": len(sessions),
                "filters": {
                    "holder_did": holder_did,
                    "verifier_did": verifier_did
                }
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get active sessions: {str(e)}"
        )


@router.get("/sessions/stats")
async def get_session_statistics(
    session_manager: SessionManager = Depends(get_session_manager)
):
    """Get session manager statistics"""
    stats = await session_manager.get_statistics()
    return JSONResponse(content=stats)


@router.get("/sessions/health")
async def session_health_check(
    session_manager: SessionManager = Depends(get_session_manager)
):
    """Check session manager health"""
    is_healthy = await session_manager.health_check()
    
    return JSONResponse(
        content={
            "healthy": is_healthy,
            "service": "session_manager",
            "timestamp": datetime.now().isoformat()
        },
        status_code=200 if is_healthy else 503
    ) 