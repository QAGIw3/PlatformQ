"""
Federated Learning API endpoints
"""

from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

router = APIRouter()


class FederatedSessionRequest(BaseModel):
    """Federated learning session request"""
    name: str = Field(..., description="Session name")
    model_config: Dict[str, Any] = Field(..., description="Base model configuration")
    dataset_config: Dict[str, Any] = Field(..., description="Dataset requirements")
    training_config: Dict[str, Any] = Field(..., description="Training hyperparameters")
    privacy_config: Dict[str, Any] = Field(default={}, description="Privacy settings")
    convergence_criteria: Dict[str, Any] = Field(default={}, description="Convergence conditions")
    max_rounds: int = Field(default=100, description="Maximum training rounds")


class SessionResponse(BaseModel):
    """Session response"""
    session_id: str
    status: str
    message: str


@router.post("/sessions", response_model=SessionResponse)
async def create_federated_session(request: FederatedSessionRequest) -> SessionResponse:
    """Create a new federated learning session"""
    try:
        from ..main import federated_coordinator
        
        if not federated_coordinator:
            raise HTTPException(status_code=503, detail="Federated coordinator not available")
        
        session_id = await federated_coordinator.create_session(request.dict())
        
        return SessionResponse(
            session_id=session_id,
            status="created",
            message="Federated learning session created successfully"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating federated session: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/sessions/{session_id}")
async def get_session_status(session_id: str) -> Dict[str, Any]:
    """Get federated learning session status"""
    try:
        from ..main import federated_coordinator
        
        if not federated_coordinator:
            raise HTTPException(status_code=503, detail="Federated coordinator not available")
        
        status = await federated_coordinator.get_session_status(session_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting session status: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.post("/sessions/{session_id}/stop")
async def stop_session(session_id: str) -> Dict[str, Any]:
    """Stop a federated learning session"""
    try:
        from ..main import federated_coordinator
        
        if not federated_coordinator:
            raise HTTPException(status_code=503, detail="Federated coordinator not available")
        
        success = await federated_coordinator.stop_session(session_id)
        
        return {
            "session_id": session_id,
            "stopped": success,
            "message": "Session stopped successfully" if success else "Failed to stop session"
        }
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error stopping session: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/sessions")
async def list_sessions(
    status: str = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """List federated learning sessions"""
    try:
        from ..main import federated_coordinator
        
        if not federated_coordinator:
            raise HTTPException(status_code=503, detail="Federated coordinator not available")
        
        # Get all sessions
        sessions = []
        for session_id, session in federated_coordinator.sessions.items():
            session_info = {
                "session_id": session_id,
                "name": session["config"].get("name"),
                "status": session["status"].value,
                "current_round": session["current_round"],
                "created_at": session["created_at"].isoformat()
            }
            sessions.append(session_info)
        
        # Filter by status if provided
        if status:
            sessions = [s for s in sessions if s["status"] == status]
        
        # Sort by creation time (newest first)
        sessions.sort(key=lambda x: x["created_at"], reverse=True)
        
        return sessions[:limit]
        
    except Exception as e:
        logger.error(f"Error listing sessions: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/metrics")
async def get_federated_metrics() -> Dict[str, Any]:
    """Get federated learning metrics"""
    try:
        from ..main import federated_coordinator
        
        if not federated_coordinator:
            raise HTTPException(status_code=503, detail="Federated coordinator not available")
        
        metrics = await federated_coordinator.get_federated_metrics()
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting federated metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/clients")
async def get_available_clients(
    min_data_samples: int = 0,
    reliability_threshold: float = 0.0
) -> List[Dict[str, Any]]:
    """Get available federated learning clients"""
    try:
        from ..main import federated_coordinator
        
        if not federated_coordinator:
            raise HTTPException(status_code=503, detail="Federated coordinator not available")
        
        # Get available clients
        client_ids = await federated_coordinator.client_manager.get_available_clients(
            min_data_samples=min_data_samples,
            reliability_threshold=reliability_threshold
        )
        
        # Get client details
        clients = []
        for client_id in client_ids:
            # This would get actual client information
            client_info = {
                "client_id": client_id,
                "status": "available",
                "data_samples": 1000,
                "reliability_score": 0.95
            }
            clients.append(client_info)
        
        return clients
        
    except Exception as e:
        logger.error(f"Error getting clients: {e}")
        raise HTTPException(status_code=500, detail="Internal server error") 