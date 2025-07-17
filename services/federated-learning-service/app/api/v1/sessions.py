from fastapi import APIRouter, Depends, BackgroundTasks, HTTPException
from typing import Dict, Any, List, Optional
import uuid
import time
from datetime import datetime
import logging
import hashlib
import json
import asyncio

from platformq_shared.jwt import get_current_tenant_and_user
from ....schemas import (
    FederatedLearningSessionRequest,
    ParticipantJoinRequest,
    ModelUpdateSubmission,
    SessionStatus,
)
from ....core.secure_aggregation import SecureAggregationProtocol, AggregationStrategy, ParticipantUpdate
from ....services import SessionService, ParticipantService, AggregationService
from ....main import session_service, participant_service, aggregation_service

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("", response_model=Dict[str, Any])
async def create_federated_learning_session(
    request: FederatedLearningSessionRequest,
    context: dict = Depends(get_current_tenant_and_user),
    session_service: SessionService = Depends(lambda: session_service)
):
    """Create a new federated learning session"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    session_data = session_service.create_session(request, tenant_id, user_id)
    
    return {
        "session_id": session_data["session_id"],
        "status": "CREATED",
        "coordinator_endpoint": session_data["coordinator_endpoint"],
        "min_participants": request.training_parameters.get("min_participants", 2),
        "total_rounds": session_data["total_rounds"]
    }


@router.post("/{session_id}/join", response_model=Dict[str, Any])
async def join_federated_learning_session(
    session_id: str,
    request: ParticipantJoinRequest,
    context: dict = Depends(get_current_tenant_and_user),
    participant_service: ParticipantService = Depends(lambda: participant_service)
):
    """Join an existing federated learning session"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    
    try:
        response = await participant_service.join_session(
            session_id, request.dict(), tenant_id, user_id
        )
        return response
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ConnectionError as e:
        raise HTTPException(status_code=503, detail=str(e))


@router.post("/{session_id}/updates", response_model=Dict[str, Any])
async def submit_model_update(
    session_id: str,
    request: ModelUpdateSubmission,
    context: dict = Depends(get_current_tenant_and_user),
    aggregation_service: AggregationService = Depends(lambda: aggregation_service)
):
    """Submit a model update for the current round"""
    tenant_id = context["tenant_id"]
    user_id = context["user_id"]
    participant_id = f"{tenant_id}:{user_id}"

    # This is a simplified version. In a real application, you would
    # probably have a more sophisticated way of handling this.
    try:
        response = await aggregation_service.submit_model_update(
            session_id, request.dict(), participant_id
        )
        return response
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))


@router.get("/{session_id}/status", response_model=SessionStatus)
async def get_session_status(
    session_id: str,
    context: dict = Depends(get_current_tenant_and_user),
    session_service: SessionService = Depends(lambda: session_service)
):
    """Get the current status of a federated learning session"""
    session = await session_service.get_session_status(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


@router.get("/{session_id}/model", response_model=Dict[str, Any])
async def get_aggregated_model(
    session_id: str,
    round_number: Optional[int] = None,
    context: dict = Depends(get_current_tenant_and_user),
    aggregation_service: AggregationService = Depends(lambda: aggregation_service)
):
    """Get the aggregated model for a specific round"""
    model = await aggregation_service.get_aggregated_model(session_id, round_number)
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    return model

@router.post("/{session_id}/encrypt", response_model=Dict[str, Any])
async def encrypt_model_update(
    session_id: str,
    model_weights: Dict[str, List[float]],
    context: dict = Depends(get_current_tenant_and_user),
    aggregation_service: AggregationService = Depends(lambda: aggregation_service)
):
    """Encrypt model weights using homomorphic encryption"""
    try:
        response = await aggregation_service.encrypt_model_update(
            session_id, model_weights, context
        )
        return response
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.post("/{session_id}/generate_zkp", response_model=Dict[str, Any])
async def generate_zero_knowledge_proof(
    session_id: str,
    proof_type: str,
    statement: str,
    public_inputs: Dict[str, Any],
    context: dict = Depends(get_current_tenant_and_user),
    aggregation_service: AggregationService = Depends(lambda: aggregation_service)
):
    """Generate zero-knowledge proof for model validation"""
    try:
        response = await aggregation_service.generate_zero_knowledge_proof(
            proof_type, statement, public_inputs
        )
        return response
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/verify_zkp", response_model=Dict[str, Any])
async def verify_zero_knowledge_proof(
    proof_data: Dict[str, Any],
    context: dict = Depends(get_current_tenant_and_user),
    aggregation_service: AggregationService = Depends(lambda: aggregation_service)
):
    """Verify a zero-knowledge proof"""
    response = await aggregation_service.verify_zero_knowledge_proof(proof_data)
    return response

@router.get("/{session_id}/privacy_report", response_model=Dict[str, Any])
async def get_privacy_report(
    session_id: str,
    context: dict = Depends(get_current_tenant_and_user),
    aggregation_service: AggregationService = Depends(lambda: aggregation_service)
):
    """Get privacy budget and metrics for a session"""
    report = await aggregation_service.get_privacy_report(session_id)
    if not report:
        raise HTTPException(status_code=404, detail="Session not found")
    return report

@router.post("/{session_id}/secure_aggregate", response_model=Dict[str, Any])
async def trigger_secure_aggregation(
    session_id: str,
    round_number: int,
    min_participants: Optional[int] = None,
    context: dict = Depends(get_current_tenant_and_user),
    aggregation_service: AggregationService = Depends(lambda: aggregation_service)
):
    """Manually trigger secure aggregation for a round"""
    try:
        response = await aggregation_service.trigger_secure_aggregation(
            session_id, round_number, min_participants, context["user_id"]
        )
        return response
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) 