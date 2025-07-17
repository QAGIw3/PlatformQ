"""
Federated Learning Coordinator Service

This service orchestrates privacy-preserving federated learning sessions
across multiple tenants with verifiable credential-based access control.
"""

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from fastapi import Depends, HTTPException, BackgroundTasks
import httpx

from platformq.shared.base_service import create_base_app
from platformq.shared.event_publisher import EventPublisher
from .core.config import settings
from platformq_shared.jwt import get_current_tenant_and_user
from .core.homomorphic_encryption import (
    CKKSEncryption, PaillierEncryption, SecureAggregator,
    HEContext, EncryptedTensor, DifferentialPrivacyWithHE
)
from .core.secure_aggregation import (
    SecureAggregationProtocol, AggregationStrategy,
    ParticipantUpdate, AggregationResult
)
from .core.privacy_preserving import (
    DifferentialPrivacy, PrivacyParameters, PrivacyMechanism,
    SecureComputation, ZeroKnowledgeProofs, PrivateInformationRetrieval
)
from .schemas import (
    FederatedLearningSessionRequest,
    ParticipantJoinRequest,
    ModelUpdateSubmission,
    SessionStatus,
    DataQualityRequirements,
)
from .api import api_router
from .services import SessionService, ParticipantService, AggregationService
from pyignite import Client as IgniteClient
import hashlib

logger = logging.getLogger(__name__)

# Initialize base app
app = create_base_app(service_name="federated-learning-service")

# Global state managers
event_publisher = EventPublisher(pulsar_url=settings.pulsar_url)
ignite_client = IgniteClient()
http_client = httpx.AsyncClient()

session_service = SessionService(ignite_client, event_publisher)
participant_service = ParticipantService(ignite_client, http_client)
aggregation_service = AggregationService(ignite_client, event_publisher)

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    event_publisher.connect()
    ignite_client.connect([(node.split(":")[0], int(node.split(":")[1])) 
                           for node in settings.ignite_nodes])
    logger.info("Federated Learning Coordinator initialized")

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    if event_publisher:
        event_publisher.close()
    if ignite_client:
        ignite_client.close()
    await http_client.aclose()


# API Endpoints
app.include_router(api_router, prefix="/api/v1")


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "federated-learning-service",
        "timestamp": datetime.utcnow().isoformat(),
        "features": {
            "homomorphic_encryption": True,
            "differential_privacy": True,
            "secure_aggregation": True,
            "zero_knowledge_proofs": True
        }
    }

@app.post('/api/v1/optimize-dag')
async def optimize_dag(dag_data: Dict):
    # Privacy-preserving tuning
    # Mask sensitive params
    for key in dag_data:
        if 'private' in key:
            dag_data[key] = hashlib.sha256(dag_data[key].encode()).hexdigest()
    return dag_data


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 