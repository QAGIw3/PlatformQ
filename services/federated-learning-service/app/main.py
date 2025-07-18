"""
Federated Learning Service

Privacy-preserving federated learning with DAO governance.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, HTTPException, Header, Query
from typing import Dict, Any, List, Optional
from datetime import datetime
import logging
import asyncio
import uuid

from platformq_shared import (
    create_base_app,
    EventProcessor,
    ServiceClients,
    add_error_handlers
)
from platformq_shared.config import ConfigLoader
from platformq_shared.event_publisher import EventPublisher

from .api.deps import (
    get_current_tenant_and_user,
    get_db_session,
    get_event_publisher,
    get_api_key_crud,
    get_user_crud,
    get_password_verifier
)
from .repository import (
    FLSessionRepository,
    ParticipantRepository,
    ModelUpdateRepository,
    AggregatedModelRepository,
    DAOProposalRepository,
    DAOVoteRepository
)
from .event_processors import (
    FLSessionProcessor,
    ModelAggregationProcessor,
    DAOGovernanceProcessor,
    PrivacyMonitoringProcessor
)
from .orchestrator import FederatedLearningOrchestrator
from .participant_service import ParticipantService
from .aggregation_service import AggregationService
from .privacy_engine import PrivacyEngine
from .dao.federated_dao_governance import (
    FederatedLearningDAO,
    ProposalType,
    VoteType
)

logger = logging.getLogger(__name__)

# Service components
session_processor = None
aggregation_processor = None
dao_processor = None
privacy_processor = None
orchestrator = None
participant_service = None
aggregation_service = None
privacy_engine = None
fl_dao = None
service_clients = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global session_processor, aggregation_processor, dao_processor, privacy_processor
    global orchestrator, participant_service, aggregation_service, privacy_engine, fl_dao, service_clients
    
    # Startup
    logger.info("Starting Federated Learning Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    settings = config_loader.load_settings()
    
    # Initialize service clients
    service_clients = ServiceClients(base_timeout=30.0, max_retries=3)
    app.state.service_clients = service_clients
    
    # Initialize repositories
    app.state.session_repo = FLSessionRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    app.state.participant_repo = ParticipantRepository(get_db_session)
    app.state.model_update_repo = ModelUpdateRepository(get_db_session)
    app.state.aggregated_model_repo = AggregatedModelRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    app.state.proposal_repo = DAOProposalRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    app.state.vote_repo = DAOVoteRepository(
        get_db_session,
        event_publisher=app.state.event_publisher
    )
    
    # Initialize core services
    orchestrator = FederatedLearningOrchestrator(
        session_repo=app.state.session_repo,
        participant_repo=app.state.participant_repo,
        ignite_config={
            'host': settings.get('ignite_host', 'ignite'),
            'port': int(settings.get('ignite_port', 10800))
        }
    )
    await orchestrator.initialize()
    app.state.orchestrator = orchestrator
    
    participant_service = ParticipantService(
        participant_repo=app.state.participant_repo,
        vc_service_url=settings.get('vc_service_url', 'http://verifiable-credential-service:8000')
    )
    await participant_service.initialize()
    app.state.participant_service = participant_service
    
    aggregation_service = AggregationService(
        model_update_repo=app.state.model_update_repo,
        aggregated_model_repo=app.state.aggregated_model_repo
    )
    app.state.aggregation_service = aggregation_service
    
    privacy_engine = PrivacyEngine(
        epsilon=float(settings.get('default_privacy_epsilon', 1.0)),
        delta=float(settings.get('default_privacy_delta', 1e-5))
    )
    app.state.privacy_engine = privacy_engine
    
    # Initialize DAO if enabled
    if settings.get('dao_enabled', 'false').lower() == 'true':
        fl_dao = FederatedLearningDAO(
            contract_address=settings.get('fl_dao_contract_address'),
            token_address=settings.get('fl_token_address'),
            web3_provider=settings.get('web3_provider_url')
        )
        await fl_dao.initialize()
        app.state.fl_dao = fl_dao
    
    # Initialize event processors
    session_processor = FLSessionProcessor(
        service_name="fl-service",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        session_repo=app.state.session_repo,
        participant_repo=app.state.participant_repo,
        orchestrator=orchestrator,
        service_clients=service_clients
    )
    
    aggregation_processor = ModelAggregationProcessor(
        service_name="fl-aggregation",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        model_update_repo=app.state.model_update_repo,
        aggregated_model_repo=app.state.aggregated_model_repo,
        session_repo=app.state.session_repo,
        aggregation_service=aggregation_service,
        privacy_engine=privacy_engine
    )
    
    if fl_dao:
        dao_processor = DAOGovernanceProcessor(
            service_name="fl-dao",
            pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
            proposal_repo=app.state.proposal_repo,
            vote_repo=app.state.vote_repo,
            fl_dao=fl_dao,
            service_clients=service_clients
        )
    
    privacy_processor = PrivacyMonitoringProcessor(
        service_name="fl-privacy",
        pulsar_url=settings.get("pulsar_url", "pulsar://pulsar:6650"),
        privacy_engine=privacy_engine,
        service_clients=service_clients
    )
    
    # Start event processors
    processors = [session_processor, aggregation_processor, privacy_processor]
    if dao_processor:
        processors.append(dao_processor)
        
    await asyncio.gather(*[p.start() for p in processors])
    
    logger.info("Federated Learning Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Federated Learning Service...")
    
    # Stop event processors
    await asyncio.gather(*[p.stop() for p in processors if p])
    
    # Cleanup resources
    if orchestrator:
        await orchestrator.cleanup()
    if participant_service:
        await participant_service.cleanup()
    if fl_dao:
        await fl_dao.cleanup()
        
    logger.info("Federated Learning Service shutdown complete")


# Create app with enhanced patterns
app = create_base_app(
    service_name="federated-learning-service",
    db_session_dependency=get_db_session,
    api_key_crud_dependency=get_api_key_crud,
    user_crud_dependency=get_user_crud,
    password_verifier_dependency=get_password_verifier,
    event_processors=[
        session_processor, aggregation_processor, 
        dao_processor, privacy_processor
    ] if all([session_processor, aggregation_processor, privacy_processor]) else []
)

# Set lifespan
app.router.lifespan_context = lifespan

# Service root endpoint
@app.get("/")
def read_root():
    return {
        "service": "federated-learning-service",
        "version": "2.0",
        "features": [
            "federated-learning",
            "differential-privacy",
            "homomorphic-encryption",
            "secure-aggregation",
            "dao-governance",
            "verifiable-credentials",
            "event-driven"
        ]
    }


# Health check with feature status
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health = {
        "status": "healthy",
        "service": "federated-learning-service",
        "timestamp": datetime.utcnow().isoformat(),
        "features": {
            "homomorphic_encryption": True,
            "differential_privacy": True,
            "secure_aggregation": True,
            "zero_knowledge_proofs": True,
            "dao_governance": fl_dao is not None
        }
    }
    
    # Check component health
    if hasattr(app.state, "orchestrator"):
        health["components"] = {
            "orchestrator": "active" if app.state.orchestrator.is_running else "inactive"
        }
        
    return health


# API Endpoints using new patterns
@app.post("/api/v1/sessions")
async def create_fl_session(
    session_name: str,
    model_type: str,
    min_participants: int = 3,
    max_participants: int = 10,
    total_rounds: int = 10,
    learning_rate: float = 0.01,
    privacy_config: Optional[Dict[str, Any]] = None,
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """Create a new federated learning session"""
    
    # Validate model type
    supported_models = ["linear_regression", "logistic_regression", "neural_network", "random_forest"]
    if model_type not in supported_models:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported model type. Supported: {supported_models}"
        )
        
    # Publish session request event
    publisher.publish_event(
        topic=f"persistent://platformq/{context['tenant_id']}/fl-session-request-events",
        event={
            "session_name": session_name,
            "model_type": model_type,
            "min_participants": min_participants,
            "max_participants": max_participants,
            "total_rounds": total_rounds,
            "learning_rate": learning_rate,
            "privacy_config": privacy_config or {
                "differential_privacy": True,
                "epsilon": 1.0,
                "homomorphic_encryption": False,
                "secure_aggregation": True
            },
            "created_by": context["user"]["id"],
            "tenant_id": context["tenant_id"],
            "requested_at": datetime.utcnow().isoformat()
        }
    )
    
    return {
        "status": "session_requested",
        "message": "Federated learning session creation requested"
    }


@app.get("/api/v1/sessions")
async def list_sessions(
    status: Optional[str] = None,
    limit: int = 100,
    context: dict = Depends(get_current_tenant_and_user)
):
    """List federated learning sessions"""
    
    session_repo = app.state.session_repo
    sessions = session_repo.list_sessions(
        tenant_id=context["tenant_id"],
        status=status,
        limit=limit
    )
    
    return {
        "sessions": sessions,
        "total": len(sessions)
    }


@app.get("/api/v1/sessions/{session_id}")
async def get_session(
    session_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get federated learning session details"""
    
    session_repo = app.state.session_repo
    session = session_repo.get_session(session_id)
    
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
        
    # Check access
    if session["tenant_id"] != context["tenant_id"] and "admin" not in context["user"].get("roles", []):
        raise HTTPException(status_code=403, detail="Access denied")
        
    # Get participants
    participant_repo = app.state.participant_repo
    participants = participant_repo.get_session_participants(session_id)
    
    # Get latest model if available
    aggregated_model_repo = app.state.aggregated_model_repo
    latest_model = aggregated_model_repo.get_latest_model(session_id)
    
    return {
        "session": session,
        "participants": participants,
        "latest_model": latest_model
    }


@app.post("/api/v1/sessions/{session_id}/join")
async def join_session(
    session_id: str,
    public_key: str,
    compute_resources: Dict[str, Any],
    data_samples: int,
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """Join a federated learning session as participant"""
    
    # Verify session exists and is accepting participants
    session_repo = app.state.session_repo
    session = session_repo.get_session(session_id)
    
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
        
    if session["status"] not in ["pending", "active"]:
        raise HTTPException(
            status_code=400,
            detail="Session not accepting participants"
        )
        
    # Get trust score
    trust_score = await app.state.participant_service.get_trust_score(
        context["user"]["id"]
    )
    
    if trust_score < 0.5:
        raise HTTPException(
            status_code=403,
            detail="Insufficient trust score to participate"
        )
        
    # Publish participant joined event
    publisher.publish_event(
        topic=f"persistent://platformq/{context['tenant_id']}/participant-joined-events",
        event={
            "session_id": session_id,
            "participant_id": context["user"]["id"],
            "participant_name": context["user"].get("username", "Unknown"),
            "public_key": public_key,
            "compute_resources": compute_resources,
            "data_samples": data_samples,
            "trust_score": trust_score,
            "joined_at": datetime.utcnow().isoformat()
        }
    )
    
    return {
        "status": "join_requested",
        "message": "Join request submitted"
    }


@app.post("/api/v1/sessions/{session_id}/submit-update")
async def submit_model_update(
    session_id: str,
    round_number: int,
    encrypted_gradients: str,  # Base64 encoded
    local_loss: float,
    local_accuracy: float,
    num_samples: int,
    computation_time: float,
    noise_scale: Optional[float] = None,
    clipping_threshold: Optional[float] = None,
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """Submit model update for current round"""
    
    # Verify participant is registered
    participant_repo = app.state.participant_repo
    participants = participant_repo.get_session_participants(session_id)
    
    if not any(p["participant_id"] == context["user"]["id"] for p in participants):
        raise HTTPException(
            status_code=403,
            detail="Not a registered participant"
        )
        
    # Publish model update event
    publisher.publish_event(
        topic=f"persistent://platformq/{context['tenant_id']}/model-update-submitted-events",
        event={
            "session_id": session_id,
            "round_number": round_number,
            "participant_id": context["user"]["id"],
            "encrypted_gradients": encrypted_gradients,
            "local_loss": local_loss,
            "local_accuracy": local_accuracy,
            "num_samples": num_samples,
            "computation_time": computation_time,
            "noise_scale": noise_scale,
            "clipping_threshold": clipping_threshold,
            "submitted_at": datetime.utcnow().isoformat()
        }
    )
    
    return {
        "status": "update_submitted",
        "message": "Model update submitted successfully"
    }


# DAO Governance Endpoints
@app.post("/api/v1/dao/proposals")
async def create_dao_proposal(
    proposal_type: ProposalType,
    title: str,
    description: str,
    parameters: Dict[str, Any],
    proposer_address: str,
    private_key: str = Header(..., description="Blockchain private key"),
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """Create a new DAO proposal for federated learning governance"""
    
    if not fl_dao:
        raise HTTPException(
            status_code=503,
            detail="DAO governance not enabled"
        )
        
    # Validate proposal parameters based on type
    if proposal_type == ProposalType.CREATE_FL_SESSION:
        required_params = ["model_type", "min_participants", "max_participants", "rounds", "learning_rate"]
        if not all(param in parameters for param in required_params):
            raise HTTPException(
                status_code=400,
                detail=f"Missing required parameters: {required_params}"
            )
            
    # Publish proposal event
    publisher.publish_event(
        topic=f"persistent://platformq/{context['tenant_id']}/dao-proposal-submitted-events",
        event={
            "proposal_type": proposal_type.value,
            "title": title,
            "description": description,
            "parameters": parameters,
            "proposer_address": proposer_address,
            "private_key": private_key,
            "tenant_id": context["tenant_id"],
            "submitted_at": datetime.utcnow().isoformat()
        }
    )
    
    return {
        "status": "proposal_submitted",
        "message": "DAO proposal submitted"
    }


@app.get("/api/v1/dao/proposals")
async def list_proposals(
    status: Optional[str] = "active",
    context: dict = Depends(get_current_tenant_and_user)
):
    """List DAO proposals"""
    
    proposal_repo = app.state.proposal_repo
    proposals = proposal_repo.get_active_proposals(
        tenant_id=context["tenant_id"] if status == "active" else None
    )
    
    return {
        "proposals": proposals,
        "total": len(proposals)
    }


@app.post("/api/v1/dao/proposals/{proposal_id}/vote")
async def vote_on_proposal(
    proposal_id: str,
    vote_type: VoteType,
    voter_address: str,
    private_key: str = Header(..., description="Blockchain private key"),
    context: dict = Depends(get_current_tenant_and_user),
    publisher: EventPublisher = Depends(get_event_publisher)
):
    """Vote on a DAO proposal"""
    
    if not fl_dao:
        raise HTTPException(
            status_code=503,
            detail="DAO governance not enabled"
        )
        
    # Publish vote event
    publisher.publish_event(
        topic=f"persistent://platformq/{context['tenant_id']}/dao-vote-submitted-events",
        event={
            "proposal_id": proposal_id,
            "vote_type": vote_type.value,
            "voter_address": voter_address,
            "private_key": private_key,
            "voted_at": datetime.utcnow().isoformat()
        }
    )
    
    return {
        "status": "vote_submitted",
        "message": "Vote submitted successfully"
    }


@app.get("/api/v1/dao/proposals/{proposal_id}/votes")
async def get_proposal_votes(
    proposal_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get vote summary for a proposal"""
    
    vote_repo = app.state.vote_repo
    votes = vote_repo.get_proposal_votes(proposal_id)
    
    return votes


# Privacy optimization endpoint (legacy)
@app.post('/api/v1/optimize-dag')
async def optimize_dag(dag_data: Dict):
    """Privacy-preserving DAG optimization"""
    import hashlib
    
    # Mask sensitive parameters
    for key in dag_data:
        if 'private' in key:
            dag_data[key] = hashlib.sha256(dag_data[key].encode()).hexdigest()
            
    return dag_data 