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

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Header, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
import httpx

from platformq_shared.jwt import get_current_tenant_and_user
from platformq_shared.config import ConfigLoader
from platformq_shared.event_publisher import EventPublisher

from .models import FederatedLearningSession, DAOProposal, DAOVote, VotingDelegation
from .database import get_db
from .orchestrator import FederatedLearningOrchestrator
from .participant_service import ParticipantService
from .aggregation_service import AggregationService
from .privacy_engine import PrivacyEngine
from .dao.federated_dao_governance import (
    FederatedLearningDAO,
    ProposalType,
    VoteType,
    FLGovernanceToken,
    FLDAOContract
)

from pyignite import Client as IgniteClient
import hashlib

logger = logging.getLogger(__name__)

# Initialize base app
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global state managers
event_publisher = EventPublisher(pulsar_url=settings.pulsar_url)
ignite_client = IgniteClient()
http_client = httpx.AsyncClient()

session_service = SessionService(ignite_client, event_publisher)
participant_service = ParticipantService(ignite_client, http_client)
aggregation_service = AggregationService(ignite_client, event_publisher)

# Initialize DAO governance if enabled
config_loader = ConfigLoader()
fl_dao = None

if config_loader.get_setting("DAO_GOVERNANCE_ENABLED", "false").lower() == "true":
    fl_dao = FederatedLearningDAO(
        web3_provider_url=config_loader.get_setting("WEB3_PROVIDER_URL", "http://localhost:8545"),
        dao_contract_address=config_loader.get_setting("FL_DAO_CONTRACT_ADDRESS", ""),
        dao_contract_abi=FLDAOContract.CONTRACT_ABI,
        token_contract_address=config_loader.get_setting("FL_TOKEN_CONTRACT_ADDRESS", ""),
        token_contract_abi=FLGovernanceToken.CONTRACT_ABI
    )


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


@app.post("/api/v1/dao/proposals")
async def create_dao_proposal(
    proposal_type: ProposalType,
    title: str,
    description: str,
    parameters: Dict[str, Any],
    proposer_address: str,
    private_key: str = Header(..., description="Blockchain private key"),
    current_user: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """
    Create a new DAO proposal for federated learning governance
    """
    if not fl_dao:
        raise HTTPException(
            status_code=503,
            detail="DAO governance not enabled"
        )
        
    try:
        # Validate proposal parameters based on type
        if proposal_type == ProposalType.CREATE_FL_SESSION:
            required_params = ["model_type", "min_participants", "max_participants", "rounds", "learning_rate"]
            if not all(param in parameters for param in required_params):
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing required parameters for FL session proposal: {required_params}"
                )
                
        # Create proposal
        result = await fl_dao.create_proposal(
            proposal_type=proposal_type,
            title=title,
            description=description,
            parameters=parameters,
            proposer_address=proposer_address,
            private_key=private_key
        )
        
        if result["success"]:
            # Store proposal in database
            proposal_record = DAOProposal(
                id=result["proposal_id"],
                proposal_type=proposal_type.value,
                title=title,
                description=description,
                parameters=json.dumps(parameters),
                proposer=proposer_address,
                created_by=current_user["id"],
                voting_deadline=result["voting_deadline"],
                execution_deadline=result["execution_deadline"],
                tx_hash=result["tx_hash"],
                created_at=datetime.utcnow()
            )
            
            db.add(proposal_record)
            db.commit()
            
            # Publish event
            await event_publisher.publish(
                "FL_DAO_PROPOSAL_CREATED",
                {
                    "proposal_id": result["proposal_id"],
                    "proposal_type": proposal_type.value,
                    "title": title,
                    "proposer": proposer_address,
                    "voting_deadline": result["voting_deadline"]
                }
            )
            
            return result
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "Failed to create proposal")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating DAO proposal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/dao/proposals/{proposal_id}/vote")
async def cast_vote_on_proposal(
    proposal_id: str,
    vote: VoteType,
    voter_address: str,
    delegation: Optional[str] = None,
    private_key: str = Header(..., description="Blockchain private key"),
    current_user: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """
    Cast vote on a DAO proposal
    """
    if not fl_dao:
        raise HTTPException(
            status_code=503,
            detail="DAO governance not enabled"
        )
        
    try:
        # Cast vote
        result = await fl_dao.cast_vote(
            proposal_id=proposal_id,
            vote=vote,
            voter_address=voter_address,
            private_key=private_key,
            delegation=delegation
        )
        
        if result["success"]:
            # Store vote record
            vote_record = DAOVote(
                id=str(uuid.uuid4()),
                proposal_id=proposal_id,
                voter=voter_address,
                vote=vote.value,
                voting_power=result["voting_power"],
                tx_hash=result["tx_hash"],
                created_by=current_user["id"],
                created_at=datetime.utcnow()
            )
            
            db.add(vote_record)
            db.commit()
            
            # Publish event
            await event_publisher.publish(
                "FL_DAO_VOTE_CAST",
                {
                    "proposal_id": proposal_id,
                    "voter": voter_address,
                    "vote": vote.value,
                    "voting_power": result["voting_power"]
                }
            )
            
            return result
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "Failed to cast vote")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error casting vote: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/dao/proposals/{proposal_id}/execute")
async def execute_dao_proposal(
    proposal_id: str,
    executor_address: str,
    private_key: str = Header(..., description="Blockchain private key"),
    current_user: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """
    Execute an approved DAO proposal
    """
    if not fl_dao:
        raise HTTPException(
            status_code=503,
            detail="DAO governance not enabled"
        )
        
    try:
        # Execute proposal
        result = await fl_dao.execute_proposal(
            proposal_id=proposal_id,
            executor_address=executor_address,
            private_key=private_key
        )
        
        if result["success"]:
            # Update proposal status
            proposal = db.query(DAOProposal).filter(
                DAOProposal.id == proposal_id
            ).first()
            
            if proposal:
                proposal.executed = True
                proposal.executed_by = executor_address
                proposal.execution_tx_hash = result.get("execution_tx_hash")
                proposal.executed_at = datetime.utcnow()
                
                # If FL session was created, link it
                if result.get("action") == "create_fl_session" and result.get("session_id"):
                    proposal.fl_session_id = result["session_id"]
                    
                db.commit()
                
            # Publish event
            await event_publisher.publish(
                "FL_DAO_PROPOSAL_EXECUTED",
                {
                    "proposal_id": proposal_id,
                    "executor": executor_address,
                    "action": result.get("action"),
                    "result": result
                }
            )
            
            return result
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "Failed to execute proposal")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing proposal: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/dao/proposals/{proposal_id}/status")
async def get_proposal_status(
    proposal_id: str,
    current_user: dict = Depends(get_current_tenant_and_user)
):
    """
    Get current status of a DAO proposal
    """
    if not fl_dao:
        raise HTTPException(
            status_code=503,
            detail="DAO governance not enabled"
        )
        
    try:
        status = await fl_dao.get_proposal_status(proposal_id)
        
        if not status["found"]:
            raise HTTPException(
                status_code=404,
                detail="Proposal not found"
            )
            
        return status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting proposal status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/dao/delegate")
async def delegate_voting_power(
    delegator: str,
    delegate: str,
    private_key: str = Header(..., description="Blockchain private key"),
    current_user: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """
    Delegate voting power to another address
    """
    if not fl_dao:
        raise HTTPException(
            status_code=503,
            detail="DAO governance not enabled"
        )
        
    try:
        result = await fl_dao.delegate_voting_power(
            delegator=delegator,
            delegate=delegate,
            private_key=private_key
        )
        
        if result["success"]:
            # Store delegation record
            delegation_record = VotingDelegation(
                id=str(uuid.uuid4()),
                delegator=delegator,
                delegate=delegate,
                tx_hash=result["tx_hash"],
                created_by=current_user["id"],
                created_at=datetime.utcnow()
            )
            
            db.add(delegation_record)
            db.commit()
            
            return result
        else:
            raise HTTPException(
                status_code=400,
                detail=result.get("error", "Failed to delegate voting power")
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error delegating voting power: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/dao/proposals")
async def list_dao_proposals(
    status: Optional[str] = Query(None, description="Filter by status"),
    proposer: Optional[str] = Query(None, description="Filter by proposer"),
    skip: int = 0,
    limit: int = 20,
    current_user: dict = Depends(get_current_tenant_and_user),
    db: Session = Depends(get_db)
):
    """
    List DAO proposals with filters
    """
    try:
        query = db.query(DAOProposal)
        
        if status:
            if status == "active":
                query = query.filter(
                    DAOProposal.voting_deadline > datetime.utcnow(),
                    DAOProposal.executed == False
                )
            elif status == "executed":
                query = query.filter(DAOProposal.executed == True)
            elif status == "expired":
                query = query.filter(
                    DAOProposal.voting_deadline < datetime.utcnow(),
                    DAOProposal.executed == False
                )
                
        if proposer:
            query = query.filter(DAOProposal.proposer == proposer)
            
        total = query.count()
        proposals = query.offset(skip).limit(limit).all()
        
        # Get voting results for each proposal
        results = []
        for proposal in proposals:
            votes = db.query(DAOVote).filter(
                DAOVote.proposal_id == proposal.id
            ).all()
            
            voting_results = {
                "for": sum(v.voting_power for v in votes if v.vote == "for"),
                "against": sum(v.voting_power for v in votes if v.vote == "against"),
                "abstain": sum(v.voting_power for v in votes if v.vote == "abstain")
            }
            
            results.append({
                "proposal": {
                    "id": proposal.id,
                    "type": proposal.proposal_type,
                    "title": proposal.title,
                    "description": proposal.description,
                    "proposer": proposal.proposer,
                    "created_at": proposal.created_at.isoformat(),
                    "voting_deadline": proposal.voting_deadline.isoformat(),
                    "executed": proposal.executed
                },
                "voting_results": voting_results
            })
            
        return {
            "total": total,
            "proposals": results
        }
        
    except Exception as e:
        logger.error(f"Error listing proposals: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Integrate DAO governance with FL session creation
async def create_fl_session_dao_governed(session_config: Dict[str, Any]) -> str:
    """Create FL session through DAO governance"""
    # This would be called by the DAO execution
    session_id = str(uuid.uuid4())
    
    # Create session with DAO-approved parameters
    session = FederatedLearningSession(
        id=session_id,
        model_type=session_config["model_type"],
        min_participants=session_config["min_participants"],
        max_participants=session_config["max_participants"],
        rounds=session_config["rounds"],
        learning_rate=session_config["learning_rate"],
        privacy_budget=session_config.get("privacy_budget", 1.0),
        reward_pool=session_config.get("reward_pool", 0),
        dao_governed=True,
        created_at=datetime.utcnow()
    )
    
    # Store and initialize session
    # ... session initialization logic ...
    
    return session_id


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 