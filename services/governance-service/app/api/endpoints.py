"""
API endpoints for DAO governance service.
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List
from datetime import datetime

from ..core import GovernanceManager

router = APIRouter()


# Request/Response models
class ProposalCreateRequest(BaseModel):
    title: str = Field(..., description="Proposal title", max_length=200)
    description: str = Field(..., description="Detailed proposal description")
    proposal_type: str = Field(..., description="Type of proposal (parameter_change, treasury_allocation, etc.)")
    voting_mechanism: str = Field("simple", description="Voting mechanism to use")
    voting_period_days: int = Field(7, description="Voting period in days", ge=1, le=30)
    chains: List[str] = Field(..., description="Chains this proposal affects")
    execution_data: Dict[str, Any] = Field(..., description="Execution parameters (targets, values, calldatas)")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class VoteRequest(BaseModel):
    proposal_id: str = Field(..., description="Proposal ID to vote on")
    support: str = Field(..., description="Vote support: for, against, or abstain")
    chain: str = Field(..., description="Chain to vote from")
    signature: Optional[str] = Field(None, description="Vote signature for verification")


class ProposalExecuteRequest(BaseModel):
    proposal_id: str = Field(..., description="Proposal ID to execute")


# Dependency to get governance manager
def get_governance_manager():
    """Get governance manager instance from app state"""
    from fastapi import Request
    
    def _get_manager(request: Request):
        return request.app.state.governance_manager
    
    return _get_manager


# Dependency to get current user (simplified)
def get_current_user():
    """Get current user from request headers"""
    from fastapi import Header
    
    def _get_user(x_user_address: str = Header(...)):
        return {"address": x_user_address}
    
    return _get_user


@router.post("/proposals")
async def create_proposal(
    request: ProposalCreateRequest,
    user=Depends(get_current_user()),
    governance=Depends(get_governance_manager())
):
    """Create a new governance proposal"""
    try:
        result = await governance.create_proposal(
            title=request.title,
            description=request.description,
            proposal_type=request.proposal_type,
            proposer=user["address"],
            voting_mechanism=request.voting_mechanism,
            voting_period_days=request.voting_period_days,
            chains=request.chains,
            execution_data=request.execution_data,
            metadata=request.metadata
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/proposals/{proposal_id}")
async def get_proposal(
    proposal_id: str,
    governance=Depends(get_governance_manager())
):
    """Get proposal details and current status"""
    try:
        return await governance.get_proposal_status(proposal_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/proposals")
async def list_proposals(
    status: Optional[str] = Query(None, description="Filter by status"),
    proposer: Optional[str] = Query(None, description="Filter by proposer"),
    governance=Depends(get_governance_manager())
):
    """List proposals with optional filters"""
    try:
        # Get all active proposals
        proposals = await governance.proposal_manager.get_active_proposals()
        
        # Apply filters
        if proposer:
            proposer_proposals = await governance.proposal_manager.get_proposals_by_proposer(proposer)
            proposals = [p for p in proposals if p.id in [pp.id for pp in proposer_proposals]]
            
        if status:
            proposals = [p for p in proposals if p.status.value == status.lower()]
            
        # Convert to response format
        return [
            {
                "proposal_id": p.id,
                "title": p.title,
                "status": p.status.value,
                "proposer": p.proposer,
                "voting_start": p.voting_start.isoformat(),
                "voting_end": p.voting_end.isoformat(),
                "votes_for": p.votes_for,
                "votes_against": p.votes_against,
                "votes_abstain": p.votes_abstain
            }
            for p in proposals
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/vote")
async def cast_vote(
    request: VoteRequest,
    user=Depends(get_current_user()),
    governance=Depends(get_governance_manager())
):
    """Cast a vote on a proposal"""
    try:
        if request.support not in ["for", "against", "abstain"]:
            raise ValueError("Invalid support value. Must be: for, against, or abstain")
            
        result = await governance.cast_vote(
            proposal_id=request.proposal_id,
            voter=user["address"],
            support=request.support,
            chain=request.chain,
            signature=request.signature
        )
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/proposals/execute")
async def execute_proposal(
    request: ProposalExecuteRequest,
    user=Depends(get_current_user()),
    governance=Depends(get_governance_manager())
):
    """Execute a passed proposal"""
    try:
        result = await governance.execute_proposal(request.proposal_id)
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/reputation/{address}")
async def get_reputation(
    address: str,
    chain: Optional[str] = Query(None, description="Specific chain or all chains"),
    governance=Depends(get_governance_manager())
):
    """Get user's reputation score(s)"""
    try:
        return await governance.get_user_reputation(address, chain)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reputation/sync/{address}")
async def sync_reputation(
    address: str,
    governance=Depends(get_governance_manager())
):
    """Synchronize user's reputation across chains"""
    try:
        return await governance.sync_reputation(address)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/reputation/leaderboard")
async def get_reputation_leaderboard(
    chain: Optional[str] = Query(None, description="Filter by chain"),
    limit: int = Query(100, ge=1, le=1000),
    governance=Depends(get_governance_manager())
):
    """Get top contributors by reputation"""
    try:
        contributors = await governance.reputation_manager.get_top_contributors(
            chain=chain,
            limit=limit
        )
        
        return {
            "chain": chain or "all",
            "limit": limit,
            "contributors": contributors
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/voting/mechanisms")
async def get_voting_mechanisms(
    governance=Depends(get_governance_manager())
):
    """Get available voting mechanisms"""
    mechanisms = governance.voting_registry.get_available_mechanisms()
    
    return {
        "mechanisms": [
            {
                "id": m.value,
                "name": m.value.replace("_", " ").title(),
                "description": f"{m.value} voting strategy"
            }
            for m in mechanisms
        ]
    }


@router.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "dao-governance",
        "timestamp": datetime.utcnow().isoformat()
    } 