"""
Proposal management endpoints for governance service
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status, BackgroundTasks
from typing import List, Optional, Dict, Any
from uuid import UUID
import httpx
import os
import logging

from ..proposals.schemas.proposal import Proposal, ProposalCreate, CrossChainProposalCreate
from ..proposals.repository import ProposalRepository
from ..proposals.blockchain.proposals import proposals_blockchain_service
from .deps import get_current_context, get_governance_manager

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/proposals", tags=["proposals"])

TREASURY_REP_THRESHOLD = 500


@router.post("/", response_model=Proposal)
async def create_proposal(
    *,
    proposal_in: ProposalCreate,
    onchain: bool = Query(False, description="Submit the proposal on-chain"),
    context: dict = Depends(get_current_context),
    governance_manager = Depends(get_governance_manager),
    background_tasks: BackgroundTasks
):
    """
    Create a new proposal. Can be off-chain or on-chain.
    Proposals that involve a value transfer require a reputation score of at least 500.
    """
    # Check if the proposal involves a value transfer
    is_value_transfer = any(value > 0 for value in proposal_in.values)

    if is_value_transfer:
        reputation = context.get("reputation", 0)
        if reputation < TREASURY_REP_THRESHOLD:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Your reputation score of {reputation} is below the required {TREASURY_REP_THRESHOLD} to create a proposal with value transfer."
            )

    proposer_id = str(context["user"].id)
    
    # Create proposal in governance manager
    proposal = await governance_manager.create_proposal(
        proposer_id=proposer_id,
        proposal_data=proposal_in.dict()
    )

    if onchain:
        # Submit to blockchain via governance manager
        background_tasks.add_task(
            governance_manager.submit_proposal_onchain,
            proposal_id=proposal.id,
            chain="ethereum"
        )

    return proposal


@router.post("/cross-chain", response_model=Proposal)
async def create_cross_chain_proposal(
    *,
    proposal_in: CrossChainProposalCreate,
    context: dict = Depends(get_current_context),
    governance_manager = Depends(get_governance_manager),
    background_tasks: BackgroundTasks
):
    """
    Create a cross-chain governance proposal that will be executed on multiple chains.
    """
    proposer_id = str(context["user"].id)
    
    # Create cross-chain proposal
    proposal = await governance_manager.create_cross_chain_proposal(
        proposer_id=proposer_id,
        proposal_data=proposal_in.dict(),
        chains=proposal_in.chains
    )
    
    # Submit to all specified chains
    for chain in proposal_in.chains:
        background_tasks.add_task(
            governance_manager.submit_proposal_onchain,
            proposal_id=proposal.id,
            chain=chain
        )
    
    return proposal


@router.get("/", response_model=List[Proposal])
async def list_proposals(
    skip: int = 0,
    limit: int = 10,
    status: Optional[str] = None,
    proposer: Optional[str] = None,
    governance_manager = Depends(get_governance_manager)
):
    """
    List all proposals with optional filtering.
    """
    filters = {}
    if status:
        filters["status"] = status
    if proposer:
        filters["proposer"] = proposer
    
    proposals = await governance_manager.list_proposals(
        skip=skip,
        limit=limit,
        filters=filters
    )
    
    return proposals


@router.get("/{proposal_id}", response_model=Proposal)
async def get_proposal(
    proposal_id: UUID,
    governance_manager = Depends(get_governance_manager)
):
    """
    Get a specific proposal by ID.
    """
    proposal = await governance_manager.get_proposal(proposal_id)
    
    if not proposal:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Proposal not found"
        )
    
    return proposal


@router.post("/{proposal_id}/vote")
async def vote_on_proposal(
    proposal_id: UUID,
    vote: str = Query(..., regex="^(for|against|abstain)$"),
    voting_power: Optional[int] = None,
    context: dict = Depends(get_current_context),
    governance_manager = Depends(get_governance_manager)
):
    """
    Vote on a proposal. Voting power can be optionally specified.
    """
    voter_id = str(context["user"].id)
    
    # Use governance manager's voting mechanism
    result = await governance_manager.cast_vote(
        proposal_id=proposal_id,
        voter_id=voter_id,
        vote_type=vote,
        voting_power=voting_power or context.get("voting_power", 1)
    )
    
    return result


@router.post("/{proposal_id}/execute")
async def execute_proposal(
    proposal_id: UUID,
    context: dict = Depends(get_current_context),
    governance_manager = Depends(get_governance_manager),
    background_tasks: BackgroundTasks
):
    """
    Execute a proposal that has passed voting.
    Only proposals in 'passed' status can be executed.
    """
    # Check if user has execution rights
    if "executor" not in context.get("roles", []):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to execute proposals"
        )
    
    proposal = await governance_manager.get_proposal(proposal_id)
    
    if not proposal:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Proposal not found"
        )
    
    if proposal.status != "passed":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Proposal is in '{proposal.status}' status. Only 'passed' proposals can be executed."
        )
    
    # Execute proposal asynchronously
    background_tasks.add_task(
        governance_manager.execute_proposal,
        proposal_id=proposal_id
    )
    
    return {"message": "Proposal execution initiated"}


@router.get("/{proposal_id}/voting-stats")
async def get_voting_statistics(
    proposal_id: UUID,
    governance_manager = Depends(get_governance_manager)
):
    """
    Get detailed voting statistics for a proposal.
    """
    stats = await governance_manager.get_voting_statistics(proposal_id)
    
    if not stats:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Proposal not found or no votes cast yet"
        )
    
    return stats


@router.post("/{proposal_id}/cancel")
async def cancel_proposal(
    proposal_id: UUID,
    reason: str,
    context: dict = Depends(get_current_context),
    governance_manager = Depends(get_governance_manager)
):
    """
    Cancel a proposal. Only the proposer or an admin can cancel.
    """
    user_id = str(context["user"].id)
    proposal = await governance_manager.get_proposal(proposal_id)
    
    if not proposal:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Proposal not found"
        )
    
    # Check permissions
    is_proposer = str(proposal.proposer) == user_id
    is_admin = "admin" in context.get("roles", [])
    
    if not (is_proposer or is_admin):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only the proposer or an admin can cancel a proposal"
        )
    
    result = await governance_manager.cancel_proposal(
        proposal_id=proposal_id,
        reason=reason,
        cancelled_by=user_id
    )
    
    return result 