from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import List, Optional
from uuid import UUID

from ..schemas.proposal import Proposal, ProposalCreate
from ..crud.proposal import proposal as crud_proposal
from ..blockchain.proposals import proposals_blockchain_service
from .deps import get_current_context

router = APIRouter()

TREASURY_REP_THRESHOLD = 500

@router.post("/", response_model=Proposal)
def create_proposal(
    *,
    proposal_in: ProposalCreate,
    onchain: bool = Query(False, description="Submit the proposal on-chain"),
    context: dict = Depends(get_current_context)
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
    proposal = crud_proposal.create(obj_in=proposal_in, proposer=proposer_id)

    if onchain:
        try:
            onchain_proposal_id = proposals_blockchain_service.propose(
                targets=proposal.targets,
                values=proposal.values,
                calldatas=proposal.calldatas,
                description=proposal.description
            )
            proposal = crud_proposal.update_onchain_status(
                id=proposal.id, onchain_proposal_id=onchain_proposal_id
            )
        except Exception as e:
            # If on-chain submission fails, we can either delete the off-chain proposal
            # or leave it as a draft. For now, we'll raise an error.
            # In a real app, you might want more sophisticated error handling.
            # crud_proposal.remove(id=proposal.id)
            raise HTTPException(status_code=500, detail=f"Failed to submit proposal on-chain: {e}")

    return proposal

@router.get("/", response_model=List[Proposal])
def read_proposals(
    context: dict = Depends(get_current_context)
):
    """
    Retrieve all proposals.
    """
    return crud_proposal.get_multi()

@router.get("/{proposal_id}", response_model=Proposal)
def read_proposal(
    *,
    proposal_id: UUID,
    context: dict = Depends(get_current_context)
):
    """
    Get a proposal by ID.
    """
    proposal = crud_proposal.get(id=proposal_id)
    if not proposal:
        raise HTTPException(status_code=404, detail="Proposal not found")
    return proposal 