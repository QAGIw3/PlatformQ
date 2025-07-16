from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import List, Optional, Dict, Any
from uuid import UUID
import httpx
import os

from ..schemas.proposal import Proposal, ProposalCreate, CrossChainProposalCreate
from ..crud.proposal import proposal as crud_proposal
from ..blockchain.proposals import proposals_blockchain_service
from .deps import get_current_context

router = APIRouter()

TREASURY_REP_THRESHOLD = 500
BLOCKCHAIN_EVENT_BRIDGE_URL = os.environ.get(
    "BLOCKCHAIN_EVENT_BRIDGE_URL", 
    "http://blockchain-event-bridge:8001"
)

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

@router.post("/cross-chain", response_model=Dict[str, Any])
async def create_cross_chain_proposal(
    *,
    proposal_in: CrossChainProposalCreate,
    context: dict = Depends(get_current_context)
):
    """
    Create a cross-chain proposal that spans multiple blockchains.
    Requires sufficient reputation on participating chains.
    """
    proposer_id = str(context["user"].id)
    reputation = context.get("reputation", 0)
    
    # Check reputation threshold for cross-chain proposals
    MIN_CROSS_CHAIN_REP = 1000
    if reputation < MIN_CROSS_CHAIN_REP:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Cross-chain proposals require reputation score of {MIN_CROSS_CHAIN_REP}. Your score: {reputation}"
        )
    
    # First, create local proposal record
    local_proposal = crud_proposal.create(
        obj_in=ProposalCreate(
            title=proposal_in.title,
            description=proposal_in.description,
            targets=[],  # Cross-chain proposals don't have single-chain targets
            values=[],
            calldatas=[],
            is_cross_chain=True
        ),
        proposer=proposer_id
    )
    
    # Prepare cross-chain proposal data
    cross_chain_data = {
        "proposalId": str(local_proposal.id),
        "title": proposal_in.title,
        "description": proposal_in.description,
        "proposerId": proposer_id,
        "votingEndTime": proposal_in.voting_end_time,
        "chains": proposal_in.chains,
        "votingParameters": proposal_in.voting_parameters
    }
    
    # Submit to blockchain event bridge
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{BLOCKCHAIN_EVENT_BRIDGE_URL}/api/v1/proposals/cross-chain",
                json=cross_chain_data,
                timeout=30.0
            )
            response.raise_for_status()
            
            result = response.json()
            
            # Update local proposal with cross-chain details
            crud_proposal.update_cross_chain_status(
                id=local_proposal.id,
                cross_chain_details={
                    "chains": proposal_in.chains,
                    "status": "submitted",
                    "bridge_response": result
                }
            )
            
            return {
                "proposalId": str(local_proposal.id),
                "status": "success",
                "message": result.get("message", "Cross-chain proposal created"),
                "chains": len(proposal_in.chains)
            }
            
        except httpx.HTTPError as e:
            # Clean up local proposal on failure
            crud_proposal.remove(id=local_proposal.id)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create cross-chain proposal: {str(e)}"
            )

@router.get("/", response_model=List[Proposal])
def read_proposals(
    cross_chain_only: bool = Query(False, description="Filter for cross-chain proposals only"),
    context: dict = Depends(get_current_context)
):
    """
    Retrieve all proposals.
    """
    proposals = crud_proposal.get_multi()
    
    if cross_chain_only:
        proposals = [p for p in proposals if p.is_cross_chain]
    
    return proposals

@router.get("/{proposal_id}", response_model=Proposal)
async def read_proposal(
    proposal_id: UUID,
    include_chain_state: bool = Query(False, description="Include current state from all chains"),
    context: dict = Depends(get_current_context)
):
    """
    Get a specific proposal by ID.
    """
    proposal = crud_proposal.get(proposal_id)
    if not proposal:
        raise HTTPException(status_code=404, detail="Proposal not found")
    
    # If cross-chain proposal and chain state requested
    if proposal.is_cross_chain and include_chain_state:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{BLOCKCHAIN_EVENT_BRIDGE_URL}/api/v1/proposals/{proposal_id}/state",
                    timeout=10.0
                )
                response.raise_for_status()
                
                chain_state = response.json()
                proposal.chain_state = chain_state
                
            except httpx.HTTPError:
                # If we can't get chain state, continue without it
                pass
    
    return proposal

@router.post("/{proposal_id}/vote")
async def vote_on_proposal(
    proposal_id: UUID,
    chain_id: str,
    support: bool,
    signature: Optional[str] = None,
    context: dict = Depends(get_current_context)
):
    """
    Cast a vote on a proposal (cross-chain or single-chain).
    """
    proposal = crud_proposal.get(proposal_id)
    if not proposal:
        raise HTTPException(status_code=404, detail="Proposal not found")
    
    voter_address = context.get("chain_addresses", {}).get(chain_id)
    if not voter_address:
        raise HTTPException(
            status_code=400,
            detail=f"No address found for chain {chain_id}"
        )
    
    if proposal.is_cross_chain:
        # Route to blockchain event bridge
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{BLOCKCHAIN_EVENT_BRIDGE_URL}/api/v1/vote",
                    json={
                        "proposalId": str(proposal_id),
                        "chainId": chain_id,
                        "voterAddress": voter_address,
                        "support": support,
                        "signature": signature
                    },
                    timeout=30.0
                )
                response.raise_for_status()
                
                return response.json()
                
            except httpx.HTTPError as e:
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to cast vote: {str(e)}"
                )
    else:
        # Handle single-chain vote
        # Implementation depends on specific blockchain
        raise HTTPException(
            status_code=501,
            detail="Single-chain voting not implemented in this version"
        )

@router.get("/reputation/sync")
async def sync_user_reputation(
    context: dict = Depends(get_current_context)
):
    """
    Synchronize user's reputation across all chains.
    """
    user_id = str(context["user"].id)
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{BLOCKCHAIN_EVENT_BRIDGE_URL}/api/v1/reputation/sync",
                json={"userId": user_id},
                timeout=30.0
            )
            response.raise_for_status()
            
            return response.json()
            
        except httpx.HTTPError as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to sync reputation: {str(e)}"
            ) 