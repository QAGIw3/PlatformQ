from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import List, Optional, Dict, Any
from uuid import UUID
import httpx
import os

from ..schemas.proposal import Proposal, ProposalCreate, CrossChainProposalCreate
from ..repository import ProposalRepository
from ..blockchain.proposals import proposals_blockchain_service
from .deps import get_current_context, get_proposal_repository

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
    context: dict = Depends(get_current_context),
    repo: ProposalRepository = Depends(get_proposal_repository),
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
    proposal = repo.add(obj_in=proposal_in, proposer=proposer_id)

    if onchain:
        try:
            onchain_proposal_id = proposals_blockchain_service.propose(
                targets=proposal.targets,
                values=proposal.values,
                calldatas=proposal.calldatas,
                description=proposal.description
            )
            proposal = repo.update_onchain_status(
                id=proposal.id, onchain_proposal_id=onchain_proposal_id
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to submit proposal on-chain: {e}")

    return proposal

@router.post("/cross-chain", response_model=Dict[str, Any])
async def create_cross_chain_proposal(
    *,
    proposal_in: CrossChainProposalCreate,
    context: dict = Depends(get_current_context),
    repo: ProposalRepository = Depends(get_proposal_repository),
):
    """
    Create a cross-chain proposal that spans multiple blockchains.
    Requires sufficient reputation on participating chains.
    """
    proposer_id = str(context["user"].id)
    reputation = context.get("reputation", 0)
    
    MIN_CROSS_CHAIN_REP = 1000
    if reputation < MIN_CROSS_CHAIN_REP:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Cross-chain proposals require reputation score of {MIN_CROSS_CHAIN_REP}. Your score: {reputation}"
        )
    
    local_proposal = repo.add(
        obj_in=ProposalCreate(
            title=proposal_in.title,
            description=proposal_in.description,
            targets=[],
            values=[],
            calldatas=[],
            is_cross_chain=True
        ),
        proposer=proposer_id
    )
    
    cross_chain_data = {
        "proposalId": str(local_proposal.id),
        "title": proposal_in.title,
        "description": proposal_in.description,
        "proposerId": proposer_id,
        "votingEndTime": proposal_in.voting_end_time,
        "chains": proposal_in.chains,
        "votingParameters": proposal_in.voting_parameters
    }
    
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{BLOCKCHAIN_EVENT_BRIDGE_URL}/api/v1/proposals/cross-chain",
                json=cross_chain_data,
                timeout=30.0
            )
            response.raise_for_status()
            
            result = response.json()
            
            repo.update_cross_chain_status(
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
            repo.delete(id=local_proposal.id)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create cross-chain proposal: {str(e)}"
            )

@router.get("/", response_model=List[Proposal])
def read_proposals(
    cross_chain_only: bool = Query(False, description="Filter for cross-chain proposals only"),
    repo: ProposalRepository = Depends(get_proposal_repository),
    context: dict = Depends(get_current_context)
):
    proposals = repo.list()
    
    if cross_chain_only:
        proposals = [p for p in proposals if p.is_cross_chain]
    
    return proposals

@router.get("/{proposal_id}", response_model=Proposal)
async def read_proposal(
    proposal_id: UUID,
    include_chain_state: bool = Query(False, description="Include current state from all chains"),
    repo: ProposalRepository = Depends(get_proposal_repository),
    context: dict = Depends(get_current_context)
):
    proposal = repo.get(proposal_id)
    if not proposal:
        raise HTTPException(status_code=404, detail="Proposal not found")
    
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
                pass
    
    return proposal

@router.post("/{proposal_id}/vote")
async def vote_on_proposal(
    proposal_id: UUID,
    chain_id: str,
    support: bool,
    signature: Optional[str] = None,
    repo: ProposalRepository = Depends(get_proposal_repository),
    context: dict = Depends(get_current_context)
):
    proposal = repo.get(proposal_id)
    if not proposal:
        raise HTTPException(status_code=404, detail="Proposal not found")
    
    voter_address = context.get("chain_addresses", {}).get(chain_id)
    if not voter_address:
        raise HTTPException(
            status_code=400,
            detail=f"No address found for chain {chain_id}"
        )
    
    if proposal.is_cross_chain:
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