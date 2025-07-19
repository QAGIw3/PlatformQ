from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from uuid import UUID

class ProposalBase(BaseModel):
    title: str
    description: str
    targets: List[str]
    values: List[int]
    calldatas: List[str]
    is_cross_chain: bool = False

class ProposalCreate(ProposalBase):
    pass

class ChainProposal(BaseModel):
    chainId: str
    proposer: str
    targets: List[str]
    values: List[str]  # String to handle big numbers
    calldatas: List[str]

class VotingParameters(BaseModel):
    quorum_percentage: int
    approval_threshold: int
    min_reputation_score: Optional[int] = None
    aggregation_strategy: str = "WEIGHTED_AVG"

class CrossChainProposalCreate(BaseModel):
    title: str
    description: str
    chains: List[ChainProposal]
    voting_end_time: int
    voting_parameters: VotingParameters

class Proposal(ProposalBase):
    id: UUID
    proposer: str
    is_onchain: bool = False
    onchain_proposal_id: Optional[str] = None
    status: str = "pending"
    cross_chain_details: Optional[Dict[str, Any]] = None
    chain_state: Optional[Dict[str, Any]] = None  # Current state from chains

    class Config:
        orm_mode = True 