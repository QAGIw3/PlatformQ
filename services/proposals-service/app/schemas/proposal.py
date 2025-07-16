from pydantic import BaseModel
from typing import List, Optional
from uuid import UUID

class ProposalBase(BaseModel):
    title: str
    description: str
    targets: List[str]
    values: List[int]
    calldatas: List[str]

class ProposalCreate(ProposalBase):
    pass

class Proposal(ProposalBase):
    id: UUID
    proposer: str
    is_onchain: bool = False
    onchain_proposal_id: Optional[str] = None
    status: str = "pending"

    class Config:
        orm_mode = True 