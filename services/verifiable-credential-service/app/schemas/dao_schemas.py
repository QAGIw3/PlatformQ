from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class DAOMembershipCredentialSubject(BaseModel):
    id: str = Field(..., description="DID or other identifier of the subject")
    daoId: str = Field(..., description="DID or identifier of the DAO")
    role: str = Field(..., description="Role of the member within the DAO (e.g., member, admin)")
    joinedAt: datetime = Field(..., description="Timestamp when the member joined the DAO")

class DAOMembershipCredential(BaseModel):
    context: List[str] = Field(default=["https://www.w3.org/2018/credentials/v1", "https://platformq.com/contexts/dao-v1.jsonld"], alias="@context")
    id: str
    type: List[str] = Field(default=["VerifiableCredential", "DAOMembershipCredential"])
    issuer: str
    issuanceDate: datetime
    credentialSubject: DAOMembershipCredentialSubject
    proof: Dict[str, Any]


class VotingPowerCredentialSubject(BaseModel):
    id: str = Field(..., description="DID or other identifier of the subject")
    daoId: str = Field(..., description="DID or identifier of the DAO")
    votingPower: int = Field(..., description="Amount of voting power this subject holds")
    validUntil: Optional[datetime] = Field(None, description="Optional timestamp until which the voting power is valid")

class VotingPowerCredential(BaseModel):
    context: List[str] = Field(default=["https://www.w3.org/2018/credentials/v1", "https://platformq.com/contexts/dao-v1.jsonld"], alias="@context")
    id: str
    type: List[str] = Field(default=["VerifiableCredential", "VotingPowerCredential"])
    issuer: str
    issuanceDate: datetime
    credentialSubject: VotingPowerCredentialSubject
    proof: Dict[str, Any]


class ReputationScoreCredentialSubject(BaseModel):
    id: str = Field(..., description="DID or other identifier of the subject")
    score: int = Field(..., description="Reputation score of the subject")
    contributions: List[str] = Field(default=[], description="List of contribution identifiers that led to this score")
    lastUpdated: datetime = Field(..., description="Timestamp when the reputation score was last updated")

class ReputationScoreCredential(BaseModel):
    context: List[str] = Field(default=["https://www.w3.org/2018/credentials/v1", "https://platformq.com/contexts/dao-v1.jsonld"], alias="@context")
    id: str
    type: List[str] = Field(default=["VerifiableCredential", "ReputationScoreCredential"])
    issuer: str
    issuanceDate: datetime
    credentialSubject: ReputationScoreCredentialSubject
    proof: Dict[str, Any]


class ProposalApprovalCredentialSubject(BaseModel):
    id: str = Field(..., description="DID or other identifier of the subject")
    daoId: str = Field(..., description="DID or identifier of the DAO")
    proposalId: str = Field(..., description="Identifier of the proposal that was approved/voted on")
    vote: str = Field(..., description="The vote cast (e.g., 'for', 'against', 'abstain')")
    votedAt: datetime = Field(..., description="Timestamp when the vote was cast")

class ProposalApprovalCredential(BaseModel):
    context: List[str] = Field(default=["https://www.w3.org/2018/credentials/v1", "https://platformq.com/contexts/dao-v1.jsonld"], alias="@context")
    id: str
    type: List[str] = Field(default=["VerifiableCredential", "ProposalApprovalCredential"])
    issuer: str
    issuanceDate: datetime
    credentialSubject: ProposalApprovalCredentialSubject
    proof: Dict[str, Any] 