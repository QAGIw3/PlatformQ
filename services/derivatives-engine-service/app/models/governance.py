from enum import Enum
from typing import Optional, Dict, Any, List
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import uuid


class ProposalStatus(Enum):
    """Proposal status types"""
    DRAFT = "draft"
    PENDING = "pending"
    ACTIVE = "active"
    PASSED = "passed"
    REJECTED = "rejected"
    EXECUTED = "executed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class VoteType(Enum):
    """Vote types"""
    FOR = "for"
    AGAINST = "against"
    ABSTAIN = "abstain"


class ProposalType(Enum):
    """Types of governance proposals"""
    MARKET_CREATION = "market_creation"
    MARKET_UPDATE = "market_update"
    RISK_PARAMETER = "risk_parameter"
    FEE_STRUCTURE = "fee_structure"
    ORACLE_UPDATE = "oracle_update"
    EMERGENCY_ACTION = "emergency_action"
    TREASURY_ALLOCATION = "treasury_allocation"


@dataclass
class MarketProposal:
    """
    Governance proposal for market-related changes
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    proposal_type: ProposalType = ProposalType.MARKET_CREATION
    status: ProposalStatus = ProposalStatus.DRAFT
    
    # Proposer details
    proposer_id: str = ""
    proposer_reputation: Decimal = Decimal("0")
    
    # Proposal content
    title: str = ""
    description: str = ""
    
    # Market specifications (for market creation/update)
    market_config: Dict[str, Any] = field(default_factory=dict)
    
    # Parameter changes (for updates)
    current_values: Dict[str, Any] = field(default_factory=dict)
    proposed_values: Dict[str, Any] = field(default_factory=dict)
    
    # Voting configuration
    voting_start: Optional[datetime] = None
    voting_end: Optional[datetime] = None
    voting_period_days: int = 7
    
    # Voting thresholds
    quorum_required: Decimal = Decimal("0.1")  # 10% of voting power
    approval_threshold: Decimal = Decimal("0.5")  # 50% approval
    
    # Vote tallies
    votes_for: Decimal = Decimal("0")
    votes_against: Decimal = Decimal("0")
    votes_abstain: Decimal = Decimal("0")
    total_votes: Decimal = Decimal("0")
    unique_voters: int = 0
    
    # Individual votes tracking
    votes: Dict[str, Dict[str, Any]] = field(default_factory=dict)  # voter_id -> vote details
    
    # Execution details
    execution_delay_hours: int = 48  # Time lock after approval
    execution_deadline: Optional[datetime] = None
    executed_at: Optional[datetime] = None
    execution_tx_hash: Optional[str] = None
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Metadata
    discussion_url: Optional[str] = None
    ipfs_hash: Optional[str] = None  # For detailed docs
    tags: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def start_voting(self):
        """Start the voting period"""
        self.status = ProposalStatus.ACTIVE
        self.voting_start = datetime.utcnow()
        self.voting_end = self.voting_start + timedelta(days=self.voting_period_days)
        self.updated_at = datetime.utcnow()
        
    def cast_vote(self, voter_id: str, vote: VoteType, voting_power: Decimal, metadata: Optional[Dict] = None):
        """Cast a vote on the proposal"""
        if self.status != ProposalStatus.ACTIVE:
            raise ValueError("Proposal is not active for voting")
            
        if datetime.utcnow() > self.voting_end:
            self.status = ProposalStatus.EXPIRED
            raise ValueError("Voting period has ended")
            
        # Remove previous vote if exists
        if voter_id in self.votes:
            prev_vote = self.votes[voter_id]
            if prev_vote["vote_type"] == VoteType.FOR.value:
                self.votes_for -= prev_vote["voting_power"]
            elif prev_vote["vote_type"] == VoteType.AGAINST.value:
                self.votes_against -= prev_vote["voting_power"]
            else:
                self.votes_abstain -= prev_vote["voting_power"]
            self.total_votes -= prev_vote["voting_power"]
        else:
            self.unique_voters += 1
            
        # Record new vote
        self.votes[voter_id] = {
            "vote_type": vote.value,
            "voting_power": voting_power,
            "timestamp": datetime.utcnow().isoformat(),
            "metadata": metadata or {}
        }
        
        # Update tallies
        if vote == VoteType.FOR:
            self.votes_for += voting_power
        elif vote == VoteType.AGAINST:
            self.votes_against += voting_power
        else:
            self.votes_abstain += voting_power
            
        self.total_votes += voting_power
        self.updated_at = datetime.utcnow()
        
    def finalize_voting(self) -> bool:
        """Finalize voting and determine outcome"""
        if self.status != ProposalStatus.ACTIVE:
            return False
            
        if datetime.utcnow() < self.voting_end:
            return False
            
        # Check quorum
        # Note: total_voting_power would come from governance token supply
        # For now, assuming it's passed as part of the call
        quorum_met = True  # Simplified for this example
        
        if not quorum_met:
            self.status = ProposalStatus.REJECTED
            return False
            
        # Check approval threshold
        if self.total_votes > 0:
            approval_rate = self.votes_for / (self.votes_for + self.votes_against)
            if approval_rate >= self.approval_threshold:
                self.status = ProposalStatus.PASSED
                self.execution_deadline = datetime.utcnow() + timedelta(hours=self.execution_delay_hours + 168)  # +1 week
                return True
                
        self.status = ProposalStatus.REJECTED
        return False
        
    def execute(self, tx_hash: str):
        """Mark proposal as executed"""
        if self.status != ProposalStatus.PASSED:
            raise ValueError("Proposal must be passed to execute")
            
        if datetime.utcnow() < self.voting_end + timedelta(hours=self.execution_delay_hours):
            raise ValueError("Execution delay period not yet elapsed")
            
        if self.execution_deadline and datetime.utcnow() > self.execution_deadline:
            self.status = ProposalStatus.EXPIRED
            raise ValueError("Execution deadline has passed")
            
        self.status = ProposalStatus.EXECUTED
        self.executed_at = datetime.utcnow()
        self.execution_tx_hash = tx_hash
        self.updated_at = datetime.utcnow()
        
    def cancel(self, reason: str):
        """Cancel the proposal"""
        if self.status in [ProposalStatus.EXECUTED, ProposalStatus.CANCELLED]:
            raise ValueError("Cannot cancel executed or already cancelled proposal")
            
        self.status = ProposalStatus.CANCELLED
        self.metadata["cancellation_reason"] = reason
        self.metadata["cancelled_at"] = datetime.utcnow().isoformat()
        self.updated_at = datetime.utcnow()
        
    def get_voting_stats(self) -> Dict[str, Any]:
        """Get voting statistics"""
        total_non_abstain = self.votes_for + self.votes_against
        
        return {
            "total_votes": str(self.total_votes),
            "unique_voters": self.unique_voters,
            "votes_for": str(self.votes_for),
            "votes_against": str(self.votes_against),
            "votes_abstain": str(self.votes_abstain),
            "approval_rate": str(self.votes_for / total_non_abstain) if total_non_abstain > 0 else "0",
            "participation_rate": "TBD",  # Would need total eligible voters
            "time_remaining": str(self.voting_end - datetime.utcnow()) if self.voting_end else None
        }
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "proposal_type": self.proposal_type.value,
            "status": self.status.value,
            "proposer_id": self.proposer_id,
            "proposer_reputation": str(self.proposer_reputation),
            "title": self.title,
            "description": self.description,
            "market_config": self.market_config,
            "current_values": self.current_values,
            "proposed_values": self.proposed_values,
            "voting_start": self.voting_start.isoformat() if self.voting_start else None,
            "voting_end": self.voting_end.isoformat() if self.voting_end else None,
            "voting_period_days": self.voting_period_days,
            "quorum_required": str(self.quorum_required),
            "approval_threshold": str(self.approval_threshold),
            "voting_stats": self.get_voting_stats(),
            "execution_delay_hours": self.execution_delay_hours,
            "execution_deadline": self.execution_deadline.isoformat() if self.execution_deadline else None,
            "executed_at": self.executed_at.isoformat() if self.executed_at else None,
            "execution_tx_hash": self.execution_tx_hash,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "discussion_url": self.discussion_url,
            "ipfs_hash": self.ipfs_hash,
            "tags": self.tags,
            "metadata": self.metadata
        } 