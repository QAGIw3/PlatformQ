"""
Proposal management for DAO governance.
"""

import logging
import uuid
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass

from pyignite import Client as IgniteClient

logger = logging.getLogger(__name__)


class ProposalStatus(Enum):
    """Proposal lifecycle states"""
    DRAFT = "draft"
    PENDING = "pending"
    ACTIVE = "active"
    SUCCEEDED = "succeeded"
    DEFEATED = "defeated"
    QUEUED = "queued"
    EXECUTED = "executed"
    CANCELLED = "cancelled"
    EXPIRED = "expired"


class ProposalType(Enum):
    """Types of governance proposals"""
    PARAMETER_CHANGE = "parameter_change"
    TREASURY_ALLOCATION = "treasury_allocation"
    CONTRACT_UPGRADE = "contract_upgrade"
    POLICY_CHANGE = "policy_change"
    MEMBER_ADDITION = "member_addition"
    MEMBER_REMOVAL = "member_removal"
    CUSTOM = "custom"


@dataclass
class Proposal:
    """Governance proposal data structure"""
    id: str
    title: str
    description: str
    proposal_type: ProposalType
    proposer: str
    status: ProposalStatus
    created_at: datetime
    voting_start: datetime
    voting_end: datetime
    execution_delay: int  # seconds after voting ends
    
    # Voting configuration
    voting_mechanism: str
    quorum_required: int  # basis points (10000 = 100%)
    approval_threshold: int  # basis points
    
    # Chain information
    chains: List[str]  # List of chains this proposal affects
    
    # Execution details
    targets: List[str]  # Contract addresses to call
    values: List[int]  # ETH/native token amounts
    calldatas: List[str]  # Encoded function calls
    
    # Metadata
    metadata: Dict[str, Any]
    ipfs_hash: Optional[str] = None
    discussion_url: Optional[str] = None
    
    # Results
    votes_for: int = 0
    votes_against: int = 0
    votes_abstain: int = 0
    unique_voters: int = 0
    executed_at: Optional[datetime] = None
    execution_tx: Optional[str] = None


class ProposalManager:
    """Manages DAO proposals across chains"""
    
    def __init__(self, ignite_client: IgniteClient):
        self.ignite_client = ignite_client
        self._proposals_cache = None
        self._votes_cache = None
        
    async def initialize(self):
        """Initialize proposal caches"""
        self._proposals_cache = await self.ignite_client.get_or_create_cache(
            "dao_proposals"
        )
        self._votes_cache = await self.ignite_client.get_or_create_cache(
            "dao_votes"
        )
        logger.info("Proposal manager initialized")
        
    async def create_proposal(
        self,
        title: str,
        description: str,
        proposal_type: ProposalType,
        proposer: str,
        voting_mechanism: str,
        voting_period_days: int,
        execution_delay_hours: int,
        chains: List[str],
        targets: List[str],
        values: List[int],
        calldatas: List[str],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Proposal:
        """Create a new governance proposal"""
        proposal_id = str(uuid.uuid4())
        now = datetime.utcnow()
        
        proposal = Proposal(
            id=proposal_id,
            title=title,
            description=description,
            proposal_type=proposal_type,
            proposer=proposer,
            status=ProposalStatus.PENDING,
            created_at=now,
            voting_start=now + timedelta(hours=1),  # 1 hour delay
            voting_end=now + timedelta(days=voting_period_days),
            execution_delay=execution_delay_hours * 3600,
            voting_mechanism=voting_mechanism,
            quorum_required=2000,  # 20% default
            approval_threshold=5000,  # 50% default
            chains=chains,
            targets=targets,
            values=values,
            calldatas=calldatas,
            metadata=metadata or {}
        )
        
        # Store in cache
        await self._proposals_cache.put(proposal_id, proposal)
        
        logger.info(f"Created proposal {proposal_id}: {title}")
        return proposal
        
    async def get_proposal(self, proposal_id: str) -> Optional[Proposal]:
        """Get a proposal by ID"""
        return await self._proposals_cache.get(proposal_id)
        
    async def update_proposal_status(
        self,
        proposal_id: str,
        new_status: ProposalStatus
    ) -> bool:
        """Update proposal status"""
        proposal = await self.get_proposal(proposal_id)
        if not proposal:
            return False
            
        # Validate status transition
        if not self._is_valid_transition(proposal.status, new_status):
            logger.warning(
                f"Invalid status transition for {proposal_id}: "
                f"{proposal.status.value} -> {new_status.value}"
            )
            return False
            
        proposal.status = new_status
        await self._proposals_cache.put(proposal_id, proposal)
        
        logger.info(f"Updated proposal {proposal_id} status to {new_status.value}")
        return True
        
    async def record_vote(
        self,
        proposal_id: str,
        voter: str,
        support: str,  # 'for', 'against', 'abstain'
        voting_power: int,
        chain: str,
        signature: Optional[str] = None
    ) -> bool:
        """Record a vote on a proposal"""
        proposal = await self.get_proposal(proposal_id)
        if not proposal:
            return False
            
        # Check if voting is active
        now = datetime.utcnow()
        if now < proposal.voting_start or now > proposal.voting_end:
            logger.warning(f"Voting not active for proposal {proposal_id}")
            return False
            
        # Create vote key
        vote_key = f"{proposal_id}:{voter}:{chain}"
        
        # Check if already voted
        existing_vote = await self._votes_cache.get(vote_key)
        if existing_vote:
            logger.warning(f"Voter {voter} already voted on {proposal_id}")
            return False
            
        # Record vote
        vote_data = {
            'proposal_id': proposal_id,
            'voter': voter,
            'support': support,
            'voting_power': voting_power,
            'chain': chain,
            'timestamp': now,
            'signature': signature
        }
        
        await self._votes_cache.put(vote_key, vote_data)
        
        # Update proposal vote counts
        if support == 'for':
            proposal.votes_for += voting_power
        elif support == 'against':
            proposal.votes_against += voting_power
        else:
            proposal.votes_abstain += voting_power
            
        proposal.unique_voters += 1
        await self._proposals_cache.put(proposal_id, proposal)
        
        logger.info(f"Recorded vote from {voter} on proposal {proposal_id}")
        return True
        
    async def get_active_proposals(self) -> List[Proposal]:
        """Get all active proposals"""
        proposals = []
        now = datetime.utcnow()
        
        # Scan cache for active proposals
        # In production, use proper indexing
        cursor = self._proposals_cache.scan()
        for _, proposal in cursor:
            if (proposal.status == ProposalStatus.ACTIVE or
                (proposal.status == ProposalStatus.PENDING and
                 proposal.voting_start <= now <= proposal.voting_end)):
                proposals.append(proposal)
                
        return proposals
        
    async def get_proposals_by_proposer(self, proposer: str) -> List[Proposal]:
        """Get all proposals by a specific proposer"""
        proposals = []
        
        cursor = self._proposals_cache.scan()
        for _, proposal in cursor:
            if proposal.proposer == proposer:
                proposals.append(proposal)
                
        return proposals
        
    async def check_proposal_state_transitions(self):
        """Check and update proposal states based on time"""
        now = datetime.utcnow()
        
        cursor = self._proposals_cache.scan()
        for proposal_id, proposal in cursor:
            updated = False
            
            # PENDING -> ACTIVE
            if (proposal.status == ProposalStatus.PENDING and
                now >= proposal.voting_start):
                proposal.status = ProposalStatus.ACTIVE
                updated = True
                
            # ACTIVE -> SUCCEEDED/DEFEATED
            elif (proposal.status == ProposalStatus.ACTIVE and
                  now > proposal.voting_end):
                # Calculate outcome
                total_votes = proposal.votes_for + proposal.votes_against
                if total_votes == 0:
                    proposal.status = ProposalStatus.DEFEATED
                else:
                    approval_rate = (proposal.votes_for / total_votes) * 10000
                    if approval_rate >= proposal.approval_threshold:
                        proposal.status = ProposalStatus.SUCCEEDED
                    else:
                        proposal.status = ProposalStatus.DEFEATED
                updated = True
                
            # SUCCEEDED -> QUEUED
            elif (proposal.status == ProposalStatus.SUCCEEDED and
                  now > proposal.voting_end + timedelta(seconds=1)):
                proposal.status = ProposalStatus.QUEUED
                updated = True
                
            # QUEUED -> EXPIRED (if not executed in time)
            elif (proposal.status == ProposalStatus.QUEUED and
                  now > proposal.voting_end + timedelta(seconds=proposal.execution_delay + 86400)):
                proposal.status = ProposalStatus.EXPIRED
                updated = True
                
            if updated:
                await self._proposals_cache.put(proposal_id, proposal)
                logger.info(f"Updated proposal {proposal_id} to {proposal.status.value}")
                
    def _is_valid_transition(
        self,
        current: ProposalStatus,
        new: ProposalStatus
    ) -> bool:
        """Check if a status transition is valid"""
        valid_transitions = {
            ProposalStatus.DRAFT: [ProposalStatus.PENDING, ProposalStatus.CANCELLED],
            ProposalStatus.PENDING: [ProposalStatus.ACTIVE, ProposalStatus.CANCELLED],
            ProposalStatus.ACTIVE: [ProposalStatus.SUCCEEDED, ProposalStatus.DEFEATED, ProposalStatus.CANCELLED],
            ProposalStatus.SUCCEEDED: [ProposalStatus.QUEUED],
            ProposalStatus.DEFEATED: [],
            ProposalStatus.QUEUED: [ProposalStatus.EXECUTED, ProposalStatus.EXPIRED],
            ProposalStatus.EXECUTED: [],
            ProposalStatus.CANCELLED: [],
            ProposalStatus.EXPIRED: []
        }
        
        return new in valid_transitions.get(current, []) 