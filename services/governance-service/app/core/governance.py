"""
Main governance manager for DAO operations.
"""

import logging
import asyncio
from typing import Dict, Optional, List, Any
from datetime import datetime

from pyignite import Client as IgniteClient
import pulsar

from ..voting import VotingStrategyRegistry, VotingMechanism
from .proposals import ProposalManager, ProposalType, ProposalStatus
from .reputation import ReputationManager
from .execution import ProposalExecutor

logger = logging.getLogger(__name__)


class GovernanceManager:
    """
    Main orchestrator for DAO governance operations.
    Coordinates proposals, voting, reputation, and execution.
    """
    
    def __init__(self, ignite_client: IgniteClient, pulsar_client: pulsar.Client,
                 blockchain_gateway):
        self.ignite_client = ignite_client
        self.pulsar_client = pulsar_client
        self.blockchain_gateway = blockchain_gateway
        
        # Initialize components
        self.voting_registry = VotingStrategyRegistry()
        self.proposal_manager = ProposalManager(ignite_client)
        self.reputation_manager = ReputationManager(ignite_client, blockchain_gateway)
        self.proposal_executor = ProposalExecutor(blockchain_gateway, self.proposal_manager)
        
        # Event publisher
        self.event_producer = None
        
        # Background tasks
        self._background_tasks = []
        
    async def initialize(self):
        """Initialize the governance manager and all components"""
        logger.info("Initializing governance manager...")
        
        # Initialize components
        await self.proposal_manager.initialize()
        await self.reputation_manager.initialize()
        
        # Create event producer
        self.event_producer = self.pulsar_client.create_producer(
            'persistent://public/governance/events'
        )
        
        # Start background tasks
        self._background_tasks.append(
            asyncio.create_task(self._proposal_state_monitor())
        )
        self._background_tasks.append(
            asyncio.create_task(self._reputation_decay_task())
        )
        
        logger.info("Governance manager initialized")
        
    async def shutdown(self):
        """Shutdown the governance manager"""
        logger.info("Shutting down governance manager...")
        
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
            
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        
        # Close event producer
        if self.event_producer:
            self.event_producer.close()
            
        logger.info("Governance manager shutdown complete")
        
    async def create_proposal(
        self,
        title: str,
        description: str,
        proposal_type: str,
        proposer: str,
        voting_mechanism: str,
        voting_period_days: int,
        chains: List[str],
        execution_data: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new governance proposal"""
        try:
            # Validate voting mechanism
            mechanism = VotingMechanism(voting_mechanism.upper())
            if not self.voting_registry.get_strategy(mechanism, {}):
                raise ValueError(f"Unknown voting mechanism: {voting_mechanism}")
                
            # Validate proposal type
            prop_type = ProposalType(proposal_type.lower())
            
            # Extract execution data
            targets = execution_data.get('targets', [])
            values = execution_data.get('values', [])
            calldatas = execution_data.get('calldatas', [])
            
            # Create proposal
            proposal = await self.proposal_manager.create_proposal(
                title=title,
                description=description,
                proposal_type=prop_type,
                proposer=proposer,
                voting_mechanism=voting_mechanism,
                voting_period_days=voting_period_days,
                execution_delay_hours=48,  # 48 hour timelock
                chains=chains,
                targets=targets,
                values=values,
                calldatas=calldatas,
                metadata=metadata
            )
            
            # Update proposer reputation
            await self.reputation_manager.update_reputation(
                user_address=proposer,
                chain=chains[0] if chains else "ethereum",
                event_type="proposal_created",
                impact=10  # Base reputation for creating proposal
            )
            
            # Emit event
            await self._emit_event("proposal_created", {
                "proposal_id": proposal.id,
                "title": proposal.title,
                "proposer": proposer,
                "chains": chains,
                "voting_mechanism": voting_mechanism
            })
            
            return {
                "proposal_id": proposal.id,
                "status": proposal.status.value,
                "voting_start": proposal.voting_start.isoformat(),
                "voting_end": proposal.voting_end.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to create proposal: {e}")
            raise
            
    async def cast_vote(
        self,
        proposal_id: str,
        voter: str,
        support: str,  # 'for', 'against', 'abstain'
        chain: str,
        signature: Optional[str] = None
    ) -> Dict[str, Any]:
        """Cast a vote on a proposal"""
        try:
            # Get proposal
            proposal = await self.proposal_manager.get_proposal(proposal_id)
            if not proposal:
                raise ValueError(f"Proposal {proposal_id} not found")
                
            # Get voter's base voting power (token balance)
            # In production, query from blockchain
            base_power = 100  # Placeholder
            
            # Get voting strategy
            mechanism = VotingMechanism(proposal.voting_mechanism.upper())
            strategy = self.voting_registry.get_strategy(mechanism, {})
            if not strategy:
                raise ValueError(f"Unknown voting mechanism: {proposal.voting_mechanism}")
                
            # Calculate voting power
            voting_power = await strategy.calculate_voting_power(
                voter=voter,
                base_power=base_power,
                proposal_data={
                    "id": proposal_id,
                    "type": proposal.proposal_type.value
                },
                chain_id=chain
            )
            
            # Apply reputation multiplier
            multiplier = await self.reputation_manager.calculate_voting_power_multiplier(
                voter,
                chain
            )
            final_voting_power = int(voting_power * multiplier)
            
            # Validate vote
            vote_valid = await strategy.validate_vote(
                voter=voter,
                vote_data={
                    "support": support,
                    "voting_power": final_voting_power,
                    "has_voted": False  # Check would be done in record_vote
                },
                proposal_data={
                    "id": proposal_id,
                    "type": proposal.proposal_type.value
                }
            )
            
            if not vote_valid:
                raise ValueError("Vote validation failed")
                
            # Record vote
            success = await self.proposal_manager.record_vote(
                proposal_id=proposal_id,
                voter=voter,
                support=support,
                voting_power=final_voting_power,
                chain=chain,
                signature=signature
            )
            
            if success:
                # Update voter reputation
                await self.reputation_manager.update_reputation(
                    user_address=voter,
                    chain=chain,
                    event_type="vote_cast",
                    impact=5  # Base reputation for voting
                )
                
                # Emit event
                await self._emit_event("vote_cast", {
                    "proposal_id": proposal_id,
                    "voter": voter,
                    "support": support,
                    "voting_power": final_voting_power,
                    "chain": chain
                })
                
                return {
                    "success": True,
                    "voting_power": final_voting_power,
                    "reputation_multiplier": multiplier
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to record vote"
                }
                
        except Exception as e:
            logger.error(f"Failed to cast vote: {e}")
            raise
            
    async def get_proposal_status(
        self,
        proposal_id: str
    ) -> Dict[str, Any]:
        """Get current status of a proposal"""
        proposal = await self.proposal_manager.get_proposal(proposal_id)
        if not proposal:
            raise ValueError(f"Proposal {proposal_id} not found")
            
        return {
            "proposal_id": proposal.id,
            "title": proposal.title,
            "status": proposal.status.value,
            "votes_for": proposal.votes_for,
            "votes_against": proposal.votes_against,
            "votes_abstain": proposal.votes_abstain,
            "unique_voters": proposal.unique_voters,
            "voting_start": proposal.voting_start.isoformat(),
            "voting_end": proposal.voting_end.isoformat(),
            "executed": proposal.executed_at.isoformat() if proposal.executed_at else None
        }
        
    async def execute_proposal(
        self,
        proposal_id: str
    ) -> Dict[str, Any]:
        """Execute a passed proposal"""
        return await self.proposal_executor.execute_proposal(proposal_id)
        
    async def get_user_reputation(
        self,
        user_address: str,
        chain: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get user's reputation score(s)"""
        scores = await self.reputation_manager.get_reputation_score(
            user_address,
            chain
        )
        
        return {
            "user": user_address,
            "scores": {
                chain: score.score if score else 0
                for chain, score in scores.items()
            }
        }
        
    async def sync_reputation(
        self,
        user_address: str
    ) -> Dict[str, Any]:
        """Sync user's reputation across chains"""
        scores = await self.reputation_manager.sync_reputation_across_chains(
            user_address
        )
        
        return {
            "user": user_address,
            "synced_scores": scores,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    async def _proposal_state_monitor(self):
        """Background task to monitor and update proposal states"""
        while True:
            try:
                await self.proposal_manager.check_proposal_state_transitions()
                await asyncio.sleep(60)  # Check every minute
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in proposal state monitor: {e}")
                await asyncio.sleep(60)
                
    async def _reputation_decay_task(self):
        """Background task to apply reputation decay"""
        while True:
            try:
                await self.reputation_manager.decay_reputation()
                await asyncio.sleep(86400)  # Run daily
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in reputation decay task: {e}")
                await asyncio.sleep(86400)
                
    async def _emit_event(self, event_type: str, data: Dict[str, Any]):
        """Emit a governance event"""
        if self.event_producer:
            event = {
                "type": event_type,
                "data": data,
                "timestamp": datetime.utcnow().isoformat(),
                "service": "dao-governance"
            }
            
            self.event_producer.send(
                json.dumps(event).encode('utf-8')
            )
            

# Import required modules
import json 