"""
Simple voting strategy implementation.
"""

from typing import Dict, Any, List
from collections import defaultdict

from .base import VotingStrategy


class SimpleVoting(VotingStrategy):
    """
    Simple majority voting implementation.
    One token = one vote.
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.quorum_percentage = config.get('quorum_percentage', 20)
        self.approval_threshold = config.get('approval_threshold', 50)
        self.min_voting_power = config.get('min_voting_power', 1)
        
    async def calculate_voting_power(
        self, 
        voter: str, 
        base_power: int,
        proposal_data: Dict[str, Any],
        chain_id: str
    ) -> int:
        """Simple 1:1 voting power"""
        if base_power < self.min_voting_power:
            return 0
        return base_power
        
    async def aggregate_votes(
        self,
        votes: List[Dict[str, Any]],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Aggregate simple votes"""
        votes_for = 0
        votes_against = 0
        votes_abstain = 0
        voters = set()
        
        for vote in votes:
            voter = vote['voter']
            support = vote['support']
            power = vote['voting_power']
            
            voters.add(voter)
            
            if support == 'for':
                votes_for += power
            elif support == 'against':
                votes_against += power
            else:  # abstain
                votes_abstain += power
                
        total_votes = votes_for + votes_against + votes_abstain
        
        return {
            'votes_for': votes_for,
            'votes_against': votes_against,
            'votes_abstain': votes_abstain,
            'total_votes': total_votes,
            'unique_voters': len(voters),
            'approval_percentage': (votes_for / total_votes * 100) if total_votes > 0 else 0
        }
        
    async def validate_vote(
        self,
        voter: str,
        vote_data: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> bool:
        """Validate a simple vote"""
        # Check if voter has already voted
        if vote_data.get('has_voted', False):
            return False
            
        # Check minimum voting power
        voting_power = vote_data.get('voting_power', 0)
        if voting_power < self.min_voting_power:
            return False
            
        # Check valid support value
        support = vote_data.get('support')
        if support not in ['for', 'against', 'abstain']:
            return False
            
        return True
        
    async def calculate_outcome(
        self,
        aggregated_votes: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate the outcome of simple voting"""
        total_supply = proposal_data.get('total_supply', 0)
        total_votes = aggregated_votes['total_votes']
        votes_for = aggregated_votes['votes_for']
        
        # Check quorum
        quorum_reached = False
        if total_supply > 0:
            participation_rate = (total_votes / total_supply) * 100
            quorum_reached = participation_rate >= self.quorum_percentage
        
        # Check approval threshold
        approval_percentage = aggregated_votes['approval_percentage']
        threshold_met = approval_percentage >= self.approval_threshold
        
        passed = quorum_reached and threshold_met
        
        return {
            'passed': passed,
            'votes_for': votes_for,
            'votes_against': aggregated_votes['votes_against'],
            'votes_abstain': aggregated_votes['votes_abstain'],
            'total_votes': total_votes,
            'unique_voters': aggregated_votes['unique_voters'],
            'approval_percentage': approval_percentage,
            'quorum_reached': quorum_reached,
            'participation_rate': participation_rate if total_supply > 0 else 0,
            'mechanism': 'simple_voting'
        } 