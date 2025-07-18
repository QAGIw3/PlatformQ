"""
Quadratic voting strategy implementation.
"""

import math
from typing import Dict, Any, List
from decimal import Decimal
from collections import defaultdict

from .base import VotingStrategy


class QuadraticVoting(VotingStrategy):
    """
    Quadratic voting implementation.
    
    In quadratic voting, the cost of votes increases quadratically:
    - 1 vote costs 1 token
    - 2 votes cost 4 tokens
    - 3 votes cost 9 tokens
    - n votes cost n^2 tokens
    
    This prevents wealthy voters from having disproportionate influence.
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.min_tokens_required = config.get('min_tokens_required', 1)
        self.max_votes_per_option = config.get('max_votes_per_option', 100)
        self.allow_negative_votes = config.get('allow_negative_votes', True)
        
    async def calculate_voting_power(
        self, 
        voter: str, 
        base_power: int,
        proposal_data: Dict[str, Any],
        chain_id: str
    ) -> int:
        """Calculate quadratic voting power based on token balance"""
        # In quadratic voting, voting power is the square root of tokens
        # This represents the maximum number of votes they can cast
        if base_power < self.min_tokens_required:
            return 0
            
        # Maximum votes = floor(sqrt(tokens))
        max_votes = int(math.sqrt(base_power))
        
        # Apply proposal-specific limits if any
        proposal_max = proposal_data.get('max_votes_per_voter', self.max_votes_per_option)
        
        return min(max_votes, proposal_max)
        
    async def aggregate_votes(
        self,
        votes: List[Dict[str, Any]],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Aggregate quadratic votes"""
        option_votes = defaultdict(int)
        option_voters = defaultdict(set)
        total_cost = Decimal(0)
        voter_costs = {}
        voter_allocations = defaultdict(lambda: defaultdict(int))
        
        # Process each vote
        for vote in votes:
            voter = vote['voter']
            option = vote['option']
            num_votes = vote.get('votes', 1)
            
            # Handle negative votes if allowed
            if num_votes < 0 and not self.allow_negative_votes:
                continue
                
            # Calculate cost for this vote
            vote_cost = num_votes ** 2
            
            # Track allocations and costs
            voter_allocations[voter][option] += num_votes
            if voter not in voter_costs:
                voter_costs[voter] = 0
            voter_costs[voter] += vote_cost
            
            # Add to option totals
            option_votes[option] += num_votes
            option_voters[option].add(voter)
            total_cost += vote_cost
            
        # Calculate vote distribution statistics
        vote_distribution = {
            opt: {
                'votes': votes,
                'voters': len(voters),
                'average_votes': votes / len(voters) if voters else 0
            }
            for opt, (votes, voters) in zip(
                option_votes.keys(),
                zip(option_votes.values(), option_voters.values())
            )
        }
        
        # Find winning option(s)
        max_votes = max(option_votes.values()) if option_votes else 0
        winners = [opt for opt, votes in option_votes.items() if votes == max_votes]
        
        return {
            'option_votes': dict(option_votes),
            'option_voters': {k: len(v) for k, v in option_voters.items()},
            'total_voters': len(voter_costs),
            'total_cost': str(total_cost),
            'average_cost_per_voter': str(total_cost / len(voter_costs)) if voter_costs else "0",
            'voter_costs': voter_costs,
            'voter_allocations': {k: dict(v) for k, v in voter_allocations.items()},
            'vote_distribution': vote_distribution,
            'winners': winners,
            'power_distribution': self._calculate_power_distribution(voter_costs)
        }
        
    async def validate_vote(
        self,
        voter: str,
        vote_data: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> bool:
        """Validate a quadratic vote"""
        # Get voter's base power (token balance)
        base_power = vote_data.get('voter_power', 0)
        if base_power < self.min_tokens_required:
            return False
            
        # Get all votes by this voter
        voter_votes = vote_data.get('all_votes', {})
        
        # Calculate total cost
        total_cost = 0
        for option, num_votes in voter_votes.items():
            if abs(num_votes) > self.max_votes_per_option:
                return False
            if num_votes < 0 and not self.allow_negative_votes:
                return False
            total_cost += num_votes ** 2
            
        # Check if voter has enough tokens
        return total_cost <= base_power
        
    async def calculate_outcome(
        self,
        aggregated_votes: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate the outcome of quadratic voting"""
        option_votes = aggregated_votes['option_votes']
        total_voters = aggregated_votes['total_voters']
        
        # Determine outcome based on proposal type
        proposal_type = proposal_data.get('type', 'single_choice')
        
        if proposal_type == 'single_choice':
            # Winner takes all
            winners = aggregated_votes['winners']
            passed = len(winners) == 1  # Tie means no decision
            winning_option = winners[0] if passed else None
            
        elif proposal_type == 'multiple_choice':
            # All options above threshold pass
            threshold = proposal_data.get('approval_threshold', 0)
            winning_options = [
                opt for opt, votes in option_votes.items()
                if votes > threshold
            ]
            passed = len(winning_options) > 0
            winning_option = winning_options
            
        elif proposal_type == 'allocation':
            # Proportional allocation based on votes
            total_votes = sum(max(0, v) for v in option_votes.values())
            if total_votes > 0:
                allocations = {
                    opt: max(0, votes) / total_votes
                    for opt, votes in option_votes.items()
                }
                passed = True
                winning_option = allocations
            else:
                passed = False
                winning_option = None
        else:
            passed = False
            winning_option = None
            
        # Calculate participation metrics
        max_possible_voters = proposal_data.get('eligible_voters', total_voters)
        participation_rate = (total_voters / max_possible_voters * 100) if max_possible_voters > 0 else 0
        
        return {
            'passed': passed,
            'winning_option': winning_option,
            'option_votes': option_votes,
            'total_voters': total_voters,
            'participation_rate': participation_rate,
            'vote_distribution': aggregated_votes['vote_distribution'],
            'power_distribution': aggregated_votes['power_distribution'],
            'proposal_type': proposal_type,
            'mechanism': 'quadratic_voting'
        }
        
    def _calculate_power_distribution(self, voter_costs: Dict[str, int]) -> Dict[str, Any]:
        """Calculate voting power distribution metrics"""
        if not voter_costs:
            return {}
            
        costs = list(voter_costs.values())
        costs.sort()
        
        # Calculate Gini coefficient
        n = len(costs)
        index = range(1, n + 1)
        gini = (2 * sum(index[i] * costs[i] for i in range(n))) / (n * sum(costs)) - (n + 1) / n
        
        return {
            'gini_coefficient': gini,
            'min_cost': min(costs),
            'max_cost': max(costs),
            'median_cost': costs[n // 2],
            'cost_buckets': self._get_power_buckets(costs)
        }
        
    def _get_power_buckets(self, costs: List[int]) -> Dict[str, int]:
        """Categorize voters by voting power used"""
        buckets = {
            '1_vote': 0,
            '2-5_votes': 0,
            '6-10_votes': 0,
            '11-25_votes': 0,
            '26-50_votes': 0,
            '51-100_votes': 0,
            '100+_votes': 0
        }
        
        for cost in costs:
            votes = int(math.sqrt(cost))
            if votes == 1:
                buckets['1_vote'] += 1
            elif votes <= 5:
                buckets['2-5_votes'] += 1
            elif votes <= 10:
                buckets['6-10_votes'] += 1
            elif votes <= 25:
                buckets['11-25_votes'] += 1
            elif votes <= 50:
                buckets['26-50_votes'] += 1
            elif votes <= 100:
                buckets['51-100_votes'] += 1
            else:
                buckets['100+_votes'] += 1
                
        return buckets
        
    def calculate_vote_cost(self, num_votes: int) -> int:
        """Calculate the cost of a given number of votes"""
        return num_votes ** 2 