"""
Quadratic voting implementation

In quadratic voting, the cost of votes increases quadratically.
This reduces the influence of large token holders and promotes broader participation.
"""

import math
from typing import Dict, Any, List, Optional
from decimal import Decimal, ROUND_DOWN

from .strategy import VotingStrategy, VotingMechanism


class QuadraticVoting(VotingStrategy):
    """
    Quadratic voting mechanism where:
    - Cost to cast n votes = n²
    - Voting power = sqrt(tokens)
    - Prevents whale domination
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.mechanism = VotingMechanism.QUADRATIC
        self.min_tokens = config.get('min_tokens', 1)
        self.voice_credit_basis = config.get('voice_credit_basis', 1000000)  # 1M basis
        self.allow_negative_votes = config.get('allow_negative_votes', True)
        
    async def calculate_voting_power(
        self, 
        voter: str, 
        base_power: int,
        proposal_data: Dict[str, Any],
        chain_id: str
    ) -> int:
        """
        Calculate quadratic voting power
        Power = sqrt(tokens) * scaling_factor
        """
        if base_power < self.min_tokens:
            return 0
        
        # Use Decimal for precision
        tokens = Decimal(str(base_power))
        
        # Calculate square root
        sqrt_tokens = tokens.sqrt()
        
        # Apply scaling factor if configured
        scaling_factor = Decimal(str(self.config.get('scaling_factor', 1)))
        voting_power = sqrt_tokens * scaling_factor
        
        # Round down to integer
        return int(voting_power.to_integral_value(ROUND_DOWN))
    
    async def aggregate_votes(
        self,
        votes: List[Dict[str, Any]],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aggregate quadratic votes
        """
        aggregated = {
            'for_votes': Decimal('0'),
            'against_votes': Decimal('0'),
            'abstain_votes': Decimal('0'),
            'total_voters': 0,
            'vote_distribution': {},
            'quadratic_scores': {
                'for': Decimal('0'),
                'against': Decimal('0'),
                'abstain': Decimal('0')
            }
        }
        
        voters_seen = set()
        
        for vote in votes:
            voter = vote['voter']
            if voter in voters_seen:
                continue  # Prevent double voting
            
            voters_seen.add(voter)
            aggregated['total_voters'] += 1
            
            # Calculate quadratic voting power
            voting_power = await self.calculate_voting_power(
                voter,
                vote['base_power'],
                proposal_data,
                vote.get('chain_id', 'unknown')
            )
            
            # Handle multi-choice voting if enabled
            if self.config.get('allow_split_votes', False) and 'vote_allocation' in vote:
                # Voter can split their votes
                allocations = vote['vote_allocation']
                total_allocated = sum(abs(v) for v in allocations.values())
                
                if total_allocated > voting_power:
                    # Invalid allocation, skip
                    continue
                
                for choice, allocation in allocations.items():
                    if choice == 'for':
                        aggregated['for_votes'] += Decimal(str(abs(allocation)))
                        aggregated['quadratic_scores']['for'] += Decimal(str(allocation))
                    elif choice == 'against':
                        aggregated['against_votes'] += Decimal(str(abs(allocation)))
                        aggregated['quadratic_scores']['against'] += Decimal(str(allocation))
                    elif choice == 'abstain':
                        aggregated['abstain_votes'] += Decimal(str(abs(allocation)))
                        aggregated['quadratic_scores']['abstain'] += Decimal(str(allocation))
            else:
                # Simple vote
                support = vote.get('support', vote.get('vote_type'))
                
                if support in [True, 'for', 'yes', 1]:
                    aggregated['for_votes'] += Decimal(str(voting_power))
                    aggregated['quadratic_scores']['for'] += Decimal(str(voting_power))
                elif support in [False, 'against', 'no', 0]:
                    aggregated['against_votes'] += Decimal(str(voting_power))
                    aggregated['quadratic_scores']['against'] += Decimal(str(voting_power))
                else:
                    aggregated['abstain_votes'] += Decimal(str(voting_power))
                    aggregated['quadratic_scores']['abstain'] += Decimal(str(voting_power))
            
            # Track distribution
            if voting_power > 0:
                power_bucket = self._get_power_bucket(voting_power)
                aggregated['vote_distribution'][power_bucket] = \
                    aggregated['vote_distribution'].get(power_bucket, 0) + 1
        
        # Convert to strings for JSON serialization
        return {
            'for_votes': str(aggregated['for_votes']),
            'against_votes': str(aggregated['against_votes']),
            'abstain_votes': str(aggregated['abstain_votes']),
            'total_voters': aggregated['total_voters'],
            'vote_distribution': aggregated['vote_distribution'],
            'quadratic_scores': {
                k: str(v) for k, v in aggregated['quadratic_scores'].items()
            },
            'mechanism': 'quadratic'
        }
    
    async def validate_vote(
        self,
        voter: str,
        vote_data: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> bool:
        """
        Validate quadratic vote
        """
        # Check minimum token requirement
        base_power = vote_data.get('base_power', 0)
        if base_power < self.min_tokens:
            return False
        
        # Check if split voting is allowed
        if 'vote_allocation' in vote_data and not self.config.get('allow_split_votes', False):
            return False
        
        # Validate split vote allocations
        if 'vote_allocation' in vote_data:
            voting_power = await self.calculate_voting_power(
                voter,
                base_power,
                proposal_data,
                vote_data.get('chain_id', 'unknown')
            )
            
            total_allocated = sum(abs(v) for v in vote_data['vote_allocation'].values())
            if total_allocated > voting_power:
                return False
            
            # Check for negative votes if not allowed
            if not self.allow_negative_votes:
                if any(v < 0 for v in vote_data['vote_allocation'].values()):
                    return False
        
        return True
    
    async def calculate_outcome(
        self,
        aggregated_votes: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate outcome based on quadratic voting
        """
        for_votes = Decimal(aggregated_votes['for_votes'])
        against_votes = Decimal(aggregated_votes['against_votes'])
        abstain_votes = Decimal(aggregated_votes['abstain_votes'])
        
        total_votes = for_votes + against_votes + abstain_votes
        
        # Calculate quorum
        total_voters = aggregated_votes['total_voters']
        eligible_voters = proposal_data.get('eligible_voters', total_voters * 2)  # Estimate
        
        participation_rate = (total_voters / eligible_voters * 100) if eligible_voters > 0 else 0
        
        # Determine outcome
        if total_votes == 0:
            status = 'NO_VOTES'
            approval_rate = 0
        else:
            approval_rate = float((for_votes / (for_votes + against_votes) * 100)) if (for_votes + against_votes) > 0 else 0
            
            # Check quorum
            quorum_required = proposal_data.get('quorum_percentage', 20)
            if participation_rate < quorum_required:
                status = 'FAILED_QUORUM'
            elif approval_rate >= proposal_data.get('approval_threshold', 50):
                status = 'APPROVED'
            else:
                status = 'REJECTED'
        
        return {
            'status': status,
            'for_votes': str(for_votes),
            'against_votes': str(against_votes),
            'abstain_votes': str(abstain_votes),
            'total_votes': str(total_votes),
            'approval_rate': approval_rate,
            'participation_rate': participation_rate,
            'vote_distribution': aggregated_votes.get('vote_distribution', {}),
            'quadratic_scores': aggregated_votes.get('quadratic_scores', {}),
            'mechanism': 'quadratic'
        }
    
    def _get_power_bucket(self, power: int) -> str:
        """Categorize voting power into buckets for analysis"""
        if power < 10:
            return "0-10"
        elif power < 100:
            return "10-100"
        elif power < 1000:
            return "100-1K"
        elif power < 10000:
            return "1K-10K"
        else:
            return "10K+"
    
    def calculate_vote_cost(self, num_votes: int) -> int:
        """Calculate the cost in tokens to cast n votes"""
        return num_votes ** 2 