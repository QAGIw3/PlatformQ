"""
Conviction voting implementation

Conviction voting allows continuous signaling where conviction (voting power)
grows over time as long as tokens remain committed to a proposal.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
import math

from .strategy import VotingStrategy, VotingMechanism


class ConvictionVoting(VotingStrategy):
    """
    Conviction voting mechanism where:
    - Voting power grows over time while tokens are staked on a proposal
    - Conviction = tokens * time_coefficient
    - Proposals pass when they reach a dynamic threshold
    - Supports continuous voting and fund allocation
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.mechanism = VotingMechanism.CONVICTION
        
        # Conviction parameters
        self.alpha = Decimal(str(config.get('alpha', 0.9)))  # Decay parameter (0-1)
        self.beta = config.get('beta', 0.5)  # Time acceleration factor
        self.max_conviction_multiplier = config.get('max_conviction_multiplier', 10)
        self.min_conviction_threshold = config.get('min_conviction_threshold', 1000)
        
        # Funding parameters (for funding proposals)
        self.max_funding_percentage = config.get('max_funding_percentage', 10)  # Max % of treasury
        self.conviction_threshold_constant = config.get('conviction_threshold_constant', 0.01)
        
    async def calculate_voting_power(
        self, 
        voter: str, 
        base_power: int,
        proposal_data: Dict[str, Any],
        chain_id: str
    ) -> int:
        """
        Calculate conviction-based voting power
        Conviction grows over time: C(t) = tokens * (1 - alpha^t)
        """
        if base_power == 0:
            return 0
        
        # Get staking duration for this proposal
        staking_info = await self._get_staking_info(
            voter,
            proposal_data['proposal_id'],
            chain_id
        )
        
        if not staking_info['is_staked']:
            return 0
        
        # Calculate conviction based on staking duration
        days_staked = staking_info['days_staked']
        conviction_multiplier = self._calculate_conviction(days_staked)
        
        # Apply conviction to base power
        conviction_power = Decimal(str(base_power)) * conviction_multiplier
        
        return int(conviction_power.to_integral_value(ROUND_DOWN))
    
    async def _get_staking_info(
        self,
        voter: str,
        proposal_id: str,
        chain_id: str
    ) -> Dict[str, Any]:
        """
        Get staking information for voter on specific proposal
        In production, this would query on-chain staking data
        """
        # Placeholder implementation
        import random
        is_staked = random.choice([True, False])
        
        return {
            'is_staked': is_staked,
            'days_staked': random.randint(1, 30) if is_staked else 0,
            'stake_amount': random.randint(100, 10000) if is_staked else 0,
            'last_update': datetime.utcnow().timestamp()
        }
    
    def _calculate_conviction(self, days: int) -> Decimal:
        """
        Calculate conviction multiplier based on staking duration
        C(t) = max_multiplier * (1 - alpha^(beta * t))
        """
        if days <= 0:
            return Decimal('0')
        
        # Calculate conviction using exponential growth
        time_factor = Decimal(str(self.beta * days))
        conviction = Decimal(str(self.max_conviction_multiplier)) * \
                    (Decimal('1') - self.alpha ** time_factor)
        
        # Cap at maximum multiplier
        return min(conviction, Decimal(str(self.max_conviction_multiplier)))
    
    def calculate_threshold(self, requested_amount: Decimal, total_funds: Decimal) -> Decimal:
        """
        Calculate dynamic conviction threshold for funding proposals
        Threshold increases with requested amount
        """
        if total_funds == 0:
            return Decimal(str(self.min_conviction_threshold))
        
        # Calculate percentage of total funds requested
        percentage_requested = requested_amount / total_funds
        
        # Threshold grows exponentially with requested percentage
        # T = min_threshold * (1 + k * percentage^2)
        threshold_multiplier = 1 + (self.conviction_threshold_constant * 
                                   float(percentage_requested) ** 2)
        
        threshold = Decimal(str(self.min_conviction_threshold)) * \
                   Decimal(str(threshold_multiplier))
        
        return threshold
    
    async def aggregate_votes(
        self,
        votes: List[Dict[str, Any]],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aggregate conviction votes
        """
        aggregated = {
            'total_conviction': Decimal('0'),
            'supporting_conviction': Decimal('0'),
            'opposing_conviction': Decimal('0'),
            'total_stakers': 0,
            'conviction_distribution': {
                '0-1x': 0,
                '1-2x': 0,
                '2-5x': 0,
                '5-10x': 0
            },
            'average_stake_days': 0,
            'total_stake_days': 0
        }
        
        voters_seen = set()
        
        for vote in votes:
            voter = vote['voter']
            if voter in voters_seen:
                continue
            
            voters_seen.add(voter)
            
            # Get staking info
            staking_info = await self._get_staking_info(
                voter,
                proposal_data['proposal_id'],
                vote.get('chain_id', 'unknown')
            )
            
            if not staking_info['is_staked']:
                continue
            
            aggregated['total_stakers'] += 1
            aggregated['total_stake_days'] += staking_info['days_staked']
            
            # Calculate conviction
            conviction_power = await self.calculate_voting_power(
                voter,
                vote['base_power'],
                proposal_data,
                vote.get('chain_id', 'unknown')
            )
            
            # Categorize conviction level
            conviction_multiplier = conviction_power / vote['base_power'] if vote['base_power'] > 0 else 0
            if conviction_multiplier < 1:
                aggregated['conviction_distribution']['0-1x'] += 1
            elif conviction_multiplier < 2:
                aggregated['conviction_distribution']['1-2x'] += 1
            elif conviction_multiplier < 5:
                aggregated['conviction_distribution']['2-5x'] += 1
            else:
                aggregated['conviction_distribution']['5-10x'] += 1
            
            # Add to totals
            aggregated['total_conviction'] += Decimal(str(conviction_power))
            
            # In conviction voting, stakes can be for or against
            support = vote.get('support', vote.get('vote_type'))
            if support in [True, 'for', 'yes', 1]:
                aggregated['supporting_conviction'] += Decimal(str(conviction_power))
            else:
                aggregated['opposing_conviction'] += Decimal(str(conviction_power))
        
        # Calculate average stake duration
        if aggregated['total_stakers'] > 0:
            aggregated['average_stake_days'] = (
                aggregated['total_stake_days'] / aggregated['total_stakers']
            )
        
        # Calculate effective conviction (supporting - opposing)
        effective_conviction = (aggregated['supporting_conviction'] - 
                               aggregated['opposing_conviction'])
        
        return {
            'total_conviction': str(aggregated['total_conviction']),
            'supporting_conviction': str(aggregated['supporting_conviction']),
            'opposing_conviction': str(aggregated['opposing_conviction']),
            'effective_conviction': str(effective_conviction),
            'total_stakers': aggregated['total_stakers'],
            'conviction_distribution': aggregated['conviction_distribution'],
            'average_stake_days': aggregated['average_stake_days'],
            'mechanism': 'conviction'
        }
    
    async def validate_vote(
        self,
        voter: str,
        vote_data: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> bool:
        """
        Validate conviction vote (stake)
        """
        # Check if voter has tokens to stake
        base_power = vote_data.get('base_power', 0)
        if base_power <= 0:
            return False
        
        # Check if already staked on conflicting proposals
        if self.config.get('exclusive_staking', True):
            # In exclusive mode, can only stake on one proposal at a time
            existing_stakes = await self._get_existing_stakes(
                voter,
                vote_data.get('chain_id', 'unknown')
            )
            
            if existing_stakes and proposal_data['proposal_id'] not in existing_stakes:
                return False
        
        # Validate stake amount doesn't exceed available tokens
        stake_amount = vote_data.get('stake_amount', base_power)
        if stake_amount > base_power:
            return False
        
        return True
    
    async def _get_existing_stakes(self, voter: str, chain_id: str) -> List[str]:
        """Get list of proposals voter is currently staked on"""
        # Placeholder - would query on-chain data
        return []
    
    async def calculate_outcome(
        self,
        aggregated_votes: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate outcome based on conviction voting
        """
        effective_conviction = Decimal(aggregated_votes.get('effective_conviction', '0'))
        supporting_conviction = Decimal(aggregated_votes['supporting_conviction'])
        opposing_conviction = Decimal(aggregated_votes['opposing_conviction'])
        
        # For funding proposals, calculate dynamic threshold
        if proposal_data.get('proposal_type') == 'funding':
            requested_amount = Decimal(str(proposal_data.get('requested_amount', 0)))
            total_funds = Decimal(str(proposal_data.get('total_available_funds', 1000000)))
            
            threshold = self.calculate_threshold(requested_amount, total_funds)
            
            # Check if conviction exceeds threshold
            if effective_conviction >= threshold:
                status = 'APPROVED'
                # Calculate actual funding amount based on conviction
                conviction_ratio = min(
                    effective_conviction / threshold,
                    Decimal(str(self.max_funding_percentage / 100))
                )
                approved_amount = requested_amount * conviction_ratio
            else:
                status = 'PENDING'  # Conviction voting is continuous
                approved_amount = Decimal('0')
                
            return {
                'status': status,
                'effective_conviction': str(effective_conviction),
                'required_conviction': str(threshold),
                'conviction_ratio': float(effective_conviction / threshold) if threshold > 0 else 0,
                'supporting_conviction': str(supporting_conviction),
                'opposing_conviction': str(opposing_conviction),
                'approved_amount': str(approved_amount) if status == 'APPROVED' else '0',
                'requested_amount': str(requested_amount),
                'total_stakers': aggregated_votes['total_stakers'],
                'average_stake_days': aggregated_votes['average_stake_days'],
                'mechanism': 'conviction'
            }
        else:
            # For non-funding proposals, use simple threshold
            threshold = Decimal(str(self.min_conviction_threshold))
            
            if effective_conviction >= threshold:
                status = 'APPROVED'
            elif opposing_conviction > supporting_conviction:
                status = 'REJECTED'
            else:
                status = 'PENDING'
            
            return {
                'status': status,
                'effective_conviction': str(effective_conviction),
                'required_conviction': str(threshold),
                'conviction_ratio': float(effective_conviction / threshold) if threshold > 0 else 0,
                'supporting_conviction': str(supporting_conviction),
                'opposing_conviction': str(opposing_conviction),
                'total_stakers': aggregated_votes['total_stakers'],
                'average_stake_days': aggregated_votes['average_stake_days'],
                'mechanism': 'conviction'
            } 