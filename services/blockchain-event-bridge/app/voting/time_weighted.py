"""
Time-weighted voting implementation

Voting power increases based on how long tokens have been held or staked.
This rewards long-term participants and reduces influence of short-term speculators.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN

from .strategy import VotingStrategy, VotingMechanism


class TimeWeightedVoting(VotingStrategy):
    """
    Time-weighted voting mechanism where:
    - Voting power = base_power * time_multiplier
    - Time multiplier increases with holding duration
    - Can include decay for very old holdings
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.mechanism = VotingMechanism.TIME_WEIGHTED
        
        # Time weight configuration
        self.min_holding_days = config.get('min_holding_days', 1)
        self.max_multiplier = config.get('max_multiplier', 4.0)
        self.half_life_days = config.get('half_life_days', 365)  # Days to reach half max multiplier
        self.enable_decay = config.get('enable_decay', False)
        self.decay_start_days = config.get('decay_start_days', 1095)  # 3 years
        self.snapshot_lookback = config.get('snapshot_lookback', 7)  # Days before proposal
        
    async def calculate_voting_power(
        self, 
        voter: str, 
        base_power: int,
        proposal_data: Dict[str, Any],
        chain_id: str
    ) -> int:
        """
        Calculate time-weighted voting power
        """
        if base_power == 0:
            return 0
        
        # Get holding duration (would come from chain data in real implementation)
        holding_duration = await self._get_holding_duration(
            voter,
            chain_id,
            proposal_data.get('snapshot_time', datetime.utcnow())
        )
        
        # Calculate time multiplier
        time_multiplier = self._calculate_time_multiplier(holding_duration)
        
        # Apply multiplier to base power
        weighted_power = Decimal(str(base_power)) * Decimal(str(time_multiplier))
        
        return int(weighted_power.to_integral_value(ROUND_DOWN))
    
    async def _get_holding_duration(
        self,
        voter: str,
        chain_id: str,
        snapshot_time: datetime
    ) -> int:
        """
        Get token holding duration in days
        In real implementation, this would query blockchain history
        """
        # Placeholder - in production this would:
        # 1. Query token transfer history
        # 2. Calculate weighted average holding time
        # 3. Consider staking periods
        
        # For demo, return random duration
        import random
        return random.randint(1, 1000)
    
    def _calculate_time_multiplier(self, holding_days: int) -> float:
        """
        Calculate time-based multiplier using exponential curve
        """
        if holding_days < self.min_holding_days:
            return 0.0
        
        # Calculate base multiplier using exponential growth
        # multiplier = max_multiplier * (1 - e^(-k * days))
        # where k is calculated to reach 50% at half_life_days
        
        import math
        k = math.log(2) / self.half_life_days
        
        base_multiplier = self.max_multiplier * (1 - math.exp(-k * holding_days))
        
        # Apply decay if enabled and holding period exceeds threshold
        if self.enable_decay and holding_days > self.decay_start_days:
            decay_days = holding_days - self.decay_start_days
            decay_factor = math.exp(-decay_days / self.half_life_days)
            base_multiplier *= decay_factor
        
        # Ensure minimum multiplier of 1.0 for valid holdings
        return max(1.0, base_multiplier)
    
    async def aggregate_votes(
        self,
        votes: List[Dict[str, Any]],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Aggregate time-weighted votes
        """
        aggregated = {
            'for_votes': Decimal('0'),
            'against_votes': Decimal('0'),
            'abstain_votes': Decimal('0'),
            'total_voters': 0,
            'time_weight_distribution': {
                '0-30d': 0,
                '30-90d': 0,
                '90-365d': 0,
                '1-2y': 0,
                '2y+': 0
            },
            'average_holding_days': 0,
            'total_holding_days': 0
        }
        
        voters_seen = set()
        
        for vote in votes:
            voter = vote['voter']
            if voter in voters_seen:
                continue
            
            voters_seen.add(voter)
            aggregated['total_voters'] += 1
            
            # Calculate time-weighted voting power
            weighted_power = await self.calculate_voting_power(
                voter,
                vote['base_power'],
                proposal_data,
                vote.get('chain_id', 'unknown')
            )
            
            # Get holding duration for statistics
            holding_days = await self._get_holding_duration(
                voter,
                vote.get('chain_id', 'unknown'),
                proposal_data.get('snapshot_time', datetime.utcnow())
            )
            
            aggregated['total_holding_days'] += holding_days
            
            # Categorize by holding period
            if holding_days < 30:
                aggregated['time_weight_distribution']['0-30d'] += 1
            elif holding_days < 90:
                aggregated['time_weight_distribution']['30-90d'] += 1
            elif holding_days < 365:
                aggregated['time_weight_distribution']['90-365d'] += 1
            elif holding_days < 730:
                aggregated['time_weight_distribution']['1-2y'] += 1
            else:
                aggregated['time_weight_distribution']['2y+'] += 1
            
            # Apply vote
            support = vote.get('support', vote.get('vote_type'))
            
            if support in [True, 'for', 'yes', 1]:
                aggregated['for_votes'] += Decimal(str(weighted_power))
            elif support in [False, 'against', 'no', 0]:
                aggregated['against_votes'] += Decimal(str(weighted_power))
            else:
                aggregated['abstain_votes'] += Decimal(str(weighted_power))
        
        # Calculate average holding period
        if aggregated['total_voters'] > 0:
            aggregated['average_holding_days'] = (
                aggregated['total_holding_days'] // aggregated['total_voters']
            )
        
        return {
            'for_votes': str(aggregated['for_votes']),
            'against_votes': str(aggregated['against_votes']),
            'abstain_votes': str(aggregated['abstain_votes']),
            'total_voters': aggregated['total_voters'],
            'time_weight_distribution': aggregated['time_weight_distribution'],
            'average_holding_days': aggregated['average_holding_days'],
            'mechanism': 'time_weighted'
        }
    
    async def validate_vote(
        self,
        voter: str,
        vote_data: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> bool:
        """
        Validate time-weighted vote
        """
        # Check if voter held tokens before snapshot
        snapshot_time = proposal_data.get('snapshot_time', datetime.utcnow())
        holding_duration = await self._get_holding_duration(
            voter,
            vote_data.get('chain_id', 'unknown'),
            snapshot_time
        )
        
        if holding_duration < self.min_holding_days:
            return False
        
        # Verify snapshot was taken before proposal creation
        if 'created_at' in proposal_data:
            proposal_time = datetime.fromtimestamp(proposal_data['created_at'])
            lookback_time = proposal_time - timedelta(days=self.snapshot_lookback)
            
            if snapshot_time > proposal_time or snapshot_time < lookback_time:
                return False
        
        return True
    
    async def calculate_outcome(
        self,
        aggregated_votes: Dict[str, Any],
        proposal_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Calculate outcome based on time-weighted voting
        """
        for_votes = Decimal(aggregated_votes['for_votes'])
        against_votes = Decimal(aggregated_votes['against_votes'])
        abstain_votes = Decimal(aggregated_votes['abstain_votes'])
        
        total_votes = for_votes + against_votes + abstain_votes
        
        # Calculate metrics
        total_voters = aggregated_votes['total_voters']
        eligible_voters = proposal_data.get('eligible_voters', total_voters * 2)
        
        participation_rate = (total_voters / eligible_voters * 100) if eligible_voters > 0 else 0
        
        # Determine outcome
        if total_votes == 0:
            status = 'NO_VOTES'
            approval_rate = 0
        else:
            approval_rate = float((for_votes / (for_votes + against_votes) * 100)) \
                if (for_votes + against_votes) > 0 else 0
            
            # Check quorum (can be adjusted based on average holding time)
            quorum_required = proposal_data.get('quorum_percentage', 20)
            
            # Bonus quorum reduction for long-term holders
            avg_holding_days = aggregated_votes.get('average_holding_days', 0)
            if avg_holding_days > 365:
                quorum_reduction = min(5, avg_holding_days // 365)  # 1% per year, max 5%
                quorum_required = max(10, quorum_required - quorum_reduction)
            
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
            'adjusted_quorum': quorum_required if total_votes > 0 else proposal_data.get('quorum_percentage', 20),
            'time_weight_distribution': aggregated_votes.get('time_weight_distribution', {}),
            'average_holding_days': aggregated_votes.get('average_holding_days', 0),
            'mechanism': 'time_weighted'
        } 