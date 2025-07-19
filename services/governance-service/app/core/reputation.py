"""
Reputation management for DAO governance.
"""

import logging
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
from dataclasses import dataclass
from decimal import Decimal

from pyignite import Client as IgniteClient
from platformq_blockchain_common import ChainType

logger = logging.getLogger(__name__)


@dataclass
class ReputationScore:
    """User reputation score data"""
    user_address: str
    chain: str
    score: int
    last_updated: datetime
    factors: Dict[str, int]  # Breakdown of score components
    

@dataclass
class ReputationEvent:
    """Event that affects reputation"""
    user_address: str
    event_type: str
    impact: int  # Positive or negative impact
    chain: str
    timestamp: datetime
    metadata: Dict[str, Any]


class ReputationManager:
    """Manages user reputation across chains"""
    
    def __init__(self, ignite_client: IgniteClient, blockchain_gateway):
        self.ignite_client = ignite_client
        self.blockchain_gateway = blockchain_gateway
        self._reputation_cache = None
        self._events_cache = None
        
        # Reputation factors and weights
        self.reputation_factors = {
            'governance_participation': 30,  # Weight for voting
            'proposal_success': 25,  # Successful proposals
            'token_holding': 20,  # Token balance/staking
            'community_contribution': 15,  # Off-chain contributions
            'time_weighted': 10  # Account age/activity
        }
        
    async def initialize(self):
        """Initialize reputation caches"""
        self._reputation_cache = await self.ignite_client.get_or_create_cache(
            "dao_reputation_scores"
        )
        self._events_cache = await self.ignite_client.get_or_create_cache(
            "dao_reputation_events"
        )
        logger.info("Reputation manager initialized")
        
    async def get_reputation_score(
        self,
        user_address: str,
        chain: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get user's reputation score(s)"""
        if chain:
            # Get score for specific chain
            cache_key = f"{user_address}:{chain}"
            score = await self._reputation_cache.get(cache_key)
            return {chain: score} if score else {chain: None}
        else:
            # Get scores for all chains
            scores = {}
            for chain_type in ChainType:
                cache_key = f"{user_address}:{chain_type.value}"
                score = await self._reputation_cache.get(cache_key)
                if score:
                    scores[chain_type.value] = score
            return scores
            
    async def update_reputation(
        self,
        user_address: str,
        chain: str,
        event_type: str,
        impact: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ReputationScore:
        """Update user's reputation based on an event"""
        # Record event
        event = ReputationEvent(
            user_address=user_address,
            event_type=event_type,
            impact=impact,
            chain=chain,
            timestamp=datetime.utcnow(),
            metadata=metadata or {}
        )
        
        event_key = f"{user_address}:{chain}:{datetime.utcnow().isoformat()}"
        await self._events_cache.put(event_key, event)
        
        # Calculate new score
        cache_key = f"{user_address}:{chain}"
        current_score = await self._reputation_cache.get(cache_key)
        
        if current_score:
            # Update existing score
            new_score_value = max(0, current_score.score + impact)
            
            # Update factor breakdown
            factor = self._event_type_to_factor(event_type)
            if factor in current_score.factors:
                current_score.factors[factor] += impact
            else:
                current_score.factors[factor] = impact
                
            current_score.score = new_score_value
            current_score.last_updated = datetime.utcnow()
        else:
            # Create new score
            factor = self._event_type_to_factor(event_type)
            current_score = ReputationScore(
                user_address=user_address,
                chain=chain,
                score=max(0, impact),
                last_updated=datetime.utcnow(),
                factors={factor: impact}
            )
            
        await self._reputation_cache.put(cache_key, current_score)
        logger.info(f"Updated reputation for {user_address} on {chain}: {current_score.score}")
        
        return current_score
        
    async def sync_reputation_across_chains(
        self,
        user_address: str
    ) -> Dict[str, int]:
        """Synchronize reputation across all chains"""
        aggregated_scores = {}
        total_weighted_score = 0
        total_weight = 0
        
        # Get scores from all chains
        for chain_type in ChainType:
            if chain_type in [ChainType.ETHEREUM, ChainType.POLYGON, 
                             ChainType.ARBITRUM, ChainType.OPTIMISM]:
                cache_key = f"{user_address}:{chain_type.value}"
                score = await self._reputation_cache.get(cache_key)
                
                if score:
                    # Weight by chain activity/importance
                    chain_weight = self._get_chain_weight(chain_type)
                    aggregated_scores[chain_type.value] = score.score
                    total_weighted_score += score.score * chain_weight
                    total_weight += chain_weight
                    
        # Calculate cross-chain reputation
        if total_weight > 0:
            cross_chain_score = int(total_weighted_score / total_weight)
        else:
            cross_chain_score = 0
            
        aggregated_scores['cross_chain'] = cross_chain_score
        
        # Store cross-chain score
        cross_chain_key = f"{user_address}:cross_chain"
        await self._reputation_cache.put(
            cross_chain_key,
            ReputationScore(
                user_address=user_address,
                chain="cross_chain",
                score=cross_chain_score,
                last_updated=datetime.utcnow(),
                factors={}
            )
        )
        
        logger.info(f"Synced reputation for {user_address}: {cross_chain_score}")
        return aggregated_scores
        
    async def calculate_voting_power_multiplier(
        self,
        user_address: str,
        chain: str
    ) -> float:
        """Calculate voting power multiplier based on reputation"""
        cache_key = f"{user_address}:{chain}"
        score = await self._reputation_cache.get(cache_key)
        
        if not score:
            return 1.0  # Base multiplier
            
        # Logarithmic scaling to prevent excessive concentration
        # Every 1000 reputation points = 10% bonus (max 2x at 10k)
        multiplier = 1.0 + min(score.score / 10000, 1.0)
        
        return multiplier
        
    async def get_top_contributors(
        self,
        chain: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get top contributors by reputation"""
        contributors = []
        
        # Scan reputation cache
        cursor = self._reputation_cache.scan()
        for key, score in cursor:
            if chain and not key.endswith(f":{chain}"):
                continue
                
            contributors.append({
                'address': score.user_address,
                'chain': score.chain,
                'score': score.score,
                'factors': score.factors,
                'last_updated': score.last_updated
            })
            
        # Sort by score and limit
        contributors.sort(key=lambda x: x['score'], reverse=True)
        return contributors[:limit]
        
    async def decay_reputation(self):
        """Apply time-based decay to reputation scores"""
        now = datetime.utcnow()
        decay_rate = 0.01  # 1% per month
        
        cursor = self._reputation_cache.scan()
        for key, score in cursor:
            # Skip cross-chain scores
            if score.chain == "cross_chain":
                continue
                
            # Calculate days since last update
            days_inactive = (now - score.last_updated).days
            
            # Apply decay after 30 days of inactivity
            if days_inactive > 30:
                months_inactive = days_inactive / 30
                decay_factor = 1 - (decay_rate * months_inactive)
                decay_factor = max(0.5, decay_factor)  # Min 50% retention
                
                score.score = int(score.score * decay_factor)
                score.last_updated = now
                
                await self._reputation_cache.put(key, score)
                logger.info(f"Applied decay to {score.user_address}: {score.score}")
                
    def _event_type_to_factor(self, event_type: str) -> str:
        """Map event type to reputation factor"""
        mapping = {
            'vote_cast': 'governance_participation',
            'proposal_created': 'governance_participation',
            'proposal_passed': 'proposal_success',
            'proposal_executed': 'proposal_success',
            'tokens_staked': 'token_holding',
            'contribution_verified': 'community_contribution'
        }
        
        return mapping.get(event_type, 'governance_participation')
        
    def _get_chain_weight(self, chain_type: ChainType) -> float:
        """Get weight for a chain in cross-chain calculation"""
        weights = {
            ChainType.ETHEREUM: 1.0,
            ChainType.POLYGON: 0.8,
            ChainType.ARBITRUM: 0.9,
            ChainType.OPTIMISM: 0.9,
            ChainType.AVALANCHE: 0.7,
            ChainType.BSC: 0.6
        }
        
        return weights.get(chain_type, 0.5) 