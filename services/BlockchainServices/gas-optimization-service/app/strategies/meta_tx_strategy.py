"""
Meta-Transaction Strategy
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from ..config import Settings
from ..models.optimization import (
    OptimizationRequest, GasRecommendation, OptimizationStrategy
)

logger = logging.getLogger(__name__)


class MetaTransactionStrategy:
    """Handles meta-transaction optimization"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self._relayer_stats: Dict[str, Dict[str, Any]] = {}
        
    async def initialize(self):
        """Initialize meta-transaction strategy"""
        logger.info("Initializing Meta-Transaction Strategy")
        # Load relayer configurations
        self._load_relayer_configs()
        
    async def shutdown(self):
        """Shutdown meta-transaction strategy"""
        pass
        
    def _load_relayer_configs(self):
        """Load relayer configurations"""
        # Initialize with default relayers
        for chain, addresses in self.settings.RELAYER_ADDRESSES.items():
            for address in addresses:
                self._relayer_stats[f"{chain}:{address}"] = {
                    "success_rate": 0.95,
                    "average_time": 30,
                    "reputation": 0.9,
                    "fee_percentage": 3.0
                }
                
    async def evaluate(
        self,
        request: OptimizationRequest,
        gas_prices: Dict[str, Any]
    ) -> Optional[GasRecommendation]:
        """Evaluate meta-transaction optimization"""
        # Check if relayers are available for this chain
        relayers = self.settings.RELAYER_ADDRESSES.get(request.chain, [])
        if not relayers:
            return None
            
        # Calculate direct transaction cost
        gas_estimate = request.estimated_gas or 100000
        gas_price = gas_prices.get(request.urgency.value, gas_prices.get('standard'))
        direct_cost = gas_estimate * int(gas_price)
        
        # Find best relayer
        best_relayer = None
        best_cost = direct_cost
        
        for relayer in relayers:
            stats = self._relayer_stats.get(f"{request.chain}:{relayer}", {})
            
            # Calculate relayer cost
            relayer_fee = direct_cost * (stats.get('fee_percentage', 5.0) / 100)
            total_cost = relayer_fee  # User pays only the relayer fee
            
            if total_cost < best_cost and stats.get('reputation', 0) > 0.8:
                best_relayer = relayer
                best_cost = total_cost
                
        if not best_relayer:
            return None
            
        # Calculate savings
        savings = direct_cost - best_cost
        if savings / direct_cost < 0.1:  # At least 10% savings
            return None
            
        stats = self._relayer_stats.get(f"{request.chain}:{best_relayer}", {})
        
        return GasRecommendation(
            strategy=OptimizationStrategy.META_TRANSACTION,
            gas_price="0",  # User doesn't pay gas
            estimated_cost=str(int(best_cost)),
            estimated_savings=str(int(savings)),
            savings_percentage=savings / direct_cost,
            expected_confirmation_time=stats.get('average_time', 30),
            confidence_score=stats.get('reputation', 0.9),
            reasoning=f"Use relayer to save {savings/direct_cost:.1%} on gas fees",
            alternatives=[{
                "relayer": best_relayer,
                "fee": str(int(best_cost)),
                "reputation": stats.get('reputation', 0.9)
            }]
        ) 