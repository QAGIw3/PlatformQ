"""
Layer 2 Migration Strategy
"""

import logging
from typing import Dict, Any, Optional, List

from ..config import Settings
from ..models.optimization import (
    OptimizationRequest, GasRecommendation, OptimizationStrategy,
    L2Suggestion
)

logger = logging.getLogger(__name__)


class L2Strategy:
    """Handles Layer 2 migration suggestions"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self._l2_mappings = {
            "ethereum": ["arbitrum", "optimism", "polygon", "zksync"],
            "bsc": ["polygon"],
        }
        self._bridge_info = {
            ("ethereum", "arbitrum"): {
                "contract": "0x...",  # Arbitrum bridge
                "time": 600,  # 10 minutes
                "cost_multiplier": 1.5
            },
            ("ethereum", "optimism"): {
                "contract": "0x...",  # Optimism bridge
                "time": 300,  # 5 minutes
                "cost_multiplier": 1.3
            },
            ("ethereum", "polygon"): {
                "contract": "0x...",  # Polygon bridge
                "time": 1800,  # 30 minutes
                "cost_multiplier": 1.2
            }
        }
        
    async def initialize(self):
        """Initialize L2 strategy"""
        logger.info("Initializing L2 Strategy")
        
    async def shutdown(self):
        """Shutdown L2 strategy"""
        pass
        
    async def evaluate(
        self,
        request: OptimizationRequest,
        gas_prices: Dict[str, Any]
    ) -> Optional[GasRecommendation]:
        """Evaluate L2 migration option"""
        # Check if L2 options exist for this chain
        l2_options = self._l2_mappings.get(request.chain, [])
        if not l2_options:
            return None
            
        # Calculate L1 cost
        gas_estimate = request.estimated_gas or 100000
        l1_gas_price = int(gas_prices.get(request.urgency.value, gas_prices.get('standard')))
        l1_cost = gas_estimate * l1_gas_price
        
        # Find best L2 option
        best_l2 = None
        best_savings = 0
        best_suggestion = None
        
        for l2_chain in l2_options:
            # Get L2 cost multiplier
            multiplier = self.settings.L2_COST_MULTIPLIER.get(l2_chain, 0.1)
            l2_cost = l1_cost * multiplier
            
            # Get bridge info
            bridge_key = (request.chain, l2_chain)
            bridge = self._bridge_info.get(bridge_key, {})
            
            # Calculate bridge cost
            bridge_cost = 0
            if bridge:
                bridge_cost = gas_estimate * l1_gas_price * bridge.get('cost_multiplier', 1.5)
                
            # Total cost including bridge
            total_cost = l2_cost + bridge_cost
            savings = l1_cost - total_cost
            
            if savings > best_savings:
                best_l2 = l2_chain
                best_savings = savings
                
                best_suggestion = L2Suggestion(
                    current_chain=request.chain,
                    suggested_chain=l2_chain,
                    l1_cost=str(l1_cost),
                    l2_cost=str(int(l2_cost)),
                    bridge_cost=str(int(bridge_cost)),
                    total_savings=str(int(savings)),
                    bridge_available=bool(bridge),
                    bridge_contract=bridge.get('contract'),
                    bridge_time=bridge.get('time'),
                    security_score=0.9,  # TODO: Real security scoring
                    liquidity_available=True,  # TODO: Check liquidity
                    compatibility_issues=[]
                )
                
        if not best_l2 or best_savings / l1_cost < 0.3:  # At least 30% savings
            return None
            
        return GasRecommendation(
            strategy=OptimizationStrategy.L2_MIGRATION,
            gas_price=str(int(l1_gas_price * self.settings.L2_COST_MULTIPLIER.get(best_l2, 0.1))),
            estimated_cost=str(int(l1_cost * self.settings.L2_COST_MULTIPLIER.get(best_l2, 0.1))),
            estimated_savings=str(int(best_savings)),
            savings_percentage=best_savings / l1_cost,
            expected_confirmation_time=15,  # L2 confirmation time
            confidence_score=0.85,
            reasoning=f"Migrate to {best_l2} for {best_savings/l1_cost:.1%} savings",
            alternatives=[best_suggestion.dict()] if best_suggestion else []
        ) 