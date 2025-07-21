"""
Time-Based Optimization Strategy
"""

import logging
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import numpy as np

from pyignite import AsyncClient as IgniteClient

from ..config import Settings
from ..models.optimization import (
    OptimizationRequest, GasRecommendation, OptimizationStrategy
)

logger = logging.getLogger(__name__)


class TimeBasedStrategy:
    """Handles time-based gas optimization"""
    
    def __init__(self, settings: Settings, ignite_client: IgniteClient):
        self.settings = settings
        self.ignite = ignite_client
        self._price_patterns: Dict[str, List[Tuple[int, float]]] = {}
        
    async def initialize(self):
        """Initialize time-based strategy"""
        logger.info("Initializing Time-Based Strategy")
        # Load historical patterns
        await self._load_price_patterns()
        
    async def shutdown(self):
        """Shutdown time-based strategy"""
        pass
        
    async def _load_price_patterns(self):
        """Load historical price patterns"""
        # Initialize with typical patterns
        # In production, this would analyze historical data
        typical_patterns = [
            (0, 0.8),   # 00:00 - Low
            (4, 0.7),   # 04:00 - Lowest
            (8, 1.2),   # 08:00 - Morning peak
            (12, 1.0),  # 12:00 - Normal
            (16, 1.3),  # 16:00 - Evening peak
            (20, 1.1),  # 20:00 - Elevated
            (23, 0.9),  # 23:00 - Declining
        ]
        
        # Apply to all chains
        for chain in ['ethereum', 'polygon', 'arbitrum', 'optimism']:
            self._price_patterns[chain] = typical_patterns
            
    async def evaluate(
        self,
        request: OptimizationRequest,
        gas_prices: Dict[str, Any]
    ) -> Optional[GasRecommendation]:
        """Evaluate time-based optimization"""
        if not request.max_wait_time:
            return None
            
        # Get price patterns for chain
        patterns = self._price_patterns.get(request.chain, [])
        if not patterns:
            return None
            
        # Current gas price
        current_price = int(gas_prices.get(request.urgency.value, gas_prices.get('standard')))
        current_hour = datetime.utcnow().hour
        
        # Find best time within wait window
        best_time = None
        best_multiplier = 1.0
        
        for hours_ahead in range(0, request.max_wait_time // 3600 + 1):
            target_hour = (current_hour + hours_ahead) % 24
            
            # Find multiplier for target hour
            multiplier = self._get_multiplier_for_hour(target_hour, patterns)
            
            if multiplier < best_multiplier:
                best_multiplier = multiplier
                best_time = datetime.utcnow() + timedelta(hours=hours_ahead)
                
        if best_multiplier >= 0.95:  # Less than 5% savings
            return None
            
        # Calculate savings
        gas_estimate = request.estimated_gas or 100000
        current_cost = gas_estimate * current_price
        future_cost = gas_estimate * int(current_price * best_multiplier)
        savings = current_cost - future_cost
        
        return GasRecommendation(
            strategy=OptimizationStrategy.TIME_BASED,
            gas_price=str(int(current_price * best_multiplier)),
            estimated_cost=str(future_cost),
            estimated_savings=str(savings),
            savings_percentage=savings / current_cost,
            recommended_time=best_time,
            expected_confirmation_time=180,  # Standard confirmation
            confidence_score=0.75,  # Historical patterns confidence
            reasoning=f"Wait until {best_time.strftime('%H:%M')} for {(1-best_multiplier)*100:.1f}% lower gas prices"
        )
        
    def _get_multiplier_for_hour(
        self,
        hour: int,
        patterns: List[Tuple[int, float]]
    ) -> float:
        """Get price multiplier for a specific hour"""
        # Find surrounding patterns
        before = None
        after = None
        
        for pattern_hour, multiplier in patterns:
            if pattern_hour <= hour:
                before = (pattern_hour, multiplier)
            if pattern_hour >= hour and after is None:
                after = (pattern_hour, multiplier)
                
        if not before:
            before = patterns[-1]  # Wrap around
        if not after:
            after = patterns[0]   # Wrap around
            
        if before == after:
            return before[1]
            
        # Linear interpolation
        hour_diff = after[0] - before[0]
        if hour_diff < 0:
            hour_diff += 24
            
        progress = (hour - before[0]) / hour_diff
        multiplier = before[1] + (after[1] - before[1]) * progress
        
        return multiplier 