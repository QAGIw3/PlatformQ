"""Dynamic fee management for AMM pools."""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from collections import deque
import logging
import math

from app.config import Settings
from app.models.amm import FeeUpdate, FeeType
from platformq_trading_common import publish_event, EventType


logger = logging.getLogger(__name__)


class DynamicFeeManager:
    """
    Manages dynamic fee adjustments for AMM pools based on:
    - Market volatility
    - Trading volume
    - Pool liquidity depth
    - Pool imbalance
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        
        # Fee bounds
        self.min_fee_bps = settings.min_fee_bps
        self.max_fee_bps = settings.max_fee_bps
        self.base_fee_bps = settings.base_fee_bps
        
        # Update parameters
        self.update_interval = settings.fee_update_interval
        self.smoothing_factor = Decimal("0.3")  # EMA smoothing
        self.max_change_per_update = 10  # Max 10 bps change per update
        
        # Historical data storage
        self.price_history: Dict[str, deque] = {}  # pool_id -> deque of (timestamp, price)
        self.volume_history: Dict[str, deque] = {}  # pool_id -> deque of (timestamp, volume)
        self.fee_history: Dict[str, deque] = {}     # pool_id -> deque of (timestamp, fee_bps)
        
        # Current fees
        self.current_fees: Dict[str, int] = {}  # pool_id -> fee_bps
        self.last_update: Dict[str, datetime] = {}
        
    async def initialize_pool(self, pool_id: str, base_fee_bps: Optional[int] = None):
        """Initialize fee tracking for a new pool."""
        self.current_fees[pool_id] = base_fee_bps or self.base_fee_bps
        self.last_update[pool_id] = datetime.utcnow()
        
        # Initialize history deques
        self.price_history[pool_id] = deque(maxlen=100)
        self.volume_history[pool_id] = deque(maxlen=24)  # 24 hours
        self.fee_history[pool_id] = deque(maxlen=50)
        
        logger.info(f"Initialized fee manager for pool {pool_id} with base fee {self.current_fees[pool_id]} bps")
        
    def should_update_fee(self, pool_id: str) -> bool:
        """Check if fee should be updated for a pool."""
        if pool_id not in self.last_update:
            return False
            
        time_since_update = datetime.utcnow() - self.last_update[pool_id]
        return time_since_update.total_seconds() >= self.update_interval
        
    async def update_fee(
        self,
        pool_id: str,
        current_price: Decimal,
        volume_24h: Decimal,
        total_liquidity: Decimal,
        imbalance_ratio: Decimal
    ) -> Optional[FeeUpdate]:
        """
        Update fee based on current market conditions.
        
        Args:
            pool_id: Pool identifier
            current_price: Current pool price
            volume_24h: 24-hour trading volume
            total_liquidity: Total liquidity in pool
            imbalance_ratio: Ratio of base value to total value (0.5 = balanced)
        """
        if not self.should_update_fee(pool_id):
            return None
            
        # Record current data
        now = datetime.utcnow()
        self.price_history[pool_id].append((now, current_price))
        self.volume_history[pool_id].append((now, volume_24h))
        
        # Calculate adjustment factors
        volatility_factor = self._calculate_volatility_factor(pool_id)
        volume_factor = self._calculate_volume_factor(volume_24h)
        liquidity_factor = self._calculate_liquidity_factor(total_liquidity)
        imbalance_factor = self._calculate_imbalance_factor(imbalance_ratio)
        
        # Combine factors (multiplicative)
        combined_factor = (
            volatility_factor *
            volume_factor *
            liquidity_factor *
            imbalance_factor
        )
        
        # Calculate target fee
        current_fee = self.current_fees[pool_id]
        target_fee = int(self.base_fee_bps * combined_factor)
        
        # Apply bounds
        target_fee = max(self.min_fee_bps, min(self.max_fee_bps, target_fee))
        
        # Apply smoothing
        smoothed_fee = int(
            current_fee * (1 - self.smoothing_factor) +
            target_fee * self.smoothing_factor
        )
        
        # Apply maximum change limit
        fee_change = smoothed_fee - current_fee
        if abs(fee_change) > self.max_change_per_update:
            fee_change = self.max_change_per_update if fee_change > 0 else -self.max_change_per_update
            
        new_fee = current_fee + fee_change
        
        # Ensure within bounds
        new_fee = max(self.min_fee_bps, min(self.max_fee_bps, new_fee))
        
        if new_fee == current_fee:
            return None
            
        # Update state
        self.current_fees[pool_id] = new_fee
        self.last_update[pool_id] = now
        self.fee_history[pool_id].append((now, new_fee))
        
        # Create update record
        fee_update = FeeUpdate(
            pool_id=pool_id,
            old_fee_bps=current_fee,
            new_fee_bps=new_fee,
            volatility_factor=volatility_factor,
            volume_factor=volume_factor,
            liquidity_factor=liquidity_factor,
            imbalance_factor=imbalance_factor,
            reasons=self._get_update_reasons(
                volatility_factor,
                volume_factor,
                liquidity_factor,
                imbalance_factor
            )
        )
        
        # Publish event
        await publish_event(
            EventType.FEE_UPDATED,
            {
                "pool_id": pool_id,
                "old_fee_bps": current_fee,
                "new_fee_bps": new_fee,
                "factors": {
                    "volatility": str(volatility_factor),
                    "volume": str(volume_factor),
                    "liquidity": str(liquidity_factor),
                    "imbalance": str(imbalance_factor)
                },
                "timestamp": now.isoformat()
            }
        )
        
        logger.info(
            f"Updated fee for pool {pool_id}: {current_fee} -> {new_fee} bps "
            f"(vol: {volatility_factor:.2f}, volume: {volume_factor:.2f}, "
            f"liq: {liquidity_factor:.2f}, imb: {imbalance_factor:.2f})"
        )
        
        return fee_update
        
    def _calculate_volatility_factor(self, pool_id: str) -> Decimal:
        """
        Calculate fee adjustment factor based on price volatility.
        Higher volatility = higher fees.
        """
        price_history = self.price_history.get(pool_id, deque())
        if len(price_history) < 10:
            return Decimal("1.0")
            
        # Calculate returns
        prices = [price for _, price in price_history]
        returns = []
        for i in range(1, len(prices)):
            ret = (prices[i] - prices[i-1]) / prices[i-1]
            returns.append(float(ret))
            
        if not returns:
            return Decimal("1.0")
            
        # Calculate volatility (standard deviation of returns)
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
        volatility = math.sqrt(variance)
        
        # Annualized volatility (assuming minute samples)
        annualized_vol = volatility * math.sqrt(525600)  # Minutes in a year
        
        # Map volatility to factor (0.5x to 2x)
        # 20% annual vol = 1x, 40% = 1.5x, 60% = 2x
        if annualized_vol < 0.2:
            factor = 0.5 + (annualized_vol / 0.2) * 0.5
        elif annualized_vol < 0.6:
            factor = 1.0 + (annualized_vol - 0.2) / 0.4
        else:
            factor = 2.0
            
        return Decimal(str(factor))
        
    def _calculate_volume_factor(self, volume_24h: Decimal) -> Decimal:
        """
        Calculate fee adjustment factor based on trading volume.
        Higher volume = lower fees (volume discount).
        """
        # Apply volume tiers
        discount = Decimal("0")
        for volume_threshold, discount_rate in self.settings.volume_fee_tiers:
            if volume_24h >= volume_threshold:
                discount = Decimal(str(discount_rate))
            else:
                break
                
        return Decimal("1") - discount
        
    def _calculate_liquidity_factor(self, total_liquidity: Decimal) -> Decimal:
        """
        Calculate fee adjustment factor based on liquidity depth.
        Lower liquidity = higher fees.
        """
        liquidity_threshold = Decimal("50000")  # $50k threshold
        
        if total_liquidity >= liquidity_threshold:
            # Adequate liquidity, no adjustment
            return Decimal("1.0")
        else:
            # Increase fees for low liquidity
            # At 50% of threshold = 1.25x, at 25% = 1.5x
            ratio = total_liquidity / liquidity_threshold
            factor = Decimal("1.0") + (Decimal("1") - ratio) * Decimal("0.5")
            return min(factor, Decimal("1.5"))
            
    def _calculate_imbalance_factor(self, imbalance_ratio: Decimal) -> Decimal:
        """
        Calculate fee adjustment factor based on pool imbalance.
        Imbalanced pool = higher fees.
        """
        # Perfect balance is 0.5
        imbalance = abs(imbalance_ratio - Decimal("0.5"))
        
        if imbalance < self.settings.imbalance_threshold:
            # Within acceptable range
            return Decimal("1.0")
        else:
            # Apply penalty for imbalance
            penalty_rate = (imbalance - self.settings.imbalance_threshold) / Decimal("0.3")
            factor = Decimal("1.0") + penalty_rate * (self.settings.imbalance_fee_multiplier - Decimal("1"))
            return min(factor, self.settings.imbalance_fee_multiplier)
            
    def _get_update_reasons(
        self,
        volatility_factor: Decimal,
        volume_factor: Decimal,
        liquidity_factor: Decimal,
        imbalance_factor: Decimal
    ) -> List[str]:
        """Generate human-readable reasons for fee update."""
        reasons = []
        
        if volatility_factor > Decimal("1.2"):
            reasons.append("High market volatility")
        elif volatility_factor < Decimal("0.8"):
            reasons.append("Low market volatility")
            
        if volume_factor < Decimal("0.9"):
            reasons.append("Volume discount applied")
            
        if liquidity_factor > Decimal("1.1"):
            reasons.append("Low liquidity depth")
            
        if imbalance_factor > Decimal("1.1"):
            reasons.append("Pool imbalance penalty")
            
        if not reasons:
            reasons.append("Routine fee adjustment")
            
        return reasons
        
    def get_current_fee(self, pool_id: str) -> Optional[int]:
        """Get current fee for a pool."""
        return self.current_fees.get(pool_id)
        
    async def get_fee_history(
        self,
        pool_id: str,
        hours: int = 24
    ) -> List[Tuple[datetime, int]]:
        """Get fee history for a pool."""
        history = self.fee_history.get(pool_id, deque())
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        
        return [
            (timestamp, fee)
            for timestamp, fee in history
            if timestamp >= cutoff
        ] 