"""
Dynamic Fee Manager for AMM

Adjusts trading fees based on market conditions, volatility, and liquidity depth
"""

from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from collections import deque
import math
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class FeeAdjustmentReason(Enum):
    """Reasons for fee adjustment"""
    HIGH_VOLATILITY = "high_volatility"
    LOW_VOLATILITY = "low_volatility"
    HIGH_VOLUME = "high_volume"
    LOW_VOLUME = "low_volume"
    LOW_LIQUIDITY = "low_liquidity"
    HIGH_LIQUIDITY = "high_liquidity"
    IMBALANCE = "imbalance"
    MANUAL = "manual"


class DynamicFeeManager:
    """
    Manages dynamic fee adjustments for AMM pools
    
    Features:
    - Volatility-responsive fees
    - Volume-based fee tiers
    - Liquidity depth consideration
    - Pool imbalance penalties
    - Time-weighted adjustments
    - Fee smoothing to prevent manipulation
    """
    
    def __init__(
        self,
        base_fee: Decimal = Decimal("0.003"),  # 0.3% base
        min_fee: Decimal = Decimal("0.0001"),   # 0.01% minimum
        max_fee: Decimal = Decimal("0.01"),     # 1% maximum
        update_interval: int = 300              # 5 minutes
    ):
        self.base_fee = base_fee
        self.min_fee = min_fee
        self.max_fee = max_fee
        self.update_interval = update_interval
        
        # Current fee state
        self.current_fee = base_fee
        self.last_update = datetime.utcnow()
        
        # Historical data for calculations
        self.price_history = deque(maxlen=100)  # Last 100 price points
        self.volume_history = deque(maxlen=24)  # Last 24 hours of volume
        self.fee_history = deque(maxlen=50)     # Last 50 fee updates
        
        # Volatility parameters
        self.volatility_window = 20  # Price points for volatility calc
        self.volatility_multiplier = Decimal("2")  # How much volatility affects fees
        
        # Volume parameters
        self.volume_tiers = [
            (Decimal("0"), Decimal("1")),        # 0% volume discount
            (Decimal("100000"), Decimal("0.9")),  # 10% discount > $100k
            (Decimal("1000000"), Decimal("0.8")), # 20% discount > $1M
            (Decimal("10000000"), Decimal("0.7")) # 30% discount > $10M
        ]
        
        # Liquidity parameters
        self.liquidity_threshold = Decimal("50000")  # $50k minimum healthy liquidity
        self.liquidity_multiplier = Decimal("1.5")  # Fee increase for low liquidity
        
        # Imbalance parameters
        self.imbalance_threshold = Decimal("0.2")  # 20% imbalance triggers fee
        self.imbalance_multiplier = Decimal("1.3") # 30% fee increase for imbalance
        
        # Smoothing parameters
        self.smoothing_factor = Decimal("0.3")  # EMA smoothing
        self.max_change_per_update = Decimal("0.0005")  # Max 0.05% change per update
        
        # Fee adjustment tracking
        self.adjustment_history = []
        
    def update_fee(
        self,
        current_price: Decimal,
        volume_24h: Decimal,
        total_liquidity: Decimal,
        pool_balance_ratio: Decimal  # base_value / total_value
    ) -> Tuple[Decimal, List[FeeAdjustmentReason]]:
        """
        Update fee based on current market conditions
        
        Returns: (new_fee, adjustment_reasons)
        """
        # Check if update is needed
        if not self._should_update():
            return self.current_fee, []
            
        # Record current price
        self.price_history.append((datetime.utcnow(), current_price))
        
        # Calculate components
        volatility_factor = self._calculate_volatility_factor()
        volume_factor = self._calculate_volume_factor(volume_24h)
        liquidity_factor = self._calculate_liquidity_factor(total_liquidity)
        imbalance_factor = self._calculate_imbalance_factor(pool_balance_ratio)
        
        # Combine factors
        combined_factor = (
            volatility_factor *
            volume_factor *
            liquidity_factor *
            imbalance_factor
        )
        
        # Calculate target fee
        target_fee = self.base_fee * combined_factor
        
        # Apply bounds
        target_fee = max(self.min_fee, min(self.max_fee, target_fee))
        
        # Apply smoothing
        new_fee = self._apply_smoothing(target_fee)
        
        # Apply max change limit
        new_fee = self._apply_change_limit(new_fee)
        
        # Determine adjustment reasons
        reasons = self._determine_adjustment_reasons(
            volatility_factor,
            volume_factor,
            liquidity_factor,
            imbalance_factor
        )
        
        # Update state
        self.current_fee = new_fee
        self.last_update = datetime.utcnow()
        self.fee_history.append((self.last_update, new_fee))
        
        # Record adjustment
        self.adjustment_history.append({
            "timestamp": self.last_update,
            "old_fee": self.current_fee,
            "new_fee": new_fee,
            "reasons": reasons,
            "factors": {
                "volatility": float(volatility_factor),
                "volume": float(volume_factor),
                "liquidity": float(liquidity_factor),
                "imbalance": float(imbalance_factor)
            }
        })
        
        logger.info(f"Fee updated: {self.current_fee} -> {new_fee}, reasons: {reasons}")
        
        return new_fee, reasons
        
    def _should_update(self) -> bool:
        """Check if fee should be updated"""
        time_since_update = (datetime.utcnow() - self.last_update).total_seconds()
        return time_since_update >= self.update_interval
        
    def _calculate_volatility_factor(self) -> Decimal:
        """
        Calculate fee multiplier based on price volatility
        Higher volatility = higher fees
        """
        if len(self.price_history) < self.volatility_window:
            return Decimal("1")
            
        # Get recent prices
        recent_prices = [
            price for _, price in 
            list(self.price_history)[-self.volatility_window:]
        ]
        
        # Calculate returns
        returns = []
        for i in range(1, len(recent_prices)):
            ret = (recent_prices[i] - recent_prices[i-1]) / recent_prices[i-1]
            returns.append(float(ret))
            
        if not returns:
            return Decimal("1")
            
        # Calculate volatility (standard deviation of returns)
        mean_return = sum(returns) / len(returns)
        variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
        volatility = math.sqrt(variance)
        
        # Convert to annualized volatility (assuming 5-minute intervals)
        periods_per_year = 365 * 24 * 12  # 5-minute periods in a year
        annualized_vol = volatility * math.sqrt(periods_per_year)
        
        # Map volatility to fee factor
        # 0% vol = 0.5x fees, 50% vol = 1x fees, 100% vol = 2x fees
        if annualized_vol <= 0.5:  # 50% annualized
            factor = Decimal("0.5") + Decimal(str(annualized_vol))
        else:
            factor = Decimal("1") + (Decimal(str(annualized_vol - 0.5)) * self.volatility_multiplier)
            
        return factor
        
    def _calculate_volume_factor(self, volume_24h: Decimal) -> Decimal:
        """
        Calculate fee discount based on volume
        Higher volume = lower fees
        """
        # Find applicable tier
        discount_factor = Decimal("1")
        for volume_threshold, factor in self.volume_tiers:
            if volume_24h >= volume_threshold:
                discount_factor = factor
                
        return discount_factor
        
    def _calculate_liquidity_factor(self, total_liquidity: Decimal) -> Decimal:
        """
        Calculate fee adjustment based on liquidity depth
        Lower liquidity = higher fees
        """
        if total_liquidity >= self.liquidity_threshold:
            return Decimal("1")
            
        # Linear increase as liquidity decreases
        liquidity_ratio = total_liquidity / self.liquidity_threshold
        
        # Factor ranges from 1.0 to liquidity_multiplier
        factor = Decimal("1") + (
            (Decimal("1") - liquidity_ratio) * 
            (self.liquidity_multiplier - Decimal("1"))
        )
        
        return factor
        
    def _calculate_imbalance_factor(self, pool_balance_ratio: Decimal) -> Decimal:
        """
        Calculate fee penalty for pool imbalance
        More imbalance = higher fees
        """
        # Perfect balance is 0.5 (50/50)
        imbalance = abs(pool_balance_ratio - Decimal("0.5"))
        
        if imbalance <= self.imbalance_threshold:
            return Decimal("1")
            
        # Linear increase with imbalance
        excess_imbalance = imbalance - self.imbalance_threshold
        max_imbalance = Decimal("0.5") - self.imbalance_threshold
        
        # Factor ranges from 1.0 to imbalance_multiplier
        factor = Decimal("1") + (
            (excess_imbalance / max_imbalance) *
            (self.imbalance_multiplier - Decimal("1"))
        )
        
        return min(factor, self.imbalance_multiplier)
        
    def _apply_smoothing(self, target_fee: Decimal) -> Decimal:
        """Apply exponential moving average smoothing"""
        if not self.fee_history:
            return target_fee
            
        # EMA: new = α * target + (1 - α) * current
        smoothed = (
            self.smoothing_factor * target_fee +
            (Decimal("1") - self.smoothing_factor) * self.current_fee
        )
        
        return smoothed
        
    def _apply_change_limit(self, new_fee: Decimal) -> Decimal:
        """Limit maximum fee change per update"""
        max_increase = self.current_fee + self.max_change_per_update
        max_decrease = self.current_fee - self.max_change_per_update
        
        return max(max_decrease, min(max_increase, new_fee))
        
    def _determine_adjustment_reasons(
        self,
        volatility_factor: Decimal,
        volume_factor: Decimal,
        liquidity_factor: Decimal,
        imbalance_factor: Decimal
    ) -> List[FeeAdjustmentReason]:
        """Determine reasons for fee adjustment"""
        reasons = []
        
        # Volatility reasons
        if volatility_factor > Decimal("1.2"):
            reasons.append(FeeAdjustmentReason.HIGH_VOLATILITY)
        elif volatility_factor < Decimal("0.8"):
            reasons.append(FeeAdjustmentReason.LOW_VOLATILITY)
            
        # Volume reasons
        if volume_factor < Decimal("0.9"):
            reasons.append(FeeAdjustmentReason.HIGH_VOLUME)
        elif volume_factor == Decimal("1"):
            reasons.append(FeeAdjustmentReason.LOW_VOLUME)
            
        # Liquidity reasons
        if liquidity_factor > Decimal("1.1"):
            reasons.append(FeeAdjustmentReason.LOW_LIQUIDITY)
        elif liquidity_factor < Decimal("0.95"):
            reasons.append(FeeAdjustmentReason.HIGH_LIQUIDITY)
            
        # Imbalance reasons
        if imbalance_factor > Decimal("1.1"):
            reasons.append(FeeAdjustmentReason.IMBALANCE)
            
        return reasons
        
    def get_fee_for_size(
        self,
        trade_size: Decimal,
        is_buy: bool,
        current_pool_state: Dict[str, Decimal]
    ) -> Decimal:
        """
        Get dynamic fee for a specific trade size
        Larger trades may have different fees due to price impact
        """
        base_fee = self.current_fee
        
        # Calculate price impact
        price_impact = self._estimate_price_impact(
            trade_size,
            is_buy,
            current_pool_state
        )
        
        # Higher price impact = higher fee
        if price_impact > Decimal("0.01"):  # > 1% impact
            impact_multiplier = Decimal("1") + price_impact
            return min(base_fee * impact_multiplier, self.max_fee)
            
        return base_fee
        
    def _estimate_price_impact(
        self,
        trade_size: Decimal,
        is_buy: bool,
        pool_state: Dict[str, Decimal]
    ) -> Decimal:
        """Estimate price impact of a trade"""
        # Simple constant product formula estimation
        if is_buy:
            reserve_in = pool_state.get("quote_reserve", Decimal("1"))
            reserve_out = pool_state.get("base_reserve", Decimal("1"))
        else:
            reserve_in = pool_state.get("base_reserve", Decimal("1"))
            reserve_out = pool_state.get("quote_reserve", Decimal("1"))
            
        # Price before
        price_before = reserve_out / reserve_in
        
        # Price after (simplified)
        new_reserve_in = reserve_in + trade_size
        new_reserve_out = reserve_out - (trade_size * reserve_out) / (reserve_in + trade_size)
        price_after = new_reserve_out / new_reserve_in
        
        # Calculate impact
        impact = abs(price_after - price_before) / price_before
        
        return impact
        
    def set_manual_fee(
        self,
        new_fee: Decimal,
        reason: str = "Manual adjustment"
    ) -> bool:
        """Manually set fee (for governance or emergency)"""
        if new_fee < self.min_fee or new_fee > self.max_fee:
            return False
            
        self.current_fee = new_fee
        self.last_update = datetime.utcnow()
        self.fee_history.append((self.last_update, new_fee))
        
        self.adjustment_history.append({
            "timestamp": self.last_update,
            "old_fee": self.current_fee,
            "new_fee": new_fee,
            "reasons": [FeeAdjustmentReason.MANUAL],
            "manual_reason": reason
        })
        
        return True
        
    def get_fee_stats(self) -> Dict[str, Any]:
        """Get fee statistics"""
        if not self.fee_history:
            return {
                "current_fee": str(self.current_fee),
                "average_fee": str(self.base_fee),
                "fee_changes_24h": 0
            }
            
        # Calculate stats
        fees = [fee for _, fee in self.fee_history]
        
        # Changes in last 24h
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        recent_changes = [
            adj for adj in self.adjustment_history
            if adj["timestamp"] > cutoff_time
        ]
        
        return {
            "current_fee": str(self.current_fee),
            "base_fee": str(self.base_fee),
            "min_fee": str(self.min_fee),
            "max_fee": str(self.max_fee),
            "average_fee": str(sum(fees) / len(fees)),
            "fee_changes_24h": len(recent_changes),
            "last_update": self.last_update.isoformat(),
            "update_interval": self.update_interval
        }
        
    def get_adjustment_history(
        self,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get recent fee adjustment history"""
        return self.adjustment_history[-limit:] 