"""Concentrated liquidity AMM implementation."""

import math
from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

from app.config import Settings
from app.models.amm import (
    LiquidityPool, TickData, LiquidityPosition,
    SwapDirection, SwapResult, PoolType
)
from platformq_trading_common import publish_event, EventType


# Set high precision for calculations
getcontext().prec = 28

logger = logging.getLogger(__name__)


class ConcentratedLiquidityAMM:
    """
    Concentrated liquidity AMM implementation (similar to Uniswap V3).
    
    Features:
    - Capital efficient liquidity provision
    - Custom price ranges for LPs
    - Multiple fee tiers
    - Price tick system
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.tick_spacing = settings.tick_spacing
        self.max_tick = settings.max_tick
        self.min_tick = settings.min_tick
        
        # Constants for calculations
        self.Q96 = 2 ** 96  # Fixed point precision
        self.MIN_SQRT_RATIO = 4295128739  # sqrt(1.0001^min_tick) * 2^96
        self.MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342  # sqrt(1.0001^max_tick) * 2^96
        
    def create_pool(
        self,
        pool_id: str,
        base_asset: str,
        quote_asset: str,
        initial_price: Decimal,
        fee_bps: int = 30
    ) -> LiquidityPool:
        """Create a new concentrated liquidity pool."""
        # Calculate initial tick from price
        initial_tick = self._price_to_tick(initial_price)
        initial_sqrt_price = self._tick_to_sqrt_price_x96(initial_tick)
        
        pool = LiquidityPool(
            pool_id=pool_id,
            pool_type=PoolType.CONCENTRATED,
            base_asset=base_asset,
            quote_asset=quote_asset,
            base_fee_bps=fee_bps,
            tick_spacing=self.tick_spacing,
            current_price=initial_price,
            current_tick=initial_tick
        )
        
        # Store sqrt price in virtual reserves (hack for now)
        pool.virtual_base_reserve = Decimal(str(initial_sqrt_price))
        
        return pool
        
    async def add_liquidity(
        self,
        pool: LiquidityPool,
        position: LiquidityPosition,
        tick_lower: int,
        tick_upper: int,
        liquidity_amount: Decimal
    ) -> Tuple[Decimal, Decimal]:
        """
        Add liquidity to specified price range.
        
        Returns: (base_amount, quote_amount) deposited
        """
        # Validate ticks
        if tick_lower >= tick_upper:
            raise ValueError("tick_lower must be less than tick_upper")
            
        if tick_lower < self.min_tick or tick_upper > self.max_tick:
            raise ValueError("Ticks out of range")
            
        # Ensure ticks are on spacing grid
        if tick_lower % self.tick_spacing != 0 or tick_upper % self.tick_spacing != 0:
            raise ValueError(f"Ticks must be multiples of {self.tick_spacing}")
            
        # Calculate amounts needed
        current_tick = pool.current_tick
        sqrt_price = self._tick_to_sqrt_price_x96(current_tick)
        sqrt_lower = self._tick_to_sqrt_price_x96(tick_lower)
        sqrt_upper = self._tick_to_sqrt_price_x96(tick_upper)
        
        if current_tick < tick_lower:
            # Current price below range, only need quote tokens
            base_amount = Decimal("0")
            quote_amount = self._get_amount1_for_liquidity(
                sqrt_lower, sqrt_upper, liquidity_amount
            )
        elif current_tick >= tick_upper:
            # Current price above range, only need base tokens
            base_amount = self._get_amount0_for_liquidity(
                sqrt_lower, sqrt_upper, liquidity_amount
            )
            quote_amount = Decimal("0")
        else:
            # Current price in range, need both tokens
            base_amount = self._get_amount0_for_liquidity(
                sqrt_price, sqrt_upper, liquidity_amount
            )
            quote_amount = self._get_amount1_for_liquidity(
                sqrt_lower, sqrt_price, liquidity_amount
            )
            
        # Update position
        position.tick_lower = tick_lower
        position.tick_upper = tick_upper
        position.liquidity = liquidity_amount
        position.base_amount = base_amount
        position.quote_amount = quote_amount
        
        # Update pool liquidity
        pool.total_liquidity += liquidity_amount
        pool.base_reserve += base_amount
        pool.quote_reserve += quote_amount
        
        # Publish event
        await publish_event(
            EventType.LIQUIDITY_ADDED,
            {
                "pool_id": pool.pool_id,
                "position_id": position.position_id,
                "provider": position.provider,
                "tick_lower": tick_lower,
                "tick_upper": tick_upper,
                "liquidity": str(liquidity_amount),
                "base_amount": str(base_amount),
                "quote_amount": str(quote_amount),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return base_amount, quote_amount
        
    async def remove_liquidity(
        self,
        pool: LiquidityPool,
        position: LiquidityPosition,
        liquidity_amount: Decimal
    ) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
        """
        Remove liquidity from position.
        
        Returns: (base_amount, quote_amount, fees_base, fees_quote)
        """
        if liquidity_amount > position.liquidity:
            raise ValueError("Cannot remove more liquidity than position has")
            
        # Calculate amounts to return
        proportion = liquidity_amount / position.liquidity
        base_amount = position.base_amount * proportion
        quote_amount = position.quote_amount * proportion
        
        # Calculate fees
        fees_base = position.uncollected_fees_base * proportion
        fees_quote = position.uncollected_fees_quote * proportion
        
        # Update position
        position.liquidity -= liquidity_amount
        position.base_amount -= base_amount
        position.quote_amount -= quote_amount
        position.uncollected_fees_base -= fees_base
        position.uncollected_fees_quote -= fees_quote
        position.total_fees_collected_base += fees_base
        position.total_fees_collected_quote += fees_quote
        
        # Update pool
        pool.total_liquidity -= liquidity_amount
        pool.base_reserve -= base_amount
        pool.quote_reserve -= quote_amount
        
        # Publish event
        await publish_event(
            EventType.LIQUIDITY_REMOVED,
            {
                "pool_id": pool.pool_id,
                "position_id": position.position_id,
                "liquidity": str(liquidity_amount),
                "base_amount": str(base_amount),
                "quote_amount": str(quote_amount),
                "fees_base": str(fees_base),
                "fees_quote": str(fees_quote),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return base_amount, quote_amount, fees_base, fees_quote
        
    async def swap(
        self,
        pool: LiquidityPool,
        direction: SwapDirection,
        amount_in: Decimal,
        min_amount_out: Optional[Decimal] = None
    ) -> SwapResult:
        """Execute a swap through concentrated liquidity."""
        # Get current state
        sqrt_price = Decimal(str(pool.virtual_base_reserve))  # Stored sqrt price
        liquidity = pool.total_liquidity
        
        # Calculate swap
        if direction == SwapDirection.BASE_TO_QUOTE:
            # Selling base for quote
            sqrt_price_new = self._get_next_sqrt_price_from_amount0(
                sqrt_price, liquidity, amount_in, True
            )
            amount_out = self._get_amount1_delta(
                sqrt_price, sqrt_price_new, liquidity, False
            )
        else:
            # Selling quote for base
            sqrt_price_new = self._get_next_sqrt_price_from_amount1(
                sqrt_price, liquidity, amount_in, True
            )
            amount_out = self._get_amount0_delta(
                sqrt_price, sqrt_price_new, liquidity, False
            )
            
        # Apply fee
        fee_amount = amount_in * Decimal(pool.base_fee_bps) / Decimal("10000")
        amount_in_after_fee = amount_in - fee_amount
        
        # Recalculate with fee
        if direction == SwapDirection.BASE_TO_QUOTE:
            sqrt_price_new = self._get_next_sqrt_price_from_amount0(
                sqrt_price, liquidity, amount_in_after_fee, True
            )
            amount_out = self._get_amount1_delta(
                sqrt_price, sqrt_price_new, liquidity, False
            )
        else:
            sqrt_price_new = self._get_next_sqrt_price_from_amount1(
                sqrt_price, liquidity, amount_in_after_fee, True
            )
            amount_out = self._get_amount0_delta(
                sqrt_price, sqrt_price_new, liquidity, False
            )
            
        # Check slippage
        if min_amount_out and amount_out < min_amount_out:
            raise ValueError(f"Insufficient output amount: {amount_out} < {min_amount_out}")
            
        # Calculate price impact
        old_price = self._sqrt_price_x96_to_price(sqrt_price)
        new_price = self._sqrt_price_x96_to_price(sqrt_price_new)
        price_impact = abs(new_price - old_price) / old_price
        
        # Update pool state
        pool.virtual_base_reserve = sqrt_price_new
        pool.current_price = new_price
        pool.current_tick = self._sqrt_price_x96_to_tick(sqrt_price_new)
        
        if direction == SwapDirection.BASE_TO_QUOTE:
            pool.base_reserve += amount_in_after_fee
            pool.quote_reserve -= amount_out
        else:
            pool.quote_reserve += amount_in_after_fee
            pool.base_reserve -= amount_out
            
        # Update metrics
        pool.volume_24h += amount_in
        pool.fees_collected_24h += fee_amount
        pool.trades_24h += 1
        
        return SwapResult(
            swap_id=f"swap_{datetime.utcnow().timestamp()}",
            pool_id=pool.pool_id,
            trader="",  # Will be set by caller
            direction=direction,
            amount_in=amount_in,
            amount_out=amount_out,
            fee_paid=fee_amount,
            execution_price=amount_out / amount_in_after_fee,
            price_impact=price_impact,
            slippage=price_impact,  # Simplified
            new_base_reserve=pool.base_reserve,
            new_quote_reserve=pool.quote_reserve,
            new_price=new_price
        )
        
    # Price/tick conversion helpers
    
    def _price_to_tick(self, price: Decimal) -> int:
        """Convert price to nearest tick."""
        return int(math.log(float(price)) / math.log(1.0001))
        
    def _tick_to_sqrt_price_x96(self, tick: int) -> int:
        """Convert tick to sqrt price in Q96 format."""
        ratio = 1.0001 ** (tick / 2)
        return int(ratio * self.Q96)
        
    def _sqrt_price_x96_to_price(self, sqrt_price_x96: Decimal) -> Decimal:
        """Convert sqrt price to actual price."""
        return (sqrt_price_x96 / Decimal(self.Q96)) ** 2
        
    def _sqrt_price_x96_to_tick(self, sqrt_price_x96: Decimal) -> int:
        """Convert sqrt price to tick."""
        price = self._sqrt_price_x96_to_price(sqrt_price_x96)
        return self._price_to_tick(price)
        
    # Liquidity math helpers
    
    def _get_amount0_for_liquidity(
        self,
        sqrt_ratio_a: Decimal,
        sqrt_ratio_b: Decimal,
        liquidity: Decimal
    ) -> Decimal:
        """Calculate base token amount for given liquidity."""
        if sqrt_ratio_a > sqrt_ratio_b:
            sqrt_ratio_a, sqrt_ratio_b = sqrt_ratio_b, sqrt_ratio_a
            
        return liquidity * (sqrt_ratio_b - sqrt_ratio_a) / (sqrt_ratio_a * sqrt_ratio_b) * Decimal(self.Q96)
        
    def _get_amount1_for_liquidity(
        self,
        sqrt_ratio_a: Decimal,
        sqrt_ratio_b: Decimal,
        liquidity: Decimal
    ) -> Decimal:
        """Calculate quote token amount for given liquidity."""
        if sqrt_ratio_a > sqrt_ratio_b:
            sqrt_ratio_a, sqrt_ratio_b = sqrt_ratio_b, sqrt_ratio_a
            
        return liquidity * (sqrt_ratio_b - sqrt_ratio_a) / Decimal(self.Q96)
        
    def _get_amount0_delta(
        self,
        sqrt_ratio_a: Decimal,
        sqrt_ratio_b: Decimal,
        liquidity: Decimal,
        round_up: bool
    ) -> Decimal:
        """Calculate base token delta for swap."""
        if sqrt_ratio_a > sqrt_ratio_b:
            sqrt_ratio_a, sqrt_ratio_b = sqrt_ratio_b, sqrt_ratio_a
            
        numerator1 = liquidity * Decimal(self.Q96)
        numerator2 = sqrt_ratio_b - sqrt_ratio_a
        
        return numerator1 * numerator2 / (sqrt_ratio_b * sqrt_ratio_a)
        
    def _get_amount1_delta(
        self,
        sqrt_ratio_a: Decimal,
        sqrt_ratio_b: Decimal,
        liquidity: Decimal,
        round_up: bool
    ) -> Decimal:
        """Calculate quote token delta for swap."""
        if sqrt_ratio_a > sqrt_ratio_b:
            sqrt_ratio_a, sqrt_ratio_b = sqrt_ratio_b, sqrt_ratio_a
            
        return liquidity * (sqrt_ratio_b - sqrt_ratio_a) / Decimal(self.Q96)
        
    def _get_next_sqrt_price_from_amount0(
        self,
        sqrt_price: Decimal,
        liquidity: Decimal,
        amount: Decimal,
        add: bool
    ) -> Decimal:
        """Calculate new sqrt price after base token swap."""
        if amount == 0:
            return sqrt_price
            
        numerator = liquidity * sqrt_price * Decimal(self.Q96)
        
        if add:
            denominator = liquidity * Decimal(self.Q96) + amount * sqrt_price
        else:
            denominator = liquidity * Decimal(self.Q96) - amount * sqrt_price
            
        return numerator / denominator
        
    def _get_next_sqrt_price_from_amount1(
        self,
        sqrt_price: Decimal,
        liquidity: Decimal,
        amount: Decimal,
        add: bool
    ) -> Decimal:
        """Calculate new sqrt price after quote token swap."""
        if add:
            return sqrt_price + (amount * Decimal(self.Q96) / liquidity)
        else:
            return sqrt_price - (amount * Decimal(self.Q96) / liquidity) 