"""
AMM Liquidity Pool Implementation

Automated Market Maker pool for derivatives trading with concentrated liquidity support
"""

from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import math
import logging

from app.models.liquidity_pool import LiquidityPool as BaseLiquidityPool, PoolConfig, PoolType
from app.models.order import Trade
from app.models.market import Market

logger = logging.getLogger(__name__)


class LiquidityPool:
    """
    AMM Liquidity Pool with advanced features
    
    Features:
    - Concentrated liquidity ranges
    - Dynamic fee adjustment
    - Impermanent loss tracking
    - Multi-asset support
    - Virtual liquidity amplification
    """
    
    def __init__(
        self,
        market: Market,
        config: Optional[PoolConfig] = None
    ):
        self.market = market
        self.config = config or PoolConfig(pool_type=PoolType.AMM)
        
        # Initialize base pool
        self.base_pool = BaseLiquidityPool(
            market_id=market.id,
            name=f"AMM Pool - {market.symbol}",
            config=self.config
        )
        
        # AMM-specific state
        self.price_scale = Decimal("1")
        self.virtual_reserves = {
            "base": Decimal("0"),
            "quote": Decimal("0")
        }
        
        # Concentrated liquidity state
        self.tick_spacing = self.config.tick_spacing or 60
        self.current_tick = 0
        self.liquidity_by_tick = {}  # tick -> liquidity
        
        # Price impact parameters
        self.max_price_impact = self.config.max_price_impact
        self.slippage_tolerance = self.config.max_slippage
        
        # Fee parameters
        self.base_fee = self.config.base_fee
        self.dynamic_fee_enabled = True
        
        # Amplification factor for stableswap
        self.amplification_factor = Decimal("100") if config.curve_type == "stableswap" else Decimal("1")
        
    def add_liquidity(
        self,
        provider: str,
        amount_base: Decimal,
        amount_quote: Decimal,
        tick_lower: Optional[int] = None,
        tick_upper: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Add liquidity to the pool
        
        For concentrated liquidity, specify tick range
        For full range, leave tick parameters as None
        """
        # Calculate current price and tick
        current_price = self._get_current_price()
        
        if tick_lower is None or tick_upper is None:
            # Full range liquidity
            tick_lower = self._get_min_tick()
            tick_upper = self._get_max_tick()
            
        # Validate tick range
        if not self._validate_tick_range(tick_lower, tick_upper):
            raise ValueError("Invalid tick range")
            
        # Calculate liquidity amount
        liquidity = self._calculate_liquidity_from_amounts(
            amount_base,
            amount_quote,
            current_price,
            tick_lower,
            tick_upper
        )
        
        # Add to concentrated liquidity positions
        position_id = f"{provider}_{tick_lower}_{tick_upper}_{datetime.utcnow().timestamp()}"
        
        # Update tick liquidity
        self._update_tick_liquidity(tick_lower, liquidity, True)
        self._update_tick_liquidity(tick_upper, liquidity, False)
        
        # Update virtual reserves
        self._update_virtual_reserves(amount_base, amount_quote, True)
        
        # Mint LP tokens through base pool
        lp_tokens = self.base_pool.add_liquidity(
            provider,
            {
                "base": amount_base,
                "quote": amount_quote
            }
        )
        
        return {
            "position_id": position_id,
            "lp_tokens": str(lp_tokens),
            "tick_lower": tick_lower,
            "tick_upper": tick_upper,
            "liquidity": str(liquidity),
            "amount_base": str(amount_base),
            "amount_quote": str(amount_quote)
        }
        
    def remove_liquidity(
        self,
        provider: str,
        position_id: str,
        liquidity_percentage: Decimal = Decimal("100")
    ) -> Dict[str, Decimal]:
        """Remove liquidity from a position"""
        # Parse position details
        parts = position_id.split("_")
        tick_lower = int(parts[1])
        tick_upper = int(parts[2])
        
        # Calculate amounts to return
        current_price = self._get_current_price()
        amounts = self._calculate_amounts_from_liquidity(
            self.base_pool.total_liquidity * (liquidity_percentage / 100),
            current_price,
            tick_lower,
            tick_upper
        )
        
        # Update tick liquidity
        liquidity_delta = self.base_pool.total_liquidity * (liquidity_percentage / 100)
        self._update_tick_liquidity(tick_lower, liquidity_delta, False)
        self._update_tick_liquidity(tick_upper, liquidity_delta, True)
        
        # Update virtual reserves
        self._update_virtual_reserves(
            amounts["base"],
            amounts["quote"],
            False
        )
        
        # Remove through base pool
        lp_tokens_to_burn = self.base_pool.liquidity_providers[provider]["lp_tokens"] * (liquidity_percentage / 100)
        returned_amounts = self.base_pool.remove_liquidity(provider, lp_tokens_to_burn)
        
        return returned_amounts
        
    def swap(
        self,
        amount_in: Decimal,
        token_in: str,  # "base" or "quote"
        min_amount_out: Optional[Decimal] = None,
        max_price_impact: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """
        Execute a swap through the AMM
        """
        # Check price impact
        price_impact = self._calculate_price_impact(amount_in, token_in)
        max_impact = max_price_impact or self.max_price_impact
        
        if price_impact > max_impact:
            raise ValueError(f"Price impact {price_impact} exceeds maximum {max_impact}")
            
        # Calculate output amount based on curve type
        if self.config.curve_type == "constant_product":
            amount_out = self._constant_product_swap(amount_in, token_in)
        elif self.config.curve_type == "stableswap":
            amount_out = self._stableswap_curve(amount_in, token_in)
        elif self.config.curve_type == "concentrated":
            amount_out = self._concentrated_liquidity_swap(amount_in, token_in)
        else:
            raise ValueError(f"Unknown curve type: {self.config.curve_type}")
            
        # Check slippage
        if min_amount_out and amount_out < min_amount_out:
            raise ValueError(f"Output {amount_out} below minimum {min_amount_out}")
            
        # Calculate fee
        fee = amount_in * self.base_fee
        amount_in_after_fee = amount_in - fee
        
        # Update reserves
        if token_in == "base":
            self.virtual_reserves["base"] += amount_in_after_fee
            self.virtual_reserves["quote"] -= amount_out
        else:
            self.virtual_reserves["quote"] += amount_in_after_fee
            self.virtual_reserves["base"] -= amount_out
            
        # Update price scale for next trade
        self._update_price_scale()
        
        # Record trade in base pool
        self.base_pool.record_trade(
            volume=amount_in,
            fee=fee,
            token=self.market.quote_asset if token_in == "quote" else self.market.underlying_asset
        )
        
        return {
            "amount_in": str(amount_in),
            "amount_out": str(amount_out),
            "fee": str(fee),
            "price_impact": str(price_impact),
            "effective_price": str(amount_out / amount_in_after_fee),
            "pool_price_after": str(self._get_current_price())
        }
        
    def _constant_product_swap(
        self,
        amount_in: Decimal,
        token_in: str
    ) -> Decimal:
        """Classic x*y=k swap calculation"""
        if token_in == "base":
            reserve_in = self.virtual_reserves["base"]
            reserve_out = self.virtual_reserves["quote"]
        else:
            reserve_in = self.virtual_reserves["quote"]
            reserve_out = self.virtual_reserves["base"]
            
        # Apply fee
        amount_in_with_fee = amount_in * (Decimal("1") - self.base_fee)
        
        # Calculate output: y = (x * Y) / (X + x)
        amount_out = (amount_in_with_fee * reserve_out) / (reserve_in + amount_in_with_fee)
        
        return amount_out
        
    def _stableswap_curve(
        self,
        amount_in: Decimal,
        token_in: str
    ) -> Decimal:
        """
        StableSwap curve for correlated assets
        Uses amplification factor to reduce slippage
        """
        A = self.amplification_factor
        
        # Get reserves
        if token_in == "base":
            x = self.virtual_reserves["base"] + amount_in
            y = self.virtual_reserves["quote"]
            solving_for_y = True
        else:
            x = self.virtual_reserves["base"]
            y = self.virtual_reserves["quote"] + amount_in
            solving_for_y = False
            
        # StableSwap invariant: An^n * sum(x_i) + D = An^n * D + D^(n+1) / (n^n * prod(x_i))
        # For n=2: 4A(x+y) + D = 4AD + D³/(4xy)
        
        # Calculate D (invariant)
        S = x + y
        P = x * y
        
        # Solve for D iteratively
        D = S
        for _ in range(10):
            D_prev = D
            numerator = D ** 3
            denominator = 4 * P
            D = (4 * A * S + numerator / denominator) / (4 * A + 2)
            if abs(D - D_prev) < Decimal("0.0001"):
                break
                
        # Calculate output amount
        if solving_for_y:
            # Solve for new y given new x
            c = D ** 3 / (4 * A * x)
            b = x + D / (4 * A)
            
            # Quadratic formula
            y_new = (-b + (b ** 2 + 4 * c).sqrt()) / 2
            amount_out = y - y_new
        else:
            # Solve for new x given new y
            c = D ** 3 / (4 * A * y)
            b = y + D / (4 * A)
            
            x_new = (-b + (b ** 2 + 4 * c).sqrt()) / 2
            amount_out = x - x_new
            
        # Apply fee
        amount_out = amount_out * (Decimal("1") - self.base_fee)
        
        return abs(amount_out)
        
    def _concentrated_liquidity_swap(
        self,
        amount_in: Decimal,
        token_in: str
    ) -> Decimal:
        """
        Concentrated liquidity swap calculation
        Traverse through active tick ranges
        """
        amount_remaining = amount_in
        amount_out = Decimal("0")
        current_tick = self.current_tick
        
        is_base_to_quote = token_in == "base"
        
        while amount_remaining > 0:
            # Get liquidity at current tick
            liquidity = self._get_liquidity_at_tick(current_tick)
            
            if liquidity == 0:
                # No liquidity, move to next tick
                current_tick = self._get_next_initialized_tick(
                    current_tick,
                    is_base_to_quote
                )
                if current_tick is None:
                    break
                continue
                
            # Calculate swap within current tick range
            sqrt_price_current = self._tick_to_sqrt_price(current_tick)
            sqrt_price_next = self._tick_to_sqrt_price(
                self._get_next_initialized_tick(current_tick, is_base_to_quote)
            )
            
            # Calculate maximum swap amount in this range
            if is_base_to_quote:
                max_amount = self._calculate_amount_base(
                    liquidity,
                    sqrt_price_current,
                    sqrt_price_next
                )
            else:
                max_amount = self._calculate_amount_quote(
                    liquidity,
                    sqrt_price_current,
                    sqrt_price_next
                )
                
            # Swap within range
            if amount_remaining <= max_amount:
                # Complete swap within current range
                if is_base_to_quote:
                    amount_out += self._calculate_amount_quote_from_base(
                        amount_remaining,
                        liquidity,
                        sqrt_price_current
                    )
                else:
                    amount_out += self._calculate_amount_base_from_quote(
                        amount_remaining,
                        liquidity,
                        sqrt_price_current
                    )
                amount_remaining = Decimal("0")
            else:
                # Partial swap, move to next tick
                if is_base_to_quote:
                    amount_out += self._calculate_amount_quote(
                        liquidity,
                        sqrt_price_current,
                        sqrt_price_next
                    )
                else:
                    amount_out += self._calculate_amount_base(
                        liquidity,
                        sqrt_price_current,
                        sqrt_price_next
                    )
                amount_remaining -= max_amount
                current_tick = self._get_next_initialized_tick(
                    current_tick,
                    is_base_to_quote
                )
                
        # Apply fee
        amount_out = amount_out * (Decimal("1") - self.base_fee)
        
        # Update current tick
        self.current_tick = current_tick
        
        return amount_out
        
    def _calculate_price_impact(
        self,
        amount_in: Decimal,
        token_in: str
    ) -> Decimal:
        """Calculate price impact of a trade"""
        # Get current price
        price_before = self._get_current_price()
        
        # Simulate swap to get price after
        if token_in == "base":
            reserve_in = self.virtual_reserves["base"]
            reserve_out = self.virtual_reserves["quote"]
            new_reserve_in = reserve_in + amount_in
            new_reserve_out = reserve_out - (amount_in * reserve_out) / (reserve_in + amount_in)
        else:
            reserve_in = self.virtual_reserves["quote"]
            reserve_out = self.virtual_reserves["base"]
            new_reserve_in = reserve_in + amount_in
            new_reserve_out = reserve_out - (amount_in * reserve_out) / (reserve_in + amount_in)
            
        # Calculate new price
        if token_in == "base":
            price_after = new_reserve_quote / new_reserve_base
        else:
            price_after = new_reserve_out / new_reserve_in
            
        # Calculate impact
        price_impact = abs(price_after - price_before) / price_before
        
        return price_impact
        
    def _get_current_price(self) -> Decimal:
        """Get current pool price"""
        if self.virtual_reserves["base"] == 0:
            return Decimal("0")
        return self.virtual_reserves["quote"] / self.virtual_reserves["base"]
        
    def _update_virtual_reserves(
        self,
        amount_base: Decimal,
        amount_quote: Decimal,
        is_add: bool
    ):
        """Update virtual reserves"""
        if is_add:
            self.virtual_reserves["base"] += amount_base
            self.virtual_reserves["quote"] += amount_quote
        else:
            self.virtual_reserves["base"] -= amount_base
            self.virtual_reserves["quote"] -= amount_quote
            
    def _update_price_scale(self):
        """Update price scale for dynamic adjustments"""
        current_price = self._get_current_price()
        if current_price > 0:
            self.price_scale = current_price
            
    # Concentrated liquidity helper functions
    def _tick_to_sqrt_price(self, tick: int) -> Decimal:
        """Convert tick to sqrt price"""
        return Decimal(1.0001 ** (tick / 2))
        
    def _sqrt_price_to_tick(self, sqrt_price: Decimal) -> int:
        """Convert sqrt price to tick"""
        return int(math.log(float(sqrt_price) ** 2) / math.log(1.0001))
        
    def _get_liquidity_at_tick(self, tick: int) -> Decimal:
        """Get total liquidity at a specific tick"""
        return self.liquidity_by_tick.get(tick, Decimal("0"))
        
    def _update_tick_liquidity(
        self,
        tick: int,
        liquidity_delta: Decimal,
        is_upper: bool
    ):
        """Update liquidity at a tick"""
        if tick not in self.liquidity_by_tick:
            self.liquidity_by_tick[tick] = Decimal("0")
            
        if is_upper:
            self.liquidity_by_tick[tick] -= liquidity_delta
        else:
            self.liquidity_by_tick[tick] += liquidity_delta
            
    def _get_next_initialized_tick(
        self,
        current_tick: int,
        search_direction_up: bool
    ) -> Optional[int]:
        """Find next tick with liquidity"""
        ticks = sorted(self.liquidity_by_tick.keys())
        
        if search_direction_up:
            for tick in ticks:
                if tick > current_tick and self.liquidity_by_tick[tick] != 0:
                    return tick
        else:
            for tick in reversed(ticks):
                if tick < current_tick and self.liquidity_by_tick[tick] != 0:
                    return tick
                    
        return None
        
    def _calculate_liquidity_from_amounts(
        self,
        amount_base: Decimal,
        amount_quote: Decimal,
        current_price: Decimal,
        tick_lower: int,
        tick_upper: int
    ) -> Decimal:
        """Calculate liquidity from token amounts"""
        sqrt_price_current = current_price.sqrt()
        sqrt_price_lower = self._tick_to_sqrt_price(tick_lower)
        sqrt_price_upper = self._tick_to_sqrt_price(tick_upper)
        
        if sqrt_price_current <= sqrt_price_lower:
            # Current price below range
            liquidity = amount_base * (sqrt_price_upper * sqrt_price_lower) / (sqrt_price_upper - sqrt_price_lower)
        elif sqrt_price_current >= sqrt_price_upper:
            # Current price above range
            liquidity = amount_quote / (sqrt_price_upper - sqrt_price_lower)
        else:
            # Current price within range
            liquidity0 = amount_base * (sqrt_price_upper * sqrt_price_current) / (sqrt_price_upper - sqrt_price_current)
            liquidity1 = amount_quote / (sqrt_price_current - sqrt_price_lower)
            liquidity = min(liquidity0, liquidity1)
            
        return liquidity
        
    def _calculate_amounts_from_liquidity(
        self,
        liquidity: Decimal,
        current_price: Decimal,
        tick_lower: int,
        tick_upper: int
    ) -> Dict[str, Decimal]:
        """Calculate token amounts from liquidity"""
        sqrt_price_current = current_price.sqrt()
        sqrt_price_lower = self._tick_to_sqrt_price(tick_lower)
        sqrt_price_upper = self._tick_to_sqrt_price(tick_upper)
        
        if sqrt_price_current <= sqrt_price_lower:
            # Current price below range
            amount_base = liquidity * (sqrt_price_upper - sqrt_price_lower) / (sqrt_price_upper * sqrt_price_lower)
            amount_quote = Decimal("0")
        elif sqrt_price_current >= sqrt_price_upper:
            # Current price above range
            amount_base = Decimal("0")
            amount_quote = liquidity * (sqrt_price_upper - sqrt_price_lower)
        else:
            # Current price within range
            amount_base = liquidity * (sqrt_price_upper - sqrt_price_current) / (sqrt_price_upper * sqrt_price_current)
            amount_quote = liquidity * (sqrt_price_current - sqrt_price_lower)
            
        return {
            "base": amount_base,
            "quote": amount_quote
        }
        
    def _calculate_amount_base(
        self,
        liquidity: Decimal,
        sqrt_price_a: Decimal,
        sqrt_price_b: Decimal
    ) -> Decimal:
        """Calculate base token amount for a price range"""
        return liquidity * abs(sqrt_price_b - sqrt_price_a) / (sqrt_price_a * sqrt_price_b)
        
    def _calculate_amount_quote(
        self,
        liquidity: Decimal,
        sqrt_price_a: Decimal,
        sqrt_price_b: Decimal
    ) -> Decimal:
        """Calculate quote token amount for a price range"""
        return liquidity * abs(sqrt_price_b - sqrt_price_a)
        
    def _calculate_amount_quote_from_base(
        self,
        amount_base: Decimal,
        liquidity: Decimal,
        sqrt_price: Decimal
    ) -> Decimal:
        """Calculate quote amount from base amount"""
        return amount_base * sqrt_price
        
    def _calculate_amount_base_from_quote(
        self,
        amount_quote: Decimal,
        liquidity: Decimal,
        sqrt_price: Decimal
    ) -> Decimal:
        """Calculate base amount from quote amount"""
        return amount_quote / sqrt_price
        
    def _validate_tick_range(self, tick_lower: int, tick_upper: int) -> bool:
        """Validate tick range"""
        return (
            tick_lower < tick_upper and
            tick_lower % self.tick_spacing == 0 and
            tick_upper % self.tick_spacing == 0 and
            tick_lower >= self._get_min_tick() and
            tick_upper <= self._get_max_tick()
        )
        
    def _get_min_tick(self) -> int:
        """Get minimum allowed tick"""
        return -887272  # Approximately 1.0001^-887272 ≈ 0
        
    def _get_max_tick(self) -> int:
        """Get maximum allowed tick"""
        return 887272  # Approximately 1.0001^887272 ≈ ∞
        
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get current pool statistics"""
        return {
            "total_liquidity": str(self.base_pool.total_liquidity),
            "volume_24h": str(self.base_pool.volume_24h),
            "fees_24h": str(self.base_pool.accumulated_fees.get(self.market.quote_asset, Decimal("0"))),
            "current_price": str(self._get_current_price()),
            "price_scale": str(self.price_scale),
            "tick_spacing": self.tick_spacing,
            "current_tick": self.current_tick,
            "virtual_reserves": {
                "base": str(self.virtual_reserves["base"]),
                "quote": str(self.virtual_reserves["quote"])
            },
            "utilization_rate": str(self.base_pool.utilization_rate),
            "unique_lps": self.base_pool.unique_lps
        } 