"""StableSwap AMM implementation for correlated assets."""

from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import logging

from app.config import Settings
from app.models.amm import (
    LiquidityPool, LiquidityPosition, SwapDirection, 
    SwapResult, PoolType
)
from platformq_trading_common import publish_event, EventType


# Set high precision
getcontext().prec = 28

logger = logging.getLogger(__name__)


class StableSwapAMM:
    """
    StableSwap AMM implementation for correlated assets.
    
    Uses StableSwap invariant: An^n * sum(x_i) + D = An^n * D + D^(n+1) / (n^n * prod(x_i))
    This provides lower slippage for assets that should maintain similar values.
    
    Ideal for:
    - Stablecoins (USDC/USDT/DAI)
    - Wrapped assets (ETH/WETH, BTC/WBTC)
    - Pegged assets
    """
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.default_amplification = settings.stableswap_amplification
        self.fee_bps = settings.stableswap_fee_bps
        
        # Precision for calculations
        self.precision = Decimal("1e18")
        self.fee_denominator = Decimal("10000")
        
    def create_pool(
        self,
        pool_id: str,
        base_asset: str,
        quote_asset: str,
        amplification: Optional[int] = None
    ) -> LiquidityPool:
        """Create a new StableSwap pool."""
        pool = LiquidityPool(
            pool_id=pool_id,
            pool_type=PoolType.STABLESWAP,
            base_asset=base_asset,
            quote_asset=quote_asset,
            base_fee_bps=self.fee_bps,
            amplification=amplification or self.default_amplification,
            current_price=Decimal("1")  # Start at 1:1 for stable pairs
        )
        
        return pool
        
    async def add_liquidity(
        self,
        pool: LiquidityPool,
        position: LiquidityPosition,
        base_amount: Decimal,
        quote_amount: Decimal,
        min_lp_tokens: Optional[Decimal] = None
    ) -> Tuple[Decimal, Decimal, Decimal]:
        """
        Add liquidity to StableSwap pool.
        
        Returns: (base_deposited, quote_deposited, lp_tokens_minted)
        """
        # First deposit initializes the pool
        if pool.total_liquidity == 0:
            if base_amount == 0 or quote_amount == 0:
                raise ValueError("Initial deposit must include both assets")
                
            # Initial LP tokens = geometric mean of amounts
            lp_tokens = (base_amount * quote_amount).sqrt()
            
            # Update pool state
            pool.base_reserve = base_amount
            pool.quote_reserve = quote_amount
            pool.total_liquidity = lp_tokens
            
            # Update position
            position.liquidity = lp_tokens
            position.base_amount = base_amount
            position.quote_amount = quote_amount
            
            await self._publish_liquidity_event(
                pool.pool_id, position, base_amount, quote_amount, lp_tokens, "add"
            )
            
            return base_amount, quote_amount, lp_tokens
            
        # Calculate optimal amounts for balanced deposit
        optimal_base, optimal_quote = self._calculate_optimal_deposit(
            pool, base_amount, quote_amount
        )
        
        # Calculate LP tokens to mint
        # Use the ratio of new liquidity to existing liquidity
        base_ratio = optimal_base / pool.base_reserve
        quote_ratio = optimal_quote / pool.quote_reserve
        
        # Should be approximately equal for balanced deposit
        lp_ratio = (base_ratio + quote_ratio) / Decimal("2")
        lp_tokens = pool.total_liquidity * lp_ratio
        
        if min_lp_tokens and lp_tokens < min_lp_tokens:
            raise ValueError(f"Insufficient LP tokens: {lp_tokens} < {min_lp_tokens}")
            
        # Update pool state
        pool.base_reserve += optimal_base
        pool.quote_reserve += optimal_quote
        pool.total_liquidity += lp_tokens
        
        # Update position
        position.liquidity += lp_tokens
        position.base_amount += optimal_base
        position.quote_amount += optimal_quote
        
        await self._publish_liquidity_event(
            pool.pool_id, position, optimal_base, optimal_quote, lp_tokens, "add"
        )
        
        return optimal_base, optimal_quote, lp_tokens
        
    async def remove_liquidity(
        self,
        pool: LiquidityPool,
        position: LiquidityPosition,
        lp_tokens: Decimal,
        min_base: Optional[Decimal] = None,
        min_quote: Optional[Decimal] = None
    ) -> Tuple[Decimal, Decimal]:
        """
        Remove liquidity from StableSwap pool.
        
        Returns: (base_amount, quote_amount)
        """
        if lp_tokens > position.liquidity:
            raise ValueError("Insufficient LP tokens")
            
        # Calculate proportional amounts
        lp_ratio = lp_tokens / pool.total_liquidity
        base_amount = pool.base_reserve * lp_ratio
        quote_amount = pool.quote_reserve * lp_ratio
        
        # Check minimums
        if min_base and base_amount < min_base:
            raise ValueError(f"Insufficient base output: {base_amount} < {min_base}")
        if min_quote and quote_amount < min_quote:
            raise ValueError(f"Insufficient quote output: {quote_amount} < {min_quote}")
            
        # Update pool state
        pool.base_reserve -= base_amount
        pool.quote_reserve -= quote_amount
        pool.total_liquidity -= lp_tokens
        
        # Update position
        position.liquidity -= lp_tokens
        position_ratio = lp_tokens / (position.liquidity + lp_tokens)
        position.base_amount -= position.base_amount * position_ratio
        position.quote_amount -= position.quote_amount * position_ratio
        
        await self._publish_liquidity_event(
            pool.pool_id, position, base_amount, quote_amount, lp_tokens, "remove"
        )
        
        return base_amount, quote_amount
        
    async def swap(
        self,
        pool: LiquidityPool,
        direction: SwapDirection,
        amount_in: Decimal,
        min_amount_out: Optional[Decimal] = None
    ) -> SwapResult:
        """Execute a swap using StableSwap curve."""
        # Apply fee
        fee_amount = amount_in * pool.base_fee_bps / self.fee_denominator
        amount_in_after_fee = amount_in - fee_amount
        
        # Calculate output using StableSwap invariant
        if direction == SwapDirection.BASE_TO_QUOTE:
            amount_out = self._calculate_swap(
                pool.base_reserve + amount_in_after_fee,
                pool.quote_reserve,
                pool.amplification,
                False  # Solving for quote amount
            )
            amount_out = pool.quote_reserve - amount_out
            
            # Update reserves
            new_base_reserve = pool.base_reserve + amount_in_after_fee
            new_quote_reserve = pool.quote_reserve - amount_out
        else:
            amount_out = self._calculate_swap(
                pool.base_reserve,
                pool.quote_reserve + amount_in_after_fee,
                pool.amplification,
                True  # Solving for base amount
            )
            amount_out = pool.base_reserve - amount_out
            
            # Update reserves
            new_base_reserve = pool.base_reserve - amount_out
            new_quote_reserve = pool.quote_reserve + amount_in_after_fee
            
        # Check slippage
        if min_amount_out and amount_out < min_amount_out:
            raise ValueError(f"Insufficient output: {amount_out} < {min_amount_out}")
            
        # Calculate price impact
        old_price = pool.quote_reserve / pool.base_reserve
        new_price = new_quote_reserve / new_base_reserve
        price_impact = abs(new_price - old_price) / old_price
        
        # Update pool state
        pool.base_reserve = new_base_reserve
        pool.quote_reserve = new_quote_reserve
        pool.current_price = new_price
        pool.volume_24h += amount_in
        pool.fees_collected_24h += fee_amount
        pool.trades_24h += 1
        
        # Update imbalance ratio
        total_value = new_base_reserve + new_quote_reserve
        pool.imbalance_ratio = new_base_reserve / total_value
        
        return SwapResult(
            swap_id=f"swap_{datetime.utcnow().timestamp()}",
            pool_id=pool.pool_id,
            trader="",  # Set by caller
            direction=direction,
            amount_in=amount_in,
            amount_out=amount_out,
            fee_paid=fee_amount,
            execution_price=amount_out / amount_in_after_fee,
            price_impact=price_impact,
            slippage=price_impact,
            new_base_reserve=new_base_reserve,
            new_quote_reserve=new_quote_reserve,
            new_price=new_price
        )
        
    def _calculate_swap(
        self,
        x_new: Decimal,
        y_old: Decimal,
        amplification: int,
        solving_for_x: bool
    ) -> Decimal:
        """
        Calculate swap output using StableSwap invariant.
        
        For 2 assets: 4A(x+y) + D = 4AD + D³/(4xy)
        """
        A = Decimal(amplification)
        
        # Calculate current D (invariant)
        if solving_for_x:
            # We have new y, old x
            x_old = y_old
            y_new = x_new
            D = self._calculate_d(x_old, y_old, A)
            
            # Solve for new x
            return self._solve_for_asset(y_new, D, A)
        else:
            # We have new x, old y
            D = self._calculate_d(x_new - (x_new - y_old), y_old, A)
            
            # Solve for new y
            return self._solve_for_asset(x_new, D, A)
            
    def _calculate_d(self, x: Decimal, y: Decimal, A: Decimal) -> Decimal:
        """Calculate StableSwap invariant D."""
        S = x + y
        if S == 0:
            return Decimal("0")
            
        # Newton's method to solve for D
        # 4A(x+y) + D = 4AD + D³/(4xy)
        D = S
        for _ in range(255):
            D_prev = D
            
            # Calculate D_P = D³/(4xy)
            D_P = D ** 3 / (Decimal("4") * x * y)
            
            # Newton iteration
            numerator = D * (Decimal("4") * A * S + D_P)
            denominator = Decimal("4") * A * D + Decimal("3") * D_P
            D = numerator / denominator
            
            # Check convergence
            if abs(D - D_prev) <= 1:
                break
                
        return D
        
    def _solve_for_asset(self, other_asset: Decimal, D: Decimal, A: Decimal) -> Decimal:
        """Solve for one asset amount given the other asset and D."""
        # Solving: 4Ay + D = 4AD + D³/(4xy)
        # Rearranged: 4xy² + (4Ax - 4AD - D)y - D³/4 = 0
        
        c = D ** 3 / (Decimal("4") * A * other_asset)
        b = other_asset + D / (Decimal("4") * A)
        
        # Quadratic formula (only positive root makes sense)
        discriminant = b ** 2 + Decimal("4") * c
        y = (discriminant.sqrt() - b) / Decimal("2")
        
        return y
        
    def _calculate_optimal_deposit(
        self,
        pool: LiquidityPool,
        base_amount: Decimal,
        quote_amount: Decimal
    ) -> Tuple[Decimal, Decimal]:
        """Calculate optimal deposit amounts to minimize slippage."""
        # For StableSwap, we want to maintain the current ratio
        current_ratio = pool.base_reserve / pool.quote_reserve
        deposit_ratio = base_amount / quote_amount if quote_amount > 0 else Decimal("0")
        
        if abs(current_ratio - deposit_ratio) < Decimal("0.001"):
            # Close enough to optimal
            return base_amount, quote_amount
            
        # Adjust to match pool ratio
        if deposit_ratio > current_ratio:
            # Too much base, reduce it
            optimal_base = quote_amount * current_ratio
            optimal_quote = quote_amount
        else:
            # Too much quote, reduce it
            optimal_base = base_amount
            optimal_quote = base_amount / current_ratio
            
        return optimal_base, optimal_quote
        
    async def _publish_liquidity_event(
        self,
        pool_id: str,
        position: LiquidityPosition,
        base_amount: Decimal,
        quote_amount: Decimal,
        lp_tokens: Decimal,
        action: str
    ):
        """Publish liquidity event."""
        event_type = EventType.LIQUIDITY_ADDED if action == "add" else EventType.LIQUIDITY_REMOVED
        
        await publish_event(
            event_type,
            {
                "pool_id": pool_id,
                "position_id": position.position_id,
                "provider": position.provider,
                "base_amount": str(base_amount),
                "quote_amount": str(quote_amount),
                "lp_tokens": str(lp_tokens),
                "action": action,
                "timestamp": datetime.utcnow().isoformat()
            }
        ) 