"""Options Automated Market Maker (AMM) implementation."""

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
import numpy as np
from dataclasses import dataclass
import logging

from platformq_derivatives_common import (
    BlackScholesEngine,
    GreeksCalculator,
    VolatilitySurfaceEngine,
    OptionType
)

logger = logging.getLogger(__name__)


@dataclass
class OptionPool:
    """Represents an options liquidity pool."""
    pool_id: str
    underlying_asset: str
    base_currency: str
    quote_currency: str
    
    # Liquidity parameters
    total_liquidity: Decimal
    base_reserves: Decimal  # Underlying asset reserves
    quote_reserves: Decimal  # Quote currency reserves
    
    # Option inventory
    call_inventory: Dict[str, Decimal]  # option_id -> quantity
    put_inventory: Dict[str, Decimal]   # option_id -> quantity
    
    # Risk parameters
    max_gamma: Decimal
    max_vega: Decimal
    target_delta: Decimal = Decimal("0")  # Target net delta (usually 0)
    
    # Fee structure
    base_fee: Decimal = Decimal("0.003")  # 0.3% base fee
    risk_premium: Decimal = Decimal("0")   # Additional fee based on risk
    
    # Pool state
    is_active: bool = True
    last_rebalance: datetime = None
    created_at: datetime = None


@dataclass
class OptionQuote:
    """Quote for an option trade."""
    option_id: str
    side: str  # "buy" or "sell"
    quantity: Decimal
    price: Decimal
    implied_volatility: Decimal
    greeks: Dict[str, Decimal]
    fee: Decimal
    slippage: Decimal
    expires_at: datetime


class OptionsAMM:
    """Automated Market Maker for options trading."""
    
    def __init__(
        self,
        pricing_engine: BlackScholesEngine,
        greeks_calculator: GreeksCalculator,
        vol_surface_engine: VolatilitySurfaceEngine,
        ignite_cache,
        pulsar_publisher
    ):
        self.pricing_engine = pricing_engine
        self.greeks_calculator = greeks_calculator
        self.vol_surface_engine = vol_surface_engine
        self.ignite = ignite_cache
        self.pulsar = pulsar_publisher
        
        self._pools: Dict[str, OptionPool] = {}
        self._running = False
        self._rebalance_task = None
        
    async def start(self):
        """Start the AMM service."""
        self._running = True
        self._rebalance_task = asyncio.create_task(self._rebalance_loop())
        logger.info("Options AMM started")
        
    async def stop(self):
        """Stop the AMM service."""
        self._running = False
        if self._rebalance_task:
            self._rebalance_task.cancel()
            try:
                await self._rebalance_task
            except asyncio.CancelledError:
                pass
        logger.info("Options AMM stopped")
        
    async def create_pool(
        self,
        underlying_asset: str,
        base_currency: str,
        quote_currency: str,
        initial_liquidity: Decimal,
        max_gamma: Decimal = Decimal("1000"),
        max_vega: Decimal = Decimal("10000")
    ) -> str:
        """Create a new options liquidity pool.
        
        Args:
            underlying_asset: Asset for options (e.g., "BTC")
            base_currency: Base currency for the pool
            quote_currency: Quote currency (usually USD)
            initial_liquidity: Initial liquidity in quote currency
            max_gamma: Maximum gamma exposure
            max_vega: Maximum vega exposure
            
        Returns:
            Pool ID
        """
        pool_id = f"{underlying_asset}-{quote_currency}-options-{datetime.utcnow().timestamp()}"
        
        pool = OptionPool(
            pool_id=pool_id,
            underlying_asset=underlying_asset,
            base_currency=base_currency,
            quote_currency=quote_currency,
            total_liquidity=initial_liquidity,
            base_reserves=Decimal("0"),
            quote_reserves=initial_liquidity,
            call_inventory={},
            put_inventory={},
            max_gamma=max_gamma,
            max_vega=max_vega,
            created_at=datetime.utcnow()
        )
        
        self._pools[pool_id] = pool
        await self._save_pool(pool)
        
        # Publish pool creation event
        await self.pulsar.publish_event(
            "options.pool.created",
            {
                "pool_id": pool_id,
                "underlying_asset": underlying_asset,
                "initial_liquidity": str(initial_liquidity)
            }
        )
        
        return pool_id
        
    async def get_quote(
        self,
        pool_id: str,
        option_id: str,
        option_type: OptionType,
        strike: Decimal,
        expiry: datetime,
        side: str,  # "buy" or "sell"
        quantity: Decimal
    ) -> Optional[OptionQuote]:
        """Get a quote for an option trade.
        
        Args:
            pool_id: Pool ID
            option_id: Option identifier
            option_type: Call or Put
            strike: Strike price
            expiry: Expiration date
            side: Buy or sell
            quantity: Number of contracts
            
        Returns:
            Option quote or None if cannot quote
        """
        pool = self._pools.get(pool_id)
        if not pool or not pool.is_active:
            return None
            
        # Get current market data
        spot_price = await self._get_spot_price(pool.underlying_asset)
        if not spot_price:
            return None
            
        # Calculate time to expiry
        time_to_expiry = (expiry - datetime.utcnow()).total_seconds() / (365 * 24 * 3600)
        if time_to_expiry <= 0:
            return None
            
        # Get implied volatility from surface
        iv = await self._get_implied_volatility(
            pool.underlying_asset,
            strike,
            expiry
        )
        
        if not iv:
            # Fallback to default volatility
            iv = Decimal("0.5")  # 50% annual volatility
            
        # Calculate theoretical price
        theo_price = self.pricing_engine.calculate_price(
            spot=spot_price,
            strike=strike,
            time_to_expiry=Decimal(str(time_to_expiry)),
            volatility=iv,
            risk_free_rate=Decimal("0.05"),  # 5% risk-free rate
            dividend_yield=Decimal("0"),
            option_type=option_type
        )
        
        # Calculate Greeks
        greeks = self.greeks_calculator.calculate_black_scholes_greeks(
            spot=spot_price,
            strike=strike,
            time_to_expiry=Decimal(str(time_to_expiry)),
            volatility=iv,
            risk_free_rate=Decimal("0.05"),
            dividend_yield=Decimal("0"),
            option_type=option_type
        )
        
        # Calculate pool's risk after trade
        risk_premium = await self._calculate_risk_premium(
            pool,
            greeks,
            quantity if side == "sell" else -quantity
        )
        
        # Calculate spread based on inventory
        inventory_spread = self._calculate_inventory_spread(
            pool,
            option_id,
            option_type,
            quantity,
            side
        )
        
        # Final pricing
        base_spread = pool.base_fee * theo_price
        total_spread = base_spread + inventory_spread + risk_premium
        
        if side == "buy":
            # Pool is selling - add spread
            price = theo_price + total_spread
        else:
            # Pool is buying - subtract spread
            price = theo_price - total_spread
            
        # Calculate fee
        fee = quantity * total_spread
        
        # Estimate slippage for large orders
        slippage = self._estimate_slippage(quantity, pool.total_liquidity)
        
        return OptionQuote(
            option_id=option_id,
            side=side,
            quantity=quantity,
            price=price,
            implied_volatility=iv,
            greeks={
                "delta": greeks.delta,
                "gamma": greeks.gamma,
                "theta": greeks.theta,
                "vega": greeks.vega,
                "rho": greeks.rho
            },
            fee=fee,
            slippage=slippage,
            expires_at=datetime.utcnow() + timedelta(seconds=30)
        )
        
    async def execute_trade(
        self,
        pool_id: str,
        quote: OptionQuote
    ) -> bool:
        """Execute a trade based on a quote.
        
        Args:
            pool_id: Pool ID
            quote: Quote to execute
            
        Returns:
            True if successful
        """
        pool = self._pools.get(pool_id)
        if not pool or not pool.is_active:
            return False
            
        # Verify quote is still valid
        if datetime.utcnow() > quote.expires_at:
            logger.warning(f"Quote expired for {quote.option_id}")
            return False
            
        # Update pool inventory
        if quote.side == "buy":
            # User is buying, pool is selling
            if quote.option_id.endswith("-C"):  # Call option
                current = pool.call_inventory.get(quote.option_id, Decimal("0"))
                pool.call_inventory[quote.option_id] = current - quote.quantity
            else:  # Put option
                current = pool.put_inventory.get(quote.option_id, Decimal("0"))
                pool.put_inventory[quote.option_id] = current - quote.quantity
                
            # Pool receives quote currency
            pool.quote_reserves += quote.price * quote.quantity
        else:
            # User is selling, pool is buying
            if quote.option_id.endswith("-C"):  # Call option
                current = pool.call_inventory.get(quote.option_id, Decimal("0"))
                pool.call_inventory[quote.option_id] = current + quote.quantity
            else:  # Put option
                current = pool.put_inventory.get(quote.option_id, Decimal("0"))
                pool.put_inventory[quote.option_id] = current + quote.quantity
                
            # Pool pays quote currency
            pool.quote_reserves -= quote.price * quote.quantity
            
        # Save updated pool state
        await self._save_pool(pool)
        
        # Publish trade event
        await self.pulsar.publish_event(
            "options.trade.executed",
            {
                "pool_id": pool_id,
                "option_id": quote.option_id,
                "side": quote.side,
                "quantity": str(quote.quantity),
                "price": str(quote.price),
                "fee": str(quote.fee)
            }
        )
        
        return True
        
    async def add_liquidity(
        self,
        pool_id: str,
        amount: Decimal,
        provider: str
    ) -> bool:
        """Add liquidity to a pool.
        
        Args:
            pool_id: Pool ID
            amount: Amount in quote currency
            provider: Liquidity provider ID
            
        Returns:
            True if successful
        """
        pool = self._pools.get(pool_id)
        if not pool:
            return False
            
        pool.quote_reserves += amount
        pool.total_liquidity += amount
        
        await self._save_pool(pool)
        
        # Track LP position (simplified - in production would mint LP tokens)
        await self.pulsar.publish_event(
            "options.liquidity.added",
            {
                "pool_id": pool_id,
                "provider": provider,
                "amount": str(amount)
            }
        )
        
        return True
        
    async def remove_liquidity(
        self,
        pool_id: str,
        amount: Decimal,
        provider: str
    ) -> Optional[Decimal]:
        """Remove liquidity from a pool.
        
        Args:
            pool_id: Pool ID
            amount: Amount to remove
            provider: Liquidity provider ID
            
        Returns:
            Amount removed or None if failed
        """
        pool = self._pools.get(pool_id)
        if not pool:
            return None
            
        # Check available liquidity
        if amount > pool.quote_reserves:
            amount = pool.quote_reserves
            
        pool.quote_reserves -= amount
        pool.total_liquidity -= amount
        
        await self._save_pool(pool)
        
        await self.pulsar.publish_event(
            "options.liquidity.removed",
            {
                "pool_id": pool_id,
                "provider": provider,
                "amount": str(amount)
            }
        )
        
        return amount
        
    async def _rebalance_loop(self):
        """Periodically rebalance pools to maintain risk limits."""
        while self._running:
            try:
                for pool_id, pool in self._pools.items():
                    if pool.is_active:
                        await self._rebalance_pool(pool)
                        
                await asyncio.sleep(60)  # Rebalance every minute
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in rebalance loop: {e}")
                await asyncio.sleep(60)
                
    async def _rebalance_pool(self, pool: OptionPool):
        """Rebalance a pool to maintain risk limits.
        
        This involves:
        1. Calculating current Greeks exposure
        2. Hedging if limits are exceeded
        3. Adjusting pricing to encourage rebalancing trades
        """
        try:
            # Calculate total Greeks for the pool
            total_delta = Decimal("0")
            total_gamma = Decimal("0")
            total_vega = Decimal("0")
            
            # Get current spot price
            spot_price = await self._get_spot_price(pool.underlying_asset)
            if not spot_price:
                return
                
            # Sum up Greeks for all positions
            # (Simplified - in production would calculate per option)
            
            # Check if rebalancing needed
            needs_rebalance = (
                abs(total_delta - pool.target_delta) > Decimal("10") or
                abs(total_gamma) > pool.max_gamma or
                abs(total_vega) > pool.max_vega
            )
            
            if needs_rebalance:
                # Execute hedging trades
                await self._hedge_pool_risk(pool, total_delta, total_gamma, total_vega)
                pool.last_rebalance = datetime.utcnow()
                await self._save_pool(pool)
                
                logger.info(f"Rebalanced pool {pool.pool_id}")
                
        except Exception as e:
            logger.error(f"Error rebalancing pool {pool.pool_id}: {e}")
            
    async def _hedge_pool_risk(
        self,
        pool: OptionPool,
        delta: Decimal,
        gamma: Decimal,
        vega: Decimal
    ):
        """Execute hedging trades to reduce risk.
        
        Simplified implementation - in production would:
        1. Trade spot/futures to hedge delta
        2. Trade options to hedge gamma/vega
        3. Use multiple venues for best execution
        """
        # Delta hedging
        if abs(delta - pool.target_delta) > Decimal("10"):
            hedge_amount = -(delta - pool.target_delta)
            # Execute spot trade to hedge delta
            # ... implementation ...
            
    async def _calculate_risk_premium(
        self,
        pool: OptionPool,
        trade_greeks: any,
        quantity: Decimal
    ) -> Decimal:
        """Calculate risk-based premium for a trade.
        
        Higher premium for trades that increase pool risk.
        """
        # Simplified - check if trade increases gamma/vega exposure
        premium = Decimal("0")
        
        # Gamma risk
        gamma_impact = abs(trade_greeks.gamma * quantity)
        if gamma_impact > pool.max_gamma * Decimal("0.1"):  # > 10% of limit
            premium += Decimal("0.001")  # 0.1% extra
            
        # Vega risk  
        vega_impact = abs(trade_greeks.vega * quantity)
        if vega_impact > pool.max_vega * Decimal("0.1"):
            premium += Decimal("0.001")
            
        return premium
        
    def _calculate_inventory_spread(
        self,
        pool: OptionPool,
        option_id: str,
        option_type: OptionType,
        quantity: Decimal,
        side: str
    ) -> Decimal:
        """Calculate spread adjustment based on inventory.
        
        Wider spread when inventory is imbalanced.
        """
        if option_type == OptionType.CALL:
            current_inventory = pool.call_inventory.get(option_id, Decimal("0"))
        else:
            current_inventory = pool.put_inventory.get(option_id, Decimal("0"))
            
        # Inventory after trade
        if side == "buy":
            # Pool sells, inventory decreases
            new_inventory = current_inventory - quantity
        else:
            # Pool buys, inventory increases
            new_inventory = current_inventory + quantity
            
        # Calculate imbalance
        imbalance = abs(new_inventory) / (pool.total_liquidity / Decimal("10000"))
        
        # Spread increases with imbalance
        inventory_spread = imbalance * Decimal("0.0001")  # 0.01% per unit
        
        return min(inventory_spread, Decimal("0.01"))  # Cap at 1%
        
    def _estimate_slippage(
        self,
        quantity: Decimal,
        total_liquidity: Decimal
    ) -> Decimal:
        """Estimate price slippage for large orders."""
        # Simple square-root model
        size_ratio = quantity / (total_liquidity / Decimal("1000"))
        slippage = Decimal(str(np.sqrt(float(size_ratio)))) * Decimal("0.001")
        
        return min(slippage, Decimal("0.05"))  # Cap at 5%
        
    async def _get_spot_price(self, asset: str) -> Optional[Decimal]:
        """Get current spot price for an asset."""
        # In production, would get from oracle service
        # Mock implementation
        prices = {
            "BTC": Decimal("50000"),
            "ETH": Decimal("3000"),
            "GPU-A100": Decimal("100")
        }
        return prices.get(asset)
        
    async def _get_implied_volatility(
        self,
        asset: str,
        strike: Decimal,
        expiry: datetime
    ) -> Optional[Decimal]:
        """Get implied volatility from volatility surface."""
        return self.vol_surface_engine.interpolate_volatility(
            underlying_asset=asset,
            strike=strike,
            expiry=expiry
        )
        
    async def _save_pool(self, pool: OptionPool):
        """Save pool state to cache."""
        await self.ignite.put(f"option_pool:{pool.pool_id}", pool)
        
    async def get_pool_stats(self, pool_id: str) -> Optional[Dict]:
        """Get statistics for a pool.
        
        Returns:
            Pool statistics including liquidity, volume, fees
        """
        pool = self._pools.get(pool_id)
        if not pool:
            return None
            
        # Calculate pool Greeks (simplified)
        total_delta = Decimal("0")
        total_gamma = Decimal("0")
        total_vega = Decimal("0")
        
        # In production, would sum up all position Greeks
        
        return {
            "pool_id": pool_id,
            "underlying_asset": pool.underlying_asset,
            "total_liquidity": str(pool.total_liquidity),
            "quote_reserves": str(pool.quote_reserves),
            "base_reserves": str(pool.base_reserves),
            "call_positions": len(pool.call_inventory),
            "put_positions": len(pool.put_inventory),
            "net_delta": str(total_delta),
            "net_gamma": str(total_gamma),
            "net_vega": str(total_vega),
            "last_rebalance": pool.last_rebalance.isoformat() if pool.last_rebalance else None,
            "is_active": pool.is_active
        } 