"""
Options AMM (Automated Market Maker) engine
Provides liquidity for options trading with dynamic pricing and hedging
"""

from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import numpy as np
from scipy import stats
import logging
import asyncio
from collections import defaultdict

from ..models.position import Position, PositionSide
from ..models.order import Order, OrderType, OrderSide
from ..engines.pricing import (
    BlackScholesEngine, 
    OptionParameters, 
    Greeks,
    VolatilitySurfaceEngine
)
from ..integrations import IgniteCache as IgniteClient, PulsarEventPublisher as PulsarClient

# Set high precision
getcontext().prec = 28

logger = logging.getLogger(__name__)


@dataclass
class OptionsLiquidity:
    """Liquidity configuration for an option"""
    strike: Decimal
    expiry: datetime
    is_call: bool
    base_size: Decimal  # Base liquidity size
    spread_bps: Decimal  # Spread in basis points
    max_position: Decimal  # Max position size
    current_position: Decimal = Decimal("0")
    volume_24h: Decimal = Decimal("0")
    
    def get_available_liquidity(self, side: OrderSide) -> Decimal:
        """Get available liquidity for a given side"""
        if side == OrderSide.BUY:
            # AMM is selling - check short position limit
            return max(Decimal("0"), self.max_position + self.current_position)
        else:
            # AMM is buying - check long position limit
            return max(Decimal("0"), self.max_position - self.current_position)


@dataclass
class HedgePosition:
    """Hedge position for delta-neutral strategy"""
    underlying_position: Decimal = Decimal("0")
    futures_position: Decimal = Decimal("0")
    options_positions: Dict[str, Decimal] = field(default_factory=dict)
    last_hedge_time: Optional[datetime] = None
    hedge_threshold: Decimal = Decimal("0.1")  # Re-hedge when delta exceeds this


@dataclass
class AMMConfig:
    """Configuration for Options AMM"""
    # Pricing parameters
    base_spread_bps: Decimal = Decimal("50")  # 0.5%
    min_spread_bps: Decimal = Decimal("20")
    max_spread_bps: Decimal = Decimal("200")
    
    # Risk parameters
    max_net_delta: Decimal = Decimal("1000")
    max_net_gamma: Decimal = Decimal("100")
    max_net_vega: Decimal = Decimal("500")
    position_limit_multiplier: Decimal = Decimal("2")
    
    # Liquidity parameters
    base_liquidity_size: Decimal = Decimal("100")
    liquidity_depth_levels: int = 5
    liquidity_level_spacing_bps: Decimal = Decimal("25")  # 0.25% between levels
    
    # Hedging parameters
    hedge_enabled: bool = True
    hedge_interval_seconds: int = 60
    delta_hedge_threshold: Decimal = Decimal("50")
    gamma_hedge_threshold: Decimal = Decimal("10")
    
    # Fee parameters
    maker_fee_bps: Decimal = Decimal("10")  # 0.1%
    taker_fee_bps: Decimal = Decimal("30")  # 0.3%


class OptionsAMM:
    """Automated Market Maker for options trading"""
    
    def __init__(
        self,
        config: AMMConfig,
        pricing_engine: BlackScholesEngine,
        vol_surface: VolatilitySurfaceEngine,
        ignite_client: IgniteClient,
        pulsar_client: PulsarClient
    ):
        self.config = config
        self.pricing_engine = pricing_engine
        self.vol_surface = vol_surface
        self.ignite_client = ignite_client
        self.pulsar_client = pulsar_client
        
        # Liquidity pools for each option
        self.liquidity_pools: Dict[str, OptionsLiquidity] = {}
        
        # AMM positions and P&L tracking
        self.positions: Dict[str, Position] = {}
        self.realized_pnl = Decimal("0")
        self.collected_fees = Decimal("0")
        
        # Hedging
        self.hedge_positions = HedgePosition()
        self.last_hedge_check = datetime.now()
        
        # Risk metrics
        self.portfolio_greeks = {
            "delta": Decimal("0"),
            "gamma": Decimal("0"),
            "theta": Decimal("0"),
            "vega": Decimal("0"),
            "rho": Decimal("0")
        }
        
        # Market data cache
        self.spot_prices: Dict[str, Decimal] = {}
        self.last_trades: Dict[str, List[Tuple[datetime, Decimal, Decimal]]] = defaultdict(list)
        
        # Start background tasks
        self._running = True
        self._tasks = []
    
    async def start(self):
        """Start AMM background tasks"""
        self._tasks.append(asyncio.create_task(self._hedge_loop()))
        self._tasks.append(asyncio.create_task(self._update_volatility_loop()))
        self._tasks.append(asyncio.create_task(self._risk_monitor_loop()))
        logger.info("Options AMM started")
    
    async def stop(self):
        """Stop AMM background tasks"""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("Options AMM stopped")
    
    def add_liquidity_pool(
        self,
        underlying: str,
        strike: Decimal,
        expiry: datetime,
        is_call: bool,
        base_size: Optional[Decimal] = None,
        custom_spread: Optional[Decimal] = None
    ) -> str:
        """Add a new liquidity pool for an option"""
        
        pool_id = f"{underlying}_{strike}_{expiry.strftime('%Y%m%d')}_{('C' if is_call else 'P')}"
        
        if pool_id in self.liquidity_pools:
            logger.warning(f"Liquidity pool {pool_id} already exists")
            return pool_id
        
        # Create liquidity configuration
        liquidity = OptionsLiquidity(
            strike=strike,
            expiry=expiry,
            is_call=is_call,
            base_size=base_size or self.config.base_liquidity_size,
            spread_bps=custom_spread or self.config.base_spread_bps,
            max_position=((base_size or self.config.base_liquidity_size) * 
                         self.config.position_limit_multiplier)
        )
        
        self.liquidity_pools[pool_id] = liquidity
        
        # Initialize position tracking
        self.positions[pool_id] = Position(
            user_id="AMM",
            market_id=pool_id,
            size=Decimal("0"),
            side=PositionSide.LONG,  # Will flip based on net position
            entry_price=Decimal("0"),
            leverage=Decimal("1")
        )
        
        logger.info(f"Added liquidity pool: {pool_id}")
        return pool_id
    
    def get_quote(
        self,
        underlying: str,
        strike: Decimal,
        expiry: datetime,
        is_call: bool,
        size: Decimal,
        side: OrderSide,
        spot_price: Optional[Decimal] = None
    ) -> Optional[Dict]:
        """Get a quote for an option trade"""
        
        pool_id = f"{underlying}_{strike}_{expiry.strftime('%Y%m%d')}_{('C' if is_call else 'P')}"
        
        if pool_id not in self.liquidity_pools:
            return None
        
        liquidity = self.liquidity_pools[pool_id]
        
        # Check available liquidity
        available = liquidity.get_available_liquidity(side)
        if size > available:
            return None
        
        # Update spot price if provided
        if spot_price:
            self.spot_prices[underlying] = spot_price
        elif underlying not in self.spot_prices:
            logger.error(f"No spot price for {underlying}")
            return None
        else:
            spot_price = self.spot_prices[underlying]
        
        # Calculate theoretical value
        time_to_expiry = Decimal(str((expiry - datetime.now()).days / 365.25))
        iv = self.vol_surface.get_implied_volatility(strike, time_to_expiry, spot_price)
        
        params = OptionParameters(
            spot=spot_price,
            strike=strike,
            time_to_expiry=time_to_expiry,
            volatility=iv,
            risk_free_rate=Decimal("0.05"),  # TODO: Get from config
            is_call=is_call
        )
        
        theo_price = self.pricing_engine.calculate_option_price(params)
        greeks = self.pricing_engine.calculate_greeks(params)
        
        # Calculate spread based on various factors
        spread_bps = self._calculate_dynamic_spread(
            pool_id, liquidity, size, greeks, side
        )
        
        # Apply spread to get bid/ask
        spread_decimal = spread_bps / Decimal("10000")
        
        if side == OrderSide.BUY:
            # User is buying, AMM is selling - add spread
            price = theo_price * (Decimal("1") + spread_decimal)
        else:
            # User is selling, AMM is buying - subtract spread
            price = theo_price * (Decimal("1") - spread_decimal)
        
        # Calculate fees
        fee_bps = self.config.taker_fee_bps
        fee = price * size * fee_bps / Decimal("10000")
        
        # Calculate price impact for large orders
        impact_bps = self._calculate_price_impact(size, liquidity, greeks)
        price = price * (Decimal("1") + impact_bps / Decimal("10000"))
        
        return {
            "pool_id": pool_id,
            "price": price,
            "size": size,
            "side": side,
            "theo_price": theo_price,
            "spread_bps": spread_bps,
            "impact_bps": impact_bps,
            "fee": fee,
            "iv": iv,
            "greeks": greeks,
            "available_liquidity": available,
            "timestamp": datetime.now()
        }
    
    async def execute_trade(
        self,
        quote: Dict,
        slippage_tolerance_bps: Decimal = Decimal("50")
    ) -> Optional[Dict]:
        """Execute a trade based on a quote"""
        
        pool_id = quote["pool_id"]
        if pool_id not in self.liquidity_pools:
            return None
        
        liquidity = self.liquidity_pools[pool_id]
        
        # Re-check the price hasn't moved too much
        current_quote = self.get_quote(
            liquidity.strike,  # Need to parse from pool_id
            liquidity.strike,
            liquidity.expiry,
            liquidity.is_call,
            quote["size"],
            quote["side"],
            self.spot_prices.get(pool_id.split("_")[0])
        )
        
        if not current_quote:
            return None
        
        # Check slippage
        price_change_bps = abs(current_quote["price"] - quote["price"]) / quote["price"] * Decimal("10000")
        if price_change_bps > slippage_tolerance_bps:
            logger.warning(f"Slippage too high: {price_change_bps} bps")
            return None
        
        # Update AMM position
        position = self.positions[pool_id]
        
        if quote["side"] == OrderSide.BUY:
            # User buying, AMM selling (short)
            position.size -= quote["size"]
        else:
            # User selling, AMM buying (long)
            position.size += quote["size"]
        
        # Update position side
        if position.size < 0:
            position.side = PositionSide.SHORT
        else:
            position.side = PositionSide.LONG
        
        # Update liquidity pool metrics
        liquidity.current_position = abs(position.size)
        liquidity.volume_24h += quote["size"]
        
        # Record the trade
        trade_record = {
            "pool_id": pool_id,
            "price": current_quote["price"],
            "size": quote["size"],
            "side": quote["side"],
            "fee": current_quote["fee"],
            "timestamp": datetime.now(),
            "position_after": position.size,
            "pnl": Decimal("0")  # Will be calculated on position close
        }
        
        # Update collected fees
        self.collected_fees += current_quote["fee"]
        
        # Store trade for volatility updates
        underlying = pool_id.split("_")[0]
        self.last_trades[underlying].append(
            (datetime.now(), current_quote["price"], quote["size"])
        )
        
        # Emit trade event
        await self.pulsar_client.publish(
            f"options-trade-{pool_id}",
            trade_record
        )
        
        # Update portfolio Greeks
        await self._update_portfolio_greeks()
        
        # Check if hedging needed
        if self.config.hedge_enabled:
            await self._check_and_hedge()
        
        return trade_record
    
    def _calculate_dynamic_spread(
        self,
        pool_id: str,
        liquidity: OptionsLiquidity,
        size: Decimal,
        greeks: Greeks,
        side: OrderSide
    ) -> Decimal:
        """Calculate dynamic spread based on market conditions"""
        
        base_spread = liquidity.spread_bps
        
        # 1. Adjust for position risk
        position_ratio = abs(liquidity.current_position) / liquidity.max_position
        position_adjustment = position_ratio * Decimal("50")  # Up to 50 bps for full position
        
        # 2. Adjust for Greeks exposure
        greek_risk = (
            abs(self.portfolio_greeks["delta"]) / self.config.max_net_delta * Decimal("30") +
            abs(self.portfolio_greeks["gamma"]) / self.config.max_net_gamma * Decimal("20") +
            abs(self.portfolio_greeks["vega"]) / self.config.max_net_vega * Decimal("25")
        )
        
        # 3. Adjust for order size
        size_ratio = size / liquidity.base_size
        size_adjustment = max(Decimal("0"), (size_ratio - Decimal("1")) * Decimal("20"))
        
        # 4. Adjust for time to expiry
        time_to_expiry = (liquidity.expiry - datetime.now()).days
        if time_to_expiry < 7:
            expiry_adjustment = Decimal("30")  # Wide spread near expiry
        elif time_to_expiry < 30:
            expiry_adjustment = Decimal("15")
        else:
            expiry_adjustment = Decimal("0")
        
        # 5. Directional adjustment - widen spread if trade increases risk
        directional_adjustment = Decimal("0")
        if side == OrderSide.BUY and liquidity.current_position < 0:
            # User buying when AMM is already short - increases risk
            directional_adjustment = Decimal("20")
        elif side == OrderSide.SELL and liquidity.current_position > 0:
            # User selling when AMM is already long - increases risk
            directional_adjustment = Decimal("20")
        
        # Calculate total spread
        total_spread = (
            base_spread + 
            position_adjustment + 
            greek_risk + 
            size_adjustment + 
            expiry_adjustment +
            directional_adjustment
        )
        
        # Apply bounds
        return max(
            self.config.min_spread_bps,
            min(total_spread, self.config.max_spread_bps)
        )
    
    def _calculate_price_impact(
        self,
        size: Decimal,
        liquidity: OptionsLiquidity,
        greeks: Greeks
    ) -> Decimal:
        """Calculate price impact for large orders"""
        
        # Simple square-root impact model
        size_ratio = size / liquidity.base_size
        base_impact = Decimal(str(float(size_ratio) ** 0.5)) * Decimal("10")  # 10 bps per unit
        
        # Adjust for gamma (nonlinear risk)
        gamma_multiplier = Decimal("1") + (greeks.gamma * size * Decimal("0.1"))
        
        return base_impact * gamma_multiplier
    
    async def _update_portfolio_greeks(self):
        """Update total portfolio Greeks"""
        
        total_greeks = {
            "delta": Decimal("0"),
            "gamma": Decimal("0"),
            "theta": Decimal("0"),
            "vega": Decimal("0"),
            "rho": Decimal("0")
        }
        
        for pool_id, position in self.positions.items():
            if position.size == Decimal("0"):
                continue
            
            liquidity = self.liquidity_pools[pool_id]
            underlying = pool_id.split("_")[0]
            
            if underlying not in self.spot_prices:
                continue
            
            spot_price = self.spot_prices[underlying]
            time_to_expiry = Decimal(str((liquidity.expiry - datetime.now()).days / 365.25))
            
            if time_to_expiry <= 0:
                continue
            
            # Get current IV
            iv = self.vol_surface.get_implied_volatility(
                liquidity.strike, time_to_expiry, spot_price
            )
            
            # Calculate Greeks for this position
            params = OptionParameters(
                spot=spot_price,
                strike=liquidity.strike,
                time_to_expiry=time_to_expiry,
                volatility=iv,
                risk_free_rate=Decimal("0.05"),
                is_call=liquidity.is_call
            )
            
            greeks = self.pricing_engine.calculate_greeks(params)
            
            # Add to portfolio (negative for short positions)
            sign = Decimal("1") if position.side == PositionSide.LONG else Decimal("-1")
            total_greeks["delta"] += greeks.delta * position.size * sign
            total_greeks["gamma"] += greeks.gamma * position.size * sign
            total_greeks["theta"] += greeks.theta * position.size * sign
            total_greeks["vega"] += greeks.vega * position.size * sign
            total_greeks["rho"] += greeks.rho * position.size * sign
        
        self.portfolio_greeks = total_greeks
        
        # Cache in Ignite
        await self.ignite_client.put(
            f"amm_portfolio_greeks",
            self.portfolio_greeks
        )
    
    async def _check_and_hedge(self):
        """Check if hedging is needed and execute"""
        
        if not self.config.hedge_enabled:
            return
        
        # Check delta hedge threshold
        net_delta = self.portfolio_greeks["delta"]
        
        if abs(net_delta) > self.config.delta_hedge_threshold:
            # Calculate hedge size
            hedge_size = -net_delta  # Opposite sign to neutralize
            
            # Execute hedge (simplified - would integrate with spot/futures market)
            logger.info(f"Executing delta hedge: {hedge_size}")
            
            self.hedge_positions.underlying_position += hedge_size
            self.hedge_positions.last_hedge_time = datetime.now()
            
            # Emit hedge event
            await self.pulsar_client.publish(
                "amm-hedge-executed",
                {
                    "type": "delta",
                    "size": hedge_size,
                    "portfolio_delta_before": net_delta,
                    "timestamp": datetime.now()
                }
            )
        
        # Check gamma hedge threshold
        net_gamma = self.portfolio_greeks["gamma"]
        
        if abs(net_gamma) > self.config.gamma_hedge_threshold:
            # Gamma hedging requires options
            # Find ATM options to hedge gamma
            logger.info(f"Gamma hedge needed: {net_gamma}")
            # Implementation would find appropriate options to trade
    
    async def _hedge_loop(self):
        """Background task for periodic hedging"""
        
        while self._running:
            try:
                await asyncio.sleep(self.config.hedge_interval_seconds)
                
                if self.config.hedge_enabled:
                    await self._check_and_hedge()
                    
            except Exception as e:
                logger.error(f"Error in hedge loop: {e}")
    
    async def _update_volatility_loop(self):
        """Background task to update implied volatility from trades"""
        
        while self._running:
            try:
                await asyncio.sleep(60)  # Update every minute
                
                # Process recent trades to update vol surface
                for underlying, trades in self.last_trades.items():
                    if not trades:
                        continue
                    
                    # Filter recent trades (last hour)
                    cutoff_time = datetime.now() - timedelta(hours=1)
                    recent_trades = [t for t in trades if t[0] > cutoff_time]
                    
                    if len(recent_trades) >= 5:  # Need minimum trades
                        # Calculate realized volatility
                        prices = [t[1] for t in recent_trades]
                        returns = [
                            float(np.log(float(prices[i]) / float(prices[i-1])))
                            for i in range(1, len(prices))
                        ]
                        
                        if returns:
                            realized_vol = Decimal(str(np.std(returns) * np.sqrt(252)))
                            
                            # Update vol surface with new information
                            # This is simplified - real implementation would
                            # update the entire surface based on traded IVs
                            logger.info(f"Updated realized vol for {underlying}: {realized_vol}")
                
                # Clean old trades
                cutoff_time = datetime.now() - timedelta(hours=24)
                for underlying in self.last_trades:
                    self.last_trades[underlying] = [
                        t for t in self.last_trades[underlying] 
                        if t[0] > cutoff_time
                    ]
                    
            except Exception as e:
                logger.error(f"Error in volatility update loop: {e}")
    
    async def _risk_monitor_loop(self):
        """Background task to monitor risk limits"""
        
        while self._running:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                # Check Greek limits
                warnings = []
                
                if abs(self.portfolio_greeks["delta"]) > self.config.max_net_delta * Decimal("0.8"):
                    warnings.append(f"Delta approaching limit: {self.portfolio_greeks['delta']}")
                
                if abs(self.portfolio_greeks["gamma"]) > self.config.max_net_gamma * Decimal("0.8"):
                    warnings.append(f"Gamma approaching limit: {self.portfolio_greeks['gamma']}")
                
                if abs(self.portfolio_greeks["vega"]) > self.config.max_net_vega * Decimal("0.8"):
                    warnings.append(f"Vega approaching limit: {self.portfolio_greeks['vega']}")
                
                # Check position limits
                for pool_id, liquidity in self.liquidity_pools.items():
                    if liquidity.current_position > liquidity.max_position * Decimal("0.9"):
                        warnings.append(f"Position limit approaching for {pool_id}")
                
                if warnings:
                    await self.pulsar_client.publish(
                        "amm-risk-warnings",
                        {
                            "warnings": warnings,
                            "portfolio_greeks": self.portfolio_greeks,
                            "timestamp": datetime.now()
                        }
                    )
                    
                    for warning in warnings:
                        logger.warning(warning)
                        
            except Exception as e:
                logger.error(f"Error in risk monitor loop: {e}")
    
    def get_portfolio_summary(self) -> Dict:
        """Get current portfolio summary"""
        
        active_positions = sum(
            1 for p in self.positions.values() 
            if p.size != Decimal("0")
        )
        
        total_notional = Decimal("0")
        for pool_id, position in self.positions.items():
            if position.size == Decimal("0"):
                continue
            
            # Get current market price
            liquidity = self.liquidity_pools[pool_id]
            underlying = pool_id.split("_")[0]
            
            if underlying in self.spot_prices:
                spot = self.spot_prices[underlying]
                params = OptionParameters(
                    spot=spot,
                    strike=liquidity.strike,
                    time_to_expiry=Decimal(str((liquidity.expiry - datetime.now()).days / 365.25)),
                    volatility=Decimal("0.3"),  # Simplified
                    risk_free_rate=Decimal("0.05"),
                    is_call=liquidity.is_call
                )
                
                price = self.pricing_engine.calculate_option_price(params)
                total_notional += abs(position.size) * price
        
        return {
            "active_positions": active_positions,
            "total_notional": total_notional,
            "portfolio_greeks": self.portfolio_greeks,
            "hedge_positions": {
                "underlying": self.hedge_positions.underlying_position,
                "futures": self.hedge_positions.futures_position
            },
            "realized_pnl": self.realized_pnl,
            "collected_fees": self.collected_fees,
            "liquidity_pools": len(self.liquidity_pools),
            "last_update": datetime.now()
        }
    
    def get_liquidity_depth(
        self,
        underlying: str,
        strike: Decimal,
        expiry: datetime,
        is_call: bool
    ) -> List[Dict]:
        """Get order book depth for an option"""
        
        pool_id = f"{underlying}_{strike}_{expiry.strftime('%Y%m%d')}_{('C' if is_call else 'P')}"
        
        if pool_id not in self.liquidity_pools:
            return []
        
        liquidity = self.liquidity_pools[pool_id]
        
        if underlying not in self.spot_prices:
            return []
        
        spot = self.spot_prices[underlying]
        
        # Generate multiple price levels
        levels = []
        
        for i in range(self.config.liquidity_depth_levels):
            # Increase spread for each level
            level_spread_bps = (
                self.config.base_spread_bps + 
                i * self.config.liquidity_level_spacing_bps
            )
            
            # Decrease size for each level
            level_size = liquidity.base_size * Decimal(str(0.8 ** i))
            
            # Calculate prices for this level
            time_to_expiry = Decimal(str((expiry - datetime.now()).days / 365.25))
            iv = self.vol_surface.get_implied_volatility(strike, time_to_expiry, spot)
            
            params = OptionParameters(
                spot=spot,
                strike=strike,
                time_to_expiry=time_to_expiry,
                volatility=iv,
                risk_free_rate=Decimal("0.05"),
                is_call=is_call
            )
            
            theo_price = self.pricing_engine.calculate_option_price(params)
            
            spread_decimal = level_spread_bps / Decimal("10000")
            
            bid_price = theo_price * (Decimal("1") - spread_decimal)
            ask_price = theo_price * (Decimal("1") + spread_decimal)
            
            levels.append({
                "level": i,
                "bid_price": bid_price,
                "bid_size": level_size,
                "ask_price": ask_price,
                "ask_size": level_size,
                "spread_bps": level_spread_bps
            })
        
        return levels 