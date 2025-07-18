"""
Delta-Neutral Options Market Making Strategy

Implements automated market making for compute options with delta hedging,
dynamic volatility adjustment, and risk management.
"""

import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import defaultdict

from app.engines.compute_options_engine import (
    ComputeOptionsEngine,
    OptionType,
    ExerciseStyle,
    Greeks
)
from app.engines.compute_spot_market import ComputeSpotMarket, OrderType, OrderSide
from app.engines.volatility_surface import VolatilitySurfaceEngine
from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient

logger = logging.getLogger(__name__)


class HedgeType(Enum):
    DELTA = "delta"
    GAMMA = "gamma"
    VEGA = "vega"
    FULL = "full"


class MMState(Enum):
    ACTIVE = "active"
    HEDGING = "hedging"
    REDUCING = "reducing"  # Reducing position
    PAUSED = "paused"
    STOPPED = "stopped"


@dataclass
class OptionPosition:
    """Represents an option position"""
    option_id: str
    option_type: OptionType
    strike: Decimal
    expiry: datetime
    quantity: Decimal  # Positive for long, negative for short
    entry_price: Decimal
    current_price: Decimal
    greeks: Greeks
    last_hedged: datetime
    pnl: Decimal = Decimal("0")


@dataclass
class MarketMakingConfig:
    """Configuration for delta-neutral market making"""
    resource_type: str
    
    # Spread configuration
    base_spread: Decimal = Decimal("0.02")  # 2%
    min_spread: Decimal = Decimal("0.01")   # 1%
    max_spread: Decimal = Decimal("0.05")   # 5%
    
    # Position limits
    max_net_delta: Decimal = Decimal("100")     # Max 100 units of directional risk
    max_net_gamma: Decimal = Decimal("50")      # Max gamma exposure
    max_net_vega: Decimal = Decimal("1000")     # Max vega exposure
    max_position_value: Decimal = Decimal("1000000")  # $1M max
    
    # Hedging parameters
    hedge_type: HedgeType = HedgeType.DELTA
    delta_hedge_threshold: Decimal = Decimal("10")   # Hedge when delta > 10
    gamma_hedge_threshold: Decimal = Decimal("5")    # Hedge when gamma > 5
    hedge_interval: int = 300  # seconds
    use_dynamic_hedging: bool = True
    
    # Volatility parameters
    use_implied_vol: bool = True
    vol_adjustment_factor: Decimal = Decimal("0.1")  # 10% vol adjustment
    min_implied_vol: Decimal = Decimal("0.2")  # 20%
    max_implied_vol: Decimal = Decimal("2.0")  # 200%
    
    # Quote parameters
    min_quote_size: Decimal = Decimal("1")
    max_quote_size: Decimal = Decimal("100")
    quote_refresh_interval: int = 10  # seconds
    
    # Risk management
    max_loss_per_day: Decimal = Decimal("10000")
    max_var_95: Decimal = Decimal("50000")
    stop_loss_percentage: Decimal = Decimal("0.05")  # 5%
    
    # Fee structure
    maker_fee: Decimal = Decimal("0.001")  # 0.1%
    taker_fee: Decimal = Decimal("0.002")  # 0.2%


@dataclass
class MarketMakingStats:
    """Performance statistics for market making"""
    total_volume: Decimal = Decimal("0")
    total_trades: int = 0
    options_bought: int = 0
    options_sold: int = 0
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    fees_earned: Decimal = Decimal("0")
    hedging_costs: Decimal = Decimal("0")
    max_drawdown: Decimal = Decimal("0")
    sharpe_ratio: Decimal = Decimal("0")
    daily_pnl: List[Decimal] = field(default_factory=list)
    position_history: List[int] = field(default_factory=list)


class DeltaNeutralOptionsMM:
    """
    Delta-neutral options market making strategy for compute resources
    """
    
    def __init__(self,
                 options_engine: ComputeOptionsEngine,
                 spot_market: ComputeSpotMarket,
                 vol_surface: VolatilitySurfaceEngine,
                 ignite: IgniteCache,
                 pulsar: PulsarEventPublisher,
                 oracle: OracleAggregatorClient):
        self.options_engine = options_engine
        self.spot_market = spot_market
        self.vol_surface = vol_surface
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        
        self.configs: Dict[str, MarketMakingConfig] = {}
        self.positions: Dict[str, Dict[str, OptionPosition]] = {}  # mm_id -> option_id -> position
        self.hedge_positions: Dict[str, Decimal] = {}  # mm_id -> spot position
        self.stats: Dict[str, MarketMakingStats] = {}
        self.active_quotes: Dict[str, Set[str]] = {}  # mm_id -> set of quote_ids
        
        self._state: Dict[str, MMState] = {}
        self._monitoring_task: Optional[asyncio.Task] = None
        self._hedging_task: Optional[asyncio.Task] = None
        self._quoting_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start market making strategy"""
        logger.info("Starting delta-neutral options market making")
        self._monitoring_task = asyncio.create_task(self._monitor_positions())
        self._hedging_task = asyncio.create_task(self._hedge_positions())
        self._quoting_task = asyncio.create_task(self._update_quotes())
        
    async def stop(self):
        """Stop market making strategy"""
        logger.info("Stopping delta-neutral options market making")
        
        # Cancel all tasks
        for task in [self._monitoring_task, self._hedging_task, self._quoting_task]:
            if task:
                task.cancel()
                
        # Close all positions
        for mm_id in list(self._state.keys()):
            await self.stop_market_making(mm_id, close_positions=True)
            
    async def start_market_making(self,
                                  config: MarketMakingConfig,
                                  user_id: str,
                                  initial_capital: Decimal) -> str:
        """Start market making for a specific configuration"""
        mm_id = f"MM_{user_id}_{datetime.utcnow().timestamp()}"
        
        self.configs[mm_id] = config
        self.positions[mm_id] = {}
        self.hedge_positions[mm_id] = Decimal("0")
        self.stats[mm_id] = MarketMakingStats()
        self.active_quotes[mm_id] = set()
        self._state[mm_id] = MMState.ACTIVE
        
        # Initialize with some quotes
        await self._post_initial_quotes(mm_id)
        
        # Publish event
        await self.pulsar.publish('mm.started', {
            'mm_id': mm_id,
            'user_id': user_id,
            'config': config.__dict__,
            'initial_capital': float(initial_capital)
        })
        
        logger.info(f"Started market making {mm_id}")
        return mm_id
        
    async def stop_market_making(self,
                                 mm_id: str,
                                 close_positions: bool = False) -> Dict[str, Any]:
        """Stop market making and optionally close positions"""
        if mm_id not in self.configs:
            raise ValueError(f"Market maker {mm_id} not found")
            
        self._state[mm_id] = MMState.STOPPED
        
        # Cancel all quotes
        await self._cancel_all_quotes(mm_id)
        
        final_pnl = Decimal("0")
        positions_closed = 0
        
        if close_positions:
            # Close all option positions
            positions = self.positions[mm_id]
            for option_id, position in positions.items():
                if position.quantity != 0:
                    # Close position at market
                    close_price = await self._get_option_price(position)
                    pnl = self._calculate_position_pnl(position, close_price)
                    final_pnl += pnl
                    positions_closed += 1
                    
            # Close hedge position
            hedge_position = self.hedge_positions[mm_id]
            if hedge_position != 0:
                # Market order to close
                await self.spot_market.place_order(
                    user_id=self.configs[mm_id].resource_type,
                    order_type=OrderType.MARKET,
                    side=OrderSide.SELL if hedge_position > 0 else OrderSide.BUY,
                    resource_type=self.configs[mm_id].resource_type,
                    quantity=abs(hedge_position),
                    price=None
                )
                
        # Calculate final statistics
        stats = self.stats[mm_id]
        total_pnl = stats.realized_pnl + final_pnl
        
        result = {
            "mm_id": mm_id,
            "total_pnl": float(total_pnl),
            "realized_pnl": float(stats.realized_pnl),
            "fees_earned": float(stats.fees_earned),
            "hedging_costs": float(stats.hedging_costs),
            "total_volume": float(stats.total_volume),
            "total_trades": stats.total_trades,
            "positions_closed": positions_closed,
            "max_drawdown": float(stats.max_drawdown),
            "sharpe_ratio": float(stats.sharpe_ratio)
        }
        
        # Clean up
        del self.configs[mm_id]
        del self.positions[mm_id]
        del self.hedge_positions[mm_id]
        del self.stats[mm_id]
        del self.active_quotes[mm_id]
        del self._state[mm_id]
        
        # Publish event
        await self.pulsar.publish('mm.stopped', result)
        
        return result
        
    async def get_market_maker_status(self, mm_id: str) -> Dict[str, Any]:
        """Get current status of a market maker"""
        if mm_id not in self.configs:
            raise ValueError(f"Market maker {mm_id} not found")
            
        config = self.configs[mm_id]
        positions = self.positions[mm_id]
        stats = self.stats[mm_id]
        
        # Calculate current Greeks
        total_greeks = self._calculate_portfolio_greeks(mm_id)
        
        # Calculate position values
        position_value = sum(
            abs(pos.quantity * pos.current_price)
            for pos in positions.values()
        )
        
        return {
            "mm_id": mm_id,
            "state": self._state[mm_id].value,
            "config": config.__dict__,
            "portfolio_greeks": {
                "delta": float(total_greeks["delta"]),
                "gamma": float(total_greeks["gamma"]),
                "vega": float(total_greeks["vega"]),
                "theta": float(total_greeks["theta"])
            },
            "positions": {
                "options_count": len(positions),
                "net_options": sum(pos.quantity for pos in positions.values()),
                "hedge_position": float(self.hedge_positions[mm_id]),
                "position_value": float(position_value)
            },
            "active_quotes": len(self.active_quotes[mm_id]),
            "statistics": {
                "total_volume": float(stats.total_volume),
                "total_trades": stats.total_trades,
                "realized_pnl": float(stats.realized_pnl),
                "unrealized_pnl": float(stats.unrealized_pnl),
                "fees_earned": float(stats.fees_earned),
                "hedging_costs": float(stats.hedging_costs),
                "max_drawdown": float(stats.max_drawdown),
                "sharpe_ratio": float(stats.sharpe_ratio)
            }
        }
        
    # Internal methods
    
    async def _post_initial_quotes(self, mm_id: str) -> None:
        """Post initial option quotes"""
        config = self.configs[mm_id]
        
        # Get current spot price
        spot_price = await self._get_spot_price(config.resource_type)
        
        # Define strike prices around spot
        strike_range = [Decimal("0.8"), Decimal("0.9"), Decimal("1.0"), Decimal("1.1"), Decimal("1.2")]
        expiries = [7, 14, 30, 60]  # Days
        
        for strike_mult in strike_range:
            strike = spot_price * strike_mult
            
            for days in expiries:
                expiry = datetime.utcnow() + timedelta(days=days)
                
                # Quote both calls and puts
                for option_type in [OptionType.CALL, OptionType.PUT]:
                    # Calculate theoretical price
                    theo_price = await self._calculate_option_price(
                        option_type,
                        strike,
                        expiry,
                        spot_price,
                        config
                    )
                    
                    # Post two-sided quotes
                    spread = self._calculate_spread(config, spot_price, strike, expiry)
                    
                    bid_price = theo_price * (Decimal("1") - spread / 2)
                    ask_price = theo_price * (Decimal("1") + spread / 2)
                    
                    # Create bid quote
                    bid_id = await self.options_engine.post_quote(
                        user_id=mm_id,
                        option_type=option_type,
                        strike=strike,
                        expiry=expiry,
                        side=OrderSide.BUY,
                        price=bid_price,
                        quantity=config.min_quote_size,
                        resource_type=config.resource_type
                    )
                    self.active_quotes[mm_id].add(bid_id)
                    
                    # Create ask quote
                    ask_id = await self.options_engine.post_quote(
                        user_id=mm_id,
                        option_type=option_type,
                        strike=strike,
                        expiry=expiry,
                        side=OrderSide.SELL,
                        price=ask_price,
                        quantity=config.min_quote_size,
                        resource_type=config.resource_type
                    )
                    self.active_quotes[mm_id].add(ask_id)
                    
    async def _update_quotes(self) -> None:
        """Periodically update option quotes"""
        while True:
            try:
                for mm_id, state in self._state.items():
                    if state != MMState.ACTIVE:
                        continue
                        
                    config = self.configs[mm_id]
                    
                    # Cancel and repost quotes
                    await self._cancel_all_quotes(mm_id)
                    await self._post_updated_quotes(mm_id)
                    
                await asyncio.sleep(10)  # Update every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error updating quotes: {e}")
                await asyncio.sleep(10)
                
    async def _post_updated_quotes(self, mm_id: str) -> None:
        """Post updated quotes based on current market conditions"""
        config = self.configs[mm_id]
        positions = self.positions[mm_id]
        
        # Get current Greeks
        portfolio_greeks = self._calculate_portfolio_greeks(mm_id)
        
        # Adjust quotes based on inventory
        inventory_adjustment = self._calculate_inventory_adjustment(
            portfolio_greeks,
            config
        )
        
        # Similar to initial quotes but with adjustments
        spot_price = await self._get_spot_price(config.resource_type)
        
        # Get active option series
        active_series = await self.options_engine.get_active_series(config.resource_type)
        
        for series in active_series[:20]:  # Limit to 20 most liquid series
            # Calculate theoretical price
            theo_price = await self._calculate_option_price(
                series["option_type"],
                series["strike"],
                series["expiry"],
                spot_price,
                config
            )
            
            # Adjust for inventory
            adjusted_price = theo_price * (Decimal("1") + inventory_adjustment)
            
            # Calculate spread
            spread = self._calculate_spread(
                config,
                spot_price,
                series["strike"],
                series["expiry"]
            )
            
            # Adjust spread based on position limits
            if self._near_position_limit(mm_id):
                spread *= Decimal("2")  # Widen spread when near limits
                
            # Post quotes
            bid_price = adjusted_price * (Decimal("1") - spread / 2)
            ask_price = adjusted_price * (Decimal("1") + spread / 2)
            
            # Size based on risk limits
            quote_size = self._calculate_quote_size(mm_id, series, config)
            
            if quote_size > 0:
                # Post bid
                bid_id = await self.options_engine.post_quote(
                    user_id=mm_id,
                    option_type=series["option_type"],
                    strike=series["strike"],
                    expiry=series["expiry"],
                    side=OrderSide.BUY,
                    price=bid_price,
                    quantity=quote_size,
                    resource_type=config.resource_type
                )
                self.active_quotes[mm_id].add(bid_id)
                
                # Post ask
                ask_id = await self.options_engine.post_quote(
                    user_id=mm_id,
                    option_type=series["option_type"],
                    strike=series["strike"],
                    expiry=series["expiry"],
                    side=OrderSide.SELL,
                    price=ask_price,
                    quantity=quote_size,
                    resource_type=config.resource_type
                )
                self.active_quotes[mm_id].add(ask_id)
                
    async def _monitor_positions(self) -> None:
        """Monitor positions and risk metrics"""
        while True:
            try:
                for mm_id, state in self._state.items():
                    if state == MMState.STOPPED:
                        continue
                        
                    # Update position prices and Greeks
                    await self._update_position_marks(mm_id)
                    
                    # Check risk limits
                    await self._check_risk_limits(mm_id)
                    
                    # Update statistics
                    await self._update_statistics(mm_id)
                    
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring positions: {e}")
                await asyncio.sleep(30)
                
    async def _hedge_positions(self) -> None:
        """Hedge delta and other Greeks"""
        while True:
            try:
                for mm_id, state in self._state.items():
                    if state != MMState.ACTIVE:
                        continue
                        
                    config = self.configs[mm_id]
                    
                    if config.hedge_type == HedgeType.DELTA:
                        await self._hedge_delta(mm_id)
                    elif config.hedge_type == HedgeType.GAMMA:
                        await self._hedge_gamma(mm_id)
                    elif config.hedge_type == HedgeType.VEGA:
                        await self._hedge_vega(mm_id)
                    elif config.hedge_type == HedgeType.FULL:
                        await self._hedge_full(mm_id)
                        
                await asyncio.sleep(60)  # Hedge every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error hedging positions: {e}")
                await asyncio.sleep(60)
                
    async def _hedge_delta(self, mm_id: str) -> None:
        """Hedge delta exposure using spot market"""
        config = self.configs[mm_id]
        portfolio_greeks = self._calculate_portfolio_greeks(mm_id)
        
        net_delta = portfolio_greeks["delta"]
        current_hedge = self.hedge_positions[mm_id]
        
        # Calculate required hedge
        required_hedge = -net_delta  # Negative delta means we need to buy spot
        hedge_adjustment = required_hedge - current_hedge
        
        # Check if adjustment is needed
        if abs(hedge_adjustment) > config.delta_hedge_threshold:
            self._state[mm_id] = MMState.HEDGING
            
            try:
                # Place hedge order
                side = OrderSide.BUY if hedge_adjustment > 0 else OrderSide.SELL
                
                order = await self.spot_market.place_order(
                    user_id=mm_id,
                    order_type=OrderType.MARKET,
                    side=side,
                    resource_type=config.resource_type,
                    quantity=abs(hedge_adjustment),
                    price=None
                )
                
                # Update hedge position
                self.hedge_positions[mm_id] = required_hedge
                
                # Track hedging cost
                spot_price = await self._get_spot_price(config.resource_type)
                hedge_cost = abs(hedge_adjustment) * spot_price * config.taker_fee
                self.stats[mm_id].hedging_costs += hedge_cost
                
                logger.info(f"Hedged delta for {mm_id}: {hedge_adjustment} units")
                
            finally:
                self._state[mm_id] = MMState.ACTIVE
                
    async def _hedge_gamma(self, mm_id: str) -> None:
        """Hedge gamma exposure using options"""
        # Simplified - would buy/sell ATM options to reduce gamma
        pass
        
    async def _hedge_vega(self, mm_id: str) -> None:
        """Hedge vega exposure using options"""
        # Simplified - would trade options to reduce vega exposure
        pass
        
    async def _hedge_full(self, mm_id: str) -> None:
        """Full hedge including delta, gamma, and vega"""
        await self._hedge_delta(mm_id)
        await self._hedge_gamma(mm_id)
        await self._hedge_vega(mm_id)
        
    def _calculate_portfolio_greeks(self, mm_id: str) -> Dict[str, Decimal]:
        """Calculate total portfolio Greeks"""
        positions = self.positions[mm_id]
        
        total_delta = Decimal("0")
        total_gamma = Decimal("0")
        total_vega = Decimal("0")
        total_theta = Decimal("0")
        
        for position in positions.values():
            if position.quantity != 0:
                total_delta += position.greeks.delta * position.quantity
                total_gamma += position.greeks.gamma * position.quantity
                total_vega += position.greeks.vega * position.quantity
                total_theta += position.greeks.theta * position.quantity
                
        # Add hedge position to delta
        total_delta += self.hedge_positions[mm_id]
        
        return {
            "delta": total_delta,
            "gamma": total_gamma,
            "vega": total_vega,
            "theta": total_theta
        }
        
    def _calculate_spread(self,
                         config: MarketMakingConfig,
                         spot: Decimal,
                         strike: Decimal,
                         expiry: datetime) -> Decimal:
        """Calculate bid-ask spread based on market conditions"""
        base_spread = config.base_spread
        
        # Adjust for moneyness
        moneyness = strike / spot
        if moneyness < Decimal("0.9") or moneyness > Decimal("1.1"):
            base_spread *= Decimal("1.5")  # Wider spread for OTM options
            
        # Adjust for time to expiry
        days_to_expiry = (expiry - datetime.utcnow()).days
        if days_to_expiry < 7:
            base_spread *= Decimal("2")  # Wider spread near expiry
            
        # Ensure within bounds
        return max(config.min_spread, min(base_spread, config.max_spread))
        
    def _calculate_inventory_adjustment(self,
                                      portfolio_greeks: Dict[str, Decimal],
                                      config: MarketMakingConfig) -> Decimal:
        """Calculate price adjustment based on inventory"""
        # Adjust prices to reduce position
        delta_ratio = portfolio_greeks["delta"] / config.max_net_delta
        
        # Skew prices to reduce inventory
        # Positive delta -> lower ask prices, higher bid prices
        adjustment = -delta_ratio * Decimal("0.01")  # 1% adjustment per 100% of limit
        
        return adjustment
        
    def _calculate_quote_size(self,
                            mm_id: str,
                            series: Dict[str, Any],
                            config: MarketMakingConfig) -> Decimal:
        """Calculate appropriate quote size based on risk limits"""
        # Start with base size
        size = config.min_quote_size
        
        # Reduce size if near position limits
        portfolio_greeks = self._calculate_portfolio_greeks(mm_id)
        
        delta_usage = abs(portfolio_greeks["delta"]) / config.max_net_delta
        gamma_usage = abs(portfolio_greeks["gamma"]) / config.max_net_gamma
        vega_usage = abs(portfolio_greeks["vega"]) / config.max_net_vega
        
        max_usage = max(delta_usage, gamma_usage, vega_usage)
        
        if max_usage > Decimal("0.8"):
            size *= (Decimal("1") - max_usage)
            
        # Ensure within bounds
        return max(Decimal("0"), min(size, config.max_quote_size))
        
    def _near_position_limit(self, mm_id: str) -> bool:
        """Check if near any position limit"""
        config = self.configs[mm_id]
        portfolio_greeks = self._calculate_portfolio_greeks(mm_id)
        
        delta_usage = abs(portfolio_greeks["delta"]) / config.max_net_delta
        gamma_usage = abs(portfolio_greeks["gamma"]) / config.max_net_gamma
        vega_usage = abs(portfolio_greeks["vega"]) / config.max_net_vega
        
        return max(delta_usage, gamma_usage, vega_usage) > Decimal("0.9")
        
    async def _check_risk_limits(self, mm_id: str) -> None:
        """Check and enforce risk limits"""
        config = self.configs[mm_id]
        stats = self.stats[mm_id]
        
        # Check daily loss limit
        if stats.realized_pnl < -config.max_loss_per_day:
            self._state[mm_id] = MMState.PAUSED
            await self._cancel_all_quotes(mm_id)
            logger.warning(f"Daily loss limit reached for {mm_id}, pausing")
            
        # Check position limits
        portfolio_greeks = self._calculate_portfolio_greeks(mm_id)
        
        if abs(portfolio_greeks["delta"]) > config.max_net_delta * Decimal("1.2"):
            # Emergency hedge
            await self._hedge_delta(mm_id)
            
    async def _update_position_marks(self, mm_id: str) -> None:
        """Update position marks and Greeks"""
        positions = self.positions[mm_id]
        
        for option_id, position in positions.items():
            # Get current option price
            current_price = await self._get_option_price(position)
            position.current_price = current_price
            
            # Update Greeks
            greeks = await self.options_engine.calculate_greeks(
                position.option_type,
                position.strike,
                position.expiry,
                self.configs[mm_id].resource_type
            )
            position.greeks = greeks
            
            # Calculate PnL
            if position.quantity > 0:
                position.pnl = (current_price - position.entry_price) * position.quantity
            else:
                position.pnl = (position.entry_price - current_price) * abs(position.quantity)
                
    async def _update_statistics(self, mm_id: str) -> None:
        """Update performance statistics"""
        stats = self.stats[mm_id]
        positions = self.positions[mm_id]
        
        # Calculate unrealized PnL
        stats.unrealized_pnl = sum(pos.pnl for pos in positions.values())
        
        # Update max drawdown
        total_pnl = stats.realized_pnl + stats.unrealized_pnl
        if total_pnl < stats.max_drawdown:
            stats.max_drawdown = total_pnl
            
        # Calculate Sharpe ratio (simplified)
        if len(stats.daily_pnl) >= 30:
            returns = np.array([float(pnl) for pnl in stats.daily_pnl[-30:]])
            if returns.std() > 0:
                stats.sharpe_ratio = Decimal(str((returns.mean() / returns.std()) * np.sqrt(365)))
                
    async def _cancel_all_quotes(self, mm_id: str) -> None:
        """Cancel all active quotes"""
        for quote_id in list(self.active_quotes[mm_id]):
            try:
                await self.options_engine.cancel_quote(quote_id, mm_id)
                self.active_quotes[mm_id].remove(quote_id)
            except Exception as e:
                logger.error(f"Failed to cancel quote {quote_id}: {e}")
                
    async def _get_spot_price(self, resource_type: str) -> Decimal:
        """Get current spot price"""
        market_data = await self.spot_market.get_market_data(resource_type)
        return market_data.get("last_price", Decimal("0"))
        
    async def _get_option_price(self, position: OptionPosition) -> Decimal:
        """Get current option price"""
        # Get from market or calculate theoretical
        market_price = await self.options_engine.get_market_price(
            position.option_type,
            position.strike,
            position.expiry,
            self.configs[position.option_id].resource_type
        )
        
        if market_price:
            return market_price
            
        # Fall back to theoretical
        spot_price = await self._get_spot_price(
            self.configs[position.option_id].resource_type
        )
        
        return await self._calculate_option_price(
            position.option_type,
            position.strike,
            position.expiry,
            spot_price,
            self.configs[position.option_id]
        )
        
    async def _calculate_option_price(self,
                                    option_type: OptionType,
                                    strike: Decimal,
                                    expiry: datetime,
                                    spot: Decimal,
                                    config: MarketMakingConfig) -> Decimal:
        """Calculate theoretical option price"""
        # Get volatility
        if config.use_implied_vol:
            vol = await self.vol_surface.get_implied_volatility(
                config.resource_type,
                strike,
                expiry
            )
        else:
            vol = Decimal("0.5")  # Default 50% volatility
            
        # Calculate using Black-Scholes
        return await self.options_engine.calculate_price(
            option_type,
            spot,
            strike,
            expiry,
            vol,
            Decimal("0.05"),  # Risk-free rate
            config.resource_type
        )
        
    def _calculate_position_pnl(self,
                              position: OptionPosition,
                              close_price: Decimal) -> Decimal:
        """Calculate PnL for closing a position"""
        if position.quantity > 0:
            return (close_price - position.entry_price) * position.quantity
        else:
            return (position.entry_price - close_price) * abs(position.quantity) 