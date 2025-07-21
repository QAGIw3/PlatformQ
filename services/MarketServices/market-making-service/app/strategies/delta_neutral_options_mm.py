"""
Delta-Neutral Options Market Making Strategy via Trading Core

Maintains delta-neutral positions while providing liquidity in options markets.
Integrated with trading-core-service for unified order management.
"""

import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from scipy.stats import norm
from collections import defaultdict

from app.integrations.trading_core_integration import TradingCoreIntegration
from app.engines.compute_options_engine import (
    ComputeOptionsEngine,
    ComputeOption,
    ComputeOptionType,
    ExerciseStyle
)
from app.engines.pricing import BlackScholesEngine, Greeks
from app.engines.volatility_surface import VolatilitySurfaceEngine
from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient

logger = logging.getLogger(__name__)


class HedgeType(Enum):
    SPOT = "spot"              # Hedge with spot
    FUTURES = "futures"        # Hedge with futures
    OPTIONS = "options"        # Hedge with other options
    DYNAMIC = "dynamic"        # Choose best hedge dynamically


class MarketMakingMode(Enum):
    CONTINUOUS = "continuous"  # Always quote
    SELECTIVE = "selective"    # Quote based on conditions
    AUCTION = "auction"       # Only participate in auctions


@dataclass
class OptionQuote:
    """Quote for an option"""
    option_id: str
    bid_price: Optional[Decimal] = None
    bid_size: Optional[Decimal] = None
    ask_price: Optional[Decimal] = None
    ask_size: Optional[Decimal] = None
    theoretical_value: Decimal = Decimal("0")
    implied_volatility: Decimal = Decimal("0")
    delta: Decimal = Decimal("0")
    gamma: Decimal = Decimal("0")
    vega: Decimal = Decimal("0")
    theta: Decimal = Decimal("0")
    bid_order_id: Optional[str] = None
    ask_order_id: Optional[str] = None
    last_update: datetime = field(default_factory=datetime.utcnow)


@dataclass
class PositionTracker:
    """Track option positions and Greeks"""
    option_id: str
    position: Decimal  # Positive for long, negative for short
    average_price: Decimal
    current_price: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    delta: Decimal = Decimal("0")
    gamma: Decimal = Decimal("0")
    vega: Decimal = Decimal("0")
    theta: Decimal = Decimal("0")
    last_hedge_time: Optional[datetime] = None


@dataclass
class MMConfig:
    """Configuration for delta-neutral market making"""
    underlying: str
    mode: MarketMakingMode = MarketMakingMode.CONTINUOUS
    
    # Quoting parameters
    quote_width: Decimal = Decimal("0.02")  # 2% bid-ask spread
    quote_size: Decimal = Decimal("10")     # Base quote size
    max_position: Decimal = Decimal("100")  # Max position per option
    
    # Hedging parameters
    hedge_type: HedgeType = HedgeType.SPOT
    delta_threshold: Decimal = Decimal("10")    # Delta units to trigger hedge
    gamma_threshold: Decimal = Decimal("5")     # Gamma units to trigger hedge
    hedge_interval: int = 30                    # Seconds between hedge checks
    
    # Risk limits
    max_net_delta: Decimal = Decimal("50")
    max_net_gamma: Decimal = Decimal("100")
    max_net_vega: Decimal = Decimal("1000")
    max_loss: Decimal = Decimal("10000")
    
    # Volatility management
    use_vol_surface: bool = True
    vol_spread: Decimal = Decimal("0.01")  # 1% vol spread
    vol_skew_adjustment: bool = True
    
    # Inventory management
    inventory_skew: bool = True
    skew_factor: Decimal = Decimal("0.001")  # Price adjustment per unit inventory
    
    # Market making parameters
    min_edge: Decimal = Decimal("0.001")     # Minimum theoretical edge
    participation_rate: Decimal = Decimal("0.2")  # Max % of market volume
    
    # Advanced features
    pin_risk_management: bool = True
    gamma_scalping: bool = True
    vol_arbitrage: bool = True


class DeltaNeutralOptionsMM:
    """
    Delta-neutral options market making strategy integrated with trading-core
    """
    
    def __init__(self,
                 options_engine: ComputeOptionsEngine,
                 pricing_engine: BlackScholesEngine,
                 vol_surface: VolatilitySurfaceEngine,
                 ignite: IgniteCache,
                 pulsar: PulsarEventPublisher,
                 oracle: OracleAggregatorClient):
        self.options_engine = options_engine
        self.pricing_engine = pricing_engine
        self.vol_surface = vol_surface
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        
        # Trading core integration
        self.trading_core = TradingCoreIntegration()
        
        # Market making state
        self.configs: Dict[str, MMConfig] = {}
        self.quotes: Dict[str, Dict[str, OptionQuote]] = {}  # strategy_id -> option_id -> quote
        self.positions: Dict[str, Dict[str, PositionTracker]] = {}  # strategy_id -> option_id -> position
        
        # Greeks aggregation
        self.net_greeks: Dict[str, Dict[str, Decimal]] = {}  # strategy_id -> greek -> value
        
        # Market registration
        self.registered_markets: Set[str] = set()
        
        # Background tasks
        self._quoting_task: Optional[asyncio.Task] = None
        self._hedging_task: Optional[asyncio.Task] = None
        self._risk_monitoring_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start market making strategy"""
        logger.info("Starting delta-neutral options market making with trading-core")
        
        # Initialize trading core
        await self.trading_core.initialize()
        
        self._quoting_task = asyncio.create_task(self._quoting_loop())
        self._hedging_task = asyncio.create_task(self._hedging_loop())
        self._risk_monitoring_task = asyncio.create_task(self._risk_monitoring_loop())
        
    async def stop(self):
        """Stop market making strategy"""
        logger.info("Stopping delta-neutral options market making")
        
        # Cancel all tasks
        for task in [self._quoting_task, self._hedging_task, self._risk_monitoring_task]:
            if task:
                task.cancel()
                
        # Cancel all quotes
        for strategy_id in list(self.configs.keys()):
            await self.stop_strategy(strategy_id)
            
    async def start_strategy(self,
                           config: MMConfig,
                           user_id: str,
                           option_filters: Optional[Dict[str, Any]] = None) -> str:
        """Start a new market making strategy"""
        strategy_id = f"DNMM_{user_id}_{datetime.utcnow().timestamp()}"
        
        # Store config
        self.configs[strategy_id] = config
        self.quotes[strategy_id] = {}
        self.positions[strategy_id] = {}
        self.net_greeks[strategy_id] = {
            "delta": Decimal("0"),
            "gamma": Decimal("0"),
            "vega": Decimal("0"),
            "theta": Decimal("0")
        }
        
        # Find options to quote
        options = await self._find_options_to_quote(config, option_filters)
        
        # Initialize quotes for each option
        for option in options:
            await self._initialize_option_quote(strategy_id, option)
            
        # Publish event
        await self.pulsar.publish('mm.strategy.started', {
            'strategy_id': strategy_id,
            'user_id': user_id,
            'underlying': config.underlying,
            'options_count': len(options)
        })
        
        logger.info(f"Started delta-neutral MM strategy {strategy_id} for {len(options)} options")
        
        return strategy_id
        
    async def stop_strategy(self, strategy_id: str):
        """Stop a market making strategy"""
        if strategy_id not in self.configs:
            return
            
        # Cancel all quotes
        await self._cancel_all_quotes(strategy_id)
        
        # Liquidate positions if any
        positions = self.positions.get(strategy_id, {})
        if positions:
            await self._liquidate_positions(strategy_id)
            
        # Clean up
        del self.configs[strategy_id]
        del self.quotes[strategy_id]
        del self.positions[strategy_id]
        del self.net_greeks[strategy_id]
        
        logger.info(f"Stopped delta-neutral MM strategy {strategy_id}")
        
    async def _find_options_to_quote(self,
                                   config: MMConfig,
                                   filters: Optional[Dict[str, Any]] = None) -> List[ComputeOption]:
        """Find options that match our criteria"""
        all_options = []
        
        # Get all active options for the underlying
        for option_id, option in self.options_engine.options.items():
            if option.underlying != config.underlying:
                continue
                
            if option.is_expired:
                continue
                
            # Apply filters if provided
            if filters:
                if "min_days_to_expiry" in filters:
                    if option.time_to_expiry * 365.25 < filters["min_days_to_expiry"]:
                        continue
                        
                if "max_days_to_expiry" in filters:
                    if option.time_to_expiry * 365.25 > filters["max_days_to_expiry"]:
                        continue
                        
                if "strike_range" in filters:
                    # Get spot price
                    spot_price = await self._get_spot_price(config.underlying)
                    min_strike = spot_price * (1 - filters["strike_range"])
                    max_strike = spot_price * (1 + filters["strike_range"])
                    
                    if option.strike_price < min_strike or option.strike_price > max_strike:
                        continue
                        
            all_options.append(option)
            
        return all_options
        
    async def _get_spot_price(self, underlying: str) -> Decimal:
        """Get spot price from trading-core"""
        market_id = f"COMPUTE_SPOT_{underlying}_global"
        orderbook = await self.trading_core.get_orderbook(market_id, depth=1)
        
        if orderbook and orderbook.get("bids") and orderbook.get("asks"):
            best_bid = Decimal(orderbook["bids"][0]["price"])
            best_ask = Decimal(orderbook["asks"][0]["price"])
            return (best_bid + best_ask) / 2
        else:
            # Fallback to oracle
            oracle_price = await self.oracle.get_aggregated_price(f"COMPUTE_{underlying}")
            return oracle_price.price if oracle_price else Decimal("100")
            
    async def _initialize_option_quote(self, strategy_id: str, option: ComputeOption):
        """Initialize quote for an option"""
        # Get theoretical value and Greeks
        spot_price = await self._get_spot_price(option.underlying)
        
        # Get implied volatility from surface
        if self.configs[strategy_id].use_vol_surface:
            iv = await self.vol_surface.get_implied_volatility(
                option.underlying,
                option.strike_price,
                option.time_to_expiry
            )
        else:
            iv = Decimal("0.5")  # Default 50% vol
            
        # Calculate theoretical value
        params = {
            "spot": float(spot_price),
            "strike": float(option.strike_price),
            "time_to_expiry": option.time_to_expiry,
            "volatility": float(iv),
            "risk_free_rate": 0.05,
            "is_call": option.option_type == ComputeOptionType.CALL
        }
        
        theo_value = self.pricing_engine.calculate_option_price(**params)
        greeks = self.pricing_engine.calculate_greeks(**params)
        
        # Create quote
        quote = OptionQuote(
            option_id=option.option_id,
            theoretical_value=Decimal(str(theo_value)),
            implied_volatility=iv,
            delta=Decimal(str(greeks.delta)),
            gamma=Decimal(str(greeks.gamma)),
            vega=Decimal(str(greeks.vega)),
            theta=Decimal(str(greeks.theta))
        )
        
        self.quotes[strategy_id][option.option_id] = quote
        
    async def _quoting_loop(self):
        """Main loop for updating and maintaining quotes"""
        while True:
            try:
                for strategy_id, config in list(self.configs.items()):
                    if config.mode == MarketMakingMode.CONTINUOUS:
                        await self._update_quotes(strategy_id)
                    elif config.mode == MarketMakingMode.SELECTIVE:
                        await self._update_selective_quotes(strategy_id)
                        
                await asyncio.sleep(1)  # Update every second
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in quoting loop: {e}")
                await asyncio.sleep(5)
                
    async def _update_quotes(self, strategy_id: str):
        """Update quotes for all options in strategy"""
        config = self.configs[strategy_id]
        quotes = self.quotes[strategy_id]
        
        # Get current spot price
        spot_price = await self._get_spot_price(config.underlying)
        
        for option_id, quote in quotes.items():
            option = self.options_engine.options.get(option_id)
            if not option or option.is_expired:
                continue
                
            # Update theoretical value and Greeks
            await self._update_option_pricing(strategy_id, option, spot_price)
            
            # Calculate bid/ask prices
            bid_price, ask_price = self._calculate_quote_prices(strategy_id, option_id)
            
            # Update quote
            quote.bid_price = bid_price
            quote.ask_price = ask_price
            quote.bid_size = config.quote_size
            quote.ask_size = config.quote_size
            
            # Send orders to trading-core
            await self._update_option_orders(strategy_id, option_id)
            
    async def _update_option_pricing(self,
                                   strategy_id: str,
                                   option: ComputeOption,
                                   spot_price: Decimal):
        """Update theoretical pricing for an option"""
        quote = self.quotes[strategy_id][option.option_id]
        config = self.configs[strategy_id]
        
        # Get implied volatility
        if config.use_vol_surface:
            iv = await self.vol_surface.get_implied_volatility(
                option.underlying,
                option.strike_price,
                option.time_to_expiry
            )
        else:
            iv = quote.implied_volatility
            
        # Calculate theoretical value
        params = {
            "spot": float(spot_price),
            "strike": float(option.strike_price),
            "time_to_expiry": option.time_to_expiry,
            "volatility": float(iv),
            "risk_free_rate": 0.05,
            "is_call": option.option_type == ComputeOptionType.CALL
        }
        
        theo_value = self.pricing_engine.calculate_option_price(**params)
        greeks = self.pricing_engine.calculate_greeks(**params)
        
        # Update quote
        quote.theoretical_value = Decimal(str(theo_value))
        quote.implied_volatility = iv
        quote.delta = Decimal(str(greeks.delta)) * option.contract_size
        quote.gamma = Decimal(str(greeks.gamma)) * option.contract_size
        quote.vega = Decimal(str(greeks.vega)) * option.contract_size
        quote.theta = Decimal(str(greeks.theta)) * option.contract_size
        quote.last_update = datetime.utcnow()
        
    def _calculate_quote_prices(self,
                               strategy_id: str,
                               option_id: str) -> Tuple[Decimal, Decimal]:
        """Calculate bid and ask prices with inventory adjustment"""
        config = self.configs[strategy_id]
        quote = self.quotes[strategy_id][option_id]
        
        # Base spread
        half_spread = quote.theoretical_value * config.quote_width / 2
        
        # Volatility spread adjustment
        if config.use_vol_surface:
            vol_adjustment = quote.theoretical_value * config.vol_spread
            half_spread += vol_adjustment
            
        # Inventory skew adjustment
        if config.inventory_skew and option_id in self.positions[strategy_id]:
            position = self.positions[strategy_id][option_id]
            inventory_adjustment = position.position * config.skew_factor
            
            # Adjust prices to reduce inventory
            bid_price = quote.theoretical_value - half_spread - inventory_adjustment
            ask_price = quote.theoretical_value + half_spread - inventory_adjustment
        else:
            bid_price = quote.theoretical_value - half_spread
            ask_price = quote.theoretical_value + half_spread
            
        # Ensure minimum edge
        theo_mid = (bid_price + ask_price) / 2
        if abs(theo_mid - quote.theoretical_value) < config.min_edge:
            # Widen spread to maintain edge
            extra_spread = config.min_edge - abs(theo_mid - quote.theoretical_value)
            bid_price -= extra_spread / 2
            ask_price += extra_spread / 2
            
        return max(Decimal("0.01"), bid_price), ask_price
        
    async def _update_option_orders(self, strategy_id: str, option_id: str):
        """Update orders for an option via trading-core"""
        config = self.configs[strategy_id]
        quote = self.quotes[strategy_id][option_id]
        
        # Ensure option market is registered
        market_id = await self._ensure_option_market_registered(option_id)
        
        # Cancel existing orders if prices changed significantly
        if quote.bid_order_id:
            # Check if we need to update
            order_status = await self.trading_core.get_order_status(quote.bid_order_id)
            if order_status and abs(Decimal(order_status.get("price", "0")) - quote.bid_price) > Decimal("0.001"):
                await self.trading_core.cancel_order(quote.bid_order_id)
                quote.bid_order_id = None
                
        if quote.ask_order_id:
            order_status = await self.trading_core.get_order_status(quote.ask_order_id)
            if order_status and abs(Decimal(order_status.get("price", "0")) - quote.ask_price) > Decimal("0.001"):
                await self.trading_core.cancel_order(quote.ask_order_id)
                quote.ask_order_id = None
                
        # Place new orders if needed
        if not quote.bid_order_id and quote.bid_price and quote.bid_size:
            result = await self.trading_core.submit_derivatives_order(
                user_id=f"MM_{strategy_id}",
                market_id=market_id,
                side="buy",
                quantity=str(quote.bid_size),
                order_type="limit",
                price=str(quote.bid_price),
                metadata={
                    "strategy_id": strategy_id,
                    "option_id": option_id,
                    "order_type": "quote_bid",
                    "theoretical_value": str(quote.theoretical_value)
                }
            )
            
            if result.get("success"):
                quote.bid_order_id = result.get("order_id")
                
        if not quote.ask_order_id and quote.ask_price and quote.ask_size:
            result = await self.trading_core.submit_derivatives_order(
                user_id=f"MM_{strategy_id}",
                market_id=market_id,
                side="sell",
                quantity=str(quote.ask_size),
                order_type="limit",
                price=str(quote.ask_price),
                metadata={
                    "strategy_id": strategy_id,
                    "option_id": option_id,
                    "order_type": "quote_ask",
                    "theoretical_value": str(quote.theoretical_value)
                }
            )
            
            if result.get("success"):
                quote.ask_order_id = result.get("order_id")
                
    async def _ensure_option_market_registered(self, option_id: str) -> str:
        """Ensure option market is registered with trading-core"""
        option = self.options_engine.options.get(option_id)
        if not option:
            raise ValueError(f"Option {option_id} not found")
            
        market_id = f"OPTION_{option.underlying}_{option.strike_price}_{option.expiry.strftime('%Y%m%d')}_{option.option_type.value}"
        
        if market_id not in self.registered_markets:
            # Let the options engine handle registration
            await self.options_engine._register_option_market(option)
            self.registered_markets.add(market_id)
            
        return market_id
        
    async def _hedging_loop(self):
        """Main loop for delta hedging"""
        while True:
            try:
                for strategy_id in list(self.configs.keys()):
                    await self._check_and_hedge(strategy_id)
                    
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in hedging loop: {e}")
                await asyncio.sleep(10)
                
    async def _check_and_hedge(self, strategy_id: str):
        """Check Greeks and hedge if necessary"""
        config = self.configs[strategy_id]
        
        # Calculate net Greeks
        await self._calculate_net_greeks(strategy_id)
        net_greeks = self.net_greeks[strategy_id]
        
        # Check if hedging is needed
        if abs(net_greeks["delta"]) > config.delta_threshold:
            await self._hedge_delta(strategy_id, net_greeks["delta"])
            
        if config.gamma_scalping and abs(net_greeks["gamma"]) > config.gamma_threshold:
            await self._hedge_gamma(strategy_id, net_greeks["gamma"])
            
    async def _calculate_net_greeks(self, strategy_id: str):
        """Calculate net Greeks across all positions"""
        positions = self.positions.get(strategy_id, {})
        quotes = self.quotes.get(strategy_id, {})
        
        net_delta = Decimal("0")
        net_gamma = Decimal("0")
        net_vega = Decimal("0")
        net_theta = Decimal("0")
        
        for option_id, position in positions.items():
            if option_id in quotes:
                quote = quotes[option_id]
                net_delta += position.position * quote.delta
                net_gamma += position.position * quote.gamma
                net_vega += position.position * quote.vega
                net_theta += position.position * quote.theta
                
        self.net_greeks[strategy_id] = {
            "delta": net_delta,
            "gamma": net_gamma,
            "vega": net_vega,
            "theta": net_theta
        }
        
    async def _hedge_delta(self, strategy_id: str, net_delta: Decimal):
        """Hedge delta exposure"""
        config = self.configs[strategy_id]
        
        # Determine hedge size and direction
        hedge_size = abs(net_delta)
        hedge_side = "sell" if net_delta > 0 else "buy"
        
        logger.info(f"Hedging delta for {strategy_id}: {hedge_side} {hedge_size} units")
        
        # Execute hedge based on type
        if config.hedge_type == HedgeType.SPOT:
            # Hedge with spot via trading-core
            result = await self.trading_core.submit_compute_order(
                user_id=f"MM_{strategy_id}",
                resource_type=config.underlying,
                market_type="spot",
                quantity=str(hedge_size),
                specifications={
                    "side": hedge_side,
                    "order_type": "market",
                    "hedge_type": "delta",
                    "strategy_id": strategy_id,
                    "net_delta": str(net_delta)
                }
            )
            
            if result.get("success"):
                logger.info(f"Delta hedge executed: {result}")
            else:
                logger.error(f"Failed to execute delta hedge: {result}")
                
        elif config.hedge_type == HedgeType.FUTURES:
            # Hedge with futures
            # Implementation would go here
            pass
            
    async def _hedge_gamma(self, strategy_id: str, net_gamma: Decimal):
        """Hedge gamma exposure with options"""
        # This is more complex - would need to find appropriate options
        # to reduce gamma exposure
        logger.info(f"Gamma hedging needed for {strategy_id}: {net_gamma}")
        # Implementation would go here
        
    async def _risk_monitoring_loop(self):
        """Monitor risk limits and P&L"""
        while True:
            try:
                for strategy_id in list(self.configs.keys()):
                    await self._check_risk_limits(strategy_id)
                    await self._update_pnl(strategy_id)
                    
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in risk monitoring: {e}")
                await asyncio.sleep(30)
                
    async def _check_risk_limits(self, strategy_id: str):
        """Check if risk limits are breached"""
        config = self.configs[strategy_id]
        net_greeks = self.net_greeks.get(strategy_id, {})
        
        # Check Greek limits
        if abs(net_greeks.get("delta", 0)) > config.max_net_delta:
            logger.warning(f"Delta limit breached for {strategy_id}")
            await self._reduce_quotes(strategy_id)
            
        if abs(net_greeks.get("gamma", 0)) > config.max_net_gamma:
            logger.warning(f"Gamma limit breached for {strategy_id}")
            await self._reduce_quotes(strategy_id)
            
        if abs(net_greeks.get("vega", 0)) > config.max_net_vega:
            logger.warning(f"Vega limit breached for {strategy_id}")
            await self._reduce_quotes(strategy_id)
            
    async def _update_pnl(self, strategy_id: str):
        """Update P&L for all positions"""
        positions = self.positions.get(strategy_id, {})
        quotes = self.quotes.get(strategy_id, {})
        
        total_pnl = Decimal("0")
        
        for option_id, position in positions.items():
            if option_id in quotes:
                current_price = quotes[option_id].theoretical_value
                position.current_price = current_price
                position.unrealized_pnl = (current_price - position.average_price) * position.position
                total_pnl += position.unrealized_pnl
                
        # Check max loss
        config = self.configs[strategy_id]
        if total_pnl < -config.max_loss:
            logger.error(f"Max loss breached for {strategy_id}: {total_pnl}")
            await self.stop_strategy(strategy_id)
            
    async def _reduce_quotes(self, strategy_id: str):
        """Reduce quote sizes when risk limits are approached"""
        config = self.configs[strategy_id]
        config.quote_size = config.quote_size * Decimal("0.5")
        logger.info(f"Reduced quote size for {strategy_id} to {config.quote_size}")
        
    async def _cancel_all_quotes(self, strategy_id: str):
        """Cancel all quotes for a strategy"""
        quotes = self.quotes.get(strategy_id, {})
        
        cancel_tasks = []
        for quote in quotes.values():
            if quote.bid_order_id:
                cancel_tasks.append(
                    self.trading_core.cancel_order(quote.bid_order_id)
                )
            if quote.ask_order_id:
                cancel_tasks.append(
                    self.trading_core.cancel_order(quote.ask_order_id)
                )
                
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
            
    async def _liquidate_positions(self, strategy_id: str):
        """Liquidate all positions for a strategy"""
        positions = self.positions.get(strategy_id, {})
        
        for option_id, position in positions.items():
            if position.position != 0:
                # Submit market order to close position
                side = "sell" if position.position > 0 else "buy"
                quantity = abs(position.position)
                
                market_id = await self._ensure_option_market_registered(option_id)
                
                result = await self.trading_core.submit_derivatives_order(
                    user_id=f"MM_{strategy_id}",
                    market_id=market_id,
                    side=side,
                    quantity=str(quantity),
                    order_type="market",
                    metadata={
                        "strategy_id": strategy_id,
                        "liquidation": True
                    }
                )
                
                if result.get("success"):
                    logger.info(f"Liquidated position in {option_id}: {result}")
                else:
                    logger.error(f"Failed to liquidate {option_id}: {result}")
                    
    async def handle_fill(self, strategy_id: str, order_id: str, fill_data: Dict[str, Any]):
        """Handle order fill notification"""
        # Find which option this fill is for
        quotes = self.quotes.get(strategy_id, {})
        
        for option_id, quote in quotes.items():
            if quote.bid_order_id == order_id or quote.ask_order_id == order_id:
                # Update position
                side = "buy" if quote.bid_order_id == order_id else "sell"
                quantity = Decimal(fill_data.get("quantity", "0"))
                price = Decimal(fill_data.get("price", "0"))
                
                await self._update_position(strategy_id, option_id, side, quantity, price)
                
                # Clear order ID
                if quote.bid_order_id == order_id:
                    quote.bid_order_id = None
                else:
                    quote.ask_order_id = None
                    
                break
                
    async def _update_position(self,
                             strategy_id: str,
                             option_id: str,
                             side: str,
                             quantity: Decimal,
                             price: Decimal):
        """Update position after a fill"""
        positions = self.positions.setdefault(strategy_id, {})
        
        if option_id not in positions:
            positions[option_id] = PositionTracker(
                option_id=option_id,
                position=Decimal("0"),
                average_price=Decimal("0")
            )
            
        position = positions[option_id]
        
        # Update position
        if side == "buy":
            # Calculate new average price
            total_value = position.position * position.average_price + quantity * price
            position.position += quantity
            position.average_price = total_value / position.position if position.position > 0 else price
        else:
            position.position -= quantity
            
        # Update Greeks from quote
        if option_id in self.quotes[strategy_id]:
            quote = self.quotes[strategy_id][option_id]
            position.delta = quote.delta
            position.gamma = quote.gamma
            position.vega = quote.vega
            position.theta = quote.theta
            
        logger.info(f"Updated position for {option_id}: {position.position} @ {position.average_price}")
        
    async def get_strategy_status(self, strategy_id: str) -> Dict[str, Any]:
        """Get current status of a strategy"""
        if strategy_id not in self.configs:
            raise ValueError(f"Strategy {strategy_id} not found")
            
        config = self.configs[strategy_id]
        quotes = self.quotes[strategy_id]
        positions = self.positions.get(strategy_id, {})
        net_greeks = self.net_greeks.get(strategy_id, {})
        
        # Calculate P&L
        total_pnl = sum(p.unrealized_pnl for p in positions.values())
        
        # Count active quotes
        active_bids = sum(1 for q in quotes.values() if q.bid_order_id)
        active_asks = sum(1 for q in quotes.values() if q.ask_order_id)
        
        return {
            "strategy_id": strategy_id,
            "underlying": config.underlying,
            "mode": config.mode.value,
            "quotes": {
                "total_options": len(quotes),
                "active_bids": active_bids,
                "active_asks": active_asks
            },
            "positions": {
                "count": len(positions),
                "total_contracts": sum(abs(p.position) for p in positions.values())
            },
            "greeks": {
                "net_delta": str(net_greeks.get("delta", 0)),
                "net_gamma": str(net_greeks.get("gamma", 0)),
                "net_vega": str(net_greeks.get("vega", 0)),
                "net_theta": str(net_greeks.get("theta", 0))
            },
            "pnl": {
                "unrealized": str(total_pnl),
                "max_loss_limit": str(config.max_loss)
            },
            "risk_limits": {
                "max_net_delta": str(config.max_net_delta),
                "max_net_gamma": str(config.max_net_gamma),
                "max_net_vega": str(config.max_net_vega)
            }
        } 