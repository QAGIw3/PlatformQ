"""
Cross-Market Arbitrage Bot for Compute Markets

Implements arbitrage strategies across spot, futures, and options markets:
- Cash and carry arbitrage (spot vs futures)
- Put-call parity arbitrage
- Box spread arbitrage
- Cross-exchange arbitrage
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

from app.engines.compute_spot_market import ComputeSpotMarket, OrderType, OrderSide
from app.engines.compute_futures_engine import ComputeFuturesEngine
from app.engines.compute_options_engine import ComputeOptionsEngine, OptionType
from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient

logger = logging.getLogger(__name__)


class ArbitrageType(Enum):
    CASH_CARRY = "cash_and_carry"
    PUT_CALL_PARITY = "put_call_parity"
    BOX_SPREAD = "box_spread"
    CALENDAR_BASIS = "calendar_basis"
    CROSS_EXCHANGE = "cross_exchange"
    TRIANGULAR = "triangular"


class ArbitrageSignal:
    """Represents an arbitrage opportunity"""
    def __init__(self,
                 signal_id: str,
                 arb_type: ArbitrageType,
                 resource_type: str,
                 expected_profit: Decimal,
                 required_capital: Decimal,
                 confidence: Decimal,
                 expiry_time: datetime,
                 legs: List[Dict[str, Any]]):
        self.signal_id = signal_id
        self.arb_type = arb_type
        self.resource_type = resource_type
        self.expected_profit = expected_profit
        self.required_capital = required_capital
        self.confidence = confidence
        self.expiry_time = expiry_time
        self.legs = legs  # Description of each leg of the trade
        self.timestamp = datetime.utcnow()
        self.executed = False
        
    @property
    def return_on_capital(self) -> Decimal:
        """Calculate expected return on capital"""
        if self.required_capital > 0:
            return self.expected_profit / self.required_capital
        return Decimal("0")
        
    @property
    def is_valid(self) -> bool:
        """Check if signal is still valid"""
        return datetime.utcnow() < self.expiry_time and not self.executed


@dataclass
class ArbitragePosition:
    """Active arbitrage position"""
    position_id: str
    signal: ArbitrageSignal
    entry_time: datetime
    
    # Position legs
    spot_positions: Dict[str, Decimal] = field(default_factory=dict)
    futures_positions: List[str] = field(default_factory=list)
    options_positions: List[str] = field(default_factory=list)
    
    # Financial metrics
    entry_cost: Decimal = Decimal("0")
    current_value: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    fees_paid: Decimal = Decimal("0")
    
    # Status
    is_closed: bool = False
    close_reason: Optional[str] = None


@dataclass
class CrossMarketArbConfig:
    """Configuration for cross-market arbitrage"""
    # Capital allocation
    max_capital_per_trade: Decimal = Decimal("100000")
    max_total_capital: Decimal = Decimal("1000000")
    
    # Risk parameters
    min_profit_threshold: Decimal = Decimal("100")  # Minimum $100 profit
    min_return_threshold: Decimal = Decimal("0.001")  # 0.1% minimum return
    max_execution_time: int = 60  # seconds
    
    # Execution parameters
    use_limit_orders: bool = False  # Use market orders for speed
    max_slippage: Decimal = Decimal("0.002")  # 0.2% max slippage
    partial_fill_threshold: Decimal = Decimal("0.8")  # Accept 80% fill
    
    # Market parameters
    futures_months: List[int] = field(default_factory=lambda: [1, 3, 6])  # Months ahead
    option_strikes: List[Decimal] = field(default_factory=lambda: [
        Decimal("0.8"), Decimal("0.9"), Decimal("1.0"), Decimal("1.1"), Decimal("1.2")
    ])  # Strike multipliers
    
    # Fee structure
    spot_fee: Decimal = Decimal("0.001")  # 0.1%
    futures_fee: Decimal = Decimal("0.0005")  # 0.05%
    options_fee: Decimal = Decimal("0.0015")  # 0.15%
    
    # Advanced features
    enable_cross_exchange: bool = False
    exchanges: List[str] = field(default_factory=list)
    enable_triangular: bool = True


@dataclass
class ArbStatistics:
    """Statistics for arbitrage performance"""
    signals_generated: int = 0
    positions_opened: int = 0
    positions_closed: int = 0
    successful_arbs: int = 0
    
    total_profit: Decimal = Decimal("0")
    total_volume: Decimal = Decimal("0")
    total_fees: Decimal = Decimal("0")
    
    avg_profit_per_trade: Decimal = Decimal("0")
    avg_execution_time: timedelta = timedelta()
    success_rate: Decimal = Decimal("0")
    
    by_type: Dict[ArbitrageType, Dict[str, Any]] = field(default_factory=dict)


class CrossMarketArbitrage:
    """
    Cross-market arbitrage bot for compute resources
    """
    
    def __init__(self,
                 spot_market: ComputeSpotMarket,
                 futures_engine: ComputeFuturesEngine,
                 options_engine: ComputeOptionsEngine,
                 ignite: IgniteCache,
                 pulsar: PulsarEventPublisher,
                 oracle: OracleAggregatorClient):
        self.spot_market = spot_market
        self.futures_engine = futures_engine
        self.options_engine = options_engine
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        
        self.configs: Dict[str, CrossMarketArbConfig] = {}
        self.active_positions: Dict[str, Dict[str, ArbitragePosition]] = {}
        self.pending_signals: Dict[str, List[ArbitrageSignal]] = {}
        self.statistics: Dict[str, ArbStatistics] = {}
        
        # Market data cache
        self._price_cache: Dict[str, Dict[str, Any]] = {}
        self._last_update: Dict[str, datetime] = {}
        
        self._monitoring_task: Optional[asyncio.Task] = None
        self._signal_task: Optional[asyncio.Task] = None
        self._execution_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start arbitrage bot"""
        logger.info("Starting cross-market arbitrage bot")
        self._monitoring_task = asyncio.create_task(self._monitor_positions())
        self._signal_task = asyncio.create_task(self._scan_for_arbitrage())
        self._execution_task = asyncio.create_task(self._execute_arbitrage())
        
    async def stop(self):
        """Stop arbitrage bot"""
        logger.info("Stopping cross-market arbitrage bot")
        
        for task in [self._monitoring_task, self._signal_task, self._execution_task]:
            if task:
                task.cancel()
                
        # Close all positions
        for bot_id in list(self.active_positions.keys()):
            await self.close_all_positions(bot_id)
            
    async def create_bot(self,
                        config: CrossMarketArbConfig,
                        resource_types: List[str],
                        user_id: str) -> str:
        """Create a new arbitrage bot instance"""
        bot_id = f"XARB_{user_id}_{datetime.utcnow().timestamp()}"
        
        self.configs[bot_id] = config
        self.active_positions[bot_id] = {}
        self.pending_signals[bot_id] = []
        self.statistics[bot_id] = ArbStatistics()
        
        # Initialize price cache for resource types
        for resource_type in resource_types:
            if resource_type not in self._price_cache:
                self._price_cache[resource_type] = {}
                
        logger.info(f"Created cross-market arbitrage bot {bot_id}")
        return bot_id
        
    async def get_bot_status(self, bot_id: str) -> Dict[str, Any]:
        """Get current status of arbitrage bot"""
        if bot_id not in self.configs:
            raise ValueError(f"Bot {bot_id} not found")
            
        config = self.configs[bot_id]
        positions = self.active_positions[bot_id]
        stats = self.statistics[bot_id]
        signals = self.pending_signals[bot_id]
        
        # Calculate capital usage
        capital_used = sum(pos.entry_cost for pos in positions.values() if not pos.is_closed)
        
        return {
            "bot_id": bot_id,
            "active_positions": len([p for p in positions.values() if not p.is_closed]),
            "pending_signals": len([s for s in signals if s.is_valid]),
            "capital_used": float(capital_used),
            "capital_available": float(config.max_total_capital - capital_used),
            "statistics": {
                "total_profit": float(stats.total_profit),
                "positions_opened": stats.positions_opened,
                "success_rate": float(stats.success_rate),
                "avg_profit": float(stats.avg_profit_per_trade),
                "total_volume": float(stats.total_volume)
            },
            "performance_by_type": {
                arb_type.value: {
                    "count": data.get("count", 0),
                    "profit": float(data.get("profit", 0)),
                    "success_rate": float(data.get("success_rate", 0))
                }
                for arb_type, data in stats.by_type.items()
            }
        }
        
    # Arbitrage scanning methods
    
    async def _scan_for_arbitrage(self) -> None:
        """Continuously scan for arbitrage opportunities"""
        while True:
            try:
                for bot_id, config in self.configs.items():
                    # Update market data
                    await self._update_market_data()
                    
                    # Check each arbitrage type
                    signals = []
                    
                    # Cash and carry arbitrage
                    cash_carry_signals = await self._find_cash_carry_arb(bot_id)
                    signals.extend(cash_carry_signals)
                    
                    # Put-call parity arbitrage
                    pcp_signals = await self._find_put_call_parity_arb(bot_id)
                    signals.extend(pcp_signals)
                    
                    # Box spread arbitrage
                    box_signals = await self._find_box_spread_arb(bot_id)
                    signals.extend(box_signals)
                    
                    # Cross-exchange arbitrage
                    if config.enable_cross_exchange:
                        cross_signals = await self._find_cross_exchange_arb(bot_id)
                        signals.extend(cross_signals)
                        
                    # Filter and rank signals
                    valid_signals = [s for s in signals if self._validate_signal(s, config)]
                    ranked_signals = sorted(valid_signals, key=lambda x: x.return_on_capital, reverse=True)
                    
                    # Store top signals
                    self.pending_signals[bot_id] = ranked_signals[:20]
                    
                await asyncio.sleep(5)  # Scan every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error scanning for arbitrage: {e}")
                await asyncio.sleep(5)
                
    async def _find_cash_carry_arb(self, bot_id: str) -> List[ArbitrageSignal]:
        """Find cash and carry arbitrage between spot and futures"""
        config = self.configs[bot_id]
        signals = []
        
        for resource_type in self._price_cache.keys():
            spot_price = await self._get_spot_price(resource_type)
            
            # Check each futures contract
            for months_ahead in config.futures_months:
                expiry = datetime.utcnow() + timedelta(days=30 * months_ahead)
                futures_price = await self._get_futures_price(resource_type, expiry)
                
                if not futures_price:
                    continue
                    
                # Calculate basis
                basis = futures_price - spot_price
                days_to_expiry = (expiry - datetime.utcnow()).days
                
                # Annualized basis
                annualized_basis = (basis / spot_price) * (365 / days_to_expiry)
                
                # Cost of carry (simplified - storage + financing)
                risk_free_rate = Decimal("0.05")  # 5% annual
                storage_cost = Decimal("0.02")    # 2% annual
                cost_of_carry = (risk_free_rate + storage_cost) * (days_to_expiry / 365)
                
                # Check for arbitrage
                expected_futures = spot_price * (Decimal("1") + cost_of_carry)
                
                if futures_price > expected_futures * (Decimal("1") + config.min_return_threshold):
                    # Sell futures, buy spot
                    profit = (futures_price - expected_futures) * Decimal("100")  # 100 units
                    
                    signal = ArbitrageSignal(
                        signal_id=f"CC_SELL_{bot_id}_{datetime.utcnow().timestamp()}",
                        arb_type=ArbitrageType.CASH_CARRY,
                        resource_type=resource_type,
                        expected_profit=profit - self._calculate_fees(profit, config),
                        required_capital=spot_price * Decimal("100"),
                        confidence=Decimal("0.9"),
                        expiry_time=datetime.utcnow() + timedelta(minutes=5),
                        legs=[
                            {"market": "spot", "side": "buy", "quantity": 100, "price": spot_price},
                            {"market": "futures", "side": "sell", "quantity": 100, "price": futures_price}
                        ]
                    )
                    signals.append(signal)
                    
                elif futures_price < expected_futures * (Decimal("1") - config.min_return_threshold):
                    # Buy futures, sell spot (if possible)
                    profit = (expected_futures - futures_price) * Decimal("100")
                    
                    signal = ArbitrageSignal(
                        signal_id=f"CC_BUY_{bot_id}_{datetime.utcnow().timestamp()}",
                        arb_type=ArbitrageType.CASH_CARRY,
                        resource_type=resource_type,
                        expected_profit=profit - self._calculate_fees(profit, config),
                        required_capital=futures_price * Decimal("100"),
                        confidence=Decimal("0.85"),
                        expiry_time=datetime.utcnow() + timedelta(minutes=5),
                        legs=[
                            {"market": "spot", "side": "sell", "quantity": 100, "price": spot_price},
                            {"market": "futures", "side": "buy", "quantity": 100, "price": futures_price}
                        ]
                    )
                    signals.append(signal)
                    
        return signals
        
    async def _find_put_call_parity_arb(self, bot_id: str) -> List[ArbitrageSignal]:
        """Find put-call parity arbitrage opportunities"""
        config = self.configs[bot_id]
        signals = []
        
        for resource_type in self._price_cache.keys():
            spot_price = await self._get_spot_price(resource_type)
            
            # Get option chains
            option_chain = await self.options_engine.get_option_chain(resource_type)
            
            for expiry, strikes in option_chain.items():
                for strike in strikes:
                    if strike not in config.option_strikes:
                        continue
                        
                    # Get call and put prices
                    call_price = await self._get_option_price(
                        resource_type, OptionType.CALL, strike, expiry
                    )
                    put_price = await self._get_option_price(
                        resource_type, OptionType.PUT, strike, expiry
                    )
                    
                    if not call_price or not put_price:
                        continue
                        
                    # Put-call parity: C - P = S - K * e^(-rt)
                    days_to_expiry = (expiry - datetime.utcnow()).days
                    discount_factor = Decimal(str(np.exp(-0.05 * days_to_expiry / 365)))
                    
                    theoretical_diff = spot_price - strike * discount_factor
                    actual_diff = call_price - put_price
                    
                    parity_violation = abs(actual_diff - theoretical_diff)
                    
                    if parity_violation > spot_price * config.min_return_threshold:
                        # Determine arbitrage direction
                        if actual_diff > theoretical_diff:
                            # Sell call, buy put, buy spot, borrow K
                            legs = [
                                {"market": "options", "type": "call", "side": "sell", "strike": strike, "price": call_price},
                                {"market": "options", "type": "put", "side": "buy", "strike": strike, "price": put_price},
                                {"market": "spot", "side": "buy", "quantity": 100, "price": spot_price}
                            ]
                        else:
                            # Buy call, sell put, sell spot, lend K
                            legs = [
                                {"market": "options", "type": "call", "side": "buy", "strike": strike, "price": call_price},
                                {"market": "options", "type": "put", "side": "sell", "strike": strike, "price": put_price},
                                {"market": "spot", "side": "sell", "quantity": 100, "price": spot_price}
                            ]
                            
                        profit = parity_violation * Decimal("100")
                        
                        signal = ArbitrageSignal(
                            signal_id=f"PCP_{bot_id}_{datetime.utcnow().timestamp()}",
                            arb_type=ArbitrageType.PUT_CALL_PARITY,
                            resource_type=resource_type,
                            expected_profit=profit - self._calculate_fees(profit, config),
                            required_capital=spot_price * Decimal("100") + strike * Decimal("100"),
                            confidence=Decimal("0.95"),
                            expiry_time=datetime.utcnow() + timedelta(minutes=5),
                            legs=legs
                        )
                        signals.append(signal)
                        
        return signals
        
    async def _find_box_spread_arb(self, bot_id: str) -> List[ArbitrageSignal]:
        """Find box spread arbitrage opportunities"""
        config = self.configs[bot_id]
        signals = []
        
        for resource_type in self._price_cache.keys():
            option_chain = await self.options_engine.get_option_chain(resource_type)
            
            for expiry, strikes in option_chain.items():
                # Need at least 2 strikes for box spread
                sorted_strikes = sorted(strikes)
                
                for i in range(len(sorted_strikes) - 1):
                    k1 = sorted_strikes[i]
                    k2 = sorted_strikes[i + 1]
                    
                    # Get all four option prices
                    c1 = await self._get_option_price(resource_type, OptionType.CALL, k1, expiry)
                    c2 = await self._get_option_price(resource_type, OptionType.CALL, k2, expiry)
                    p1 = await self._get_option_price(resource_type, OptionType.PUT, k1, expiry)
                    p2 = await self._get_option_price(resource_type, OptionType.PUT, k2, expiry)
                    
                    if not all([c1, c2, p1, p2]):
                        continue
                        
                    # Box spread value should equal (K2 - K1) * discount_factor
                    days_to_expiry = (expiry - datetime.utcnow()).days
                    discount_factor = Decimal(str(np.exp(-0.05 * days_to_expiry / 365)))
                    theoretical_value = (k2 - k1) * discount_factor
                    
                    # Actual box value
                    box_value = (c1 - c2) + (p2 - p1)
                    
                    if box_value < theoretical_value * (Decimal("1") - config.min_return_threshold):
                        # Buy box spread
                        profit = (theoretical_value - box_value) * Decimal("100")
                        
                        signal = ArbitrageSignal(
                            signal_id=f"BOX_{bot_id}_{datetime.utcnow().timestamp()}",
                            arb_type=ArbitrageType.BOX_SPREAD,
                            resource_type=resource_type,
                            expected_profit=profit - self._calculate_fees(profit, config),
                            required_capital=box_value * Decimal("100"),
                            confidence=Decimal("0.98"),
                            expiry_time=datetime.utcnow() + timedelta(minutes=5),
                            legs=[
                                {"market": "options", "type": "call", "side": "buy", "strike": k1, "price": c1},
                                {"market": "options", "type": "call", "side": "sell", "strike": k2, "price": c2},
                                {"market": "options", "type": "put", "side": "sell", "strike": k1, "price": p1},
                                {"market": "options", "type": "put", "side": "buy", "strike": k2, "price": p2}
                            ]
                        )
                        signals.append(signal)
                        
        return signals
        
    async def _find_cross_exchange_arb(self, bot_id: str) -> List[ArbitrageSignal]:
        """Find arbitrage opportunities across exchanges"""
        # Simplified - would implement actual cross-exchange logic
        return []
        
    # Execution methods
    
    async def _execute_arbitrage(self) -> None:
        """Execute profitable arbitrage opportunities"""
        while True:
            try:
                for bot_id, signals in self.pending_signals.items():
                    config = self.configs[bot_id]
                    
                    # Check capital availability
                    used_capital = sum(
                        pos.entry_cost for pos in self.active_positions[bot_id].values()
                        if not pos.is_closed
                    )
                    available_capital = config.max_total_capital - used_capital
                    
                    # Execute best available signal
                    for signal in signals:
                        if not signal.is_valid:
                            continue
                            
                        if signal.required_capital <= available_capital:
                            await self._execute_signal(bot_id, signal)
                            signal.executed = True
                            break
                            
                await asyncio.sleep(1)  # Fast execution loop
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error executing arbitrage: {e}")
                await asyncio.sleep(1)
                
    async def _execute_signal(self, bot_id: str, signal: ArbitrageSignal) -> None:
        """Execute a specific arbitrage signal"""
        config = self.configs[bot_id]
        start_time = datetime.utcnow()
        
        try:
            position = ArbitragePosition(
                position_id=f"POS_{bot_id}_{datetime.utcnow().timestamp()}",
                signal=signal,
                entry_time=start_time
            )
            
            # Execute each leg
            success = True
            for leg in signal.legs:
                if not await self._execute_leg(position, leg, config):
                    success = False
                    break
                    
            if success:
                # Calculate entry cost
                position.entry_cost = self._calculate_position_cost(position)
                
                # Add to active positions
                self.active_positions[bot_id][position.position_id] = position
                
                # Update statistics
                self.statistics[bot_id].positions_opened += 1
                self.statistics[bot_id].signals_generated += 1
                
                # Publish event
                await self.pulsar.publish('arb.position_opened', {
                    'bot_id': bot_id,
                    'position_id': position.position_id,
                    'arb_type': signal.arb_type.value,
                    'expected_profit': float(signal.expected_profit),
                    'execution_time': (datetime.utcnow() - start_time).total_seconds()
                })
                
            else:
                # Unwind any executed legs
                await self._unwind_position(position)
                logger.warning(f"Failed to execute arbitrage signal {signal.signal_id}")
                
        except Exception as e:
            logger.error(f"Error executing arbitrage signal {signal.signal_id}: {e}")
            
    async def _execute_leg(self,
                          position: ArbitragePosition,
                          leg: Dict[str, Any],
                          config: CrossMarketArbConfig) -> bool:
        """Execute a single leg of an arbitrage trade"""
        try:
            market = leg["market"]
            
            if market == "spot":
                # Execute spot trade
                order = await self.spot_market.place_order(
                    user_id=position.position_id,
                    order_type=OrderType.MARKET,
                    side=OrderSide.BUY if leg["side"] == "buy" else OrderSide.SELL,
                    resource_type=position.signal.resource_type,
                    quantity=Decimal(str(leg["quantity"])),
                    price=None
                )
                
                # Track position
                if leg["side"] == "buy":
                    position.spot_positions[position.signal.resource_type] = Decimal(str(leg["quantity"]))
                else:
                    position.spot_positions[position.signal.resource_type] = -Decimal(str(leg["quantity"]))
                    
                # Track fees
                position.fees_paid += Decimal(str(leg["quantity"])) * leg["price"] * config.spot_fee
                
            elif market == "futures":
                # Execute futures trade
                futures_id = await self.futures_engine.place_futures_order(
                    user_id=position.position_id,
                    side=leg["side"],
                    resource_type=position.signal.resource_type,
                    quantity=Decimal(str(leg["quantity"])),
                    price=leg["price"],
                    expiry=leg.get("expiry", datetime.utcnow() + timedelta(days=30))
                )
                position.futures_positions.append(futures_id)
                
                # Track fees
                position.fees_paid += Decimal(str(leg["quantity"])) * leg["price"] * config.futures_fee
                
            elif market == "options":
                # Execute options trade
                option_id = await self.options_engine.place_option_order(
                    user_id=position.position_id,
                    option_type=OptionType.CALL if leg["type"] == "call" else OptionType.PUT,
                    side=leg["side"],
                    strike=leg["strike"],
                    expiry=leg.get("expiry", datetime.utcnow() + timedelta(days=30)),
                    quantity=Decimal("100"),  # Standard lot size
                    price=leg["price"],
                    resource_type=position.signal.resource_type
                )
                position.options_positions.append(option_id)
                
                # Track fees
                position.fees_paid += Decimal("100") * leg["price"] * config.options_fee
                
            return True
            
        except Exception as e:
            logger.error(f"Error executing leg: {e}")
            return False
            
    async def _unwind_position(self, position: ArbitragePosition) -> None:
        """Unwind a partially executed position"""
        # Close spot positions
        for resource_type, quantity in position.spot_positions.items():
            if quantity != 0:
                await self.spot_market.place_order(
                    user_id=position.position_id,
                    order_type=OrderType.MARKET,
                    side=OrderSide.SELL if quantity > 0 else OrderSide.BUY,
                    resource_type=resource_type,
                    quantity=abs(quantity),
                    price=None
                )
                
        # Close futures positions
        for futures_id in position.futures_positions:
            await self.futures_engine.close_position(futures_id)
            
        # Close options positions
        for option_id in position.options_positions:
            await self.options_engine.close_position(option_id)
            
    # Monitoring methods
    
    async def _monitor_positions(self) -> None:
        """Monitor open positions and close when profitable"""
        while True:
            try:
                for bot_id, positions in self.active_positions.items():
                    for position_id, position in list(positions.items()):
                        if position.is_closed:
                            continue
                            
                        # Update position value
                        await self._update_position_value(position)
                        
                        # Check if we should close
                        if position.unrealized_pnl > position.signal.expected_profit * Decimal("0.8"):
                            # Take profit at 80% of expected
                            await self._close_position(bot_id, position_id, "profit_target")
                        elif position.unrealized_pnl < -position.signal.expected_profit * Decimal("0.5"):
                            # Stop loss at 50% of expected profit
                            await self._close_position(bot_id, position_id, "stop_loss")
                        elif datetime.utcnow() > position.signal.expiry_time:
                            # Close expired positions
                            await self._close_position(bot_id, position_id, "expired")
                            
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring positions: {e}")
                await asyncio.sleep(10)
                
    async def _update_position_value(self, position: ArbitragePosition) -> None:
        """Update current value of position"""
        current_value = Decimal("0")
        
        # Value spot positions
        for resource_type, quantity in position.spot_positions.items():
            spot_price = await self._get_spot_price(resource_type)
            current_value += quantity * spot_price
            
        # Value futures positions
        for futures_id in position.futures_positions:
            futures_value = await self.futures_engine.get_position_value(futures_id)
            current_value += futures_value
            
        # Value options positions
        for option_id in position.options_positions:
            option_value = await self.options_engine.get_position_value(option_id)
            current_value += option_value
            
        position.current_value = current_value
        position.unrealized_pnl = current_value - position.entry_cost
        
    async def _close_position(self,
                             bot_id: str,
                             position_id: str,
                             reason: str) -> None:
        """Close an arbitrage position"""
        position = self.active_positions[bot_id][position_id]
        
        # Unwind all legs
        await self._unwind_position(position)
        
        # Mark as closed
        position.is_closed = True
        position.close_reason = reason
        position.realized_pnl = position.unrealized_pnl - position.fees_paid
        
        # Update statistics
        stats = self.statistics[bot_id]
        stats.positions_closed += 1
        stats.total_profit += position.realized_pnl
        stats.total_fees += position.fees_paid
        
        if position.realized_pnl > 0:
            stats.successful_arbs += 1
            
        # Update success rate
        if stats.positions_closed > 0:
            stats.success_rate = Decimal(stats.successful_arbs) / Decimal(stats.positions_closed)
            stats.avg_profit_per_trade = stats.total_profit / Decimal(stats.positions_closed)
            
        # Update by type statistics
        arb_type = position.signal.arb_type
        if arb_type not in stats.by_type:
            stats.by_type[arb_type] = {"count": 0, "profit": Decimal("0"), "success_rate": Decimal("0")}
            
        type_stats = stats.by_type[arb_type]
        type_stats["count"] += 1
        type_stats["profit"] += position.realized_pnl
        
        # Publish event
        await self.pulsar.publish('arb.position_closed', {
            'bot_id': bot_id,
            'position_id': position_id,
            'reason': reason,
            'pnl': float(position.realized_pnl),
            'duration': (datetime.utcnow() - position.entry_time).total_seconds()
        })
        
    async def close_all_positions(self, bot_id: str) -> Dict[str, Any]:
        """Close all positions for a bot"""
        if bot_id not in self.active_positions:
            return {"closed": 0, "total_pnl": 0}
            
        closed = 0
        total_pnl = Decimal("0")
        
        for position_id in list(self.active_positions[bot_id].keys()):
            position = self.active_positions[bot_id][position_id]
            if not position.is_closed:
                await self._close_position(bot_id, position_id, "manual")
                closed += 1
                total_pnl += position.realized_pnl
                
        return {"closed": closed, "total_pnl": float(total_pnl)}
        
    # Helper methods
    
    async def _update_market_data(self) -> None:
        """Update cached market data"""
        for resource_type in list(self._price_cache.keys()):
            # Only update if stale
            if resource_type in self._last_update:
                if datetime.utcnow() - self._last_update[resource_type] < timedelta(seconds=5):
                    continue
                    
            try:
                # Get spot price
                spot_data = await self.spot_market.get_market_data(resource_type)
                self._price_cache[resource_type]["spot"] = spot_data.get("last_price", Decimal("0"))
                
                # Get futures prices
                futures_data = await self.futures_engine.get_futures_prices(resource_type)
                self._price_cache[resource_type]["futures"] = futures_data
                
                # Get option prices
                option_data = await self.options_engine.get_option_prices(resource_type)
                self._price_cache[resource_type]["options"] = option_data
                
                self._last_update[resource_type] = datetime.utcnow()
                
            except Exception as e:
                logger.error(f"Error updating market data for {resource_type}: {e}")
                
    async def _get_spot_price(self, resource_type: str) -> Decimal:
        """Get cached spot price"""
        if resource_type in self._price_cache:
            return self._price_cache[resource_type].get("spot", Decimal("0"))
        return Decimal("0")
        
    async def _get_futures_price(self,
                                resource_type: str,
                                expiry: datetime) -> Optional[Decimal]:
        """Get cached futures price"""
        if resource_type not in self._price_cache:
            return None
            
        futures_data = self._price_cache[resource_type].get("futures", {})
        
        # Find closest expiry
        for exp_str, price in futures_data.items():
            exp_date = datetime.fromisoformat(exp_str)
            if abs((exp_date - expiry).days) < 7:  # Within a week
                return Decimal(str(price))
                
        return None
        
    async def _get_option_price(self,
                               resource_type: str,
                               option_type: OptionType,
                               strike: Decimal,
                               expiry: datetime) -> Optional[Decimal]:
        """Get cached option price"""
        if resource_type not in self._price_cache:
            return None
            
        option_data = self._price_cache[resource_type].get("options", {})
        
        # Look for matching option
        key = f"{option_type.value}_{strike}_{expiry.isoformat()}"
        return option_data.get(key)
        
    def _validate_signal(self,
                        signal: ArbitrageSignal,
                        config: CrossMarketArbConfig) -> bool:
        """Validate arbitrage signal meets criteria"""
        if signal.expected_profit < config.min_profit_threshold:
            return False
            
        if signal.return_on_capital < config.min_return_threshold:
            return False
            
        if signal.required_capital > config.max_capital_per_trade:
            return False
            
        return True
        
    def _calculate_fees(self,
                       trade_value: Decimal,
                       config: CrossMarketArbConfig) -> Decimal:
        """Calculate estimated fees for a trade"""
        # Simplified - actual calculation would consider each leg
        avg_fee = (config.spot_fee + config.futures_fee + config.options_fee) / 3
        return trade_value * avg_fee * 2  # Round trip
        
    def _calculate_position_cost(self, position: ArbitragePosition) -> Decimal:
        """Calculate total cost of entering position"""
        cost = Decimal("0")
        
        # Spot positions
        for resource_type, quantity in position.spot_positions.items():
            if quantity > 0:
                spot_price = self._price_cache.get(resource_type, {}).get("spot", Decimal("0"))
                cost += quantity * spot_price
                
        # Add fees
        cost += position.fees_paid
        
        return cost 