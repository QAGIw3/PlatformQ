"""
Volatility Arbitrage Bot for Compute Markets

Implements strategies to profit from volatility discrepancies including:
- Implied vs realized volatility arbitrage
- Cross-strike volatility arbitrage
- Term structure arbitrage
- Volatility pair trading
"""

import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import pandas as pd
from collections import deque

from app.engines.compute_options_engine import (
    ComputeOptionsEngine,
    OptionType,
    ExerciseStyle
)
from app.engines.compute_spot_market import ComputeSpotMarket
from app.engines.volatility_surface import VolatilitySurfaceEngine
from app.engines.variance_swaps_engine import VarianceSwapsEngine
from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient

logger = logging.getLogger(__name__)


class VolArbStrategy(Enum):
    IMPLIED_REALIZED = "implied_vs_realized"
    CROSS_STRIKE = "cross_strike"
    TERM_STRUCTURE = "term_structure"
    PAIR_TRADING = "pair_trading"
    DISPERSION = "dispersion"
    VARIANCE_PREMIUM = "variance_premium"


class SignalStrength(Enum):
    STRONG = "strong"
    MODERATE = "moderate"
    WEAK = "weak"
    NONE = "none"


@dataclass
class VolatilitySignal:
    """Represents a volatility arbitrage signal"""
    signal_id: str
    strategy: VolArbStrategy
    strength: SignalStrength
    resource_type: str
    entry_iv: Decimal
    target_iv: Decimal
    current_rv: Decimal
    confidence: Decimal
    expected_profit: Decimal
    risk_score: Decimal
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class VolArbPosition:
    """Active volatility arbitrage position"""
    position_id: str
    signal_id: str
    strategy: VolArbStrategy
    resource_type: str
    entry_time: datetime
    
    # Position details
    long_options: List[str] = field(default_factory=list)  # Option IDs
    short_options: List[str] = field(default_factory=list)
    variance_swaps: List[str] = field(default_factory=list)
    hedge_positions: Dict[str, Decimal] = field(default_factory=dict)
    
    # Performance tracking
    entry_cost: Decimal = Decimal("0")
    current_value: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    
    # Risk metrics
    vega_exposure: Decimal = Decimal("0")
    gamma_exposure: Decimal = Decimal("0")
    max_loss: Decimal = Decimal("0")
    
    is_closed: bool = False


@dataclass
class VolArbConfig:
    """Configuration for volatility arbitrage strategies"""
    # Position sizing
    max_position_size: Decimal = Decimal("100000")
    position_sizing_method: str = "kelly"  # kelly, fixed, risk_parity
    kelly_fraction: Decimal = Decimal("0.25")  # Conservative Kelly
    
    # Risk limits
    max_vega_exposure: Decimal = Decimal("10000")
    max_gamma_exposure: Decimal = Decimal("5000")
    max_portfolio_var: Decimal = Decimal("50000")
    max_correlation_risk: Decimal = Decimal("0.7")
    
    # Signal thresholds
    min_signal_confidence: Decimal = Decimal("0.7")
    min_expected_profit: Decimal = Decimal("1000")
    max_risk_score: Decimal = Decimal("0.3")
    
    # Volatility parameters
    rv_lookback_window: int = 30  # days
    iv_rv_threshold: Decimal = Decimal("0.05")  # 5% difference
    term_structure_threshold: Decimal = Decimal("0.03")  # 3% 
    
    # Execution
    use_limit_orders: bool = True
    max_slippage: Decimal = Decimal("0.02")  # 2%
    rebalance_frequency: int = 3600  # seconds
    
    # Exit conditions
    profit_target: Decimal = Decimal("0.2")  # 20%
    stop_loss: Decimal = Decimal("0.1")  # 10%
    max_holding_period: int = 30  # days
    
    # Advanced features
    use_machine_learning: bool = False
    ml_model_path: Optional[str] = None
    use_regime_detection: bool = True


@dataclass
class VolArbStatistics:
    """Performance statistics for volatility arbitrage"""
    total_signals: int = 0
    positions_opened: int = 0
    positions_closed: int = 0
    winning_positions: int = 0
    
    total_pnl: Decimal = Decimal("0")
    total_volume: Decimal = Decimal("0")
    sharpe_ratio: Decimal = Decimal("0")
    sortino_ratio: Decimal = Decimal("0")
    max_drawdown: Decimal = Decimal("0")
    
    avg_holding_period: timedelta = timedelta()
    win_rate: Decimal = Decimal("0")
    profit_factor: Decimal = Decimal("0")
    
    strategy_performance: Dict[VolArbStrategy, Dict[str, Any]] = field(default_factory=dict)
    daily_pnl: deque = field(default_factory=lambda: deque(maxlen=365))


class VolatilityArbitrageBot:
    """
    Automated volatility arbitrage trading bot for compute markets
    """
    
    def __init__(self,
                 options_engine: ComputeOptionsEngine,
                 spot_market: ComputeSpotMarket,
                 vol_surface: VolatilitySurfaceEngine,
                 variance_swaps: VarianceSwapsEngine,
                 ignite: IgniteCache,
                 pulsar: PulsarEventPublisher,
                 oracle: OracleAggregatorClient):
        self.options_engine = options_engine
        self.spot_market = spot_market
        self.vol_surface = vol_surface
        self.variance_swaps = variance_swaps
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        
        self.configs: Dict[str, VolArbConfig] = {}
        self.active_positions: Dict[str, Dict[str, VolArbPosition]] = {}
        self.signals: Dict[str, List[VolatilitySignal]] = {}
        self.statistics: Dict[str, VolArbStatistics] = {}
        
        # Market data cache
        self.historical_volatility: Dict[str, pd.DataFrame] = {}
        self.realized_volatility: Dict[str, deque] = {}
        
        self._monitoring_task: Optional[asyncio.Task] = None
        self._signal_generation_task: Optional[asyncio.Task] = None
        self._execution_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start volatility arbitrage bot"""
        logger.info("Starting volatility arbitrage bot")
        self._monitoring_task = asyncio.create_task(self._monitor_positions())
        self._signal_generation_task = asyncio.create_task(self._generate_signals())
        self._execution_task = asyncio.create_task(self._execute_signals())
        
    async def stop(self):
        """Stop volatility arbitrage bot"""
        logger.info("Stopping volatility arbitrage bot")
        
        for task in [self._monitoring_task, self._signal_generation_task, self._execution_task]:
            if task:
                task.cancel()
                
        # Close all positions
        for bot_id in list(self.active_positions.keys()):
            await self.close_all_positions(bot_id)
            
    async def create_bot(self,
                        config: VolArbConfig,
                        resource_types: List[str],
                        user_id: str) -> str:
        """Create a new volatility arbitrage bot instance"""
        bot_id = f"VOLARB_{user_id}_{datetime.utcnow().timestamp()}"
        
        self.configs[bot_id] = config
        self.active_positions[bot_id] = {}
        self.signals[bot_id] = []
        self.statistics[bot_id] = VolArbStatistics()
        
        # Initialize historical data
        for resource_type in resource_types:
            await self._load_historical_volatility(resource_type)
            
        # Publish event
        await self.pulsar.publish('volarb.created', {
            'bot_id': bot_id,
            'user_id': user_id,
            'resource_types': resource_types,
            'config': config.__dict__
        })
        
        logger.info(f"Created volatility arbitrage bot {bot_id}")
        return bot_id
        
    async def get_bot_status(self, bot_id: str) -> Dict[str, Any]:
        """Get current status of volatility arbitrage bot"""
        if bot_id not in self.configs:
            raise ValueError(f"Bot {bot_id} not found")
            
        config = self.configs[bot_id]
        positions = self.active_positions[bot_id]
        stats = self.statistics[bot_id]
        signals = self.signals[bot_id]
        
        # Calculate current exposures
        total_vega = sum(pos.vega_exposure for pos in positions.values())
        total_gamma = sum(pos.gamma_exposure for pos in positions.values())
        
        return {
            "bot_id": bot_id,
            "config": config.__dict__,
            "active_positions": len(positions),
            "pending_signals": len([s for s in signals if s.timestamp > datetime.utcnow() - timedelta(minutes=5)]),
            "exposures": {
                "vega": float(total_vega),
                "gamma": float(total_gamma),
                "position_value": float(sum(pos.current_value for pos in positions.values()))
            },
            "statistics": {
                "total_pnl": float(stats.total_pnl),
                "positions_opened": stats.positions_opened,
                "win_rate": float(stats.win_rate),
                "sharpe_ratio": float(stats.sharpe_ratio),
                "max_drawdown": float(stats.max_drawdown),
                "profit_factor": float(stats.profit_factor)
            },
            "strategy_breakdown": stats.strategy_performance
        }
        
    # Signal generation methods
    
    async def _generate_signals(self) -> None:
        """Generate volatility arbitrage signals"""
        while True:
            try:
                for bot_id, config in self.configs.items():
                    # Generate signals for each strategy
                    signals = []
                    
                    # Implied vs Realized
                    iv_rv_signals = await self._find_iv_rv_arbitrage(bot_id)
                    signals.extend(iv_rv_signals)
                    
                    # Cross-strike arbitrage
                    cross_strike_signals = await self._find_cross_strike_arbitrage(bot_id)
                    signals.extend(cross_strike_signals)
                    
                    # Term structure arbitrage
                    term_signals = await self._find_term_structure_arbitrage(bot_id)
                    signals.extend(term_signals)
                    
                    # Variance premium
                    var_premium_signals = await self._find_variance_premium(bot_id)
                    signals.extend(var_premium_signals)
                    
                    # Filter and rank signals
                    filtered_signals = self._filter_signals(signals, config)
                    ranked_signals = self._rank_signals(filtered_signals)
                    
                    # Store signals
                    self.signals[bot_id] = ranked_signals[:10]  # Keep top 10
                    
                await asyncio.sleep(300)  # Generate signals every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error generating signals: {e}")
                await asyncio.sleep(300)
                
    async def _find_iv_rv_arbitrage(self, bot_id: str) -> List[VolatilitySignal]:
        """Find implied vs realized volatility arbitrage opportunities"""
        config = self.configs[bot_id]
        signals = []
        
        # Get all active option series
        active_series = await self.options_engine.get_active_series()
        
        for series in active_series:
            resource_type = series["resource_type"]
            
            # Get current IV
            current_iv = await self.vol_surface.get_implied_volatility(
                resource_type,
                series["strike"],
                series["expiry"]
            )
            
            # Calculate realized volatility
            rv = await self._calculate_realized_volatility(
                resource_type,
                config.rv_lookback_window
            )
            
            # Check for significant divergence
            iv_rv_diff = current_iv - rv
            
            if abs(iv_rv_diff) > config.iv_rv_threshold:
                # Calculate expected profit
                vega = await self._estimate_vega(series)
                expected_profit = abs(iv_rv_diff) * vega * Decimal("100")  # Rough estimate
                
                # Create signal
                signal = VolatilitySignal(
                    signal_id=f"IVRV_{bot_id}_{datetime.utcnow().timestamp()}",
                    strategy=VolArbStrategy.IMPLIED_REALIZED,
                    strength=self._calculate_signal_strength(iv_rv_diff, config.iv_rv_threshold),
                    resource_type=resource_type,
                    entry_iv=current_iv,
                    target_iv=rv,
                    current_rv=rv,
                    confidence=self._calculate_confidence(iv_rv_diff, rv),
                    expected_profit=expected_profit,
                    risk_score=self._calculate_risk_score(series, iv_rv_diff),
                    timestamp=datetime.utcnow(),
                    metadata={
                        "strike": float(series["strike"]),
                        "expiry": series["expiry"].isoformat(),
                        "iv_rv_diff": float(iv_rv_diff),
                        "direction": "sell_vol" if iv_rv_diff > 0 else "buy_vol"
                    }
                )
                
                signals.append(signal)
                
        return signals
        
    async def _find_cross_strike_arbitrage(self, bot_id: str) -> List[VolatilitySignal]:
        """Find cross-strike volatility arbitrage opportunities"""
        config = self.configs[bot_id]
        signals = []
        
        # Get volatility surface
        surface_data = await self.vol_surface.get_surface_data()
        
        for resource_type, surface in surface_data.items():
            # Group by expiry
            expiry_groups = {}
            for point in surface:
                expiry = point["expiry"]
                if expiry not in expiry_groups:
                    expiry_groups[expiry] = []
                expiry_groups[expiry].append(point)
                
            # Check each expiry for arbitrage
            for expiry, points in expiry_groups.items():
                if len(points) < 3:
                    continue
                    
                # Sort by strike
                points.sort(key=lambda x: x["strike"])
                
                # Check for butterfly arbitrage
                for i in range(len(points) - 2):
                    strike1, iv1 = points[i]["strike"], points[i]["implied_vol"]
                    strike2, iv2 = points[i+1]["strike"], points[i+1]["implied_vol"]
                    strike3, iv3 = points[i+2]["strike"], points[i+2]["implied_vol"]
                    
                    # Check convexity violation
                    expected_iv2 = iv1 + (iv3 - iv1) * (strike2 - strike1) / (strike3 - strike1)
                    
                    if abs(iv2 - expected_iv2) > Decimal("0.02"):  # 2% violation
                        # Butterfly arbitrage opportunity
                        signal = VolatilitySignal(
                            signal_id=f"CROSS_{bot_id}_{datetime.utcnow().timestamp()}",
                            strategy=VolArbStrategy.CROSS_STRIKE,
                            strength=SignalStrength.MODERATE,
                            resource_type=resource_type,
                            entry_iv=iv2,
                            target_iv=expected_iv2,
                            current_rv=Decimal("0"),  # Not relevant for this strategy
                            confidence=Decimal("0.8"),
                            expected_profit=self._calculate_butterfly_profit(
                                strike1, strike2, strike3, iv1, iv2, iv3
                            ),
                            risk_score=Decimal("0.2"),
                            timestamp=datetime.utcnow(),
                            metadata={
                                "butterfly_strikes": [float(strike1), float(strike2), float(strike3)],
                                "butterfly_ivs": [float(iv1), float(iv2), float(iv3)],
                                "expiry": expiry.isoformat()
                            }
                        )
                        
                        signals.append(signal)
                        
        return signals
        
    async def _find_term_structure_arbitrage(self, bot_id: str) -> List[VolatilitySignal]:
        """Find term structure arbitrage opportunities"""
        config = self.configs[bot_id]
        signals = []
        
        # Get term structure for each resource type
        for resource_type in await self._get_active_resource_types():
            term_structure = await self.vol_surface.get_term_structure(
                resource_type,
                await self._get_spot_price(resource_type)  # ATM
            )
            
            if len(term_structure) < 2:
                continue
                
            # Check for calendar spread opportunities
            for i in range(len(term_structure) - 1):
                near_term = term_structure[i]
                far_term = term_structure[i + 1]
                
                # Calculate expected term structure slope
                days_diff = (far_term["expiry"] - near_term["expiry"]).days
                expected_slope = Decimal("0.001") * days_diff  # Simplified expectation
                
                actual_slope = far_term["implied_vol"] - near_term["implied_vol"]
                
                if abs(actual_slope - expected_slope) > config.term_structure_threshold:
                    signal = VolatilitySignal(
                        signal_id=f"TERM_{bot_id}_{datetime.utcnow().timestamp()}",
                        strategy=VolArbStrategy.TERM_STRUCTURE,
                        strength=SignalStrength.MODERATE,
                        resource_type=resource_type,
                        entry_iv=near_term["implied_vol"],
                        target_iv=far_term["implied_vol"],
                        current_rv=Decimal("0"),
                        confidence=Decimal("0.75"),
                        expected_profit=self._calculate_calendar_profit(
                            near_term, far_term, actual_slope, expected_slope
                        ),
                        risk_score=Decimal("0.25"),
                        timestamp=datetime.utcnow(),
                        metadata={
                            "near_expiry": near_term["expiry"].isoformat(),
                            "far_expiry": far_term["expiry"].isoformat(),
                            "slope_diff": float(actual_slope - expected_slope)
                        }
                    )
                    
                    signals.append(signal)
                    
        return signals
        
    async def _find_variance_premium(self, bot_id: str) -> List[VolatilitySignal]:
        """Find variance risk premium opportunities"""
        config = self.configs[bot_id]
        signals = []
        
        for resource_type in await self._get_active_resource_types():
            # Get variance swap quotes
            var_swap_quote = await self.variance_swaps.get_fair_variance(
                resource_type,
                30  # 30 day variance
            )
            
            # Calculate historical variance premium
            historical_premium = await self._calculate_variance_premium(
                resource_type,
                lookback_days=90
            )
            
            current_premium = var_swap_quote["implied_variance"] - var_swap_quote["realized_variance"]
            
            # Check if premium is significantly different from historical
            if abs(current_premium - historical_premium) > Decimal("0.0025"):  # 25 bps
                signal = VolatilitySignal(
                    signal_id=f"VARPREM_{bot_id}_{datetime.utcnow().timestamp()}",
                    strategy=VolArbStrategy.VARIANCE_PREMIUM,
                    strength=SignalStrength.STRONG,
                    resource_type=resource_type,
                    entry_iv=Decimal(str(np.sqrt(float(var_swap_quote["implied_variance"]) * 365))),
                    target_iv=Decimal(str(np.sqrt(float(var_swap_quote["realized_variance"]) * 365))),
                    current_rv=Decimal(str(np.sqrt(float(var_swap_quote["realized_variance"]) * 365))),
                    confidence=Decimal("0.85"),
                    expected_profit=abs(current_premium - historical_premium) * Decimal("100000"),  # Notional
                    risk_score=Decimal("0.3"),
                    timestamp=datetime.utcnow(),
                    metadata={
                        "current_premium": float(current_premium),
                        "historical_premium": float(historical_premium),
                        "variance_swap_strike": float(var_swap_quote["strike_variance"])
                    }
                )
                
                signals.append(signal)
                
        return signals
        
    # Execution methods
    
    async def _execute_signals(self) -> None:
        """Execute profitable volatility arbitrage signals"""
        while True:
            try:
                for bot_id, signals in self.signals.items():
                    config = self.configs[bot_id]
                    
                    # Check position limits
                    if not self._can_open_position(bot_id):
                        continue
                        
                    # Execute best signal
                    for signal in signals:
                        if signal.confidence >= config.min_signal_confidence:
                            await self._execute_signal(bot_id, signal)
                            break  # Execute one at a time
                            
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error executing signals: {e}")
                await asyncio.sleep(60)
                
    async def _execute_signal(self, bot_id: str, signal: VolatilitySignal) -> None:
        """Execute a specific volatility arbitrage signal"""
        config = self.configs[bot_id]
        
        try:
            position = None
            
            if signal.strategy == VolArbStrategy.IMPLIED_REALIZED:
                position = await self._execute_iv_rv_trade(bot_id, signal)
            elif signal.strategy == VolArbStrategy.CROSS_STRIKE:
                position = await self._execute_butterfly_trade(bot_id, signal)
            elif signal.strategy == VolArbStrategy.TERM_STRUCTURE:
                position = await self._execute_calendar_trade(bot_id, signal)
            elif signal.strategy == VolArbStrategy.VARIANCE_PREMIUM:
                position = await self._execute_variance_trade(bot_id, signal)
                
            if position:
                self.active_positions[bot_id][position.position_id] = position
                self.statistics[bot_id].positions_opened += 1
                
                # Publish event
                await self.pulsar.publish('volarb.position_opened', {
                    'bot_id': bot_id,
                    'position_id': position.position_id,
                    'strategy': position.strategy.value,
                    'expected_profit': float(signal.expected_profit)
                })
                
        except Exception as e:
            logger.error(f"Error executing signal {signal.signal_id}: {e}")
            
    async def _execute_iv_rv_trade(self,
                                  bot_id: str,
                                  signal: VolatilitySignal) -> Optional[VolArbPosition]:
        """Execute implied vs realized volatility trade"""
        config = self.configs[bot_id]
        
        # Determine position size
        position_size = self._calculate_position_size(signal, config)
        
        if signal.metadata["direction"] == "sell_vol":
            # Sell volatility - sell straddle and delta hedge
            strike = Decimal(str(signal.metadata["strike"]))
            expiry = datetime.fromisoformat(signal.metadata["expiry"])
            
            # Sell ATM straddle
            call_order = await self.options_engine.sell_option(
                option_type=OptionType.CALL,
                strike=strike,
                expiry=expiry,
                quantity=position_size,
                resource_type=signal.resource_type,
                user_id=bot_id
            )
            
            put_order = await self.options_engine.sell_option(
                option_type=OptionType.PUT,
                strike=strike,
                expiry=expiry,
                quantity=position_size,
                resource_type=signal.resource_type,
                user_id=bot_id
            )
            
            position = VolArbPosition(
                position_id=f"POS_{bot_id}_{datetime.utcnow().timestamp()}",
                signal_id=signal.signal_id,
                strategy=signal.strategy,
                resource_type=signal.resource_type,
                entry_time=datetime.utcnow(),
                short_options=[call_order.order_id, put_order.order_id],
                entry_cost=-position_size * (call_order.price + put_order.price)  # Premium received
            )
            
        else:
            # Buy volatility - buy straddle
            strike = Decimal(str(signal.metadata["strike"]))
            expiry = datetime.fromisoformat(signal.metadata["expiry"])
            
            call_order = await self.options_engine.buy_option(
                option_type=OptionType.CALL,
                strike=strike,
                expiry=expiry,
                quantity=position_size,
                resource_type=signal.resource_type,
                user_id=bot_id
            )
            
            put_order = await self.options_engine.buy_option(
                option_type=OptionType.PUT,
                strike=strike,
                expiry=expiry,
                quantity=position_size,
                resource_type=signal.resource_type,
                user_id=bot_id
            )
            
            position = VolArbPosition(
                position_id=f"POS_{bot_id}_{datetime.utcnow().timestamp()}",
                signal_id=signal.signal_id,
                strategy=signal.strategy,
                resource_type=signal.resource_type,
                entry_time=datetime.utcnow(),
                long_options=[call_order.order_id, put_order.order_id],
                entry_cost=position_size * (call_order.price + put_order.price)
            )
            
        return position
        
    async def _execute_butterfly_trade(self,
                                      bot_id: str,
                                      signal: VolatilitySignal) -> Optional[VolArbPosition]:
        """Execute butterfly spread for cross-strike arbitrage"""
        strikes = signal.metadata["butterfly_strikes"]
        expiry = datetime.fromisoformat(signal.metadata["expiry"])
        position_size = self._calculate_position_size(signal, self.configs[bot_id])
        
        # Buy 1 low strike, sell 2 middle strike, buy 1 high strike
        orders = []
        
        # Buy low strike call
        order1 = await self.options_engine.buy_option(
            option_type=OptionType.CALL,
            strike=Decimal(str(strikes[0])),
            expiry=expiry,
            quantity=position_size,
            resource_type=signal.resource_type,
            user_id=bot_id
        )
        orders.append(order1.order_id)
        
        # Sell 2x middle strike calls
        order2 = await self.options_engine.sell_option(
            option_type=OptionType.CALL,
            strike=Decimal(str(strikes[1])),
            expiry=expiry,
            quantity=position_size * 2,
            resource_type=signal.resource_type,
            user_id=bot_id
        )
        orders.append(order2.order_id)
        
        # Buy high strike call
        order3 = await self.options_engine.buy_option(
            option_type=OptionType.CALL,
            strike=Decimal(str(strikes[2])),
            expiry=expiry,
            quantity=position_size,
            resource_type=signal.resource_type,
            user_id=bot_id
        )
        orders.append(order3.order_id)
        
        # Calculate net cost
        net_cost = position_size * (order1.price - 2 * order2.price + order3.price)
        
        position = VolArbPosition(
            position_id=f"POS_{bot_id}_{datetime.utcnow().timestamp()}",
            signal_id=signal.signal_id,
            strategy=signal.strategy,
            resource_type=signal.resource_type,
            entry_time=datetime.utcnow(),
            long_options=[order1.order_id, order3.order_id],
            short_options=[order2.order_id],
            entry_cost=net_cost,
            metadata={"strikes": strikes}
        )
        
        return position
        
    async def _execute_calendar_trade(self,
                                     bot_id: str,
                                     signal: VolatilitySignal) -> Optional[VolArbPosition]:
        """Execute calendar spread for term structure arbitrage"""
        near_expiry = datetime.fromisoformat(signal.metadata["near_expiry"])
        far_expiry = datetime.fromisoformat(signal.metadata["far_expiry"])
        
        # Use ATM strike
        strike = await self._get_spot_price(signal.resource_type)
        position_size = self._calculate_position_size(signal, self.configs[bot_id])
        
        # Sell near-term, buy far-term
        near_order = await self.options_engine.sell_option(
            option_type=OptionType.CALL,
            strike=strike,
            expiry=near_expiry,
            quantity=position_size,
            resource_type=signal.resource_type,
            user_id=bot_id
        )
        
        far_order = await self.options_engine.buy_option(
            option_type=OptionType.CALL,
            strike=strike,
            expiry=far_expiry,
            quantity=position_size,
            resource_type=signal.resource_type,
            user_id=bot_id
        )
        
        net_cost = position_size * (far_order.price - near_order.price)
        
        position = VolArbPosition(
            position_id=f"POS_{bot_id}_{datetime.utcnow().timestamp()}",
            signal_id=signal.signal_id,
            strategy=signal.strategy,
            resource_type=signal.resource_type,
            entry_time=datetime.utcnow(),
            long_options=[far_order.order_id],
            short_options=[near_order.order_id],
            entry_cost=net_cost
        )
        
        return position
        
    async def _execute_variance_trade(self,
                                     bot_id: str,
                                     signal: VolatilitySignal) -> Optional[VolArbPosition]:
        """Execute variance swap trade"""
        # Trade variance swap
        notional = self._calculate_position_size(signal, self.configs[bot_id]) * Decimal("1000")
        
        if signal.metadata["current_premium"] > signal.metadata["historical_premium"]:
            # Sell variance
            var_swap = await self.variance_swaps.sell_variance_swap(
                resource_type=signal.resource_type,
                tenor_days=30,
                variance_strike=Decimal(str(signal.metadata["variance_swap_strike"])),
                notional=notional,
                user_id=bot_id
            )
        else:
            # Buy variance
            var_swap = await self.variance_swaps.buy_variance_swap(
                resource_type=signal.resource_type,
                tenor_days=30,
                variance_strike=Decimal(str(signal.metadata["variance_swap_strike"])),
                notional=notional,
                user_id=bot_id
            )
            
        position = VolArbPosition(
            position_id=f"POS_{bot_id}_{datetime.utcnow().timestamp()}",
            signal_id=signal.signal_id,
            strategy=signal.strategy,
            resource_type=signal.resource_type,
            entry_time=datetime.utcnow(),
            variance_swaps=[var_swap.swap_id],
            entry_cost=Decimal("0")  # Variance swaps have no upfront cost
        )
        
        return position
        
    # Monitoring and risk management
    
    async def _monitor_positions(self) -> None:
        """Monitor open positions and manage risk"""
        while True:
            try:
                for bot_id, positions in self.active_positions.items():
                    config = self.configs[bot_id]
                    
                    for position_id, position in list(positions.items()):
                        if position.is_closed:
                            continue
                            
                        # Update position value and Greeks
                        await self._update_position_metrics(position)
                        
                        # Check exit conditions
                        should_exit, reason = self._check_exit_conditions(position, config)
                        
                        if should_exit:
                            await self._close_position(bot_id, position_id, reason)
                            
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring positions: {e}")
                await asyncio.sleep(60)
                
    async def _update_position_metrics(self, position: VolArbPosition) -> None:
        """Update position value and risk metrics"""
        current_value = Decimal("0")
        total_vega = Decimal("0")
        total_gamma = Decimal("0")
        
        # Value long options
        for option_id in position.long_options:
            option_value = await self.options_engine.get_option_value(option_id)
            current_value += option_value["market_value"]
            total_vega += option_value["vega"]
            total_gamma += option_value["gamma"]
            
        # Value short options
        for option_id in position.short_options:
            option_value = await self.options_engine.get_option_value(option_id)
            current_value -= option_value["market_value"]
            total_vega -= option_value["vega"]
            total_gamma -= option_value["gamma"]
            
        # Value variance swaps
        for swap_id in position.variance_swaps:
            swap_value = await self.variance_swaps.get_swap_value(swap_id)
            current_value += swap_value["mtm_value"]
            total_vega += swap_value["vega_notional"]
            
        position.current_value = current_value
        position.unrealized_pnl = current_value - position.entry_cost
        position.vega_exposure = total_vega
        position.gamma_exposure = total_gamma
        
    def _check_exit_conditions(self,
                              position: VolArbPosition,
                              config: VolArbConfig) -> Tuple[bool, str]:
        """Check if position should be closed"""
        # Profit target
        if position.unrealized_pnl >= position.entry_cost * config.profit_target:
            return True, "profit_target"
            
        # Stop loss
        if position.unrealized_pnl <= -position.entry_cost * config.stop_loss:
            return True, "stop_loss"
            
        # Max holding period
        if datetime.utcnow() - position.entry_time > timedelta(days=config.max_holding_period):
            return True, "max_holding_period"
            
        # Risk limits breached
        if abs(position.vega_exposure) > config.max_vega_exposure:
            return True, "vega_limit"
            
        if abs(position.gamma_exposure) > config.max_gamma_exposure:
            return True, "gamma_limit"
            
        return False, ""
        
    async def _close_position(self,
                             bot_id: str,
                             position_id: str,
                             reason: str) -> None:
        """Close a volatility arbitrage position"""
        position = self.active_positions[bot_id][position_id]
        
        # Close all legs
        for option_id in position.long_options:
            await self.options_engine.close_position(option_id, bot_id)
            
        for option_id in position.short_options:
            await self.options_engine.close_position(option_id, bot_id)
            
        for swap_id in position.variance_swaps:
            await self.variance_swaps.close_position(swap_id, bot_id)
            
        # Update statistics
        position.is_closed = True
        stats = self.statistics[bot_id]
        stats.positions_closed += 1
        stats.total_pnl += position.unrealized_pnl
        
        if position.unrealized_pnl > 0:
            stats.winning_positions += 1
            
        # Publish event
        await self.pulsar.publish('volarb.position_closed', {
            'bot_id': bot_id,
            'position_id': position_id,
            'reason': reason,
            'pnl': float(position.unrealized_pnl),
            'holding_period': (datetime.utcnow() - position.entry_time).total_seconds()
        })
        
    async def close_all_positions(self, bot_id: str) -> Dict[str, Any]:
        """Close all positions for a bot"""
        if bot_id not in self.active_positions:
            return {"closed": 0, "total_pnl": 0}
            
        closed_count = 0
        total_pnl = Decimal("0")
        
        for position_id in list(self.active_positions[bot_id].keys()):
            position = self.active_positions[bot_id][position_id]
            if not position.is_closed:
                await self._close_position(bot_id, position_id, "manual_close")
                closed_count += 1
                total_pnl += position.unrealized_pnl
                
        return {
            "closed": closed_count,
            "total_pnl": float(total_pnl)
        }
        
    # Helper methods
    
    def _calculate_position_size(self,
                                signal: VolatilitySignal,
                                config: VolArbConfig) -> Decimal:
        """Calculate appropriate position size"""
        if config.position_sizing_method == "fixed":
            return config.max_position_size / Decimal("10")  # 10% of max
        elif config.position_sizing_method == "kelly":
            # Simplified Kelly criterion
            win_prob = signal.confidence
            win_loss_ratio = signal.expected_profit / (config.max_position_size * config.stop_loss)
            kelly = (win_prob * win_loss_ratio - (Decimal("1") - win_prob)) / win_loss_ratio
            
            # Apply Kelly fraction
            position_size = config.max_position_size * kelly * config.kelly_fraction
            
            return max(Decimal("0"), min(position_size, config.max_position_size))
        else:
            # Risk parity - size inversely proportional to risk
            return config.max_position_size * (Decimal("1") - signal.risk_score)
            
    def _filter_signals(self,
                       signals: List[VolatilitySignal],
                       config: VolArbConfig) -> List[VolatilitySignal]:
        """Filter signals based on config criteria"""
        filtered = []
        
        for signal in signals:
            if (signal.confidence >= config.min_signal_confidence and
                signal.expected_profit >= config.min_expected_profit and
                signal.risk_score <= config.max_risk_score):
                filtered.append(signal)
                
        return filtered
        
    def _rank_signals(self, signals: List[VolatilitySignal]) -> List[VolatilitySignal]:
        """Rank signals by expected risk-adjusted return"""
        for signal in signals:
            # Simple Sharpe-like ratio
            signal.score = signal.expected_profit / (signal.risk_score + Decimal("0.1"))
            
        return sorted(signals, key=lambda x: x.score, reverse=True)
        
    def _calculate_signal_strength(self,
                                  divergence: Decimal,
                                  threshold: Decimal) -> SignalStrength:
        """Calculate signal strength based on divergence"""
        ratio = abs(divergence) / threshold
        
        if ratio > 3:
            return SignalStrength.STRONG
        elif ratio > 2:
            return SignalStrength.MODERATE
        elif ratio > 1:
            return SignalStrength.WEAK
        else:
            return SignalStrength.NONE
            
    def _calculate_confidence(self,
                             iv_rv_diff: Decimal,
                             rv: Decimal) -> Decimal:
        """Calculate confidence in signal"""
        # Higher confidence for larger relative differences
        relative_diff = abs(iv_rv_diff) / (rv + Decimal("0.1"))
        
        # Sigmoid-like function
        confidence = Decimal("1") / (Decimal("1") + Decimal("-2") * relative_diff).exp()
        
        return min(Decimal("0.95"), confidence)
        
    def _calculate_risk_score(self,
                             series: Dict[str, Any],
                             divergence: Decimal) -> Decimal:
        """Calculate risk score for a signal"""
        # Factors: time to expiry, moneyness, divergence size
        days_to_expiry = (series["expiry"] - datetime.utcnow()).days
        
        # Higher risk for short-dated options
        time_risk = Decimal("1") / (Decimal(days_to_expiry) + Decimal("1"))
        
        # Higher risk for extreme divergences (possible data error)
        divergence_risk = min(Decimal("1"), abs(divergence) / Decimal("0.5"))
        
        return (time_risk + divergence_risk) / Decimal("2")
        
    def _calculate_butterfly_profit(self,
                                   strike1: Decimal,
                                   strike2: Decimal,
                                   strike3: Decimal,
                                   iv1: Decimal,
                                   iv2: Decimal,
                                   iv3: Decimal) -> Decimal:
        """Estimate profit from butterfly arbitrage"""
        # Simplified - actual calculation would use option pricing model
        convexity_violation = abs(iv2 - (iv1 + iv3) / 2)
        notional = (strike3 - strike1) * Decimal("100")  # Position size
        
        return convexity_violation * notional * Decimal("0.1")  # Rough estimate
        
    def _calculate_calendar_profit(self,
                                  near_term: Dict[str, Any],
                                  far_term: Dict[str, Any],
                                  actual_slope: Decimal,
                                  expected_slope: Decimal) -> Decimal:
        """Estimate profit from calendar spread"""
        slope_diff = abs(actual_slope - expected_slope)
        days_diff = (far_term["expiry"] - near_term["expiry"]).days
        
        # Rough estimate based on vega and time
        return slope_diff * Decimal(days_diff) * Decimal("1000")
        
    async def _calculate_realized_volatility(self,
                                           resource_type: str,
                                           lookback_days: int) -> Decimal:
        """Calculate realized volatility over lookback period"""
        # Get historical prices
        prices = await self._get_historical_prices(resource_type, lookback_days)
        
        if len(prices) < 2:
            return Decimal("0.5")  # Default
            
        # Calculate log returns
        returns = []
        for i in range(1, len(prices)):
            ret = np.log(float(prices[i]) / float(prices[i-1]))
            returns.append(ret)
            
        # Annualized volatility
        return Decimal(str(np.std(returns) * np.sqrt(365)))
        
    async def _calculate_variance_premium(self,
                                        resource_type: str,
                                        lookback_days: int) -> Decimal:
        """Calculate historical variance risk premium"""
        # Simplified - would query historical data
        return Decimal("0.002")  # 20 bps default
        
    async def _get_spot_price(self, resource_type: str) -> Decimal:
        """Get current spot price"""
        market_data = await self.spot_market.get_market_data(resource_type)
        return market_data.get("last_price", Decimal("0"))
        
    async def _get_historical_prices(self,
                                    resource_type: str,
                                    days: int) -> List[Decimal]:
        """Get historical price data"""
        # Simplified - would query from database
        current = await self._get_spot_price(resource_type)
        prices = [current]
        
        # Generate synthetic history for demo
        for i in range(days - 1):
            change = Decimal(str(np.random.normal(0, 0.02)))
            prices.append(prices[-1] * (Decimal("1") + change))
            
        return prices[::-1]  # Oldest first
        
    async def _get_active_resource_types(self) -> List[str]:
        """Get list of active resource types"""
        # Would query from configuration
        return ["GPU", "TPU", "CPU", "MEMORY"]
        
    async def _load_historical_volatility(self, resource_type: str) -> None:
        """Load historical volatility data"""
        # Simplified - would load from database
        self.realized_volatility[resource_type] = deque(maxlen=365)
        
        # Generate synthetic data
        for i in range(30):
            vol = Decimal(str(0.3 + np.random.normal(0, 0.05)))
            self.realized_volatility[resource_type].append(vol)
            
    async def _estimate_vega(self, series: Dict[str, Any]) -> Decimal:
        """Estimate vega for an option series"""
        # Simplified - would use proper Greeks calculation
        days_to_expiry = (series["expiry"] - datetime.utcnow()).days
        return Decimal(str(days_to_expiry)) * Decimal("10")
        
    def _can_open_position(self, bot_id: str) -> bool:
        """Check if bot can open new positions"""
        config = self.configs[bot_id]
        positions = self.active_positions[bot_id]
        
        # Check position count
        active_count = sum(1 for p in positions.values() if not p.is_closed)
        if active_count >= 10:  # Max 10 concurrent positions
            return False
            
        # Check exposure limits
        total_vega = sum(p.vega_exposure for p in positions.values() if not p.is_closed)
        total_gamma = sum(p.gamma_exposure for p in positions.values() if not p.is_closed)
        
        if abs(total_vega) > config.max_vega_exposure * Decimal("0.8"):
            return False
            
        if abs(total_gamma) > config.max_gamma_exposure * Decimal("0.8"):
            return False
            
        return True 