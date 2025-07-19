"""
Prediction Signal Trader

Automated trading system that executes strategies based on prediction market signals.
Uses crowd wisdom from prediction markets to inform trading decisions.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any, Set
from decimal import Decimal
from datetime import datetime, timedelta
from enum import Enum
import logging
from dataclasses import dataclass
import numpy as np

from platformq_shared import ServiceClient, ProcessingResult, ProcessingStatus
from app.integrations.prediction_markets import PredictionMarketsIntegration, MarketType

logger = logging.getLogger(__name__)


class SignalStrength(Enum):
    """Signal strength levels"""
    STRONG_BUY = "strong_buy"
    BUY = "buy"
    NEUTRAL = "neutral"
    SELL = "sell"
    STRONG_SELL = "strong_sell"


class TradingStrategy(Enum):
    """Automated trading strategies"""
    MOMENTUM = "momentum"  # Follow strong market sentiment
    CONTRARIAN = "contrarian"  # Trade against extreme sentiment
    MEAN_REVERSION = "mean_reversion"  # Bet on return to average
    BREAKOUT = "breakout"  # Trade on sentiment breakouts
    HEDGED = "hedged"  # Always maintain hedged positions


@dataclass
class TradingSignal:
    """Trading signal generated from prediction markets"""
    timestamp: datetime
    strategy_id: str
    asset: str
    signal_strength: SignalStrength
    confidence: float  # 0-1
    prediction_market_data: Dict[str, Any]
    recommended_action: str
    position_size: Decimal
    stop_loss: Optional[Decimal]
    take_profit: Optional[Decimal]
    reasoning: str


@dataclass
class AutomatedStrategy:
    """Configuration for an automated trading strategy"""
    strategy_id: str
    name: str
    strategy_type: TradingStrategy
    assets: List[str]  # Assets to trade
    min_confidence: float = 0.7
    max_position_size: Decimal = Decimal("10000")
    risk_per_trade: Decimal = Decimal("0.02")  # 2% risk per trade
    enabled: bool = True
    
    # Signal parameters
    sentiment_threshold: float = 0.75  # Sentiment extremes
    volume_threshold: Decimal = Decimal("100000")  # Min market volume
    
    # Risk management
    max_daily_trades: int = 10
    max_open_positions: int = 5
    correlation_limit: float = 0.7


class PredictionSignalTrader:
    """
    Automated trading system based on prediction market signals
    """
    
    def __init__(self):
        self.markets_integration = PredictionMarketsIntegration()
        
        self.derivatives_client = ServiceClient(
            service_name="derivatives-engine-service",
            circuit_breaker_threshold=5,
            rate_limit=100.0
        )
        
        self.risk_client = ServiceClient(
            service_name="derivatives-engine-service",
            circuit_breaker_threshold=5,
            rate_limit=50.0
        )
        
        self.graph_client = ServiceClient(
            service_name="graph-intelligence-service",
            circuit_breaker_threshold=5,
            rate_limit=50.0
        )
        
        # Active strategies
        self.active_strategies: Dict[str, AutomatedStrategy] = {}
        self.open_positions: Dict[str, Dict[str, Any]] = {}
        self.daily_trades: Dict[str, int] = {}
        
        # Performance tracking
        self.signal_history: List[TradingSignal] = []
        self.trade_performance: Dict[str, Dict[str, Any]] = {}
        
    async def create_automated_strategy(
        self,
        tenant_id: str,
        user_id: str,
        config: AutomatedStrategy
    ) -> ProcessingResult:
        """
        Create a new automated trading strategy
        """
        try:
            # Validate strategy configuration
            validation = await self._validate_strategy_config(config)
            if not validation["valid"]:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=validation["reason"]
                )
            
            # Check user permissions and limits
            permissions = await self._check_user_permissions(tenant_id, user_id)
            if not permissions["can_use_automation"]:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="User not authorized for automated trading"
                )
            
            # Store strategy
            self.active_strategies[config.strategy_id] = config
            
            # Initialize performance tracking
            self.trade_performance[config.strategy_id] = {
                "total_trades": 0,
                "winning_trades": 0,
                "total_pnl": Decimal("0"),
                "sharpe_ratio": 0.0,
                "max_drawdown": Decimal("0"),
                "created_at": datetime.utcnow()
            }
            
            logger.info(
                f"Created automated strategy {config.strategy_id} "
                f"type {config.strategy_type.value} for user {user_id}"
            )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "strategy_id": config.strategy_id,
                    "status": "active",
                    "monitoring_assets": config.assets
                }
            )
            
        except Exception as e:
            logger.error(f"Error creating automated strategy: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def monitor_and_trade(
        self,
        tenant_id: str,
        user_id: str,
        strategy_id: str
    ) -> None:
        """
        Monitor prediction markets and execute trades based on signals
        """
        strategy = self.active_strategies.get(strategy_id)
        if not strategy or not strategy.enabled:
            return
        
        try:
            while strategy.enabled:
                # Get market sentiment for all monitored assets
                signals = []
                
                for asset in strategy.assets:
                    # Get prediction market sentiment
                    sentiment = await self.markets_integration.get_strategy_market_sentiment(
                        tenant_id,
                        asset
                    )
                    
                    if "error" not in sentiment:
                        # Analyze sentiment and generate signal
                        signal = await self._analyze_market_sentiment(
                            strategy,
                            asset,
                            sentiment
                        )
                        
                        if signal and signal.confidence >= strategy.min_confidence:
                            signals.append(signal)
                
                # Filter and prioritize signals
                prioritized_signals = await self._prioritize_signals(
                    strategy,
                    signals
                )
                
                # Execute trades for top signals
                for signal in prioritized_signals:
                    if await self._can_execute_trade(strategy, signal):
                        await self._execute_automated_trade(
                            tenant_id,
                            user_id,
                            strategy,
                            signal
                        )
                
                # Wait before next check
                await asyncio.sleep(60)  # Check every minute
                
        except Exception as e:
            logger.error(f"Error in automated trading monitor: {str(e)}")
    
    async def _analyze_market_sentiment(
        self,
        strategy: AutomatedStrategy,
        asset: str,
        sentiment: Dict[str, Any]
    ) -> Optional[TradingSignal]:
        """
        Analyze prediction market sentiment and generate trading signal
        """
        try:
            # Extract key metrics
            bullish_score = sentiment.get("bullish_score", 0.5)
            bearish_score = sentiment.get("bearish_score", 0.5)
            confidence_level = sentiment.get("confidence_level", 0)
            market_predictions = sentiment.get("market_predictions", [])
            
            # Get additional market data
            market_data = await self._get_market_data(asset)
            
            # Apply strategy-specific logic
            signal = None
            
            if strategy.strategy_type == TradingStrategy.MOMENTUM:
                signal = self._momentum_strategy(
                    asset,
                    bullish_score,
                    bearish_score,
                    confidence_level,
                    market_data
                )
                
            elif strategy.strategy_type == TradingStrategy.CONTRARIAN:
                signal = self._contrarian_strategy(
                    asset,
                    bullish_score,
                    bearish_score,
                    confidence_level,
                    market_data
                )
                
            elif strategy.strategy_type == TradingStrategy.MEAN_REVERSION:
                signal = self._mean_reversion_strategy(
                    asset,
                    bullish_score,
                    bearish_score,
                    confidence_level,
                    market_data,
                    market_predictions
                )
                
            elif strategy.strategy_type == TradingStrategy.BREAKOUT:
                signal = self._breakout_strategy(
                    asset,
                    bullish_score,
                    bearish_score,
                    confidence_level,
                    market_data,
                    sentiment
                )
                
            elif strategy.strategy_type == TradingStrategy.HEDGED:
                signal = self._hedged_strategy(
                    asset,
                    bullish_score,
                    bearish_score,
                    confidence_level,
                    market_data
                )
            
            if signal:
                # Enhance signal with ML predictions
                enhanced = await self._enhance_signal_with_ml(signal, market_data)
                if enhanced:
                    signal = enhanced
                
                # Add to history
                self.signal_history.append(signal)
            
            return signal
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment: {str(e)}")
            return None
    
    def _momentum_strategy(
        self,
        asset: str,
        bullish_score: float,
        bearish_score: float,
        confidence: float,
        market_data: Dict[str, Any]
    ) -> Optional[TradingSignal]:
        """
        Momentum strategy: Follow strong market sentiment
        """
        signal_strength = None
        recommended_action = ""
        reasoning = ""
        
        if bullish_score > 0.8 and confidence > 0.7:
            signal_strength = SignalStrength.STRONG_BUY
            recommended_action = "BUY"
            reasoning = f"Strong bullish sentiment ({bullish_score:.2f}) with high confidence"
            
        elif bullish_score > 0.65:
            signal_strength = SignalStrength.BUY
            recommended_action = "BUY"
            reasoning = f"Moderate bullish sentiment ({bullish_score:.2f})"
            
        elif bearish_score > 0.8 and confidence > 0.7:
            signal_strength = SignalStrength.STRONG_SELL
            recommended_action = "SELL"
            reasoning = f"Strong bearish sentiment ({bearish_score:.2f}) with high confidence"
            
        elif bearish_score > 0.65:
            signal_strength = SignalStrength.SELL
            recommended_action = "SELL"
            reasoning = f"Moderate bearish sentiment ({bearish_score:.2f})"
            
        else:
            return None
        
        # Calculate position size based on confidence
        position_size = self._calculate_position_size(
            confidence,
            market_data.get("volatility", 0.3)
        )
        
        # Set stops and targets
        current_price = Decimal(str(market_data.get("current_price", 0)))
        atr = Decimal(str(market_data.get("atr", current_price * Decimal("0.02"))))
        
        if recommended_action == "BUY":
            stop_loss = current_price - (atr * 2)
            take_profit = current_price + (atr * 3)
        else:
            stop_loss = current_price + (atr * 2)
            take_profit = current_price - (atr * 3)
        
        return TradingSignal(
            timestamp=datetime.utcnow(),
            strategy_id="momentum",
            asset=asset,
            signal_strength=signal_strength,
            confidence=confidence,
            prediction_market_data={
                "bullish_score": bullish_score,
                "bearish_score": bearish_score
            },
            recommended_action=recommended_action,
            position_size=position_size,
            stop_loss=stop_loss,
            take_profit=take_profit,
            reasoning=reasoning
        )
    
    def _contrarian_strategy(
        self,
        asset: str,
        bullish_score: float,
        bearish_score: float,
        confidence: float,
        market_data: Dict[str, Any]
    ) -> Optional[TradingSignal]:
        """
        Contrarian strategy: Trade against extreme sentiment
        """
        signal_strength = None
        recommended_action = ""
        reasoning = ""
        
        # Look for extreme sentiment to fade
        if bullish_score > 0.9 and confidence > 0.8:
            signal_strength = SignalStrength.SELL
            recommended_action = "SELL"
            reasoning = f"Extreme bullish sentiment ({bullish_score:.2f}) - contrarian sell"
            
        elif bearish_score > 0.9 and confidence > 0.8:
            signal_strength = SignalStrength.BUY
            recommended_action = "BUY"
            reasoning = f"Extreme bearish sentiment ({bearish_score:.2f}) - contrarian buy"
            
        else:
            return None
        
        # Contrarian trades use smaller position sizes
        position_size = self._calculate_position_size(
            confidence * 0.7,  # Reduce confidence for contrarian
            market_data.get("volatility", 0.3)
        )
        
        current_price = Decimal(str(market_data.get("current_price", 0)))
        atr = Decimal(str(market_data.get("atr", current_price * Decimal("0.02"))))
        
        # Tighter stops for contrarian trades
        if recommended_action == "BUY":
            stop_loss = current_price - (atr * 1.5)
            take_profit = current_price + (atr * 2.5)
        else:
            stop_loss = current_price + (atr * 1.5)
            take_profit = current_price - (atr * 2.5)
        
        return TradingSignal(
            timestamp=datetime.utcnow(),
            strategy_id="contrarian",
            asset=asset,
            signal_strength=signal_strength,
            confidence=confidence * 0.7,
            prediction_market_data={
                "bullish_score": bullish_score,
                "bearish_score": bearish_score
            },
            recommended_action=recommended_action,
            position_size=position_size,
            stop_loss=stop_loss,
            take_profit=take_profit,
            reasoning=reasoning
        )
    
    def _mean_reversion_strategy(
        self,
        asset: str,
        bullish_score: float,
        bearish_score: float,
        confidence: float,
        market_data: Dict[str, Any],
        market_predictions: List[Dict[str, Any]]
    ) -> Optional[TradingSignal]:
        """
        Mean reversion strategy: Trade expecting return to average sentiment
        """
        # Calculate average sentiment over time
        if not market_predictions:
            return None
        
        avg_bullish = np.mean([
            p.get("bullish_probability", 0.5)
            for p in market_predictions
        ])
        
        deviation = abs(bullish_score - avg_bullish)
        
        if deviation < 0.15:  # Not enough deviation
            return None
        
        signal_strength = None
        recommended_action = ""
        reasoning = ""
        
        if bullish_score > avg_bullish + 0.2:
            # Sentiment too bullish, expect reversion down
            signal_strength = SignalStrength.SELL
            recommended_action = "SELL"
            reasoning = f"Sentiment {bullish_score:.2f} above average {avg_bullish:.2f}"
            
        elif bullish_score < avg_bullish - 0.2:
            # Sentiment too bearish, expect reversion up
            signal_strength = SignalStrength.BUY
            recommended_action = "BUY"
            reasoning = f"Sentiment {bullish_score:.2f} below average {avg_bullish:.2f}"
            
        else:
            return None
        
        position_size = self._calculate_position_size(
            confidence * deviation,  # Size based on deviation
            market_data.get("volatility", 0.3)
        )
        
        current_price = Decimal(str(market_data.get("current_price", 0)))
        atr = Decimal(str(market_data.get("atr", current_price * Decimal("0.02"))))
        
        # Targets based on mean reversion
        if recommended_action == "BUY":
            stop_loss = current_price - (atr * 2)
            take_profit = current_price + (atr * 1.5)  # Smaller profit target
        else:
            stop_loss = current_price + (atr * 2)
            take_profit = current_price - (atr * 1.5)
        
        return TradingSignal(
            timestamp=datetime.utcnow(),
            strategy_id="mean_reversion",
            asset=asset,
            signal_strength=signal_strength,
            confidence=confidence * deviation,
            prediction_market_data={
                "bullish_score": bullish_score,
                "avg_bullish": avg_bullish,
                "deviation": deviation
            },
            recommended_action=recommended_action,
            position_size=position_size,
            stop_loss=stop_loss,
            take_profit=take_profit,
            reasoning=reasoning
        )
    
    def _breakout_strategy(
        self,
        asset: str,
        bullish_score: float,
        bearish_score: float,
        confidence: float,
        market_data: Dict[str, Any],
        sentiment: Dict[str, Any]
    ) -> Optional[TradingSignal]:
        """
        Breakout strategy: Trade on sentiment regime changes
        """
        # Check for sentiment breakout
        market_predictions = sentiment.get("market_predictions", [])
        if len(market_predictions) < 3:
            return None
        
        # Get recent sentiment trend
        recent_sentiments = [
            p.get("bullish_probability", 0.5)
            for p in market_predictions[-3:]
        ]
        
        # Check for breakout
        is_bullish_breakout = (
            bullish_score > 0.7 and
            all(s < 0.5 for s in recent_sentiments) and
            confidence > 0.6
        )
        
        is_bearish_breakout = (
            bearish_score > 0.7 and
            all(s > 0.5 for s in recent_sentiments) and
            confidence > 0.6
        )
        
        if not (is_bullish_breakout or is_bearish_breakout):
            return None
        
        if is_bullish_breakout:
            signal_strength = SignalStrength.STRONG_BUY
            recommended_action = "BUY"
            reasoning = "Bullish sentiment breakout detected"
        else:
            signal_strength = SignalStrength.STRONG_SELL
            recommended_action = "SELL"
            reasoning = "Bearish sentiment breakout detected"
        
        # Larger position for breakouts
        position_size = self._calculate_position_size(
            confidence * 1.2,  # Increase size for breakouts
            market_data.get("volatility", 0.3)
        )
        
        current_price = Decimal(str(market_data.get("current_price", 0)))
        atr = Decimal(str(market_data.get("atr", current_price * Decimal("0.02"))))
        
        # Wider targets for breakouts
        if recommended_action == "BUY":
            stop_loss = current_price - (atr * 1.5)
            take_profit = current_price + (atr * 4)
        else:
            stop_loss = current_price + (atr * 1.5)
            take_profit = current_price - (atr * 4)
        
        return TradingSignal(
            timestamp=datetime.utcnow(),
            strategy_id="breakout",
            asset=asset,
            signal_strength=signal_strength,
            confidence=confidence,
            prediction_market_data={
                "bullish_score": bullish_score,
                "bearish_score": bearish_score,
                "breakout_type": "bullish" if is_bullish_breakout else "bearish"
            },
            recommended_action=recommended_action,
            position_size=position_size,
            stop_loss=stop_loss,
            take_profit=take_profit,
            reasoning=reasoning
        )
    
    def _hedged_strategy(
        self,
        asset: str,
        bullish_score: float,
        bearish_score: float,
        confidence: float,
        market_data: Dict[str, Any]
    ) -> Optional[TradingSignal]:
        """
        Hedged strategy: Always maintain hedged positions
        """
        # This strategy creates pairs of positions to hedge risk
        # For simplicity, returning single signals that would be paired
        
        if abs(bullish_score - 0.5) < 0.1:
            return None  # Too neutral
        
        primary_action = "BUY" if bullish_score > 0.5 else "SELL"
        
        signal_strength = SignalStrength.BUY if primary_action == "BUY" else SignalStrength.SELL
        
        # Hedged positions use moderate sizing
        position_size = self._calculate_position_size(
            confidence * 0.8,
            market_data.get("volatility", 0.3)
        )
        
        current_price = Decimal(str(market_data.get("current_price", 0)))
        atr = Decimal(str(market_data.get("atr", current_price * Decimal("0.02"))))
        
        # Conservative targets for hedged positions
        if primary_action == "BUY":
            stop_loss = current_price - (atr * 2.5)
            take_profit = current_price + (atr * 2)
        else:
            stop_loss = current_price + (atr * 2.5)
            take_profit = current_price - (atr * 2)
        
        return TradingSignal(
            timestamp=datetime.utcnow(),
            strategy_id="hedged",
            asset=asset,
            signal_strength=signal_strength,
            confidence=confidence,
            prediction_market_data={
                "bullish_score": bullish_score,
                "bearish_score": bearish_score,
                "hedge_required": True
            },
            recommended_action=primary_action,
            position_size=position_size,
            stop_loss=stop_loss,
            take_profit=take_profit,
            reasoning=f"Hedged {primary_action} position"
        )
    
    def _calculate_position_size(
        self,
        confidence: float,
        volatility: float
    ) -> Decimal:
        """
        Calculate position size based on confidence and volatility
        """
        # Base size adjusted by confidence
        base_size = Decimal("1000") * Decimal(str(confidence))
        
        # Adjust for volatility (inverse relationship)
        volatility_adjustment = Decimal("1") / (Decimal("1") + Decimal(str(volatility)))
        
        return base_size * volatility_adjustment
    
    async def _get_market_data(self, asset: str) -> Dict[str, Any]:
        """
        Get current market data for asset
        """
        # This would fetch real market data
        # For now, return mock data
        return {
            "current_price": 50000 if "BTC" in asset else 3000,
            "volatility": 0.3,
            "atr": 1000 if "BTC" in asset else 50,
            "volume_24h": 1000000000,
            "price_change_24h": 0.02
        }
    
    async def _enhance_signal_with_ml(
        self,
        signal: TradingSignal,
        market_data: Dict[str, Any]
    ) -> Optional[TradingSignal]:
        """
        Enhance trading signal with ML predictions
        """
        try:
            # Call graph intelligence service for pattern analysis
            response = await self.graph_client.post(
                "/api/v1/patterns/analyze",
                json={
                    "asset": signal.asset,
                    "signal_type": signal.signal_strength.value,
                    "market_data": market_data
                }
            )
            
            if response.status_code == 200:
                ml_insights = response.json()
                
                # Adjust confidence based on ML
                if ml_insights.get("pattern_confidence", 0) > 0.8:
                    signal.confidence *= 1.2
                elif ml_insights.get("pattern_confidence", 0) < 0.3:
                    signal.confidence *= 0.8
                
                # Update reasoning
                signal.reasoning += f". ML pattern: {ml_insights.get('pattern_name', 'unknown')}"
                
            return signal
            
        except Exception as e:
            logger.error(f"Error enhancing signal with ML: {str(e)}")
            return signal
    
    async def _prioritize_signals(
        self,
        strategy: AutomatedStrategy,
        signals: List[TradingSignal]
    ) -> List[TradingSignal]:
        """
        Prioritize and filter trading signals
        """
        if not signals:
            return []
        
        # Sort by confidence and signal strength
        strength_scores = {
            SignalStrength.STRONG_BUY: 5,
            SignalStrength.BUY: 4,
            SignalStrength.NEUTRAL: 3,
            SignalStrength.SELL: 2,
            SignalStrength.STRONG_SELL: 1
        }
        
        signals.sort(
            key=lambda s: (s.confidence, strength_scores.get(s.signal_strength, 0)),
            reverse=True
        )
        
        # Check correlation between signals
        filtered_signals = []
        selected_assets = set()
        
        for signal in signals:
            # Skip if we already have a signal for a correlated asset
            if not await self._is_correlated_asset(
                signal.asset,
                selected_assets,
                strategy.correlation_limit
            ):
                filtered_signals.append(signal)
                selected_assets.add(signal.asset)
                
                # Limit number of signals
                if len(filtered_signals) >= strategy.max_open_positions:
                    break
        
        return filtered_signals
    
    async def _is_correlated_asset(
        self,
        asset: str,
        selected_assets: Set[str],
        correlation_limit: float
    ) -> bool:
        """
        Check if asset is highly correlated with already selected assets
        """
        # This would check actual correlations
        # For now, simple logic
        if not selected_assets:
            return False
        
        # Assume high correlation between similar assets
        for selected in selected_assets:
            if asset[:3] == selected[:3]:  # Same base currency
                return True
        
        return False
    
    async def _can_execute_trade(
        self,
        strategy: AutomatedStrategy,
        signal: TradingSignal
    ) -> bool:
        """
        Check if trade can be executed based on limits and risk
        """
        # Check daily trade limit
        today = datetime.utcnow().date()
        daily_key = f"{strategy.strategy_id}:{today}"
        
        if self.daily_trades.get(daily_key, 0) >= strategy.max_daily_trades:
            logger.warning(f"Daily trade limit reached for {strategy.strategy_id}")
            return False
        
        # Check open positions limit
        strategy_positions = [
            p for p in self.open_positions.values()
            if p.get("strategy_id") == strategy.strategy_id
        ]
        
        if len(strategy_positions) >= strategy.max_open_positions:
            logger.warning(f"Open position limit reached for {strategy.strategy_id}")
            return False
        
        # Check if already have position in this asset
        for pos in strategy_positions:
            if pos.get("asset") == signal.asset:
                logger.info(f"Already have position in {signal.asset}")
                return False
        
        return True
    
    async def _execute_automated_trade(
        self,
        tenant_id: str,
        user_id: str,
        strategy: AutomatedStrategy,
        signal: TradingSignal
    ) -> ProcessingResult:
        """
        Execute automated trade based on signal
        """
        try:
            # Prepare order
            order_request = {
                "tenant_id": tenant_id,
                "user_id": user_id,
                "asset": signal.asset,
                "side": signal.recommended_action,
                "quantity": float(signal.position_size),
                "order_type": "MARKET",
                "metadata": {
                    "strategy_id": strategy.strategy_id,
                    "signal_confidence": signal.confidence,
                    "signal_strength": signal.signal_strength.value,
                    "automated": True
                }
            }
            
            # Execute trade
            response = await self.derivatives_client.post(
                "/api/v1/trading/orders",
                json=order_request
            )
            
            if response.status_code == 201:
                order_data = response.json()
                
                # Track position
                self.open_positions[order_data["order_id"]] = {
                    "strategy_id": strategy.strategy_id,
                    "asset": signal.asset,
                    "side": signal.recommended_action,
                    "quantity": signal.position_size,
                    "entry_price": order_data.get("fill_price"),
                    "stop_loss": signal.stop_loss,
                    "take_profit": signal.take_profit,
                    "signal": signal,
                    "opened_at": datetime.utcnow()
                }
                
                # Update daily trades
                today = datetime.utcnow().date()
                daily_key = f"{strategy.strategy_id}:{today}"
                self.daily_trades[daily_key] = self.daily_trades.get(daily_key, 0) + 1
                
                # Update performance tracking
                perf = self.trade_performance[strategy.strategy_id]
                perf["total_trades"] += 1
                
                # Set stop loss and take profit orders
                await self._set_exit_orders(
                    tenant_id,
                    user_id,
                    order_data["order_id"],
                    signal
                )
                
                logger.info(
                    f"Executed automated trade: {signal.recommended_action} "
                    f"{signal.position_size} {signal.asset} for strategy {strategy.strategy_id}"
                )
                
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    data=order_data
                )
            else:
                logger.error(f"Failed to execute trade: {response.text}")
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=response.text
                )
                
        except Exception as e:
            logger.error(f"Error executing automated trade: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def _set_exit_orders(
        self,
        tenant_id: str,
        user_id: str,
        position_id: str,
        signal: TradingSignal
    ) -> None:
        """
        Set stop loss and take profit orders
        """
        try:
            # Stop loss order
            if signal.stop_loss:
                stop_order = {
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "asset": signal.asset,
                    "side": "SELL" if signal.recommended_action == "BUY" else "BUY",
                    "quantity": float(signal.position_size),
                    "order_type": "STOP",
                    "stop_price": float(signal.stop_loss),
                    "metadata": {
                        "parent_position": position_id,
                        "order_purpose": "stop_loss"
                    }
                }
                
                await self.derivatives_client.post(
                    "/api/v1/trading/orders",
                    json=stop_order
                )
            
            # Take profit order
            if signal.take_profit:
                profit_order = {
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "asset": signal.asset,
                    "side": "SELL" if signal.recommended_action == "BUY" else "BUY",
                    "quantity": float(signal.position_size),
                    "order_type": "LIMIT",
                    "limit_price": float(signal.take_profit),
                    "metadata": {
                        "parent_position": position_id,
                        "order_purpose": "take_profit"
                    }
                }
                
                await self.derivatives_client.post(
                    "/api/v1/trading/orders",
                    json=profit_order
                )
                
        except Exception as e:
            logger.error(f"Error setting exit orders: {str(e)}")
    
    async def get_strategy_performance(
        self,
        strategy_id: str
    ) -> Dict[str, Any]:
        """
        Get performance metrics for an automated strategy
        """
        if strategy_id not in self.trade_performance:
            return {"error": "Strategy not found"}
        
        perf = self.trade_performance[strategy_id]
        
        # Calculate additional metrics
        win_rate = (
            perf["winning_trades"] / perf["total_trades"]
            if perf["total_trades"] > 0 else 0
        )
        
        avg_pnl = (
            perf["total_pnl"] / perf["total_trades"]
            if perf["total_trades"] > 0 else Decimal("0")
        )
        
        return {
            "strategy_id": strategy_id,
            "total_trades": perf["total_trades"],
            "winning_trades": perf["winning_trades"],
            "win_rate": win_rate,
            "total_pnl": str(perf["total_pnl"]),
            "average_pnl": str(avg_pnl),
            "sharpe_ratio": perf["sharpe_ratio"],
            "max_drawdown": str(perf["max_drawdown"]),
            "active_since": perf["created_at"].isoformat(),
            "open_positions": len([
                p for p in self.open_positions.values()
                if p.get("strategy_id") == strategy_id
            ])
        }
    
    async def stop_strategy(self, strategy_id: str) -> ProcessingResult:
        """
        Stop an automated trading strategy
        """
        if strategy_id in self.active_strategies:
            self.active_strategies[strategy_id].enabled = False
            
            # Close all open positions for this strategy
            positions_to_close = [
                (pos_id, pos) for pos_id, pos in self.open_positions.items()
                if pos.get("strategy_id") == strategy_id
            ]
            
            for pos_id, pos in positions_to_close:
                # Close position at market
                # Implementation would close actual positions
                del self.open_positions[pos_id]
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "strategy_id": strategy_id,
                    "status": "stopped",
                    "positions_closed": len(positions_to_close)
                }
            )
        else:
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error="Strategy not found"
            )
    
    async def _validate_strategy_config(
        self,
        config: AutomatedStrategy
    ) -> Dict[str, Any]:
        """
        Validate strategy configuration
        """
        if config.min_confidence < 0 or config.min_confidence > 1:
            return {"valid": False, "reason": "Invalid confidence range"}
        
        if config.risk_per_trade > Decimal("0.1"):  # Max 10% risk
            return {"valid": False, "reason": "Risk per trade too high"}
        
        if config.max_daily_trades > 50:
            return {"valid": False, "reason": "Daily trade limit too high"}
        
        if not config.assets:
            return {"valid": False, "reason": "No assets specified"}
        
        return {"valid": True}
    
    async def _check_user_permissions(
        self,
        tenant_id: str,
        user_id: str
    ) -> Dict[str, bool]:
        """
        Check if user has permissions for automated trading
        """
        # This would check actual permissions
        # For now, return mock data
        return {
            "can_use_automation": True,
            "max_strategies": 5,
            "current_strategies": len([
                s for s in self.active_strategies.values()
                if s.enabled
            ])
        } 