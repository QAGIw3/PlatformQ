"""
Trading Core Integration for Market Intelligence Service

Provides real-time market insights and predictive analytics to trading-core-service.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
import asyncio
import numpy as np
from dataclasses import dataclass
from enum import Enum

from platformq_shared import ServiceClient
from ..analytics.ml_models import MarketPredictor, VolatilityModel, SentimentAnalyzer
from ..oracle.price_aggregator import PriceAggregator

logger = logging.getLogger(__name__)


class SignalType(Enum):
    """Types of trading signals"""
    BUY_STRONG = "buy_strong"
    BUY = "buy"
    NEUTRAL = "neutral"
    SELL = "sell"
    SELL_STRONG = "sell_strong"


class RiskLevel(Enum):
    """Market risk levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    EXTREME = "extreme"


@dataclass
class MarketInsight:
    """Market insight data"""
    market_id: str
    signal: SignalType
    confidence: float  # 0-1
    price_prediction: Decimal
    volatility_forecast: Decimal
    risk_level: RiskLevel
    support_levels: List[Decimal]
    resistance_levels: List[Decimal]
    volume_analysis: Dict[str, Any]
    timestamp: datetime


@dataclass
class TradingRecommendation:
    """Trading recommendation based on ML analysis"""
    market_id: str
    action: str  # "enter", "exit", "hold", "scale_in", "scale_out"
    position_size: Decimal
    entry_price: Optional[Decimal]
    stop_loss: Optional[Decimal]
    take_profit: Optional[Decimal]
    reasoning: List[str]
    confidence: float
    valid_until: datetime


class TradingCoreMarketIntelligence:
    """Provides market intelligence to trading-core-service"""
    
    def __init__(self):
        self.trading_core_client = ServiceClient(
            service_name="trading-core-service",
            circuit_breaker_threshold=5,
            rate_limit=100.0
        )
        
        # ML models (would be properly initialized)
        self.market_predictor = MarketPredictor()
        self.volatility_model = VolatilityModel()
        self.sentiment_analyzer = SentimentAnalyzer()
        self.price_aggregator = PriceAggregator()
        
        # Cache for market data
        self.market_cache: Dict[str, List[Tuple[datetime, Dict]]] = {}
        self.cache_ttl = timedelta(minutes=5)
        
        # Active subscriptions
        self.subscriptions: Dict[str, Dict[str, Any]] = {}
        
        # Background tasks
        self._analysis_task = None
        self._subscription_task = None
        
    async def initialize(self):
        """Initialize the integration"""
        logger.info("Initializing Trading Core Market Intelligence integration")
        
        # Initialize ML models
        await self.market_predictor.load_models()
        await self.volatility_model.load_models()
        await self.sentiment_analyzer.initialize()
        
        # Start background tasks
        self._analysis_task = asyncio.create_task(self._continuous_analysis_loop())
        self._subscription_task = asyncio.create_task(self._subscription_handler_loop())
        
        # Register with trading-core as intelligence provider
        await self._register_as_provider()
        
    async def subscribe_market_insights(
        self,
        market_id: str,
        callback_url: Optional[str] = None,
        indicators: List[str] = None
    ) -> str:
        """Subscribe to real-time market insights"""
        subscription_id = f"SUB_{market_id}_{datetime.utcnow().timestamp()}"
        
        self.subscriptions[subscription_id] = {
            "market_id": market_id,
            "callback_url": callback_url,
            "indicators": indicators or ["all"],
            "created_at": datetime.utcnow()
        }
        
        logger.info(f"Created market insight subscription: {subscription_id}")
        return subscription_id
        
    async def get_market_insight(self, market_id: str) -> MarketInsight:
        """Get current market insight with ML predictions"""
        # Get market data from trading-core
        market_data = await self._fetch_market_data(market_id)
        
        if not market_data:
            raise ValueError(f"No market data available for {market_id}")
            
        # Run ML analysis
        price_pred = await self.market_predictor.predict_price(
            market_id=market_id,
            historical_data=market_data["price_history"],
            features=market_data.get("features", {})
        )
        
        volatility = await self.volatility_model.forecast_volatility(
            market_id=market_id,
            returns=market_data["returns"],
            horizon=24  # 24 hour forecast
        )
        
        sentiment = await self.sentiment_analyzer.analyze_market_sentiment(
            market_id=market_id
        )
        
        # Technical analysis
        support_resistance = self._calculate_support_resistance(
            market_data["price_history"]
        )
        
        # Volume analysis
        volume_analysis = self._analyze_volume_patterns(
            market_data["volume_history"]
        )
        
        # Generate trading signal
        signal = self._generate_signal(
            price_prediction=price_pred["prediction"],
            current_price=Decimal(market_data["current_price"]),
            volatility=volatility["forecast"],
            sentiment=sentiment["score"],
            volume_trend=volume_analysis["trend"]
        )
        
        # Assess risk level
        risk_level = self._assess_risk_level(
            volatility=volatility["forecast"],
            market_conditions=market_data.get("conditions", {})
        )
        
        return MarketInsight(
            market_id=market_id,
            signal=signal["type"],
            confidence=signal["confidence"],
            price_prediction=Decimal(str(price_pred["prediction"])),
            volatility_forecast=Decimal(str(volatility["forecast"])),
            risk_level=risk_level,
            support_levels=support_resistance["support"],
            resistance_levels=support_resistance["resistance"],
            volume_analysis=volume_analysis,
            timestamp=datetime.utcnow()
        )
        
    async def get_trading_recommendation(
        self,
        market_id: str,
        user_risk_profile: Dict[str, Any],
        current_position: Optional[Dict[str, Any]] = None
    ) -> TradingRecommendation:
        """Get personalized trading recommendation"""
        # Get market insight
        insight = await self.get_market_insight(market_id)
        
        # Consider user risk profile
        risk_tolerance = user_risk_profile.get("tolerance", "medium")
        max_position_size = Decimal(str(user_risk_profile.get("max_position_size", "1000")))
        
        # Analyze current position if exists
        if current_position:
            return await self._analyze_existing_position(
                insight, current_position, risk_tolerance
            )
        else:
            return await self._generate_new_position_recommendation(
                insight, risk_tolerance, max_position_size
            )
            
    async def analyze_market_regime(self, market_id: str) -> Dict[str, Any]:
        """Analyze current market regime (trending, ranging, volatile)"""
        market_data = await self._fetch_market_data(market_id)
        
        # Use ML to classify market regime
        regime = await self.market_predictor.classify_regime(
            price_data=market_data["price_history"],
            volume_data=market_data["volume_history"]
        )
        
        # Calculate regime-specific metrics
        if regime["type"] == "trending":
            trend_strength = self._calculate_trend_strength(
                market_data["price_history"]
            )
            regime["trend_strength"] = trend_strength
            
        elif regime["type"] == "ranging":
            range_bounds = self._calculate_range_bounds(
                market_data["price_history"]
            )
            regime["range_bounds"] = range_bounds
            
        return regime
        
    async def predict_liquidity_needs(
        self,
        market_id: str,
        horizon_hours: int = 24
    ) -> Dict[str, Any]:
        """Predict future liquidity needs for market makers"""
        # Analyze historical patterns
        market_data = await self._fetch_market_data(market_id)
        
        # Use ML to predict volume and spread
        liquidity_forecast = await self.market_predictor.forecast_liquidity(
            historical_volume=market_data["volume_history"],
            historical_spread=market_data["spread_history"],
            horizon=horizon_hours
        )
        
        # Identify high-liquidity periods
        peak_periods = self._identify_peak_liquidity_periods(
            liquidity_forecast["hourly_forecast"]
        )
        
        return {
            "market_id": market_id,
            "forecast_horizon_hours": horizon_hours,
            "predicted_volume": liquidity_forecast["total_volume"],
            "average_spread": liquidity_forecast["avg_spread"],
            "peak_periods": peak_periods,
            "confidence_intervals": liquidity_forecast["confidence_intervals"],
            "recommendations": self._generate_liquidity_recommendations(
                liquidity_forecast
            )
        }
        
    async def detect_anomalies(self, market_id: str) -> List[Dict[str, Any]]:
        """Detect market anomalies and potential manipulation"""
        market_data = await self._fetch_market_data(market_id)
        
        anomalies = []
        
        # Price anomalies
        price_anomalies = await self.market_predictor.detect_price_anomalies(
            price_data=market_data["price_history"],
            volume_data=market_data["volume_history"]
        )
        anomalies.extend(price_anomalies)
        
        # Volume anomalies
        volume_anomalies = self._detect_volume_anomalies(
            market_data["volume_history"]
        )
        anomalies.extend(volume_anomalies)
        
        # Order flow anomalies
        if "order_flow" in market_data:
            flow_anomalies = self._detect_order_flow_anomalies(
                market_data["order_flow"]
            )
            anomalies.extend(flow_anomalies)
            
        # Assign severity scores
        for anomaly in anomalies:
            anomaly["severity"] = self._calculate_anomaly_severity(anomaly)
            
        return sorted(anomalies, key=lambda x: x["severity"], reverse=True)
        
    async def _fetch_market_data(self, market_id: str) -> Dict[str, Any]:
        """Fetch market data from trading-core"""
        # Check cache first
        if market_id in self.market_cache:
            cache_data = self.market_cache[market_id]
            if cache_data and (datetime.utcnow() - cache_data[-1][0]) < self.cache_ttl:
                return cache_data[-1][1]
                
        # Fetch from trading-core
        try:
            # Get orderbook
            orderbook = await self.trading_core_client.request(
                method="GET",
                path=f"/api/v1/markets/{market_id}/orderbook",
                params={"depth": 50}
            )
            
            # Get recent trades
            trades = await self.trading_core_client.request(
                method="GET",
                path=f"/api/v1/markets/{market_id}/trades",
                params={"limit": 1000}
            )
            
            # Get historical prices (would come from a data service)
            price_history = await self._fetch_price_history(market_id)
            
            market_data = {
                "market_id": market_id,
                "current_price": self._calculate_mid_price(orderbook),
                "orderbook": orderbook,
                "recent_trades": trades,
                "price_history": price_history,
                "volume_history": self._extract_volume_history(trades),
                "spread_history": self._calculate_spread_history(orderbook),
                "returns": self._calculate_returns(price_history),
                "timestamp": datetime.utcnow()
            }
            
            # Update cache
            if market_id not in self.market_cache:
                self.market_cache[market_id] = []
            self.market_cache[market_id].append((datetime.utcnow(), market_data))
            
            # Trim old cache entries
            cutoff = datetime.utcnow() - timedelta(hours=24)
            self.market_cache[market_id] = [
                (ts, data) for ts, data in self.market_cache[market_id]
                if ts > cutoff
            ]
            
            return market_data
            
        except Exception as e:
            logger.error(f"Failed to fetch market data for {market_id}: {e}")
            return {}
            
    async def _register_as_provider(self):
        """Register as intelligence provider with trading-core"""
        try:
            # This would register the service as a provider of market intelligence
            # Trading-core could then query this service for insights
            pass
        except Exception as e:
            logger.error(f"Failed to register with trading-core: {e}")
            
    async def _continuous_analysis_loop(self):
        """Continuously analyze subscribed markets"""
        while True:
            try:
                for sub_id, subscription in list(self.subscriptions.items()):
                    market_id = subscription["market_id"]
                    
                    # Get latest insight
                    insight = await self.get_market_insight(market_id)
                    
                    # Check for significant changes
                    if self._is_significant_change(market_id, insight):
                        # Notify subscriber
                        await self._notify_subscriber(sub_id, insight)
                        
                await asyncio.sleep(30)  # Analyze every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in analysis loop: {e}")
                await asyncio.sleep(60)
                
    def _generate_signal(
        self,
        price_prediction: Decimal,
        current_price: Decimal,
        volatility: Decimal,
        sentiment: float,
        volume_trend: str
    ) -> Dict[str, Any]:
        """Generate trading signal based on multiple factors"""
        # Calculate expected return
        expected_return = (price_prediction - current_price) / current_price
        
        # Adjust for volatility
        risk_adjusted_return = expected_return / (volatility + Decimal("0.01"))
        
        # Incorporate sentiment
        sentiment_multiplier = 1 + (sentiment - 0.5) * 0.2
        
        # Volume confirmation
        volume_multiplier = {
            "increasing": 1.1,
            "stable": 1.0,
            "decreasing": 0.9
        }.get(volume_trend, 1.0)
        
        # Calculate final score
        score = float(risk_adjusted_return) * sentiment_multiplier * volume_multiplier
        
        # Determine signal type
        if score > 0.1:
            signal_type = SignalType.BUY_STRONG if score > 0.2 else SignalType.BUY
        elif score < -0.1:
            signal_type = SignalType.SELL_STRONG if score < -0.2 else SignalType.SELL
        else:
            signal_type = SignalType.NEUTRAL
            
        # Calculate confidence
        confidence = min(abs(score) * 2, 1.0)
        
        return {
            "type": signal_type,
            "score": score,
            "confidence": confidence
        }
        
    def _assess_risk_level(
        self,
        volatility: Decimal,
        market_conditions: Dict[str, Any]
    ) -> RiskLevel:
        """Assess current market risk level"""
        vol_float = float(volatility)
        
        # Base risk on volatility
        if vol_float < 0.15:
            base_risk = RiskLevel.LOW
        elif vol_float < 0.25:
            base_risk = RiskLevel.MEDIUM
        elif vol_float < 0.40:
            base_risk = RiskLevel.HIGH
        else:
            base_risk = RiskLevel.EXTREME
            
        # Adjust for market conditions
        if market_conditions.get("circuit_breaker_near", False):
            # Increase risk level if near circuit breaker
            risk_levels = list(RiskLevel)
            current_idx = risk_levels.index(base_risk)
            if current_idx < len(risk_levels) - 1:
                base_risk = risk_levels[current_idx + 1]
                
        return base_risk
        
    # Additional helper methods...
    def _calculate_support_resistance(
        self,
        price_history: List[Tuple[datetime, Decimal]]
    ) -> Dict[str, List[Decimal]]:
        """Calculate support and resistance levels"""
        # Simplified implementation
        prices = [p for _, p in price_history[-100:]]  # Last 100 prices
        
        if not prices:
            return {"support": [], "resistance": []}
            
        # Find local minima and maxima
        support_levels = []
        resistance_levels = []
        
        for i in range(2, len(prices) - 2):
            # Local minimum (support)
            if (prices[i] <= prices[i-1] and prices[i] <= prices[i-2] and
                prices[i] <= prices[i+1] and prices[i] <= prices[i+2]):
                support_levels.append(prices[i])
                
            # Local maximum (resistance)
            if (prices[i] >= prices[i-1] and prices[i] >= prices[i-2] and
                prices[i] >= prices[i+1] and prices[i] >= prices[i+2]):
                resistance_levels.append(prices[i])
                
        # Sort and deduplicate
        support_levels = sorted(list(set(support_levels)))[-3:]  # Top 3
        resistance_levels = sorted(list(set(resistance_levels)))[:3]  # Bottom 3
        
        return {
            "support": support_levels,
            "resistance": resistance_levels
        } 