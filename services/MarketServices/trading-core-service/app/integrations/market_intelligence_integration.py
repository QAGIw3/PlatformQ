"""
Market Intelligence Integration for Trading Core Service

Integrates with market-intelligence-service to enhance trading decisions with ML insights.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import asyncio

from platformq_shared import ServiceClient
from ..core.matching_engine import MatchingEngine, MarketConfig

logger = logging.getLogger(__name__)


class MarketIntelligenceIntegration:
    """Integration with market-intelligence-service for ML-powered insights"""
    
    def __init__(self, matching_engine: MatchingEngine):
        self.matching_engine = matching_engine
        self.market_intel_client = ServiceClient(
            service_name="market-intelligence-service",
            circuit_breaker_threshold=5,
            rate_limit=50.0  # Lower rate limit for ML services
        )
        
        # Cache for insights
        self.insight_cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = timedelta(minutes=1)
        
        # Subscription tracking
        self.active_subscriptions: Dict[str, str] = {}  # market_id -> subscription_id
        
        # Configuration
        self.enable_ml_routing = True
        self.enable_risk_adjustment = True
        self.enable_anomaly_detection = True
        
        # Background tasks
        self._insight_update_task = None
        self._anomaly_monitor_task = None
        
    async def initialize(self):
        """Initialize market intelligence integration"""
        logger.info("Initializing market intelligence integration")
        
        try:
            # Verify connectivity
            health = await self.market_intel_client.request(
                method="GET",
                path="/health"
            )
            logger.info(f"Market intelligence service health: {health}")
            
            # Start background tasks
            self._insight_update_task = asyncio.create_task(self._update_insights_loop())
            self._anomaly_monitor_task = asyncio.create_task(self._monitor_anomalies_loop())
            
            # Subscribe to insights for active markets
            await self._subscribe_active_markets()
            
        except Exception as e:
            logger.error(f"Failed to initialize market intelligence: {e}")
            # Continue without ML insights
            self.enable_ml_routing = False
            
    async def get_market_insight(self, market_id: str) -> Optional[Dict[str, Any]]:
        """Get current market insight for a specific market"""
        # Check cache first
        if market_id in self.insight_cache:
            cached = self.insight_cache[market_id]
            if datetime.utcnow() - cached["timestamp"] < self.cache_ttl:
                return cached["insight"]
                
        try:
            # Fetch fresh insight
            result = await self.market_intel_client.request(
                method="GET",
                path=f"/api/v1/insights/{market_id}"
            )
            
            # Cache the result
            self.insight_cache[market_id] = {
                "insight": result,
                "timestamp": datetime.utcnow()
            }
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get market insight for {market_id}: {e}")
            return None
            
    async def get_trading_recommendation(
        self,
        market_id: str,
        user_id: str,
        current_position: Optional[Dict[str, Any]] = None
    ) -> Optional[Dict[str, Any]]:
        """Get personalized trading recommendation"""
        try:
            # Get user risk profile (would fetch from user service)
            risk_profile = {
                "tolerance": "medium",
                "max_position_size": "10000"
            }
            
            result = await self.market_intel_client.request(
                method="POST",
                path="/api/v1/recommendations",
                json={
                    "market_id": market_id,
                    "user_risk_profile": risk_profile,
                    "current_position": current_position
                }
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to get trading recommendation: {e}")
            return None
            
    async def enhance_order_routing(
        self,
        order: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Enhance order routing with ML insights"""
        if not self.enable_ml_routing:
            return order
            
        market_id = order.get("market_id")
        insight = await self.get_market_insight(market_id)
        
        if not insight:
            return order
            
        # Adjust order based on insights
        enhanced_order = order.copy()
        
        # Adjust price based on predicted movement
        if order.get("type") == "limit":
            current_price = Decimal(order.get("price", "0"))
            predicted_price = Decimal(insight.get("price_prediction", str(current_price)))
            
            # For buy orders, adjust down if price predicted to fall
            if order.get("side") == "buy" and predicted_price < current_price:
                enhanced_order["price"] = str(predicted_price * Decimal("0.995"))  # 0.5% buffer
                
            # For sell orders, adjust up if price predicted to rise
            elif order.get("side") == "sell" and predicted_price > current_price:
                enhanced_order["price"] = str(predicted_price * Decimal("1.005"))  # 0.5% buffer
                
        # Add ML metadata
        enhanced_order["ml_metadata"] = {
            "signal": insight.get("signal"),
            "confidence": insight.get("confidence"),
            "risk_level": insight.get("risk_level"),
            "enhanced": True
        }
        
        logger.info(f"Enhanced order routing for {market_id} with ML insights")
        
        return enhanced_order
        
    async def adjust_market_parameters(self, market_id: str) -> bool:
        """Adjust market parameters based on ML predictions"""
        if not self.enable_risk_adjustment:
            return False
            
        try:
            # Get market insight
            insight = await self.get_market_insight(market_id)
            if not insight:
                return False
                
            # Get current market config
            market_config = self.matching_engine.market_configs.get(market_id)
            if not market_config:
                return False
                
            # Adjust based on risk level
            risk_level = insight.get("risk_level", "medium")
            volatility = Decimal(insight.get("volatility_forecast", "0"))
            
            # Update circuit breaker thresholds
            if risk_level == "extreme":
                market_config.circuit_breaker_threshold = Decimal("0.05")  # 5% for extreme risk
                market_config.max_order_size_multiplier = Decimal("0.5")  # Reduce max order size
            elif risk_level == "high":
                market_config.circuit_breaker_threshold = Decimal("0.075")  # 7.5% for high risk
                market_config.max_order_size_multiplier = Decimal("0.75")
            else:
                # Reset to defaults
                market_config.circuit_breaker_threshold = Decimal("0.1")  # 10% default
                market_config.max_order_size_multiplier = Decimal("1.0")
                
            # Adjust tick size based on volatility
            if volatility > Decimal("0.3"):  # High volatility
                market_config.tick_size = market_config.tick_size * 2  # Wider tick size
                
            logger.info(f"Adjusted market parameters for {market_id} based on ML insights")
            return True
            
        except Exception as e:
            logger.error(f"Failed to adjust market parameters: {e}")
            return False
            
    async def detect_market_manipulation(
        self,
        market_id: str,
        order_flow: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Detect potential market manipulation using ML"""
        if not self.enable_anomaly_detection:
            return []
            
        try:
            result = await self.market_intel_client.request(
                method="POST",
                path="/api/v1/anomalies/detect",
                json={
                    "market_id": market_id,
                    "order_flow": order_flow[-100:]  # Last 100 orders
                }
            )
            
            anomalies = result.get("anomalies", [])
            
            # Process high-severity anomalies
            for anomaly in anomalies:
                if anomaly.get("severity") == "high":
                    await self._handle_manipulation_alert(market_id, anomaly)
                    
            return anomalies
            
        except Exception as e:
            logger.error(f"Failed to detect market manipulation: {e}")
            return []
            
    async def predict_liquidity_needs(
        self,
        market_id: str,
        horizon_hours: int = 4
    ) -> Optional[Dict[str, Any]]:
        """Predict future liquidity needs for market makers"""
        try:
            result = await self.market_intel_client.request(
                method="GET",
                path=f"/api/v1/liquidity/forecast/{market_id}",
                params={"horizon_hours": horizon_hours}
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to predict liquidity needs: {e}")
            return None
            
    async def _subscribe_active_markets(self):
        """Subscribe to insights for all active markets"""
        for market_id in self.matching_engine.market_configs.keys():
            try:
                result = await self.market_intel_client.request(
                    method="POST",
                    path="/api/v1/subscriptions",
                    json={
                        "market_id": market_id,
                        "indicators": ["signal", "risk", "anomalies"]
                    }
                )
                
                subscription_id = result.get("subscription_id")
                if subscription_id:
                    self.active_subscriptions[market_id] = subscription_id
                    logger.info(f"Subscribed to insights for market {market_id}")
                    
            except Exception as e:
                logger.error(f"Failed to subscribe to market {market_id}: {e}")
                
    async def _update_insights_loop(self):
        """Periodically update market insights"""
        while True:
            try:
                for market_id in self.matching_engine.market_configs.keys():
                    # Update insight
                    insight = await self.get_market_insight(market_id)
                    
                    if insight:
                        # Adjust market parameters if needed
                        await self.adjust_market_parameters(market_id)
                        
                        # Check for trading opportunities
                        if insight.get("signal") in ["buy_strong", "sell_strong"]:
                            await self._notify_trading_opportunity(market_id, insight)
                            
                await asyncio.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in insight update loop: {e}")
                await asyncio.sleep(60)
                
    async def _monitor_anomalies_loop(self):
        """Monitor for market anomalies"""
        while True:
            try:
                for market_id in self.matching_engine.market_configs.keys():
                    # Get recent order flow
                    order_flow = self._get_recent_order_flow(market_id)
                    
                    if order_flow:
                        # Detect anomalies
                        anomalies = await self.detect_market_manipulation(
                            market_id, order_flow
                        )
                        
                        # Log significant anomalies
                        for anomaly in anomalies:
                            if anomaly.get("severity") in ["high", "critical"]:
                                logger.warning(
                                    f"Market anomaly detected in {market_id}: "
                                    f"{anomaly.get('type')} - {anomaly.get('description')}"
                                )
                                
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in anomaly monitor loop: {e}")
                await asyncio.sleep(120)
                
    def _get_recent_order_flow(self, market_id: str) -> List[Dict[str, Any]]:
        """Get recent order flow for a market"""
        # This would fetch from the matching engine's order history
        # Simplified for now
        return []
        
    async def _handle_manipulation_alert(
        self,
        market_id: str,
        anomaly: Dict[str, Any]
    ):
        """Handle potential market manipulation alert"""
        logger.warning(
            f"MANIPULATION ALERT for {market_id}: "
            f"{anomaly.get('type')} detected with severity {anomaly.get('severity')}"
        )
        
        # Could trigger additional actions:
        # - Temporarily increase monitoring
        # - Adjust risk parameters
        # - Notify compliance team
        # - Halt trading if severe
        
    async def _notify_trading_opportunity(
        self,
        market_id: str,
        insight: Dict[str, Any]
    ):
        """Notify about significant trading opportunities"""
        # This would publish to a notification service
        # For now, just log
        logger.info(
            f"Trading opportunity in {market_id}: "
            f"{insight.get('signal')} signal with {insight.get('confidence')} confidence"
        ) 