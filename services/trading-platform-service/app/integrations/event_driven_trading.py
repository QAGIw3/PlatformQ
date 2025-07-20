"""
Event-Driven Trading Integration

Connects trading platform with:
- Event Router Service for real-time event processing
- Graph Intelligence Service for risk network analysis
- Data Platform Service for medallion architecture
"""

import logging
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
from decimal import Decimal
import asyncio
import json
from enum import Enum

import httpx
from platformq_shared import ServiceClient

logger = logging.getLogger(__name__)


class TradingEventType(Enum):
    """Trading event types"""
    TRADE_EXECUTED = "trade.executed"
    POSITION_UPDATED = "position.updated"
    RISK_ALERT = "risk.alert"
    MARGIN_CALL = "margin.call"
    LIQUIDATION_TRIGGERED = "liquidation.triggered"
    STRATEGY_SIGNAL = "strategy.signal"


class EventDrivenTradingIntegration:
    """Integrates trading platform with event-driven architecture"""
    
    def __init__(self, vault_consul_integration=None):
        self.vault_consul = vault_consul_integration
        
        # Service clients
        self.event_router_client = ServiceClient(
            service_name="event-router-service",
            circuit_breaker_threshold=5,
            rate_limit=1000.0  # High rate limit for events
        )
        
        self.graph_intelligence_client = ServiceClient(
            service_name="graph-intelligence-service",
            circuit_breaker_threshold=5,
            rate_limit=100.0
        )
        
        self.data_platform_client = ServiceClient(
            service_name="data-platform-service",
            circuit_breaker_threshold=5,
            rate_limit=200.0
        )
        
        # Event handlers
        self.event_handlers: Dict[TradingEventType, List[Callable]] = {
            event_type: [] for event_type in TradingEventType
        }
        
        # Metrics
        self.events_published = 0
        self.events_failed = 0
        
    async def initialize(self):
        """Initialize integration"""
        logger.info("Initializing event-driven trading integration")
        
        # Register default handlers
        self._register_default_handlers()
        
        # Start background tasks
        asyncio.create_task(self._monitor_event_processing())
        
    def register_event_handler(self, event_type: TradingEventType, handler: Callable):
        """Register handler for specific event type"""
        self.event_handlers[event_type].append(handler)
        logger.info(f"Registered handler for {event_type.value}")
    
    async def publish_trading_event(self, 
                                  event_type: TradingEventType,
                                  trader_id: str,
                                  market_id: str,
                                  data: Dict[str, Any],
                                  metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Publish trading event to event router"""
        try:
            event = {
                "event_type": event_type.value,
                "trader_id": trader_id,
                "market_id": market_id,
                "timestamp": datetime.utcnow().isoformat(),
                "data": data,
                "metadata": metadata or {}
            }
            
            # Send to event router
            response = await self.event_router_client.post(
                "/api/v1/trading/events",
                json=event
            )
            
            self.events_published += 1
            
            # Execute local handlers asynchronously
            asyncio.create_task(self._execute_local_handlers(event_type, event))
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to publish trading event: {e}")
            self.events_failed += 1
            raise
    
    async def update_trader_risk_profile(self,
                                       trader_id: str,
                                       risk_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Update trader risk profile in graph intelligence"""
        try:
            response = await self.graph_intelligence_client.post(
                f"/api/v1/graph/trading-risk/traders/{trader_id}/risk",
                json={
                    "trader_id": trader_id,
                    "risk_score": risk_metrics.get("risk_score", 0.5),
                    "exposure": risk_metrics.get("exposure", 0),
                    "leverage": risk_metrics.get("leverage", 1),
                    "margin_utilization": risk_metrics.get("margin_utilization", 0),
                    "position_count": risk_metrics.get("position_count", 0),
                    "liquidity": risk_metrics.get("liquidity", 0),
                    "metadata": risk_metrics.get("metadata", {})
                }
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to update trader risk profile: {e}")
            raise
    
    async def add_trading_relationship(self,
                                     from_trader: str,
                                     to_trader: str,
                                     relationship_type: str,
                                     strength: float,
                                     exposure_amount: Decimal) -> Dict[str, Any]:
        """Add trading relationship to risk network"""
        try:
            response = await self.graph_intelligence_client.post(
                "/api/v1/graph/trading-risk/relationships",
                json={
                    "from_trader": from_trader,
                    "to_trader": to_trader,
                    "relationship_type": relationship_type,
                    "strength": strength,
                    "exposure_amount": str(exposure_amount),
                    "metadata": {
                        "created_at": datetime.utcnow().isoformat()
                    }
                }
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to add trading relationship: {e}")
            raise
    
    async def analyze_risk_propagation(self,
                                     source_trader: str,
                                     risk_event: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze risk propagation through network"""
        try:
            response = await self.graph_intelligence_client.post(
                "/api/v1/graph/trading-risk/analyze/propagation",
                json={
                    "source_trader": source_trader,
                    "risk_event": risk_event
                }
            )
            
            # Process risk propagation results
            if response.get("systemic_risk_score", 0) > 0.7:
                await self._trigger_systemic_risk_mitigation(response)
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to analyze risk propagation: {e}")
            raise
    
    async def ingest_trading_data(self,
                                events: List[Dict[str, Any]],
                                event_type: str) -> Dict[str, Any]:
        """Ingest trading data into medallion architecture"""
        try:
            response = await self.data_platform_client.post(
                "/api/v1/lake/trading/ingest",
                json={
                    "events": events,
                    "event_type": event_type,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to ingest trading data: {e}")
            raise
    
    async def get_trading_features(self,
                                 feature_sets: List[str],
                                 entities: Dict[str, Any]) -> Dict[str, Any]:
        """Get real-time trading features from feature store"""
        try:
            response = await self.data_platform_client.post(
                "/api/v1/features/serve",
                json={
                    "feature_sets": feature_sets,
                    "entities": entities
                }
            )
            
            return response.get("features", {})
            
        except Exception as e:
            logger.error(f"Failed to get trading features: {e}")
            raise
    
    async def detect_risk_clusters(self) -> List[Dict[str, Any]]:
        """Detect clusters of high-risk traders"""
        try:
            response = await self.graph_intelligence_client.get(
                "/api/v1/graph/trading-risk/clusters/risk"
            )
            
            return response.get("clusters", [])
            
        except Exception as e:
            logger.error(f"Failed to detect risk clusters: {e}")
            return []
    
    async def simulate_cascade_failure(self,
                                     failing_trader: str,
                                     failure_type: str = "liquidation") -> Dict[str, Any]:
        """Simulate cascade effects of trader failure"""
        try:
            response = await self.graph_intelligence_client.post(
                "/api/v1/graph/trading-risk/simulate/cascade",
                json={
                    "failing_trader": failing_trader,
                    "failure_type": failure_type
                }
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Failed to simulate cascade failure: {e}")
            raise
    
    async def get_systemic_importance(self, trader_id: str) -> float:
        """Get systemic importance score for trader"""
        try:
            response = await self.graph_intelligence_client.get(
                f"/api/v1/graph/trading-risk/traders/{trader_id}/systemic-importance"
            )
            
            return response.get("systemic_importance", 0)
            
        except Exception as e:
            logger.error(f"Failed to get systemic importance: {e}")
            return 0
    
    # Event processing methods
    async def process_trade_execution(self, trade: Dict[str, Any]):
        """Process trade execution event"""
        try:
            # Publish event
            await self.publish_trading_event(
                event_type=TradingEventType.TRADE_EXECUTED,
                trader_id=trade["trader_id"],
                market_id=trade["market_id"],
                data={
                    "trade_id": trade["id"],
                    "price": str(trade["price"]),
                    "quantity": str(trade["quantity"]),
                    "side": trade["side"],
                    "order_id": trade.get("order_id"),
                    "order_type": trade.get("order_type", "MARKET"),
                    "fees": str(trade.get("fees", 0))
                }
            )
            
            # Update risk metrics
            await self._update_trader_risk_after_trade(trade)
            
            # Ingest to data lake
            await self.ingest_trading_data(
                events=[trade],
                event_type="trades"
            )
            
        except Exception as e:
            logger.error(f"Failed to process trade execution: {e}")
    
    async def process_position_update(self, position: Dict[str, Any]):
        """Process position update event"""
        try:
            # Calculate position metrics
            unrealized_pnl = self._calculate_unrealized_pnl(position)
            position_risk = self._calculate_position_risk(position)
            
            # Publish event
            await self.publish_trading_event(
                event_type=TradingEventType.POSITION_UPDATED,
                trader_id=position["trader_id"],
                market_id=position["market_id"],
                data={
                    "position_id": position["id"],
                    "position_size": str(position["quantity"]),
                    "entry_price": str(position["entry_price"]),
                    "current_price": str(position.get("current_price", position["entry_price"])),
                    "pnl": str(unrealized_pnl),
                    "risk_score": position_risk
                }
            )
            
            # Check for risk alerts
            if position_risk > 0.7:
                await self._trigger_risk_alert(position, position_risk)
            
        except Exception as e:
            logger.error(f"Failed to process position update: {e}")
    
    async def process_risk_alert(self, alert: Dict[str, Any]):
        """Process risk alert event"""
        try:
            # Publish event
            await self.publish_trading_event(
                event_type=TradingEventType.RISK_ALERT,
                trader_id=alert["trader_id"],
                market_id=alert.get("market_id", "ALL"),
                data={
                    "alert_id": alert["id"],
                    "risk_level": alert["risk_level"],
                    "risk_type": alert["risk_type"],
                    "risk_metrics": alert["risk_metrics"],
                    "alert_message": alert["message"]
                }
            )
            
            # Analyze risk propagation if high risk
            if alert["risk_level"] in ["high", "critical"]:
                await self.analyze_risk_propagation(
                    source_trader=alert["trader_id"],
                    risk_event=alert
                )
            
        except Exception as e:
            logger.error(f"Failed to process risk alert: {e}")
    
    # Helper methods
    def _register_default_handlers(self):
        """Register default event handlers"""
        # Trade execution handler
        self.register_event_handler(
            TradingEventType.TRADE_EXECUTED,
            self._handle_trade_executed
        )
        
        # Risk alert handler
        self.register_event_handler(
            TradingEventType.RISK_ALERT,
            self._handle_risk_alert
        )
        
        # Liquidation handler
        self.register_event_handler(
            TradingEventType.LIQUIDATION_TRIGGERED,
            self._handle_liquidation
        )
    
    async def _execute_local_handlers(self, event_type: TradingEventType, event: Dict[str, Any]):
        """Execute local event handlers"""
        handlers = self.event_handlers.get(event_type, [])
        
        for handler in handlers:
            try:
                await handler(event)
            except Exception as e:
                logger.error(f"Error in event handler {handler.__name__}: {e}")
    
    async def _handle_trade_executed(self, event: Dict[str, Any]):
        """Default handler for trade execution"""
        logger.info(f"Trade executed: {event['data']['trade_id']}")
    
    async def _handle_risk_alert(self, event: Dict[str, Any]):
        """Default handler for risk alerts"""
        logger.warning(f"Risk alert for trader {event['trader_id']}: {event['data']['alert_message']}")
    
    async def _handle_liquidation(self, event: Dict[str, Any]):
        """Default handler for liquidation events"""
        logger.critical(f"Liquidation triggered for trader {event['trader_id']}")
        
        # Trigger cascade analysis
        await self.simulate_cascade_failure(
            failing_trader=event["trader_id"],
            failure_type="liquidation"
        )
    
    async def _update_trader_risk_after_trade(self, trade: Dict[str, Any]):
        """Update trader risk metrics after trade"""
        # Get current trader metrics
        trader_metrics = await self._get_trader_metrics(trade["trader_id"])
        
        # Update metrics
        trader_metrics["position_count"] += 1
        trader_metrics["exposure"] += float(trade["quantity"]) * float(trade["price"])
        
        # Recalculate risk score
        risk_score = self._calculate_trader_risk_score(trader_metrics)
        trader_metrics["risk_score"] = risk_score
        
        # Update in graph
        await self.update_trader_risk_profile(
            trader_id=trade["trader_id"],
            risk_metrics=trader_metrics
        )
    
    async def _trigger_risk_alert(self, position: Dict[str, Any], risk_score: float):
        """Trigger risk alert for position"""
        alert = {
            "id": f"alert_{position['id']}_{datetime.utcnow().timestamp()}",
            "trader_id": position["trader_id"],
            "market_id": position["market_id"],
            "risk_level": "high" if risk_score > 0.8 else "medium",
            "risk_type": "position_risk",
            "risk_metrics": {
                "position_risk_score": risk_score,
                "position_size": position["quantity"],
                "unrealized_pnl": self._calculate_unrealized_pnl(position)
            },
            "message": f"High risk position detected: {position['id']}"
        }
        
        await self.process_risk_alert(alert)
    
    async def _trigger_systemic_risk_mitigation(self, propagation_result: Dict[str, Any]):
        """Trigger systemic risk mitigation actions"""
        logger.critical(f"Systemic risk detected: {propagation_result['systemic_risk_score']}")
        
        # Execute mitigation actions
        for action in propagation_result.get("mitigation_actions", []):
            logger.info(f"Executing mitigation: {action}")
            # Would implement actual mitigation logic
    
    def _calculate_unrealized_pnl(self, position: Dict[str, Any]) -> Decimal:
        """Calculate unrealized PnL for position"""
        entry_price = Decimal(str(position["entry_price"]))
        current_price = Decimal(str(position.get("current_price", entry_price)))
        quantity = Decimal(str(position["quantity"]))
        
        if position["side"] == "LONG":
            return (current_price - entry_price) * quantity
        else:
            return (entry_price - current_price) * quantity
    
    def _calculate_position_risk(self, position: Dict[str, Any]) -> float:
        """Calculate risk score for position"""
        # Simplified risk calculation
        leverage = float(position.get("leverage", 1))
        position_size = float(position["quantity"]) * float(position.get("current_price", position["entry_price"]))
        account_value = float(position.get("account_value", 100000))
        
        position_ratio = position_size / account_value
        risk_score = min(position_ratio * leverage * 2, 1.0)
        
        return risk_score
    
    def _calculate_trader_risk_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate overall trader risk score"""
        # Weighted risk factors
        margin_weight = 0.3
        leverage_weight = 0.3
        concentration_weight = 0.2
        volatility_weight = 0.2
        
        margin_risk = metrics.get("margin_utilization", 0)
        leverage_risk = min(metrics.get("leverage", 1) / 10, 1.0)
        concentration_risk = min(metrics.get("position_count", 0) / 20, 1.0)
        volatility_risk = metrics.get("portfolio_volatility", 0.5)
        
        risk_score = (
            margin_weight * margin_risk +
            leverage_weight * leverage_risk +
            concentration_weight * concentration_risk +
            volatility_weight * volatility_risk
        )
        
        return min(risk_score, 1.0)
    
    async def _get_trader_metrics(self, trader_id: str) -> Dict[str, Any]:
        """Get current trader metrics"""
        # Would fetch from database or cache
        return {
            "position_count": 0,
            "exposure": 0,
            "leverage": 1,
            "margin_utilization": 0,
            "liquidity": 100000,
            "portfolio_volatility": 0.02
        }
    
    async def _monitor_event_processing(self):
        """Monitor event processing metrics"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Log metrics
                logger.info(f"Event processing metrics - Published: {self.events_published}, Failed: {self.events_failed}")
                
                # Check for issues
                if self.events_failed > 10:
                    logger.warning(f"High event failure rate: {self.events_failed} failures")
                
                # Reset counters periodically
                if self.events_published > 10000:
                    self.events_published = 0
                    self.events_failed = 0
                    
            except Exception as e:
                logger.error(f"Error in event monitoring: {e}") 