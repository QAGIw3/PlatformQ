"""State manager for distributed risk data using Apache Ignite."""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import json

from pyignite import Client as IgniteClient
import pulsar

from ..models import PortfolioRisk, MarginCall, RiskAlert

logger = logging.getLogger(__name__)


class StateManager:
    """Manages distributed state for the Risk Engine Service."""
    
    def __init__(self, ignite_client: IgniteClient, pulsar_client: pulsar.Client):
        self.ignite = ignite_client
        self.pulsar = pulsar_client
        self.caches: Dict[str, Any] = {}
        self.producers: Dict[str, pulsar.Producer] = {}
        self.flink_processor = None  # Will be set after initialization
        
    async def initialize(self):
        """Initialize caches and event producers."""
        # Initialize Ignite caches
        self.caches['positions'] = await self.ignite.get_or_create_cache('risk_positions')
        self.caches['market_data'] = await self.ignite.get_or_create_cache('risk_market_data')
        self.caches['portfolio_risk'] = await self.ignite.get_or_create_cache('portfolio_risk')
        self.caches['margin_calls'] = await self.ignite.get_or_create_cache('margin_calls')
        self.caches['risk_limits'] = await self.ignite.get_or_create_cache('risk_limits')
        self.caches['stress_scenarios'] = await self.ignite.get_or_create_cache('stress_scenarios')
        self.caches['alerts'] = await self.ignite.get_or_create_cache('risk_alerts')
        
        # Initialize Pulsar producers
        self.producers['risk-events'] = self.pulsar.create_producer('risk-events')
        self.producers['margin-calls'] = self.pulsar.create_producer('margin-calls')
        self.producers['risk-alerts'] = self.pulsar.create_producer('risk-alerts')
        
        logger.info("State manager initialized")
        
    def set_flink_processor(self, flink_processor):
        """Set the Flink processor for event streaming."""
        self.flink_processor = flink_processor

    # Position Management
    async def get_position(self, position_id: str) -> Optional[Dict[str, Any]]:
        """Get a position by ID."""
        cache = self.ignite.get_cache(self.positions_cache)
        position_data = cache.get(position_id)
        return json.loads(position_data) if position_data else None
    
    async def get_user_positions(self, user_id: str) -> List[Dict[str, Any]]:
        """Get all positions for a user."""
        cache = self.ignite.get_cache(self.positions_cache)
        positions = []
        
        # Simple scan - in production, use SQL queries on Ignite
        for key, value in cache.scan():
            position = json.loads(value)
            if position.get("user_id") == user_id:
                positions.append(position)
        
        return positions
    
    async def get_portfolio_positions(self, portfolio_id: str) -> List[Dict[str, Any]]:
        """Get all positions in a portfolio."""
        cache = self.ignite.get_cache(self.positions_cache)
        positions = []
        
        for key, value in cache.scan():
            position = json.loads(value)
            if position.get("portfolio_id") == portfolio_id:
                positions.append(position)
        
        return positions
    
    # Market Data
    async def get_market_data(self, market_id: str) -> Optional[Dict[str, Any]]:
        """Get market data for a market."""
        cache = self.ignite.get_cache(self.market_data_cache)
        data = cache.get(market_id)
        return json.loads(data) if data else None
    
    async def get_market_data_batch(self, market_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get market data for multiple markets."""
        cache = self.ignite.get_cache(self.market_data_cache)
        result = {}
        
        for market_id in market_ids:
            data = cache.get(market_id)
            if data:
                result[market_id] = json.loads(data)
        
        return result
    
    # Portfolio Risk
    async def get_portfolio_risk(self, user_id: str) -> Optional[PortfolioRisk]:
        """Get cached portfolio risk."""
        cache = self.ignite.get_cache(self.portfolio_risk_cache)
        data = cache.get(f"portfolio_{user_id}")
        
        if data:
            risk_data = json.loads(data)
            # Convert back to PortfolioRisk object
            return PortfolioRisk(**risk_data)
        return None
    
    async def cache_portfolio_risk(self, user_id: str, risk: PortfolioRisk):
        """Cache portfolio risk calculation."""
        cache = self.ignite.get_cache(self.portfolio_risk_cache)
        
        # Convert to JSON-serializable format
        risk_data = {
            "user_id": risk.user_id,
            "total_positions": risk.total_positions,
            "total_value": str(risk.total_value),
            "total_collateral": str(risk.total_collateral),
            "total_unrealized_pnl": str(risk.total_unrealized_pnl),
            "margin_usage": str(risk.margin_usage),
            "portfolio_var": str(risk.portfolio_var),
            "portfolio_leverage": str(risk.portfolio_leverage),
            "max_concentration": str(risk.max_concentration),
            "risk_score": risk.risk_score,
            "timestamp": risk.timestamp.isoformat(),
            "alerts": [
                {
                    "alert_type": alert.alert_type,
                    "severity": alert.severity,
                    "message": alert.message
                }
                for alert in risk.alerts
            ]
        }
        
        cache.put(f"portfolio_{user_id}", json.dumps(risk_data), ttl=self.config["risk_cache_ttl"])
        self._stats["risk_calculations"] += 1
    
    # Margin Calls
    async def create_margin_call(self, margin_call: MarginCall):
        """Create a new margin call."""
        cache = self.ignite.get_cache(self.margin_calls_cache)
        
        call_data = {
            "call_id": margin_call.call_id,
            "user_id": margin_call.user_id,
            "amount_required": str(margin_call.amount_required),
            "amount_deposited": str(margin_call.amount_deposited),
            "deadline": margin_call.deadline.isoformat(),
            "reason": margin_call.reason,
            "issued_by": margin_call.issued_by,
            "issued_at": margin_call.issued_at.isoformat(),
            "status": margin_call.status
        }
        
        cache.put(margin_call.call_id, json.dumps(call_data))
        self._stats["margin_calls"] += 1
    
    async def get_margin_call(self, call_id: str) -> Optional[MarginCall]:
        """Get a margin call by ID."""
        cache = self.ignite.get_cache(self.margin_calls_cache)
        data = cache.get(call_id)
        
        if data:
            call_data = json.loads(data)
            return MarginCall(
                call_id=call_data["call_id"],
                user_id=call_data["user_id"],
                amount_required=Decimal(call_data["amount_required"]),
                amount_deposited=Decimal(call_data["amount_deposited"]),
                deadline=datetime.fromisoformat(call_data["deadline"]),
                reason=call_data["reason"],
                issued_by=call_data["issued_by"],
                issued_at=datetime.fromisoformat(call_data["issued_at"]),
                status=call_data["status"]
            )
        return None
    
    async def get_margin_calls(self, user_id: Optional[str] = None, status: Optional[str] = None, limit: int = 100) -> List[MarginCall]:
        """Get margin calls with filters."""
        cache = self.ignite.get_cache(self.margin_calls_cache)
        calls = []
        
        for key, value in cache.scan():
            call_data = json.loads(value)
            
            # Apply filters
            if user_id and call_data["user_id"] != user_id:
                continue
            if status and call_data["status"] != status:
                continue
            
            calls.append(MarginCall(
                call_id=call_data["call_id"],
                user_id=call_data["user_id"],
                amount_required=Decimal(call_data["amount_required"]),
                amount_deposited=Decimal(call_data["amount_deposited"]),
                deadline=datetime.fromisoformat(call_data["deadline"]),
                reason=call_data["reason"],
                issued_by=call_data["issued_by"],
                issued_at=datetime.fromisoformat(call_data["issued_at"]),
                status=call_data["status"]
            ))
            
            if len(calls) >= limit:
                break
        
        return calls
    
    async def update_margin_call(self, margin_call: MarginCall):
        """Update a margin call."""
        await self.create_margin_call(margin_call)  # Put overwrites
    
    # Risk Limits
    async def get_risk_limits(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Get risk limits for a user."""
        cache = self.ignite.get_cache(self.risk_limits_cache)
        data = cache.get(user_id)
        
        if data:
            limits = json.loads(data)
            # Convert string values back to Decimal
            for key in ["max_position_value", "max_leverage", "max_concentration", "max_var", 
                       "max_loss_daily", "max_loss_weekly", "max_loss_monthly"]:
                if key in limits:
                    limits[key] = Decimal(limits[key])
            return limits
        return None
    
    async def update_risk_limits(self, user_id: str, limits: Dict[str, Any]):
        """Update risk limits for a user."""
        cache = self.ignite.get_cache(self.risk_limits_cache)
        
        # Convert Decimal to string for JSON
        limits_data = {}
        for key, value in limits.items():
            if isinstance(value, Decimal):
                limits_data[key] = str(value)
            else:
                limits_data[key] = value
        
        cache.put(user_id, json.dumps(limits_data))
    
    async def get_risk_usage(self, user_id: str) -> Dict[str, Decimal]:
        """Get current risk usage for a user."""
        # In production, this would aggregate from positions
        positions = await self.get_user_positions(user_id)
        
        usage = {
            "max_position_value": Decimal("0"),
            "max_leverage": Decimal("0"),
            "max_open_positions": len(positions),
            "max_concentration": Decimal("0"),
            "max_var": Decimal("0"),
            "max_loss_daily": Decimal("0"),
            "max_loss_weekly": Decimal("0"),
            "max_loss_monthly": Decimal("0")
        }
        
        # Calculate actual usage from positions
        for position in positions:
            # Add position value
            position_value = Decimal(str(position.get("notional_value", "0")))
            usage["max_position_value"] += position_value
            
            # Track max leverage
            leverage = Decimal(str(position.get("leverage", "0")))
            usage["max_leverage"] = max(usage["max_leverage"], leverage)
        
        return usage
    
    # Alerts
    async def publish_risk_alert(self, user_id: str, portfolio_risk: PortfolioRisk):
        """Publish risk alerts to Pulsar."""
        producer = self.pulsar.create_producer(self.risk_events_topic)
        
        for alert in portfolio_risk.alerts:
            event = {
                "event_type": "risk_alert",
                "user_id": user_id,
                "alert": {
                    "type": alert.alert_type,
                    "severity": alert.severity,
                    "message": alert.message,
                    "position_id": alert.position_id,
                    "metric_value": alert.metric_value
                },
                "timestamp": datetime.utcnow().isoformat()
            }
            
            producer.send(json.dumps(event).encode('utf-8'))
        
        producer.close()
    
    async def get_risk_alerts(self, user_id: Optional[str] = None, severity: Optional[str] = None, limit: int = 100) -> List[RiskAlert]:
        """Get active risk alerts."""
        cache = self.ignite.get_cache(self.alerts_cache)
        alerts = []
        
        for key, value in cache.scan():
            alert_data = json.loads(value)
            
            # Apply filters
            if user_id and alert_data.get("user_id") != user_id:
                continue
            if severity and alert_data["severity"] != severity:
                continue
            
            alerts.append(RiskAlert(
                alert_type=alert_data["alert_type"],
                severity=alert_data["severity"],
                message=alert_data["message"],
                position_id=alert_data.get("position_id"),
                metric_value=alert_data.get("metric_value"),
                timestamp=datetime.fromisoformat(alert_data["timestamp"])
            ))
            
            if len(alerts) >= limit:
                break
        
        return alerts
    
    # User Balance (simplified)
    async def get_user_balance(self, user_id: str) -> Decimal:
        """Get user balance (simplified for demo)."""
        # In production, this would integrate with account service
        return Decimal("100000")  # $100k default
    
    # Notifications
    async def notify_margin_call(self, user_id: str, margin_call: MarginCall):
        """Send margin call notification."""
        producer = self.pulsar.create_producer(self.risk_events_topic)
        
        event = {
            "event_type": "margin_call",
            "user_id": user_id,
            "margin_call": {
                "call_id": margin_call.call_id,
                "amount_required": str(margin_call.amount_required),
                "deadline": margin_call.deadline.isoformat(),
                "reason": margin_call.reason
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
        producer.send(json.dumps(event).encode('utf-8'))
        producer.close()
    
    # Health Checks
    async def check_ignite_health(self) -> bool:
        """Check Ignite connection health."""
        try:
            # Try to access a cache
            cache = self.ignite.get_cache(self.positions_cache)
            return cache is not None
        except Exception:
            return False
    
    async def check_pulsar_health(self) -> bool:
        """Check Pulsar connection health."""
        try:
            # Try to create a producer
            producer = self.pulsar.create_producer(self.risk_events_topic)
            producer.close()
            return True
        except Exception:
            return False
    
    async def is_ready(self) -> bool:
        """Check if service is ready."""
        return await self.check_ignite_health() and await self.check_pulsar_health()
    
    async def get_service_stats(self) -> Dict[str, Any]:
        """Get service statistics."""
        stats = self._stats.copy()
        stats["start_time"] = self.start_time
        return stats
    
    # Additional methods for other endpoints would go here... 