"""Real-time risk monitoring engine"""

import asyncio
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass
import time
import logging
import numpy as np

from platformq_events import (
    EventType, RiskEvent, PositionEvent, MarketEventPublisher
)
from platformq_direct_comm import MarketMessageType, RiskCheckRequest, RiskCheckResponse

from ..config import Settings
from ..models.risk import RiskState, AlertLevel, MarginStatus, RiskLevel
from ..models.risk import MarketRisk, PositionRisk
from .ml_risk_engine import MLRiskEngine

logger = logging.getLogger(__name__)


@dataclass
class MonitoringResult:
    """Result of risk monitoring check"""
    user_id: str
    timestamp: datetime
    risk_metrics: Dict[str, Any]
    margin_status: MarginStatus
    alerts: List[Dict]
    violations: List[Dict]
    actions_required: List[Dict]
    ml_assessment: Optional[Dict] = None  # ML-based assessment


class RiskMonitor:
    """Real-time risk monitoring engine with ML capabilities"""
    
    def __init__(
        self,
        settings: Settings,
        event_publisher: MarketEventPublisher,
        ignite_client=None,
        direct_comm=None,
        pulsar_client=None
    ):
        self.settings = settings
        self.event_publisher = event_publisher
        self.direct_comm = direct_comm
        self.ignite_client = ignite_client
        self.pulsar_client = pulsar_client
        
        # Initialize ML risk engine
        self.ml_engine = MLRiskEngine(settings, ignite_client)
        
        # Active monitoring
        self.monitored_users: Set[str] = set()
        self.user_portfolios: Dict[str, Dict] = {}
        self.user_limits: Dict[str, Dict] = {}
        self.user_states: Dict[str, RiskState] = {}
        
        # Price cache
        self.price_cache: Dict[str, Decimal] = {}
        self.price_cache_timestamp: Dict[str, datetime] = {}
        
        # Market data cache for ML features
        self.market_data_cache: Dict[str, Dict] = {}
        
        # Historical data for calculations
        self.returns_data: Dict[str, List[float]] = {}
        
        # Monitoring tasks
        self._running = False
        self._tasks = []
        
        # Direct communication handlers
        if self.direct_comm:
            self._register_direct_handlers()
    
    def _register_direct_handlers(self):
        """Register handlers for direct communication"""
        asyncio.create_task(
            self.direct_comm.register_handler(
                MarketMessageType.RISK_CHECK_REQUEST,
                self._handle_risk_check_request
            )
        )
    
    async def _handle_risk_check_request(self, request: RiskCheckRequest) -> RiskCheckResponse:
        """Handle ultra-low latency risk check request"""
        start_time = time.time()
        
        # Quick risk check
        user_state = self.user_states.get(request.user_id)
        
        # Default response
        response = RiskCheckResponse(
            service_id=self.settings.service_id,
            check_id=request.check_id,
            approved=True,
            margin_required=0,
            margin_available=0,
            position_value=0,
            max_size=0
        )
        
        if user_state:
            # Check if user has capacity
            margin_used = user_state.margin_used
            margin_available = user_state.total_collateral - margin_used
            
            # Calculate required margin for new order
            position_value = request.size * request.price // 10**8  # Adjust for scaling
            margin_required = position_value // request.leverage
            
            if margin_available >= margin_required:
                response.approved = True
                response.margin_required = margin_required
                response.margin_available = margin_available
                response.position_value = position_value
                response.max_size = (margin_available * request.leverage * 10**8) // request.price
            else:
                response.approved = False
                response.reason = "Insufficient margin"
                response.margin_required = margin_required
                response.margin_available = margin_available
        
        # Calculate latency
        response.check_latency_us = int((time.time() - start_time) * 1_000_000)
        
        return response
    
    async def start(self):
        """Start risk monitoring"""
        if self._running:
            return
            
        self._running = True
        
        # Start monitoring tasks
        self._tasks = [
            asyncio.create_task(self._continuous_monitoring_loop()),
            asyncio.create_task(self._price_update_loop()),
            asyncio.create_task(self._ml_assessment_loop())
        ]
        
        logger.info("Risk monitoring started")
    
    async def stop(self):
        """Stop risk monitoring"""
        self._running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        logger.info("Risk monitoring stopped")
    
    async def add_user_monitoring(
        self,
        user_id: str,
        limits: Optional[Dict] = None
    ):
        """Add a user to active monitoring"""
        self.monitored_users.add(user_id)
        
        # Set default limits if not provided
        if limits is None:
            limits = {
                "max_position_size": Decimal("1000000"),
                "max_leverage": Decimal("20"),
                "max_loss_per_trade": Decimal("50000"),
                "max_daily_loss": Decimal("100000"),
                "max_open_positions": 10,
                "min_margin_level": Decimal("120"),  # 120%
                "concentration_limit": Decimal("30")  # 30%
            }
        
        self.user_limits[user_id] = limits
        self.user_states[user_id] = RiskState(user_id=user_id)
        
        # Fetch initial portfolio
        await self._refresh_user_portfolio(user_id)
        
        logger.info(f"Added user {user_id} to monitoring")
    
    async def remove_user_monitoring(self, user_id: str):
        """Remove a user from active monitoring"""
        self.monitored_users.discard(user_id)
        self.user_portfolios.pop(user_id, None)
        self.user_limits.pop(user_id, None)
        self.user_states.pop(user_id, None)
        
        logger.info(f"Removed user {user_id} from monitoring")
    
    async def check_user_risk(self, user_id: str) -> MonitoringResult:
        """Check risk for a specific user with ML enhancement"""
        # Get portfolio
        portfolio = self.user_portfolios.get(user_id)
        if not portfolio:
            await self._refresh_user_portfolio(user_id)
            portfolio = self.user_portfolios.get(user_id)
        
        if not portfolio:
            raise ValueError(f"No portfolio found for user {user_id}")
        
        # Update position prices
        await self._update_portfolio_prices(portfolio)
        
        # Traditional risk metrics
        risk_metrics = await self._calculate_risk_metrics(portfolio)
        
        # Check margin status
        margin_status = self._calculate_margin_status(portfolio)
        
        # ML-based assessment for each position
        ml_assessments = {}
        for position in portfolio.get("positions", []):
            market_data = await self._get_enriched_market_data(position["market_id"])
            if market_data:
                # Get ML market risk assessment
                market_risk = await self.ml_engine.assess_market_risk(
                    position["market_id"],
                    market_data
                )
                
                # Get position risk assessment
                position_risk = await self.ml_engine.assess_position_risk(
                    position,
                    market_risk
                )
                
                ml_assessments[position["position_id"]] = {
                    "market_risk": market_risk.to_dict() if hasattr(market_risk, 'to_dict') else market_risk,
                    "position_risk": position_risk.to_dict() if hasattr(position_risk, 'to_dict') else position_risk
                }
        
        # Check for violations
        limits = self.user_limits[user_id]
        violations = self._check_risk_violations(portfolio, limits, risk_metrics)
        
        # Generate alerts (enhanced with ML insights)
        alerts = self._generate_alerts(user_id, portfolio, risk_metrics, margin_status, ml_assessments)
        
        # Determine actions required
        actions_required = self._determine_actions(violations, margin_status, ml_assessments)
        
        # Update user state
        state = self.user_states[user_id]
        state.update_from_monitoring(risk_metrics, margin_status, alerts, violations)
        
        # Publish events if needed
        await self._publish_risk_events(user_id, state, violations, actions_required)
        
        return MonitoringResult(
            user_id=user_id,
            timestamp=datetime.utcnow(),
            risk_metrics=risk_metrics,
            margin_status=margin_status,
            alerts=alerts,
            violations=violations,
            actions_required=actions_required,
            ml_assessment=ml_assessments
        )
    
    async def _continuous_monitoring_loop(self):
        """Continuously monitor all active users"""
        while self._running:
            try:
                # Monitor each user
                for user_id in list(self.monitored_users):
                    try:
                        await self.check_user_risk(user_id)
                    except Exception as e:
                        logger.error(f"Error monitoring user {user_id}: {e}")
                
                # Wait before next cycle
                await asyncio.sleep(self.settings.RISK_CALCULATION_INTERVAL_SECONDS)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(5)
    
    async def _price_update_loop(self):
        """Update cached prices periodically"""
        while self._running:
            try:
                # Get unique markets from all portfolios
                markets = set()
                for portfolio in self.user_portfolios.values():
                    for position in portfolio.get("positions", []):
                        markets.add(position["market_id"])
                
                # Update prices
                for market_id in markets:
                    await self._update_market_price(market_id)
                
                await asyncio.sleep(1)  # Update every second
                
            except Exception as e:
                logger.error(f"Error in price update loop: {e}")
                await asyncio.sleep(1)
    
    async def _ml_assessment_loop(self):
        """Periodic ML assessment for all markets"""
        while self._running:
            try:
                # Get unique markets
                markets = set()
                for portfolio in self.user_portfolios.values():
                    for position in portfolio.get("positions", []):
                        markets.add(position["market_id"])
                
                # Run ML assessment for each market
                for market_id in markets:
                    market_data = await self._get_enriched_market_data(market_id)
                    if market_data:
                        market_risk = await self.ml_engine.assess_market_risk(
                            market_id, market_data
                        )
                        
                        # Cache the assessment
                        if self.ignite_client:
                            cache = self.ignite_client.get_or_create_cache("ml_assessments")
                            cache.put(f"market_risk:{market_id}", market_risk, ttl=30)
                
                await asyncio.sleep(30)  # Run every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in ML assessment loop: {e}")
                await asyncio.sleep(30)
    
    async def _refresh_user_portfolio(self, user_id: str):
        """Refresh user portfolio from cache or service"""
        if self.ignite_client:
            try:
                portfolio_cache = self.ignite_client.get_or_create_cache("user_portfolios")
                portfolio = portfolio_cache.get(user_id)
                if portfolio:
                    self.user_portfolios[user_id] = portfolio
                    return
            except Exception as e:
                logger.error(f"Error getting portfolio from cache: {e}")
        
        # If not in cache, create empty portfolio
        self.user_portfolios[user_id] = {
            "positions": [],
            "total_collateral": Decimal("0"),
            "total_margin_used": Decimal("0")
        }
    
    async def _update_portfolio_prices(self, portfolio: Dict):
        """Update position prices in portfolio"""
        for position in portfolio.get("positions", []):
            market_id = position["market_id"]
            price = self.price_cache.get(market_id)
            
            if price:
                position["mark_price"] = price
                # Recalculate unrealized P&L
                size = Decimal(str(position["size"]))
                entry_price = Decimal(str(position["entry_price"]))
                
                if position["side"] == "long":
                    position["unrealized_pnl"] = size * (price - entry_price)
                else:
                    position["unrealized_pnl"] = size * (entry_price - price)
    
    async def _update_market_price(self, market_id: str):
        """Update cached market price"""
        # In production, this would fetch from market data service
        # For now, simulate with slight random walk
        current_price = self.price_cache.get(market_id, Decimal("100"))
        change = Decimal(str(np.random.normal(0, 0.001)))  # 0.1% volatility
        new_price = current_price * (1 + change)
        
        self.price_cache[market_id] = new_price
        self.price_cache_timestamp[market_id] = datetime.utcnow()
    
    async def _get_enriched_market_data(self, market_id: str) -> Optional[Dict]:
        """Get enriched market data for ML features"""
        # Check cache first
        if market_id in self.market_data_cache:
            cached_data = self.market_data_cache[market_id]
            if datetime.utcnow() - cached_data["timestamp"] < timedelta(minutes=1):
                return cached_data["data"]
        
        # Simulate market data
        # In production, this would fetch from market data service
        data = {
            "volatility_24h": float(np.random.uniform(0.02, 0.15)),
            "volume_24h": float(np.random.uniform(1e6, 1e8)),
            "price_change_24h": float(np.random.uniform(-0.1, 0.1)),
            "rsi": float(np.random.uniform(30, 70)),
            "bollinger_position": float(np.random.uniform(0, 1)),
            "bid_ask_spread": float(np.random.uniform(0.0001, 0.001)),
            "order_book_imbalance": float(np.random.uniform(-0.5, 0.5)),
            "funding_rate": float(np.random.uniform(-0.001, 0.001)),
            "open_interest_change": float(np.random.uniform(-0.2, 0.2)),
            "historical_returns": list(np.random.normal(0, 0.02, 100)),
            "avg_volume": float(np.random.uniform(5e6, 5e7)),
            "avg_volatility": 0.08,
            "order_book_depth": float(np.random.uniform(1e5, 1e7))
        }
        
        # Cache the data
        self.market_data_cache[market_id] = {
            "timestamp": datetime.utcnow(),
            "data": data
        }
        
        return data
    
    async def _calculate_risk_metrics(self, portfolio: Dict) -> Dict[str, Any]:
        """Calculate traditional risk metrics"""
        total_value = Decimal("0")
        total_unrealized_pnl = Decimal("0")
        total_exposure = Decimal("0")
        
        for position in portfolio.get("positions", []):
            position_value = Decimal(str(position["size"])) * Decimal(str(position["mark_price"]))
            total_value += position_value
            total_unrealized_pnl += Decimal(str(position.get("unrealized_pnl", 0)))
            total_exposure += position_value
        
        return {
            "total_value": total_value,
            "total_exposure": total_exposure,
            "total_unrealized_pnl": total_unrealized_pnl,
            "net_exposure": total_exposure,  # Simplified - would calculate long-short
            "var_95": total_value * Decimal("0.05"),  # Simplified VaR
            "largest_position_pct": Decimal("0.25") if portfolio.get("positions") else Decimal("0")
        }
    
    def _calculate_margin_status(self, portfolio: Dict) -> MarginStatus:
        """Calculate margin status"""
        total_collateral = Decimal(str(portfolio.get("total_collateral", 0)))
        total_margin_used = Decimal(str(portfolio.get("total_margin_used", 0)))
        
        if total_margin_used > 0:
            margin_level = (total_collateral / total_margin_used) * 100
        else:
            margin_level = Decimal("999")
        
        # Determine health status
        if margin_level < 110:
            health_status = "critical"
        elif margin_level < 130:
            health_status = "warning"
        else:
            health_status = "healthy"
        
        return MarginStatus(
            margin_level=margin_level,
            margin_used=total_margin_used,
            free_margin=total_collateral - total_margin_used,
            equity=total_collateral,
            health_status=health_status
        )
    
    def _check_risk_violations(
        self,
        portfolio: Dict,
        limits: Dict,
        risk_metrics: Dict
    ) -> List[Dict]:
        """Check for risk limit violations"""
        violations = []
        
        # Check leverage
        if portfolio.get("positions"):
            max_leverage = max(
                Decimal(str(p.get("leverage", 1)))
                for p in portfolio["positions"]
            )
            if max_leverage > limits["max_leverage"]:
                violations.append({
                    "type": "leverage",
                    "current": max_leverage,
                    "limit": limits["max_leverage"],
                    "severity": "high"
                })
        
        # Check position count
        if len(portfolio.get("positions", [])) > limits["max_open_positions"]:
            violations.append({
                "type": "position_count",
                "current": len(portfolio["positions"]),
                "limit": limits["max_open_positions"],
                "severity": "medium"
            })
        
        return violations
    
    def _generate_alerts(
        self,
        user_id: str,
        portfolio: Dict,
        risk_metrics: Dict,
        margin_status: MarginStatus,
        ml_assessments: Dict
    ) -> List[Dict]:
        """Generate risk alerts enhanced with ML insights"""
        alerts = []
        
        # Margin alerts
        if margin_status.margin_level < 110:
            alerts.append({
                "type": "margin_call",
                "severity": "critical",
                "message": f"Margin level critical: {margin_status.margin_level:.1f}%",
                "action_required": True
            })
        elif margin_status.margin_level < 130:
            alerts.append({
                "type": "margin_warning",
                "severity": "high",
                "message": f"Margin level low: {margin_status.margin_level:.1f}%",
                "action_required": False
            })
        
        # ML-based alerts
        for position_id, assessment in ml_assessments.items():
            position_risk = assessment.get("position_risk", {})
            
            if isinstance(position_risk, dict):
                liquidation_prob = position_risk.get("liquidation_probability", 0)
                if liquidation_prob > 0.3:
                    alerts.append({
                        "type": "liquidation_risk",
                        "severity": "high" if liquidation_prob > 0.5 else "medium",
                        "message": f"Position {position_id}: {liquidation_prob*100:.0f}% liquidation probability",
                        "ml_insight": True,
                        "recommendations": position_risk.get("recommendations", [])
                    })
        
        return alerts
    
    def _determine_actions(
        self,
        violations: List[Dict],
        margin_status: MarginStatus,
        ml_assessments: Dict
    ) -> List[Dict]:
        """Determine required actions"""
        actions = []
        
        if margin_status.margin_level < 110:
            actions.append({
                "type": "liquidation_required",
                "urgency": "immediate",
                "description": "Liquidate positions to restore margin"
            })
        elif margin_status.margin_level < 130:
            actions.append({
                "type": "add_collateral",
                "urgency": "high",
                "description": "Add collateral or reduce positions"
            })
        
        # ML-based recommendations
        for assessment in ml_assessments.values():
            position_risk = assessment.get("position_risk", {})
            if isinstance(position_risk, dict):
                for rec in position_risk.get("recommendations", []):
                    actions.append({
                        "type": "ml_recommendation",
                        "urgency": "medium",
                        "description": rec
                    })
        
        return actions
    
    async def _publish_risk_events(
        self,
        user_id: str,
        state: RiskState,
        violations: List[Dict],
        actions_required: List[Dict]
    ):
        """Publish risk events"""
        # Publish margin call events
        if state.margin_status.health_status == "critical":
            event = RiskEvent(
                event_type=EventType.MARGIN_CALL,
                user_id=user_id,
                risk_type="margin_call",
                severity="critical",
                metric_name="margin_level",
                metric_value=state.margin_status.margin_level,
                action_required=True,
                message=f"Margin call for user {user_id}"
            )
            await self.event_publisher.publish_event(event)
        
        # Publish violation events
        for violation in violations:
            event = RiskEvent(
                event_type=EventType.RISK_ALERT,
                user_id=user_id,
                risk_type=violation["type"],
                severity=violation["severity"],
                metric_name=violation["type"],
                metric_value=Decimal(str(violation["current"])),
                threshold_value=Decimal(str(violation["limit"])),
                action_required=True,
                message=f"Risk limit violation: {violation['type']}"
            )
            await self.event_publisher.publish_event(event) 