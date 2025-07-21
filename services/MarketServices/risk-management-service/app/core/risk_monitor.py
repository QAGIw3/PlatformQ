"""Real-time risk monitoring engine"""

import asyncio
from decimal import Decimal
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from dataclasses import dataclass
import time
import logging

from platformq_trading_common.risk.models import (
    Position, Portfolio, RiskMetrics, RiskLimits, RiskCalculator
)
from platformq_trading_common.events.trading_events import (
    EventType, RiskEvent, PositionEvent, EventPublisher
)
from platformq_trading_common.models.orders import MarketType

from ..config import RiskManagementConfig
from ..models.risk_state import RiskState, AlertLevel, MarginStatus
from ..models.risk_models import MarketRisk, PositionRisk
from ..integrations.market_data import MarketDataClient
from ..integrations.position_service import PositionServiceClient
from .ml_risk_engine import MLRiskEngine


logger = logging.getLogger(__name__)


@dataclass
class MonitoringResult:
    """Result of risk monitoring check"""
    trader_id: str
    timestamp: datetime
    risk_metrics: RiskMetrics
    margin_status: MarginStatus
    alerts: List[Dict]
    violations: List[Dict]
    actions_required: List[Dict]
    ml_assessment: Optional[Dict] = None  # ML-based assessment


class RiskMonitor:
    """Real-time risk monitoring engine with ML capabilities"""
    
    def __init__(
        self,
        config: RiskManagementConfig,
        market_data_client: MarketDataClient,
        position_client: PositionServiceClient,
        event_publisher: EventPublisher,
        ignite_client=None
    ):
        self.config = config
        self.market_data = market_data_client
        self.position_client = position_client
        self.event_publisher = event_publisher
        
        # Initialize ML risk engine
        self.ml_engine = MLRiskEngine(config, ignite_client)
        
        # Active monitoring
        self.monitored_traders: Set[str] = set()
        self.trader_portfolios: Dict[str, Portfolio] = {}
        self.trader_limits: Dict[str, RiskLimits] = {}
        self.trader_states: Dict[str, RiskState] = {}
        
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
    
    async def start(self):
        """Start risk monitoring"""
        self._running = True
        
        # Start monitoring tasks
        self._tasks.append(
            asyncio.create_task(self._continuous_monitoring())
        )
        self._tasks.append(
            asyncio.create_task(self._price_update_task())
        )
        self._tasks.append(
            asyncio.create_task(self._liquidation_monitor())
        )
        self._tasks.append(
            asyncio.create_task(self._market_data_update_task())
        )
        
        logger.info("Risk monitoring started with ML capabilities")
    
    async def stop(self):
        """Stop risk monitoring"""
        self._running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        logger.info("Risk monitoring stopped")
    
    async def add_trader_monitoring(
        self,
        trader_id: str,
        risk_limits: Optional[RiskLimits] = None
    ):
        """Add trader to active monitoring"""
        self.monitored_traders.add(trader_id)
        
        # Set risk limits
        if risk_limits:
            self.trader_limits[trader_id] = risk_limits
        else:
            # Use default limits
            self.trader_limits[trader_id] = RiskLimits(
                max_position_size=self.config.DEFAULT_MAX_POSITION_SIZE,
                max_leverage=self.config.DEFAULT_MAX_LEVERAGE,
                max_loss_per_trade=self.config.DEFAULT_MAX_POSITION_SIZE * Decimal("0.02"),
                max_daily_loss=self.config.DEFAULT_MAX_POSITION_SIZE * Decimal("0.05"),
                max_open_positions=20,
                min_margin_level=self.config.DEFAULT_MIN_MARGIN_LEVEL,
                concentration_limit=self.config.DEFAULT_CONCENTRATION_LIMIT
            )
        
        # Initialize portfolio
        await self._refresh_trader_portfolio(trader_id)
        
        logger.info(f"Added trader {trader_id} to monitoring")
    
    async def remove_trader_monitoring(self, trader_id: str):
        """Remove trader from active monitoring"""
        self.monitored_traders.discard(trader_id)
        self.trader_portfolios.pop(trader_id, None)
        self.trader_limits.pop(trader_id, None)
        self.trader_states.pop(trader_id, None)
        
        logger.info(f"Removed trader {trader_id} from monitoring")
    
    async def check_trader_risk(self, trader_id: str) -> MonitoringResult:
        """Check risk for a specific trader with ML enhancement"""
        # Get portfolio
        portfolio = self.trader_portfolios.get(trader_id)
        if not portfolio:
            await self._refresh_trader_portfolio(trader_id)
            portfolio = self.trader_portfolios.get(trader_id)
        
        if not portfolio:
            raise ValueError(f"No portfolio found for trader {trader_id}")
        
        # Update position prices
        await self._update_portfolio_prices(portfolio)
        
        # Traditional risk metrics
        risk_metrics = await self._calculate_risk_metrics(portfolio)
        
        # Check margin status
        margin_status = self._calculate_margin_status(portfolio)
        
        # ML-based assessment for each position
        ml_assessments = {}
        for position in portfolio.positions:
            market_data = await self._get_enriched_market_data(position.market_id)
            if market_data:
                # Get ML market risk assessment
                market_risk = await self.ml_engine.assess_market_risk(
                    position.market_id,
                    market_data
                )
                
                # Get position risk assessment
                position_dict = self._position_to_dict(position)
                position_risk = await self.ml_engine.assess_position_risk(
                    position_dict,
                    market_risk
                )
                
                ml_assessments[position.position_id] = {
                    "market_risk": market_risk.to_dict(),
                    "position_risk": position_risk.to_dict()
                }
        
        # Check for violations
        limits = self.trader_limits[trader_id]
        violations = RiskCalculator.check_risk_limits(portfolio, limits)
        
        # Generate alerts (enhanced with ML insights)
        alerts = self._generate_alerts(trader_id, portfolio, risk_metrics, margin_status)
        ml_alerts = self._generate_ml_alerts(ml_assessments)
        alerts.extend(ml_alerts)
        
        # Determine required actions (enhanced with ML recommendations)
        actions = self._determine_actions(trader_id, portfolio, margin_status, violations)
        ml_actions = self._determine_ml_actions(ml_assessments)
        actions.extend(ml_actions)
        
        # Update trader state
        self.trader_states[trader_id] = RiskState(
            trader_id=trader_id,
            risk_metrics=risk_metrics,
            margin_status=margin_status,
            active_alerts=alerts,
            last_check=datetime.utcnow()
        )
        
        return MonitoringResult(
            trader_id=trader_id,
            timestamp=datetime.utcnow(),
            risk_metrics=risk_metrics,
            margin_status=margin_status,
            alerts=alerts,
            violations=violations,
            actions_required=actions,
            ml_assessment=ml_assessments
        )
    
    def _position_to_dict(self, position: Position) -> Dict:
        """Convert Position object to dictionary for ML engine"""
        return {
            "position_id": position.position_id,
            "market_id": position.market_id,
            "side": position.side,
            "size": str(position.size),
            "entry_price": str(position.entry_price),
            "mark_price": str(position.mark_price),
            "leverage": str(position.leverage),
            "margin_used": str(position.margin_used),
            "collateral_value": str(position.initial_margin),
            "health_factor": float(position.margin_used / position.initial_margin) if position.initial_margin > 0 else 1.0,
            "unrealized_pnl_percent": float(position.unrealized_pnl / position.notional_value) if position.notional_value > 0 else 0,
            "time_held_hours": 24,  # Placeholder
            "notional_value": str(position.notional_value)
        }
    
    def _generate_ml_alerts(self, ml_assessments: Dict) -> List[Dict]:
        """Generate alerts based on ML assessments"""
        alerts = []
        
        for position_id, assessment in ml_assessments.items():
            position_risk = assessment["position_risk"]
            market_risk = assessment["market_risk"]
            
            # High liquidation probability alert
            if float(position_risk["liquidation_probability"]) > 0.3:
                alerts.append({
                    "type": "ml_liquidation_risk",
                    "level": AlertLevel.HIGH,
                    "message": f"ML model predicts {float(position_risk['liquidation_probability'])*100:.1f}% liquidation probability for position {position_id}",
                    "data": {
                        "position_id": position_id,
                        "liquidation_probability": float(position_risk["liquidation_probability"])
                    }
                })
            
            # Market anomaly alert
            if market_risk["anomaly_score"] > 0.7:
                alerts.append({
                    "type": "market_anomaly",
                    "level": AlertLevel.HIGH,
                    "message": f"Anomalous market behavior detected for {market_risk['market_id']}",
                    "data": {
                        "market_id": market_risk["market_id"],
                        "anomaly_score": market_risk["anomaly_score"]
                    }
                })
            
            # Volatility spike alert
            if float(market_risk["predicted_volatility"]) > float(market_risk["current_volatility"]) * 1.5:
                alerts.append({
                    "type": "volatility_spike_predicted",
                    "level": AlertLevel.MEDIUM,
                    "message": f"Volatility spike predicted for {market_risk['market_id']}: {float(market_risk['predicted_volatility'])*100:.1f}%",
                    "data": {
                        "market_id": market_risk["market_id"],
                        "current_volatility": float(market_risk["current_volatility"]),
                        "predicted_volatility": float(market_risk["predicted_volatility"])
                    }
                })
        
        return alerts
    
    def _determine_ml_actions(self, ml_assessments: Dict) -> List[Dict]:
        """Determine actions based on ML assessments"""
        actions = []
        
        for position_id, assessment in ml_assessments.items():
            position_risk = assessment["position_risk"]
            
            # Add ML recommendations as actions
            for recommendation in position_risk["recommendations"]:
                actions.append({
                    "action": "ml_recommendation",
                    "urgency": "medium",
                    "reason": recommendation,
                    "position_id": position_id,
                    "risk_score": position_risk.get("risk_score", 0)
                })
            
            # Check stress test results
            for stress_test in position_risk["stress_test_results"]:
                if not stress_test["survival"]:
                    actions.append({
                        "action": "hedge_position",
                        "urgency": "high",
                        "reason": f"Position would not survive {stress_test['scenario']} scenario",
                        "position_id": position_id,
                        "scenario": stress_test["scenario"],
                        "potential_loss": stress_test["potential_loss"]
                    })
        
        return actions
    
    async def _get_enriched_market_data(self, market_id: str) -> Optional[Dict]:
        """Get enriched market data for ML features"""
        # Check cache first
        if market_id in self.market_data_cache:
            cache_age = (datetime.utcnow() - self.market_data_cache[market_id].get("timestamp", datetime.min)).seconds
            if cache_age < 30:  # 30 second cache
                return self.market_data_cache[market_id]
        
        try:
            # Get basic price data
            price_data = await self.market_data.get_price(market_id)
            if not price_data:
                return None
            
            # Get historical returns
            historical_returns = self.returns_data.get(market_id, [])
            
            # Calculate basic metrics
            enriched_data = {
                "market_id": market_id,
                "price": float(price_data["price"]),
                "volatility_24h": 0.02,  # Placeholder
                "volume_24h": 1000000,  # Placeholder
                "avg_volume": 1000000,  # Placeholder
                "price_change_24h": 0.01,  # Placeholder
                "rsi": 50,  # Placeholder
                "bollinger_position": 0.5,  # Placeholder
                "bid_ask_spread": 0.0001,  # Placeholder
                "order_book_imbalance": 0,  # Placeholder
                "funding_rate": 0.0001,  # Placeholder
                "open_interest_change": 0,  # Placeholder
                "order_book_depth": 500000,  # Placeholder
                "historical_returns": historical_returns,
                "timestamp": datetime.utcnow()
            }
            
            # Cache the data
            self.market_data_cache[market_id] = enriched_data
            
            return enriched_data
            
        except Exception as e:
            logger.error(f"Error getting enriched market data for {market_id}: {e}")
            return None
    
    async def _market_data_update_task(self):
        """Update market data for ML features"""
        while self._running:
            try:
                # Get unique markets from all portfolios
                market_ids = set()
                for portfolio in self.trader_portfolios.values():
                    for position in portfolio.positions:
                        market_ids.add(position.market_id)
                
                # Update market data for each market
                for market_id in market_ids:
                    await self._get_enriched_market_data(market_id)
                
                await asyncio.sleep(10)  # Update every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in market data update task: {e}")
                await asyncio.sleep(1)
    
    async def _continuous_monitoring(self):
        """Continuously monitor all traders"""
        while self._running:
            try:
                # Monitor each trader
                tasks = []
                for trader_id in list(self.monitored_traders):
                    tasks.append(self._monitor_trader(trader_id))
                
                # Run monitoring in parallel
                if tasks:
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Process results
                    for result in results:
                        if isinstance(result, Exception):
                            logger.error(f"Error in monitoring: {result}")
                
                # Wait for next interval
                await asyncio.sleep(self.config.RISK_CALCULATION_INTERVAL_SECONDS)
                
            except Exception as e:
                logger.error(f"Error in continuous monitoring: {e}")
                await asyncio.sleep(1)
    
    async def _monitor_trader(self, trader_id: str):
        """Monitor a single trader"""
        try:
            result = await self.check_trader_risk(trader_id)
            
            # Publish events for alerts
            for alert in result.alerts:
                if alert["level"] in (AlertLevel.HIGH, AlertLevel.CRITICAL):
                    await self._publish_risk_event(trader_id, alert)
            
            # Handle margin calls
            if result.margin_status.level <= self.config.MARGIN_CALL_THRESHOLD:
                await self._handle_margin_call(trader_id, result)
            
            # Handle liquidations
            if result.margin_status.level <= self.config.LIQUIDATION_THRESHOLD:
                await self._handle_liquidation(trader_id, result)
                
        except Exception as e:
            logger.error(f"Error monitoring trader {trader_id}: {e}")
    
    async def _refresh_trader_portfolio(self, trader_id: str):
        """Refresh trader's portfolio from position service"""
        try:
            # Get positions from position service
            positions_data = await self.position_client.get_trader_positions(trader_id)
            
            # Create portfolio
            portfolio = Portfolio(
                portfolio_id=f"portfolio_{trader_id}",
                trader_id=trader_id,
                positions=[],
                cash_balance=positions_data.get("cash_balance", Decimal(0))
            )
            
            # Add positions
            for pos_data in positions_data.get("positions", []):
                position = Position(
                    position_id=pos_data["position_id"],
                    market_id=pos_data["market_id"],
                    trader_id=trader_id,
                    side=pos_data["side"],
                    size=Decimal(pos_data["size"]),
                    entry_price=Decimal(pos_data["entry_price"]),
                    mark_price=Decimal(pos_data.get("mark_price", pos_data["entry_price"])),
                    leverage=Decimal(pos_data.get("leverage", "1")),
                    initial_margin=Decimal(pos_data.get("initial_margin", "0")),
                    maintenance_margin=Decimal(pos_data.get("maintenance_margin", "0")),
                    margin_used=Decimal(pos_data.get("margin_used", "0")),
                    realized_pnl=Decimal(pos_data.get("realized_pnl", "0")),
                    unrealized_pnl=Decimal(pos_data.get("unrealized_pnl", "0")),
                    fees_paid=Decimal(pos_data.get("fees_paid", "0"))
                )
                portfolio.add_position(position)
            
            self.trader_portfolios[trader_id] = portfolio
            
        except Exception as e:
            logger.error(f"Error refreshing portfolio for {trader_id}: {e}")
    
    async def _update_portfolio_prices(self, portfolio: Portfolio):
        """Update mark prices for all positions"""
        for position in portfolio.positions:
            # Get latest price
            price = await self._get_market_price(position.market_id)
            if price:
                position.mark_price = price
                position.unrealized_pnl = position.calculate_unrealized_pnl(price)
    
    async def _get_market_price(self, market_id: str) -> Optional[Decimal]:
        """Get latest market price"""
        # Check cache first
        if market_id in self.price_cache:
            cache_time = self.price_cache_timestamp.get(market_id)
            if cache_time and (datetime.utcnow() - cache_time).seconds < 5:
                return self.price_cache[market_id]
        
        # Fetch from market data service
        try:
            price_data = await self.market_data.get_price(market_id)
            price = Decimal(price_data["price"])
            
            # Update cache
            self.price_cache[market_id] = price
            self.price_cache_timestamp[market_id] = datetime.utcnow()
            
            return price
            
        except Exception as e:
            logger.error(f"Error fetching price for {market_id}: {e}")
            return self.price_cache.get(market_id)
    
    async def _calculate_risk_metrics(self, portfolio: Portfolio) -> RiskMetrics:
        """Calculate comprehensive risk metrics"""
        metrics = RiskMetrics()
        
        # Calculate exposures
        long_exposure = Decimal(0)
        short_exposure = Decimal(0)
        
        for position in portfolio.positions:
            if position.side == "long":
                long_exposure += position.notional_value
            else:
                short_exposure += position.notional_value
        
        metrics.gross_exposure = long_exposure + short_exposure
        metrics.net_exposure = long_exposure - short_exposure
        metrics.total_exposure = metrics.gross_exposure
        
        # Calculate concentration
        if portfolio.total_value > 0:
            position_values = [p.notional_value for p in portfolio.positions]
            if position_values:
                largest = max(position_values)
                metrics.largest_position_pct = float(largest / portfolio.total_value)
        
        # Calculate VaR if we have returns data
        market_ids = [p.market_id for p in portfolio.positions]
        returns_data = {}
        
        for market_id in market_ids:
            if market_id in self.returns_data:
                returns_data[market_id] = self.returns_data[market_id]
        
        if returns_data and portfolio.positions:
            metrics.var_95 = RiskCalculator.calculate_portfolio_var(
                portfolio.positions,
                returns_data,
                self.config.VAR_CONFIDENCE_LEVEL
            )
        
        return metrics
    
    def _calculate_margin_status(self, portfolio: Portfolio) -> MarginStatus:
        """Calculate margin status"""
        return MarginStatus(
            margin_level=portfolio.margin_level,
            margin_used=portfolio.total_margin_used,
            free_margin=portfolio.free_margin,
            equity=portfolio.total_value,
            is_margin_call=portfolio.margin_level < self.config.MARGIN_CALL_THRESHOLD,
            is_liquidation=portfolio.margin_level < self.config.LIQUIDATION_THRESHOLD
        )
    
    def _generate_alerts(
        self,
        trader_id: str,
        portfolio: Portfolio,
        risk_metrics: RiskMetrics,
        margin_status: MarginStatus
    ) -> List[Dict]:
        """Generate risk alerts"""
        alerts = []
        
        # Margin alerts
        if margin_status.is_liquidation:
            alerts.append({
                "type": "liquidation_imminent",
                "level": AlertLevel.CRITICAL,
                "message": f"Liquidation imminent! Margin level: {margin_status.margin_level}%",
                "data": {"margin_level": float(margin_status.margin_level)}
            })
        elif margin_status.is_margin_call:
            alerts.append({
                "type": "margin_call",
                "level": AlertLevel.HIGH,
                "message": f"Margin call! Margin level: {margin_status.margin_level}%",
                "data": {"margin_level": float(margin_status.margin_level)}
            })
        elif margin_status.margin_level < self.config.WARNING_THRESHOLD:
            alerts.append({
                "type": "margin_warning",
                "level": AlertLevel.MEDIUM,
                "message": f"Low margin warning. Margin level: {margin_status.margin_level}%",
                "data": {"margin_level": float(margin_status.margin_level)}
            })
        
        # Concentration alerts
        if risk_metrics.largest_position_pct > float(self.config.DEFAULT_CONCENTRATION_LIMIT) / 100:
            alerts.append({
                "type": "concentration_risk",
                "level": AlertLevel.MEDIUM,
                "message": f"High concentration risk: {risk_metrics.largest_position_pct * 100:.1f}% in single position",
                "data": {"concentration": risk_metrics.largest_position_pct}
            })
        
        return alerts
    
    def _determine_actions(
        self,
        trader_id: str,
        portfolio: Portfolio,
        margin_status: MarginStatus,
        violations: Dict[str, bool]
    ) -> List[Dict]:
        """Determine required risk management actions"""
        actions = []
        
        if margin_status.is_liquidation:
            actions.append({
                "action": "liquidate_positions",
                "urgency": "immediate",
                "reason": "Below liquidation threshold",
                "target_positions": [p.position_id for p in portfolio.positions]
            })
        
        elif margin_status.is_margin_call:
            required_margin = portfolio.total_margin_used * (
                self.config.MARGIN_CALL_THRESHOLD / 100
            ) - portfolio.total_value
            
            actions.append({
                "action": "deposit_margin",
                "urgency": "high",
                "reason": "Margin call",
                "required_amount": str(required_margin)
            })
            
            actions.append({
                "action": "reduce_positions",
                "urgency": "high",
                "reason": "Margin call - alternative to deposit",
                "suggested_reduction": "20%"
            })
        
        # Handle specific violations
        for violation_key, violated in violations.items():
            if violated and violation_key.startswith("position_size_"):
                position_id = violation_key.replace("position_size_", "")
                actions.append({
                    "action": "reduce_position",
                    "urgency": "medium",
                    "reason": "Position size limit exceeded",
                    "position_id": position_id
                })
        
        return actions
    
    async def _publish_risk_event(self, trader_id: str, alert: Dict):
        """Publish risk event to Pulsar"""
        event = RiskEvent(
            event_type=EventType.RISK_LIMIT_BREACH,
            source_service=self.config.SERVICE_NAME,
            trader_id=trader_id,
            risk_type=alert["type"],
            severity=alert["level"].value,
            current_value=str(alert["data"].get("margin_level", 0)),
            threshold_value=str(self.config.MARGIN_CALL_THRESHOLD),
            required_action=alert["message"]
        )
        
        await self.event_publisher.publish_event(event)
    
    async def _handle_margin_call(self, trader_id: str, result: MonitoringResult):
        """Handle margin call"""
        logger.warning(f"Margin call for trader {trader_id}")
        
        # Publish margin call event
        event = RiskEvent(
            event_type=EventType.MARGIN_CALL,
            source_service=self.config.SERVICE_NAME,
            trader_id=trader_id,
            risk_type="margin_call",
            severity="high",
            current_value=str(result.margin_status.margin_level),
            threshold_value=str(self.config.MARGIN_CALL_THRESHOLD),
            required_action="Deposit margin or reduce positions",
            deadline=datetime.utcnow() + timedelta(hours=24)
        )
        
        await self.event_publisher.publish_event(event)
    
    async def _handle_liquidation(self, trader_id: str, result: MonitoringResult):
        """Handle liquidation"""
        logger.critical(f"Initiating liquidation for trader {trader_id}")
        
        # This would trigger the liquidation engine
        # For now, just publish event
        event = RiskEvent(
            event_type=EventType.LIQUIDATION_WARNING,
            source_service=self.config.SERVICE_NAME,
            trader_id=trader_id,
            risk_type="liquidation",
            severity="critical",
            current_value=str(result.margin_status.margin_level),
            threshold_value=str(self.config.LIQUIDATION_THRESHOLD),
            required_action="Immediate liquidation required",
            affected_positions=[p.position_id for p in result.actions_required[0].get("target_positions", [])]
        )
        
        await self.event_publisher.publish_event(event)
    
    async def _price_update_task(self):
        """Update prices periodically"""
        while self._running:
            try:
                # Update prices for all monitored markets
                market_ids = set()
                for portfolio in self.trader_portfolios.values():
                    for position in portfolio.positions:
                        market_ids.add(position.market_id)
                
                # Fetch prices in parallel
                tasks = [self._get_market_price(market_id) for market_id in market_ids]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                await asyncio.sleep(1)  # Update every second
                
            except Exception as e:
                logger.error(f"Error in price update task: {e}")
                await asyncio.sleep(1)
    
    async def _liquidation_monitor(self):
        """Monitor for liquidation conditions"""
        while self._running:
            try:
                # Check all traders for liquidation
                for trader_id in list(self.monitored_traders):
                    state = self.trader_states.get(trader_id)
                    if state and state.margin_status.is_liquidation:
                        await self._handle_liquidation(trader_id, None)
                
                await asyncio.sleep(1)  # Check every second
                
            except Exception as e:
                logger.error(f"Error in liquidation monitor: {e}")
                await asyncio.sleep(1) 