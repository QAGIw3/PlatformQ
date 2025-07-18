"""
Real-time Risk Monitoring System

Leverages Apache Flink for stream processing and Ignite for distributed caching
to provide real-time risk monitoring and automated mitigation.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any, Set
from decimal import Decimal, getcontext
from datetime import datetime, timedelta
from enum import Enum
import logging
from dataclasses import dataclass, field
import numpy as np
from collections import defaultdict
import json

from platformq_shared import ServiceClient, ProcessingResult, ProcessingStatus
from ..integrations import IgniteCache as IgniteClient, PulsarEventPublisher as PulsarClient
from ..models.position import Position, PositionSide
from ..models.market import Market
from .oracle_risk_engine import OracleRiskEngine, RiskProfile, RiskMetric

# Set high precision
getcontext().prec = 28

logger = logging.getLogger(__name__)


class RiskAlert(Enum):
    """Types of risk alerts"""
    POSITION_LIMIT_WARNING = "position_limit_warning"
    POSITION_LIMIT_BREACH = "position_limit_breach"
    LEVERAGE_WARNING = "leverage_warning"
    LEVERAGE_BREACH = "leverage_breach"
    LIQUIDATION_WARNING = "liquidation_warning"
    CONCENTRATION_WARNING = "concentration_warning"
    VOLATILITY_SPIKE = "volatility_spike"
    CORRELATION_BREAKDOWN = "correlation_breakdown"
    FUNDING_RATE_SPIKE = "funding_rate_spike"
    SYSTEM_RISK_ELEVATED = "system_risk_elevated"


class MitigationAction(Enum):
    """Automated risk mitigation actions"""
    REDUCE_POSITION = "reduce_position"
    DELEVERAGE = "deleverage"
    HEDGE_POSITION = "hedge_position"
    HALT_TRADING = "halt_trading"
    INCREASE_MARGIN = "increase_margin"
    CIRCUIT_BREAKER = "circuit_breaker"
    AUTO_DELEVERAGE = "auto_deleverage"
    LIQUIDATE = "liquidate"


@dataclass
class RiskLimit:
    """Risk limits configuration"""
    max_position_size: Decimal
    max_leverage: Decimal
    max_concentration: Decimal  # % of total portfolio
    max_var_percent: Decimal  # % of capital
    max_drawdown: Decimal  # % drawdown trigger
    min_margin_ratio: Decimal
    
    # Dynamic limits based on volatility
    vol_adjustment_enabled: bool = True
    base_volatility: Decimal = Decimal("0.2")  # 20% annual vol


@dataclass
class CircuitBreaker:
    """Circuit breaker configuration"""
    breaker_id: str
    market_id: str
    trigger_type: str  # price, volume, volatility
    threshold: Decimal
    duration: timedelta
    
    # State
    triggered: bool = False
    trigger_time: Optional[datetime] = None
    trigger_value: Optional[Decimal] = None


@dataclass
class RiskEvent:
    """Risk event for stream processing"""
    event_id: str
    timestamp: datetime
    event_type: str
    market_id: Optional[str]
    user_id: Optional[str]
    data: Dict[str, Any]
    severity: int  # 1-5 scale
    
    def to_json(self) -> str:
        """Convert to JSON for Flink processing"""
        return json.dumps({
            "event_id": self.event_id,
            "timestamp": self.timestamp.isoformat(),
            "event_type": self.event_type,
            "market_id": self.market_id,
            "user_id": self.user_id,
            "data": self.data,
            "severity": self.severity
        })


@dataclass
class AggregatedRiskMetrics:
    """Aggregated risk metrics for system monitoring"""
    timestamp: datetime
    total_positions: int
    total_notional: Decimal
    total_var: Decimal
    average_leverage: Decimal
    concentration_metrics: Dict[str, Decimal]
    stressed_positions: int
    liquidation_candidates: int
    system_risk_score: int  # 0-100


class RealtimeRiskMonitor:
    """Real-time risk monitoring with Flink stream processing"""
    
    def __init__(
        self,
        oracle_engine: OracleRiskEngine,
        ignite_client: IgniteClient,
        pulsar_client: PulsarClient,
        flink_job_manager: Optional[str] = "localhost:8081"
    ):
        self.oracle_engine = oracle_engine
        self.ignite_client = ignite_client
        self.pulsar_client = pulsar_client
        self.flink_job_manager = flink_job_manager
        
        # Risk limits by user/account type
        self.risk_limits: Dict[str, RiskLimit] = {}
        self.default_risk_limit = RiskLimit(
            max_position_size=Decimal("1000000"),  # $1M
            max_leverage=Decimal("20"),
            max_concentration=Decimal("0.2"),  # 20%
            max_var_percent=Decimal("0.1"),  # 10% of capital
            max_drawdown=Decimal("0.25"),  # 25% drawdown
            min_margin_ratio=Decimal("0.05")  # 5% minimum
        )
        
        # Circuit breakers
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        
        # Position tracking for real-time monitoring
        self.monitored_positions: Dict[str, Position] = {}
        self.position_risk_profiles: Dict[str, RiskProfile] = {}
        
        # Alert tracking
        self.active_alerts: Dict[str, List[RiskAlert]] = defaultdict(list)
        self.alert_history: List[Dict] = []
        
        # System metrics
        self.system_metrics = AggregatedRiskMetrics(
            timestamp=datetime.now(),
            total_positions=0,
            total_notional=Decimal("0"),
            total_var=Decimal("0"),
            average_leverage=Decimal("0"),
            concentration_metrics={},
            stressed_positions=0,
            liquidation_candidates=0,
            system_risk_score=0
        )
        
        # Flink job IDs
        self.flink_jobs: Dict[str, str] = {}
        
        # Background tasks
        self._running = True
        self._tasks = []
    
    async def start(self):
        """Start risk monitoring system"""
        # Initialize Flink jobs
        await self._initialize_flink_jobs()
        
        # Start background monitoring tasks
        self._tasks.append(asyncio.create_task(self._position_monitor_loop()))
        self._tasks.append(asyncio.create_task(self._system_risk_loop()))
        self._tasks.append(asyncio.create_task(self._circuit_breaker_loop()))
        self._tasks.append(asyncio.create_task(self._alert_processor_loop()))
        
        logger.info("Real-time risk monitoring started")
    
    async def stop(self):
        """Stop risk monitoring system"""
        self._running = False
        
        # Cancel Flink jobs
        for job_name, job_id in self.flink_jobs.items():
            await self._cancel_flink_job(job_id)
        
        # Cancel background tasks
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        logger.info("Real-time risk monitoring stopped")
    
    async def _initialize_flink_jobs(self):
        """Initialize Flink streaming jobs for risk processing"""
        try:
            # 1. Position risk monitoring job
            position_job = await self._submit_flink_job(
                "position-risk-monitor",
                self._generate_position_monitoring_job()
            )
            self.flink_jobs["position_monitor"] = position_job
            
            # 2. Market risk aggregation job
            market_job = await self._submit_flink_job(
                "market-risk-aggregator",
                self._generate_market_risk_job()
            )
            self.flink_jobs["market_risk"] = market_job
            
            # 3. Alert detection job
            alert_job = await self._submit_flink_job(
                "risk-alert-detector",
                self._generate_alert_detection_job()
            )
            self.flink_jobs["alert_detector"] = alert_job
            
            logger.info(f"Initialized {len(self.flink_jobs)} Flink jobs")
            
        except Exception as e:
            logger.error(f"Error initializing Flink jobs: {str(e)}")
    
    def _generate_position_monitoring_job(self) -> Dict:
        """Generate Flink job for position monitoring"""
        return {
            "name": "position-risk-monitor",
            "source": {
                "type": "pulsar",
                "topic": "position-updates",
                "subscription": "risk-monitor"
            },
            "transformations": [
                {
                    "type": "map",
                    "function": "calculate_position_risk"
                },
                {
                    "type": "filter",
                    "predicate": "risk_score > 70"
                },
                {
                    "type": "window",
                    "window_type": "sliding",
                    "size": "5m",
                    "slide": "1m"
                }
            ],
            "sink": {
                "type": "ignite",
                "cache": "position_risk_profiles"
            }
        }
    
    def _generate_market_risk_job(self) -> Dict:
        """Generate Flink job for market risk aggregation"""
        return {
            "name": "market-risk-aggregator",
            "source": {
                "type": "pulsar",
                "topics": ["trades", "orderbook-updates", "funding-rates"]
            },
            "transformations": [
                {
                    "type": "keyBy",
                    "field": "market_id"
                },
                {
                    "type": "window",
                    "window_type": "tumbling",
                    "size": "1m"
                },
                {
                    "type": "aggregate",
                    "functions": ["volatility", "volume", "spread"]
                }
            ],
            "sink": {
                "type": "ignite",
                "cache": "market_risk_metrics"
            }
        }
    
    def _generate_alert_detection_job(self) -> Dict:
        """Generate Flink job for alert detection"""
        return {
            "name": "risk-alert-detector",
            "source": {
                "type": "ignite",
                "cache": "position_risk_profiles",
                "continuous": True
            },
            "transformations": [
                {
                    "type": "flatMap",
                    "function": "detect_risk_conditions"
                },
                {
                    "type": "filter",
                    "predicate": "severity >= 3"
                }
            ],
            "sink": {
                "type": "pulsar",
                "topic": "risk-alerts"
            }
        }
    
    async def _submit_flink_job(self, name: str, job_spec: Dict) -> str:
        """Submit a Flink job and return job ID"""
        # In production, this would submit to actual Flink cluster
        # For now, return mock job ID
        job_id = f"flink-job-{name}-{datetime.now().timestamp()}"
        logger.info(f"Submitted Flink job: {name} -> {job_id}")
        return job_id
    
    async def _cancel_flink_job(self, job_id: str):
        """Cancel a running Flink job"""
        logger.info(f"Cancelling Flink job: {job_id}")
        # In production, would call Flink REST API
    
    async def monitor_position(
        self,
        position: Position,
        market: Market,
        user_capital: Decimal
    ) -> ProcessingResult:
        """Monitor a position for risk"""
        try:
            # Store position for monitoring
            position_key = f"{position.user_id}_{position.market_id}"
            self.monitored_positions[position_key] = position
            
            # Calculate risk profile
            risk_profile = await self.oracle_engine.calculate_position_risk(
                position, market
            )
            
            # Store risk profile
            self.position_risk_profiles[position_key] = risk_profile
            await self.ignite_client.put(
                f"risk_profile_{position_key}",
                risk_profile
            )
            
            # Check against limits
            risk_limit = self.risk_limits.get(
                position.user_id,
                self.default_risk_limit
            )
            
            alerts = await self._check_risk_limits(
                position, risk_profile, risk_limit, user_capital
            )
            
            # Process alerts
            if alerts:
                self.active_alerts[position_key] = alerts
                
                # Emit risk event
                for alert in alerts:
                    event = RiskEvent(
                        event_id=f"risk-{datetime.now().timestamp()}",
                        timestamp=datetime.now(),
                        event_type=alert.value,
                        market_id=position.market_id,
                        user_id=position.user_id,
                        data={
                            "position_size": str(position.size),
                            "risk_score": risk_profile.risk_score,
                            "var_95": str(risk_profile.var_95)
                        },
                        severity=self._get_alert_severity(alert)
                    )
                    
                    await self.pulsar_client.publish(
                        "risk-events",
                        event.to_json()
                    )
            
            # Check if mitigation needed
            mitigation_actions = await self._determine_mitigation(
                position, risk_profile, alerts
            )
            
            if mitigation_actions:
                await self._execute_mitigation(
                    position, mitigation_actions
                )
            
            return ProcessingResult(
                status=ProcessingStatus.COMPLETED,
                data={
                    "risk_profile": risk_profile.__dict__,
                    "alerts": [a.value for a in alerts],
                    "mitigations": [m.value for m in mitigation_actions]
                }
            )
            
        except Exception as e:
            logger.error(f"Error monitoring position: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def _check_risk_limits(
        self,
        position: Position,
        risk_profile: RiskProfile,
        risk_limit: RiskLimit,
        user_capital: Decimal
    ) -> List[RiskAlert]:
        """Check position against risk limits"""
        alerts = []
        
        # Position size limit
        if abs(position.size * position.entry_price) > risk_limit.max_position_size:
            alerts.append(RiskAlert.POSITION_LIMIT_BREACH)
        elif abs(position.size * position.entry_price) > risk_limit.max_position_size * Decimal("0.8"):
            alerts.append(RiskAlert.POSITION_LIMIT_WARNING)
        
        # Leverage limit
        if position.leverage > risk_limit.max_leverage:
            alerts.append(RiskAlert.LEVERAGE_BREACH)
        elif position.leverage > risk_limit.max_leverage * Decimal("0.8"):
            alerts.append(RiskAlert.LEVERAGE_WARNING)
        
        # VaR limit
        var_percent = risk_profile.var_95 / user_capital
        if var_percent > risk_limit.max_var_percent:
            alerts.append(RiskAlert.SYSTEM_RISK_ELEVATED)
        
        # Liquidation warning
        if risk_profile.margin_ratio < risk_limit.min_margin_ratio * Decimal("1.5"):
            alerts.append(RiskAlert.LIQUIDATION_WARNING)
        
        # Concentration check
        portfolio_value = self._calculate_portfolio_value(position.user_id)
        if portfolio_value > 0:
            concentration = abs(position.size * position.entry_price) / portfolio_value
            if concentration > risk_limit.max_concentration:
                alerts.append(RiskAlert.CONCENTRATION_WARNING)
        
        return alerts
    
    async def _determine_mitigation(
        self,
        position: Position,
        risk_profile: RiskProfile,
        alerts: List[RiskAlert]
    ) -> List[MitigationAction]:
        """Determine required mitigation actions"""
        actions = []
        
        # Critical alerts requiring immediate action
        critical_alerts = {
            RiskAlert.POSITION_LIMIT_BREACH,
            RiskAlert.LEVERAGE_BREACH,
            RiskAlert.LIQUIDATION_WARNING
        }
        
        if any(alert in critical_alerts for alert in alerts):
            # Check margin ratio
            if risk_profile.margin_ratio < Decimal("0.075"):  # 7.5%
                actions.append(MitigationAction.INCREASE_MARGIN)
            
            if risk_profile.margin_ratio < Decimal("0.05"):  # 5%
                actions.append(MitigationAction.REDUCE_POSITION)
            
            # Leverage breach
            if RiskAlert.LEVERAGE_BREACH in alerts:
                actions.append(MitigationAction.DELEVERAGE)
            
            # Position limit breach
            if RiskAlert.POSITION_LIMIT_BREACH in alerts:
                actions.append(MitigationAction.REDUCE_POSITION)
        
        # Market-wide risk elevation
        if self.system_metrics.system_risk_score > 80:
            actions.append(MitigationAction.AUTO_DELEVERAGE)
        
        # Extreme conditions
        if risk_profile.risk_score > 95:
            actions.append(MitigationAction.CIRCUIT_BREAKER)
        
        return actions
    
    async def _execute_mitigation(
        self,
        position: Position,
        actions: List[MitigationAction]
    ):
        """Execute risk mitigation actions"""
        for action in actions:
            try:
                if action == MitigationAction.REDUCE_POSITION:
                    # Reduce position by 25%
                    reduction_size = position.size * Decimal("0.25")
                    await self._reduce_position(position, reduction_size)
                    
                elif action == MitigationAction.DELEVERAGE:
                    # Reduce leverage to acceptable level
                    target_leverage = self.default_risk_limit.max_leverage * Decimal("0.8")
                    await self._deleverage_position(position, target_leverage)
                    
                elif action == MitigationAction.INCREASE_MARGIN:
                    # Request additional margin
                    await self._request_margin_increase(position)
                    
                elif action == MitigationAction.CIRCUIT_BREAKER:
                    # Trigger circuit breaker
                    await self._trigger_circuit_breaker(position.market_id)
                    
                elif action == MitigationAction.AUTO_DELEVERAGE:
                    # System-wide deleveraging
                    await self._auto_deleverage_market(position.market_id)
                
                # Log mitigation
                await self.pulsar_client.publish(
                    "risk-mitigation-executed",
                    {
                        "position": position.__dict__,
                        "action": action.value,
                        "timestamp": datetime.now()
                    }
                )
                
            except Exception as e:
                logger.error(f"Error executing mitigation {action}: {str(e)}")
    
    async def _reduce_position(self, position: Position, reduction_size: Decimal):
        """Reduce position size"""
        # This would integrate with trading engine
        logger.info(f"Reducing position {position.market_id} by {reduction_size}")
    
    async def _deleverage_position(self, position: Position, target_leverage: Decimal):
        """Reduce position leverage"""
        current_notional = position.size * position.entry_price
        target_notional = current_notional * (target_leverage / position.leverage)
        reduction = current_notional - target_notional
        
        await self._reduce_position(position, reduction / position.entry_price)
    
    async def _request_margin_increase(self, position: Position):
        """Request user to add margin"""
        # Send notification to user
        logger.info(f"Requesting margin increase for position {position.market_id}")
    
    async def _trigger_circuit_breaker(self, market_id: str):
        """Trigger market circuit breaker"""
        breaker = CircuitBreaker(
            breaker_id=f"CB-{market_id}-{datetime.now().timestamp()}",
            market_id=market_id,
            trigger_type="risk",
            threshold=Decimal("95"),  # Risk score threshold
            duration=timedelta(minutes=5),
            triggered=True,
            trigger_time=datetime.now()
        )
        
        self.circuit_breakers[breaker.breaker_id] = breaker
        
        # Halt trading
        await self.pulsar_client.publish(
            "circuit-breaker-triggered",
            {
                "market_id": market_id,
                "duration": str(breaker.duration),
                "reason": "risk_threshold_exceeded"
            }
        )
    
    async def _auto_deleverage_market(self, market_id: str):
        """Auto-deleverage highest risk positions in market"""
        # Get all positions in market sorted by risk
        market_positions = [
            p for p in self.monitored_positions.values()
            if p.market_id == market_id
        ]
        
        # Sort by risk score
        risk_sorted = sorted(
            market_positions,
            key=lambda p: self.position_risk_profiles.get(
                f"{p.user_id}_{p.market_id}",
                RiskProfile(
                    timestamp=datetime.now(),
                    var_95=Decimal("0"),
                    var_99=Decimal("0"),
                    cvar_95=Decimal("0"),
                    portfolio_beta=0,
                    sharpe_ratio=0,
                    max_drawdown=Decimal("0"),
                    liquidation_price=None,
                    margin_ratio=Decimal("1"),
                    risk_score=0,
                    alerts=[]
                )
            ).risk_score,
            reverse=True
        )
        
        # Deleverage top 10% highest risk
        deleverage_count = max(1, len(risk_sorted) // 10)
        
        for position in risk_sorted[:deleverage_count]:
            await self._deleverage_position(
                position,
                self.default_risk_limit.max_leverage * Decimal("0.5")
            )
    
    def _calculate_portfolio_value(self, user_id: str) -> Decimal:
        """Calculate total portfolio value for user"""
        total = Decimal("0")
        for key, position in self.monitored_positions.items():
            if position.user_id == user_id:
                total += abs(position.size * position.entry_price)
        return total
    
    def _get_alert_severity(self, alert: RiskAlert) -> int:
        """Get severity level for alert (1-5)"""
        severity_map = {
            RiskAlert.POSITION_LIMIT_WARNING: 2,
            RiskAlert.POSITION_LIMIT_BREACH: 4,
            RiskAlert.LEVERAGE_WARNING: 2,
            RiskAlert.LEVERAGE_BREACH: 4,
            RiskAlert.LIQUIDATION_WARNING: 5,
            RiskAlert.CONCENTRATION_WARNING: 3,
            RiskAlert.VOLATILITY_SPIKE: 3,
            RiskAlert.CORRELATION_BREAKDOWN: 4,
            RiskAlert.FUNDING_RATE_SPIKE: 2,
            RiskAlert.SYSTEM_RISK_ELEVATED: 5
        }
        return severity_map.get(alert, 3)
    
    async def _position_monitor_loop(self):
        """Monitor all positions continuously"""
        while self._running:
            try:
                await asyncio.sleep(1)  # Check every second
                
                # Process positions in batches
                positions = list(self.monitored_positions.values())
                
                for position in positions:
                    # Get latest market data
                    market = await self._get_market_data(position.market_id)
                    if not market:
                        continue
                    
                    # Update risk profile
                    risk_profile = await self.oracle_engine.calculate_position_risk(
                        position, market
                    )
                    
                    position_key = f"{position.user_id}_{position.market_id}"
                    self.position_risk_profiles[position_key] = risk_profile
                    
                    # Check for alerts
                    if risk_profile.risk_score > 70:
                        await self.monitor_position(
                            position,
                            market,
                            Decimal("100000")  # Mock capital
                        )
                        
            except Exception as e:
                logger.error(f"Error in position monitor loop: {str(e)}")
    
    async def _system_risk_loop(self):
        """Calculate system-wide risk metrics"""
        while self._running:
            try:
                await asyncio.sleep(10)  # Update every 10 seconds
                
                # Aggregate metrics
                total_positions = len(self.monitored_positions)
                total_notional = Decimal("0")
                total_var = Decimal("0")
                total_leverage = Decimal("0")
                stressed_count = 0
                liquidation_count = 0
                
                for position_key, risk_profile in self.position_risk_profiles.items():
                    position = self.monitored_positions.get(position_key)
                    if not position:
                        continue
                    
                    notional = abs(position.size * position.entry_price)
                    total_notional += notional
                    total_var += risk_profile.var_95
                    total_leverage += position.leverage * notional
                    
                    if risk_profile.risk_score > 70:
                        stressed_count += 1
                    
                    if risk_profile.margin_ratio < Decimal("0.075"):
                        liquidation_count += 1
                
                # Update system metrics
                self.system_metrics = AggregatedRiskMetrics(
                    timestamp=datetime.now(),
                    total_positions=total_positions,
                    total_notional=total_notional,
                    total_var=total_var,
                    average_leverage=(total_leverage / total_notional 
                                    if total_notional > 0 else Decimal("0")),
                    concentration_metrics=self._calculate_concentration(),
                    stressed_positions=stressed_count,
                    liquidation_candidates=liquidation_count,
                    system_risk_score=self._calculate_system_risk_score()
                )
                
                # Store in Ignite
                await self.ignite_client.put(
                    "system_risk_metrics",
                    self.system_metrics
                )
                
                # Emit system risk update
                await self.pulsar_client.publish(
                    "system-risk-update",
                    {
                        "timestamp": self.system_metrics.timestamp,
                        "risk_score": self.system_metrics.system_risk_score,
                        "total_var": str(self.system_metrics.total_var),
                        "stressed_positions": self.system_metrics.stressed_positions
                    }
                )
                
            except Exception as e:
                logger.error(f"Error in system risk loop: {str(e)}")
    
    async def _circuit_breaker_loop(self):
        """Monitor and manage circuit breakers"""
        while self._running:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds
                
                current_time = datetime.now()
                
                for breaker_id, breaker in list(self.circuit_breakers.items()):
                    if breaker.triggered:
                        # Check if duration expired
                        if (breaker.trigger_time and 
                            current_time - breaker.trigger_time > breaker.duration):
                            
                            # Reset circuit breaker
                            breaker.triggered = False
                            
                            # Resume trading
                            await self.pulsar_client.publish(
                                "circuit-breaker-reset",
                                {
                                    "market_id": breaker.market_id,
                                    "breaker_id": breaker_id,
                                    "duration": str(breaker.duration)
                                }
                            )
                            
                            # Remove from active breakers
                            del self.circuit_breakers[breaker_id]
                            
                            logger.info(f"Circuit breaker reset: {breaker_id}")
                            
            except Exception as e:
                logger.error(f"Error in circuit breaker loop: {str(e)}")
    
    async def _alert_processor_loop(self):
        """Process risk alerts from Flink"""
        while self._running:
            try:
                # In production, would consume from Pulsar topic
                await asyncio.sleep(1)
                
                # Process active alerts
                for position_key, alerts in list(self.active_alerts.items()):
                    # Check if alerts resolved
                    position = self.monitored_positions.get(position_key)
                    if not position:
                        del self.active_alerts[position_key]
                        continue
                    
                    risk_profile = self.position_risk_profiles.get(position_key)
                    if risk_profile and risk_profile.risk_score < 50:
                        # Risk reduced, clear alerts
                        del self.active_alerts[position_key]
                        
                        await self.pulsar_client.publish(
                            "risk-alerts-cleared",
                            {
                                "position_key": position_key,
                                "cleared_alerts": [a.value for a in alerts]
                            }
                        )
                        
            except Exception as e:
                logger.error(f"Error in alert processor: {str(e)}")
    
    def _calculate_concentration(self) -> Dict[str, Decimal]:
        """Calculate concentration metrics"""
        market_exposure = defaultdict(Decimal)
        user_exposure = defaultdict(Decimal)
        
        total_notional = self.system_metrics.total_notional
        
        for position in self.monitored_positions.values():
            notional = abs(position.size * position.entry_price)
            market_exposure[position.market_id] += notional
            user_exposure[position.user_id] += notional
        
        concentration = {}
        
        # Top market concentration
        if market_exposure and total_notional > 0:
            top_market = max(market_exposure.values())
            concentration["top_market"] = top_market / total_notional
        
        # Top user concentration
        if user_exposure and total_notional > 0:
            top_user = max(user_exposure.values())
            concentration["top_user"] = top_user / total_notional
        
        return concentration
    
    def _calculate_system_risk_score(self) -> int:
        """Calculate overall system risk score (0-100)"""
        score = 0
        
        # VaR component (30%)
        if self.system_metrics.total_notional > 0:
            var_ratio = self.system_metrics.total_var / self.system_metrics.total_notional
            score += min(30, int(var_ratio * 300))
        
        # Leverage component (20%)
        avg_leverage = self.system_metrics.average_leverage
        if avg_leverage > Decimal("10"):
            score += min(20, int((avg_leverage - Decimal("10")) * 2))
        
        # Stressed positions (25%)
        if self.system_metrics.total_positions > 0:
            stressed_ratio = (self.system_metrics.stressed_positions / 
                            self.system_metrics.total_positions)
            score += int(stressed_ratio * 25)
        
        # Liquidation candidates (25%)
        if self.system_metrics.total_positions > 0:
            liquidation_ratio = (self.system_metrics.liquidation_candidates / 
                               self.system_metrics.total_positions)
            score += int(liquidation_ratio * 25)
        
        return min(100, score)
    
    async def _get_market_data(self, market_id: str) -> Optional[Market]:
        """Get latest market data"""
        # In production, would fetch from market data service
        # Return mock for now
        return Market(
            market_id=market_id,
            base_currency="ETH",
            quote_currency="USD",
            market_type="perpetual",
            contract_size=Decimal("1"),
            tick_size=Decimal("0.01"),
            min_order_size=Decimal("0.001"),
            max_order_size=Decimal("1000"),
            base_currency_volume=Decimal("10000")
        )
    
    async def get_risk_dashboard(self) -> Dict:
        """Get comprehensive risk dashboard data"""
        return {
            "system_metrics": self.system_metrics.__dict__,
            "active_alerts": {
                k: [a.value for a in v] 
                for k, v in self.active_alerts.items()
            },
            "circuit_breakers": {
                k: {
                    "market_id": v.market_id,
                    "triggered": v.triggered,
                    "trigger_time": v.trigger_time
                }
                for k, v in self.circuit_breakers.items()
            },
            "top_risk_positions": self._get_top_risk_positions(10),
            "risk_limits": {
                "default": self.default_risk_limit.__dict__
            }
        }
    
    def _get_top_risk_positions(self, limit: int) -> List[Dict]:
        """Get top risk positions"""
        positions_with_risk = []
        
        for position_key, risk_profile in self.position_risk_profiles.items():
            position = self.monitored_positions.get(position_key)
            if position:
                positions_with_risk.append({
                    "position_key": position_key,
                    "market_id": position.market_id,
                    "size": str(position.size),
                    "leverage": str(position.leverage),
                    "risk_score": risk_profile.risk_score,
                    "var_95": str(risk_profile.var_95),
                    "margin_ratio": str(risk_profile.margin_ratio)
                })
        
        # Sort by risk score
        positions_with_risk.sort(key=lambda x: x["risk_score"], reverse=True)
        
        return positions_with_risk[:limit] 