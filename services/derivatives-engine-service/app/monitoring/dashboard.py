"""
Monitoring Dashboard System

Provides real-time monitoring and analytics dashboards.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any, Set
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict, deque
import logging
import json

from app.integrations import IgniteCache, PulsarEventPublisher
from app.models.market import Market
from app.models.position import Position
from app.models.order import Order

logger = logging.getLogger(__name__)


@dataclass
class MetricSnapshot:
    """Point-in-time metric snapshot"""
    timestamp: datetime
    value: Decimal
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DashboardMetric:
    """Dashboard metric configuration"""
    metric_id: str
    name: str
    category: str  # volume, liquidity, risk, performance
    aggregation: str  # sum, avg, max, min, last
    time_window: timedelta
    
    # Data points
    snapshots: deque = field(default_factory=lambda: deque(maxlen=1000))
    current_value: Decimal = Decimal("0")
    
    # Alerts
    alert_thresholds: Dict[str, Decimal] = field(default_factory=dict)
    is_alerting: bool = False
    
    def add_snapshot(self, value: Decimal, metadata: Optional[Dict] = None):
        """Add a new snapshot"""
        snapshot = MetricSnapshot(
            timestamp=datetime.utcnow(),
            value=value,
            metadata=metadata or {}
        )
        self.snapshots.append(snapshot)
        self.current_value = value
        
    def get_time_series(self, lookback: Optional[timedelta] = None) -> List[Dict]:
        """Get time series data"""
        cutoff = datetime.utcnow() - (lookback or self.time_window)
        
        return [
            {
                "timestamp": s.timestamp.isoformat(),
                "value": str(s.value),
                "metadata": s.metadata
            }
            for s in self.snapshots
            if s.timestamp >= cutoff
        ]


class MonitoringDashboard:
    """
    Main monitoring dashboard system
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        
        # Metrics registry
        self.metrics: Dict[str, DashboardMetric] = {}
        
        # Alert state
        self.active_alerts: Set[str] = set()
        self.alert_history: deque = deque(maxlen=10000)
        
        # Real-time streams
        self.websocket_connections: Set[Any] = set()
        
        # Initialize core metrics
        self._initialize_core_metrics()
        
    def _initialize_core_metrics(self):
        """Initialize core platform metrics"""
        # Trading volume metrics
        self.add_metric(
            metric_id="total_volume_24h",
            name="24h Trading Volume",
            category="volume",
            aggregation="sum",
            time_window=timedelta(hours=24)
        )
        
        self.add_metric(
            metric_id="derivative_volume_24h",
            name="24h Derivatives Volume",
            category="volume",
            aggregation="sum",
            time_window=timedelta(hours=24)
        )
        
        # Liquidity metrics
        self.add_metric(
            metric_id="total_liquidity",
            name="Total Platform Liquidity",
            category="liquidity",
            aggregation="last",
            time_window=timedelta(hours=1)
        )
        
        self.add_metric(
            metric_id="bid_ask_spread",
            name="Average Bid-Ask Spread",
            category="liquidity",
            aggregation="avg",
            time_window=timedelta(minutes=5)
        )
        
        # Risk metrics
        self.add_metric(
            metric_id="platform_var_95",
            name="Platform VaR (95%)",
            category="risk",
            aggregation="last",
            time_window=timedelta(hours=1)
        )
        
        self.add_metric(
            metric_id="open_interest",
            name="Total Open Interest",
            category="risk",
            aggregation="last",
            time_window=timedelta(hours=1)
        )
        
        self.add_metric(
            metric_id="liquidations_24h",
            name="24h Liquidations",
            category="risk",
            aggregation="sum",
            time_window=timedelta(hours=24),
            alert_thresholds={"high": Decimal("1000000")}
        )
        
        # Performance metrics
        self.add_metric(
            metric_id="order_latency_p99",
            name="Order Latency (p99)",
            category="performance",
            aggregation="avg",
            time_window=timedelta(minutes=5),
            alert_thresholds={"high": Decimal("100")}  # 100ms
        )
        
        self.add_metric(
            metric_id="api_success_rate",
            name="API Success Rate",
            category="performance",
            aggregation="avg",
            time_window=timedelta(minutes=15),
            alert_thresholds={"low": Decimal("0.95")}  # 95%
        )
        
        # User metrics
        self.add_metric(
            metric_id="active_users",
            name="Active Users",
            category="users",
            aggregation="last",
            time_window=timedelta(hours=1)
        )
        
        self.add_metric(
            metric_id="new_users_24h",
            name="New Users (24h)",
            category="users",
            aggregation="sum",
            time_window=timedelta(hours=24)
        )
        
    async def start(self):
        """Start monitoring dashboard"""
        asyncio.create_task(self._metric_aggregation_loop())
        asyncio.create_task(self._alert_monitoring_loop())
        asyncio.create_task(self._publish_updates_loop())
        logger.info("Monitoring dashboard started")
        
    def add_metric(
        self,
        metric_id: str,
        name: str,
        category: str,
        aggregation: str,
        time_window: timedelta,
        alert_thresholds: Optional[Dict[str, Decimal]] = None
    ) -> DashboardMetric:
        """Add a new metric to monitor"""
        metric = DashboardMetric(
            metric_id=metric_id,
            name=name,
            category=category,
            aggregation=aggregation,
            time_window=time_window,
            alert_thresholds=alert_thresholds or {}
        )
        
        self.metrics[metric_id] = metric
        return metric
        
    async def update_metric(
        self,
        metric_id: str,
        value: Decimal,
        metadata: Optional[Dict] = None
    ):
        """Update a metric value"""
        if metric_id not in self.metrics:
            logger.warning(f"Unknown metric: {metric_id}")
            return
            
        metric = self.metrics[metric_id]
        metric.add_snapshot(value, metadata)
        
        # Check alerts
        await self._check_metric_alerts(metric)
        
        # Store in cache
        await self.ignite.set(
            f"metric:{metric_id}:current",
            {
                "value": str(value),
                "timestamp": datetime.utcnow().isoformat(),
                "metadata": metadata
            }
        )
        
    async def get_dashboard_data(
        self,
        categories: Optional[List[str]] = None,
        lookback: Optional[timedelta] = None
    ) -> Dict[str, Any]:
        """Get dashboard data for specified categories"""
        dashboard_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "metrics": {},
            "alerts": list(self.active_alerts),
            "summary": {}
        }
        
        # Filter metrics by category
        for metric_id, metric in self.metrics.items():
            if categories and metric.category not in categories:
                continue
                
            dashboard_data["metrics"][metric_id] = {
                "name": metric.name,
                "category": metric.category,
                "current_value": str(metric.current_value),
                "time_series": metric.get_time_series(lookback),
                "is_alerting": metric.is_alerting
            }
            
        # Calculate summary statistics
        dashboard_data["summary"] = await self._calculate_summary_stats()
        
        return dashboard_data
        
    async def get_market_overview(self) -> Dict[str, Any]:
        """Get comprehensive market overview"""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "trading": {
                "total_volume_24h": str(self.metrics["total_volume_24h"].current_value),
                "derivative_volume_24h": str(self.metrics["derivative_volume_24h"].current_value),
                "active_markets": await self._count_active_markets(),
                "top_markets": await self._get_top_markets()
            },
            "liquidity": {
                "total_liquidity": str(self.metrics["total_liquidity"].current_value),
                "average_spread": str(self.metrics["bid_ask_spread"].current_value),
                "depth_imbalance": await self._calculate_depth_imbalance()
            },
            "risk": {
                "platform_var": str(self.metrics["platform_var_95"].current_value),
                "open_interest": str(self.metrics["open_interest"].current_value),
                "liquidations_24h": str(self.metrics["liquidations_24h"].current_value),
                "risk_score": await self._calculate_risk_score()
            },
            "users": {
                "active_users": int(self.metrics["active_users"].current_value),
                "new_users_24h": int(self.metrics["new_users_24h"].current_value),
                "user_growth": await self._calculate_user_growth()
            }
        }
        
    async def get_performance_metrics(self) -> Dict[str, Any]:
        """Get system performance metrics"""
        return {
            "latency": {
                "order_latency_p99": f"{self.metrics['order_latency_p99'].current_value}ms",
                "order_latency_p95": await self._get_latency_percentile(95),
                "order_latency_p50": await self._get_latency_percentile(50)
            },
            "throughput": {
                "orders_per_second": await self._calculate_order_throughput(),
                "trades_per_second": await self._calculate_trade_throughput(),
                "messages_per_second": await self._calculate_message_throughput()
            },
            "availability": {
                "api_success_rate": f"{self.metrics['api_success_rate'].current_value:.2%}",
                "uptime_percentage": await self._calculate_uptime(),
                "error_rate": await self._calculate_error_rate()
            },
            "infrastructure": {
                "cpu_usage": await self._get_cpu_usage(),
                "memory_usage": await self._get_memory_usage(),
                "disk_usage": await self._get_disk_usage()
            }
        }
        
    async def get_alert_dashboard(self) -> Dict[str, Any]:
        """Get alert dashboard data"""
        # Recent alerts
        recent_alerts = []
        for alert in list(self.alert_history)[-50:]:  # Last 50 alerts
            recent_alerts.append(alert)
            
        # Alert statistics
        alert_stats = defaultdict(int)
        for alert in self.alert_history:
            alert_stats[alert["severity"]] += 1
            
        return {
            "active_alerts": [
                {
                    "metric_id": alert_id.split(":")[0],
                    "condition": alert_id.split(":")[1],
                    "triggered_at": datetime.utcnow().isoformat()
                }
                for alert_id in self.active_alerts
            ],
            "recent_alerts": recent_alerts,
            "statistics": {
                "total_alerts_24h": len([a for a in self.alert_history if datetime.fromisoformat(a["timestamp"]) > datetime.utcnow() - timedelta(hours=24)]),
                "by_severity": dict(alert_stats),
                "mttr": await self._calculate_mttr()  # Mean time to resolve
            }
        }
        
    async def add_websocket_connection(self, websocket):
        """Add WebSocket connection for real-time updates"""
        self.websocket_connections.add(websocket)
        
        # Send initial data
        await websocket.send_json({
            "type": "initial",
            "data": await self.get_dashboard_data()
        })
        
    async def remove_websocket_connection(self, websocket):
        """Remove WebSocket connection"""
        self.websocket_connections.discard(websocket)
        
    async def _metric_aggregation_loop(self):
        """Continuously aggregate metrics"""
        while True:
            try:
                # Update volume metrics
                await self._update_volume_metrics()
                
                # Update liquidity metrics
                await self._update_liquidity_metrics()
                
                # Update risk metrics
                await self._update_risk_metrics()
                
                # Update performance metrics
                await self._update_performance_metrics()
                
                # Update user metrics
                await self._update_user_metrics()
                
            except Exception as e:
                logger.error(f"Error in metric aggregation: {e}")
                
            await asyncio.sleep(10)  # Update every 10 seconds
            
    async def _alert_monitoring_loop(self):
        """Monitor metrics for alert conditions"""
        while True:
            try:
                for metric_id, metric in self.metrics.items():
                    await self._check_metric_alerts(metric)
                    
            except Exception as e:
                logger.error(f"Error in alert monitoring: {e}")
                
            await asyncio.sleep(5)  # Check every 5 seconds
            
    async def _publish_updates_loop(self):
        """Publish updates to WebSocket connections"""
        while True:
            try:
                if self.websocket_connections:
                    update_data = {
                        "type": "update",
                        "timestamp": datetime.utcnow().isoformat(),
                        "metrics": {}
                    }
                    
                    # Get updated metrics
                    for metric_id, metric in self.metrics.items():
                        update_data["metrics"][metric_id] = {
                            "current_value": str(metric.current_value),
                            "is_alerting": metric.is_alerting
                        }
                        
                    # Send to all connections
                    disconnected = set()
                    for websocket in self.websocket_connections:
                        try:
                            await websocket.send_json(update_data)
                        except:
                            disconnected.add(websocket)
                            
                    # Remove disconnected
                    self.websocket_connections -= disconnected
                    
            except Exception as e:
                logger.error(f"Error publishing updates: {e}")
                
            await asyncio.sleep(1)  # Update every second
            
    async def _check_metric_alerts(self, metric: DashboardMetric):
        """Check if metric triggers any alerts"""
        for condition, threshold in metric.alert_thresholds.items():
            alert_id = f"{metric.metric_id}:{condition}"
            
            triggered = False
            if condition == "high" and metric.current_value > threshold:
                triggered = True
            elif condition == "low" and metric.current_value < threshold:
                triggered = True
                
            if triggered and alert_id not in self.active_alerts:
                # New alert
                self.active_alerts.add(alert_id)
                metric.is_alerting = True
                
                alert_record = {
                    "metric_id": metric.metric_id,
                    "metric_name": metric.name,
                    "condition": condition,
                    "threshold": str(threshold),
                    "current_value": str(metric.current_value),
                    "severity": self._determine_severity(metric, condition),
                    "timestamp": datetime.utcnow().isoformat()
                }
                
                self.alert_history.append(alert_record)
                
                # Publish alert
                await self.pulsar.publish("monitoring-alert", alert_record)
                
            elif not triggered and alert_id in self.active_alerts:
                # Alert cleared
                self.active_alerts.remove(alert_id)
                metric.is_alerting = False
                
                await self.pulsar.publish(
                    "monitoring-alert-cleared",
                    {
                        "metric_id": metric.metric_id,
                        "condition": condition,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
    def _determine_severity(self, metric: DashboardMetric, condition: str) -> str:
        """Determine alert severity"""
        # Critical metrics
        if metric.metric_id in ["liquidations_24h", "platform_var_95"]:
            return "critical"
        
        # High severity for performance
        if metric.category == "performance":
            return "high"
            
        # Default medium
        return "medium"
        
    async def _update_volume_metrics(self):
        """Update trading volume metrics"""
        # Would query from trading engine
        total_volume = await self._calculate_total_volume(timedelta(hours=24))
        await self.update_metric("total_volume_24h", total_volume)
        
        derivative_volume = await self._calculate_derivative_volume(timedelta(hours=24))
        await self.update_metric("derivative_volume_24h", derivative_volume)
        
    async def _update_liquidity_metrics(self):
        """Update liquidity metrics"""
        total_liquidity = await self._calculate_total_liquidity()
        await self.update_metric("total_liquidity", total_liquidity)
        
        avg_spread = await self._calculate_average_spread()
        await self.update_metric("bid_ask_spread", avg_spread)
        
    async def _update_risk_metrics(self):
        """Update risk metrics"""
        platform_var = await self._calculate_platform_var()
        await self.update_metric("platform_var_95", platform_var)
        
        open_interest = await self._calculate_open_interest()
        await self.update_metric("open_interest", open_interest)
        
        liquidations = await self._calculate_liquidations(timedelta(hours=24))
        await self.update_metric("liquidations_24h", liquidations)
        
    async def _update_performance_metrics(self):
        """Update performance metrics"""
        latency_p99 = await self._get_latency_percentile(99)
        await self.update_metric("order_latency_p99", Decimal(str(latency_p99)))
        
        success_rate = await self._calculate_api_success_rate()
        await self.update_metric("api_success_rate", success_rate)
        
    async def _update_user_metrics(self):
        """Update user metrics"""
        active_users = await self._count_active_users()
        await self.update_metric("active_users", Decimal(str(active_users)))
        
        new_users = await self._count_new_users(timedelta(hours=24))
        await self.update_metric("new_users_24h", Decimal(str(new_users)))
        
    # Calculation methods (placeholders - would integrate with actual services)
    
    async def _calculate_total_volume(self, period: timedelta) -> Decimal:
        """Calculate total trading volume"""
        return Decimal("50000000")  # $50M placeholder
        
    async def _calculate_derivative_volume(self, period: timedelta) -> Decimal:
        """Calculate derivative trading volume"""
        return Decimal("30000000")  # $30M placeholder
        
    async def _calculate_total_liquidity(self) -> Decimal:
        """Calculate total platform liquidity"""
        return Decimal("100000000")  # $100M placeholder
        
    async def _calculate_average_spread(self) -> Decimal:
        """Calculate average bid-ask spread"""
        return Decimal("0.001")  # 0.1% placeholder
        
    async def _calculate_platform_var(self) -> Decimal:
        """Calculate platform Value at Risk"""
        return Decimal("5000000")  # $5M placeholder
        
    async def _calculate_open_interest(self) -> Decimal:
        """Calculate total open interest"""
        return Decimal("75000000")  # $75M placeholder
        
    async def _calculate_liquidations(self, period: timedelta) -> Decimal:
        """Calculate liquidation volume"""
        return Decimal("500000")  # $500k placeholder
        
    async def _get_latency_percentile(self, percentile: int) -> float:
        """Get latency percentile"""
        if percentile == 99:
            return 45.5  # 45.5ms
        elif percentile == 95:
            return 25.0
        else:
            return 10.0
            
    async def _calculate_api_success_rate(self) -> Decimal:
        """Calculate API success rate"""
        return Decimal("0.997")  # 99.7%
        
    async def _count_active_users(self) -> int:
        """Count active users"""
        return 5432  # Placeholder
        
    async def _count_new_users(self, period: timedelta) -> int:
        """Count new users in period"""
        return 127  # Placeholder
        
    async def _calculate_summary_stats(self) -> Dict[str, Any]:
        """Calculate summary statistics"""
        return {
            "health_score": await self._calculate_health_score(),
            "alerts_active": len(self.active_alerts),
            "top_performers": await self._get_top_performing_markets(),
            "risk_indicators": await self._get_risk_indicators()
        }
        
    async def _calculate_health_score(self) -> float:
        """Calculate overall platform health score (0-100)"""
        score = 100.0
        
        # Deduct for alerts
        score -= len(self.active_alerts) * 5
        
        # Deduct for high risk
        if self.metrics["platform_var_95"].current_value > Decimal("10000000"):
            score -= 10
            
        # Deduct for low liquidity
        if self.metrics["total_liquidity"].current_value < Decimal("50000000"):
            score -= 10
            
        return max(0, min(100, score))
        
    async def _count_active_markets(self) -> int:
        """Count active markets"""
        return 42  # Placeholder
        
    async def _get_top_markets(self) -> List[Dict]:
        """Get top markets by volume"""
        return [
            {"market": "BTC-USD-PERP", "volume": "15000000"},
            {"market": "ETH-USD-PERP", "volume": "10000000"},
            {"market": "SOL-USD-PERP", "volume": "5000000"}
        ]
        
    async def _calculate_depth_imbalance(self) -> float:
        """Calculate order book depth imbalance"""
        return 0.05  # 5% more on bid side
        
    async def _calculate_risk_score(self) -> int:
        """Calculate risk score (0-100, higher = riskier)"""
        return 35  # Placeholder
        
    async def _calculate_user_growth(self) -> float:
        """Calculate user growth rate"""
        return 0.023  # 2.3% daily growth
        
    async def _calculate_order_throughput(self) -> float:
        """Calculate orders per second"""
        return 1250.5
        
    async def _calculate_trade_throughput(self) -> float:
        """Calculate trades per second"""
        return 425.3
        
    async def _calculate_message_throughput(self) -> float:
        """Calculate messages per second"""
        return 15420.7
        
    async def _calculate_uptime(self) -> float:
        """Calculate uptime percentage"""
        return 0.9995  # 99.95%
        
    async def _calculate_error_rate(self) -> float:
        """Calculate error rate"""
        return 0.003  # 0.3%
        
    async def _get_cpu_usage(self) -> float:
        """Get CPU usage percentage"""
        return 0.42  # 42%
        
    async def _get_memory_usage(self) -> float:
        """Get memory usage percentage"""
        return 0.68  # 68%
        
    async def _get_disk_usage(self) -> float:
        """Get disk usage percentage"""
        return 0.35  # 35%
        
    async def _calculate_mttr(self) -> float:
        """Calculate mean time to resolve alerts"""
        return 12.5  # 12.5 minutes
        
    async def _get_top_performing_markets(self) -> List[str]:
        """Get top performing markets"""
        return ["BTC-USD-PERP", "ETH-USD-PERP", "COMP-FUTURES-DEC24"]
        
    async def _get_risk_indicators(self) -> Dict[str, str]:
        """Get key risk indicators"""
        return {
            "concentration_risk": "low",
            "liquidity_risk": "medium",
            "operational_risk": "low",
            "market_risk": "medium"
        } 