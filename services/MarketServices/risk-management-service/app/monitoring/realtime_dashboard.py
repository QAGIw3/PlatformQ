"""Real-time risk monitoring dashboard."""

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict, deque
import numpy as np

from platformq_risk_common import RiskMetric, RiskProfile

logger = logging.getLogger(__name__)


class MetricSnapshot:
    """Snapshot of a metric at a point in time."""
    
    def __init__(self, value: Decimal, timestamp: datetime):
        self.value = value
        self.timestamp = timestamp


class RealTimeDashboard:
    """Real-time risk monitoring dashboard."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # Time series data storage
        self.metric_history = defaultdict(lambda: defaultdict(lambda: deque(maxlen=1000)))
        
        # Current state
        self.current_metrics = defaultdict(dict)
        self.entity_metadata = {}
        
        # Aggregated metrics
        self.system_metrics = {
            "total_positions": 0,
            "total_exposure": Decimal("0"),
            "total_margin_used": Decimal("0"),
            "average_leverage": Decimal("0"),
            "system_var": Decimal("0"),
            "at_risk_positions": 0,
            "critical_alerts": 0
        }
        
        # Monitoring configuration
        self.update_interval = config.get("dashboard_update_interval", 5)  # seconds
        self.retention_hours = config.get("metric_retention_hours", 24)
        
        # Thresholds for visual indicators
        self.thresholds = {
            "margin_ratio": {"warning": 1.5, "critical": 1.2},
            "leverage": {"warning": 15, "critical": 18},
            "var_percentage": {"warning": 0.05, "critical": 0.08},
            "concentration": {"warning": 0.3, "critical": 0.5}
        }
        
        # WebSocket connections for real-time updates
        self.ws_connections = set()
        
    async def update_metrics(
        self,
        entity_id: str,
        entity_type: str,
        metrics: Dict[str, Any],
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Update metrics for an entity."""
        timestamp = datetime.utcnow()
        
        # Store current metrics
        self.current_metrics[entity_id] = {
            "entity_type": entity_type,
            "metrics": metrics,
            "timestamp": timestamp,
            "metadata": metadata or {}
        }
        
        # Update entity metadata
        if metadata:
            self.entity_metadata[entity_id] = metadata
        
        # Store historical data
        for metric_name, value in metrics.items():
            snapshot = MetricSnapshot(Decimal(str(value)), timestamp)
            self.metric_history[entity_id][metric_name].append(snapshot)
        
        # Update system-wide metrics
        await self._update_system_metrics()
        
        # Broadcast updates
        await self._broadcast_update({
            "type": "metric_update",
            "entity_id": entity_id,
            "entity_type": entity_type,
            "metrics": metrics,
            "timestamp": timestamp.isoformat()
        })
    
    async def _update_system_metrics(self):
        """Update system-wide aggregated metrics."""
        total_positions = 0
        total_exposure = Decimal("0")
        total_margin_used = Decimal("0")
        total_leverage_weighted = Decimal("0")
        at_risk_positions = 0
        
        for entity_id, data in self.current_metrics.items():
            if data["entity_type"] == "position":
                total_positions += 1
                metrics = data["metrics"]
                
                # Aggregate exposure
                if "notional_value" in metrics:
                    total_exposure += Decimal(str(metrics["notional_value"]))
                
                # Aggregate margin
                if "margin_used" in metrics:
                    total_margin_used += Decimal(str(metrics["margin_used"]))
                
                # Weighted leverage
                if "leverage" in metrics and "notional_value" in metrics:
                    leverage = Decimal(str(metrics["leverage"]))
                    notional = Decimal(str(metrics["notional_value"]))
                    total_leverage_weighted += leverage * notional
                
                # Count at-risk positions
                if self._is_position_at_risk(metrics):
                    at_risk_positions += 1
        
        # Calculate averages
        if total_positions > 0 and total_exposure > 0:
            average_leverage = total_leverage_weighted / total_exposure
        else:
            average_leverage = Decimal("0")
        
        # Update system metrics
        self.system_metrics.update({
            "total_positions": total_positions,
            "total_exposure": total_exposure,
            "total_margin_used": total_margin_used,
            "average_leverage": average_leverage,
            "at_risk_positions": at_risk_positions,
            "last_updated": datetime.utcnow()
        })
    
    def _is_position_at_risk(self, metrics: Dict[str, Any]) -> bool:
        """Check if a position is at risk based on metrics."""
        # Check margin ratio
        if "margin_ratio" in metrics:
            margin_ratio = Decimal(str(metrics["margin_ratio"]))
            if margin_ratio < self.thresholds["margin_ratio"]["warning"]:
                return True
        
        # Check leverage
        if "leverage" in metrics:
            leverage = Decimal(str(metrics["leverage"]))
            if leverage > self.thresholds["leverage"]["warning"]:
                return True
        
        # Check VaR
        if "var_percentage" in metrics:
            var_pct = Decimal(str(metrics["var_percentage"]))
            if var_pct > self.thresholds["var_percentage"]["warning"]:
                return True
        
        return False
    
    async def _broadcast_update(self, update: Dict[str, Any]):
        """Broadcast update to all connected WebSocket clients."""
        # In a real implementation, this would send to WebSocket connections
        logger.debug(f"Broadcasting update: {update['type']}")
    
    def get_current_snapshot(self) -> Dict[str, Any]:
        """Get current snapshot of all metrics."""
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "system_metrics": self.system_metrics,
            "entity_count": len(self.current_metrics),
            "entities": self._get_entity_summaries()
        }
    
    def _get_entity_summaries(self) -> List[Dict[str, Any]]:
        """Get summarized view of all entities."""
        summaries = []
        
        for entity_id, data in self.current_metrics.items():
            metrics = data["metrics"]
            
            # Determine health status
            health = self._calculate_health_status(metrics)
            
            summary = {
                "entity_id": entity_id,
                "entity_type": data["entity_type"],
                "health": health,
                "key_metrics": {
                    "margin_ratio": metrics.get("margin_ratio"),
                    "leverage": metrics.get("leverage"),
                    "var": metrics.get("var_percentage"),
                    "pnl": metrics.get("unrealized_pnl")
                },
                "last_updated": data["timestamp"].isoformat()
            }
            
            summaries.append(summary)
        
        # Sort by health status (critical first)
        health_order = {"critical": 0, "warning": 1, "normal": 2}
        summaries.sort(key=lambda x: health_order.get(x["health"], 3))
        
        return summaries
    
    def _calculate_health_status(self, metrics: Dict[str, Any]) -> str:
        """Calculate health status based on metrics."""
        # Check each metric against thresholds
        for metric_name, thresholds in self.thresholds.items():
            if metric_name in metrics:
                value = Decimal(str(metrics[metric_name]))
                
                # Handle metrics where higher is worse
                if metric_name in ["leverage", "var_percentage", "concentration"]:
                    if value >= thresholds["critical"]:
                        return "critical"
                    elif value >= thresholds["warning"]:
                        return "warning"
                
                # Handle metrics where lower is worse
                elif metric_name in ["margin_ratio"]:
                    if value <= thresholds["critical"]:
                        return "critical"
                    elif value <= thresholds["warning"]:
                        return "warning"
        
        return "normal"
    
    def get_metric_history(
        self,
        entity_id: str,
        metric_name: str,
        hours: int = 1
    ) -> List[Dict[str, Any]]:
        """Get historical data for a specific metric."""
        history = self.metric_history[entity_id][metric_name]
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        
        return [
            {
                "timestamp": snapshot.timestamp.isoformat(),
                "value": str(snapshot.value)
            }
            for snapshot in history
            if snapshot.timestamp > cutoff
        ]
    
    def get_top_risky_entities(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top risky entities based on multiple factors."""
        risk_scores = []
        
        for entity_id, data in self.current_metrics.items():
            metrics = data["metrics"]
            
            # Calculate composite risk score
            risk_score = self._calculate_risk_score(metrics)
            
            if risk_score > 0:
                risk_scores.append({
                    "entity_id": entity_id,
                    "entity_type": data["entity_type"],
                    "risk_score": risk_score,
                    "metrics": metrics,
                    "factors": self._get_risk_factors(metrics)
                })
        
        # Sort by risk score (highest first)
        risk_scores.sort(key=lambda x: x["risk_score"], reverse=True)
        
        return risk_scores[:limit]
    
    def _calculate_risk_score(self, metrics: Dict[str, Any]) -> float:
        """Calculate composite risk score (0-100)."""
        score = 0.0
        
        # Margin ratio component (0-30 points)
        if "margin_ratio" in metrics:
            margin_ratio = float(metrics["margin_ratio"])
            if margin_ratio < 1.2:
                score += 30
            elif margin_ratio < 1.5:
                score += 20 * (1.5 - margin_ratio) / 0.3
        
        # Leverage component (0-30 points)
        if "leverage" in metrics:
            leverage = float(metrics["leverage"])
            if leverage > 20:
                score += 30
            elif leverage > 15:
                score += 30 * (leverage - 15) / 5
        
        # VaR component (0-20 points)
        if "var_percentage" in metrics:
            var_pct = float(metrics["var_percentage"])
            if var_pct > 0.1:
                score += 20
            elif var_pct > 0.05:
                score += 20 * (var_pct - 0.05) / 0.05
        
        # P&L component (0-20 points)
        if "unrealized_pnl" in metrics and "notional_value" in metrics:
            pnl = float(metrics["unrealized_pnl"])
            notional = float(metrics["notional_value"])
            if notional > 0:
                loss_pct = -pnl / notional
                if loss_pct > 0.1:
                    score += 20
                elif loss_pct > 0.05:
                    score += 20 * (loss_pct - 0.05) / 0.05
        
        return min(score, 100)
    
    def _get_risk_factors(self, metrics: Dict[str, Any]) -> List[str]:
        """Identify risk factors for an entity."""
        factors = []
        
        if "margin_ratio" in metrics:
            margin_ratio = Decimal(str(metrics["margin_ratio"]))
            if margin_ratio < self.thresholds["margin_ratio"]["critical"]:
                factors.append("Critical margin level")
            elif margin_ratio < self.thresholds["margin_ratio"]["warning"]:
                factors.append("Low margin")
        
        if "leverage" in metrics:
            leverage = Decimal(str(metrics["leverage"]))
            if leverage > self.thresholds["leverage"]["critical"]:
                factors.append("Excessive leverage")
            elif leverage > self.thresholds["leverage"]["warning"]:
                factors.append("High leverage")
        
        if "var_percentage" in metrics:
            var_pct = Decimal(str(metrics["var_percentage"]))
            if var_pct > self.thresholds["var_percentage"]["critical"]:
                factors.append("High VaR")
        
        if "unrealized_pnl" in metrics:
            pnl = Decimal(str(metrics["unrealized_pnl"]))
            if pnl < 0 and "notional_value" in metrics:
                notional = Decimal(str(metrics["notional_value"]))
                if notional > 0:
                    loss_pct = -pnl / notional
                    if loss_pct > Decimal("0.1"):
                        factors.append("Large unrealized loss")
        
        return factors
    
    def get_metric_statistics(
        self,
        metric_name: str,
        entity_type: Optional[str] = None,
        hours: int = 24
    ) -> Dict[str, Any]:
        """Get statistics for a metric across all entities."""
        values = []
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        
        for entity_id, data in self.current_metrics.items():
            # Filter by entity type if specified
            if entity_type and data["entity_type"] != entity_type:
                continue
            
            # Get historical values
            history = self.metric_history[entity_id][metric_name]
            for snapshot in history:
                if snapshot.timestamp > cutoff:
                    values.append(float(snapshot.value))
        
        if not values:
            return {
                "metric": metric_name,
                "count": 0,
                "statistics": {}
            }
        
        values_array = np.array(values)
        
        return {
            "metric": metric_name,
            "count": len(values),
            "statistics": {
                "mean": float(np.mean(values_array)),
                "median": float(np.median(values_array)),
                "std": float(np.std(values_array)),
                "min": float(np.min(values_array)),
                "max": float(np.max(values_array)),
                "p25": float(np.percentile(values_array, 25)),
                "p75": float(np.percentile(values_array, 75)),
                "p95": float(np.percentile(values_array, 95))
            }
        }
    
    async def cleanup_old_data(self):
        """Clean up old historical data."""
        cutoff = datetime.utcnow() - timedelta(hours=self.retention_hours)
        
        for entity_id in list(self.metric_history.keys()):
            for metric_name in list(self.metric_history[entity_id].keys()):
                history = self.metric_history[entity_id][metric_name]
                
                # Remove old entries
                while history and history[0].timestamp < cutoff:
                    history.popleft()
                
                # Remove empty histories
                if not history:
                    del self.metric_history[entity_id][metric_name]
            
            # Remove entities with no history
            if not self.metric_history[entity_id]:
                del self.metric_history[entity_id]
                
        logger.info(f"Cleaned up historical data older than {self.retention_hours} hours") 