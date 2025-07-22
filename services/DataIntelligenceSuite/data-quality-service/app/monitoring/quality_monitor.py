"""
Data Quality Monitoring

Provides real-time monitoring, alerting, and trending for data quality metrics.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Set
from enum import Enum
import asyncio
from dataclasses import dataclass
import json

from data_intelligence_common import StructuredLogger, MetricsCollector
from data_intelligence_common.vault_consul import VaultConsulIntegration
from platformq_shared.event_publisher import EventPublisher

logger = StructuredLogger.get_logger(__name__)


class QualityMetric(Enum):
    """Data quality metrics"""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    INTEGRITY = "integrity"
    CONFORMITY = "conformity"


class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class QualityAlert:
    """Data quality alert"""
    id: str
    dataset: str
    metric: QualityMetric
    severity: AlertSeverity
    current_value: float
    threshold: float
    message: str
    timestamp: datetime
    metadata: Dict[str, Any]


@dataclass
class QualityTrend:
    """Quality metric trend data"""
    dataset: str
    metric: QualityMetric
    timestamps: List[datetime]
    values: List[float]
    trend_direction: str  # improving, declining, stable
    change_rate: float


@dataclass
class MonitoringConfig:
    """Monitoring configuration"""
    check_interval_seconds: int = 300
    alert_cooldown_minutes: int = 30
    trend_window_hours: int = 24
    metric_retention_days: int = 30
    
    # Thresholds by metric
    thresholds: Dict[QualityMetric, Dict[str, float]] = None
    
    def __post_init__(self):
        if self.thresholds is None:
            self.thresholds = {
                QualityMetric.COMPLETENESS: {"warning": 0.95, "error": 0.90, "critical": 0.85},
                QualityMetric.ACCURACY: {"warning": 0.98, "error": 0.95, "critical": 0.90},
                QualityMetric.CONSISTENCY: {"warning": 0.95, "error": 0.90, "critical": 0.85},
                QualityMetric.TIMELINESS: {"warning": 3600, "error": 7200, "critical": 14400},  # seconds
                QualityMetric.VALIDITY: {"warning": 0.98, "error": 0.95, "critical": 0.90},
                QualityMetric.UNIQUENESS: {"warning": 0.99, "error": 0.95, "critical": 0.90},
                QualityMetric.INTEGRITY: {"warning": 0.99, "error": 0.95, "critical": 0.90},
                QualityMetric.CONFORMITY: {"warning": 0.95, "error": 0.90, "critical": 0.85}
            }


class DataQualityMonitor:
    """
    Monitors data quality metrics and generates alerts
    """
    
    def __init__(
        self,
        config: MonitoringConfig,
        vault_consul: VaultConsulIntegration,
        event_publisher: Optional[EventPublisher] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.vault_consul = vault_consul
        self.event_publisher = event_publisher
        self.metrics = metrics_collector
        
        # State
        self.active_alerts: Dict[str, QualityAlert] = {}
        self.alert_history: List[QualityAlert] = []
        self.metric_history: Dict[str, List[Dict[str, Any]]] = {}
        self.monitoring_tasks: Dict[str, asyncio.Task] = {}
        self.is_running = False
        
        # Define metrics
        if self.metrics:
            self._define_metrics()
    
    def _define_metrics(self):
        """Define Prometheus metrics"""
        # Quality scores by dataset and metric
        self.metrics.define_metric(
            "data_quality_score",
            "gauge",
            "Data quality score",
            ["dataset", "metric"]
        )
        
        # Active alerts
        self.metrics.define_metric(
            "data_quality_alerts_active",
            "gauge",
            "Number of active data quality alerts",
            ["severity"]
        )
        
        # Alert occurrences
        self.metrics.define_metric(
            "data_quality_alerts_total",
            "counter",
            "Total data quality alerts generated",
            ["dataset", "metric", "severity"]
        )
        
        # Monitoring checks
        self.metrics.define_metric(
            "data_quality_checks_total",
            "counter",
            "Total data quality checks performed",
            ["dataset", "status"]
        )
    
    async def start(self):
        """Start monitoring"""
        logger.info("starting_quality_monitor")
        self.is_running = True
        
        # Start background monitoring task
        self.monitoring_tasks["main"] = asyncio.create_task(
            self._monitoring_loop()
        )
        
        # Start alert cleanup task
        self.monitoring_tasks["cleanup"] = asyncio.create_task(
            self._cleanup_loop()
        )
        
        logger.info("quality_monitor_started")
    
    async def stop(self):
        """Stop monitoring"""
        logger.info("stopping_quality_monitor")
        self.is_running = False
        
        # Cancel all tasks
        for task in self.monitoring_tasks.values():
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(
            *self.monitoring_tasks.values(),
            return_exceptions=True
        )
        
        self.monitoring_tasks.clear()
        logger.info("quality_monitor_stopped")
    
    async def _monitoring_loop(self):
        """Main monitoring loop"""
        while self.is_running:
            try:
                # Get monitored datasets from config
                datasets = await self._get_monitored_datasets()
                
                for dataset in datasets:
                    await self._check_dataset_quality(dataset)
                
                # Update active alert metrics
                self._update_alert_metrics()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("monitoring_error", error=str(e))
            
            # Wait for next check
            await asyncio.sleep(self.config.check_interval_seconds)
    
    async def _cleanup_loop(self):
        """Cleanup old data"""
        while self.is_running:
            try:
                await self._cleanup_old_data()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("cleanup_error", error=str(e))
            
            # Run cleanup daily
            await asyncio.sleep(86400)  # 24 hours
    
    async def _get_monitored_datasets(self) -> List[str]:
        """Get list of datasets to monitor from Consul"""
        try:
            config_key = f"data-quality/monitored-datasets"
            datasets = await self.vault_consul.get_config(config_key, [])
            return datasets
        except Exception as e:
            logger.error("get_datasets_error", error=str(e))
            return []
    
    async def _check_dataset_quality(self, dataset: str):
        """Check quality metrics for a dataset"""
        logger.debug("checking_dataset_quality", dataset=dataset)
        
        try:
            # Get current quality metrics (would come from quality engine)
            metrics = await self._get_quality_metrics(dataset)
            
            # Record metrics
            await self._record_metrics(dataset, metrics)
            
            # Check thresholds and generate alerts
            await self._check_thresholds(dataset, metrics)
            
            # Update Prometheus metrics
            for metric_name, value in metrics.items():
                if self.metrics and metric_name in QualityMetric.__members__:
                    self.metrics.update_metric(
                        "data_quality_score",
                        value,
                        {"dataset": dataset, "metric": metric_name.lower()}
                    )
            
            # Increment check counter
            if self.metrics:
                self.metrics.update_metric(
                    "data_quality_checks_total",
                    1,
                    {"dataset": dataset, "status": "success"}
                )
            
        except Exception as e:
            logger.error("check_quality_error", dataset=dataset, error=str(e))
            if self.metrics:
                self.metrics.update_metric(
                    "data_quality_checks_total",
                    1,
                    {"dataset": dataset, "status": "error"}
                )
    
    async def _get_quality_metrics(self, dataset: str) -> Dict[str, float]:
        """Get current quality metrics for dataset"""
        # This would integrate with the quality engine
        # For now, return placeholder data
        return {
            QualityMetric.COMPLETENESS.value: 0.96,
            QualityMetric.ACCURACY.value: 0.99,
            QualityMetric.CONSISTENCY.value: 0.97,
            QualityMetric.TIMELINESS.value: 1800,  # seconds since last update
            QualityMetric.VALIDITY.value: 0.98,
            QualityMetric.UNIQUENESS.value: 0.999,
            QualityMetric.INTEGRITY.value: 0.995,
            QualityMetric.CONFORMITY.value: 0.96
        }
    
    async def _record_metrics(self, dataset: str, metrics: Dict[str, float]):
        """Record metrics in history"""
        if dataset not in self.metric_history:
            self.metric_history[dataset] = []
        
        self.metric_history[dataset].append({
            "timestamp": datetime.utcnow(),
            "metrics": metrics
        })
    
    async def _check_thresholds(self, dataset: str, metrics: Dict[str, float]):
        """Check metrics against thresholds and generate alerts"""
        for metric_name, value in metrics.items():
            try:
                metric = QualityMetric(metric_name)
                thresholds = self.config.thresholds.get(metric, {})
                
                # Check each severity level
                for severity_name in ["critical", "error", "warning"]:
                    threshold = thresholds.get(severity_name)
                    if threshold is None:
                        continue
                    
                    # For timeliness, higher values are worse
                    if metric == QualityMetric.TIMELINESS:
                        violates_threshold = value > threshold
                    else:
                        violates_threshold = value < threshold
                    
                    if violates_threshold:
                        severity = AlertSeverity(severity_name)
                        await self._generate_alert(
                            dataset, metric, severity, value, threshold
                        )
                        break  # Only generate highest severity alert
                
            except ValueError:
                logger.warning("unknown_metric", metric=metric_name)
    
    async def _generate_alert(
        self,
        dataset: str,
        metric: QualityMetric,
        severity: AlertSeverity,
        current_value: float,
        threshold: float
    ):
        """Generate a quality alert"""
        alert_id = f"{dataset}_{metric.value}_{severity.value}"
        
        # Check if alert already active
        if alert_id in self.active_alerts:
            existing = self.active_alerts[alert_id]
            if (datetime.utcnow() - existing.timestamp).total_seconds() < \
               self.config.alert_cooldown_minutes * 60:
                return  # Still in cooldown
        
        # Create alert
        alert = QualityAlert(
            id=alert_id,
            dataset=dataset,
            metric=metric,
            severity=severity,
            current_value=current_value,
            threshold=threshold,
            message=f"Data quality {metric.value} for {dataset} is {current_value:.2%} (threshold: {threshold:.2%})",
            timestamp=datetime.utcnow(),
            metadata={
                "trend": await self._get_metric_trend(dataset, metric)
            }
        )
        
        # Store alert
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        
        # Update metrics
        if self.metrics:
            self.metrics.update_metric(
                "data_quality_alerts_total",
                1,
                {
                    "dataset": dataset,
                    "metric": metric.value,
                    "severity": severity.value
                }
            )
        
        # Publish alert event
        if self.event_publisher:
            await self.event_publisher.publish(
                "data.quality.alert",
                {
                    "alert_id": alert.id,
                    "dataset": alert.dataset,
                    "metric": alert.metric.value,
                    "severity": alert.severity.value,
                    "current_value": alert.current_value,
                    "threshold": alert.threshold,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat(),
                    "metadata": alert.metadata
                }
            )
        
        logger.warning(
            "quality_alert_generated",
            alert_id=alert_id,
            dataset=dataset,
            metric=metric.value,
            severity=severity.value,
            current_value=current_value,
            threshold=threshold
        )
    
    async def _get_metric_trend(self, dataset: str, metric: QualityMetric) -> str:
        """Analyze metric trend"""
        history = self.metric_history.get(dataset, [])
        if len(history) < 2:
            return "unknown"
        
        # Get values for this metric over trend window
        cutoff = datetime.utcnow() - timedelta(hours=self.config.trend_window_hours)
        recent_values = []
        
        for entry in history:
            if entry["timestamp"] >= cutoff:
                value = entry["metrics"].get(metric.value)
                if value is not None:
                    recent_values.append(value)
        
        if len(recent_values) < 2:
            return "stable"
        
        # Calculate trend
        first_half_avg = sum(recent_values[:len(recent_values)//2]) / (len(recent_values)//2)
        second_half_avg = sum(recent_values[len(recent_values)//2:]) / (len(recent_values) - len(recent_values)//2)
        
        change_rate = (second_half_avg - first_half_avg) / first_half_avg
        
        if abs(change_rate) < 0.01:
            return "stable"
        elif change_rate > 0:
            return "improving" if metric != QualityMetric.TIMELINESS else "declining"
        else:
            return "declining" if metric != QualityMetric.TIMELINESS else "improving"
    
    def _update_alert_metrics(self):
        """Update Prometheus metrics for active alerts"""
        if not self.metrics:
            return
        
        # Count alerts by severity
        severity_counts = {s.value: 0 for s in AlertSeverity}
        for alert in self.active_alerts.values():
            severity_counts[alert.severity.value] += 1
        
        # Update metrics
        for severity, count in severity_counts.items():
            self.metrics.update_metric(
                "data_quality_alerts_active",
                count,
                {"severity": severity}
            )
    
    async def _cleanup_old_data(self):
        """Clean up old metric history and alerts"""
        cutoff = datetime.utcnow() - timedelta(days=self.config.metric_retention_days)
        
        # Clean metric history
        for dataset in list(self.metric_history.keys()):
            self.metric_history[dataset] = [
                entry for entry in self.metric_history[dataset]
                if entry["timestamp"] >= cutoff
            ]
            if not self.metric_history[dataset]:
                del self.metric_history[dataset]
        
        # Clean alert history
        self.alert_history = [
            alert for alert in self.alert_history
            if alert.timestamp >= cutoff
        ]
        
        # Clean inactive alerts
        for alert_id in list(self.active_alerts.keys()):
            alert = self.active_alerts[alert_id]
            if (datetime.utcnow() - alert.timestamp).days > 1:
                del self.active_alerts[alert_id]
        
        logger.info(
            "cleanup_completed",
            datasets_remaining=len(self.metric_history),
            alerts_remaining=len(self.alert_history)
        )
    
    async def get_dataset_metrics(self, dataset: str) -> Dict[str, Any]:
        """Get current metrics for a dataset"""
        current = await self._get_quality_metrics(dataset)
        history = self.metric_history.get(dataset, [])
        
        return {
            "dataset": dataset,
            "current": current,
            "history": history[-100:],  # Last 100 entries
            "alerts": [
                alert for alert in self.active_alerts.values()
                if alert.dataset == dataset
            ]
        }
    
    async def get_quality_trends(
        self, 
        dataset: Optional[str] = None,
        hours: int = 24
    ) -> List[QualityTrend]:
        """Get quality trends"""
        trends = []
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        
        datasets = [dataset] if dataset else list(self.metric_history.keys())
        
        for ds in datasets:
            history = self.metric_history.get(ds, [])
            
            # Group by metric
            metric_data: Dict[str, List[tuple[datetime, float]]] = {}
            
            for entry in history:
                if entry["timestamp"] >= cutoff:
                    for metric_name, value in entry["metrics"].items():
                        if metric_name not in metric_data:
                            metric_data[metric_name] = []
                        metric_data[metric_name].append(
                            (entry["timestamp"], value)
                        )
            
            # Create trends
            for metric_name, values in metric_data.items():
                if len(values) < 2:
                    continue
                
                timestamps = [v[0] for v in values]
                metric_values = [v[1] for v in values]
                
                # Calculate trend
                first_half_avg = sum(metric_values[:len(metric_values)//2]) / (len(metric_values)//2)
                second_half_avg = sum(metric_values[len(metric_values)//2:]) / (len(metric_values) - len(metric_values)//2)
                change_rate = (second_half_avg - first_half_avg) / first_half_avg if first_half_avg > 0 else 0
                
                # Determine direction
                if abs(change_rate) < 0.01:
                    direction = "stable"
                elif change_rate > 0:
                    direction = "improving" if metric_name != QualityMetric.TIMELINESS.value else "declining"
                else:
                    direction = "declining" if metric_name != QualityMetric.TIMELINESS.value else "improving"
                
                trends.append(QualityTrend(
                    dataset=ds,
                    metric=QualityMetric(metric_name),
                    timestamps=timestamps,
                    values=metric_values,
                    trend_direction=direction,
                    change_rate=change_rate
                ))
        
        return trends 