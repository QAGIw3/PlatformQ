"""
SeaTunnel Pipeline Monitoring Module

This module provides comprehensive monitoring for SeaTunnel pipelines
using Apache Ignite for real-time metrics storage and alerting.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json
import asyncio
from enum import Enum

from pyignite import Client as IgniteClient
from pyignite.datatypes import String, IntObject, LongObject, DoubleObject
import httpx
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry

logger = logging.getLogger(__name__)


class MetricType(str, Enum):
    """Types of metrics tracked"""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class AlertSeverity(str, Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class PipelineMetrics:
    """Container for pipeline metrics"""
    
    def __init__(self, job_id: str, pattern: str):
        self.job_id = job_id
        self.pattern = pattern
        self.start_time = datetime.utcnow()
        
        # Prometheus metrics
        registry = CollectorRegistry()
        
        # Counters
        self.rows_processed = Counter(
            'seatunnel_rows_processed_total',
            'Total number of rows processed',
            ['job_id', 'pattern'],
            registry=registry
        )
        
        self.errors_count = Counter(
            'seatunnel_errors_total',
            'Total number of errors',
            ['job_id', 'pattern', 'error_type'],
            registry=registry
        )
        
        # Gauges
        self.processing_lag = Gauge(
            'seatunnel_processing_lag_seconds',
            'Current processing lag in seconds',
            ['job_id', 'pattern'],
            registry=registry
        )
        
        self.throughput = Gauge(
            'seatunnel_throughput_rows_per_second',
            'Current throughput in rows per second',
            ['job_id', 'pattern'],
            registry=registry
        )
        
        # Histograms
        self.processing_time = Histogram(
            'seatunnel_processing_time_seconds',
            'Processing time distribution',
            ['job_id', 'pattern'],
            buckets=(0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0),
            registry=registry
        )
        
        self.batch_size = Histogram(
            'seatunnel_batch_size',
            'Batch size distribution',
            ['job_id', 'pattern'],
            buckets=(10, 50, 100, 500, 1000, 5000, 10000),
            registry=registry
        )
        
        # Pattern-specific metrics
        self._init_pattern_metrics(pattern, registry)
    
    def _init_pattern_metrics(self, pattern: str, registry):
        """Initialize pattern-specific metrics"""
        if pattern == "streaming":
            self.checkpoint_duration = Histogram(
                'seatunnel_checkpoint_duration_seconds',
                'Checkpoint duration',
                ['job_id'],
                registry=registry
            )
            
            self.backpressure = Gauge(
                'seatunnel_backpressure_ratio',
                'Backpressure ratio (0-1)',
                ['job_id'],
                registry=registry
            )
            
        elif pattern == "cdc":
            self.replication_lag = Gauge(
                'seatunnel_replication_lag_seconds',
                'CDC replication lag',
                ['job_id', 'table'],
                registry=registry
            )
            
            self.change_events = Counter(
                'seatunnel_change_events_total',
                'Total CDC change events',
                ['job_id', 'operation'],
                registry=registry
            )
            
        elif pattern == "aggregation":
            self.aggregation_time = Histogram(
                'seatunnel_aggregation_time_seconds',
                'Time spent in aggregation',
                ['job_id', 'aggregation_type'],
                registry=registry
            )
            
            self.groups_processed = Counter(
                'seatunnel_groups_processed_total',
                'Total aggregation groups processed',
                ['job_id'],
                registry=registry
            )


class IgniteMetricsStore:
    """Store and retrieve metrics using Apache Ignite"""
    
    def __init__(self, ignite_hosts: List[str] = None):
        self.ignite_hosts = ignite_hosts or ['ignite:10800']
        self.client = None
        self._connect()
    
    def _connect(self):
        """Connect to Ignite cluster"""
        try:
            self.client = IgniteClient()
            self.client.connect(self.ignite_hosts)
            logger.info("Connected to Ignite cluster")
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
    
    def store_metric(self, cache_name: str, key: str, value: Dict[str, Any]):
        """Store a metric in Ignite cache"""
        try:
            cache = self.client.get_or_create_cache(cache_name)
            
            # Add timestamp if not present
            if 'timestamp' not in value:
                value['timestamp'] = datetime.utcnow().isoformat()
            
            cache.put(key, json.dumps(value))
            
        except Exception as e:
            logger.error(f"Failed to store metric: {e}")
    
    def get_metrics(self, cache_name: str, key_prefix: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieve metrics from Ignite cache"""
        try:
            cache = self.client.get_cache(cache_name)
            
            if key_prefix:
                # Scan cache for keys with prefix
                query = cache.scan(lambda k, v: k.startswith(key_prefix))
                return [json.loads(v) for k, v in query]
            else:
                # Get all entries
                return [json.loads(v) for k, v in cache.scan()]
                
        except Exception as e:
            logger.error(f"Failed to retrieve metrics: {e}")
            return []
    
    def get_aggregated_metrics(
        self,
        cache_name: str,
        metric_name: str,
        aggregation: str = "avg",
        time_window: timedelta = timedelta(minutes=5)
    ) -> float:
        """Get aggregated metrics over time window"""
        try:
            cache = self.client.get_cache(cache_name)
            cutoff_time = datetime.utcnow() - time_window
            
            values = []
            for key, value_str in cache.scan():
                value = json.loads(value_str)
                
                if value.get('metric_name') == metric_name:
                    timestamp = datetime.fromisoformat(value['timestamp'])
                    if timestamp >= cutoff_time:
                        values.append(value.get('value', 0))
            
            if not values:
                return 0.0
            
            if aggregation == "sum":
                return sum(values)
            elif aggregation == "avg":
                return sum(values) / len(values)
            elif aggregation == "max":
                return max(values)
            elif aggregation == "min":
                return min(values)
            else:
                return values[-1]  # Latest value
                
        except Exception as e:
            logger.error(f"Failed to aggregate metrics: {e}")
            return 0.0
    
    def cleanup_old_metrics(self, cache_name: str, retention_days: int = 30):
        """Clean up metrics older than retention period"""
        try:
            cache = self.client.get_cache(cache_name)
            cutoff_time = datetime.utcnow() - timedelta(days=retention_days)
            
            keys_to_remove = []
            for key, value_str in cache.scan():
                value = json.loads(value_str)
                timestamp = datetime.fromisoformat(value.get('timestamp', datetime.utcnow().isoformat()))
                
                if timestamp < cutoff_time:
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                cache.remove(key)
            
            logger.info(f"Cleaned up {len(keys_to_remove)} old metrics from {cache_name}")
            
        except Exception as e:
            logger.error(f"Failed to cleanup metrics: {e}")
    
    def close(self):
        """Close Ignite connection"""
        if self.client:
            self.client.close()


class AlertManager:
    """Manage alerts for pipeline metrics"""
    
    def __init__(self, webhook_url: Optional[str] = None, email_config: Optional[Dict] = None):
        self.webhook_url = webhook_url
        self.email_config = email_config
        self.alert_history = {}
        self.alert_cooldown = timedelta(minutes=5)
    
    async def check_alert_rules(
        self,
        metrics_store: IgniteMetricsStore,
        cache_name: str,
        alert_rules: List[Dict[str, Any]]
    ):
        """Check alert rules against current metrics"""
        for rule in alert_rules:
            metric_name = rule['metric']
            threshold = rule['threshold']
            window = rule.get('window', '5m')
            severity = rule.get('severity', AlertSeverity.WARNING)
            
            # Parse window
            window_minutes = int(window.rstrip('m'))
            time_window = timedelta(minutes=window_minutes)
            
            # Get metric value
            current_value = metrics_store.get_aggregated_metrics(
                cache_name,
                metric_name,
                aggregation="avg",
                time_window=time_window
            )
            
            # Check threshold
            if self._should_alert(metric_name, current_value, threshold, rule.get('operator', '>')):
                await self._send_alert(
                    metric_name=metric_name,
                    current_value=current_value,
                    threshold=threshold,
                    severity=severity,
                    cache_name=cache_name
                )
    
    def _should_alert(self, metric_name: str, current_value: float, threshold: float, operator: str) -> bool:
        """Check if alert should be sent"""
        # Check cooldown
        last_alert = self.alert_history.get(metric_name)
        if last_alert and datetime.utcnow() - last_alert < self.alert_cooldown:
            return False
        
        # Check threshold
        if operator == '>':
            should_alert = current_value > threshold
        elif operator == '<':
            should_alert = current_value < threshold
        elif operator == '>=':
            should_alert = current_value >= threshold
        elif operator == '<=':
            should_alert = current_value <= threshold
        else:
            should_alert = False
        
        if should_alert:
            self.alert_history[metric_name] = datetime.utcnow()
        
        return should_alert
    
    async def _send_alert(
        self,
        metric_name: str,
        current_value: float,
        threshold: float,
        severity: AlertSeverity,
        cache_name: str
    ):
        """Send alert via configured channels"""
        alert_message = {
            "metric": metric_name,
            "current_value": current_value,
            "threshold": threshold,
            "severity": severity.value,
            "job_id": cache_name.replace("pipeline_metrics_", ""),
            "timestamp": datetime.utcnow().isoformat(),
            "message": f"Alert: {metric_name} is {current_value:.2f} (threshold: {threshold})"
        }
        
        # Send webhook
        if self.webhook_url:
            await self._send_webhook_alert(alert_message)
        
        # Send email
        if self.email_config:
            await self._send_email_alert(alert_message)
        
        logger.warning(f"Alert sent: {alert_message['message']}")
    
    async def _send_webhook_alert(self, alert: Dict[str, Any]):
        """Send alert via webhook"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    self.webhook_url,
                    json=alert,
                    timeout=10.0
                )
                response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {e}")
    
    async def _send_email_alert(self, alert: Dict[str, Any]):
        """Send alert via email"""
        # Implementation depends on email service
        # This is a placeholder
        logger.info(f"Email alert would be sent: {alert}")


class PipelineMonitor:
    """Main monitoring class for SeaTunnel pipelines"""
    
    def __init__(
        self,
        job_id: str,
        pattern: str,
        ignite_hosts: Optional[List[str]] = None,
        alert_config: Optional[Dict[str, Any]] = None
    ):
        self.job_id = job_id
        self.pattern = pattern
        self.cache_name = f"pipeline_metrics_{job_id}"
        
        # Initialize components
        self.metrics = PipelineMetrics(job_id, pattern)
        self.metrics_store = IgniteMetricsStore(ignite_hosts)
        self.alert_manager = AlertManager(
            webhook_url=alert_config.get('webhook_url') if alert_config else None,
            email_config=alert_config.get('email') if alert_config else None
        )
        
        self.alert_rules = alert_config.get('rules', []) if alert_config else []
        self._monitoring_task = None
    
    def record_metric(self, metric_name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Record a metric value"""
        metric_data = {
            "metric_name": metric_name,
            "value": value,
            "job_id": self.job_id,
            "pattern": self.pattern,
            "labels": labels or {}
        }
        
        # Store in Ignite
        key = f"{metric_name}_{datetime.utcnow().timestamp()}"
        self.metrics_store.store_metric(self.cache_name, key, metric_data)
        
        # Update Prometheus metrics
        self._update_prometheus_metric(metric_name, value, labels)
    
    def _update_prometheus_metric(self, metric_name: str, value: float, labels: Optional[Dict[str, str]]):
        """Update Prometheus metric"""
        try:
            if metric_name == "rows_processed":
                self.metrics.rows_processed.labels(job_id=self.job_id, pattern=self.pattern).inc(value)
            elif metric_name == "errors":
                error_type = labels.get('error_type', 'unknown') if labels else 'unknown'
                self.metrics.errors_count.labels(job_id=self.job_id, pattern=self.pattern, error_type=error_type).inc()
            elif metric_name == "processing_lag":
                self.metrics.processing_lag.labels(job_id=self.job_id, pattern=self.pattern).set(value)
            elif metric_name == "throughput":
                self.metrics.throughput.labels(job_id=self.job_id, pattern=self.pattern).set(value)
            elif metric_name == "processing_time":
                self.metrics.processing_time.labels(job_id=self.job_id, pattern=self.pattern).observe(value)
                
        except Exception as e:
            logger.error(f"Failed to update Prometheus metric {metric_name}: {e}")
    
    async def start_monitoring(self, interval: int = 60):
        """Start continuous monitoring"""
        self._monitoring_task = asyncio.create_task(self._monitor_loop(interval))
    
    async def _monitor_loop(self, interval: int):
        """Main monitoring loop"""
        while True:
            try:
                # Check alert rules
                await self.alert_manager.check_alert_rules(
                    self.metrics_store,
                    self.cache_name,
                    self.alert_rules
                )
                
                # Cleanup old metrics periodically
                if datetime.utcnow().hour == 0 and datetime.utcnow().minute < 1:
                    self.metrics_store.cleanup_old_metrics(self.cache_name)
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(interval)
    
    def stop_monitoring(self):
        """Stop monitoring"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        
        self.metrics_store.close()
    
    def get_dashboard_data(self, time_range: timedelta = timedelta(hours=1)) -> Dict[str, Any]:
        """Get data for monitoring dashboard"""
        dashboard_data = {
            "job_id": self.job_id,
            "pattern": self.pattern,
            "time_range": {
                "start": (datetime.utcnow() - time_range).isoformat(),
                "end": datetime.utcnow().isoformat()
            },
            "metrics": {}
        }
        
        # Aggregate key metrics
        key_metrics = ["rows_processed", "errors", "throughput", "processing_lag"]
        
        for metric in key_metrics:
            dashboard_data["metrics"][metric] = {
                "current": self.metrics_store.get_aggregated_metrics(
                    self.cache_name,
                    metric,
                    aggregation="avg",
                    time_window=timedelta(minutes=1)
                ),
                "hourly_avg": self.metrics_store.get_aggregated_metrics(
                    self.cache_name,
                    metric,
                    aggregation="avg",
                    time_window=timedelta(hours=1)
                ),
                "hourly_max": self.metrics_store.get_aggregated_metrics(
                    self.cache_name,
                    metric,
                    aggregation="max",
                    time_window=timedelta(hours=1)
                )
            }
        
        return dashboard_data 