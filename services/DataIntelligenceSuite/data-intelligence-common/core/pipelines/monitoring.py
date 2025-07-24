"""
Pipeline Monitoring

Provides comprehensive monitoring and observability for pipeline execution.
"""

import asyncio
from typing import Dict, List, Any, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque
import json
import statistics

from .base import PipelineStage, StageResult, PipelineResult, StageStatus
from ..events import EventBus, Event
from ...monitoring import StructuredLogger, MetricsCollector

logger = StructuredLogger.get_logger(__name__)


class MetricType(str, Enum):
    """Types of pipeline metrics"""
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    ERROR_RATE = "error_rate"
    RESOURCE_USAGE = "resource_usage"
    QUEUE_SIZE = "queue_size"
    STAGE_DURATION = "stage_duration"


@dataclass
class PipelineMetrics:
    """Metrics for a pipeline execution"""
    pipeline_id: str
    timestamp: datetime
    
    # Performance metrics
    total_duration_seconds: float = 0.0
    stage_durations: Dict[str, float] = field(default_factory=dict)
    throughput_records_per_second: float = 0.0
    
    # Resource metrics
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    
    # Quality metrics
    total_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    error_rate: float = 0.0
    
    # Stage metrics
    stage_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class AlertConfig:
    """Configuration for pipeline alerts"""
    enable_alerts: bool = True
    
    # Thresholds
    error_rate_threshold: float = 0.05  # 5%
    latency_threshold_seconds: float = 300  # 5 minutes
    memory_threshold_mb: float = 8192  # 8GB
    cpu_threshold_percent: float = 80
    
    # Alert destinations
    alert_channels: List[str] = field(default_factory=lambda: ["log", "metrics"])
    alert_cooldown_seconds: int = 300  # 5 minutes between alerts


@dataclass
class Alert:
    """Pipeline alert"""
    alert_id: str
    pipeline_id: str
    alert_type: str
    severity: str  # info, warning, error, critical
    message: str
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


class PipelineMonitor:
    """
    Monitors pipeline execution and provides observability.
    """
    
    def __init__(
        self,
        alert_config: Optional[AlertConfig] = None,
        event_bus: Optional[EventBus] = None,
        metrics: Optional[MetricsCollector] = None
    ):
        self.alert_config = alert_config or AlertConfig()
        self.event_bus = event_bus
        self.metrics = metrics or MetricsCollector()
        
        # Monitoring state
        self._active_pipelines: Dict[str, PipelineMetrics] = {}
        self._metrics_history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=1000)
        )
        self._alerts: List[Alert] = []
        self._last_alert_time: Dict[str, datetime] = {}
        
        # Real-time metrics
        self._stage_timings: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=100)
        )
        
    async def start_monitoring(self, pipeline_id: str):
        """Start monitoring a pipeline execution"""
        metrics = PipelineMetrics(
            pipeline_id=pipeline_id,
            timestamp=datetime.utcnow()
        )
        self._active_pipelines[pipeline_id] = metrics
        
        logger.info(f"Started monitoring pipeline: {pipeline_id}")
        
        # Publish start event
        if self.event_bus:
            await self.event_bus.publish(Event(
                type="pipeline.monitoring.started",
                source="pipeline_monitor",
                data={"pipeline_id": pipeline_id}
            ))
            
    async def stop_monitoring(self, pipeline_id: str):
        """Stop monitoring a pipeline execution"""
        if pipeline_id in self._active_pipelines:
            metrics = self._active_pipelines.pop(pipeline_id)
            
            # Store in history
            self._metrics_history[pipeline_id].append(metrics)
            
            # Generate final report
            report = self._generate_report(metrics)
            logger.info(f"Pipeline monitoring stopped: {pipeline_id}\n{report}")
            
            # Publish stop event
            if self.event_bus:
                await self.event_bus.publish(Event(
                    type="pipeline.monitoring.stopped",
                    source="pipeline_monitor",
                    data={
                        "pipeline_id": pipeline_id,
                        "metrics": self._metrics_to_dict(metrics)
                    }
                ))
                
    async def record_stage_start(self, pipeline_id: str, stage_id: str):
        """Record stage start"""
        if pipeline_id not in self._active_pipelines:
            return
            
        metrics = self._active_pipelines[pipeline_id]
        metrics.stage_metrics[stage_id] = {
            "started_at": datetime.utcnow(),
            "status": "running"
        }
        
        # Update metrics
        if self.metrics:
            self.metrics.increment(
                "pipeline_stage_started",
                labels={"pipeline": pipeline_id, "stage": stage_id}
            )
            
    async def record_stage_complete(
        self,
        pipeline_id: str,
        stage_id: str,
        result: StageResult
    ):
        """Record stage completion"""
        if pipeline_id not in self._active_pipelines:
            return
            
        metrics = self._active_pipelines[pipeline_id]
        stage_metric = metrics.stage_metrics.get(stage_id, {})
        
        # Calculate duration
        if "started_at" in stage_metric:
            duration = (datetime.utcnow() - stage_metric["started_at"]).total_seconds()
            metrics.stage_durations[stage_id] = duration
            self._stage_timings[stage_id].append(duration)
            
            # Update stage metrics
            stage_metric.update({
                "completed_at": datetime.utcnow(),
                "duration_seconds": duration,
                "status": result.status.value,
                "records_processed": getattr(result, "records_processed", 0),
                "error": result.error if result.status == StageStatus.FAILED else None
            })
            
            # Check for anomalies
            await self._check_stage_anomalies(pipeline_id, stage_id, duration)
            
        # Update pipeline metrics
        if hasattr(result, "records_processed"):
            metrics.total_records += result.records_processed
            if result.status == StageStatus.COMPLETED:
                metrics.successful_records += result.records_processed
            else:
                metrics.failed_records += result.records_processed
                
        # Update error rate
        if metrics.total_records > 0:
            metrics.error_rate = metrics.failed_records / metrics.total_records
            
        # Check alerts
        await self._check_alerts(pipeline_id, metrics)
        
        # Update external metrics
        if self.metrics:
            self.metrics.record_histogram(
                "pipeline_stage_duration",
                duration,
                labels={"pipeline": pipeline_id, "stage": stage_id}
            )
            
    async def record_pipeline_complete(
        self,
        pipeline_id: str,
        result: PipelineResult
    ):
        """Record pipeline completion"""
        if pipeline_id not in self._active_pipelines:
            return
            
        metrics = self._active_pipelines[pipeline_id]
        
        # Update final metrics
        metrics.total_duration_seconds = (
            datetime.utcnow() - metrics.timestamp
        ).total_seconds()
        
        if metrics.total_duration_seconds > 0:
            metrics.throughput_records_per_second = (
                metrics.total_records / metrics.total_duration_seconds
            )
            
        # Get resource usage
        import psutil
        process = psutil.Process()
        metrics.memory_usage_mb = process.memory_info().rss / 1024 / 1024
        metrics.cpu_usage_percent = process.cpu_percent(interval=0.1)
        
        # Update external metrics
        if self.metrics:
            self.metrics.record_histogram(
                "pipeline_duration",
                metrics.total_duration_seconds,
                labels={"pipeline": pipeline_id}
            )
            self.metrics.record_histogram(
                "pipeline_throughput",
                metrics.throughput_records_per_second,
                labels={"pipeline": pipeline_id}
            )
            
    async def _check_stage_anomalies(
        self,
        pipeline_id: str,
        stage_id: str,
        duration: float
    ):
        """Check for stage execution anomalies"""
        timings = list(self._stage_timings[stage_id])
        
        if len(timings) < 10:
            return  # Not enough data
            
        # Calculate statistics
        mean_duration = statistics.mean(timings[:-1])  # Exclude current
        std_duration = statistics.stdev(timings[:-1]) if len(timings) > 2 else 0
        
        # Check if current duration is anomalous (> 3 std devs)
        if std_duration > 0 and abs(duration - mean_duration) > 3 * std_duration:
            await self._create_alert(
                pipeline_id=pipeline_id,
                alert_type="stage_anomaly",
                severity="warning",
                message=f"Stage {stage_id} duration anomaly: {duration:.2f}s "
                       f"(expected: {mean_duration:.2f}s ± {std_duration:.2f}s)"
            )
            
    async def _check_alerts(self, pipeline_id: str, metrics: PipelineMetrics):
        """Check if any alerts should be triggered"""
        if not self.alert_config.enable_alerts:
            return
            
        # Check error rate
        if metrics.error_rate > self.alert_config.error_rate_threshold:
            await self._create_alert(
                pipeline_id=pipeline_id,
                alert_type="high_error_rate",
                severity="error",
                message=f"High error rate: {metrics.error_rate:.2%} "
                       f"(threshold: {self.alert_config.error_rate_threshold:.2%})"
            )
            
        # Check latency
        if metrics.total_duration_seconds > self.alert_config.latency_threshold_seconds:
            await self._create_alert(
                pipeline_id=pipeline_id,
                alert_type="high_latency",
                severity="warning",
                message=f"High pipeline latency: {metrics.total_duration_seconds:.2f}s "
                       f"(threshold: {self.alert_config.latency_threshold_seconds}s)"
            )
            
        # Check resource usage
        if metrics.memory_usage_mb > self.alert_config.memory_threshold_mb:
            await self._create_alert(
                pipeline_id=pipeline_id,
                alert_type="high_memory_usage",
                severity="warning",
                message=f"High memory usage: {metrics.memory_usage_mb:.2f}MB "
                       f"(threshold: {self.alert_config.memory_threshold_mb}MB)"
            )
            
        if metrics.cpu_usage_percent > self.alert_config.cpu_threshold_percent:
            await self._create_alert(
                pipeline_id=pipeline_id,
                alert_type="high_cpu_usage",
                severity="warning",
                message=f"High CPU usage: {metrics.cpu_usage_percent:.1f}% "
                       f"(threshold: {self.alert_config.cpu_threshold_percent}%)"
            )
            
    async def _create_alert(
        self,
        pipeline_id: str,
        alert_type: str,
        severity: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Create and send an alert"""
        # Check cooldown
        alert_key = f"{pipeline_id}:{alert_type}"
        last_alert = self._last_alert_time.get(alert_key)
        
        if last_alert:
            elapsed = (datetime.utcnow() - last_alert).total_seconds()
            if elapsed < self.alert_config.alert_cooldown_seconds:
                return  # Still in cooldown
                
        # Create alert
        alert = Alert(
            alert_id=f"{pipeline_id}_{alert_type}_{datetime.utcnow().timestamp()}",
            pipeline_id=pipeline_id,
            alert_type=alert_type,
            severity=severity,
            message=message,
            timestamp=datetime.utcnow(),
            metadata=metadata or {}
        )
        
        self._alerts.append(alert)
        self._last_alert_time[alert_key] = datetime.utcnow()
        
        # Send alert
        await self._send_alert(alert)
        
    async def _send_alert(self, alert: Alert):
        """Send alert through configured channels"""
        for channel in self.alert_config.alert_channels:
            if channel == "log":
                if alert.severity == "critical":
                    logger.critical(alert.message, **alert.metadata)
                elif alert.severity == "error":
                    logger.error(alert.message, **alert.metadata)
                elif alert.severity == "warning":
                    logger.warning(alert.message, **alert.metadata)
                else:
                    logger.info(alert.message, **alert.metadata)
                    
            elif channel == "metrics" and self.metrics:
                self.metrics.increment(
                    "pipeline_alerts_total",
                    labels={
                        "pipeline": alert.pipeline_id,
                        "type": alert.alert_type,
                        "severity": alert.severity
                    }
                )
                
            elif channel == "event" and self.event_bus:
                await self.event_bus.publish(Event(
                    type=f"pipeline.alert.{alert.severity}",
                    source="pipeline_monitor",
                    data={
                        "alert_id": alert.alert_id,
                        "pipeline_id": alert.pipeline_id,
                        "type": alert.alert_type,
                        "message": alert.message,
                        "metadata": alert.metadata
                    }
                ))
                
    def get_pipeline_metrics(self, pipeline_id: str) -> Optional[PipelineMetrics]:
        """Get current metrics for a pipeline"""
        return self._active_pipelines.get(pipeline_id)
        
    def get_pipeline_history(
        self,
        pipeline_id: str,
        limit: Optional[int] = None
    ) -> List[PipelineMetrics]:
        """Get historical metrics for a pipeline"""
        history = list(self._metrics_history.get(pipeline_id, []))
        if limit:
            history = history[-limit:]
        return history
        
    def get_alerts(
        self,
        pipeline_id: Optional[str] = None,
        severity: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Alert]:
        """Get alerts with optional filtering"""
        alerts = self._alerts
        
        if pipeline_id:
            alerts = [a for a in alerts if a.pipeline_id == pipeline_id]
            
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
            
        if limit:
            alerts = alerts[-limit:]
            
        return alerts
        
    def _generate_report(self, metrics: PipelineMetrics) -> str:
        """Generate a report from pipeline metrics"""
        report = f"\nPipeline Metrics Report\n"
        report += f"Pipeline ID: {metrics.pipeline_id}\n"
        report += f"Duration: {metrics.total_duration_seconds:.2f} seconds\n"
        report += f"Throughput: {metrics.throughput_records_per_second:.2f} records/sec\n"
        report += f"Total Records: {metrics.total_records}\n"
        report += f"Success Rate: {(1 - metrics.error_rate):.2%}\n"
        report += f"Memory Usage: {metrics.memory_usage_mb:.2f} MB\n"
        report += f"CPU Usage: {metrics.cpu_usage_percent:.1f}%\n"
        
        if metrics.stage_durations:
            report += "\nStage Durations:\n"
            for stage, duration in sorted(
                metrics.stage_durations.items(),
                key=lambda x: x[1],
                reverse=True
            ):
                report += f"  - {stage}: {duration:.2f}s\n"
                
        return report
        
    def _metrics_to_dict(self, metrics: PipelineMetrics) -> Dict[str, Any]:
        """Convert metrics to dictionary"""
        return {
            "pipeline_id": metrics.pipeline_id,
            "timestamp": metrics.timestamp.isoformat(),
            "total_duration_seconds": metrics.total_duration_seconds,
            "throughput_records_per_second": metrics.throughput_records_per_second,
            "total_records": metrics.total_records,
            "successful_records": metrics.successful_records,
            "failed_records": metrics.failed_records,
            "error_rate": metrics.error_rate,
            "memory_usage_mb": metrics.memory_usage_mb,
            "cpu_usage_percent": metrics.cpu_usage_percent,
            "stage_durations": metrics.stage_durations,
            "stage_metrics": metrics.stage_metrics
        }


class PipelineTracer:
    """
    Distributed tracing for pipeline execution.
    """
    
    def __init__(self):
        self._traces: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._active_spans: Dict[str, Dict[str, Any]] = {}
        
    def start_trace(self, trace_id: str, pipeline_id: str) -> str:
        """Start a new trace"""
        span = {
            "trace_id": trace_id,
            "span_id": f"{trace_id}_root",
            "parent_span_id": None,
            "pipeline_id": pipeline_id,
            "operation": "pipeline_execution",
            "start_time": datetime.utcnow(),
            "tags": {"pipeline_id": pipeline_id}
        }
        
        self._active_spans[span["span_id"]] = span
        self._traces[trace_id].append(span)
        
        return span["span_id"]
        
    def start_span(
        self,
        trace_id: str,
        parent_span_id: str,
        operation: str,
        tags: Optional[Dict[str, Any]] = None
    ) -> str:
        """Start a new span"""
        span_id = f"{trace_id}_{operation}_{datetime.utcnow().timestamp()}"
        
        span = {
            "trace_id": trace_id,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "operation": operation,
            "start_time": datetime.utcnow(),
            "tags": tags or {}
        }
        
        self._active_spans[span_id] = span
        self._traces[trace_id].append(span)
        
        return span_id
        
    def end_span(self, span_id: str, error: Optional[str] = None):
        """End a span"""
        if span_id in self._active_spans:
            span = self._active_spans.pop(span_id)
            span["end_time"] = datetime.utcnow()
            span["duration_ms"] = (
                span["end_time"] - span["start_time"]
            ).total_seconds() * 1000
            
            if error:
                span["error"] = error
                span["status"] = "error"
            else:
                span["status"] = "success"
                
    def get_trace(self, trace_id: str) -> List[Dict[str, Any]]:
        """Get all spans for a trace"""
        return self._traces.get(trace_id, []) 