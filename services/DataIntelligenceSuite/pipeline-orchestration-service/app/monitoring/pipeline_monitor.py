"""
Pipeline Monitor

Monitors pipeline execution, performance, and health.
"""

from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timedelta
import asyncio
from collections import defaultdict
from dataclasses import dataclass
import json

from data_intelligence_common import StructuredLogger, MetricsCollector
from data_intelligence_common.vault_consul import VaultConsulIntegration
from platformq_shared.event_publisher import EventPublisher

logger = StructuredLogger.get_logger(__name__)


@dataclass
class PipelineMetrics:
    """Pipeline execution metrics"""
    pipeline_id: str
    total_executions: int = 0
    successful_executions: int = 0
    failed_executions: int = 0
    cancelled_executions: int = 0
    average_duration_seconds: float = 0.0
    min_duration_seconds: float = float('inf')
    max_duration_seconds: float = 0.0
    last_execution: Optional[datetime] = None
    last_success: Optional[datetime] = None
    last_failure: Optional[datetime] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        if self.total_executions == 0:
            return 0.0
        return self.successful_executions / self.total_executions
    
    @property
    def failure_rate(self) -> float:
        """Calculate failure rate"""
        if self.total_executions == 0:
            return 0.0
        return self.failed_executions / self.total_executions


@dataclass
class ExecutionMetrics:
    """Individual execution metrics"""
    execution_id: str
    pipeline_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    status: Optional[str] = None
    steps_completed: int = 0
    steps_failed: int = 0
    duration_seconds: Optional[float] = None
    error_count: int = 0


class PipelineMonitor:
    """
    Monitors pipeline health and performance
    """
    
    def __init__(
        self,
        vault_consul: VaultConsulIntegration,
        event_publisher: Optional[EventPublisher] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.vault_consul = vault_consul
        self.event_publisher = event_publisher
        self.metrics = metrics_collector
        
        # Monitoring data
        self.pipeline_metrics: Dict[str, PipelineMetrics] = {}
        self.execution_metrics: Dict[str, ExecutionMetrics] = {}
        self.alerts: List[Dict[str, Any]] = []
        
        # Monitoring configuration
        self.alert_thresholds = {
            "failure_rate": 0.2,  # Alert if > 20% failures
            "duration_increase": 2.0,  # Alert if duration > 2x average
            "consecutive_failures": 3  # Alert after 3 consecutive failures
        }
        
        # Background tasks
        self.monitoring_task: Optional[asyncio.Task] = None
        self.is_running = False
        
        # Performance tracking
        self.performance_history: Dict[str, List[float]] = defaultdict(list)
        self.max_history_size = 100
    
    async def start(self):
        """Start monitoring"""
        logger.info("starting_pipeline_monitor")
        
        self.is_running = True
        
        # Load historical metrics
        await self._load_metrics()
        
        # Start monitoring loop
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        
        logger.info("pipeline_monitor_started")
    
    async def stop(self):
        """Stop monitoring"""
        logger.info("stopping_pipeline_monitor")
        
        self.is_running = False
        
        # Cancel monitoring task
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        # Save metrics
        await self._save_metrics()
        
        logger.info("pipeline_monitor_stopped")
    
    async def record_execution_start(
        self,
        execution_id: str,
        pipeline_id: str
    ):
        """Record execution start"""
        self.execution_metrics[execution_id] = ExecutionMetrics(
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            started_at=datetime.utcnow()
        )
        
        # Update pipeline metrics
        if pipeline_id not in self.pipeline_metrics:
            self.pipeline_metrics[pipeline_id] = PipelineMetrics(pipeline_id=pipeline_id)
        
        self.pipeline_metrics[pipeline_id].total_executions += 1
        self.pipeline_metrics[pipeline_id].last_execution = datetime.utcnow()
        
        # Update Prometheus metrics if available
        if self.metrics:
            self.metrics.update_metric(
                "pipeline_executions_started_total",
                1,
                {"pipeline_id": pipeline_id}
            )
    
    async def record_execution_completion(
        self,
        execution_id: str,
        status: str,
        error_count: int = 0
    ):
        """Record execution completion"""
        if execution_id not in self.execution_metrics:
            logger.warning("unknown_execution", execution_id=execution_id)
            return
        
        execution = self.execution_metrics[execution_id]
        execution.completed_at = datetime.utcnow()
        execution.status = status
        execution.error_count = error_count
        execution.duration_seconds = (
            execution.completed_at - execution.started_at
        ).total_seconds()
        
        # Update pipeline metrics
        pipeline_metrics = self.pipeline_metrics.get(execution.pipeline_id)
        if pipeline_metrics:
            if status == "completed":
                pipeline_metrics.successful_executions += 1
                pipeline_metrics.last_success = datetime.utcnow()
            elif status == "failed":
                pipeline_metrics.failed_executions += 1
                pipeline_metrics.last_failure = datetime.utcnow()
                
                # Check for consecutive failures
                await self._check_consecutive_failures(execution.pipeline_id)
            elif status == "cancelled":
                pipeline_metrics.cancelled_executions += 1
            
            # Update duration metrics
            self._update_duration_metrics(pipeline_metrics, execution.duration_seconds)
            
            # Track performance history
            self.performance_history[execution.pipeline_id].append(
                execution.duration_seconds
            )
            if len(self.performance_history[execution.pipeline_id]) > self.max_history_size:
                self.performance_history[execution.pipeline_id].pop(0)
        
        # Update Prometheus metrics
        if self.metrics:
            self.metrics.update_metric(
                "pipeline_executions_completed_total",
                1,
                {"pipeline_id": execution.pipeline_id, "status": status}
            )
            
            self.metrics.update_metric(
                "pipeline_execution_duration_seconds",
                execution.duration_seconds,
                {"pipeline_id": execution.pipeline_id}
            )
        
        # Check for alerts
        await self._check_alerts(execution.pipeline_id)
    
    async def record_step_completion(
        self,
        execution_id: str,
        step_name: str,
        status: str,
        duration_seconds: float
    ):
        """Record step completion"""
        if execution_id not in self.execution_metrics:
            return
        
        execution = self.execution_metrics[execution_id]
        
        if status == "completed":
            execution.steps_completed += 1
        elif status == "failed":
            execution.steps_failed += 1
        
        # Update Prometheus metrics
        if self.metrics:
            self.metrics.update_metric(
                "pipeline_steps_completed_total",
                1,
                {
                    "pipeline_id": execution.pipeline_id,
                    "step_name": step_name,
                    "status": status
                }
            )
            
            self.metrics.update_metric(
                "pipeline_step_duration_seconds",
                duration_seconds,
                {
                    "pipeline_id": execution.pipeline_id,
                    "step_name": step_name
                }
            )
    
    def _update_duration_metrics(
        self,
        pipeline_metrics: PipelineMetrics,
        duration: float
    ):
        """Update duration metrics"""
        # Update min/max
        pipeline_metrics.min_duration_seconds = min(
            pipeline_metrics.min_duration_seconds,
            duration
        )
        pipeline_metrics.max_duration_seconds = max(
            pipeline_metrics.max_duration_seconds,
            duration
        )
        
        # Update average
        total_completed = (
            pipeline_metrics.successful_executions +
            pipeline_metrics.failed_executions
        )
        if total_completed > 0:
            current_avg = pipeline_metrics.average_duration_seconds
            pipeline_metrics.average_duration_seconds = (
                (current_avg * (total_completed - 1) + duration) / total_completed
            )
    
    async def _check_alerts(self, pipeline_id: str):
        """Check for alert conditions"""
        pipeline_metrics = self.pipeline_metrics.get(pipeline_id)
        if not pipeline_metrics:
            return
        
        # Check failure rate
        if pipeline_metrics.failure_rate > self.alert_thresholds["failure_rate"]:
            await self._create_alert(
                pipeline_id,
                "high_failure_rate",
                f"Pipeline failure rate is {pipeline_metrics.failure_rate:.1%}",
                {
                    "failure_rate": pipeline_metrics.failure_rate,
                    "threshold": self.alert_thresholds["failure_rate"]
                }
            )
        
        # Check duration increase
        if (pipeline_metrics.average_duration_seconds > 0 and
            len(self.performance_history[pipeline_id]) > 0):
            
            recent_duration = self.performance_history[pipeline_id][-1]
            if recent_duration > (
                pipeline_metrics.average_duration_seconds * 
                self.alert_thresholds["duration_increase"]
            ):
                await self._create_alert(
                    pipeline_id,
                    "performance_degradation",
                    f"Recent execution took {recent_duration:.1f}s vs average {pipeline_metrics.average_duration_seconds:.1f}s",
                    {
                        "recent_duration": recent_duration,
                        "average_duration": pipeline_metrics.average_duration_seconds
                    }
                )
    
    async def _check_consecutive_failures(self, pipeline_id: str):
        """Check for consecutive failures"""
        recent_executions = [
            e for e in self.execution_metrics.values()
            if e.pipeline_id == pipeline_id and e.status is not None
        ]
        
        # Sort by completion time
        recent_executions.sort(
            key=lambda e: e.completed_at or datetime.utcnow(),
            reverse=True
        )
        
        # Count consecutive failures
        consecutive_failures = 0
        for execution in recent_executions[:10]:  # Check last 10
            if execution.status == "failed":
                consecutive_failures += 1
            else:
                break
        
        if consecutive_failures >= self.alert_thresholds["consecutive_failures"]:
            await self._create_alert(
                pipeline_id,
                "consecutive_failures",
                f"Pipeline has failed {consecutive_failures} times consecutively",
                {
                    "consecutive_failures": consecutive_failures,
                    "threshold": self.alert_thresholds["consecutive_failures"]
                }
            )
    
    async def _create_alert(
        self,
        pipeline_id: str,
        alert_type: str,
        message: str,
        metadata: Dict[str, Any]
    ):
        """Create an alert"""
        alert = {
            "alert_id": f"{pipeline_id}_{alert_type}_{datetime.utcnow().timestamp()}",
            "pipeline_id": pipeline_id,
            "alert_type": alert_type,
            "message": message,
            "metadata": metadata,
            "timestamp": datetime.utcnow().isoformat(),
            "acknowledged": False
        }
        
        self.alerts.append(alert)
        
        # Publish alert event
        if self.event_publisher:
            await self.event_publisher.publish(
                "pipeline.alert.created",
                alert
            )
        
        logger.warning("pipeline_alert",
                      pipeline_id=pipeline_id,
                      alert_type=alert_type,
                      message=message)
    
    async def get_pipeline_metrics(
        self,
        pipeline_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get metrics for a pipeline"""
        metrics = self.pipeline_metrics.get(pipeline_id)
        if not metrics:
            return None
        
        return {
            "pipeline_id": metrics.pipeline_id,
            "total_executions": metrics.total_executions,
            "successful_executions": metrics.successful_executions,
            "failed_executions": metrics.failed_executions,
            "cancelled_executions": metrics.cancelled_executions,
            "success_rate": metrics.success_rate,
            "failure_rate": metrics.failure_rate,
            "average_duration_seconds": metrics.average_duration_seconds,
            "min_duration_seconds": metrics.min_duration_seconds if metrics.min_duration_seconds != float('inf') else None,
            "max_duration_seconds": metrics.max_duration_seconds,
            "last_execution": metrics.last_execution.isoformat() if metrics.last_execution else None,
            "last_success": metrics.last_success.isoformat() if metrics.last_success else None,
            "last_failure": metrics.last_failure.isoformat() if metrics.last_failure else None,
            "performance_history": self.performance_history.get(pipeline_id, [])[-20:]  # Last 20
        }
    
    async def get_all_metrics(self) -> Dict[str, Any]:
        """Get all pipeline metrics"""
        return {
            pipeline_id: await self.get_pipeline_metrics(pipeline_id)
            for pipeline_id in self.pipeline_metrics
        }
    
    async def get_alerts(
        self,
        pipeline_id: Optional[str] = None,
        acknowledged: Optional[bool] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get alerts"""
        alerts = self.alerts
        
        if pipeline_id:
            alerts = [a for a in alerts if a["pipeline_id"] == pipeline_id]
        
        if acknowledged is not None:
            alerts = [a for a in alerts if a["acknowledged"] == acknowledged]
        
        # Sort by timestamp descending
        alerts.sort(key=lambda a: a["timestamp"], reverse=True)
        
        return alerts[:limit]
    
    async def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert"""
        for alert in self.alerts:
            if alert["alert_id"] == alert_id:
                alert["acknowledged"] = True
                alert["acknowledged_at"] = datetime.utcnow().isoformat()
                return True
        return False
    
    async def _monitoring_loop(self):
        """Background monitoring loop"""
        while self.is_running:
            try:
                # Periodic health checks
                await self._check_pipeline_health()
                
                # Save metrics periodically
                await self._save_metrics()
                
                # Clean old data
                await self._cleanup_old_data()
                
                # Sleep
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("monitoring_loop_error", error=str(e))
                await asyncio.sleep(300)  # Wait longer on error
    
    async def _check_pipeline_health(self):
        """Check overall pipeline health"""
        # Calculate aggregate metrics
        total_pipelines = len(self.pipeline_metrics)
        active_pipelines = sum(
            1 for m in self.pipeline_metrics.values()
            if m.last_execution and 
            (datetime.utcnow() - m.last_execution).days < 1
        )
        
        failing_pipelines = sum(
            1 for m in self.pipeline_metrics.values()
            if m.failure_rate > self.alert_thresholds["failure_rate"]
        )
        
        # Update system metrics
        if self.metrics:
            self.metrics.update_metric(
                "pipelines_total",
                total_pipelines
            )
            
            self.metrics.update_metric(
                "pipelines_active",
                active_pipelines
            )
            
            self.metrics.update_metric(
                "pipelines_failing",
                failing_pipelines
            )
    
    async def _cleanup_old_data(self):
        """Clean up old monitoring data"""
        cutoff = datetime.utcnow() - timedelta(days=7)
        
        # Clean old executions
        old_executions = [
            eid for eid, e in self.execution_metrics.items()
            if e.completed_at and e.completed_at < cutoff
        ]
        
        for eid in old_executions:
            del self.execution_metrics[eid]
        
        # Clean old alerts
        self.alerts = [
            a for a in self.alerts
            if datetime.fromisoformat(a["timestamp"]) >= cutoff
        ]
        
        logger.info("cleaned_old_monitoring_data",
                   executions_removed=len(old_executions),
                   alerts_remaining=len(self.alerts))
    
    async def _load_metrics(self):
        """Load metrics from storage"""
        try:
            metrics_data = await self.vault_consul.get_config(
                "pipeline-orchestration/metrics",
                {}
            )
            
            # Restore pipeline metrics
            for pipeline_id, data in metrics_data.get("pipelines", {}).items():
                self.pipeline_metrics[pipeline_id] = PipelineMetrics(
                    pipeline_id=pipeline_id,
                    total_executions=data.get("total_executions", 0),
                    successful_executions=data.get("successful_executions", 0),
                    failed_executions=data.get("failed_executions", 0),
                    cancelled_executions=data.get("cancelled_executions", 0),
                    average_duration_seconds=data.get("average_duration_seconds", 0.0),
                    min_duration_seconds=data.get("min_duration_seconds", float('inf')),
                    max_duration_seconds=data.get("max_duration_seconds", 0.0),
                    last_execution=datetime.fromisoformat(data["last_execution"]) if data.get("last_execution") else None,
                    last_success=datetime.fromisoformat(data["last_success"]) if data.get("last_success") else None,
                    last_failure=datetime.fromisoformat(data["last_failure"]) if data.get("last_failure") else None
                )
            
            logger.info("loaded_pipeline_metrics", count=len(self.pipeline_metrics))
            
        except Exception as e:
            logger.error("load_metrics_error", error=str(e))
    
    async def _save_metrics(self):
        """Save metrics to storage"""
        try:
            metrics_data = {
                "pipelines": {
                    pid: {
                        "total_executions": m.total_executions,
                        "successful_executions": m.successful_executions,
                        "failed_executions": m.failed_executions,
                        "cancelled_executions": m.cancelled_executions,
                        "average_duration_seconds": m.average_duration_seconds,
                        "min_duration_seconds": m.min_duration_seconds if m.min_duration_seconds != float('inf') else None,
                        "max_duration_seconds": m.max_duration_seconds,
                        "last_execution": m.last_execution.isoformat() if m.last_execution else None,
                        "last_success": m.last_success.isoformat() if m.last_success else None,
                        "last_failure": m.last_failure.isoformat() if m.last_failure else None
                    }
                    for pid, m in self.pipeline_metrics.items()
                },
                "last_updated": datetime.utcnow().isoformat()
            }
            
            await self.vault_consul.consul.kv.put(
                "pipeline-orchestration/metrics",
                json.dumps(metrics_data)
            )
            
        except Exception as e:
            logger.error("save_metrics_error", error=str(e)) 