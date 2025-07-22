"""
Pipeline Metrics Collector

Collects and exposes pipeline-specific metrics for monitoring.
"""

from typing import Dict, Optional
from data_intelligence_common import MetricsCollector

class PipelineMetricsCollector:
    """
    Collects pipeline-specific metrics
    """
    
    def __init__(self, metrics_collector: Optional[MetricsCollector] = None):
        self.metrics = metrics_collector
        
        if self.metrics:
            self._define_metrics()
    
    def _define_metrics(self):
        """Define pipeline-specific metrics"""
        # Pipeline execution metrics
        self.metrics.define_metric(
            "pipeline_executions_started_total",
            "counter",
            "Total number of pipeline executions started",
            ["pipeline_id"]
        )
        
        self.metrics.define_metric(
            "pipeline_executions_completed_total",
            "counter",
            "Total number of pipeline executions completed",
            ["pipeline_id", "status"]
        )
        
        self.metrics.define_metric(
            "pipeline_execution_duration_seconds",
            "histogram",
            "Pipeline execution duration in seconds",
            ["pipeline_id"]
        )
        
        # Step metrics
        self.metrics.define_metric(
            "pipeline_steps_completed_total",
            "counter",
            "Total number of pipeline steps completed",
            ["pipeline_id", "step_name", "status"]
        )
        
        self.metrics.define_metric(
            "pipeline_step_duration_seconds",
            "histogram",
            "Pipeline step duration in seconds",
            ["pipeline_id", "step_name"]
        )
        
        # System metrics
        self.metrics.define_metric(
            "pipelines_total",
            "gauge",
            "Total number of registered pipelines"
        )
        
        self.metrics.define_metric(
            "pipelines_active",
            "gauge",
            "Number of active pipelines"
        )
        
        self.metrics.define_metric(
            "pipelines_failing",
            "gauge",
            "Number of pipelines with high failure rate"
        )
        
        self.metrics.define_metric(
            "pipeline_queue_size",
            "gauge",
            "Number of pipelines waiting to execute"
        )
        
        # Scheduler metrics
        self.metrics.define_metric(
            "scheduled_pipelines_total",
            "gauge",
            "Total number of scheduled pipelines"
        )
        
        self.metrics.define_metric(
            "pipeline_schedule_lag_seconds",
            "histogram",
            "Time between scheduled and actual execution",
            ["pipeline_id"]
        )
        
        # Resource metrics
        self.metrics.define_metric(
            "pipeline_memory_usage_bytes",
            "gauge",
            "Memory usage by pipeline executions",
            ["pipeline_id"]
        )
        
        self.metrics.define_metric(
            "pipeline_cpu_usage_percent",
            "gauge",
            "CPU usage by pipeline executions",
            ["pipeline_id"]
        ) 