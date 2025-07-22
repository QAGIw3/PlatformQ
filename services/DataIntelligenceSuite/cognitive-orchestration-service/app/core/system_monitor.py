"""
System Monitor

Real-time system monitoring and metrics collection
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import psutil
import numpy as np
from prometheus_client import Counter, Gauge, Histogram, Summary
import structlog

logger = structlog.get_logger()


@dataclass
class SystemMetrics:
    """System metrics snapshot"""
    timestamp: datetime
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    network_io: Dict[str, float]
    active_processes: int
    service_metrics: Dict[str, Any]


class SystemMonitor:
    """Real-time system monitoring"""
    
    def __init__(self, settings):
        self.settings = settings
        self._running = False
        self._metrics_history: List[SystemMetrics] = []
        self._monitoring_task = None
        
        # Prometheus metrics
        self.cpu_gauge = Gauge('system_cpu_usage', 'CPU usage percentage')
        self.memory_gauge = Gauge('system_memory_usage', 'Memory usage percentage')
        self.disk_gauge = Gauge('system_disk_usage', 'Disk usage percentage')
        self.workflow_counter = Counter('workflow_executions_total', 'Total workflow executions')
        self.optimization_histogram = Histogram('optimization_duration_seconds', 'Optimization duration')
        
    async def start(self):
        """Start monitoring"""
        self._running = True
        self._monitoring_task = asyncio.create_task(self._monitor_loop())
        logger.info("System monitor started")
        
    async def stop(self):
        """Stop monitoring"""
        self._running = False
        if self._monitoring_task:
            self._monitoring_task.cancel()
            await asyncio.gather(self._monitoring_task, return_exceptions=True)
        logger.info("System monitor stopped")
        
    async def get_current_metrics(self) -> Dict[str, Any]:
        """Get current system metrics"""
        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        network = psutil.net_io_counters()
        
        return {
            "cpu_usage": cpu_percent,
            "memory_usage": memory.percent,
            "disk_usage": disk.percent,
            "available_memory_gb": memory.available / (1024**3),
            "network_bytes_sent": network.bytes_sent,
            "network_bytes_recv": network.bytes_recv,
            "active_workflows": len(asyncio.all_tasks()),
            "timestamp": datetime.utcnow()
        }
        
    async def collect_metrics(self) -> SystemMetrics:
        """Collect comprehensive system metrics"""
        metrics = SystemMetrics(
            timestamp=datetime.utcnow(),
            cpu_usage=psutil.cpu_percent(interval=0.1),
            memory_usage=psutil.virtual_memory().percent,
            disk_usage=psutil.disk_usage('/').percent,
            network_io={
                "bytes_sent": psutil.net_io_counters().bytes_sent,
                "bytes_recv": psutil.net_io_counters().bytes_recv
            },
            active_processes=len(psutil.pids()),
            service_metrics=await self._collect_service_metrics()
        )
        
        # Update Prometheus metrics
        self.cpu_gauge.set(metrics.cpu_usage)
        self.memory_gauge.set(metrics.memory_usage)
        self.disk_gauge.set(metrics.disk_usage)
        
        # Store in history
        self._metrics_history.append(metrics)
        
        # Keep only recent history
        cutoff = datetime.utcnow() - timedelta(hours=24)
        self._metrics_history = [m for m in self._metrics_history if m.timestamp > cutoff]
        
        return metrics
        
    async def get_step_resources(self, step_name: str) -> Dict[str, float]:
        """Get resource usage for a specific step"""
        # In real implementation, would track per-step metrics
        current = await self.get_current_metrics()
        return {
            "cpu": current["cpu_usage"],
            "memory": current["memory_usage"],
            "duration": 0  # Would be tracked separately
        }
        
    async def get_historical_metrics(self, hours: int = 24) -> List[SystemMetrics]:
        """Get historical metrics"""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return [m for m in self._metrics_history if m.timestamp > cutoff]
        
    async def _monitor_loop(self):
        """Background monitoring loop"""
        while self._running:
            try:
                await self.collect_metrics()
                await asyncio.sleep(self.settings.metrics_collection_interval)
            except Exception as e:
                logger.error(f"Monitoring error: {e}")
                await asyncio.sleep(60)
                
    async def _collect_service_metrics(self) -> Dict[str, Any]:
        """Collect metrics from integrated services"""
        metrics = {}
        
        # Would collect from actual services
        metrics["data_platform"] = {
            "query_queue_size": 0,
            "active_queries": 0
        }
        
        metrics["ml_platform"] = {
            "training_jobs": 0,
            "model_serving_requests": 0
        }
        
        return metrics 