"""Resource Manager for Batch Processing Service

Manages cluster resources, monitors utilization, and provides resource allocation.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import psutil

from app.core.config import Settings


logger = logging.getLogger(__name__)


class ResourceMetrics:
    """Container for resource metrics"""
    
    def __init__(self):
        self.cpu_percent: float = 0.0
        self.memory_used_gb: float = 0.0
        self.memory_total_gb: float = 0.0
        self.memory_percent: float = 0.0
        self.disk_used_gb: float = 0.0
        self.disk_total_gb: float = 0.0
        self.disk_percent: float = 0.0
        self.executors_active: int = 0
        self.executors_total: int = 0
        self.cores_used: int = 0
        self.cores_total: int = 0
        self.timestamp: datetime = datetime.utcnow()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "cpu_percent": self.cpu_percent,
            "memory": {
                "used_gb": self.memory_used_gb,
                "total_gb": self.memory_total_gb,
                "percent": self.memory_percent
            },
            "disk": {
                "used_gb": self.disk_used_gb,
                "total_gb": self.disk_total_gb,
                "percent": self.disk_percent
            },
            "executors": {
                "active": self.executors_active,
                "total": self.executors_total
            },
            "cores": {
                "used": self.cores_used,
                "total": self.cores_total
            },
            "timestamp": self.timestamp.isoformat()
        }


class ResourceManager:
    """Manages cluster resources and allocation"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.current_metrics = ResourceMetrics()
        self.resource_history: List[ResourceMetrics] = []
        self._monitor_task: Optional[asyncio.Task] = None
        self._allocated_resources: Dict[str, Dict[str, Any]] = {}
        
    async def start(self):
        """Start the resource manager"""
        logger.info("Starting ResourceManager")
        
        # Start monitoring task
        self._monitor_task = asyncio.create_task(self._monitor_resources())
        
        logger.info("ResourceManager started")
        
    async def stop(self):
        """Stop the resource manager"""
        logger.info("Stopping ResourceManager")
        
        # Cancel monitoring task
        if self._monitor_task:
            self._monitor_task.cancel()
            
        logger.info("ResourceManager stopped")
        
    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get current cluster status"""
        return {
            "metrics": self.current_metrics.to_dict(),
            "allocated_jobs": len(self._allocated_resources),
            "available_profiles": list(self.settings.resource_profiles.keys()),
            "cluster_health": self._calculate_health_status()
        }
        
    async def allocate_resources(self, job_id: str, resource_profile: str) -> bool:
        """Allocate resources for a job"""
        profile = self.settings.resource_profiles.get(resource_profile)
        if not profile:
            logger.error(f"Invalid resource profile: {resource_profile}")
            return False
            
        # Check if resources are available
        required_memory_gb = self._parse_memory_string(profile["executor_memory"]) * profile["max_executors"]
        required_cores = profile["executor_cores"] * profile["max_executors"]
        
        available_memory = self.current_metrics.memory_total_gb - self.current_metrics.memory_used_gb
        available_cores = self.current_metrics.cores_total - self.current_metrics.cores_used
        
        if required_memory_gb > available_memory or required_cores > available_cores:
            logger.warning(f"Insufficient resources for job {job_id}")
            return False
            
        # Allocate resources
        self._allocated_resources[job_id] = {
            "profile": resource_profile,
            "memory_gb": required_memory_gb,
            "cores": required_cores,
            "allocated_at": datetime.utcnow()
        }
        
        # Update metrics
        self.current_metrics.memory_used_gb += required_memory_gb
        self.current_metrics.cores_used += required_cores
        self.current_metrics.executors_active += profile["max_executors"]
        
        logger.info(f"Allocated resources for job {job_id}: {resource_profile}")
        return True
        
    async def release_resources(self, job_id: str):
        """Release resources allocated to a job"""
        allocation = self._allocated_resources.get(job_id)
        if not allocation:
            return
            
        # Release resources
        self.current_metrics.memory_used_gb -= allocation["memory_gb"]
        self.current_metrics.cores_used -= allocation["cores"]
        
        profile = self.settings.resource_profiles[allocation["profile"]]
        self.current_metrics.executors_active -= profile["max_executors"]
        
        del self._allocated_resources[job_id]
        logger.info(f"Released resources for job {job_id}")
        
    def get_resource_utilization(self) -> Dict[str, float]:
        """Get current resource utilization percentages"""
        return {
            "cpu": self.current_metrics.cpu_percent,
            "memory": self.current_metrics.memory_percent,
            "disk": self.current_metrics.disk_percent,
            "executors": (self.current_metrics.executors_active / 
                         max(self.current_metrics.executors_total, 1)) * 100
        }
        
    def get_available_resources(self) -> Dict[str, Any]:
        """Get available resources"""
        return {
            "memory_gb": self.current_metrics.memory_total_gb - self.current_metrics.memory_used_gb,
            "cores": self.current_metrics.cores_total - self.current_metrics.cores_used,
            "executors": self.current_metrics.executors_total - self.current_metrics.executors_active
        }
        
    async def _monitor_resources(self):
        """Monitor system resources"""
        while True:
            try:
                await asyncio.sleep(30)  # Update every 30 seconds
                
                # Collect system metrics
                metrics = ResourceMetrics()
                
                # CPU metrics
                metrics.cpu_percent = psutil.cpu_percent(interval=1)
                
                # Memory metrics
                memory = psutil.virtual_memory()
                metrics.memory_total_gb = memory.total / (1024**3)
                metrics.memory_used_gb = memory.used / (1024**3)
                metrics.memory_percent = memory.percent
                
                # Disk metrics
                disk = psutil.disk_usage('/')
                metrics.disk_total_gb = disk.total / (1024**3)
                metrics.disk_used_gb = disk.used / (1024**3)
                metrics.disk_percent = disk.percent
                
                # Cluster metrics (in production, query Spark cluster)
                metrics.cores_total = psutil.cpu_count() * self.settings.spark_max_executors
                metrics.executors_total = self.settings.spark_max_executors
                
                # Update current metrics
                self.current_metrics = metrics
                
                # Add to history (keep last 100 samples)
                self.resource_history.append(metrics)
                if len(self.resource_history) > 100:
                    self.resource_history.pop(0)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring resources: {e}")
                
    def _calculate_health_status(self) -> str:
        """Calculate cluster health status"""
        utilization = self.get_resource_utilization()
        
        # Check thresholds
        critical_threshold = 90
        warning_threshold = 70
        
        if any(v > critical_threshold for v in utilization.values()):
            return "critical"
        elif any(v > warning_threshold for v in utilization.values()):
            return "warning"
        else:
            return "healthy"
            
    def _parse_memory_string(self, memory_str: str) -> float:
        """Parse memory string to GB"""
        memory_str = memory_str.lower()
        if memory_str.endswith('g'):
            return float(memory_str[:-1])
        elif memory_str.endswith('m'):
            return float(memory_str[:-1]) / 1024
        else:
            return float(memory_str) 