"""
Health check implementations for Consul-enabled services
"""

import asyncio
import time
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health check status"""
    PASSING = "passing"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class HealthCheck:
    """Individual health check"""
    name: str
    check_fn: Callable[[], bool]
    status: HealthStatus = HealthStatus.PASSING
    output: str = ""
    last_check: Optional[float] = None
    interval: int = 10  # seconds
    timeout: int = 5    # seconds
    
    async def execute(self):
        """Execute the health check"""
        try:
            start_time = time.time()
            
            # Run check with timeout
            result = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, self.check_fn),
                timeout=self.timeout
            )
            
            self.last_check = time.time()
            
            if result:
                self.status = HealthStatus.PASSING
                self.output = f"Check passed in {time.time() - start_time:.2f}s"
            else:
                self.status = HealthStatus.CRITICAL
                self.output = "Check returned false"
                
        except asyncio.TimeoutError:
            self.status = HealthStatus.CRITICAL
            self.output = f"Check timed out after {self.timeout}s"
            
        except Exception as e:
            self.status = HealthStatus.CRITICAL
            self.output = f"Check failed: {str(e)}"
            logger.exception(f"Health check {self.name} failed")


class HealthCheckRegistry:
    """Registry for service health checks"""
    
    def __init__(self):
        self.checks: Dict[str, HealthCheck] = {}
        self._running = False
        self._task = None
        
    def register(self, name: str, check_fn: Callable[[], bool], 
                 interval: int = 10, timeout: int = 5):
        """
        Register a health check
        
        Args:
            name: Name of the health check
            check_fn: Function that returns True if healthy
            interval: Check interval in seconds
            timeout: Check timeout in seconds
        """
        self.checks[name] = HealthCheck(
            name=name,
            check_fn=check_fn,
            interval=interval,
            timeout=timeout
        )
        logger.info(f"Registered health check: {name}")
        
    def unregister(self, name: str):
        """Unregister a health check"""
        if name in self.checks:
            del self.checks[name]
            logger.info(f"Unregistered health check: {name}")
            
    async def run_checks(self):
        """Run all health checks"""
        tasks = []
        for check in self.checks.values():
            if (check.last_check is None or 
                time.time() - check.last_check >= check.interval):
                tasks.append(check.execute())
                
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
    def get_status(self) -> Dict[str, Any]:
        """
        Get overall health status
        
        Returns:
            Dictionary with status and individual check results
        """
        check_results = []
        overall_status = HealthStatus.PASSING
        
        for check in self.checks.values():
            check_results.append({
                "name": check.name,
                "status": check.status.value,
                "output": check.output,
                "last_check": check.last_check
            })
            
            if check.status == HealthStatus.CRITICAL:
                overall_status = HealthStatus.CRITICAL
            elif check.status == HealthStatus.WARNING and overall_status == HealthStatus.PASSING:
                overall_status = HealthStatus.WARNING
                
        return {
            "status": overall_status.value,
            "checks": check_results,
            "timestamp": time.time()
        }
        
    async def start(self):
        """Start background health check loop"""
        if self._running:
            return
            
        self._running = True
        self._task = asyncio.create_task(self._check_loop())
        logger.info("Started health check loop")
        
    async def stop(self):
        """Stop background health check loop"""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped health check loop")
        
    async def _check_loop(self):
        """Background loop for running health checks"""
        while self._running:
            try:
                await self.run_checks()
                await asyncio.sleep(1)  # Check every second for any due checks
            except Exception as e:
                logger.exception("Error in health check loop")
                await asyncio.sleep(5)  # Back off on error


# Common health check implementations
def create_database_check(connection_fn: Callable) -> Callable[[], bool]:
    """
    Create a database health check
    
    Args:
        connection_fn: Function that returns a database connection
    """
    def check():
        try:
            conn = connection_fn()
            # Simple query to verify connection
            conn.execute("SELECT 1")
            return True
        except Exception:
            return False
    return check


def create_cache_check(cache_client) -> Callable[[], bool]:
    """
    Create a cache health check
    
    Args:
        cache_client: Cache client instance
    """
    def check():
        try:
            # Try to set and get a value
            key = "__health_check__"
            value = str(time.time())
            cache_client.set(key, value, expire=10)
            result = cache_client.get(key)
            return result == value
        except Exception:
            return False
    return check


def create_message_queue_check(queue_client) -> Callable[[], bool]:
    """
    Create a message queue health check
    
    Args:
        queue_client: Message queue client instance
    """
    def check():
        try:
            # Check if client is connected
            return queue_client.is_connected()
        except Exception:
            return False
    return check


def create_service_dependency_check(service_url: str) -> Callable[[], bool]:
    """
    Create a health check for a dependent service
    
    Args:
        service_url: URL of the dependent service
    """
    def check():
        try:
            import requests
            response = requests.get(f"{service_url}/health", timeout=5)
            return response.status_code == 200
        except Exception:
            return False
    return check


# FastAPI integration
def create_health_endpoint(registry: HealthCheckRegistry):
    """
    Create a FastAPI health endpoint
    
    Args:
        registry: Health check registry
        
    Returns:
        FastAPI router with health endpoint
    """
    from fastapi import APIRouter, Response
    
    router = APIRouter()
    
    @router.get("/health")
    async def health_check(response: Response):
        # Run checks
        await registry.run_checks()
        
        # Get status
        status = registry.get_status()
        
        # Set response code based on status
        if status["status"] == "critical":
            response.status_code = 503
        elif status["status"] == "warning":
            response.status_code = 200  # Still return 200 for warnings
            
        return status
        
    @router.get("/health/live")
    async def liveness_check():
        """Simple liveness check"""
        return {"status": "ok"}
        
    @router.get("/health/ready")
    async def readiness_check(response: Response):
        """Readiness check based on all health checks"""
        status = registry.get_status()
        
        if status["status"] == "critical":
            response.status_code = 503
            return {"ready": False, "status": status}
        
        return {"ready": True, "status": status}
        
    return router 