"""Health check management for DataIntelligenceSuite services."""

from typing import Dict, Any, Callable, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Service health status levels."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"  
    UNHEALTHY = "unhealthy"


@dataclass
class HealthCheckResult:
    """Result of a health check."""
    
    name: str
    status: HealthStatus
    message: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    duration_ms: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OverallHealth:
    """Overall service health status."""
    
    status: HealthStatus
    message: Optional[str] = None
    checks: List[HealthCheckResult] = field(default_factory=list)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API responses."""
        return {
            "status": self.status.value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "checks": [
                {
                    "name": check.name,
                    "status": check.status.value,
                    "message": check.message,
                    "duration_ms": check.duration_ms,
                    "metadata": check.metadata
                }
                for check in self.checks
            ]
        }


class HealthCheckManager:
    """
    Manages health checks for DataIntelligenceSuite services.
    
    Features:
    - Multiple health check registration
    - Async health check execution
    - Overall health calculation
    - Health history tracking
    - Check timeouts and retries
    """
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.checks: Dict[str, Callable] = {}
        self.check_results: Dict[str, HealthCheckResult] = {}
        self.overall_status = HealthStatus.HEALTHY
        self.startup_time = datetime.utcnow()
        
        # Configuration
        self.check_timeout = 5.0  # seconds
        self.max_retries = 3
        
        # History
        self.health_history: List[OverallHealth] = []
        self.max_history_size = 100
        
    def add_check(self, name: str, check_func: Callable):
        """Add a health check."""
        self.checks[name] = check_func
        logger.info(f"Added health check: {name}")
        
    def remove_check(self, name: str):
        """Remove a health check."""
        if name in self.checks:
            del self.checks[name]
            if name in self.check_results:
                del self.check_results[name]
            logger.info(f"Removed health check: {name}")
            
    async def check_health(self) -> OverallHealth:
        """Run all health checks and return overall status."""
        results = []
        
        # Run all checks concurrently
        check_tasks = []
        for name, check_func in self.checks.items():
            task = asyncio.create_task(
                self._run_check_with_timeout(name, check_func)
            )
            check_tasks.append(task)
            
        # Wait for all checks
        check_results = await asyncio.gather(*check_tasks, return_exceptions=True)
        
        # Process results
        unhealthy_count = 0
        degraded_count = 0
        
        for i, (name, _) in enumerate(self.checks.items()):
            result = check_results[i]
            
            if isinstance(result, Exception):
                # Check failed with exception
                health_result = HealthCheckResult(
                    name=name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Check failed: {str(result)}"
                )
            else:
                health_result = result
                
            results.append(health_result)
            self.check_results[name] = health_result
            
            # Count statuses
            if health_result.status == HealthStatus.UNHEALTHY:
                unhealthy_count += 1
            elif health_result.status == HealthStatus.DEGRADED:
                degraded_count += 1
                
        # Calculate overall status
        if unhealthy_count > 0:
            overall_status = HealthStatus.UNHEALTHY
            message = f"{unhealthy_count} checks unhealthy"
        elif degraded_count > 0:
            overall_status = HealthStatus.DEGRADED
            message = f"{degraded_count} checks degraded"
        else:
            overall_status = HealthStatus.HEALTHY
            message = "All checks healthy"
            
        # Create overall health
        overall = OverallHealth(
            status=overall_status,
            message=message,
            checks=results
        )
        
        # Update state
        self.overall_status = overall_status
        
        # Add to history
        self._add_to_history(overall)
        
        return overall
        
    async def _run_check_with_timeout(
        self,
        name: str,
        check_func: Callable
    ) -> HealthCheckResult:
        """Run a single health check with timeout and retries."""
        start_time = datetime.utcnow()
        
        for attempt in range(self.max_retries):
            try:
                # Run check with timeout
                result = await asyncio.wait_for(
                    self._run_single_check(check_func),
                    timeout=self.check_timeout
                )
                
                # Calculate duration
                duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
                
                # Create result
                if result is True:
                    return HealthCheckResult(
                        name=name,
                        status=HealthStatus.HEALTHY,
                        duration_ms=duration_ms
                    )
                elif result is False:
                    return HealthCheckResult(
                        name=name,
                        status=HealthStatus.UNHEALTHY,
                        message="Check returned False",
                        duration_ms=duration_ms
                    )
                elif isinstance(result, dict):
                    # Check returned detailed result
                    return HealthCheckResult(
                        name=name,
                        status=result.get("status", HealthStatus.HEALTHY),
                        message=result.get("message"),
                        duration_ms=duration_ms,
                        metadata=result.get("metadata", {})
                    )
                else:
                    # Unknown result type
                    return HealthCheckResult(
                        name=name,
                        status=HealthStatus.DEGRADED,
                        message=f"Unknown result type: {type(result)}",
                        duration_ms=duration_ms
                    )
                    
            except asyncio.TimeoutError:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Health check {name} timed out, retrying...")
                    await asyncio.sleep(0.5)
                else:
                    return HealthCheckResult(
                        name=name,
                        status=HealthStatus.UNHEALTHY,
                        message=f"Timeout after {self.check_timeout}s"
                    )
                    
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(f"Health check {name} failed: {e}, retrying...")
                    await asyncio.sleep(0.5)
                else:
                    return HealthCheckResult(
                        name=name,
                        status=HealthStatus.UNHEALTHY,
                        message=str(e)
                    )
                    
    async def _run_single_check(self, check_func: Callable) -> Any:
        """Run a single health check function."""
        # Handle both sync and async functions
        if asyncio.iscoroutinefunction(check_func):
            return await check_func()
        else:
            return check_func()
            
    def _add_to_history(self, health: OverallHealth):
        """Add health status to history."""
        self.health_history.append(health)
        
        # Trim history
        if len(self.health_history) > self.max_history_size:
            self.health_history = self.health_history[-self.max_history_size:]
            
    def set_status(self, status: HealthStatus, message: Optional[str] = None):
        """Manually set overall health status."""
        self.overall_status = status
        
        # Create a simple overall health record
        overall = OverallHealth(
            status=status,
            message=message,
            checks=list(self.check_results.values())
        )
        
        self._add_to_history(overall)
        
    def get_status(self) -> HealthStatus:
        """Get current overall health status."""
        return self.overall_status
        
    def get_history(self, limit: int = 10) -> List[OverallHealth]:
        """Get recent health history."""
        return self.health_history[-limit:]
        
    def get_uptime(self) -> float:
        """Get service uptime in seconds."""
        return (datetime.utcnow() - self.startup_time).total_seconds()
        
    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for monitoring."""
        healthy_count = sum(
            1 for r in self.check_results.values()
            if r.status == HealthStatus.HEALTHY
        )
        degraded_count = sum(
            1 for r in self.check_results.values()
            if r.status == HealthStatus.DEGRADED
        )
        unhealthy_count = sum(
            1 for r in self.check_results.values()
            if r.status == HealthStatus.UNHEALTHY
        )
        
        return {
            "service": self.service_name,
            "status": self.overall_status.value,
            "uptime_seconds": self.get_uptime(),
            "checks": {
                "total": len(self.checks),
                "healthy": healthy_count,
                "degraded": degraded_count,
                "unhealthy": unhealthy_count
            },
            "last_check": self.health_history[-1].timestamp.isoformat() if self.health_history else None
        } 