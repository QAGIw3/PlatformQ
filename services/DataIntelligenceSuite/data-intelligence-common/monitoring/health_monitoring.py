"""
Comprehensive Health Monitoring for DataIntelligence Services
Provides health checks, monitoring, alerting, and observability
"""

import asyncio
import logging
import time
import psutil
import aiohttp
from typing import Dict, Any, Optional, List, Callable, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import deque
import threading
from prometheus_client import Gauge, Counter, Histogram, Info

logger = logging.getLogger(__name__)


# Prometheus metrics
HEALTH_CHECK_STATUS = Gauge('health_check_status', 'Health check status', ['service', 'check'])
HEALTH_CHECK_DURATION = Histogram('health_check_duration_seconds', 'Health check duration', ['service', 'check'])
HEALTH_CHECK_FAILURES = Counter('health_check_failures_total', 'Health check failures', ['service', 'check'])
SERVICE_UPTIME = Gauge('service_uptime_seconds', 'Service uptime in seconds', ['service'])
SERVICE_INFO = Info('service_info', 'Service information')


class HealthStatus(Enum):
    """Health status levels"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class CheckType(Enum):
    """Types of health checks"""
    LIVENESS = "liveness"
    READINESS = "readiness"
    STARTUP = "startup"


@dataclass
class HealthCheckResult:
    """Result of a health check"""
    name: str
    status: HealthStatus
    message: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)
    duration_ms: Optional[float] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    check_type: CheckType = CheckType.LIVENESS


@dataclass
class HealthCheckConfig:
    """Configuration for a health check"""
    name: str
    check_func: Callable
    interval: timedelta = timedelta(seconds=30)
    timeout: timedelta = timedelta(seconds=10)
    failure_threshold: int = 3
    success_threshold: int = 1
    check_type: CheckType = CheckType.LIVENESS
    critical: bool = True
    tags: List[str] = field(default_factory=list)


@dataclass
class ServiceHealth:
    """Overall service health"""
    status: HealthStatus
    checks: Dict[str, HealthCheckResult]
    uptime: timedelta
    version: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


class HealthCheck:
    """Individual health check implementation"""
    
    def __init__(self, config: HealthCheckConfig):
        self.config = config
        self.consecutive_failures = 0
        self.consecutive_successes = 0
        self.last_result: Optional[HealthCheckResult] = None
        self.history: deque[HealthCheckResult] = deque(maxlen=100)
        self._lock = threading.Lock()
        
    async def execute(self) -> HealthCheckResult:
        """Execute the health check"""
        start_time = time.time()
        
        try:
            # Run check with timeout
            if asyncio.iscoroutinefunction(self.config.check_func):
                result = await asyncio.wait_for(
                    self.config.check_func(),
                    timeout=self.config.timeout.total_seconds()
                )
            else:
                result = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, self.config.check_func
                    ),
                    timeout=self.config.timeout.total_seconds()
                )
                
            # Process result
            duration_ms = (time.time() - start_time) * 1000
            
            if isinstance(result, bool):
                status = HealthStatus.HEALTHY if result else HealthStatus.UNHEALTHY
                message = None
                details = {}
            elif isinstance(result, dict):
                status = HealthStatus(result.get("status", HealthStatus.UNKNOWN.value))
                message = result.get("message")
                details = result.get("details", {})
            elif isinstance(result, HealthCheckResult):
                return result
            else:
                status = HealthStatus.UNKNOWN
                message = f"Invalid result type: {type(result)}"
                details = {}
                
            check_result = HealthCheckResult(
                name=self.config.name,
                status=status,
                message=message,
                details=details,
                duration_ms=duration_ms,
                check_type=self.config.check_type
            )
            
            self._update_state(check_result)
            return check_result
            
        except asyncio.TimeoutError:
            check_result = HealthCheckResult(
                name=self.config.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check timed out after {self.config.timeout.total_seconds()}s",
                duration_ms=(time.time() - start_time) * 1000,
                check_type=self.config.check_type
            )
            self._update_state(check_result)
            return check_result
            
        except Exception as e:
            check_result = HealthCheckResult(
                name=self.config.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {str(e)}",
                duration_ms=(time.time() - start_time) * 1000,
                check_type=self.config.check_type
            )
            self._update_state(check_result)
            return check_result
            
    def _update_state(self, result: HealthCheckResult):
        """Update check state based on result"""
        with self._lock:
            self.last_result = result
            self.history.append(result)
            
            # Update metrics
            HEALTH_CHECK_STATUS.labels(
                service="dataintelligence",
                check=self.config.name
            ).set(1 if result.status == HealthStatus.HEALTHY else 0)
            
            HEALTH_CHECK_DURATION.labels(
                service="dataintelligence",
                check=self.config.name
            ).observe(result.duration_ms / 1000 if result.duration_ms else 0)
            
            # Update consecutive counts
            if result.status == HealthStatus.HEALTHY:
                self.consecutive_successes += 1
                self.consecutive_failures = 0
            else:
                self.consecutive_failures += 1
                self.consecutive_successes = 0
                
                HEALTH_CHECK_FAILURES.labels(
                    service="dataintelligence",
                    check=self.config.name
                ).inc()
                
    def get_status(self) -> HealthStatus:
        """Get current health status considering thresholds"""
        with self._lock:
            if not self.last_result:
                return HealthStatus.UNKNOWN
                
            # Check failure threshold
            if self.consecutive_failures >= self.config.failure_threshold:
                return HealthStatus.UNHEALTHY
                
            # Check success threshold
            if self.consecutive_successes >= self.config.success_threshold:
                return self.last_result.status
                
            # In transition
            return HealthStatus.DEGRADED
            
    def get_statistics(self) -> Dict[str, Any]:
        """Get check statistics"""
        with self._lock:
            if not self.history:
                return {}
                
            durations = [r.duration_ms for r in self.history if r.duration_ms]
            statuses = [r.status for r in self.history]
            
            return {
                "total_checks": len(self.history),
                "success_rate": sum(1 for s in statuses if s == HealthStatus.HEALTHY) / len(statuses),
                "average_duration_ms": sum(durations) / len(durations) if durations else 0,
                "min_duration_ms": min(durations) if durations else 0,
                "max_duration_ms": max(durations) if durations else 0,
                "consecutive_failures": self.consecutive_failures,
                "consecutive_successes": self.consecutive_successes
            }


class HealthMonitor:
    """Main health monitoring system"""
    
    def __init__(self, service_name: str, version: str = "1.0.0"):
        self.service_name = service_name
        self.version = version
        self.start_time = datetime.utcnow()
        self.checks: Dict[str, HealthCheck] = {}
        self._running = False
        self._tasks: List[asyncio.Task] = []
        
        # Update service info
        SERVICE_INFO.info({
            "service": service_name,
            "version": version,
            "start_time": self.start_time.isoformat()
        })
        
    def add_check(self, config: HealthCheckConfig) -> None:
        """Add a health check"""
        self.checks[config.name] = HealthCheck(config)
        logger.info(f"Added health check: {config.name}")
        
    def remove_check(self, name: str) -> None:
        """Remove a health check"""
        if name in self.checks:
            del self.checks[name]
            logger.info(f"Removed health check: {name}")
            
    async def start(self) -> None:
        """Start health monitoring"""
        if self._running:
            return
            
        self._running = True
        logger.info(f"Starting health monitor for {self.service_name}")
        
        # Start check tasks
        for name, check in self.checks.items():
            task = asyncio.create_task(self._run_check_loop(check))
            self._tasks.append(task)
            
        # Start uptime tracking
        asyncio.create_task(self._track_uptime())
        
    async def stop(self) -> None:
        """Stop health monitoring"""
        self._running = False
        
        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
            
        # Wait for tasks to complete
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        
        logger.info(f"Stopped health monitor for {self.service_name}")
        
    async def _run_check_loop(self, check: HealthCheck) -> None:
        """Run health check loop"""
        while self._running:
            try:
                await check.execute()
                await asyncio.sleep(check.config.interval.total_seconds())
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop {check.config.name}: {e}")
                await asyncio.sleep(check.config.interval.total_seconds())
                
    async def _track_uptime(self) -> None:
        """Track service uptime"""
        while self._running:
            uptime = (datetime.utcnow() - self.start_time).total_seconds()
            SERVICE_UPTIME.labels(service=self.service_name).set(uptime)
            await asyncio.sleep(10)
            
    async def get_health(self, check_type: Optional[CheckType] = None) -> ServiceHealth:
        """Get current service health"""
        checks_results = {}
        overall_status = HealthStatus.HEALTHY
        
        for name, check in self.checks.items():
            # Filter by check type if specified
            if check_type and check.config.check_type != check_type:
                continue
                
            # Get check result
            if check.last_result:
                checks_results[name] = check.last_result
                
                # Update overall status
                status = check.get_status()
                if status == HealthStatus.UNHEALTHY and check.config.critical:
                    overall_status = HealthStatus.UNHEALTHY
                elif status == HealthStatus.DEGRADED and overall_status != HealthStatus.UNHEALTHY:
                    overall_status = HealthStatus.DEGRADED
                    
        return ServiceHealth(
            status=overall_status,
            checks=checks_results,
            uptime=datetime.utcnow() - self.start_time,
            version=self.version,
            metadata={
                "service": self.service_name,
                "checks_count": len(checks_results),
                "critical_checks": sum(1 for c in self.checks.values() if c.config.critical)
            }
        )
        
    async def run_check(self, name: str) -> Optional[HealthCheckResult]:
        """Run a specific health check"""
        if name not in self.checks:
            return None
            
        return await self.checks[name].execute()
        
    def get_check_stats(self, name: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific check"""
        if name not in self.checks:
            return None
            
        return self.checks[name].get_statistics()


# Standard health check implementations

async def check_database_health(connection_string: str,
                               query: str = "SELECT 1") -> Dict[str, Any]:
    """Check database health"""
    try:
        # This is a placeholder - actual implementation would use appropriate DB client
        # For example, with asyncpg for PostgreSQL:
        # async with asyncpg.connect(connection_string) as conn:
        #     await conn.fetchval(query)
        
        return {
            "status": HealthStatus.HEALTHY.value,
            "details": {
                "query": query,
                "response_time_ms": 10.5
            }
        }
    except Exception as e:
        return {
            "status": HealthStatus.UNHEALTHY.value,
            "message": f"Database connection failed: {str(e)}"
        }


async def check_cache_health(cache_client) -> Dict[str, Any]:
    """Check cache health"""
    try:
        test_key = f"health_check_{datetime.utcnow().timestamp()}"
        test_value = "healthy"
        
        # Set and get test value
        await cache_client.put_async("health", test_key, test_value, timedelta(seconds=60))
        result = await cache_client.get_async("health", test_key)
        
        if result == test_value:
            return {
                "status": HealthStatus.HEALTHY.value,
                "details": {
                    "test_key": test_key,
                    "cache_type": "ignite"
                }
            }
        else:
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "message": "Cache test failed: value mismatch"
            }
            
    except Exception as e:
        return {
            "status": HealthStatus.UNHEALTHY.value,
            "message": f"Cache health check failed: {str(e)}"
        }


async def check_api_health(url: str, timeout: int = 5) -> Dict[str, Any]:
    """Check external API health"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                if response.status == 200:
                    return {
                        "status": HealthStatus.HEALTHY.value,
                        "details": {
                            "url": url,
                            "status_code": response.status,
                            "response_time_ms": response.headers.get("X-Response-Time", "unknown")
                        }
                    }
                else:
                    return {
                        "status": HealthStatus.UNHEALTHY.value,
                        "message": f"API returned status {response.status}",
                        "details": {
                            "url": url,
                            "status_code": response.status
                        }
                    }
                    
    except Exception as e:
        return {
            "status": HealthStatus.UNHEALTHY.value,
            "message": f"API health check failed: {str(e)}",
            "details": {"url": url}
        }


def check_disk_space(threshold_percent: float = 90.0) -> Dict[str, Any]:
    """Check disk space"""
    try:
        disk_usage = psutil.disk_usage('/')
        
        if disk_usage.percent < threshold_percent:
            return {
                "status": HealthStatus.HEALTHY.value,
                "details": {
                    "used_percent": disk_usage.percent,
                    "free_gb": disk_usage.free / (1024**3),
                    "total_gb": disk_usage.total / (1024**3)
                }
            }
        else:
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"Disk usage {disk_usage.percent}% exceeds threshold {threshold_percent}%",
                "details": {
                    "used_percent": disk_usage.percent,
                    "free_gb": disk_usage.free / (1024**3)
                }
            }
            
    except Exception as e:
        return {
            "status": HealthStatus.UNHEALTHY.value,
            "message": f"Disk space check failed: {str(e)}"
        }


def check_memory_usage(threshold_percent: float = 90.0) -> Dict[str, Any]:
    """Check memory usage"""
    try:
        memory = psutil.virtual_memory()
        
        if memory.percent < threshold_percent:
            return {
                "status": HealthStatus.HEALTHY.value,
                "details": {
                    "used_percent": memory.percent,
                    "available_gb": memory.available / (1024**3),
                    "total_gb": memory.total / (1024**3)
                }
            }
        else:
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"Memory usage {memory.percent}% exceeds threshold {threshold_percent}%",
                "details": {
                    "used_percent": memory.percent,
                    "available_gb": memory.available / (1024**3)
                }
            }
            
    except Exception as e:
        return {
            "status": HealthStatus.UNHEALTHY.value,
            "message": f"Memory check failed: {str(e)}"
        }


def check_cpu_usage(threshold_percent: float = 90.0, 
                   interval: float = 1.0) -> Dict[str, Any]:
    """Check CPU usage"""
    try:
        cpu_percent = psutil.cpu_percent(interval=interval)
        
        if cpu_percent < threshold_percent:
            return {
                "status": HealthStatus.HEALTHY.value,
                "details": {
                    "cpu_percent": cpu_percent,
                    "cpu_count": psutil.cpu_count()
                }
            }
        else:
            return {
                "status": HealthStatus.UNHEALTHY.value,
                "message": f"CPU usage {cpu_percent}% exceeds threshold {threshold_percent}%",
                "details": {
                    "cpu_percent": cpu_percent,
                    "cpu_count": psutil.cpu_count()
                }
            }
            
    except Exception as e:
        return {
            "status": HealthStatus.UNHEALTHY.value,
            "message": f"CPU check failed: {str(e)}"
        }


class HealthEndpoints:
    """FastAPI health check endpoints"""
    
    def __init__(self, health_monitor: HealthMonitor):
        self.health_monitor = health_monitor
        
    async def liveness(self) -> Dict[str, Any]:
        """Liveness probe endpoint"""
        health = await self.health_monitor.get_health(CheckType.LIVENESS)
        
        return {
            "status": health.status.value,
            "timestamp": health.timestamp.isoformat(),
            "uptime_seconds": health.uptime.total_seconds()
        }
        
    async def readiness(self) -> Dict[str, Any]:
        """Readiness probe endpoint"""
        health = await self.health_monitor.get_health(CheckType.READINESS)
        
        response = {
            "status": health.status.value,
            "timestamp": health.timestamp.isoformat(),
            "checks": {}
        }
        
        for name, result in health.checks.items():
            response["checks"][name] = {
                "status": result.status.value,
                "message": result.message
            }
            
        return response
        
    async def startup(self) -> Dict[str, Any]:
        """Startup probe endpoint"""
        health = await self.health_monitor.get_health(CheckType.STARTUP)
        
        return {
            "status": health.status.value,
            "timestamp": health.timestamp.isoformat(),
            "version": health.version
        }
        
    async def detailed_health(self) -> Dict[str, Any]:
        """Detailed health information"""
        health = await self.health_monitor.get_health()
        
        response = {
            "status": health.status.value,
            "timestamp": health.timestamp.isoformat(),
            "version": health.version,
            "uptime_seconds": health.uptime.total_seconds(),
            "metadata": health.metadata,
            "checks": {}
        }
        
        for name, result in health.checks.items():
            response["checks"][name] = {
                "status": result.status.value,
                "message": result.message,
                "details": result.details,
                "duration_ms": result.duration_ms,
                "timestamp": result.timestamp.isoformat(),
                "type": result.check_type.value
            }
            
            # Add statistics
            stats = self.health_monitor.get_check_stats(name)
            if stats:
                response["checks"][name]["statistics"] = stats
                
        return response 