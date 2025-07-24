"""
Core Mixins for DataIntelligenceSuite

This module provides reusable mixins that consolidate common functionality
across all base classes in the system.
"""

import asyncio
import uuid
import time
import json
import logging
from typing import Any, Dict, List, Optional, Set, Callable, TypeVar, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from abc import ABC, abstractmethod
import threading

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..monitoring import MetricsCollector, StructuredLogger
from ..core.caching import CacheManager, cached
from ..core.events import EventBus, Event
from ..core.patterns.resilience import CircuitBreakerPattern, RetryConfig, retry

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')


class LifecycleMixin:
    """
    Provides lifecycle management functionality.
    
    Common pattern across BaseService, BaseProcessor, BaseEngine, BaseOrchestrator
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._initialized = False
        self._running = False
        self._shutting_down = False
        self._start_time: Optional[datetime] = None
        self._background_tasks: Set[asyncio.Task] = set()
        self._lifecycle_lock = asyncio.Lock()
        
    async def initialize(self) -> None:
        """Initialize the component"""
        async with self._lifecycle_lock:
            if self._initialized:
                logger.warning(f"{self.__class__.__name__} already initialized")
                return
                
            logger.info(f"Initializing {self.__class__.__name__}")
            self._start_time = datetime.utcnow()
            
            try:
                await self._initialize_internal()
                self._initialized = True
                logger.info(f"{self.__class__.__name__} initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize {self.__class__.__name__}: {e}")
                raise
                
    async def _initialize_internal(self) -> None:
        """Override this method for specific initialization logic"""
        pass
        
    async def start(self) -> None:
        """Start the component"""
        if not self._initialized:
            await self.initialize()
            
        async with self._lifecycle_lock:
            if self._running:
                logger.warning(f"{self.__class__.__name__} already running")
                return
                
            logger.info(f"Starting {self.__class__.__name__}")
            self._running = True
            
            try:
                await self._start_internal()
            except Exception as e:
                self._running = False
                logger.error(f"Failed to start {self.__class__.__name__}: {e}")
                raise
                
    async def _start_internal(self) -> None:
        """Override this method for specific start logic"""
        pass
        
    async def stop(self) -> None:
        """Stop the component"""
        async with self._lifecycle_lock:
            if not self._running:
                logger.warning(f"{self.__class__.__name__} not running")
                return
                
            logger.info(f"Stopping {self.__class__.__name__}")
            self._shutting_down = True
            self._running = False
            
            try:
                await self._stop_internal()
                await self._cancel_background_tasks()
            except Exception as e:
                logger.error(f"Error stopping {self.__class__.__name__}: {e}")
                raise
            finally:
                self._shutting_down = False
                
    async def _stop_internal(self) -> None:
        """Override this method for specific stop logic"""
        pass
        
    async def _cancel_background_tasks(self) -> None:
        """Cancel all background tasks"""
        for task in self._background_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._background_tasks.clear()
        
    def create_background_task(self, coro) -> asyncio.Task:
        """Create and track a background task"""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task
        
    @property
    def is_initialized(self) -> bool:
        """Check if component is initialized"""
        return self._initialized
        
    @property
    def is_running(self) -> bool:
        """Check if component is running"""
        return self._running
        
    @property
    def uptime(self) -> Optional[timedelta]:
        """Get component uptime"""
        if self._start_time:
            return datetime.utcnow() - self._start_time
        return None


class MetricsMixin:
    """
    Provides metrics collection functionality.
    
    Common pattern across all base classes
    """
    
    def __init__(self, *args, metrics_collector: Optional[MetricsCollector] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.metrics = metrics_collector or self._create_metrics_collector()
        self._setup_base_metrics()
        
    def _create_metrics_collector(self) -> MetricsCollector:
        """Create default metrics collector"""
        name = getattr(self, 'name', self.__class__.__name__.lower())
        return MetricsCollector(name)
        
    def _setup_base_metrics(self) -> None:
        """Setup base metrics"""
        # Request/operation counters
        self.metrics.create_counter(
            "operations_total",
            "Total number of operations",
            ["operation", "status"]
        )
        
        # Duration histograms
        self.metrics.create_histogram(
            "operation_duration_seconds",
            "Operation duration in seconds",
            ["operation"]
        )
        
        # Error counter
        self.metrics.create_counter(
            "errors_total",
            "Total number of errors",
            ["operation", "error_type"]
        )
        
        # Active operations gauge
        self.metrics.create_gauge(
            "active_operations",
            "Number of active operations",
            ["operation"]
        )
        
    def record_operation_start(self, operation: str) -> float:
        """Record operation start"""
        self.metrics.gauge("active_operations", labels={"operation": operation}).inc()
        return time.time()
        
    def record_operation_end(self, operation: str, start_time: float, status: str = "success") -> None:
        """Record operation end"""
        duration = time.time() - start_time
        
        self.metrics.counter(
            "operations_total",
            labels={"operation": operation, "status": status}
        ).inc()
        
        self.metrics.histogram(
            "operation_duration_seconds",
            labels={"operation": operation}
        ).observe(duration)
        
        self.metrics.gauge("active_operations", labels={"operation": operation}).dec()
        
    def record_error(self, operation: str, error_type: str) -> None:
        """Record error"""
        self.metrics.counter(
            "errors_total",
            labels={"operation": operation, "error_type": error_type}
        ).inc()


class CachingMixin:
    """
    Provides caching functionality.
    
    Consolidates caching patterns from various base classes
    """
    
    def __init__(self, *args, cache_manager: Optional[CacheManager] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache_manager = cache_manager
        self._cache_enabled = cache_manager is not None
        
    @cached(ttl=timedelta(minutes=5))
    async def cached_operation(self, key: str) -> Any:
        """Example cached operation - override in subclasses"""
        pass
        
    async def invalidate_cache(self, pattern: Optional[str] = None) -> None:
        """Invalidate cache entries"""
        if not self._cache_enabled:
            return
            
        cache_name = getattr(self, 'cache_name', self.__class__.__name__.lower())
        
        if pattern:
            # Pattern-based invalidation
            logger.info(f"Invalidating cache entries matching pattern: {pattern}")
            # This would require cache backend support
        else:
            # Clear entire cache
            await self.cache_manager.clear(cache_name)
            logger.info(f"Cleared cache: {cache_name}")
            
    async def warm_cache(self, keys: List[str]) -> None:
        """Warm cache with specific keys"""
        if not self._cache_enabled:
            return
            
        logger.info(f"Warming cache with {len(keys)} keys")
        # Implementation depends on specific use case


class EventMixin:
    """
    Provides event publishing functionality.
    
    Common pattern across processors, engines, and services
    """
    
    def __init__(self, *args, event_bus: Optional[EventBus] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.event_bus = event_bus
        self._event_enabled = event_bus is not None
        
    async def publish_event(self, event_type: str, data: Dict[str, Any], 
                          metadata: Optional[Dict[str, Any]] = None) -> None:
        """Publish an event"""
        if not self._event_enabled:
            return
            
        event = Event(
            id=str(uuid.uuid4()),
            type=event_type,
            source=self.__class__.__name__,
            timestamp=datetime.utcnow(),
            data=data,
            metadata=metadata or {}
        )
        
        await self.event_bus.publish(event)
        logger.debug(f"Published event: {event_type}")
        
    async def publish_lifecycle_event(self, lifecycle_stage: str) -> None:
        """Publish lifecycle event"""
        await self.publish_event(
            f"{self.__class__.__name__.lower()}.{lifecycle_stage}",
            {
                "component": self.__class__.__name__,
                "stage": lifecycle_stage,
                "timestamp": datetime.utcnow().isoformat()
            }
        )


class VaultConsulMixin:
    """
    Provides Vault and Consul integration.
    
    Common pattern across services and clients
    """
    
    def __init__(self, *args, 
                 vault_client: Optional[VaultClient] = None,
                 consul_client: Optional[ConsulClient] = None,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.vault_client = vault_client
        self.consul_client = consul_client
        self._secrets_cache: Dict[str, Tuple[Any, datetime]] = {}
        self._config_cache: Dict[str, Tuple[Any, datetime]] = {}
        
    async def get_secret(self, path: str, key: Optional[str] = None,
                        cache_ttl: timedelta = timedelta(minutes=5)) -> Any:
        """Get secret from Vault with caching"""
        cache_key = f"{path}:{key}" if key else path
        
        # Check cache
        if cache_key in self._secrets_cache:
            value, expiry = self._secrets_cache[cache_key]
            if datetime.utcnow() < expiry:
                return value
                
        # Fetch from Vault
        if not self.vault_client:
            raise RuntimeError("Vault client not configured")
            
        secret_data = await self.vault_client.read_secret(path)
        value = secret_data.get(key) if key else secret_data
        
        # Cache the result
        self._secrets_cache[cache_key] = (value, datetime.utcnow() + cache_ttl)
        
        return value
        
    async def get_config(self, key: str, default: Any = None,
                        cache_ttl: timedelta = timedelta(minutes=1)) -> Any:
        """Get configuration from Consul with caching"""
        # Check cache
        if key in self._config_cache:
            value, expiry = self._config_cache[key]
            if datetime.utcnow() < expiry:
                return value
                
        # Fetch from Consul
        if not self.consul_client:
            return default
            
        value = await self.consul_client.get_value(key)
        if value is None:
            value = default
            
        # Cache the result
        if value is not None:
            self._config_cache[key] = (value, datetime.utcnow() + cache_ttl)
            
        return value
        
    async def register_service(self, name: str, port: int, 
                             tags: Optional[List[str]] = None,
                             health_check_interval: str = "10s") -> None:
        """Register service with Consul"""
        if not self.consul_client:
            logger.warning("Consul client not configured, skipping service registration")
            return
            
        await self.consul_client.register_service(
            name=name,
            service_id=f"{name}-{uuid.uuid4().hex[:8]}",
            port=port,
            tags=tags or [],
            check={
                "http": f"http://localhost:{port}/health",
                "interval": health_check_interval
            }
        )
        logger.info(f"Registered service {name} with Consul")


class ResilienceMixin:
    """
    Provides resilience patterns.
    
    Consolidates retry, circuit breaker, and timeout functionality
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._circuit_breakers: Dict[str, CircuitBreakerPattern] = {}
        
    def get_circuit_breaker(self, name: str) -> CircuitBreakerPattern:
        """Get or create circuit breaker for operation"""
        if name not in self._circuit_breakers:
            from ..core.patterns.resilience import CircuitBreakerConfig
            config = CircuitBreakerConfig(
                failure_threshold=5,
                recovery_timeout=timedelta(seconds=60),
                success_threshold=2
            )
            self._circuit_breakers[name] = CircuitBreakerPattern(config)
        return self._circuit_breakers[name]
        
    @retry(RetryConfig(max_attempts=3, initial_delay=1.0))
    async def retry_operation(self, operation: Callable, *args, **kwargs) -> Any:
        """Execute operation with retry"""
        return await operation(*args, **kwargs)
        
    async def execute_with_circuit_breaker(self, name: str, operation: Callable, 
                                         *args, **kwargs) -> Any:
        """Execute operation with circuit breaker"""
        breaker = self.get_circuit_breaker(name)
        return await breaker.execute_async(operation, *args, **kwargs)


class MonitoringMixin(MetricsMixin, EventMixin):
    """
    Comprehensive monitoring functionality.
    
    Combines metrics and events with health checking
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._health_checks: Dict[str, Callable] = {}
        self._health_status: Dict[str, bool] = {}
        
    def register_health_check(self, name: str, check_func: Callable) -> None:
        """Register a health check"""
        self._health_checks[name] = check_func
        
    async def check_health(self) -> Dict[str, Any]:
        """Run all health checks"""
        results = {
            "status": "healthy",
            "checks": {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        for name, check_func in self._health_checks.items():
            try:
                if asyncio.iscoroutinefunction(check_func):
                    result = await check_func()
                else:
                    result = check_func()
                    
                self._health_status[name] = result
                results["checks"][name] = {
                    "status": "healthy" if result else "unhealthy",
                    "result": result
                }
            except Exception as e:
                self._health_status[name] = False
                results["checks"][name] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                results["status"] = "unhealthy"
                
        return results


class ConfigurationMixin:
    """
    Provides configuration management.
    
    Common pattern for handling configuration across components
    """
    
    def __init__(self, *args, config: Optional[Dict[str, Any]] = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._config = config or {}
        self._config_validators: List[Callable] = []
        
    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self._config.get(key, default)
        
    def set_config(self, key: str, value: Any) -> None:
        """Set configuration value"""
        self._config[key] = value
        self._validate_config()
        
    def update_config(self, config: Dict[str, Any]) -> None:
        """Update configuration"""
        self._config.update(config)
        self._validate_config()
        
    def register_config_validator(self, validator: Callable) -> None:
        """Register configuration validator"""
        self._config_validators.append(validator)
        
    def _validate_config(self) -> None:
        """Validate configuration"""
        for validator in self._config_validators:
            validator(self._config)


class StateMixin:
    """
    Provides state management functionality.
    
    Common pattern for managing component state
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._state: Dict[str, Any] = {}
        self._state_lock = threading.Lock()
        self._state_listeners: Dict[str, List[Callable]] = {}
        
    def get_state(self, key: str, default: Any = None) -> Any:
        """Get state value"""
        with self._state_lock:
            return self._state.get(key, default)
            
    def set_state(self, key: str, value: Any) -> None:
        """Set state value"""
        with self._state_lock:
            old_value = self._state.get(key)
            self._state[key] = value
            
        # Notify listeners
        if key in self._state_listeners:
            for listener in self._state_listeners[key]:
                try:
                    listener(old_value, value)
                except Exception as e:
                    logger.error(f"Error in state listener: {e}")
                    
    def register_state_listener(self, key: str, listener: Callable) -> None:
        """Register state change listener"""
        if key not in self._state_listeners:
            self._state_listeners[key] = []
        self._state_listeners[key].append(listener)
        
    def get_full_state(self) -> Dict[str, Any]:
        """Get full state snapshot"""
        with self._state_lock:
            return self._state.copy()


class ResourceMixin:
    """
    Provides resource management functionality.
    
    Common pattern for managing resources (memory, CPU, connections, etc.)
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._resources: Dict[str, Any] = {}
        self._resource_limits: Dict[str, Any] = {}
        self._resource_lock = asyncio.Lock()
        
    async def acquire_resource(self, name: str, amount: int = 1) -> bool:
        """Acquire resource"""
        async with self._resource_lock:
            current = self._resources.get(name, 0)
            limit = self._resource_limits.get(name)
            
            if limit and current + amount > limit:
                return False
                
            self._resources[name] = current + amount
            return True
            
    async def release_resource(self, name: str, amount: int = 1) -> None:
        """Release resource"""
        async with self._resource_lock:
            current = self._resources.get(name, 0)
            self._resources[name] = max(0, current - amount)
            
    def set_resource_limit(self, name: str, limit: int) -> None:
        """Set resource limit"""
        self._resource_limits[name] = limit
        
    @asynccontextmanager
    async def resource_context(self, name: str, amount: int = 1):
        """Context manager for resource acquisition"""
        acquired = await self.acquire_resource(name, amount)
        if not acquired:
            raise RuntimeError(f"Failed to acquire resource: {name}")
            
        try:
            yield
        finally:
            await self.release_resource(name, amount)


# Composite mixins that combine multiple functionalities

class ServiceMixin(LifecycleMixin, MonitoringMixin, VaultConsulMixin, 
                  ConfigurationMixin, ResilienceMixin):
    """Composite mixin for services"""
    pass


class ProcessorMixin(LifecycleMixin, MetricsMixin, CachingMixin, 
                    EventMixin, StateMixin, ResourceMixin):
    """Composite mixin for processors"""
    pass


class ClientMixin(MetricsMixin, CachingMixin, ResilienceMixin, 
                 VaultConsulMixin):
    """Composite mixin for clients"""
    pass


# Export all mixins
__all__ = [
    # Base mixins
    'LifecycleMixin',
    'MetricsMixin',
    'CachingMixin',
    'EventMixin',
    'VaultConsulMixin',
    'ResilienceMixin',
    'MonitoringMixin',
    'ConfigurationMixin',
    'StateMixin',
    'ResourceMixin',
    
    # Composite mixins
    'ServiceMixin',
    'ProcessorMixin',
    'ClientMixin'
] 