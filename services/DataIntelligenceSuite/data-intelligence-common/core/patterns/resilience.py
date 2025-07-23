"""
Resilience patterns for fault-tolerant systems.

Provides implementations of:
- Retry with exponential backoff
- Circuit breaker
- Bulkhead isolation
- Timeout handling
- Fallback mechanisms
"""

import asyncio
import functools
import time
import random
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import logging
from collections import deque

from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RetryConfig:
    """Retry pattern configuration"""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retry_on: List[type] = field(default_factory=lambda: [Exception])
    retry_condition: Optional[Callable[[Exception], bool]] = None
    
    def should_retry(self, exception: Exception) -> bool:
        """Check if should retry for exception"""
        if self.retry_condition:
            return self.retry_condition(exception)
        return any(isinstance(exception, exc_type) for exc_type in self.retry_on)
        
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for attempt"""
        delay = min(
            self.initial_delay * (self.exponential_base ** (attempt - 1)),
            self.max_delay
        )
        
        if self.jitter:
            # Add jitter to prevent thundering herd
            delay *= (0.5 + random.random())
            
        return delay


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: timedelta = timedelta(seconds=60)
    expected_exception: type = Exception
    exclude_exceptions: List[type] = field(default_factory=list)
    
    def should_count_failure(self, exception: Exception) -> bool:
        """Check if exception should count as failure"""
        if any(isinstance(exception, exc_type) for exc_type in self.exclude_exceptions):
            return False
        return isinstance(exception, self.expected_exception)


@dataclass
class BulkheadConfig:
    """Bulkhead isolation configuration"""
    max_concurrent: int = 10
    max_queue_size: int = 100
    timeout: timedelta = timedelta(seconds=30)


@dataclass
class TimeoutConfig:
    """Timeout configuration"""
    timeout: timedelta
    cancel_on_timeout: bool = True


@dataclass
class FallbackConfig:
    """Fallback configuration"""
    fallback_function: Optional[Callable] = None
    fallback_value: Any = None
    cache_result: bool = False
    cache_ttl: timedelta = timedelta(minutes=5)


@dataclass
class ResilienceConfig:
    """Combined resilience configuration"""
    retry: Optional[RetryConfig] = None
    circuit_breaker: Optional[CircuitBreakerConfig] = None
    bulkhead: Optional[BulkheadConfig] = None
    timeout: Optional[TimeoutConfig] = None
    fallback: Optional[FallbackConfig] = None


class RetryPattern:
    """
    Retry pattern implementation with exponential backoff.
    
    Features:
    - Configurable retry attempts
    - Exponential backoff with jitter
    - Custom retry conditions
    - Async and sync support
    """
    
    def __init__(self, config: RetryConfig):
        self.config = config
        self._retry_metrics = {
            "attempts": 0,
            "successes": 0,
            "failures": 0
        }
        
    async def execute_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute async function with retry"""
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                self._retry_metrics["attempts"] += 1
                result = await func(*args, **kwargs)
                self._retry_metrics["successes"] += 1
                return result
                
            except Exception as e:
                last_exception = e
                
                if not self.config.should_retry(e) or attempt >= self.config.max_attempts:
                    self._retry_metrics["failures"] += 1
                    raise
                    
                delay = self.config.calculate_delay(attempt)
                logger.warning(
                    f"Retry attempt {attempt}/{self.config.max_attempts} "
                    f"failed: {e}. Retrying in {delay:.2f}s"
                )
                
                await asyncio.sleep(delay)
                
        raise last_exception
        
    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute sync function with retry"""
        last_exception = None
        
        for attempt in range(1, self.config.max_attempts + 1):
            try:
                self._retry_metrics["attempts"] += 1
                result = func(*args, **kwargs)
                self._retry_metrics["successes"] += 1
                return result
                
            except Exception as e:
                last_exception = e
                
                if not self.config.should_retry(e) or attempt >= self.config.max_attempts:
                    self._retry_metrics["failures"] += 1
                    raise
                    
                delay = self.config.calculate_delay(attempt)
                logger.warning(
                    f"Retry attempt {attempt}/{self.config.max_attempts} "
                    f"failed: {e}. Retrying in {delay:.2f}s"
                )
                
                time.sleep(delay)
                
        raise last_exception
        
    def get_metrics(self) -> Dict[str, int]:
        """Get retry metrics"""
        return self._retry_metrics.copy()


class CircuitBreakerPattern:
    """
    Circuit breaker pattern implementation.
    
    Features:
    - Three states: Closed, Open, Half-Open
    - Automatic recovery
    - Failure counting
    - Custom failure conditions
    """
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._state_changed_at = datetime.utcnow()
        
        self._metrics = {
            "calls": 0,
            "successes": 0,
            "failures": 0,
            "rejections": 0,
            "state_changes": 0
        }
        
    @property
    def state(self) -> CircuitState:
        """Get current state"""
        return self._state
        
    @property
    def is_open(self) -> bool:
        """Check if circuit is open"""
        return self._state == CircuitState.OPEN
        
    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed"""
        return self._state == CircuitState.CLOSED
        
    def _transition_to(self, new_state: CircuitState):
        """Transition to new state"""
        if self._state != new_state:
            logger.info(f"Circuit breaker state change: {self._state.value} -> {new_state.value}")
            self._state = new_state
            self._state_changed_at = datetime.utcnow()
            self._metrics["state_changes"] += 1
            
            if new_state == CircuitState.CLOSED:
                self._failure_count = 0
                self._success_count = 0
                
    def _should_attempt_reset(self) -> bool:
        """Check if should attempt reset from open state"""
        if self._state != CircuitState.OPEN:
            return False
            
        if self._last_failure_time:
            time_since_failure = datetime.utcnow() - self._last_failure_time
            return time_since_failure >= self.config.timeout
            
        return False
        
    async def execute_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute async function with circuit breaker"""
        self._metrics["calls"] += 1
        
        # Check if should attempt reset
        if self._should_attempt_reset():
            self._transition_to(CircuitState.HALF_OPEN)
            
        # Check if circuit is open
        if self._state == CircuitState.OPEN:
            self._metrics["rejections"] += 1
            raise Exception(f"Circuit breaker is OPEN")
            
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure(e)
            raise
            
    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute sync function with circuit breaker"""
        self._metrics["calls"] += 1
        
        # Check if should attempt reset
        if self._should_attempt_reset():
            self._transition_to(CircuitState.HALF_OPEN)
            
        # Check if circuit is open
        if self._state == CircuitState.OPEN:
            self._metrics["rejections"] += 1
            raise Exception(f"Circuit breaker is OPEN")
            
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure(e)
            raise
            
    def _on_success(self):
        """Handle successful execution"""
        self._metrics["successes"] += 1
        
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            
            if self._success_count >= self.config.success_threshold:
                self._transition_to(CircuitState.CLOSED)
                
        elif self._state == CircuitState.CLOSED:
            self._failure_count = 0
            
    def _on_failure(self, exception: Exception):
        """Handle failed execution"""
        if not self.config.should_count_failure(exception):
            return
            
        self._metrics["failures"] += 1
        self._last_failure_time = datetime.utcnow()
        
        if self._state == CircuitState.HALF_OPEN:
            self._transition_to(CircuitState.OPEN)
            
        elif self._state == CircuitState.CLOSED:
            self._failure_count += 1
            
            if self._failure_count >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)
                
    def get_metrics(self) -> Dict[str, Any]:
        """Get circuit breaker metrics"""
        return {
            **self._metrics,
            "current_state": self._state.value,
            "failure_count": self._failure_count,
            "time_in_state": (datetime.utcnow() - self._state_changed_at).total_seconds()
        }


class BulkheadPattern:
    """
    Bulkhead isolation pattern implementation.
    
    Features:
    - Limits concurrent executions
    - Queue for pending executions
    - Timeout handling
    - Resource isolation
    """
    
    def __init__(self, config: BulkheadConfig):
        self.config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrent)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=config.max_queue_size)
        self._active_count = 0
        
        self._metrics = {
            "executions": 0,
            "rejections": 0,
            "timeouts": 0,
            "queue_full": 0
        }
        
    async def execute_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute async function with bulkhead isolation"""
        self._metrics["executions"] += 1
        
        # Try to acquire semaphore
        try:
            async with asyncio.wait_for(
                self._semaphore.acquire(),
                timeout=self.config.timeout.total_seconds()
            ):
                self._active_count += 1
                
                try:
                    return await func(*args, **kwargs)
                finally:
                    self._active_count -= 1
                    self._semaphore.release()
                    
        except asyncio.TimeoutError:
            self._metrics["timeouts"] += 1
            raise TimeoutError(f"Bulkhead timeout after {self.config.timeout}")
            
        except asyncio.QueueFull:
            self._metrics["queue_full"] += 1
            self._metrics["rejections"] += 1
            raise Exception("Bulkhead queue is full")
            
    def get_metrics(self) -> Dict[str, Any]:
        """Get bulkhead metrics"""
        return {
            **self._metrics,
            "active_count": self._active_count,
            "available_permits": self.config.max_concurrent - self._active_count
        }


class TimeoutPattern:
    """
    Timeout pattern implementation.
    
    Features:
    - Configurable timeout
    - Cancellation on timeout
    - Async support
    """
    
    def __init__(self, config: TimeoutConfig):
        self.config = config
        self._metrics = {
            "executions": 0,
            "timeouts": 0,
            "completions": 0
        }
        
    async def execute_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute async function with timeout"""
        self._metrics["executions"] += 1
        
        try:
            result = await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=self.config.timeout.total_seconds()
            )
            self._metrics["completions"] += 1
            return result
            
        except asyncio.TimeoutError:
            self._metrics["timeouts"] += 1
            raise TimeoutError(f"Operation timed out after {self.config.timeout}")
            
    def get_metrics(self) -> Dict[str, int]:
        """Get timeout metrics"""
        return self._metrics.copy()


class FallbackPattern:
    """
    Fallback pattern implementation.
    
    Features:
    - Fallback function or value
    - Result caching
    - Error handling
    """
    
    def __init__(self, config: FallbackConfig):
        self.config = config
        self._cache: Optional[Tuple[Any, datetime]] = None
        self._metrics = {
            "executions": 0,
            "fallbacks": 0,
            "cache_hits": 0
        }
        
    async def execute_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute async function with fallback"""
        self._metrics["executions"] += 1
        
        # Check cache
        if self.config.cache_result and self._cache:
            cached_value, cached_at = self._cache
            if datetime.utcnow() - cached_at < self.config.cache_ttl:
                self._metrics["cache_hits"] += 1
                return cached_value
                
        try:
            result = await func(*args, **kwargs)
            
            # Cache result
            if self.config.cache_result:
                self._cache = (result, datetime.utcnow())
                
            return result
            
        except Exception as e:
            logger.warning(f"Primary function failed, using fallback: {e}")
            self._metrics["fallbacks"] += 1
            
            if self.config.fallback_function:
                if asyncio.iscoroutinefunction(self.config.fallback_function):
                    return await self.config.fallback_function(*args, **kwargs)
                else:
                    return self.config.fallback_function(*args, **kwargs)
            else:
                return self.config.fallback_value
                
    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute sync function with fallback"""
        self._metrics["executions"] += 1
        
        # Check cache
        if self.config.cache_result and self._cache:
            cached_value, cached_at = self._cache
            if datetime.utcnow() - cached_at < self.config.cache_ttl:
                self._metrics["cache_hits"] += 1
                return cached_value
                
        try:
            result = func(*args, **kwargs)
            
            # Cache result
            if self.config.cache_result:
                self._cache = (result, datetime.utcnow())
                
            return result
            
        except Exception as e:
            logger.warning(f"Primary function failed, using fallback: {e}")
            self._metrics["fallbacks"] += 1
            
            if self.config.fallback_function:
                return self.config.fallback_function(*args, **kwargs)
            else:
                return self.config.fallback_value
                
    def get_metrics(self) -> Dict[str, int]:
        """Get fallback metrics"""
        return self._metrics.copy()


class ResiliencePolicy:
    """
    Combined resilience policy with multiple patterns.
    
    Features:
    - Combines retry, circuit breaker, bulkhead, timeout, and fallback
    - Configurable pattern order
    - Unified metrics
    """
    
    def __init__(self, config: ResilienceConfig):
        self.config = config
        
        # Initialize patterns
        self.retry = RetryPattern(config.retry) if config.retry else None
        self.circuit_breaker = CircuitBreakerPattern(config.circuit_breaker) if config.circuit_breaker else None
        self.bulkhead = BulkheadPattern(config.bulkhead) if config.bulkhead else None
        self.timeout = TimeoutPattern(config.timeout) if config.timeout else None
        self.fallback = FallbackPattern(config.fallback) if config.fallback else None
        
    async def execute_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute async function with resilience policy"""
        # Wrap function with patterns in order
        wrapped = func
        
        # Timeout wraps the innermost function
        if self.timeout:
            async def timeout_wrapper(*a, **kw):
                return await self.timeout.execute_async(wrapped, *a, **kw)
            wrapped = timeout_wrapper
            
        # Bulkhead wraps timeout
        if self.bulkhead:
            async def bulkhead_wrapper(*a, **kw):
                return await self.bulkhead.execute_async(wrapped, *a, **kw)
            wrapped = bulkhead_wrapper
            
        # Circuit breaker wraps bulkhead
        if self.circuit_breaker:
            async def circuit_wrapper(*a, **kw):
                return await self.circuit_breaker.execute_async(wrapped, *a, **kw)
            wrapped = circuit_wrapper
            
        # Retry wraps circuit breaker
        if self.retry:
            async def retry_wrapper(*a, **kw):
                return await self.retry.execute_async(wrapped, *a, **kw)
            wrapped = retry_wrapper
            
        # Fallback wraps everything
        if self.fallback:
            return await self.fallback.execute_async(wrapped, *args, **kwargs)
        else:
            return await wrapped(*args, **kwargs)
            
    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute sync function with resilience policy"""
        # Similar wrapping for sync functions
        wrapped = func
        
        if self.circuit_breaker:
            def circuit_wrapper(*a, **kw):
                return self.circuit_breaker.execute(wrapped, *a, **kw)
            wrapped = circuit_wrapper
            
        if self.retry:
            def retry_wrapper(*a, **kw):
                return self.retry.execute(wrapped, *a, **kw)
            wrapped = retry_wrapper
            
        if self.fallback:
            return self.fallback.execute(wrapped, *args, **kwargs)
        else:
            return wrapped(*args, **kwargs)
            
    def get_metrics(self) -> Dict[str, Any]:
        """Get combined metrics from all patterns"""
        metrics = {}
        
        if self.retry:
            metrics["retry"] = self.retry.get_metrics()
        if self.circuit_breaker:
            metrics["circuit_breaker"] = self.circuit_breaker.get_metrics()
        if self.bulkhead:
            metrics["bulkhead"] = self.bulkhead.get_metrics()
        if self.timeout:
            metrics["timeout"] = self.timeout.get_metrics()
        if self.fallback:
            metrics["fallback"] = self.fallback.get_metrics()
            
        return metrics


# Decorator functions for easy use

def retry(config: Optional[RetryConfig] = None):
    """Retry decorator"""
    def decorator(func):
        pattern = RetryPattern(config or RetryConfig())
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await pattern.execute_async(func, *args, **kwargs)
            
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            return pattern.execute(func, *args, **kwargs)
            
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


def circuit_breaker(config: Optional[CircuitBreakerConfig] = None):
    """Circuit breaker decorator"""
    def decorator(func):
        pattern = CircuitBreakerPattern(config or CircuitBreakerConfig())
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await pattern.execute_async(func, *args, **kwargs)
            
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            return pattern.execute(func, *args, **kwargs)
            
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


def bulkhead(config: Optional[BulkheadConfig] = None):
    """Bulkhead decorator"""
    def decorator(func):
        pattern = BulkheadPattern(config or BulkheadConfig())
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await pattern.execute_async(func, *args, **kwargs)
            
        return wrapper
    return decorator


def timeout(duration: Union[timedelta, float]):
    """Timeout decorator"""
    if isinstance(duration, (int, float)):
        duration = timedelta(seconds=duration)
        
    def decorator(func):
        pattern = TimeoutPattern(TimeoutConfig(timeout=duration))
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await pattern.execute_async(func, *args, **kwargs)
            
        return wrapper
    return decorator


def fallback(fallback_func: Optional[Callable] = None, fallback_value: Any = None):
    """Fallback decorator"""
    def decorator(func):
        config = FallbackConfig(
            fallback_function=fallback_func,
            fallback_value=fallback_value
        )
        pattern = FallbackPattern(config)
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await pattern.execute_async(func, *args, **kwargs)
            
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            return pattern.execute(func, *args, **kwargs)
            
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator 