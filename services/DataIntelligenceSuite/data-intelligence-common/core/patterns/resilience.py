"""
Resilience patterns for fault-tolerant systems.

Provides implementations of:
- Retry with exponential backoff
- Circuit breaker
- Bulkhead isolation
- Timeout handling
- Fallback mechanisms
- Rate limiting
- Adaptive concurrency

This module consolidates all resilience patterns from across the codebase.
"""

import asyncio
import functools
import time
import random
import threading
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import logging
from collections import deque
from contextlib import asynccontextmanager

from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open"""
    pass


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
    on_retry: Optional[Callable[[int, float, Exception], None]] = None
    
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
    half_open_requests: int = 1
    on_open: Optional[Callable[[], None]] = None
    on_close: Optional[Callable[[], None]] = None
    on_half_open: Optional[Callable[[], None]] = None
    
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
    fallback_function: Callable[..., Any]
    fallback_on: List[type] = field(default_factory=lambda: [Exception])
    
    def should_fallback(self, exception: Exception) -> bool:
        """Check if should use fallback for exception"""
        return any(isinstance(exception, exc_type) for exc_type in self.fallback_on)


@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""
    max_calls: int
    period: timedelta
    burst_size: Optional[int] = None


@dataclass
class ResilienceConfig:
    """Combined resilience configuration"""
    retry: Optional[RetryConfig] = None
    circuit_breaker: Optional[CircuitBreakerConfig] = None
    bulkhead: Optional[BulkheadConfig] = None
    timeout: Optional[TimeoutConfig] = None
    fallback: Optional[FallbackConfig] = None
    rate_limit: Optional[RateLimitConfig] = None


class RetryPattern:
    """
    Retry pattern implementation with exponential backoff.
    
    Features:
    - Configurable retry attempts
    - Exponential backoff with jitter
    - Custom retry conditions
    - Async and sync support
    - Retry callbacks
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
                
                if self.config.on_retry:
                    self.config.on_retry(attempt, delay, e)
                    
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
                
                if self.config.on_retry:
                    self.config.on_retry(attempt, delay, e)
                    
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
    - Thread-safe operation
    - State change callbacks
    """
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._state_changed_at = datetime.utcnow()
        self._half_open_calls = 0
        self._lock = threading.Lock()
        
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
            old_state = self._state
            logger.info(f"Circuit breaker state change: {old_state.value} -> {new_state.value}")
            self._state = new_state
            self._state_changed_at = datetime.utcnow()
            self._metrics["state_changes"] += 1
            
            # Reset counters based on state
            if new_state == CircuitState.CLOSED:
                self._failure_count = 0
                self._success_count = 0
                if self.config.on_close:
                    self.config.on_close()
            elif new_state == CircuitState.OPEN:
                if self.config.on_open:
                    self.config.on_open()
            elif new_state == CircuitState.HALF_OPEN:
                self._success_count = 0
                self._half_open_calls = 0
                if self.config.on_half_open:
                    self.config.on_half_open()
                
    def _should_attempt_reset(self) -> bool:
        """Check if should attempt reset from open state"""
        if self._state != CircuitState.OPEN:
            return False
            
        if self._last_failure_time:
            time_since_failure = datetime.utcnow() - self._last_failure_time
            return time_since_failure >= self.config.timeout
            
        return False
        
    def _allow_request(self) -> bool:
        """Check if request is allowed"""
        if self._state == CircuitState.CLOSED:
            return True
            
        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._transition_to(CircuitState.HALF_OPEN)
                return True
            return False
            
        if self._state == CircuitState.HALF_OPEN:
            if self._half_open_calls < self.config.half_open_requests:
                self._half_open_calls += 1
                return True
            return False
            
        return False
        
    def _on_success(self):
        """Handle successful call"""
        with self._lock:
            self._metrics["successes"] += 1
            
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
            else:
                self._failure_count = 0
                
    def _on_failure(self, exception: Exception):
        """Handle failed call"""
        with self._lock:
            if not self.config.should_count_failure(exception):
                return
                
            self._metrics["failures"] += 1
            self._failure_count += 1
            self._last_failure_time = datetime.utcnow()
            
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif self._failure_count >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)
        
    async def execute_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute async function with circuit breaker"""
        self._metrics["calls"] += 1
        
        with self._lock:
            if not self._allow_request():
                self._metrics["rejections"] += 1
                raise CircuitBreakerError(f"Circuit breaker is {self._state.value}")
            
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
        
        with self._lock:
            if not self._allow_request():
                self._metrics["rejections"] += 1
                raise CircuitBreakerError(f"Circuit breaker is {self._state.value}")
            
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure(e)
            raise
            
    def get_metrics(self) -> Dict[str, Any]:
        """Get circuit breaker metrics"""
        with self._lock:
            return {
                **self._metrics.copy(),
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count
            }
            
    def get_state(self) -> Dict[str, Any]:
        """Get circuit breaker state info"""
        with self._lock:
            return {
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time.isoformat() if self._last_failure_time else None,
                "state_changed_at": self._state_changed_at.isoformat()
            }


class BulkheadPattern:
    """
    Bulkhead isolation pattern implementation.
    
    Features:
    - Limits concurrent executions
    - Queue management
    - Timeout handling
    - Async support
    """
    
    def __init__(self, config: BulkheadConfig):
        self.config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrent)
        self._queue_size = 0
        self._queue_lock = asyncio.Lock()
        
        self._metrics = {
            "executions": 0,
            "rejections": 0,
            "timeouts": 0,
            "active": 0
        }
        
    async def execute_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute async function with bulkhead isolation"""
        # Check queue size
        async with self._queue_lock:
            if self._queue_size >= self.config.max_queue_size:
                self._metrics["rejections"] += 1
                raise Exception(f"Bulkhead queue full: {self._queue_size}/{self.config.max_queue_size}")
            self._queue_size += 1
            
        try:
            # Acquire semaphore with timeout
            try:
                async with asyncio.timeout(self.config.timeout.total_seconds()):
                    async with self._semaphore:
                        self._metrics["active"] += 1
                        self._metrics["executions"] += 1
                        
                        try:
                            return await func(*args, **kwargs)
                        finally:
                            self._metrics["active"] -= 1
                            
            except asyncio.TimeoutError:
                self._metrics["timeouts"] += 1
                raise
                
        finally:
            async with self._queue_lock:
                self._queue_size -= 1
                
    def get_metrics(self) -> Dict[str, int]:
        """Get bulkhead metrics"""
        return self._metrics.copy()


class TimeoutPattern:
    """
    Timeout pattern implementation.
    
    Features:
    - Configurable timeout
    - Cancellation support
    - Async support
    """
    
    def __init__(self, config: TimeoutConfig):
        self.config = config
        self._metrics = {
            "executions": 0,
            "timeouts": 0
        }
        
    async def execute_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute async function with timeout"""
        self._metrics["executions"] += 1
        
        try:
            async with asyncio.timeout(self.config.timeout.total_seconds()):
                return await func(*args, **kwargs)
        except asyncio.TimeoutError:
            self._metrics["timeouts"] += 1
            if self.config.cancel_on_timeout:
                raise
            return None
            
    def get_metrics(self) -> Dict[str, int]:
        """Get timeout metrics"""
        return self._metrics.copy()


class FallbackPattern:
    """
    Fallback pattern implementation.
    
    Features:
    - Configurable fallback function
    - Exception-based fallback
    - Async support
    """
    
    def __init__(self, config: FallbackConfig):
        self.config = config
        self._metrics = {
            "executions": 0,
            "fallbacks": 0
        }
        
    async def execute_async(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute async function with fallback"""
        self._metrics["executions"] += 1
        
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            if self.config.should_fallback(e):
                self._metrics["fallbacks"] += 1
                if asyncio.iscoroutinefunction(self.config.fallback_function):
                    return await self.config.fallback_function(*args, **kwargs)
                else:
                    return self.config.fallback_function(*args, **kwargs)
            raise
            
    def execute(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute sync function with fallback"""
        self._metrics["executions"] += 1
        
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if self.config.should_fallback(e):
                self._metrics["fallbacks"] += 1
                return self.config.fallback_function(*args, **kwargs)
            raise
            
    def get_metrics(self) -> Dict[str, int]:
        """Get fallback metrics"""
        return self._metrics.copy()


class RateLimiter:
    """
    Rate limiting implementation with token bucket algorithm.
    
    Features:
    - Configurable rate and burst
    - Async support
    - Per-key limiting
    """
    
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._buckets: Dict[str, Tuple[float, datetime]] = {}
        self._lock = asyncio.Lock()
        
        # Calculate rate
        self._rate = config.max_calls / config.period.total_seconds()
        self._burst = config.burst_size or config.max_calls
        
    async def is_allowed(self, key: str = "default") -> bool:
        """Check if request is allowed"""
        async with self._lock:
            now = datetime.utcnow()
            
            if key in self._buckets:
                tokens, last_update = self._buckets[key]
                
                # Calculate tokens accumulated
                elapsed = (now - last_update).total_seconds()
                tokens = min(self._burst, tokens + elapsed * self._rate)
            else:
                tokens = self._burst
                
            if tokens >= 1:
                # Consume token
                self._buckets[key] = (tokens - 1, now)
                return True
            else:
                # Update timestamp without consuming
                self._buckets[key] = (tokens, now)
                return False


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
        self.rate_limiter = RateLimiter(config.rate_limit) if config.rate_limit else None
        
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
        async def async_wrapper(*args, **kwargs):
            return await pattern.execute_async(func, *args, **kwargs)
            
        return async_wrapper
    return decorator


def timeout(config: Optional[TimeoutConfig] = None):
    """Timeout decorator"""
    def decorator(func):
        pattern = TimeoutPattern(config or TimeoutConfig(timeout=timedelta(seconds=30)))
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await pattern.execute_async(func, *args, **kwargs)
            
        return async_wrapper
    return decorator


def fallback(fallback_func: Callable, exceptions: List[type] = None):
    """Fallback decorator"""
    def decorator(func):
        config = FallbackConfig(
            fallback_function=fallback_func,
            fallback_on=exceptions or [Exception]
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


def rate_limit(max_calls: int, period: timedelta):
    """Rate limiting decorator"""
    def decorator(func):
        config = RateLimitConfig(max_calls=max_calls, period=period)
        limiter = RateLimiter(config)
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Extract key from first argument if it has an 'id' attribute
            key = "default"
            if args and hasattr(args[0], 'id'):
                key = str(args[0].id)
                
            if not await limiter.is_allowed(key):
                raise Exception(f"Rate limit exceeded for key: {key}")
                
            return await func(*args, **kwargs)
            
        return async_wrapper
    return decorator


def resilient(
    retry_attempts: int = 3,
    circuit_breaker_failures: int = 5,
    timeout_seconds: int = 30,
    fallback_func: Optional[Callable] = None
):
    """Combined resilience decorator with sensible defaults"""
    def decorator(func):
        config = ResilienceConfig(
            retry=RetryConfig(max_attempts=retry_attempts),
            circuit_breaker=CircuitBreakerConfig(failure_threshold=circuit_breaker_failures),
            timeout=TimeoutConfig(timeout=timedelta(seconds=timeout_seconds)),
            fallback=FallbackConfig(fallback_function=fallback_func) if fallback_func else None
        )
        policy = ResiliencePolicy(config)
        
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await policy.execute_async(func, *args, **kwargs)
            
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            return policy.execute(func, *args, **kwargs)
            
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


# Export all public classes and decorators
__all__ = [
    # States and Errors
    "CircuitState",
    "CircuitBreakerError",
    
    # Configurations
    "RetryConfig",
    "CircuitBreakerConfig", 
    "BulkheadConfig",
    "TimeoutConfig",
    "FallbackConfig",
    "RateLimitConfig",
    "ResilienceConfig",
    
    # Patterns
    "RetryPattern",
    "CircuitBreakerPattern",
    "BulkheadPattern",
    "TimeoutPattern",
    "FallbackPattern",
    "RateLimiter",
    "ResiliencePolicy",
    
    # Decorators
    "retry",
    "circuit_breaker",
    "bulkhead",
    "timeout",
    "fallback",
    "rate_limit",
    "resilient"
] 