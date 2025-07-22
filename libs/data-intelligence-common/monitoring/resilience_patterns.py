"""
Resilience Patterns for DataIntelligence Services
Includes circuit breakers, rate limiting, retries, bulkheads, and timeouts
"""

import asyncio
import time
import logging
from typing import Dict, Any, Optional, Callable, Union, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, defaultdict
import threading
from functools import wraps
import random
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class RateLimitStrategy(Enum):
    """Rate limiting strategies"""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker"""
    failure_threshold: int = 5
    recovery_timeout: timedelta = timedelta(seconds=60)
    expected_exception: type = Exception
    success_threshold: int = 3
    half_open_requests: int = 1
    on_open: Optional[Callable] = None
    on_close: Optional[Callable] = None
    on_half_open: Optional[Callable] = None


@dataclass
class RateLimiterConfig:
    """Configuration for rate limiter"""
    requests: int = 100
    window: timedelta = timedelta(minutes=1)
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET
    burst_size: Optional[int] = None
    key_func: Optional[Callable] = None


@dataclass
class RetryConfig:
    """Configuration for retry logic"""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retryable_exceptions: Tuple[type, ...] = (Exception,)
    on_retry: Optional[Callable] = None


@dataclass
class BulkheadConfig:
    """Configuration for bulkhead isolation"""
    max_concurrent_calls: int = 10
    max_queue_size: int = 100
    timeout: timedelta = timedelta(seconds=30)


class CircuitBreaker:
    """Advanced circuit breaker implementation"""
    
    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.half_open_calls = 0
        self._lock = threading.Lock()
        
    def __call__(self, func: Callable) -> Callable:
        """Decorator usage"""
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await self.call_async(func, *args, **kwargs)
            
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
            
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
        
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker"""
        with self._lock:
            if not self._allow_request():
                raise CircuitBreakerError(f"Circuit breaker is {self.state.value}")
                
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
            
        except self.config.expected_exception as e:
            self._on_failure()
            raise
            
    async def call_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute async function with circuit breaker"""
        with self._lock:
            if not self._allow_request():
                raise CircuitBreakerError(f"Circuit breaker is {self.state.value}")
                
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
            
        except self.config.expected_exception as e:
            self._on_failure()
            raise
            
    def _allow_request(self) -> bool:
        """Check if request is allowed"""
        if self.state == CircuitState.CLOSED:
            return True
            
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._transition_to_half_open()
                return True
            return False
            
        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls < self.config.half_open_requests:
                self.half_open_calls += 1
                return True
            return False
            
        return False
        
    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt reset"""
        return (
            self.last_failure_time and
            datetime.utcnow() - self.last_failure_time >= self.config.recovery_timeout
        )
        
    def _on_success(self):
        """Handle successful call"""
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self._transition_to_closed()
            else:
                self.failure_count = 0
                
    def _on_failure(self):
        """Handle failed call"""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = datetime.utcnow()
            
            if self.state == CircuitState.HALF_OPEN:
                self._transition_to_open()
            elif self.failure_count >= self.config.failure_threshold:
                self._transition_to_open()
                
    def _transition_to_open(self):
        """Transition to open state"""
        self.state = CircuitState.OPEN
        self.half_open_calls = 0
        logger.warning(f"Circuit breaker opened after {self.failure_count} failures")
        
        if self.config.on_open:
            self.config.on_open()
            
    def _transition_to_closed(self):
        """Transition to closed state"""
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.half_open_calls = 0
        logger.info("Circuit breaker closed")
        
        if self.config.on_close:
            self.config.on_close()
            
    def _transition_to_half_open(self):
        """Transition to half-open state"""
        self.state = CircuitState.HALF_OPEN
        self.success_count = 0
        self.half_open_calls = 0
        logger.info("Circuit breaker half-open, testing recovery")
        
        if self.config.on_half_open:
            self.config.on_half_open()
            
    def get_state(self) -> Dict[str, Any]:
        """Get circuit breaker state"""
        return {
            "state": self.state.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None
        }


class RateLimiter:
    """Advanced rate limiter with multiple strategies"""
    
    def __init__(self, config: RateLimiterConfig):
        self.config = config
        self._buckets: Dict[str, Any] = {}
        self._lock = threading.Lock()
        
        # Initialize strategy
        if config.strategy == RateLimitStrategy.TOKEN_BUCKET:
            self._strategy = TokenBucketStrategy(config)
        elif config.strategy == RateLimitStrategy.SLIDING_WINDOW:
            self._strategy = SlidingWindowStrategy(config)
        elif config.strategy == RateLimitStrategy.FIXED_WINDOW:
            self._strategy = FixedWindowStrategy(config)
        else:
            self._strategy = LeakyBucketStrategy(config)
            
    def __call__(self, func: Callable) -> Callable:
        """Decorator usage"""
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            key = self._get_key(*args, **kwargs)
            if not await self.is_allowed_async(key):
                raise RateLimitExceeded(f"Rate limit exceeded for key: {key}")
            return await func(*args, **kwargs)
            
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            key = self._get_key(*args, **kwargs)
            if not self.is_allowed(key):
                raise RateLimitExceeded(f"Rate limit exceeded for key: {key}")
            return func(*args, **kwargs)
            
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
        
    def is_allowed(self, key: str = "default") -> bool:
        """Check if request is allowed"""
        with self._lock:
            return self._strategy.is_allowed(key)
            
    async def is_allowed_async(self, key: str = "default") -> bool:
        """Async check if request is allowed"""
        return self.is_allowed(key)
        
    def _get_key(self, *args, **kwargs) -> str:
        """Get rate limit key"""
        if self.config.key_func:
            return self.config.key_func(*args, **kwargs)
        return "default"
        
    def reset(self, key: str = "default"):
        """Reset rate limit for key"""
        with self._lock:
            self._strategy.reset(key)


class TokenBucketStrategy:
    """Token bucket rate limiting strategy"""
    
    def __init__(self, config: RateLimiterConfig):
        self.config = config
        self.buckets: Dict[str, Tuple[float, float]] = {}
        self.tokens_per_second = config.requests / config.window.total_seconds()
        self.burst_size = config.burst_size or config.requests
        
    def is_allowed(self, key: str) -> bool:
        """Check if request is allowed"""
        now = time.time()
        
        if key not in self.buckets:
            self.buckets[key] = (self.burst_size - 1, now)
            return True
            
        tokens, last_update = self.buckets[key]
        
        # Refill tokens
        elapsed = now - last_update
        tokens = min(self.burst_size, tokens + elapsed * self.tokens_per_second)
        
        if tokens >= 1:
            self.buckets[key] = (tokens - 1, now)
            return True
            
        self.buckets[key] = (tokens, now)
        return False
        
    def reset(self, key: str):
        """Reset bucket for key"""
        if key in self.buckets:
            del self.buckets[key]


class SlidingWindowStrategy:
    """Sliding window rate limiting strategy"""
    
    def __init__(self, config: RateLimiterConfig):
        self.config = config
        self.windows: Dict[str, deque] = defaultdict(deque)
        
    def is_allowed(self, key: str) -> bool:
        """Check if request is allowed"""
        now = time.time()
        window_start = now - self.config.window.total_seconds()
        
        # Remove old entries
        window = self.windows[key]
        while window and window[0] < window_start:
            window.popleft()
            
        if len(window) < self.config.requests:
            window.append(now)
            return True
            
        return False
        
    def reset(self, key: str):
        """Reset window for key"""
        if key in self.windows:
            del self.windows[key]


class FixedWindowStrategy:
    """Fixed window rate limiting strategy"""
    
    def __init__(self, config: RateLimiterConfig):
        self.config = config
        self.windows: Dict[str, Tuple[int, float]] = {}
        
    def is_allowed(self, key: str) -> bool:
        """Check if request is allowed"""
        now = time.time()
        window_size = self.config.window.total_seconds()
        current_window = int(now / window_size)
        
        if key not in self.windows:
            self.windows[key] = (1, current_window)
            return True
            
        count, window = self.windows[key]
        
        if window != current_window:
            self.windows[key] = (1, current_window)
            return True
            
        if count < self.config.requests:
            self.windows[key] = (count + 1, window)
            return True
            
        return False
        
    def reset(self, key: str):
        """Reset window for key"""
        if key in self.windows:
            del self.windows[key]


class LeakyBucketStrategy:
    """Leaky bucket rate limiting strategy"""
    
    def __init__(self, config: RateLimiterConfig):
        self.config = config
        self.buckets: Dict[str, Tuple[float, float]] = {}
        self.leak_rate = config.requests / config.window.total_seconds()
        
    def is_allowed(self, key: str) -> bool:
        """Check if request is allowed"""
        now = time.time()
        
        if key not in self.buckets:
            self.buckets[key] = (1, now)
            return True
            
        water_level, last_leak = self.buckets[key]
        
        # Leak water
        elapsed = now - last_leak
        water_level = max(0, water_level - elapsed * self.leak_rate)
        
        if water_level + 1 <= self.config.requests:
            self.buckets[key] = (water_level + 1, now)
            return True
            
        self.buckets[key] = (water_level, now)
        return False
        
    def reset(self, key: str):
        """Reset bucket for key"""
        if key in self.buckets:
            del self.buckets[key]


class RetryPolicy:
    """Advanced retry policy with exponential backoff and jitter"""
    
    def __init__(self, config: RetryConfig):
        self.config = config
        
    def __call__(self, func: Callable) -> Callable:
        """Decorator usage"""
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await self.execute_async(func, *args, **kwargs)
            
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            return self.execute(func, *args, **kwargs)
            
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
        
    def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry logic"""
        last_exception = None
        
        for attempt in range(self.config.max_attempts):
            try:
                return func(*args, **kwargs)
                
            except self.config.retryable_exceptions as e:
                last_exception = e
                
                if attempt < self.config.max_attempts - 1:
                    delay = self._get_delay(attempt)
                    
                    if self.config.on_retry:
                        self.config.on_retry(attempt + 1, delay, e)
                        
                    time.sleep(delay)
                    
        raise last_exception
        
    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute async function with retry logic"""
        last_exception = None
        
        for attempt in range(self.config.max_attempts):
            try:
                return await func(*args, **kwargs)
                
            except self.config.retryable_exceptions as e:
                last_exception = e
                
                if attempt < self.config.max_attempts - 1:
                    delay = self._get_delay(attempt)
                    
                    if self.config.on_retry:
                        if asyncio.iscoroutinefunction(self.config.on_retry):
                            await self.config.on_retry(attempt + 1, delay, e)
                        else:
                            self.config.on_retry(attempt + 1, delay, e)
                            
                    await asyncio.sleep(delay)
                    
        raise last_exception
        
    def _get_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt"""
        # Exponential backoff
        delay = min(
            self.config.base_delay * (self.config.exponential_base ** attempt),
            self.config.max_delay
        )
        
        # Add jitter
        if self.config.jitter:
            delay *= random.uniform(0.5, 1.5)
            
        return delay


class Bulkhead:
    """Bulkhead isolation pattern"""
    
    def __init__(self, config: BulkheadConfig):
        self.config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrent_calls)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=config.max_queue_size)
        self._active_calls = 0
        
    def __call__(self, func: Callable) -> Callable:
        """Decorator usage"""
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await self.execute_async(func, *args, **kwargs)
            
        if not asyncio.iscoroutinefunction(func):
            raise ValueError("Bulkhead only supports async functions")
            
        return async_wrapper
        
    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with bulkhead isolation"""
        # Try to acquire semaphore
        try:
            async with asyncio.timeout(self.config.timeout.total_seconds()):
                async with self._semaphore:
                    self._active_calls += 1
                    try:
                        return await func(*args, **kwargs)
                    finally:
                        self._active_calls -= 1
                        
        except asyncio.TimeoutError:
            raise BulkheadRejected("Bulkhead timeout exceeded")
            
    def get_stats(self) -> Dict[str, Any]:
        """Get bulkhead statistics"""
        return {
            "active_calls": self._active_calls,
            "available_permits": self.config.max_concurrent_calls - self._active_calls,
            "queue_size": self._queue.qsize() if hasattr(self._queue, 'qsize') else 0
        }


class Timeout:
    """Timeout pattern"""
    
    def __init__(self, timeout: Union[float, timedelta]):
        if isinstance(timeout, timedelta):
            timeout = timeout.total_seconds()
        self.timeout = timeout
        
    def __call__(self, func: Callable) -> Callable:
        """Decorator usage"""
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            async with asyncio.timeout(self.timeout):
                return await func(*args, **kwargs)
                
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # For sync functions, we need to run in thread with timeout
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(func, *args, **kwargs)
                try:
                    return future.result(timeout=self.timeout)
                except concurrent.futures.TimeoutError:
                    raise TimeoutError(f"Operation timed out after {self.timeout} seconds")
                    
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper


class ResilienceManager:
    """Manages multiple resilience patterns"""
    
    def __init__(self):
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.rate_limiters: Dict[str, RateLimiter] = {}
        self.bulkheads: Dict[str, Bulkhead] = {}
        
    def get_circuit_breaker(self, name: str, 
                           config: Optional[CircuitBreakerConfig] = None) -> CircuitBreaker:
        """Get or create circuit breaker"""
        if name not in self.circuit_breakers:
            if not config:
                config = CircuitBreakerConfig()
            self.circuit_breakers[name] = CircuitBreaker(config)
        return self.circuit_breakers[name]
        
    def get_rate_limiter(self, name: str,
                        config: Optional[RateLimiterConfig] = None) -> RateLimiter:
        """Get or create rate limiter"""
        if name not in self.rate_limiters:
            if not config:
                config = RateLimiterConfig()
            self.rate_limiters[name] = RateLimiter(config)
        return self.rate_limiters[name]
        
    def get_bulkhead(self, name: str,
                    config: Optional[BulkheadConfig] = None) -> Bulkhead:
        """Get or create bulkhead"""
        if name not in self.bulkheads:
            if not config:
                config = BulkheadConfig()
            self.bulkheads[name] = Bulkhead(config)
        return self.bulkheads[name]
        
    def with_resilience(self,
                       circuit_breaker: Optional[str] = None,
                       rate_limiter: Optional[str] = None,
                       retry: Optional[RetryConfig] = None,
                       bulkhead: Optional[str] = None,
                       timeout: Optional[Union[float, timedelta]] = None):
        """Decorator to apply multiple resilience patterns"""
        def decorator(func):
            wrapped = func
            
            # Apply patterns in order
            if timeout:
                wrapped = Timeout(timeout)(wrapped)
                
            if retry:
                wrapped = RetryPolicy(retry)(wrapped)
                
            if bulkhead:
                wrapped = self.get_bulkhead(bulkhead)(wrapped)
                
            if rate_limiter:
                wrapped = self.get_rate_limiter(rate_limiter)(wrapped)
                
            if circuit_breaker:
                wrapped = self.get_circuit_breaker(circuit_breaker)(wrapped)
                
            return wrapped
            
        return decorator
        
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of all resilience components"""
        return {
            "circuit_breakers": {
                name: cb.get_state() 
                for name, cb in self.circuit_breakers.items()
            },
            "bulkheads": {
                name: bh.get_stats()
                for name, bh in self.bulkheads.items()
            }
        }


# Exceptions
class CircuitBreakerError(Exception):
    """Circuit breaker is open"""
    pass


class RateLimitExceeded(Exception):
    """Rate limit exceeded"""
    pass


class BulkheadRejected(Exception):
    """Bulkhead rejected request"""
    pass


# Global resilience manager
resilience_manager = ResilienceManager() 