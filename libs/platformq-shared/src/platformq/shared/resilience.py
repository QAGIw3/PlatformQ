"""
Resilience patterns for third-party integrations

This module provides reusable resilience patterns including:
- Circuit breaker for preventing cascading failures
- Rate limiting with adaptive backoff
- Connection pooling for HTTP clients
- Retry logic with jitter
- Metrics collection for monitoring
"""

import time
import random
import logging
from typing import Callable, Dict, Any, Optional, Union, TypeVar, Generic
from functools import wraps
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock, RLock
import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from collections import deque
import json

logger = logging.getLogger(__name__)

T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"  # Normal operation
    OPEN = "open"      # Failing, reject calls
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreakerError(Exception):
    """Raised when circuit breaker is open"""
    pass


class RateLimitExceeded(Exception):
    """Raised when rate limit is exceeded"""
    pass


class CircuitBreaker:
    """
    Circuit breaker pattern implementation
    
    Prevents cascading failures by failing fast when a service is down
    """
    
    def __init__(self, 
                 failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 expected_exception: type = Exception,
                 name: str = "CircuitBreaker"):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception
        self.name = name
        
        self._failure_count = 0
        self._last_failure_time = None
        self._state = CircuitState.CLOSED
        self._lock = RLock()
        
    @property
    def state(self) -> CircuitState:
        with self._lock:
            if self._state == CircuitState.OPEN:
                if self._last_failure_time and \
                   datetime.now() - self._last_failure_time > timedelta(seconds=self.recovery_timeout):
                    logger.info(f"Circuit breaker '{self.name}' transitioning to HALF_OPEN")
                    self._state = CircuitState.HALF_OPEN
                    self._failure_count = 0
            return self._state
    
    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        """Execute function with circuit breaker protection"""
        with self._lock:
            if self.state == CircuitState.OPEN:
                raise CircuitBreakerError(f"Circuit breaker '{self.name}' is OPEN")
                
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise
            
    def _on_success(self):
        """Handle successful call"""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                logger.info(f"Circuit breaker '{self.name}' transitioning to CLOSED")
                self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._last_failure_time = None
            
    def _on_failure(self):
        """Handle failed call"""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = datetime.now()
            
            if self._failure_count >= self.failure_threshold:
                logger.warning(f"Circuit breaker '{self.name}' transitioning to OPEN after {self._failure_count} failures")
                self._state = CircuitState.OPEN
                
    def reset(self):
        """Manually reset the circuit breaker"""
        with self._lock:
            self._failure_count = 0
            self._last_failure_time = None
            self._state = CircuitState.CLOSED
            logger.info(f"Circuit breaker '{self.name}' manually reset")


def circuit_breaker(failure_threshold: int = 5,
                    recovery_timeout: int = 60,
                    expected_exception: type = Exception,
                    name: str = None):
    """Decorator for applying circuit breaker pattern"""
    def decorator(func):
        breaker_name = name or f"{func.__module__}.{func.__name__}"
        breaker = CircuitBreaker(failure_threshold, recovery_timeout, expected_exception, breaker_name)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            return breaker.call(func, *args, **kwargs)
        
        wrapper.circuit_breaker = breaker
        return wrapper
    return decorator


class RateLimiter:
    """
    Token bucket rate limiter with adaptive backoff
    """
    
    def __init__(self, 
                 rate: float,  # Requests per second
                 burst: int = None,  # Max burst size
                 window_size: int = 60):  # Rolling window in seconds
        self.rate = rate
        self.burst = burst or int(rate * 2)
        self.window_size = window_size
        
        self._tokens = self.burst
        self._last_update = time.time()
        self._lock = Lock()
        
        # Track request times for rolling window
        self._request_times = deque()
        
    def acquire(self, tokens: int = 1, blocking: bool = True, timeout: float = None) -> bool:
        """
        Acquire tokens from the bucket
        
        Args:
            tokens: Number of tokens to acquire
            blocking: Whether to block until tokens available
            timeout: Max time to wait if blocking
            
        Returns:
            True if tokens acquired, False otherwise
        """
        start_time = time.time()
        
        while True:
            with self._lock:
                now = time.time()
                
                # Remove old requests outside the window
                cutoff = now - self.window_size
                while self._request_times and self._request_times[0] < cutoff:
                    self._request_times.popleft()
                
                # Refill tokens based on time elapsed
                elapsed = now - self._last_update
                self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
                self._last_update = now
                
                # Check if we can acquire tokens
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    self._request_times.append(now)
                    return True
                    
            if not blocking:
                return False
                
            # Calculate wait time
            wait_time = (tokens - self._tokens) / self.rate
            
            if timeout is not None:
                remaining = timeout - (time.time() - start_time)
                if remaining <= 0:
                    return False
                wait_time = min(wait_time, remaining)
                
            time.sleep(wait_time)
            
    def get_rate_limit_info(self) -> Dict[str, Any]:
        """Get current rate limit status"""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_size
            recent_requests = sum(1 for t in self._request_times if t > cutoff)
            
            return {
                "rate": self.rate,
                "burst": self.burst,
                "available_tokens": self._tokens,
                "recent_requests": recent_requests,
                "window_size": self.window_size
            }


def rate_limit(rate: float, burst: int = None, raise_on_limit: bool = True):
    """Decorator for rate limiting function calls"""
    limiter = RateLimiter(rate, burst)
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if not limiter.acquire(blocking=not raise_on_limit):
                raise RateLimitExceeded(f"Rate limit exceeded for {func.__name__}")
            return func(*args, **kwargs)
            
        wrapper.rate_limiter = limiter
        return wrapper
    return decorator


class AdaptiveRetry:
    """
    Advanced retry logic with:
    - Exponential backoff with jitter
    - Adaptive backoff based on error patterns
    - Different strategies for different error types
    """
    
    def __init__(self,
                 max_retries: int = 3,
                 initial_delay: float = 1.0,
                 max_delay: float = 60.0,
                 exponential_base: float = 2.0,
                 jitter: bool = True):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        
        # Track error patterns
        self._error_history = deque(maxlen=100)
        self._last_errors = deque(maxlen=10)
        
    def calculate_delay(self, attempt: int, error: Exception = None) -> float:
        """Calculate delay with exponential backoff and jitter"""
        # Base exponential backoff
        delay = min(self.initial_delay * (self.exponential_base ** attempt), self.max_delay)
        
        # Adaptive adjustment based on recent errors
        if error:
            self._error_history.append({
                'time': datetime.now(),
                'type': type(error).__name__,
                'attempt': attempt
            })
            
            # If seeing many errors recently, increase delay
            recent_errors = sum(1 for e in self._error_history 
                               if datetime.now() - e['time'] < timedelta(minutes=5))
            if recent_errors > 10:
                delay *= 1.5
                
        # Add jitter to prevent thundering herd
        if self.jitter:
            delay *= (0.5 + random.random())
            
        return delay
        
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """Determine if we should retry based on error type and history"""
        if attempt >= self.max_retries:
            return False
            
        # Don't retry on certain errors
        non_retryable = (
            KeyboardInterrupt,
            SystemExit,
            MemoryError,
        )
        if isinstance(error, non_retryable):
            return False
            
        # Check for repeated errors
        self._last_errors.append(type(error).__name__)
        if len(self._last_errors) == self._last_errors.maxlen and \
           len(set(self._last_errors)) == 1:
            logger.warning(f"Repeated {type(error).__name__} errors, stopping retries")
            return False
            
        return True


def adaptive_retry(max_retries: int = 3,
                   initial_delay: float = 1.0,
                   max_delay: float = 60.0,
                   retry_on: tuple = (Exception,)):
    """Decorator for adaptive retry with exponential backoff"""
    def decorator(func):
        retry_logic = AdaptiveRetry(max_retries, initial_delay, max_delay)
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retry_on as e:
                    last_exception = e
                    
                    if not retry_logic.should_retry(e, attempt):
                        raise
                        
                    if attempt < max_retries:
                        delay = retry_logic.calculate_delay(attempt, e)
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        time.sleep(delay)
                        
            raise last_exception
            
        wrapper.retry_logic = retry_logic
        return wrapper
    return decorator


class ConnectionPoolManager:
    """
    Manages HTTP connection pools for better performance and resource usage
    """
    
    def __init__(self,
                 pool_connections: int = 10,
                 pool_maxsize: int = 50,
                 max_retries: int = 3,
                 backoff_factor: float = 0.3,
                 status_forcelist: tuple = (500, 502, 503, 504)):
        self.pool_connections = pool_connections
        self.pool_maxsize = pool_maxsize
        
        # Configure retry strategy
        self.retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"],
            raise_on_status=False
        )
        
        # Pool registry
        self._pools: Dict[str, requests.Session] = {}
        self._lock = Lock()
        
    def get_session(self, base_url: str, 
                    headers: Dict[str, str] = None,
                    auth: Any = None,
                    timeout: float = 30.0) -> requests.Session:
        """Get or create a session with connection pooling"""
        with self._lock:
            if base_url not in self._pools:
                session = requests.Session()
                
                # Configure connection pooling
                adapter = HTTPAdapter(
                    pool_connections=self.pool_connections,
                    pool_maxsize=self.pool_maxsize,
                    max_retries=self.retry_strategy
                )
                
                session.mount("http://", adapter)
                session.mount("https://", adapter)
                
                # Set default headers and auth
                if headers:
                    session.headers.update(headers)
                if auth:
                    session.auth = auth
                    
                # Set default timeout
                session.request = self._wrap_request_with_timeout(session.request, timeout)
                
                self._pools[base_url] = session
                logger.info(f"Created new connection pool for {base_url}")
                
            return self._pools[base_url]
            
    def _wrap_request_with_timeout(self, request_func: Callable, default_timeout: float):
        """Wrap request method to ensure timeout is always set"""
        @wraps(request_func)
        def wrapper(*args, **kwargs):
            if 'timeout' not in kwargs:
                kwargs['timeout'] = default_timeout
            return request_func(*args, **kwargs)
        return wrapper
        
    def close_all(self):
        """Close all connection pools"""
        with self._lock:
            for base_url, session in self._pools.items():
                session.close()
                logger.info(f"Closed connection pool for {base_url}")
            self._pools.clear()
            
    def get_pool_stats(self) -> Dict[str, Any]:
        """Get statistics about connection pools"""
        with self._lock:
            stats = {}
            for base_url, session in self._pools.items():
                # Get adapter stats
                adapter = session.get_adapter("https://")
                pool_manager = getattr(adapter, 'poolmanager', None)
                
                if pool_manager:
                    stats[base_url] = {
                        "num_pools": len(pool_manager.pools),
                        "pool_connections": self.pool_connections,
                        "pool_maxsize": self.pool_maxsize
                    }
                else:
                    stats[base_url] = {"status": "no_pool_manager"}
                    
            return stats


class MetricsCollector:
    """
    Collects metrics for monitoring integration health
    """
    
    def __init__(self, window_size: int = 3600):  # 1 hour window
        self.window_size = window_size
        self._metrics: Dict[str, deque] = {}
        self._lock = Lock()
        
    def record_request(self, service: str, endpoint: str, 
                      duration: float, status_code: int, 
                      error: Optional[str] = None):
        """Record a request metric"""
        with self._lock:
            key = f"{service}:{endpoint}"
            if key not in self._metrics:
                self._metrics[key] = deque()
                
            self._metrics[key].append({
                'timestamp': datetime.now(),
                'duration': duration,
                'status_code': status_code,
                'error': error,
                'success': 200 <= status_code < 300
            })
            
            # Clean old metrics
            self._clean_old_metrics(key)
            
    def _clean_old_metrics(self, key: str):
        """Remove metrics older than window_size"""
        cutoff = datetime.now() - timedelta(seconds=self.window_size)
        while self._metrics[key] and self._metrics[key][0]['timestamp'] < cutoff:
            self._metrics[key].popleft()
            
    def get_stats(self, service: str = None, endpoint: str = None) -> Dict[str, Any]:
        """Get statistics for a service/endpoint"""
        with self._lock:
            stats = {}
            
            for key, metrics in self._metrics.items():
                if service and not key.startswith(f"{service}:"):
                    continue
                if endpoint and not key.endswith(f":{endpoint}"):
                    continue
                    
                if not metrics:
                    continue
                    
                total = len(metrics)
                successful = sum(1 for m in metrics if m['success'])
                failed = total - successful
                
                durations = [m['duration'] for m in metrics if m['success']]
                
                stats[key] = {
                    'total_requests': total,
                    'successful_requests': successful,
                    'failed_requests': failed,
                    'success_rate': successful / total if total > 0 else 0,
                    'avg_duration': sum(durations) / len(durations) if durations else 0,
                    'min_duration': min(durations) if durations else 0,
                    'max_duration': max(durations) if durations else 0,
                    'errors': [m['error'] for m in metrics if m['error']][-5:]  # Last 5 errors
                }
                
            return stats


# Global instances
_connection_pool_manager = ConnectionPoolManager()
_metrics_collector = MetricsCollector()


def get_connection_pool_manager() -> ConnectionPoolManager:
    """Get the global connection pool manager"""
    return _connection_pool_manager


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector"""
    return _metrics_collector


def with_resilience(service_name: str,
                    circuit_breaker_threshold: int = 5,
                    rate_limit: float = 10.0,
                    max_retries: int = 3):
    """
    Decorator that combines multiple resilience patterns
    
    Args:
        service_name: Name of the service for metrics
        circuit_breaker_threshold: Number of failures before opening circuit
        rate_limit: Requests per second limit
        max_retries: Maximum number of retries
    """
    def decorator(func):
        # Apply resilience patterns in order
        wrapped = func
        
        # Rate limiting
        if rate_limit:
            limiter = RateLimiter(rate_limit)
            
            @wraps(wrapped)
            def rate_limited(*args, **kwargs):
                if not limiter.acquire(blocking=True, timeout=30):
                    raise RateLimitExceeded(f"Rate limit exceeded for {service_name}")
                return wrapped(*args, **kwargs)
                
            wrapped = rate_limited
            
        # Circuit breaker
        breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            name=f"{service_name}.{func.__name__}"
        )
        
        @wraps(wrapped)
        def circuit_broken(*args, **kwargs):
            return breaker.call(wrapped, *args, **kwargs)
            
        wrapped = circuit_broken
        
        # Retry logic
        retry_logic = AdaptiveRetry(max_retries=max_retries)
        
        @wraps(wrapped)
        def with_retry(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_retries + 1):
                start_time = time.time()
                
                try:
                    result = wrapped(*args, **kwargs)
                    duration = time.time() - start_time
                    
                    # Record success metric
                    _metrics_collector.record_request(
                        service_name, func.__name__, duration, 200
                    )
                    
                    return result
                    
                except Exception as e:
                    duration = time.time() - start_time
                    last_exception = e
                    
                    # Record failure metric
                    _metrics_collector.record_request(
                        service_name, func.__name__, duration, 
                        getattr(e, 'status_code', 500),
                        str(e)
                    )
                    
                    if not retry_logic.should_retry(e, attempt):
                        raise
                        
                    if attempt < max_retries:
                        delay = retry_logic.calculate_delay(attempt, e)
                        logger.warning(
                            f"{service_name}.{func.__name__} attempt {attempt + 1} failed: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        time.sleep(delay)
                        
            raise last_exception
            
        # Store references to patterns for introspection
        with_retry.circuit_breaker = breaker
        with_retry.rate_limiter = limiter if rate_limit else None
        with_retry.retry_logic = retry_logic
        
        return with_retry
        
    return decorator 