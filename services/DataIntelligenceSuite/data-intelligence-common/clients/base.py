"""
Unified base client framework with enhanced decorators and patterns.

Provides a foundation for all client implementations with built-in support for:
- Automatic retries with backoff
- Response caching
- Circuit breaker pattern
- Metrics and monitoring
- Authentication handling
- Request/response transformation
"""

import asyncio
import functools
import time
from typing import Any, Dict, Optional, Callable, TypeVar, Union, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import logging
import json
import hashlib
from contextlib import asynccontextmanager
import uuid

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..monitoring import MetricsCollector, StructuredLogger
from ..caching import CacheManager

logger = StructuredLogger.get_logger(__name__)

T = TypeVar('T')
R = TypeVar('R')


class ClientError(Exception):
    """Base client error"""
    pass


class ConnectionError(ClientError):
    """Connection-related errors"""
    pass


class AuthenticationError(ClientError):
    """Authentication-related errors"""
    pass


class RateLimitError(ClientError):
    """Rate limit errors"""
    pass


class CircuitBreakerError(ClientError):
    """Circuit breaker open error"""
    pass


@dataclass
class RetryConfig:
    """Retry configuration"""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    retry_on: List[type] = field(default_factory=lambda: [ConnectionError, TimeoutError])
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for attempt"""
        delay = min(self.initial_delay * (self.exponential_base ** (attempt - 1)), self.max_delay)
        if self.jitter:
            import random
            delay *= (0.5 + random.random())
        return delay


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration"""
    failure_threshold: int = 5
    recovery_timeout: timedelta = timedelta(seconds=60)
    expected_exception: type = Exception
    success_threshold: int = 2


class CircuitBreakerState(Enum):
    """Circuit breaker states"""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Circuit breaker implementation"""
    config: CircuitBreakerConfig
    state: CircuitBreakerState = CircuitBreakerState.CLOSED
    failures: int = 0
    successes: int = 0
    last_failure_time: Optional[datetime] = None
    
    def call_succeeded(self):
        """Record successful call"""
        self.failures = 0
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.successes += 1
            if self.successes >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.successes = 0
                
    def call_failed(self):
        """Record failed call"""
        self.failures += 1
        self.last_failure_time = datetime.utcnow()
        
        if self.failures >= self.config.failure_threshold:
            self.state = CircuitBreakerState.OPEN
            
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN
            self.successes = 0
            
    def can_execute(self) -> bool:
        """Check if execution is allowed"""
        if self.state == CircuitBreakerState.CLOSED:
            return True
            
        if self.state == CircuitBreakerState.OPEN:
            if self.last_failure_time and \
               datetime.utcnow() - self.last_failure_time > self.config.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                return True
            return False
            
        # HALF_OPEN
        return True


@dataclass
class ClientConfig:
    """Base client configuration"""
    name: str
    base_url: Optional[str] = None
    timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    
    # Security
    auth_enabled: bool = True
    auth_type: str = "bearer"  # bearer, basic, api_key, oauth2
    
    # Retry
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    
    # Circuit breaker
    circuit_breaker_enabled: bool = True
    circuit_breaker_config: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    
    # Caching
    cache_enabled: bool = True
    cache_ttl: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    cache_key_prefix: str = ""
    
    # Monitoring
    metrics_enabled: bool = True
    trace_requests: bool = True
    
    # Rate limiting
    rate_limit: Optional[int] = None  # requests per minute
    
    # Custom headers
    headers: Dict[str, str] = field(default_factory=dict)


# Decorators

def retry(config: Optional[RetryConfig] = None):
    """
    Retry decorator with exponential backoff.
    
    Usage:
        @retry(RetryConfig(max_attempts=5))
        async def make_request():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> T:
            retry_config = config or RetryConfig()
            last_exception = None
            
            for attempt in range(1, retry_config.max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    # Check if should retry
                    if not any(isinstance(e, exc_type) for exc_type in retry_config.retry_on):
                        raise
                        
                    if attempt < retry_config.max_attempts:
                        delay = retry_config.calculate_delay(attempt)
                        logger.warning(
                            f"Attempt {attempt} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        await asyncio.sleep(delay)
                    else:
                        raise
                        
            raise last_exception
            
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs) -> T:
            retry_config = config or RetryConfig()
            last_exception = None
            
            for attempt in range(1, retry_config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if not any(isinstance(e, exc_type) for exc_type in retry_config.retry_on):
                        raise
                        
                    if attempt < retry_config.max_attempts:
                        delay = retry_config.calculate_delay(attempt)
                        logger.warning(
                            f"Attempt {attempt} failed for {func.__name__}: {e}. "
                            f"Retrying in {delay:.2f}s..."
                        )
                        time.sleep(delay)
                    else:
                        raise
                        
            raise last_exception
            
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator


def cached(ttl: Optional[timedelta] = None, key_func: Optional[Callable] = None):
    """
    Caching decorator.
    
    Usage:
        @cached(ttl=timedelta(minutes=10))
        async def get_data(id: str):
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs) -> T:
            if not hasattr(self, 'cache') or not self.cache or not self.config.cache_enabled:
                return await func(self, *args, **kwargs)
                
            # Generate cache key
            if key_func:
                cache_key = key_func(self, *args, **kwargs)
            else:
                # Default key generation
                key_parts = [self.config.cache_key_prefix, func.__name__]
                key_parts.extend(str(arg) for arg in args)
                key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
                cache_key = ":".join(key_parts)
                
            # Try cache
            cached_value = await self.cache.get(self.config.name, cache_key)
            if cached_value is not None:
                logger.debug(f"Cache hit for {cache_key}")
                return cached_value
                
            # Call function
            result = await func(self, *args, **kwargs)
            
            # Cache result
            cache_ttl = ttl or self.config.cache_ttl
            await self.cache.put(self.config.name, cache_key, result, cache_ttl)
            
            return result
            
        return wrapper
    return decorator


def circuit_breaker(config: Optional[CircuitBreakerConfig] = None):
    """
    Circuit breaker decorator.
    
    Usage:
        @circuit_breaker(CircuitBreakerConfig(failure_threshold=3))
        async def external_call():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        breaker = CircuitBreaker(config or CircuitBreakerConfig())
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            if not breaker.can_execute():
                raise CircuitBreakerError(
                    f"Circuit breaker is OPEN for {func.__name__}"
                )
                
            try:
                result = await func(*args, **kwargs)
                breaker.call_succeeded()
                return result
            except breaker.config.expected_exception as e:
                breaker.call_failed()
                raise
                
        return wrapper
    return decorator


def rate_limited(max_calls: int, period: timedelta):
    """
    Rate limiting decorator.
    
    Usage:
        @rate_limited(max_calls=100, period=timedelta(minutes=1))
        async def api_call():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        calls = []
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> T:
            now = datetime.utcnow()
            
            # Remove old calls
            cutoff = now - period
            nonlocal calls
            calls = [call_time for call_time in calls if call_time > cutoff]
            
            # Check rate limit
            if len(calls) >= max_calls:
                raise RateLimitError(
                    f"Rate limit exceeded for {func.__name__}: "
                    f"{max_calls} calls per {period}"
                )
                
            # Record call
            calls.append(now)
            
            # Execute
            return await func(*args, **kwargs)
            
        return wrapper
    return decorator


def monitored(metric_name: Optional[str] = None):
    """
    Monitoring decorator for metrics collection.
    
    Usage:
        @monitored("api_calls")
        async def make_request():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs) -> T:
            if not hasattr(self, 'metrics') or not self.metrics:
                return await func(self, *args, **kwargs)
                
            name = metric_name or f"{self.config.name}.{func.__name__}"
            start_time = time.time()
            
            try:
                result = await func(self, *args, **kwargs)
                
                # Record success
                self.metrics.increment(f"{name}.success")
                self.metrics.record_timing(
                    f"{name}.duration",
                    (time.time() - start_time) * 1000
                )
                
                return result
                
            except Exception as e:
                # Record failure
                self.metrics.increment(f"{name}.failure")
                self.metrics.increment(
                    f"{name}.error.{type(e).__name__}"
                )
                raise
                
        return wrapper
    return decorator


def authenticated(auth_type: Optional[str] = None):
    """
    Authentication decorator.
    
    Usage:
        @authenticated("bearer")
        async def secure_request():
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs) -> T:
            if not self.config.auth_enabled:
                return await func(self, *args, **kwargs)
                
            # Ensure authentication
            if not hasattr(self, '_auth_token') or not self._auth_token:
                await self._authenticate()
                
            # Add auth headers
            if hasattr(self, '_add_auth_headers'):
                await self._add_auth_headers()
                
            try:
                return await func(self, *args, **kwargs)
            except AuthenticationError:
                # Retry with fresh auth
                await self._authenticate()
                return await func(self, *args, **kwargs)
                
        return wrapper
    return decorator


class BaseClient(ABC):
    """
    Enhanced base client with built-in patterns.
    
    Features:
    - Automatic retry with backoff
    - Circuit breaker
    - Response caching
    - Rate limiting
    - Metrics collection
    - Authentication handling
    - Request/response transformation
    """
    
    def __init__(
        self,
        config: ClientConfig,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        cache_manager: Optional[CacheManager] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.cache = cache_manager
        self.metrics = metrics_collector or MetricsCollector(config.name)
        
        # Authentication state
        self._auth_token: Optional[str] = None
        self._auth_expires: Optional[datetime] = None
        
        # Circuit breakers per endpoint
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        
        # Rate limiting
        self._rate_limit_calls: List[datetime] = []
        
    async def initialize(self):
        """Initialize client"""
        logger.info(f"Initializing {self.config.name} client")
        
        # Load configuration from Consul
        if self.consul_client:
            await self._load_config()
            
        # Initialize authentication
        if self.config.auth_enabled:
            await self._authenticate()
            
        # Custom initialization
        await self._initialize()
        
        logger.info(f"{self.config.name} client initialized")
        
    async def shutdown(self):
        """Shutdown client"""
        logger.info(f"Shutting down {self.config.name} client")
        await self._shutdown()
        
    @abstractmethod
    async def _initialize(self):
        """Custom initialization logic"""
        pass
        
    @abstractmethod
    async def _shutdown(self):
        """Custom shutdown logic"""
        pass
        
    async def _load_config(self):
        """Load configuration from Consul"""
        try:
            config_data = await self.consul_client.kv_get(
                f"clients/{self.config.name}/config"
            )
            if config_data:
                # Update config with Consul values
                config_dict = json.loads(config_data)
                for key, value in config_dict.items():
                    if hasattr(self.config, key):
                        setattr(self.config, key, value)
                        
                logger.info(f"Loaded configuration from Consul for {self.config.name}")
                
        except Exception as e:
            logger.error(f"Failed to load config from Consul: {e}")
            
    async def _authenticate(self):
        """Authenticate with service"""
        if not self.config.auth_enabled:
            return
            
        logger.info(f"Authenticating {self.config.name} client")
        
        if self.config.auth_type == "bearer":
            await self._authenticate_bearer()
        elif self.config.auth_type == "basic":
            await self._authenticate_basic()
        elif self.config.auth_type == "api_key":
            await self._authenticate_api_key()
        elif self.config.auth_type == "oauth2":
            await self._authenticate_oauth2()
        else:
            raise ValueError(f"Unknown auth type: {self.config.auth_type}")
            
    async def _authenticate_bearer(self):
        """Bearer token authentication"""
        # Get token from Vault
        if self.vault_client:
            secret = await self.vault_client.get_secret(
                f"clients/{self.config.name}/auth"
            )
            if secret and "token" in secret:
                self._auth_token = secret["token"]
                # Set expiry (default 1 hour)
                self._auth_expires = datetime.utcnow() + timedelta(hours=1)
                
    async def _authenticate_basic(self):
        """Basic authentication"""
        # Get credentials from Vault
        if self.vault_client:
            secret = await self.vault_client.get_secret(
                f"clients/{self.config.name}/auth"
            )
            if secret and "username" in secret and "password" in secret:
                import base64
                credentials = f"{secret['username']}:{secret['password']}"
                self._auth_token = base64.b64encode(credentials.encode()).decode()
                
    async def _authenticate_api_key(self):
        """API key authentication"""
        # Get API key from Vault
        if self.vault_client:
            secret = await self.vault_client.get_secret(
                f"clients/{self.config.name}/auth"
            )
            if secret and "api_key" in secret:
                self._auth_token = secret["api_key"]
                
    async def _authenticate_oauth2(self):
        """OAuth2 authentication"""
        # This would implement OAuth2 flow
        raise NotImplementedError("OAuth2 authentication not implemented")
        
    def _add_auth_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Add authentication headers"""
        if not self.config.auth_enabled or not self._auth_token:
            return headers
            
        if self.config.auth_type == "bearer":
            headers["Authorization"] = f"Bearer {self._auth_token}"
        elif self.config.auth_type == "basic":
            headers["Authorization"] = f"Basic {self._auth_token}"
        elif self.config.auth_type == "api_key":
            headers["X-API-Key"] = self._auth_token
            
        return headers
        
    def _check_rate_limit(self):
        """Check rate limit"""
        if not self.config.rate_limit:
            return
            
        now = datetime.utcnow()
        cutoff = now - timedelta(minutes=1)
        
        # Remove old calls
        self._rate_limit_calls = [
            call_time for call_time in self._rate_limit_calls
            if call_time > cutoff
        ]
        
        # Check limit
        if len(self._rate_limit_calls) >= self.config.rate_limit:
            raise RateLimitError(
                f"Rate limit exceeded: {self.config.rate_limit} calls per minute"
            )
            
        # Record call
        self._rate_limit_calls.append(now)
        
    def _get_circuit_breaker(self, endpoint: str) -> CircuitBreaker:
        """Get or create circuit breaker for endpoint"""
        if endpoint not in self._circuit_breakers:
            self._circuit_breakers[endpoint] = CircuitBreaker(
                self.config.circuit_breaker_config
            )
        return self._circuit_breakers[endpoint]
        
    @asynccontextmanager
    async def _request_context(self, endpoint: str, method: str = "GET"):
        """Context manager for requests with monitoring"""
        request_id = str(uuid.uuid4())
        start_time = time.time()
        
        # Log request
        logger.info(
            f"Request started",
            extra={
                "client": self.config.name,
                "endpoint": endpoint,
                "method": method,
                "request_id": request_id
            }
        )
        
        try:
            yield request_id
            
            # Record success
            if self.metrics:
                self.metrics.increment(f"{self.config.name}.request.success")
                self.metrics.record_timing(
                    f"{self.config.name}.request.duration",
                    (time.time() - start_time) * 1000
                )
                
        except Exception as e:
            # Record failure
            if self.metrics:
                self.metrics.increment(f"{self.config.name}.request.failure")
                self.metrics.increment(
                    f"{self.config.name}.request.error.{type(e).__name__}"
                )
                
            logger.error(
                f"Request failed",
                extra={
                    "client": self.config.name,
                    "endpoint": endpoint,
                    "method": method,
                    "request_id": request_id,
                    "error": str(e),
                    "duration_ms": (time.time() - start_time) * 1000
                }
            )
            raise
            
    def _generate_cache_key(self, *args, **kwargs) -> str:
        """Generate cache key from arguments"""
        key_parts = [self.config.cache_key_prefix]
        key_parts.extend(str(arg) for arg in args)
        key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
        
        key_string = ":".join(key_parts)
        
        # Hash if too long
        if len(key_string) > 250:
            key_hash = hashlib.sha256(key_string.encode()).hexdigest()
            return f"{self.config.cache_key_prefix}:hash:{key_hash}"
            
        return key_string


class RESTClient(BaseClient):
    """Base REST API client"""
    
    def __init__(self, *args, session=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = session
        
    async def _initialize(self):
        """Initialize HTTP session"""
        if not self.session:
            import aiohttp
            timeout = aiohttp.ClientTimeout(
                total=self.config.timeout.total_seconds()
            )
            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers=self.config.headers
            )
            
    async def _shutdown(self):
        """Close HTTP session"""
        if self.session and hasattr(self.session, 'close'):
            await self.session.close()
            
    @retry()
    @circuit_breaker()
    @monitored()
    @authenticated()
    async def get(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """GET request"""
        async with self._request_context(endpoint, "GET"):
            self._check_rate_limit()
            
            url = f"{self.config.base_url}{endpoint}"
            headers = self._add_auth_headers(kwargs.pop("headers", {}))
            
            async with self.session.get(url, headers=headers, **kwargs) as response:
                response.raise_for_status()
                return await response.json()
                
    @retry()
    @circuit_breaker()
    @monitored()
    @authenticated()
    async def post(self, endpoint: str, data: Any = None, **kwargs) -> Dict[str, Any]:
        """POST request"""
        async with self._request_context(endpoint, "POST"):
            self._check_rate_limit()
            
            url = f"{self.config.base_url}{endpoint}"
            headers = self._add_auth_headers(kwargs.pop("headers", {}))
            
            if data and not isinstance(data, (str, bytes)):
                kwargs["json"] = data
            else:
                kwargs["data"] = data
                
            async with self.session.post(url, headers=headers, **kwargs) as response:
                response.raise_for_status()
                return await response.json()
                
    @retry()
    @circuit_breaker()
    @monitored()
    @authenticated()
    async def put(self, endpoint: str, data: Any = None, **kwargs) -> Dict[str, Any]:
        """PUT request"""
        async with self._request_context(endpoint, "PUT"):
            self._check_rate_limit()
            
            url = f"{self.config.base_url}{endpoint}"
            headers = self._add_auth_headers(kwargs.pop("headers", {}))
            
            if data and not isinstance(data, (str, bytes)):
                kwargs["json"] = data
            else:
                kwargs["data"] = data
                
            async with self.session.put(url, headers=headers, **kwargs) as response:
                response.raise_for_status()
                return await response.json()
                
    @retry()
    @circuit_breaker()
    @monitored()
    @authenticated()
    async def delete(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """DELETE request"""
        async with self._request_context(endpoint, "DELETE"):
            self._check_rate_limit()
            
            url = f"{self.config.base_url}{endpoint}"
            headers = self._add_auth_headers(kwargs.pop("headers", {}))
            
            async with self.session.delete(url, headers=headers, **kwargs) as response:
                response.raise_for_status()
                if response.content_length:
                    return await response.json()
                return {"status": "success"} 