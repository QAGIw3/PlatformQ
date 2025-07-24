"""
Unified base client framework with enhanced decorators and patterns.

Provides a foundation for all client implementations with built-in support for:
- Automatic retries with backoff
- Response caching
- Circuit breaker pattern
- Metrics and monitoring
- Authentication handling
- Request/response transformation
- Service discovery via Consul
- Dynamic credentials from Vault
- Health checking and load balancing
- mTLS support
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
import ssl
import random

import aiohttp
from aiohttp import ClientTimeout, ClientError as AioHttpClientError
from tenacity import retry as tenacity_retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..monitoring import MetricsCollector, StructuredLogger
from ..caching import CacheManager
from ..vault_consul import VaultConsulIntegration, DataServiceConfig

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


class ServiceDiscoveryMode(Enum):
    """Service discovery modes"""
    CONSUL = "consul"
    STATIC = "static"
    DNS = "dns"


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
    """Unified client configuration with all features"""
    name: str
    service_name: Optional[str] = None  # For service discovery
    base_url: Optional[str] = None
    timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    
    # Service discovery
    use_service_discovery: bool = True
    discovery_mode: ServiceDiscoveryMode = ServiceDiscoveryMode.CONSUL
    consul_url: str = "http://localhost:8500"
    
    # Vault integration
    use_vault_credentials: bool = True
    vault_url: str = "http://localhost:8200"
    vault_role: str = "readonly"
    credential_ttl: int = 3600  # seconds
    
    # Timeouts
    connect_timeout: float = 5.0
    read_timeout: float = 30.0
    total_timeout: float = 60.0
    
    # Security
    auth_enabled: bool = True
    auth_type: str = "bearer"  # bearer, basic, api_key, oauth2
    auth_token: Optional[str] = None
    auth_header: str = "Authorization"
    
    # SSL/TLS
    verify_ssl: bool = True
    use_mtls: bool = False
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    
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
    
    # Health check
    health_check_path: str = "/health"
    health_check_interval: int = 30
    
    # Load balancing
    load_balancing_strategy: str = "round_robin"  # round_robin, random, least_conn
    
    # Custom headers
    headers: Dict[str, str] = field(default_factory=dict)
    
    def __post_init__(self):
        """Post-init processing"""
        # Use service_name if provided, otherwise use name
        if not self.service_name:
            self.service_name = self.name


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
    Unified base client with all features.
    
    Features:
    - Automatic retry with backoff
    - Circuit breaker
    - Response caching
    - Rate limiting
    - Metrics collection
    - Authentication handling
    - Request/response transformation
    - Dynamic service discovery via Consul
    - Dynamic credentials from Vault
    - Automatic credential renewal
    - Health checking
    - Load balancing
    - mTLS support
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
        
        # HTTP session
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Service instances cache
        self._service_instances: List[Dict[str, Any]] = []
        self._current_instance_index = 0
        self._last_discovery: Optional[datetime] = None
        
        # Credentials cache
        self._credentials: Optional[Dict[str, Any]] = None
        self._credentials_lease_id: Optional[str] = None
        self._credentials_expiry: Optional[datetime] = None
        
        # Authentication state
        self._auth_token: Optional[str] = None
        self._auth_expires: Optional[datetime] = None
        
        # SSL context
        self._ssl_context: Optional[ssl.SSLContext] = None
        
        # Circuit breakers per endpoint
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        
        # Rate limiting
        self._rate_limit_calls: List[datetime] = []
        
        # Background tasks
        self._renewal_task: Optional[asyncio.Task] = None
        self._health_check_task: Optional[asyncio.Task] = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.shutdown()
        
    async def initialize(self):
        """Initialize client"""
        logger.info(f"Initializing {self.config.name} client")
        
        try:
            # Create SSL context if needed
            if self.config.verify_ssl or self.config.use_mtls:
                self._ssl_context = self._create_ssl_context()
                
            # Create HTTP session
            timeout = ClientTimeout(
                sock_connect=self.config.connect_timeout,
                sock_read=self.config.read_timeout,
                total=self.config.total_timeout
            )
            
            connector = aiohttp.TCPConnector(
                ssl=self._ssl_context,
                limit=100,
                ttl_dns_cache=300
            )
            
            self._session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers=self.config.headers
            )
            
            # Load configuration from Consul
            if self.consul_client:
                await self._load_config()
                
            # Discover service instances if using service discovery
            if self.config.use_service_discovery and self.config.service_name:
                await self._discover_service_instances()
                
            # Initialize authentication
            if self.config.auth_enabled:
                await self._authenticate()
                
            # Start background tasks
            if self.config.use_vault_credentials and self.vault_client:
                self._renewal_task = asyncio.create_task(self._credential_renewal_loop())
                
            if self.config.use_service_discovery and self.consul_client:
                self._health_check_task = asyncio.create_task(self._health_check_loop())
                
            # Custom initialization
            await self._initialize()
            
            logger.info(f"{self.config.name} client initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize: {e}")
            await self.shutdown()
            raise
        
    async def shutdown(self):
        """Shutdown client"""
        logger.info(f"Shutting down {self.config.name} client")
        
        # Cancel background tasks
        if self._renewal_task:
            self._renewal_task.cancel()
            try:
                await self._renewal_task
            except asyncio.CancelledError:
                pass
                
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass
                
        # Revoke credentials if using Vault
        if self._credentials_lease_id and self.vault_client:
            try:
                await self.vault_client.revoke_lease(self._credentials_lease_id)
            except Exception as e:
                logger.error(f"Failed to revoke credentials: {e}")
                
        # Custom shutdown
        await self._shutdown()
        
        # Close HTTP session
        if self._session:
            await self._session.close()
            
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
            
    async def _get_service_url(self) -> str:
        """Get service URL with load balancing"""
        if self.config.base_url:
            return self.config.base_url
            
        if not self._service_instances:
            await self._discover_service_instances()
            
        if not self._service_instances:
            raise ValueError(f"No instances found for service {self.config.service_name}")
            
        # Load balancing
        if self.config.load_balancing_strategy == "round_robin":
            instance = self._service_instances[self._current_instance_index]
            self._current_instance_index = (self._current_instance_index + 1) % len(self._service_instances)
        elif self.config.load_balancing_strategy == "random":
            instance = random.choice(self._service_instances)
        else:  # least_conn or default
            # For now, just use round robin
            instance = self._service_instances[self._current_instance_index]
            self._current_instance_index = (self._current_instance_index + 1) % len(self._service_instances)
            
        return f"http://{instance['address']}:{instance['port']}"
        
    async def _discover_service_instances(self):
        """Discover service instances from Consul"""
        if not self.consul_client or not self.config.service_name:
            logger.warning("Consul client not available or service_name not set, using static URL")
            return
            
        try:
            instances = await self.consul_client.discover_service(
                self.config.service_name,
                passing_only=True
            )
            
            self._service_instances = instances
            self._last_discovery = datetime.utcnow()
            
            logger.info(f"Discovered {len(instances)} instances of {self.config.service_name}")
            
        except Exception as e:
            logger.error(f"Service discovery failed: {e}")
            
    async def _get_credentials(self) -> Optional[Dict[str, Any]]:
        """Get credentials from Vault or cache"""
        if not self.config.use_vault_credentials or not self.vault_client:
            return None
            
        # Check cache
        if self._credentials and self._credentials_expiry:
            if datetime.utcnow() < self._credentials_expiry:
                return self._credentials
                
        # Get new credentials
        try:
            creds = await self.vault_client.get_database_credentials(
                self.config.service_name,
                self.config.vault_role
            )
            
            self._credentials = creds["data"]
            self._credentials_lease_id = creds["lease_id"]
            self._credentials_expiry = datetime.utcnow() + timedelta(seconds=self.config.credential_ttl)
            
            logger.info(f"Obtained new credentials for {self.config.service_name}")
            
            return self._credentials
            
        except Exception as e:
            logger.error(f"Failed to get credentials: {e}")
            return None
            
    async def _credential_renewal_loop(self):
        """Background task to renew credentials"""
        while True:
            try:
                # Wait until half the TTL
                await asyncio.sleep(self.config.credential_ttl / 2)
                
                # Renew credentials
                if self._credentials_lease_id:
                    await self.vault_client.renew_lease(self._credentials_lease_id)
                    logger.debug(f"Renewed credentials for {self.config.service_name}")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Credential renewal failed: {e}")
                await asyncio.sleep(60)
                
    async def _health_check_loop(self):
        """Background task to check service health"""
        while True:
            try:
                await asyncio.sleep(self.config.health_check_interval)
                
                # Re-discover services periodically
                await self._discover_service_instances()
                
                # Check health of each instance
                healthy_instances = []
                for instance in self._service_instances:
                    if await self._check_instance_health(instance):
                        healthy_instances.append(instance)
                        
                self._service_instances = healthy_instances
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check failed: {e}")
                
    async def _check_instance_health(self, instance: Dict[str, Any]) -> bool:
        """Check health of a service instance"""
        try:
            url = f"http://{instance['address']}:{instance['port']}{self.config.health_check_path}"
            async with self._session.get(url, timeout=5) as response:
                return response.status == 200
        except:
            return False
            
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
        # Check if token provided in config
        if self.config.auth_token:
            self._auth_token = self.config.auth_token
            self._auth_expires = datetime.utcnow() + timedelta(hours=1)
            return
            
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
        """OAuth2 authentication with client credentials and refresh token support"""
        try:
            # Get OAuth2 configuration from Vault if available
            oauth_config = {}
            if self.vault_client and self.config.use_vault_credentials:
                try:
                    secret_path = f"secret/data/{self.config.name}/oauth2"
                    secret = await self.vault_client.read_secret(secret_path)
                    if secret and "data" in secret:
                        oauth_config = secret["data"]
                except Exception as e:
                    logger.warning(f"Could not load OAuth2 config from Vault: {e}")
            
            # Override with any config values
            oauth_config.update({
                "token_url": oauth_config.get("token_url", getattr(self.config, "oauth2_token_url", None)),
                "client_id": oauth_config.get("client_id", getattr(self.config, "oauth2_client_id", None)),
                "client_secret": oauth_config.get("client_secret", getattr(self.config, "oauth2_client_secret", None)),
                "scope": oauth_config.get("scope", getattr(self.config, "oauth2_scope", "")),
                "grant_type": oauth_config.get("grant_type", "client_credentials")
            })
            
            # Validate required fields
            if not all([oauth_config.get("token_url"), oauth_config.get("client_id"), oauth_config.get("client_secret")]):
                raise ValueError("OAuth2 requires token_url, client_id, and client_secret")
            
            # Check if we have a valid token in cache
            cache_key = f"oauth2_token_{self.config.name}_{oauth_config['client_id']}"
            if self.cache_manager:
                cached_token = await self.cache_manager.get("auth_tokens", cache_key)
                if cached_token and cached_token.get("expires_at", 0) > time.time():
                    self._auth_token = cached_token["access_token"]
                    self._auth_headers = {"Authorization": f"Bearer {self._auth_token}"}
                    return
            
            # Request new token
            async with aiohttp.ClientSession() as session:
                # Prepare request data based on grant type
                if oauth_config["grant_type"] == "client_credentials":
                    data = {
                        "grant_type": "client_credentials",
                        "client_id": oauth_config["client_id"],
                        "client_secret": oauth_config["client_secret"],
                        "scope": oauth_config["scope"]
                    }
                elif oauth_config["grant_type"] == "refresh_token":
                    # Get refresh token from cache or config
                    refresh_token = None
                    if self.cache_manager:
                        cached_refresh = await self.cache_manager.get("auth_tokens", f"{cache_key}_refresh")
                        if cached_refresh:
                            refresh_token = cached_refresh.get("refresh_token")
                    
                    if not refresh_token:
                        refresh_token = oauth_config.get("refresh_token")
                        
                    if not refresh_token:
                        # Fall back to client credentials
                        oauth_config["grant_type"] = "client_credentials"
                        data = {
                            "grant_type": "client_credentials",
                            "client_id": oauth_config["client_id"],
                            "client_secret": oauth_config["client_secret"],
                            "scope": oauth_config["scope"]
                        }
                    else:
                        data = {
                            "grant_type": "refresh_token",
                            "refresh_token": refresh_token,
                            "client_id": oauth_config["client_id"],
                            "client_secret": oauth_config["client_secret"]
                        }
                else:
                    raise ValueError(f"Unsupported OAuth2 grant type: {oauth_config['grant_type']}")
                
                # Make token request
                async with session.post(
                    oauth_config["token_url"],
                    data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise AuthenticationError(f"OAuth2 token request failed: {response.status} - {error_text}")
                    
                    token_data = await response.json()
                    
                    # Extract token information
                    self._auth_token = token_data["access_token"]
                    self._auth_headers = {"Authorization": f"Bearer {self._auth_token}"}
                    
                    # Calculate expiration time
                    expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
                    expires_at = time.time() + expires_in - 60  # Refresh 1 minute early
                    
                    # Cache the token
                    if self.cache_manager:
                        cache_data = {
                            "access_token": self._auth_token,
                            "token_type": token_data.get("token_type", "Bearer"),
                            "expires_at": expires_at,
                            "scope": token_data.get("scope", oauth_config["scope"])
                        }
                        
                        # Cache with TTL based on expiration
                        ttl = timedelta(seconds=max(expires_in - 60, 60))
                        await self.cache_manager.put("auth_tokens", cache_key, cache_data, ttl=ttl)
                        
                        # Cache refresh token if provided
                        if "refresh_token" in token_data:
                            await self.cache_manager.put(
                                "auth_tokens",
                                f"{cache_key}_refresh",
                                {"refresh_token": token_data["refresh_token"]},
                                ttl=timedelta(days=30)  # Refresh tokens typically last longer
                            )
                    
                    logger.info(f"OAuth2 authentication successful for {self.config.name}")
                    
        except Exception as e:
            logger.error(f"OAuth2 authentication failed: {e}")
            raise AuthenticationError(f"OAuth2 authentication failed: {str(e)}")
        
    def _add_auth_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        """Add authentication headers"""
        if not self.config.auth_enabled or not self._auth_token:
            return headers
            
        if self.config.auth_type == "bearer":
            headers[self.config.auth_header] = f"Bearer {self._auth_token}"
        elif self.config.auth_type == "basic":
            headers[self.config.auth_header] = f"Basic {self._auth_token}"
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
        
    def _is_circuit_open(self, url: str) -> bool:
        """Check if circuit breaker is open for URL"""
        breaker = self._circuit_breakers.get(url)
        if breaker:
            return not breaker.can_execute()
        return False
        
    def _record_success(self, url: str):
        """Record successful request"""
        breaker = self._circuit_breakers.get(url)
        if breaker:
            breaker.call_succeeded()
        
    def _record_failure(self, url: str):
        """Record failed request"""
        breaker = self._circuit_breakers.get(url)
        if not breaker:
            breaker = CircuitBreaker(self.config.circuit_breaker_config)
            self._circuit_breakers[url] = breaker
        breaker.call_failed()
        
    def _create_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context for HTTPS connections"""
        context = ssl.create_default_context()
        
        if not self.config.verify_ssl:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
        if self.config.use_mtls and self.config.ssl_cert and self.config.ssl_key:
            context.load_cert_chain(self.config.ssl_cert, self.config.ssl_key)
            
        return context
        
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
        
    @tenacity_retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(AioHttpClientError)
    )
    async def request(
        self,
        method: str,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """Make HTTP request with retries and circuit breaker"""
        url = await self._get_service_url()
        full_url = f"{url}{path}"
        
        # Check circuit breaker
        if self._is_circuit_open(url):
            raise CircuitBreakerError(f"Circuit breaker open for {url}")
            
        # Get credentials if needed
        creds = await self._get_credentials()
        
        # Build headers
        request_headers = dict(self.config.headers)
        if headers:
            request_headers.update(headers)
            
        # Add authentication
        request_headers = self._add_auth_headers(request_headers)
        if creds and "token" in creds and not self._auth_token:
            request_headers[self.config.auth_header] = f"Bearer {creds['token']}"
            
        # Make request
        try:
            async with self._session.request(
                method,
                full_url,
                json=json_data,
                params=params,
                headers=request_headers,
                **kwargs
            ) as response:
                response.raise_for_status()
                
                self._record_success(url)
                
                # Record metrics
                if self.metrics:
                    self.metrics.record_request(
                        method=method,
                        path=path,
                        status=response.status,
                        duration=(datetime.utcnow() - datetime.utcnow()).total_seconds()
                    )
                    
                return await response.json()
                
        except Exception as e:
            self._record_failure(url)
            logger.error(f"Request failed: {method} {full_url} - {e}")
            raise
            
    async def get(self, path: str, **kwargs) -> Dict[str, Any]:
        """GET request"""
        return await self.request("GET", path, **kwargs)
        
    async def post(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """POST request"""
        return await self.request("POST", path, json_data=json_data, **kwargs)
        
    async def put(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """PUT request"""
        return await self.request("PUT", path, json_data=json_data, **kwargs)
        
    async def patch(
        self,
        path: str,
        json_data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """PATCH request"""
        return await self.request("PATCH", path, json_data=json_data, **kwargs)
        
    async def delete(self, path: str, **kwargs) -> Dict[str, Any]:
        """DELETE request"""
        return await self.request("DELETE", path, **kwargs)
        
    async def health_check(self) -> bool:
        """Check if service is healthy"""
        try:
            await self.get(self.config.health_check_path)
            return True
        except:
            return False
            
    @abstractmethod
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get client-specific configuration from Consul"""
        pass


class RESTClient(BaseClient):
    """Base REST API client"""
    
    async def _initialize(self):
        """Initialize is handled in parent class"""
        pass
            
    async def _shutdown(self):
        """Shutdown is handled in parent class"""
        pass
            
    @retry()
    @circuit_breaker()
    @monitored()
    @authenticated()
    async def get(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """GET request with decorators"""
        async with self._request_context(endpoint, "GET"):
            self._check_rate_limit()
            return await super().get(endpoint, **kwargs)
                
    @retry()
    @circuit_breaker()
    @monitored()
    @authenticated()
    async def post(self, endpoint: str, data: Any = None, **kwargs) -> Dict[str, Any]:
        """POST request with decorators"""
        async with self._request_context(endpoint, "POST"):
            self._check_rate_limit()
            return await super().post(endpoint, json_data=data, **kwargs)
                
    @retry()
    @circuit_breaker()
    @monitored()
    @authenticated()
    async def put(self, endpoint: str, data: Any = None, **kwargs) -> Dict[str, Any]:
        """PUT request with decorators"""
        async with self._request_context(endpoint, "PUT"):
            self._check_rate_limit()
            return await super().put(endpoint, json_data=data, **kwargs)
                
    @retry()
    @circuit_breaker()
    @monitored()
    @authenticated()
    async def delete(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """DELETE request with decorators"""
        async with self._request_context(endpoint, "DELETE"):
            self._check_rate_limit()
            return await super().delete(endpoint, **kwargs)
            
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Default implementation - can be overridden"""
        return {}


# Backward compatibility alias
BaseServiceClient = BaseClient 