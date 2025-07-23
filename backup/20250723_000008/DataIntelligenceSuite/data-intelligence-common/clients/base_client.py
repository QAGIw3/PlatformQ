"""Base client for service integrations with Vault/Consul support."""

from typing import Dict, Any, Optional, List, Union, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import asyncio
import logging
import ssl
from abc import ABC, abstractmethod
from enum import Enum

import aiohttp
from aiohttp import ClientTimeout, ClientError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..monitoring import MetricsCollector
from ..vault_consul import VaultConsulIntegration, DataServiceConfig

logger = logging.getLogger(__name__)


class ServiceDiscoveryMode(Enum):
    """Service discovery modes"""
    CONSUL = "consul"
    STATIC = "static"
    DNS = "dns"


@dataclass
class ClientConfig:
    """Configuration for service client with Vault/Consul support"""
    service_name: str
    base_url: Optional[str] = None
    
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
    
    # Retry
    max_retries: int = 3
    retry_delay: float = 1.0
    retry_backoff: float = 2.0
    
    # Circuit breaker
    circuit_breaker_failures: int = 5
    circuit_breaker_timeout: int = 60
    
    # Authentication
    auth_token: Optional[str] = None
    auth_header: str = "Authorization"
    
    # SSL/TLS
    verify_ssl: bool = True
    use_mtls: bool = False
    ssl_cert: Optional[str] = None
    ssl_key: Optional[str] = None
    
    # Health check
    health_check_path: str = "/health"
    health_check_interval: int = 30
    
    # Load balancing
    load_balancing_strategy: str = "round_robin"  # round_robin, random, least_conn
    
    # Additional headers
    headers: Dict[str, str] = field(default_factory=dict)


class BaseServiceClient(ABC):
    """
    Base client for service integrations with Vault/Consul support.
    
    Features:
    - Dynamic service discovery via Consul
    - Dynamic credentials from Vault
    - Automatic credential renewal
    - Circuit breaker pattern
    - Health checking
    - Load balancing
    - mTLS support
    """
    
    def __init__(
        self,
        config: ClientConfig,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        metrics: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.metrics = metrics or MetricsCollector(f"{config.service_name}_client")
        
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
        
        # SSL context
        self._ssl_context: Optional[ssl.SSLContext] = None
        
        # Circuit breaker state
        self._circuit_breaker_failures: Dict[str, int] = {}
        self._circuit_breaker_last_failure: Dict[str, datetime] = {}
        
        # Background tasks
        self._renewal_task: Optional[asyncio.Task] = None
        self._health_check_task: Optional[asyncio.Task] = None
        
    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
        
    async def connect(self):
        """Initialize client connection"""
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
            
            # Discover service instances if using service discovery
            if self.config.use_service_discovery:
                await self._discover_service_instances()
                
            # Start background tasks
            if self.config.use_vault_credentials and self.vault_client:
                self._renewal_task = asyncio.create_task(self._credential_renewal_loop())
                
            if self.config.use_service_discovery and self.consul_client:
                self._health_check_task = asyncio.create_task(self._health_check_loop())
                
            logger.info(f"Connected to {self.config.service_name} service")
            
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            await self.close()
            raise
            
    async def close(self):
        """Close client connection"""
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
                
        # Close HTTP session
        if self._session:
            await self._session.close()
            
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
            import random
            instance = random.choice(self._service_instances)
        else:  # least_conn or default
            # For now, just use round robin
            instance = self._service_instances[self._current_instance_index]
            self._current_instance_index = (self._current_instance_index + 1) % len(self._service_instances)
            
        return f"http://{instance['address']}:{instance['port']}"
        
    async def _discover_service_instances(self):
        """Discover service instances from Consul"""
        if not self.consul_client:
            logger.warning("Consul client not available, using static URL")
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
            
    def _is_circuit_open(self, url: str) -> bool:
        """Check if circuit breaker is open for URL"""
        failures = self._circuit_breaker_failures.get(url, 0)
        if failures >= self.config.circuit_breaker_failures:
            last_failure = self._circuit_breaker_last_failure.get(url)
            if last_failure:
                if (datetime.utcnow() - last_failure).seconds < self.config.circuit_breaker_timeout:
                    return True
                else:
                    # Reset circuit breaker
                    self._circuit_breaker_failures[url] = 0
        return False
        
    def _record_success(self, url: str):
        """Record successful request"""
        self._circuit_breaker_failures[url] = 0
        
    def _record_failure(self, url: str):
        """Record failed request"""
        self._circuit_breaker_failures[url] = self._circuit_breaker_failures.get(url, 0) + 1
        self._circuit_breaker_last_failure[url] = datetime.utcnow()
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(ClientError)
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
            raise ClientError(f"Circuit breaker open for {url}")
            
        # Get credentials if needed
        creds = await self._get_credentials()
        
        # Build headers
        request_headers = dict(self.config.headers)
        if headers:
            request_headers.update(headers)
            
        # Add authentication
        if self.config.auth_token:
            request_headers[self.config.auth_header] = f"Bearer {self.config.auth_token}"
        elif creds and "token" in creds:
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
        
    def _create_ssl_context(self) -> ssl.SSLContext:
        """Create SSL context for HTTPS connections"""
        context = ssl.create_default_context()
        
        if not self.config.verify_ssl:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
        if self.config.use_mtls and self.config.ssl_cert and self.config.ssl_key:
            context.load_cert_chain(self.config.ssl_cert, self.config.ssl_key)
            
        return context
        
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