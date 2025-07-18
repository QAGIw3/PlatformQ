"""
Service Client Library for PlatformQ

Provides resilient inter-service communication with circuit breakers,
retries, distributed tracing, and service discovery.
"""

import asyncio
import logging
import json
from typing import Dict, Any, Optional, Union, Type, TypeVar, Callable
from urllib.parse import urljoin
from datetime import datetime, timedelta
import os

import httpx
from pydantic import BaseModel
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from .resilience import with_resilience, CircuitBreaker, RateLimiter, get_connection_pool_manager
from .config import get_settings

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)

T = TypeVar('T', bound=BaseModel)


class ServiceClientError(Exception):
    """Base exception for service client errors"""
    pass


class ServiceNotFoundError(ServiceClientError):
    """Service not found in discovery"""
    pass


class ServiceTimeoutError(ServiceClientError):
    """Service call timed out"""
    pass


class ServiceClient:
    """
    Resilient HTTP client for inter-service communication.
    
    Features:
    - Automatic service discovery
    - Circuit breaker pattern
    - Exponential backoff retries
    - Rate limiting
    - Distributed tracing
    - Connection pooling
    """
    
    def __init__(self, 
                 service_name: str,
                 timeout: float = 30.0,
                 max_retries: int = 3,
                 circuit_breaker_threshold: int = 5,
                 rate_limit: float = 100.0,
                 use_service_discovery: bool = True):
        self.service_name = service_name
        self.timeout = timeout
        self.max_retries = max_retries
        self.use_service_discovery = use_service_discovery
        
        # Initialize resilience components
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=circuit_breaker_threshold,
            recovery_timeout=60,
            name=f"{service_name}_circuit_breaker"
        )
        
        self.rate_limiter = RateLimiter(rate_limit)
        
        # Get base URL
        self.base_url = self._get_service_url()
        
        # Get connection pool manager
        self.pool_manager = get_connection_pool_manager()
        self.session = self.pool_manager.get_session(
            self.base_url,
            timeout=self.timeout
        )
        
    def _get_service_url(self) -> str:
        """Get service URL from discovery or environment"""
        if self.use_service_discovery:
            # In Kubernetes, services are available at http://service-name:port
            # This could be enhanced with Consul or other service discovery
            return f"http://{self.service_name}:8000"
        else:
            # Fallback to environment variable
            env_var = f"{self.service_name.upper().replace('-', '_')}_URL"
            return os.getenv(env_var, f"http://{self.service_name}:8000")
            
    async def get(self, 
                  endpoint: str,
                  params: Optional[Dict[str, Any]] = None,
                  headers: Optional[Dict[str, str]] = None,
                  response_model: Optional[Type[T]] = None) -> Union[Dict[str, Any], T]:
        """Make GET request"""
        return await self._request(
            "GET", endpoint, params=params, headers=headers, response_model=response_model
        )
        
    async def post(self,
                   endpoint: str,
                   data: Optional[Union[Dict[str, Any], BaseModel]] = None,
                   params: Optional[Dict[str, Any]] = None,
                   headers: Optional[Dict[str, str]] = None,
                   response_model: Optional[Type[T]] = None) -> Union[Dict[str, Any], T]:
        """Make POST request"""
        if isinstance(data, BaseModel):
            json_data = data.dict()
        else:
            json_data = data
            
        return await self._request(
            "POST", endpoint, json=json_data, params=params, 
            headers=headers, response_model=response_model
        )
        
    async def put(self,
                  endpoint: str,
                  data: Optional[Union[Dict[str, Any], BaseModel]] = None,
                  params: Optional[Dict[str, Any]] = None,
                  headers: Optional[Dict[str, str]] = None,
                  response_model: Optional[Type[T]] = None) -> Union[Dict[str, Any], T]:
        """Make PUT request"""
        if isinstance(data, BaseModel):
            json_data = data.dict()
        else:
            json_data = data
            
        return await self._request(
            "PUT", endpoint, json=json_data, params=params,
            headers=headers, response_model=response_model
        )
        
    async def delete(self,
                     endpoint: str,
                     params: Optional[Dict[str, Any]] = None,
                     headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Make DELETE request"""
        return await self._request(
            "DELETE", endpoint, params=params, headers=headers
        )
        
    async def patch(self,
                    endpoint: str,
                    data: Optional[Union[Dict[str, Any], BaseModel]] = None,
                    params: Optional[Dict[str, Any]] = None,
                    headers: Optional[Dict[str, str]] = None,
                    response_model: Optional[Type[T]] = None) -> Union[Dict[str, Any], T]:
        """Make PATCH request"""
        if isinstance(data, BaseModel):
            json_data = data.dict()
        else:
            json_data = data
            
        return await self._request(
            "PATCH", endpoint, json=json_data, params=params,
            headers=headers, response_model=response_model
        )
        
    @with_resilience("service_client", circuit_breaker_threshold=5, rate_limit=100.0, max_retries=3)
    async def _request(self,
                       method: str,
                       endpoint: str,
                       response_model: Optional[Type[T]] = None,
                       **kwargs) -> Union[Dict[str, Any], T]:
        """Make HTTP request with resilience patterns"""
        url = urljoin(self.base_url, endpoint)
        
        # Start tracing span
        with tracer.start_as_current_span(
            f"{self.service_name}.{method}",
            attributes={
                "http.method": method,
                "http.url": url,
                "http.target": endpoint,
                "service.name": self.service_name
            }
        ) as span:
            try:
                # Check circuit breaker
                if self.circuit_breaker.state.name == "OPEN":
                    raise ServiceClientError(f"Circuit breaker open for {self.service_name}")
                    
                # Check rate limit
                if not self.rate_limiter.acquire(blocking=False):
                    raise ServiceClientError(f"Rate limit exceeded for {self.service_name}")
                    
                # Make request
                response = await self.session.request(method, url, **kwargs)
                
                # Add response info to span
                span.set_attribute("http.status_code", response.status_code)
                
                # Check response
                response.raise_for_status()
                
                # Parse response
                if response.headers.get("content-type", "").startswith("application/json"):
                    data = response.json()
                    
                    # Convert to response model if provided
                    if response_model:
                        return response_model(**data)
                    return data
                else:
                    return {"content": response.text}
                    
            except httpx.TimeoutException as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise ServiceTimeoutError(f"Request to {self.service_name} timed out")
                
            except httpx.HTTPStatusError as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                
                # Extract error details
                try:
                    error_data = e.response.json()
                    error_message = error_data.get("detail", str(e))
                except:
                    error_message = str(e)
                    
                raise ServiceClientError(f"Service error from {self.service_name}: {error_message}")
                
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                raise ServiceClientError(f"Error calling {self.service_name}: {str(e)}")


class ServiceRegistry:
    """
    Service registry for managing multiple service clients.
    """
    
    def __init__(self):
        self._clients: Dict[str, ServiceClient] = {}
        
    def get_client(self, service_name: str, **kwargs) -> ServiceClient:
        """Get or create a service client"""
        if service_name not in self._clients:
            self._clients[service_name] = ServiceClient(service_name, **kwargs)
        return self._clients[service_name]
        
    def register_client(self, service_name: str, client: ServiceClient):
        """Register a custom service client"""
        self._clients[service_name] = client
        
    async def close_all(self):
        """Close all service clients"""
        for client in self._clients.values():
            if hasattr(client.session, 'aclose'):
                await client.session.aclose()


# Global service registry
_service_registry = ServiceRegistry()


def get_service_client(service_name: str, **kwargs) -> ServiceClient:
    """Get a service client from the global registry"""
    return _service_registry.get_client(service_name, **kwargs)


async def close_all_clients():
    """Close all service clients"""
    await _service_registry.close_all()


class ServiceClientContext:
    """
    Context manager for service clients with automatic cleanup.
    """
    
    def __init__(self, service_name: str, **kwargs):
        self.service_name = service_name
        self.kwargs = kwargs
        self.client = None
        
    async def __aenter__(self) -> ServiceClient:
        self.client = ServiceClient(self.service_name, **self.kwargs)
        return self.client
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client and hasattr(self.client.session, 'aclose'):
            await self.client.session.aclose()


# Convenience functions for common services
class ServiceClients:
    """Convenience class for accessing common services"""
    
    @staticmethod
    def digital_asset() -> ServiceClient:
        return get_service_client("digital-asset-service")
        
    @staticmethod
    def auth() -> ServiceClient:
        return get_service_client("auth-service")
        
    @staticmethod
    def search() -> ServiceClient:
        return get_service_client("search-service")
        
    @staticmethod
    def graph_intelligence() -> ServiceClient:
        return get_service_client("graph-intelligence-service")
        
    @staticmethod
    def workflow() -> ServiceClient:
        return get_service_client("workflow-service")
        
    @staticmethod
    def mlops() -> ServiceClient:
        return get_service_client("mlops-service")
        
    @staticmethod
    def analytics() -> ServiceClient:
        return get_service_client("analytics-service")
        
    @staticmethod
    def storage_proxy() -> ServiceClient:
        return get_service_client("storage-proxy-service") 