"""Core dependencies for Market Making Service"""

import logging
from typing import Optional, AsyncGenerator
from contextlib import asynccontextmanager

import redis.asyncio as redis
from pyignite import AsyncClient as IgniteClient
import pulsar
from httpx import AsyncClient

from app.config import settings

logger = logging.getLogger(__name__)

# Global clients
_ignite_client: Optional[IgniteClient] = None
_pulsar_client: Optional[pulsar.Client] = None
_redis_client: Optional[redis.Redis] = None
_http_client: Optional[AsyncClient] = None


async def init_services():
    """Initialize all service dependencies"""
    global _ignite_client, _pulsar_client, _redis_client, _http_client
    
    try:
        # Initialize Ignite
        _ignite_client = IgniteClient()
        await _ignite_client.connect([(settings.IGNITE_HOST, settings.IGNITE_PORT)])
        logger.info("Connected to Apache Ignite")
        
        # Initialize Pulsar
        _pulsar_client = pulsar.Client(
            settings.PULSAR_URL,
            operation_timeout_seconds=30,
            connection_timeout_seconds=30
        )
        logger.info("Connected to Apache Pulsar")
        
        # Initialize Redis
        _redis_client = redis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True
        )
        await _redis_client.ping()
        logger.info("Connected to Redis")
        
        # Initialize HTTP client
        _http_client = AsyncClient(timeout=30.0)
        logger.info("HTTP client initialized")
        
    except Exception as e:
        logger.error(f"Failed to initialize services: {e}")
        raise


async def cleanup_services():
    """Cleanup all service connections"""
    global _ignite_client, _pulsar_client, _redis_client, _http_client
    
    try:
        if _ignite_client:
            _ignite_client.close()
            logger.info("Closed Ignite connection")
            
        if _pulsar_client:
            _pulsar_client.close()
            logger.info("Closed Pulsar connection")
            
        if _redis_client:
            await _redis_client.close()
            logger.info("Closed Redis connection")
            
        if _http_client:
            await _http_client.aclose()
            logger.info("Closed HTTP client")
            
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")


async def get_ignite_client() -> IgniteClient:
    """Get Ignite client instance"""
    if not _ignite_client:
        raise RuntimeError("Ignite client not initialized")
    return _ignite_client


async def get_pulsar_client() -> pulsar.Client:
    """Get Pulsar client instance"""
    if not _pulsar_client:
        raise RuntimeError("Pulsar client not initialized")
    return _pulsar_client


async def get_redis_client() -> redis.Redis:
    """Get Redis client instance"""
    if not _redis_client:
        raise RuntimeError("Redis client not initialized")
    return _redis_client


async def get_http_client() -> AsyncClient:
    """Get HTTP client instance"""
    if not _http_client:
        raise RuntimeError("HTTP client not initialized")
    return _http_client


@asynccontextmanager
async def get_ignite_cache(cache_name: str):
    """Get Ignite cache by name"""
    client = await get_ignite_client()
    cache = await client.get_or_create_cache(cache_name)
    try:
        yield cache
    finally:
        pass  # Cache doesn't need explicit cleanup


class ServiceClients:
    """Container for external service clients"""
    
    def __init__(self):
        self._http_client: Optional[AsyncClient] = None
    
    async def init(self):
        """Initialize service clients"""
        self._http_client = await get_http_client()
    
    async def call_trading_core(self, method: str, endpoint: str, **kwargs) -> dict:
        """Call Trading Core Service"""
        url = f"{settings.TRADING_CORE_SERVICE_URL}{endpoint}"
        response = await self._http_client.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()
    
    async def call_risk_engine(self, method: str, endpoint: str, **kwargs) -> dict:
        """Call Risk Engine Service"""
        url = f"{settings.RISK_ENGINE_SERVICE_URL}{endpoint}"
        response = await self._http_client.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()
    
    async def call_oracle(self, method: str, endpoint: str, **kwargs) -> dict:
        """Call Oracle Service"""
        url = f"{settings.ORACLE_SERVICE_URL}{endpoint}"
        response = await self._http_client.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()
    
    async def call_analytics(self, method: str, endpoint: str, **kwargs) -> dict:
        """Call Analytics Service"""
        url = f"{settings.ANALYTICS_SERVICE_URL}{endpoint}"
        response = await self._http_client.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()


# Singleton instance
service_clients = ServiceClients()


async def get_service_clients() -> ServiceClients:
    """Get service clients instance"""
    if not service_clients._http_client:
        await service_clients.init()
    return service_clients 