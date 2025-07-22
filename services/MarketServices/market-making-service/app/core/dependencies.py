"""Dependency injection for market making service."""

from typing import Optional
import logging
from contextlib import asynccontextmanager
import httpx
import pulsar
import redis
from pyignite import Client as IgniteClient
from fastapi import Depends

from platformq_direct_comm import DirectCommunicator

from ..config import Settings
from ..risk import RiskChecker

logger = logging.getLogger(__name__)

# Global instances
_settings: Optional[Settings] = None
_ignite_client: Optional[IgniteClient] = None
_pulsar_client: Optional[pulsar.Client] = None
_redis_client: Optional[redis.Redis] = None
_http_client: Optional[httpx.AsyncClient] = None
_service_clients: Optional["ServiceClients"] = None
_direct_communicator: Optional[DirectCommunicator] = None
_risk_checker: Optional[RiskChecker] = None


async def init_services():
    """Initialize all service dependencies."""
    global _settings, _ignite_client, _pulsar_client, _redis_client
    global _http_client, _service_clients, _direct_communicator, _risk_checker
    
    # Load settings
    _settings = Settings()
    
    # Initialize Ignite
    try:
        _ignite_client = IgniteClient()
        _ignite_client.connect([
            (_settings.IGNITE_HOST, _settings.IGNITE_PORT)
        ])
        logger.info("Connected to Apache Ignite")
    except Exception as e:
        logger.error(f"Failed to connect to Ignite: {e}")
        _ignite_client = None
    
    # Initialize Pulsar
    try:
        _pulsar_client = pulsar.Client(_settings.PULSAR_URL)
        logger.info("Connected to Apache Pulsar")
    except Exception as e:
        logger.error(f"Failed to connect to Pulsar: {e}")
        _pulsar_client = None
    
    # Initialize Redis
    try:
        _redis_client = redis.from_url(_settings.REDIS_URL, decode_responses=True)
        await _redis_client.ping()
        logger.info("Connected to Redis")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        _redis_client = None
    
    # Initialize HTTP client
    _http_client = httpx.AsyncClient(timeout=30.0)
    
    # Initialize service clients
    _service_clients = ServiceClients()
    await _service_clients.init()
    
    # Initialize direct communication if enabled
    if _settings.ENABLE_DIRECT_COMM and _ignite_client:
        try:
            _direct_communicator = DirectCommunicator(
                service_id=_settings.SERVICE_ID,
                ignite_client=_ignite_client,
                batch_size=_settings.DIRECT_COMM_BATCH_SIZE,
                process_interval_ms=5.0  # 5ms for market making
            )
            await _direct_communicator.start()
            logger.info("Direct communication initialized")
        except Exception as e:
            logger.error(f"Failed to initialize direct communication: {e}")
            _direct_communicator = None
    
    # Initialize risk checker
    _risk_checker = RiskChecker(
        direct_communicator=_direct_communicator,
        service_clients=_service_clients,
        settings=_settings
    )
    logger.info("Risk checker initialized")
    
    logger.info("All services initialized successfully")


async def cleanup_services():
    """Cleanup all service connections."""
    global _ignite_client, _pulsar_client, _redis_client
    global _http_client, _direct_communicator
    
    if _direct_communicator:
        await _direct_communicator.stop()
        logger.info("Stopped direct communicator")
    
    if _ignite_client:
        _ignite_client.close()
        logger.info("Closed Ignite connection")
    
    if _pulsar_client:
        _pulsar_client.close()
        logger.info("Closed Pulsar connection")
    
    if _redis_client:
        _redis_client.close()
        logger.info("Closed Redis connection")
    
    if _http_client:
        await _http_client.aclose()
        logger.info("Closed HTTP client")
    
    logger.info("All services cleaned up")


# Dependency injection functions
async def get_settings() -> Settings:
    """Get settings instance."""
    return _settings


async def get_ignite_client() -> IgniteClient:
    """Get Ignite client."""
    if not _ignite_client:
        raise RuntimeError("Ignite client not initialized")
    return _ignite_client


async def get_pulsar_client() -> pulsar.Client:
    """Get Pulsar client."""
    if not _pulsar_client:
        raise RuntimeError("Pulsar client not initialized")
    return _pulsar_client


async def get_redis_client() -> redis.Redis:
    """Get Redis client."""
    if not _redis_client:
        raise RuntimeError("Redis client not initialized")
    return _redis_client


async def get_http_client() -> httpx.AsyncClient:
    """Get HTTP client."""
    if not _http_client:
        raise RuntimeError("HTTP client not initialized")
    return _http_client


@asynccontextmanager
async def get_ignite_cache(cache_name: str):
    """Get an Ignite cache by name."""
    if not _ignite_client:
        raise RuntimeError("Ignite client not initialized")
    
    cache = _ignite_client.get_or_create_cache(cache_name)
    try:
        yield cache
    finally:
        pass  # Cache cleanup if needed


class ServiceClients:
    """HTTP clients for service-to-service communication."""
    
    def __init__(self):
        self.settings = Settings()
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def init(self):
        """Initialize service clients."""
        pass
    
    async def call_trading_core(self, method: str, endpoint: str, **kwargs) -> dict:
        """Call Trading Core Service."""
        url = f"{self.settings.TRADING_CORE_SERVICE_URL}{endpoint}"
        response = await self.client.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()
    
    async def call_risk_engine(self, method: str, endpoint: str, **kwargs) -> dict:
        """Call Risk Engine Service."""
        url = f"{self.settings.RISK_ENGINE_SERVICE_URL}{endpoint}"
        response = await self.client.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()
    
    async def call_oracle(self, method: str, endpoint: str, **kwargs) -> dict:
        """Call Oracle Service."""
        url = f"{self.settings.ORACLE_SERVICE_URL}{endpoint}"
        response = await self.client.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()
    
    async def call_analytics(self, method: str, endpoint: str, **kwargs) -> dict:
        """Call Analytics Service."""
        url = f"{self.settings.ANALYTICS_SERVICE_URL}{endpoint}"
        response = await self.client.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()


async def get_service_clients() -> ServiceClients:
    """Get service clients."""
    if not _service_clients:
        raise RuntimeError("Service clients not initialized")
    return _service_clients


async def get_direct_communicator() -> Optional[DirectCommunicator]:
    """Get direct communicator if available."""
    return _direct_communicator


async def get_risk_checker() -> RiskChecker:
    """Get risk checker."""
    if not _risk_checker:
        raise RuntimeError("Risk checker not initialized")
    return _risk_checker 