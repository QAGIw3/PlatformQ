"""Base service class for DataIntelligenceSuite services."""

from typing import Dict, Any, Optional, List, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
import asyncio
import logging
from abc import ABC, abstractmethod

from fastapi import FastAPI
from prometheus_client import Counter, Histogram, Gauge

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.event_publisher import EventPublisher

from ..vault_consul import VaultConsulIntegration, DataServiceConfig
from ..monitoring import MetricsCollector, StructuredLogger
from ..event_handlers import BaseEventProcessor
from .health import HealthCheckManager, HealthStatus

logger = logging.getLogger(__name__)


@dataclass
class ServiceMetadata:
    """Service metadata for registration and discovery."""
    
    name: str
    version: str
    description: str
    capabilities: List[str]
    dependencies: List[str]
    
    # Resource requirements
    min_memory_mb: int = 512
    min_cpu_cores: float = 0.5
    
    # Performance characteristics
    max_concurrent_requests: int = 100
    request_timeout_seconds: int = 300
    
    # Data characteristics
    data_sources: List[str] = None
    data_outputs: List[str] = None
    
    def __post_init__(self):
        if self.data_sources is None:
            self.data_sources = []
        if self.data_outputs is None:
            self.data_outputs = []


class DataIntelligenceBaseService(ABC):
    """
    Base service class for all DataIntelligenceSuite services.
    
    Provides:
    - Standardized initialization and shutdown
    - Vault/Consul integration
    - Health checking
    - Metrics collection
    - Event processing
    - Configuration management
    - Service discovery
    """
    
    def __init__(
        self,
        metadata: ServiceMetadata,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        event_publisher: Optional[EventPublisher] = None
    ):
        self.metadata = metadata
        self.app: Optional[FastAPI] = None
        
        # Core integrations
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.event_publisher = event_publisher
        
        # Unified Vault/Consul integration
        self.vault_consul: Optional[VaultConsulIntegration] = None
        
        # Health checking
        self.health_manager = HealthCheckManager(metadata.name)
        
        # Metrics
        self.metrics = MetricsCollector(metadata.name)
        self._setup_base_metrics()
        
        # Event processing
        self.event_processor: Optional[BaseEventProcessor] = None
        
        # Service state
        self._initialized = False
        self._shutting_down = False
        
        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        
    def _setup_base_metrics(self):
        """Set up base metrics for all services."""
        # Request metrics
        self.metrics.request_counter = Counter(
            "data_intelligence_requests_total",
            "Total number of requests",
            ["service", "method", "status"]
        )
        
        self.metrics.request_duration = Histogram(
            "data_intelligence_request_duration_seconds",
            "Request duration in seconds",
            ["service", "method"]
        )
        
        # Service health
        self.metrics.health_status = Gauge(
            "data_intelligence_health_status",
            "Service health status (0=unhealthy, 1=degraded, 2=healthy)",
            ["service"]
        )
        
        # Resource usage
        self.metrics.active_connections = Gauge(
            "data_intelligence_active_connections",
            "Number of active connections",
            ["service", "type"]
        )
        
    @asynccontextmanager
    async def lifespan(self, app: FastAPI):
        """Lifespan context manager for FastAPI."""
        # Startup
        await self.startup()
        
        yield
        
        # Shutdown
        await self.shutdown()
        
    async def startup(self):
        """Initialize the service."""
        if self._initialized:
            logger.warning(f"Service {self.metadata.name} already initialized")
            return
            
        try:
            logger.info(f"Starting {self.metadata.name} v{self.metadata.version}")
            
            # Initialize Vault/Consul if clients provided
            if self.vault_client and self.consul_client:
                await self._initialize_vault_consul()
                
            # Initialize event processor
            if self.event_publisher:
                await self._initialize_event_processor()
                
            # Register health checks
            await self._register_health_checks()
            
            # Service-specific initialization
            await self.initialize_service()
            
            # Start background tasks
            await self._start_background_tasks()
            
            self._initialized = True
            self.health_manager.set_status(HealthStatus.HEALTHY)
            
            logger.info(f"Service {self.metadata.name} started successfully")
            
        except Exception as e:
            logger.error(f"Failed to start service: {e}")
            self.health_manager.set_status(HealthStatus.UNHEALTHY, str(e))
            raise
            
    async def shutdown(self):
        """Shutdown the service gracefully."""
        if self._shutting_down:
            return
            
        self._shutting_down = True
        logger.info(f"Shutting down {self.metadata.name}")
        
        try:
            # Stop background tasks
            await self._stop_background_tasks()
            
            # Service-specific cleanup
            await self.cleanup_service()
            
            # Cleanup integrations
            if self.vault_consul:
                await self.vault_consul.shutdown()
                
            if self.event_processor:
                await self.event_processor.stop()
                
            logger.info(f"Service {self.metadata.name} shutdown complete")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
            
    async def _initialize_vault_consul(self):
        """Initialize Vault/Consul integration."""
        config = DataServiceConfig(
            service_name=self.metadata.name,
            service_version=self.metadata.version,
            max_concurrent_requests=self.metadata.max_concurrent_requests,
            request_timeout_seconds=self.metadata.request_timeout_seconds,
            tags=[
                "data-intelligence",
                f"version:{self.metadata.version}",
                *[f"capability:{cap}" for cap in self.metadata.capabilities],
                *[f"dependency:{dep}" for dep in self.metadata.dependencies]
            ],
            metadata={
                "description": self.metadata.description,
                "min_memory_mb": str(self.metadata.min_memory_mb),
                "min_cpu_cores": str(self.metadata.min_cpu_cores),
                "data_sources": ",".join(self.metadata.data_sources),
                "data_outputs": ",".join(self.metadata.data_outputs)
            }
        )
        
        self.vault_consul = VaultConsulIntegration(
            self.vault_client,
            self.consul_client,
            config
        )
        
        await self.vault_consul.initialize()
        
    async def _initialize_event_processor(self):
        """Initialize event processor."""
        # This will be implemented by derived classes
        pass
        
    async def _register_health_checks(self):
        """Register standard health checks."""
        # Vault/Consul connectivity
        if self.vault_consul:
            self.health_manager.add_check(
                "vault_consul",
                self._check_vault_consul_health
            )
            
        # Database connectivity
        for data_source in self.metadata.data_sources:
            self.health_manager.add_check(
                f"database_{data_source}",
                lambda: self._check_database_health(data_source)
            )
            
        # Event publisher
        if self.event_publisher:
            self.health_manager.add_check(
                "event_publisher",
                self._check_event_publisher_health
            )
            
    async def _check_vault_consul_health(self) -> bool:
        """Check Vault/Consul health."""
        try:
            # Check if we can get a config value
            test_value = await self.vault_consul.get_config("health_check")
            return True
        except:
            return False
            
    async def _check_database_health(self, database: str) -> bool:
        """Check database connectivity."""
        try:
            async with self.vault_consul.get_database_connection(database) as conn:
                # Simple connectivity check
                return conn is not None
        except:
            return False
            
    async def _check_event_publisher_health(self) -> bool:
        """Check event publisher health."""
        try:
            # Assume event publisher has a health check method
            return await self.event_publisher.is_healthy()
        except:
            return False
            
    async def _start_background_tasks(self):
        """Start background tasks."""
        # Health check updates
        self._background_tasks.append(
            asyncio.create_task(self._health_check_loop())
        )
        
        # Metrics reporting
        self._background_tasks.append(
            asyncio.create_task(self._metrics_reporting_loop())
        )
        
    async def _stop_background_tasks(self):
        """Stop background tasks."""
        for task in self._background_tasks:
            task.cancel()
            
        await asyncio.gather(
            *self._background_tasks,
            return_exceptions=True
        )
        
    async def _health_check_loop(self):
        """Periodically run health checks."""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                # Run health checks
                overall_health = await self.health_manager.check_health()
                
                # Update metrics
                health_value = {
                    HealthStatus.HEALTHY: 2,
                    HealthStatus.DEGRADED: 1,
                    HealthStatus.UNHEALTHY: 0
                }[overall_health.status]
                
                self.metrics.health_status.labels(
                    service=self.metadata.name
                ).set(health_value)
                
                # Report to Consul if integrated
                if self.vault_consul:
                    await self.vault_consul.report_health(
                        overall_health.status,
                        overall_health.message or ""
                    )
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                
    async def _metrics_reporting_loop(self):
        """Periodically report metrics."""
        while True:
            try:
                await asyncio.sleep(60)  # Report every minute
                
                # Get service metrics
                metrics = self.get_service_metrics()
                
                # Log metrics
                logger.info(f"Service metrics: {metrics}")
                
                # Could also push to monitoring system
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics reporting: {e}")
                
    def get_service_metrics(self) -> Dict[str, Any]:
        """Get current service metrics."""
        metrics = {
            "service": self.metadata.name,
            "version": self.metadata.version,
            "uptime_seconds": (datetime.utcnow() - self.health_manager.startup_time).total_seconds(),
            "health_status": self.health_manager.overall_status.value,
        }
        
        # Add Vault/Consul metrics
        if self.vault_consul:
            metrics.update(self.vault_consul.get_integration_metrics())
            
        return metrics
        
    # Abstract methods to be implemented by derived services
    @abstractmethod
    async def initialize_service(self):
        """Initialize service-specific components."""
        pass
        
    @abstractmethod
    async def cleanup_service(self):
        """Cleanup service-specific components."""
        pass
        
    # Helper methods for derived services
    async def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value."""
        if self.vault_consul:
            return await self.vault_consul.get_config(key, default)
        return default
        
    async def discover_service(self, service_name: str) -> List[Dict[str, Any]]:
        """Discover instances of another service."""
        if self.vault_consul:
            return await self.vault_consul.discover_service(service_name)
        return []
        
    async def get_service_url(self, service_name: str) -> str:
        """Get URL for another service."""
        if self.vault_consul:
            return await self.vault_consul.get_service_url(service_name)
        raise ValueError(f"Service discovery not available")
        
    async def publish_event(self, topic: str, event: Any):
        """Publish an event."""
        if self.event_publisher:
            await self.event_publisher.publish(topic, event)
        else:
            logger.warning(f"Event publisher not available, dropping event to {topic}") 