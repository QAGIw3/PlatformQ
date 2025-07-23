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
import pulsar

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.event_publisher import EventPublisher

from ..vault_consul import VaultConsulIntegration, DataServiceConfig
from ..monitoring import MetricsCollector, StructuredLogger
from ..event_handlers import BaseEventProcessor
from ..core.caching import CacheManager, DistributedCacheClient
from .health import HealthCheckManager, HealthStatus
from .config import ServiceConfig, CacheConfig, EventConfig
from .middleware import CircuitBreakerManager

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
    - Caching with Ignite
    - Rate limiting
    - Circuit breakers
    """
    
    def __init__(
        self,
        metadata: ServiceMetadata,
        config: Optional[ServiceConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        event_publisher: Optional[EventPublisher] = None
    ):
        self.metadata = metadata
        self.config = config or ServiceConfig(name=metadata.name, version=metadata.version)
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
        
        # Caching
        self.cache_manager: Optional[CacheManager] = None
        self.distributed_cache: Optional[DistributedCacheClient] = None
        
        # Circuit breakers
        self.circuit_breaker_manager: Optional[CircuitBreakerManager] = None
        
        # Pulsar client for events
        self.pulsar_client: Optional[pulsar.Client] = None
        self._publishers: Dict[str, pulsar.Producer] = {}
        self._consumers: Dict[str, pulsar.Consumer] = {}
        
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
        
        # Cache metrics
        self.metrics.cache_hits = Counter(
            "data_intelligence_cache_hits_total",
            "Total cache hits",
            ["service", "cache"]
        )
        
        self.metrics.cache_misses = Counter(
            "data_intelligence_cache_misses_total",
            "Total cache misses",
            ["service", "cache"]
        )
        
        # Event metrics
        self.metrics.events_published = Counter(
            "data_intelligence_events_published_total",
            "Total events published",
            ["service", "event_type"]
        )
        
        self.metrics.events_consumed = Counter(
            "data_intelligence_events_consumed_total",
            "Total events consumed",
            ["service", "event_type"]
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
                
            # Initialize caching if enabled
            if self.config.enable_caching:
                await self._initialize_cache()
                
            # Initialize Pulsar client if events enabled
            if self.config.enable_events:
                await self._initialize_pulsar()
                
            # Initialize circuit breaker manager
            if self.config.enable_circuit_breaker:
                self.circuit_breaker_manager = CircuitBreakerManager(
                    service_name=self.metadata.name,
                    fail_max=self.config.circuit_breaker_failures,
                    reset_timeout=self.config.circuit_breaker_timeout,
                    expected_exception=self.config.circuit_breaker_expected_exception
                )
                
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
            
            # Close Pulsar connections
            if self.pulsar_client:
                try:
                    # Close all producers
                    for producer in self._publishers.values():
                        producer.close()
                    self._publishers.clear()
                    
                    # Close all consumers
                    for consumer in self._consumers.values():
                        consumer.close()
                    self._consumers.clear()
                    
                    # Close client
                    self.pulsar_client.close()
                    logger.info("Pulsar client closed")
                except Exception as e:
                    logger.error(f"Error closing Pulsar: {e}")
                    
            # Close cache connections
            if self.distributed_cache:
                try:
                    await self.distributed_cache.close()
                    logger.info("Cache connections closed")
                except Exception as e:
                    logger.error(f"Error closing cache: {e}")
            
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
        
    async def _initialize_cache(self):
        """Initialize Ignite cache connection."""
        try:
            from pyignite.aio import AioClient
            
            # Create Ignite client
            ignite_client = AioClient()
            await ignite_client.connect(self.config.ignite_nodes)
            
            # Create distributed cache client wrapper
            self.distributed_cache = DistributedCacheClient(ignite_client)
            
            # Initialize cache manager
            self.cache_manager = CacheManager(self.distributed_cache)
            
            # Create standard caches
            cache_config = CacheConfig()
            await self.cache_manager.create_cache(
                cache_config.session_cache,
                ttl=cache_config.session_ttl
            )
            await self.cache_manager.create_cache(
                cache_config.configuration_cache,
                ttl=cache_config.configuration_ttl
            )
            await self.cache_manager.create_cache(
                cache_config.query_results_cache,
                ttl=cache_config.query_results_ttl
            )
            
            logger.info("Cache initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize cache: {e}")
            # Cache is optional, don't fail startup
            
    async def _initialize_pulsar(self):
        """Initialize Pulsar client."""
        try:
            self.pulsar_client = pulsar.Client(
                self.config.pulsar_url,
                # Add authentication if needed
            )
            
            logger.info("Pulsar client initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Pulsar: {e}")
            # Events are optional, don't fail startup
        
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
            
        # Cache health check
        if self.distributed_cache:
            self.health_manager.add_check(
                "cache",
                self._check_cache_health
            )
            
        # Pulsar health check
        if self.pulsar_client:
            self.health_manager.add_check(
                "pulsar",
                self._check_pulsar_health
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
            
    async def _check_cache_health(self) -> bool:
        """Check Ignite cache health."""
        try:
            # Check if the distributed cache client is initialized
            return self.distributed_cache is not None and self.distributed_cache.is_connected()
        except:
            return False
            
    async def _check_pulsar_health(self) -> bool:
        """Check Pulsar client health."""
        try:
            # Check if the pulsar client is initialized and connected
            return self.pulsar_client is not None and self.pulsar_client.is_connected()
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
        """Discover service instances."""
        if self.consul_client:
            return await self.consul_client.discover_service(service_name)
        return []
        
    async def get_service_url(self, service_name: str) -> str:
        """Get URL for another service."""
        if self.vault_consul:
            return await self.vault_consul.get_service_url(service_name)
        raise ValueError(f"Service discovery not available")
        
    async def publish_event(self, topic: str, event: Dict[str, Any],
                           event_type: Optional[str] = None) -> bool:
        """Publish event to Pulsar."""
        if not self.pulsar_client:
            return False
            
        try:
            # Get or create producer
            if topic not in self._publishers:
                self._publishers[topic] = self.pulsar_client.create_producer(topic)
                
            # Add metadata
            event["_metadata"] = {
                "source": self.metadata.name,
                "timestamp": datetime.utcnow().isoformat(),
                "event_type": event_type or "generic"
            }
            
            # Publish
            import json
            self._publishers[topic].send(
                json.dumps(event).encode('utf-8')
            )
            
            # Update metrics
            self.metrics.events_published.labels(
                service=self.metadata.name,
                event_type=event_type or "generic"
            ).inc()
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            return False
            
    def subscribe_event(self, topic: str, subscription: str,
                       handler: Callable[[Dict[str, Any]], None]) -> None:
        """Subscribe to events from Pulsar."""
        if not self.pulsar_client:
            return
            
        async def consumer_loop():
            try:
                # Create consumer
                consumer = self.pulsar_client.subscribe(
                    topic,
                    subscription,
                    consumer_type=pulsar.ConsumerType.Shared
                )
                self._consumers[f"{topic}:{subscription}"] = consumer
                
                while True:
                    msg = consumer.receive()
                    
                    try:
                        # Parse message
                        import json
                        data = msg.data().decode('utf-8')
                        event = json.loads(data)
                        
                        # Extract metadata
                        metadata = event.get("_metadata", {})
                        event_type = metadata.get("event_type", "generic")
                        
                        # Update metrics
                        self.metrics.events_consumed.labels(
                            service=self.metadata.name,
                            event_type=event_type
                        ).inc()
                        
                        # Call handler
                        if asyncio.iscoroutinefunction(handler):
                            await handler(event)
                        else:
                            handler(event)
                            
                        # Acknowledge
                        consumer.acknowledge(msg)
                        
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        consumer.negative_acknowledge(msg)
                        
            except Exception as e:
                logger.error(f"Consumer loop error: {e}")
                
        # Start consumer loop
        asyncio.create_task(consumer_loop())
        
    def get_circuit_breaker(self, name: str):
        """Get circuit breaker for external service calls."""
        if self.circuit_breaker_manager:
            return self.circuit_breaker_manager.get_circuit_breaker(name)
        return None 