"""Base service class for DataIntelligenceSuite services."""

from typing import Dict, Any, Optional, List, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
import asyncio
import logging
from abc import ABC

from fastapi import FastAPI
from prometheus_client import Counter, Histogram, Gauge
import pulsar

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.event_publisher import EventPublisher

from ..vault_consul import VaultConsulIntegration, DataServiceConfig
from ..monitoring import MetricsCollector, StructuredLogger
from ..core.events import BaseEventProcessor
from ..core.caching import CacheManager, DistributedCacheClient
from ..monitoring import HealthCheckManager, HealthStatus
from .config import ServiceConfig, CacheConfig, EventConfig
from .middleware import CircuitBreakerManager
from ..core.mixins import ServiceMixin

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


class DataIntelligenceBaseService(ServiceMixin, ABC):
    """
    Base service class for all DataIntelligenceSuite services.
    
    Now uses mixin-based architecture for common functionality:
    - LifecycleMixin: Initialization, start, stop, background tasks
    - MonitoringMixin: Metrics, events, health checks
    - VaultConsulMixin: Secret and configuration management
    - ConfigurationMixin: Configuration handling
    - ResilienceMixin: Retry and circuit breaker patterns
    
    Service-specific functionality:
    - FastAPI application management
    - Service discovery and registration
    - Pulsar client management
    - Event processing
    """
    
    def __init__(
        self,
        metadata: ServiceMetadata,
        config: Optional[ServiceConfig] = None,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        event_publisher: Optional[EventPublisher] = None,
        metrics_collector: Optional[MetricsCollector] = None,
        event_bus: Optional[Any] = None
    ):
        # Initialize mixins
        super().__init__(
            vault_client=vault_client,
            consul_client=consul_client,
            metrics_collector=metrics_collector,
            event_bus=event_bus,
            config=config.__dict__ if config else {}
        )
        
        self.metadata = metadata
        self.config = config or ServiceConfig(name=metadata.name, version=metadata.version)
        self.app: Optional[FastAPI] = None
        
        # Additional service-specific components
        self.event_publisher = event_publisher
        
        # Unified Vault/Consul integration
        self.vault_consul: Optional[VaultConsulIntegration] = None
        
        # Health checking (in addition to mixin functionality)
        self.health_manager = HealthCheckManager(metadata.name)
        
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
        
        # Register health checks from mixin
        self._register_service_health_checks()
        
    def _register_service_health_checks(self):
        """Register service-specific health checks"""
        self.register_health_check("vault_consul", self._check_vault_consul_health)
        self.register_health_check("pulsar", self._check_pulsar_health)
        self.register_health_check("cache", self._check_cache_health)
        
    async def _check_vault_consul_health(self) -> bool:
        """Check Vault/Consul health"""
        if not self.vault_consul:
            return True  # Not configured, so consider healthy
        return await self.vault_consul.health_check()
        
    async def _check_pulsar_health(self) -> bool:
        """Check Pulsar health"""
        if not self.pulsar_client:
            return True  # Not configured
        try:
            # Simple health check - can be enhanced
            return True
        except Exception:
            return False
            
    async def _check_cache_health(self) -> bool:
        """Check cache health"""
        if not self.cache_manager:
            return True  # Not configured
        try:
            # Simple health check
            await self.cache_manager.get("health_check", "test")
            return True
        except Exception:
            return False
    
    async def _initialize_internal(self):
        """Service-specific initialization"""
        # Setup Vault/Consul integration
        if self.vault_client and self.consul_client:
            data_service_config = DataServiceConfig(
                service_name=self.metadata.name,
                service_type="data-intelligence",
                vault_mount_path=f"data-intelligence/{self.metadata.name}",
                consul_key_prefix=f"data-intelligence/{self.metadata.name}"
            )
            
            self.vault_consul = VaultConsulIntegration(
                vault_client=self.vault_client,
                consul_client=self.consul_client,
                config=data_service_config
            )
            
            await self.vault_consul.initialize()
            
        # Initialize health manager
        await self.health_manager.initialize()
        
        # Initialize caching
        if self.config.caching.enable_distributed_cache:
            await self._initialize_caching()
            
        # Initialize Pulsar
        if self.config.events.enabled:
            await self._initialize_pulsar()
            
        # Initialize circuit breakers
        self.circuit_breaker_manager = CircuitBreakerManager()
        
        # Publish initialization event
        await self.publish_lifecycle_event("initialized")
        
    async def _initialize_caching(self):
        """Initialize caching subsystem"""
        from ..core.caching import CacheManager, CacheConfig
        
        # Get Ignite nodes from config or Consul
        ignite_nodes = await self._get_ignite_nodes()
        
        self.cache_manager = CacheManager(
            ignite_nodes=ignite_nodes,
            service_name=self.metadata.name,
            vault_client=self.vault_client,
            consul_client=self.consul_client,
            metrics_collector=self.metrics,
            enable_encryption=self.config.security.enable_encryption
        )
        
        await self.cache_manager.initialize()
        
        # Setup default caches
        await self._setup_default_caches()
        
    async def _get_ignite_nodes(self) -> List[tuple]:
        """Get Ignite nodes from Consul or config"""
        if self.consul_client:
            # Get from Consul
            services = await self.consul_client.get_service("ignite")
            return [(s["ServiceAddress"], s["ServicePort"]) for s in services]
        else:
            # Fallback to config
            return [("localhost", 10800)]
            
    async def _setup_default_caches(self):
        """Setup default caches for the service"""
        from ..core.caching import CacheConfig, CacheStrategy
        
        # Session cache
        await self.cache_manager.create_cache(
            CacheConfig(
                name=self.config.caching.session_cache,
                strategy=CacheStrategy.CACHE_ASIDE,
                ttl=self.config.caching.session_ttl,
                encrypt_data=True
            )
        )
        
        # Configuration cache
        await self.cache_manager.create_cache(
            CacheConfig(
                name=self.config.caching.configuration_cache,
                strategy=CacheStrategy.READ_THROUGH,
                ttl=self.config.caching.configuration_ttl,
                loader=self._load_configuration
            )
        )
        
        # Query results cache
        await self.cache_manager.create_cache(
            CacheConfig(
                name=self.config.caching.query_results_cache,
                strategy=CacheStrategy.CACHE_ASIDE,
                ttl=self.config.caching.query_results_ttl
            )
        )
        
    async def _load_configuration(self, key: str) -> Any:
        """Load configuration from Consul"""
        if self.consul_client:
            return await self.consul_client.get_value(key)
        return None
        
    async def _initialize_pulsar(self):
        """Initialize Pulsar client and setup topics"""
        pulsar_url = await self.get_config("pulsar_url", "pulsar://localhost:6650")
        
        self.pulsar_client = pulsar.Client(
            pulsar_url,
            authentication=None,  # TODO: Add authentication
            operation_timeout_seconds=30
        )
        
        # Setup default topics
        await self._setup_default_topics()
        
    async def _setup_default_topics(self):
        """Setup default Pulsar topics"""
        # Create event topic producer
        event_topic = f"persistent://data-intelligence/{self.metadata.name}/events"
        self._publishers["events"] = self.pulsar_client.create_producer(event_topic)
        
        # Create metrics topic producer
        metrics_topic = f"persistent://data-intelligence/{self.metadata.name}/metrics"
        self._publishers["metrics"] = self.pulsar_client.create_producer(metrics_topic)
        
    async def _start_internal(self):
        """Service-specific start logic"""
        # Register with Consul
        if self.consul_client and self.config.service_discovery.enabled:
            await self.register_service(
                name=self.metadata.name,
                port=self.config.port,
                tags=[
                    f"version:{self.metadata.version}",
                    "data-intelligence",
                    *self.metadata.capabilities
                ]
            )
            
        # Start background tasks
        if self.config.monitoring.enable_metrics:
            self.create_background_task(self._metrics_reporter())
            
        if self.config.events.enabled:
            self.create_background_task(self._event_processor())
            
        # Publish start event
        await self.publish_lifecycle_event("started")
        
    async def _stop_internal(self):
        """Service-specific stop logic"""
        # Close Pulsar connections
        if self.pulsar_client:
            for producer in self._publishers.values():
                producer.close()
            for consumer in self._consumers.values():
                consumer.close()
            self.pulsar_client.close()
            
        # Shutdown cache manager
        if self.cache_manager:
            await self.cache_manager.shutdown()
            
        # Shutdown health manager
        await self.health_manager.shutdown()
        
        # Deregister from Consul
        if self.consul_client and self.config.service_discovery.enabled:
            # Consul client handles deregistration
            pass
            
        # Publish stop event
        await self.publish_lifecycle_event("stopped")
        
    async def _metrics_reporter(self):
        """Background task to report metrics"""
        while self.is_running:
            try:
                # Collect and publish metrics
                metrics_data = {
                    "service": self.metadata.name,
                    "timestamp": datetime.utcnow().isoformat(),
                    "uptime": self.uptime.total_seconds() if self.uptime else 0,
                    "health": await self.check_health()
                }
                
                if "metrics" in self._publishers:
                    self._publishers["metrics"].send_async(
                        json.dumps(metrics_data).encode('utf-8')
                    )
                    
                await asyncio.sleep(self.config.monitoring.metrics_interval_seconds)
                
            except Exception as e:
                logger.error(f"Error in metrics reporter: {e}")
                await asyncio.sleep(60)  # Back off on error
                
    async def _event_processor(self):
        """Background task to process events"""
        # This would be implemented by specific services
        pass
        
    def create_pulsar_producer(self, topic: str) -> pulsar.Producer:
        """Create a Pulsar producer for a topic"""
        if not self.pulsar_client:
            raise RuntimeError("Pulsar client not initialized")
            
        if topic not in self._publishers:
            self._publishers[topic] = self.pulsar_client.create_producer(topic)
            
        return self._publishers[topic]
        
    def create_pulsar_consumer(self, topic: str, subscription: str) -> pulsar.Consumer:
        """Create a Pulsar consumer for a topic"""
        if not self.pulsar_client:
            raise RuntimeError("Pulsar client not initialized")
            
        consumer_key = f"{topic}:{subscription}"
        if consumer_key not in self._consumers:
            self._consumers[consumer_key] = self.pulsar_client.subscribe(
                topic,
                subscription,
                consumer_type=pulsar.ConsumerType.Shared
            )
            
        return self._consumers[consumer_key]
        
    @asynccontextmanager
    async def lifespan(self):
        """Async context manager for service lifecycle"""
        await self.initialize()
        await self.start()
        try:
            yield
        finally:
            await self.stop()
            
    def mount_app(self, app: FastAPI):
        """Mount FastAPI application"""
        self.app = app
        
        # Add health endpoint
        @app.get("/health")
        async def health():
            return await self.check_health()
            
        # Add metrics endpoint if enabled
        if self.config.monitoring.enable_metrics:
            from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
            from fastapi.responses import Response
            
            @app.get("/metrics")
            async def metrics():
                return Response(
                    generate_latest(),
                    media_type=CONTENT_TYPE_LATEST
                ) 