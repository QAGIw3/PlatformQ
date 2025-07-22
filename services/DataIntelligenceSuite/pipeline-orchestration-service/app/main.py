"""
Pipeline Orchestration Service

Manages data pipeline creation, execution, monitoring, and optimization.
"""

import os
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI
from data_intelligence_common import (
    DataIntelligenceBaseService,
    create_data_intelligence_app,
    VaultConsulIntegration,
    ServiceMetadata,
    DataServiceConfig,
    StructuredLogger,
    MetricsCollector,
)
from data_intelligence_common.vault_consul import VaultConfig, ConsulConfig
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.event_subscriber import EventSubscriber
import consul.aio
import hvac

from .core import PipelineCoordinator, PipelineOptimizer
from .pipelines import PipelineRepository, PipelineScheduler, PipelineExecutor
from .monitoring import PipelineMonitor, PipelineMetricsCollector
from .events import PipelineEventProcessor
from .api import (
    pipeline_router,
    execution_router,
    monitoring_router,
    template_router
)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="pipeline-orchestration-service",
    version="1.0.0",
    description="Pipeline orchestration and management service",
    dependencies=["vault", "consul", "pulsar", "ignite"],
    health_checks=["pipeline_coordinator", "scheduler", "executor", "monitor"]
)

logger = StructuredLogger.get_logger(__name__)


class PipelineOrchestrationService(DataIntelligenceBaseService):
    """Pipeline Orchestration Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        
        # Core components
        self.coordinator: Optional[PipelineCoordinator] = None
        self.optimizer: Optional[PipelineOptimizer] = None
        self.repository: Optional[PipelineRepository] = None
        self.scheduler: Optional[PipelineScheduler] = None
        self.executor: Optional[PipelineExecutor] = None
        self.monitor: Optional[PipelineMonitor] = None
        self.metrics_collector: Optional[PipelineMetricsCollector] = None
        self.event_processor: Optional[PipelineEventProcessor] = None
    
    async def initialize_service(self):
        """Initialize service-specific components"""
        logger.info("initializing_pipeline_orchestration_service")
        
        # Initialize repository
        self.repository = PipelineRepository(self.vault_consul)
        await self.repository.initialize()
        
        # Initialize monitoring
        self.monitor = PipelineMonitor(
            self.vault_consul,
            self.event_publisher,
            self.metrics_collector
        )
        await self.monitor.start()
        
        # Initialize metrics collector
        self.metrics_collector = PipelineMetricsCollector(
            self.metrics_collector
        )
        
        # Initialize coordinator
        self.coordinator = PipelineCoordinator(
            repository=self.repository,
            monitor=self.monitor,
            vault_consul=self.vault_consul,
            event_publisher=self.event_publisher,
            metrics_collector=self.metrics_collector
        )
        await self.coordinator.initialize()
        
        # Initialize optimizer
        self.optimizer = PipelineOptimizer(
            coordinator=self.coordinator,
            repository=self.repository,
            vault_consul=self.vault_consul
        )
        await self.optimizer.initialize()
        
        # Initialize scheduler
        self.scheduler = PipelineScheduler(
            repository=self.repository,
            coordinator=self.coordinator,
            vault_consul=self.vault_consul
        )
        await self.scheduler.start()
        
        # Initialize executor
        self.executor = PipelineExecutor(
            coordinator=self.coordinator,
            monitor=self.monitor,
            vault_consul=self.vault_consul,
            event_publisher=self.event_publisher
        )
        await self.executor.initialize()
        
        # Initialize event processor
        if self.event_subscriber:
            self.event_processor = PipelineEventProcessor(
                event_subscriber=self.event_subscriber,
                coordinator=self.coordinator,
                executor=self.executor,
                monitor=self.monitor
            )
            await self.event_processor.start()
        
        # Register health checks
        self.health_manager.register_check(
            "pipeline_coordinator",
            self._check_coordinator_health,
            critical=True
        )
        self.health_manager.register_check(
            "scheduler",
            self._check_scheduler_health,
            critical=True
        )
        self.health_manager.register_check(
            "executor",
            self._check_executor_health,
            critical=True
        )
        self.health_manager.register_check(
            "monitor",
            self._check_monitor_health,
            critical=False
        )
        
        logger.info("pipeline_orchestration_service_initialized")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("cleaning_up_pipeline_orchestration_service")
        
        # Stop event processor
        if self.event_processor:
            await self.event_processor.stop()
        
        # Stop scheduler
        if self.scheduler:
            await self.scheduler.stop()
        
        # Stop monitor
        if self.monitor:
            await self.monitor.stop()
        
        # Cleanup coordinator
        if self.coordinator:
            await self.coordinator.cleanup()
        
        # Cleanup optimizer
        if self.optimizer:
            await self.optimizer.cleanup()
        
        # Cleanup executor
        if self.executor:
            await self.executor.cleanup()
        
        logger.info("pipeline_orchestration_service_cleaned_up")
    
    async def _check_coordinator_health(self) -> bool:
        """Check coordinator health"""
        return self.coordinator is not None and await self.coordinator.is_healthy()
    
    async def _check_scheduler_health(self) -> bool:
        """Check scheduler health"""
        return self.scheduler is not None and self.scheduler.is_running
    
    async def _check_executor_health(self) -> bool:
        """Check executor health"""
        return self.executor is not None and await self.executor.is_healthy()
    
    async def _check_monitor_health(self) -> bool:
        """Check monitor health"""
        return self.monitor is not None and self.monitor.is_running


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    # Get configuration from environment
    vault_addr = os.getenv("VAULT_ADDR", "http://localhost:8200")
    vault_token = os.getenv("VAULT_TOKEN")
    consul_host = os.getenv("CONSUL_HOST", "localhost")
    consul_port = int(os.getenv("CONSUL_PORT", "8500"))
    consul_token = os.getenv("CONSUL_TOKEN")
    
    # Create Vault client
    vault_client = hvac.Client(url=vault_addr, token=vault_token)
    
    # Create Consul client
    consul_client = consul.aio.Consul(
        host=consul_host,
        port=consul_port,
        token=consul_token
    )
    
    # Create event publisher
    pulsar_url = os.getenv("PULSAR_SERVICE_URL", "pulsar://localhost:6650")
    event_topic = os.getenv("EVENT_TOPIC_PREFIX", "persistent://platformq/data-intelligence")
    event_publisher = EventPublisher(pulsar_url, event_topic)
    
    # Create event subscriber
    event_subscriber = EventSubscriber(
        pulsar_url,
        event_topic,
        subscription_name="pipeline-orchestration-service"
    )
    
    # Create app and service
    app, service = create_data_intelligence_app(
        service_metadata=SERVICE_METADATA,
        vault_client=vault_client,
        consul_client=consul_client,
        event_publisher=event_publisher,
        cors_origins=["*"],  # Configure appropriately for production
        include_health_endpoint=True,
        include_metrics_endpoint=True,
        include_ready_endpoint=True
    )
    
    # Store service instance in app state
    app.state.service = service
    
    # Include routers
    app.include_router(pipeline_router)
    app.include_router(execution_router)
    app.include_router(monitoring_router)
    app.include_router(template_router)
    
    return app


# Create app instance
app = create_app()


# Add custom endpoints
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": SERVICE_METADATA.name,
        "version": SERVICE_METADATA.version,
        "description": SERVICE_METADATA.description,
        "status": "operational"
    }


if __name__ == "__main__":
    import uvicorn
    
    port = int(os.getenv("SERVICE_PORT", "8004"))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level="info"
    ) 