"""
Data Quality Service

Autonomous data quality management for the DataIntelligenceSuite.
"""

import os
from typing import Optional

from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    DataIntelligenceBaseService,
    BaseEventProcessor
)
from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from platformq_shared.event_publisher import EventPublisher

from .core.quality_engine import DataQualityEngine
from .core.profiler import DataQualityProfiler
from .remediation.orchestrator import RemediationOrchestrator
from .monitoring.quality_monitor import QualityMonitor
from .rules.rule_engine import RuleEngine
from .api import quality_api, rules_api, remediation_api, monitoring_api
from .integrations.lineage_client import DataLineageClient
from .integrations.pipeline_client import PipelineClient

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="data-quality-service",
    version="1.0.0",
    description="Autonomous data quality management with ML-driven remediation",
    capabilities=[
        "quality-profiling",
        "anomaly-detection",
        "data-validation",
        "self-healing",
        "quality-monitoring",
        "rule-management",
        "ml-remediation"
    ],
    dependencies=[
        "data-platform-service",
        "ml-platform-service",
        "dih-service"
    ],
    data_sources=[
        "postgres",
        "cassandra",
        "elasticsearch",
        "ignite"
    ],
    data_outputs=["quality-reports", "events"],
    min_memory_mb=1024,
    min_cpu_cores=1.0
)


class DataQualityService(DataIntelligenceBaseService):
    """Data Quality Service implementation."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        self.quality_engine: Optional[DataQualityEngine] = None
        self.profiler: Optional[DataQualityProfiler] = None
        self.remediation_orchestrator: Optional[RemediationOrchestrator] = None
        self.quality_monitor: Optional[QualityMonitor] = None
        self.rule_engine: Optional[RuleEngine] = None
        self.event_processor: Optional[DataQualityEventProcessor] = None
        
    async def initialize_service(self):
        """Initialize Data Quality Service components."""
        # Initialize rule engine
        self.rule_engine = RuleEngine(
            vault_consul=self.vault_consul,
            metrics_collector=self.metrics
        )
        await self.rule_engine.initialize()
        
        # Initialize profiler
        self.profiler = DataQualityProfiler(
            vault_consul=self.vault_consul,
            rule_engine=self.rule_engine,
            metrics_collector=self.metrics
        )
        await self.profiler.initialize()
        
        # Initialize lineage and pipeline clients
        lineage_client = DataLineageClient(
            vault_consul=self.vault_consul,
            event_publisher=self.event_publisher
        )
        
        pipeline_client = PipelineClient(
            vault_consul=self.vault_consul,
            event_publisher=self.event_publisher
        )
        
        # Initialize remediation orchestrator
        self.remediation_orchestrator = RemediationOrchestrator(
            vault_consul=self.vault_consul,
            rule_engine=self.rule_engine,
            lineage_client=lineage_client,
            pipeline_client=pipeline_client,
            event_publisher=self.event_publisher
        )
        await self.remediation_orchestrator.initialize()
        
        # Initialize quality engine
        self.quality_engine = DataQualityEngine(
            profiler=self.profiler,
            remediation_orchestrator=self.remediation_orchestrator,
            vault_consul=self.vault_consul,
            event_publisher=self.event_publisher,
            metrics_collector=self.metrics
        )
        await self.quality_engine.initialize()
        
        # Initialize quality monitor
        self.quality_monitor = QualityMonitor(
            quality_engine=self.quality_engine,
            vault_consul=self.vault_consul,
            event_publisher=self.event_publisher
        )
        await self.quality_monitor.start()
        
        # Initialize event processor
        self.event_processor = DataQualityEventProcessor(
            service_name=self.metadata.name,
            quality_engine=self.quality_engine,
            remediation_orchestrator=self.remediation_orchestrator,
            event_publisher=self.event_publisher
        )
        await self.event_processor.start()
        
        # Store components in app state
        self.app.state.quality_engine = self.quality_engine
        self.app.state.profiler = self.profiler
        self.app.state.remediation_orchestrator = self.remediation_orchestrator
        self.app.state.quality_monitor = self.quality_monitor
        self.app.state.rule_engine = self.rule_engine
        
    async def cleanup_service(self):
        """Cleanup Data Quality Service components."""
        if self.event_processor:
            await self.event_processor.stop()
            
        if self.quality_monitor:
            await self.quality_monitor.stop()
            
        if self.quality_engine:
            await self.quality_engine.cleanup()
            
        if self.remediation_orchestrator:
            await self.remediation_orchestrator.cleanup()
            
        if self.profiler:
            await self.profiler.cleanup()
            
        if self.rule_engine:
            await self.rule_engine.cleanup()


class DataQualityEventProcessor(BaseEventProcessor):
    """Event processor for data quality events."""
    
    def __init__(
        self,
        service_name: str,
        quality_engine: DataQualityEngine,
        remediation_orchestrator: RemediationOrchestrator,
        event_publisher: Optional[EventPublisher] = None
    ):
        super().__init__(
            service_name=service_name,
            event_topics=[
                "data.ingested",
                "data.transformed",
                "pipeline.completed",
                "quality.check.requested",
                "remediation.requested"
            ],
            event_publisher=event_publisher
        )
        self.quality_engine = quality_engine
        self.remediation_orchestrator = remediation_orchestrator
        
    async def register_handlers(self):
        """Register event handlers."""
        # Data ingestion events
        self.event_router.register_handler(
            "data.ingested",
            self.handle_data_ingested,
            priority=10
        )
        
        # Data transformation events
        self.event_router.register_handler(
            "data.transformed",
            self.handle_data_transformed,
            priority=10
        )
        
        # Pipeline completion events
        self.event_router.register_handler(
            "pipeline.completed",
            self.handle_pipeline_completed,
            priority=5
        )
        
        # Quality check requests
        self.event_router.register_handler(
            "quality.check.requested",
            self.handle_quality_check_request,
            priority=20
        )
        
        # Remediation requests
        self.event_router.register_handler(
            "remediation.requested",
            self.handle_remediation_request,
            priority=15
        )
        
    async def handle_data_ingested(self, event_data: dict):
        """Handle data ingestion events."""
        dataset_id = event_data.get("dataset_id")
        source = event_data.get("source")
        
        # Trigger quality profiling
        await self.quality_engine.profile_dataset(
            dataset_id=dataset_id,
            source=source,
            trigger="ingestion"
        )
        
    async def handle_data_transformed(self, event_data: dict):
        """Handle data transformation events."""
        dataset_id = event_data.get("dataset_id")
        transformation = event_data.get("transformation")
        
        # Check quality after transformation
        await self.quality_engine.validate_transformation(
            dataset_id=dataset_id,
            transformation=transformation
        )
        
    async def handle_pipeline_completed(self, event_data: dict):
        """Handle pipeline completion events."""
        pipeline_id = event_data.get("pipeline_id")
        output_datasets = event_data.get("output_datasets", [])
        
        # Run quality checks on pipeline outputs
        for dataset_id in output_datasets:
            await self.quality_engine.check_quality(
                dataset_id=dataset_id,
                context={"pipeline_id": pipeline_id}
            )
            
    async def handle_quality_check_request(self, event_data: dict):
        """Handle explicit quality check requests."""
        dataset_id = event_data.get("dataset_id")
        check_type = event_data.get("check_type", "full")
        rules = event_data.get("rules", [])
        
        # Perform requested quality check
        result = await self.quality_engine.check_quality(
            dataset_id=dataset_id,
            check_type=check_type,
            rules=rules
        )
        
        # Publish result
        if self.event_publisher:
            await self.event_publisher.publish(
                "quality.check.completed",
                {
                    "dataset_id": dataset_id,
                    "result": result,
                    "timestamp": event_data.get("timestamp")
                }
            )
            
    async def handle_remediation_request(self, event_data: dict):
        """Handle remediation requests."""
        issue_id = event_data.get("issue_id")
        dataset_id = event_data.get("dataset_id")
        strategy = event_data.get("strategy")
        
        # Execute remediation
        result = await self.remediation_orchestrator.remediate(
            issue_id=issue_id,
            dataset_id=dataset_id,
            strategy=strategy
        )
        
        # Publish result
        if self.event_publisher:
            await self.event_publisher.publish(
                "remediation.completed",
                {
                    "issue_id": issue_id,
                    "dataset_id": dataset_id,
                    "result": result
                }
            )


# Create FastAPI app and service
def create_app():
    """Create the Data Quality Service application."""
    # Get environment configuration
    vault_addr = os.getenv("VAULT_ADDR", "http://vault:8200")
    vault_token = os.getenv("VAULT_TOKEN")
    consul_addr = os.getenv("CONSUL_ADDR", "http://consul:8500")
    
    # Create clients if configured
    vault_client = None
    consul_client = None
    
    if vault_token:
        vault_client = VaultClient(addr=vault_addr, token=vault_token)
        consul_client = ConsulClient(addr=consul_addr)
    
    # Create event publisher
    event_publisher = EventPublisher()
    
    # Create app with common setup
    app, service = create_data_intelligence_app(
        service_metadata=SERVICE_METADATA,
        vault_client=vault_client,
        consul_client=consul_client,
        event_publisher=event_publisher
    )
    
    # Include API routers
    app.include_router(quality_api.router, prefix="/api/v1/quality", tags=["quality"])
    app.include_router(rules_api.router, prefix="/api/v1/rules", tags=["rules"])
    app.include_router(remediation_api.router, prefix="/api/v1/remediation", tags=["remediation"])
    app.include_router(monitoring_api.router, prefix="/api/v1/monitoring", tags=["monitoring"])
    
    return app, service


# Create the app
app, service = create_app()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001) 