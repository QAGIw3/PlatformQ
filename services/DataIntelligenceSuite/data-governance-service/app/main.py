"""
Data Governance Service

Provides comprehensive data quality validation, profiling, anomaly detection,
and automated remediation capabilities.
"""

import os
import asyncio
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn

from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    DataIntelligenceBaseService
)

from app.engines.quality import (
    QualityValidator,
    QualityProfiler,
    AnomalyDetector,
    RemediationEngine
)

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="data-governance-service",
    version="2.0.0",
    description="Comprehensive data quality and governance platform",
    dependencies=["vault", "consul", "pulsar", "ignite", "cassandra"],
    health_checks=["quality_validator", "quality_profiler", "anomaly_detector", "remediation_engine"]
)


class DataGovernanceService(DataIntelligenceBaseService):
    """Data Governance Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        
        # Quality engines
        self.quality_validator: Optional[QualityValidator] = None
        self.quality_profiler: Optional[QualityProfiler] = None
        self.anomaly_detector: Optional[AnomalyDetector] = None
        self.remediation_engine: Optional[RemediationEngine] = None
        
        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None
        self._auto_remediation_task: Optional[asyncio.Task] = None
    
    async def initialize_service(self):
        """Initialize service-specific components"""
        logger.info("Initializing Data Governance Service")
        
        # Initialize quality validator
        self.quality_validator = QualityValidator(
            event_bus=self.event_bus,
            cache_manager=self.cache_manager,
            ignite_client=self.ignite_client
        )
        await self.quality_validator.initialize()
        
        # Initialize quality profiler
        self.quality_profiler = QualityProfiler(
            event_bus=self.event_bus,
            cache_manager=self.cache_manager
        )
        await self.quality_profiler.initialize()
        
        # Initialize anomaly detector
        self.anomaly_detector = AnomalyDetector(
            event_bus=self.event_bus,
            cache_manager=self.cache_manager
        )
        await self.anomaly_detector.initialize()
        
        # Initialize remediation engine
        self.remediation_engine = RemediationEngine(
            event_bus=self.event_bus,
            cache_manager=self.cache_manager
        )
        await self.remediation_engine.initialize()
        
        # Set up API dependencies
        from app.api.v1.endpoints.data_quality import set_engines
        set_engines(
            self.quality_validator,
            self.quality_profiler,
            self.anomaly_detector,
            self.remediation_engine
        )
        
        # Register health checks
        self.health_manager.register_check(
            "quality_validator",
            self._check_quality_validator_health,
            critical=True
        )
        self.health_manager.register_check(
            "quality_profiler",
            self._check_quality_profiler_health,
            critical=False
        )
        self.health_manager.register_check(
            "anomaly_detector",
            self._check_anomaly_detector_health,
            critical=False
        )
        self.health_manager.register_check(
            "remediation_engine",
            self._check_remediation_engine_health,
            critical=False
        )
        
        # Start background tasks
        self._monitoring_task = asyncio.create_task(self._monitor_quality_metrics())
        self._auto_remediation_task = asyncio.create_task(self._auto_remediation_monitor())
        
        # Subscribe to events
        await self._setup_event_subscriptions()
        
        logger.info("Data Governance Service initialized successfully")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("Cleaning up Data Governance Service")
        
        # Cancel background tasks
        if self._monitoring_task:
            self._monitoring_task.cancel()
        if self._auto_remediation_task:
            self._auto_remediation_task.cancel()
        
        # No specific cleanup needed for engines
        # They use shared resources that are cleaned up by base class
        
        logger.info("Data Governance Service cleaned up")
    
    async def _check_quality_validator_health(self) -> bool:
        """Check quality validator health"""
        return self.quality_validator is not None
    
    async def _check_quality_profiler_health(self) -> bool:
        """Check quality profiler health"""
        return self.quality_profiler is not None
    
    async def _check_anomaly_detector_health(self) -> bool:
        """Check anomaly detector health"""
        return self.anomaly_detector is not None
    
    async def _check_remediation_engine_health(self) -> bool:
        """Check remediation engine health"""
        return self.remediation_engine is not None
    
    async def _monitor_quality_metrics(self):
        """Monitor quality metrics in background"""
        while True:
            try:
                # Collect metrics from all engines
                validator_stats = self.quality_validator.get_statistics()
                
                # Report metrics
                await self.metrics_collector.record_gauge(
                    "quality_rules_total",
                    validator_stats["total_rules"],
                    {"service": "data-governance"}
                )
                
                await self.metrics_collector.record_gauge(
                    "quality_validations_total",
                    validator_stats["total_validations"],
                    {"service": "data-governance"}
                )
                
                # Sleep for 60 seconds
                await asyncio.sleep(60)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring quality metrics: {e}")
                await asyncio.sleep(60)
    
    async def _auto_remediation_monitor(self):
        """Monitor for auto-remediation opportunities"""
        while True:
            try:
                # Check if auto-remediation is enabled
                if self.remediation_engine.auto_remediate:
                    # This would check for pending quality issues
                    # and trigger remediation as needed
                    pass
                
                # Sleep for 5 minutes
                await asyncio.sleep(300)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in auto-remediation monitor: {e}")
                await asyncio.sleep(300)
    
    async def _setup_event_subscriptions(self):
        """Set up event subscriptions for cross-service communication"""
        # Subscribe to data platform events
        await self.event_bus.subscribe(
            "data.dataset.created",
            self._handle_dataset_created
        )
        await self.event_bus.subscribe(
            "data.dataset.updated",
            self._handle_dataset_updated
        )
        
        # Subscribe to ML platform events for model quality
        await self.event_bus.subscribe(
            "ml.model.trained",
            self._handle_model_trained
        )
    
    async def _handle_dataset_created(self, event_data: dict):
        """Handle new dataset creation"""
        try:
            dataset_id = event_data.get("dataset_id")
            if dataset_id:
                # Trigger automatic profiling
                await self.event_bus.publish("quality.profile.request", {
                    "dataset_id": dataset_id,
                    "profile_type": "basic",
                    "auto_triggered": True
                })
        except Exception as e:
            logger.error(f"Error handling dataset created event: {e}")
    
    async def _handle_dataset_updated(self, event_data: dict):
        """Handle dataset update"""
        try:
            dataset_id = event_data.get("dataset_id")
            if dataset_id:
                # Trigger quality validation
                await self.event_bus.publish("quality.validate", {
                    "dataset_id": dataset_id,
                    "auto_triggered": True
                })
        except Exception as e:
            logger.error(f"Error handling dataset updated event: {e}")
    
    async def _handle_model_trained(self, event_data: dict):
        """Handle model training completion"""
        try:
            model_id = event_data.get("model_id")
            training_data_id = event_data.get("training_data_id")
            
            if training_data_id:
                # Validate training data quality
                await self.event_bus.publish("quality.validate", {
                    "dataset_id": training_data_id,
                    "context": "model_training",
                    "model_id": model_id
                })
        except Exception as e:
            logger.error(f"Error handling model trained event: {e}")


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    # Create app with lifespan
    app, service = create_data_intelligence_app(
        service_class=DataGovernanceService,
        service_metadata=SERVICE_METADATA,
        cors_origins=["*"],  # Configure appropriately for production
        include_health_endpoint=True,
        include_metrics_endpoint=True,
        include_ready_endpoint=True
    )
    
    # Include routers
    from app.api.v1.api import api_router
    app.include_router(api_router, prefix="/api/v1")
    
    return app


# Create app instance
app = create_app()


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": SERVICE_METADATA.name,
        "version": SERVICE_METADATA.version,
        "description": SERVICE_METADATA.description,
        "endpoints": {
            "quality": "/api/v1/quality",
            "health": "/health",
            "metrics": "/metrics",
            "ready": "/ready",
            "docs": "/docs"
        }
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("SERVICE_PORT", "8020")),
        reload=os.getenv("ENVIRONMENT", "development") == "development"
    ) 