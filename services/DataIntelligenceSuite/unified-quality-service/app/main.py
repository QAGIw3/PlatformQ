"""
Unified Quality Service

Combines data quality validation, profiling, anomaly detection, ML-powered remediation,
and SeaTunnel integration into a single comprehensive service.
"""

import os
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks, Request
from fastapi.responses import JSONResponse
import uvicorn

from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    DataIntelligenceBaseService,
    BaseEventProcessor,
    VaultConsulIntegration,
    MetricsCollector,
    StructuredLogger
)
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.event_subscriber import EventSubscriber

from .core import (
    QualityEngine,
    QualityProfiler,
    RemediationOrchestrator,
    AnomalyDetector,
    MLQualityOptimizer
)
from .seatunnel import SeaTunnelQualityPipelines
from .api import quality_router, profile_router, remediation_router, seatunnel_router
from .events import QualityEventProcessor

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="unified-quality-service",
    version="1.0.0",
    description="Comprehensive ML-powered data quality management platform",
    dependencies=["vault", "consul", "pulsar", "ignite", "seatunnel"],
    health_checks=["quality_engine", "ml_optimizer", "seatunnel"]
)

logger = StructuredLogger.get_logger(__name__)


class UnifiedQualityService(DataIntelligenceBaseService):
    """Unified Quality Service implementation"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(SERVICE_METADATA, *args, **kwargs)
        
        # Core components
        self.quality_engine: Optional[QualityEngine] = None
        self.quality_profiler: Optional[QualityProfiler] = None
        self.remediation_orchestrator: Optional[RemediationOrchestrator] = None
        self.anomaly_detector: Optional[AnomalyDetector] = None
        self.ml_optimizer: Optional[MLQualityOptimizer] = None
        self.seatunnel_pipelines: Optional[SeaTunnelQualityPipelines] = None
        self.event_processor: Optional[QualityEventProcessor] = None
    
    async def initialize_service(self):
        """Initialize service-specific components"""
        logger.info("initializing_unified_quality_service")
        
        # Initialize quality engine
        self.quality_engine = QualityEngine(
            vault_consul=self.vault_consul,
            metrics_collector=self.metrics_collector
        )
        await self.quality_engine.initialize()
        
        # Initialize quality profiler
        self.quality_profiler = QualityProfiler(
            vault_consul=self.vault_consul,
            quality_engine=self.quality_engine
        )
        await self.quality_profiler.initialize()
        
        # Initialize ML optimizer
        self.ml_optimizer = MLQualityOptimizer(
            quality_engine=self.quality_engine,
            metrics_collector=self.metrics_collector
        )
        await self.ml_optimizer.initialize()
        
        # Initialize anomaly detector
        self.anomaly_detector = AnomalyDetector(
            ml_optimizer=self.ml_optimizer,
            vault_consul=self.vault_consul
        )
        await self.anomaly_detector.initialize()
        
        # Initialize remediation orchestrator
        self.remediation_orchestrator = RemediationOrchestrator(
            quality_engine=self.quality_engine,
            ml_optimizer=self.ml_optimizer,
            vault_consul=self.vault_consul,
            event_publisher=self.event_publisher
        )
        await self.remediation_orchestrator.initialize()
        
        # Initialize SeaTunnel integration
        self.seatunnel_pipelines = SeaTunnelQualityPipelines(
            quality_engine=self.quality_engine,
            vault_consul=self.vault_consul
        )
        await self.seatunnel_pipelines.initialize()
        
        # Initialize event processor
        if self.event_subscriber:
            self.event_processor = QualityEventProcessor(
                event_subscriber=self.event_subscriber,
                quality_engine=self.quality_engine,
                remediation_orchestrator=self.remediation_orchestrator
            )
            await self.event_processor.start()
        
        # Register health checks
        self.health_manager.register_check(
            "quality_engine",
            self._check_quality_engine_health,
            critical=True
        )
        self.health_manager.register_check(
            "ml_optimizer",
            self._check_ml_optimizer_health,
            critical=False
        )
        self.health_manager.register_check(
            "seatunnel",
            self._check_seatunnel_health,
            critical=False
        )
        
        # Store components in app state for API access
        self.app.state.quality_engine = self.quality_engine
        self.app.state.quality_profiler = self.quality_profiler
        self.app.state.remediation_orchestrator = self.remediation_orchestrator
        self.app.state.anomaly_detector = self.anomaly_detector
        self.app.state.ml_optimizer = self.ml_optimizer
        self.app.state.seatunnel_pipelines = self.seatunnel_pipelines
        
        logger.info("unified_quality_service_initialized")
    
    async def cleanup_service(self):
        """Cleanup service-specific components"""
        logger.info("cleaning_up_unified_quality_service")
        
        # Stop event processor
        if self.event_processor:
            await self.event_processor.stop()
        
        # Cleanup components
        if self.remediation_orchestrator:
            await self.remediation_orchestrator.cleanup()
        if self.ml_optimizer:
            await self.ml_optimizer.cleanup()
        if self.quality_engine:
            await self.quality_engine.cleanup()
        
        logger.info("unified_quality_service_cleaned_up")
    
    async def _check_quality_engine_health(self) -> bool:
        """Check quality engine health"""
        return self.quality_engine is not None and await self.quality_engine.is_healthy()
    
    async def _check_ml_optimizer_health(self) -> bool:
        """Check ML optimizer health"""
        return self.ml_optimizer is not None and await self.ml_optimizer.is_healthy()
    
    async def _check_seatunnel_health(self) -> bool:
        """Check SeaTunnel integration health"""
        return self.seatunnel_pipelines is not None and await self.seatunnel_pipelines.is_healthy()


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    # Create app and service
    app, service = create_data_intelligence_app(
        service_class=UnifiedQualityService,
        service_metadata=SERVICE_METADATA,
        cors_origins=["*"],  # Configure appropriately for production
        include_health_endpoint=True,
        include_metrics_endpoint=True,
        include_ready_endpoint=True
    )
    
    # Include routers
    app.include_router(quality_router, prefix="/api/v1/quality", tags=["quality"])
    app.include_router(profile_router, prefix="/api/v1/profile", tags=["profile"])
    app.include_router(remediation_router, prefix="/api/v1/remediation", tags=["remediation"])
    app.include_router(seatunnel_router, prefix="/api/v1/seatunnel", tags=["seatunnel"])
    
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
        "features": [
            "ml-powered-quality",
            "self-healing",
            "anomaly-detection",
            "quality-profiling",
            "seatunnel-integration",
            "automated-remediation"
        ]
    }


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("SERVICE_PORT", "8003")),
        reload=os.getenv("ENVIRONMENT", "development") == "development"
    ) 