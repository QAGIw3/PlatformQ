"""
Unified Orchestration Service

Combines workflow management, pipeline orchestration, and ML-driven optimization 
with Apache Airflow and SeaTunnel integration.
"""

import os
import asyncio
from typing import Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Request
import uvicorn

from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    DataIntelligenceBaseService,
    VaultConsulIntegration,
    MetricsCollector,
    StructuredLogger
)

# Import core components
from app.core import (
    settings, AirflowBridge, PipelineManager, MLPipelineOptimizer,
    SeaTunnelOrchestrator, EventOrchestrator, CredentialAttestor, K8sManager
)

# Import API routers
from app.api import (
    workflows_router, pipelines_router, optimization_router,
    seatunnel_router, event_mappings_router, attestations_router,
    k8s_router, monitoring_router, health_router,
    set_workflows_deps, set_pipelines_deps, set_optimization_deps,
    set_seatunnel_deps, set_event_deps, set_attestations_deps,
    set_k8s_deps, set_monitoring_deps, set_health_deps
)

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="unified-orchestration-service",
    version="1.0.0",
    description="Unified orchestration platform with Airflow and SeaTunnel",
    dependencies=["vault", "consul", "pulsar", "airflow", "seatunnel"],
    health_checks=["airflow", "seatunnel", "scheduler"]
)

logger = StructuredLogger.get_logger(__name__)

# Global components
airflow_bridge: Optional[AirflowBridge] = None
pipeline_manager: Optional[PipelineManager] = None
ml_optimizer: Optional[MLPipelineOptimizer] = None
seatunnel_orchestrator: Optional[SeaTunnelOrchestrator] = None
event_orchestrator: Optional[EventOrchestrator] = None
credential_attestor: Optional[CredentialAttestor] = None
k8s_manager: Optional[K8sManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    global airflow_bridge, pipeline_manager, ml_optimizer
    global seatunnel_orchestrator, event_orchestrator, credential_attestor, k8s_manager
    
    # Startup
    logger.info("initializing_unified_orchestration_service")
    
    try:
        # Initialize Airflow Bridge
        airflow_bridge = AirflowBridge()
        await airflow_bridge.initialize()
        
        # Initialize Pipeline Manager
        pipeline_manager = PipelineManager()
        await pipeline_manager.initialize()
        
        # Initialize ML Optimizer
        ml_optimizer = MLPipelineOptimizer()
        await ml_optimizer.initialize()
        
        # Initialize SeaTunnel Orchestrator
        seatunnel_orchestrator = SeaTunnelOrchestrator()
        await seatunnel_orchestrator.initialize()
        
        # Initialize Event Orchestrator
        event_orchestrator = EventOrchestrator()
        await event_orchestrator.initialize()
        
        # Initialize Credential Attestor
        credential_attestor = CredentialAttestor()
        await credential_attestor.initialize()
        
        # Initialize K8s Manager
        k8s_namespace = settings.k8s_namespace if hasattr(settings, 'k8s_namespace') else 'default'
        k8s_in_cluster = settings.k8s_in_cluster if hasattr(settings, 'k8s_in_cluster') else True
        k8s_manager = K8sManager(in_cluster=k8s_in_cluster, namespace=k8s_namespace)
        
        # Set dependencies for API routers
        set_workflows_deps(airflow_bridge)
        set_pipelines_deps(pipeline_manager)
        set_optimization_deps(ml_optimizer)
        set_seatunnel_deps(seatunnel_orchestrator)
        set_event_deps(event_orchestrator)
        set_attestations_deps(credential_attestor)
        set_k8s_deps(k8s_manager)
        set_monitoring_deps(
            airflow_bridge, pipeline_manager, ml_optimizer,
            seatunnel_orchestrator, event_orchestrator, credential_attestor
        )
        set_health_deps({
            'airflow': airflow_bridge,
            'pipeline': pipeline_manager,
            'ml_optimizer': ml_optimizer,
            'seatunnel': seatunnel_orchestrator,
            'events': event_orchestrator,
            'credentials': credential_attestor,
            'k8s': k8s_manager
        })
        
        logger.info("unified_orchestration_service_initialized")
        
    except Exception as e:
        logger.error(f"Failed to initialize service: {e}")
        raise
        
    yield
    
    # Shutdown
    logger.info("cleaning_up_unified_orchestration_service")
    
    try:
        if event_orchestrator:
            await event_orchestrator.cleanup()
        if credential_attestor:
            await credential_attestor.cleanup()
        if seatunnel_orchestrator:
            await seatunnel_orchestrator.cleanup()
        if ml_optimizer:
            await ml_optimizer.cleanup()
        if pipeline_manager:
            await pipeline_manager.cleanup()
        if airflow_bridge:
            await airflow_bridge.cleanup()
            
    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        
    logger.info("unified_orchestration_service_cleaned_up")


# Create FastAPI app
def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
    
    app = FastAPI(
        title=SERVICE_METADATA.name,
        version=SERVICE_METADATA.version,
        description=SERVICE_METADATA.description,
        lifespan=lifespan
    )
    
    # Include routers
    app.include_router(workflows_router)
    app.include_router(pipelines_router)
    app.include_router(optimization_router)
    app.include_router(seatunnel_router)
    app.include_router(event_mappings_router)
    app.include_router(attestations_router)
    app.include_router(k8s_router)
    app.include_router(monitoring_router)
    app.include_router(health_router)
    
    @app.get("/")
    async def root():
        """Root endpoint"""
        return {
            "service": SERVICE_METADATA.name,
            "version": SERVICE_METADATA.version,
            "description": SERVICE_METADATA.description,
            "features": [
                "airflow-integration",
                "seatunnel-orchestration",
                "ml-optimization",
                "event-driven-workflows",
                "pipeline-management",
                "verifiable-credentials",
                "kubernetes-orchestration"
            ],
            "endpoints": {
                "workflows": "/api/v1/workflows",
                "pipelines": "/api/v1/pipelines",
                "optimization": "/api/v1/optimize",
                "seatunnel": "/api/v1/seatunnel",
                "events": "/api/v1/event-mappings",
                "attestations": "/api/v1/attestations",
                "kubernetes": "/k8s",
                "monitoring": "/api/v1/monitoring",
                "health": "/health",
                "docs": "/docs"
            }
        }
    
    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=int(os.getenv("SERVICE_PORT", "8019")),
        reload=os.getenv("ENVIRONMENT", "development") == "development"
    ) 