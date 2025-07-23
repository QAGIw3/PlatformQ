"""
Orchestration Service

Comprehensive orchestration platform for workflows, pipelines, and data movement.
"""
import logging
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from data_intelligence_common import (
    create_data_intelligence_app,
    ServiceMetadata,
    DataIntelligenceBaseService,
    VaultConsulIntegration,
    EventBus,
    StructuredLogger
)

# Import engines
from .engines.workflow import WorkflowManager, AirflowBridge, DAGGenerator
from .engines.pipeline import PipelineManager, PipelineExecutor, DependencyResolver
from .engines.optimization import MLOptimizer
from .engines.seatunnel import SeaTunnelOrchestrator
from .engines.event import EventOrchestrator

# Import API routers
from .api import workflows, pipelines, optimization, seatunnel, event_mappings

# Service metadata
SERVICE_METADATA = ServiceMetadata(
    name="orchestration-service",
    version="2.0.0",
    description="Unified orchestration platform with Airflow, SeaTunnel, and ML optimization",
    dependencies=["vault", "consul", "pulsar", "airflow", "seatunnel", "ignite"],
    health_checks=["airflow", "seatunnel", "scheduler"]
)

logger = StructuredLogger.get_logger(__name__)

# Global instances
vault_consul: Optional[VaultConsulIntegration] = None
event_bus: Optional[EventBus] = None
workflow_manager: Optional[WorkflowManager] = None
pipeline_manager: Optional[PipelineManager] = None
ml_optimizer: Optional[MLOptimizer] = None
seatunnel_orchestrator: Optional[SeaTunnelOrchestrator] = None
event_orchestrator: Optional[EventOrchestrator] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global vault_consul, event_bus, workflow_manager, pipeline_manager
    global ml_optimizer, seatunnel_orchestrator, event_orchestrator
    
    logger.info("Starting Orchestration Service...")
    
    try:
        # Initialize Vault and Consul
        vault_consul = VaultConsulIntegration()
        await vault_consul.initialize()
        
        # Initialize event bus
        event_bus = EventBus()
        await event_bus.initialize()
        
        # Initialize workflow engine
        airflow_bridge = AirflowBridge()
        dag_generator = DAGGenerator()
        workflow_manager = WorkflowManager(
            vault_consul, event_bus, airflow_bridge, dag_generator
        )
        await workflow_manager.initialize()
        
        # Initialize pipeline engine
        pipeline_executor = PipelineExecutor()
        dependency_resolver = DependencyResolver()
        pipeline_manager = PipelineManager(
            vault_consul, event_bus, pipeline_executor, dependency_resolver
        )
        await pipeline_manager.initialize()
        
        # Initialize ML optimizer
        ml_optimizer = MLOptimizer(vault_consul, event_bus)
        await ml_optimizer.initialize()
        
        # Initialize SeaTunnel orchestrator
        seatunnel_orchestrator = SeaTunnelOrchestrator(vault_consul, event_bus)
        await seatunnel_orchestrator.initialize()
        
        # Initialize event orchestrator
        event_orchestrator = EventOrchestrator(vault_consul, event_bus, workflow_manager)
        await event_orchestrator.initialize()
        
        logger.info("Orchestration Service initialized successfully")
        
        yield
        
    finally:
        # Cleanup
        logger.info("Shutting down Orchestration Service...")
        
        if workflow_manager:
            await workflow_manager.cleanup()
        if pipeline_manager:
            await pipeline_manager.cleanup()
        if ml_optimizer:
            await ml_optimizer.cleanup()
        if seatunnel_orchestrator:
            await seatunnel_orchestrator.cleanup()
        if event_orchestrator:
            await event_orchestrator.cleanup()
        if event_bus:
            await event_bus.cleanup()
        if vault_consul:
            await vault_consul.cleanup()


# Create app
app = create_data_intelligence_app(SERVICE_METADATA, lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(workflows.router, prefix="/api/v1", tags=["workflows"])
app.include_router(pipelines.router, prefix="/api/v1", tags=["pipelines"])
app.include_router(optimization.router, prefix="/api/v1", tags=["optimization"])
app.include_router(seatunnel.router, prefix="/api/v1/seatunnel", tags=["seatunnel"])
app.include_router(event_mappings.router, prefix="/api/v1", tags=["events"])

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": SERVICE_METADATA.name,
        "version": SERVICE_METADATA.version,
        "status": "operational",
        "description": SERVICE_METADATA.description,
        "capabilities": {
            "workflow_management": {
                "airflow_integration": True,
                "visual_dag_design": True,
                "advanced_scheduling": True,
                "dynamic_dag_generation": True
            },
            "pipeline_orchestration": {
                "types": ["etl", "transformation", "streaming", "ml_training", "data_quality"],
                "dependency_resolution": True,
                "resource_allocation": True,
                "quality_gates": True
            },
            "ml_optimization": {
                "predictive_optimization": True,
                "resource_prediction": True,
                "anomaly_detection": True,
                "auto_scaling": True,
                "performance_tuning": True
            },
            "seatunnel_integration": {
                "data_movement": True,
                "etl_pipelines": True,
                "stream_processing": True,
                "cross_system_sync": True,
                "connectors": ["jdbc", "kafka", "pulsar", "elasticsearch", "s3", "clickhouse"]
            },
            "event_driven": {
                "event_mappings": True,
                "reactive_workflows": True,
                "event_correlation": True,
                "complex_patterns": True
            }
        },
        "endpoints": {
            "workflows": "/api/v1/workflows",
            "pipelines": "/api/v1/pipelines",
            "optimization": "/api/v1/optimize",
            "seatunnel": "/api/v1/seatunnel",
            "events": "/api/v1/event-mappings",
            "health": "/health",
            "metrics": "/metrics",
            "docs": "/docs"
        }
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 