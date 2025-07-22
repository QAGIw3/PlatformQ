"""
Cognitive Orchestration Service

Self-learning orchestrator that optimizes workflows based on:
- Historical performance patterns
- Resource utilization
- Business objectives
- Real-time system state
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
import structlog

from app.core.config import get_settings
from app.core.cognitive_orchestrator import CognitiveOrchestrator
from app.core.ml_optimizer import MLOptimizer
from app.core.system_monitor import SystemMonitor
from app.api import orchestration, optimization, monitoring
from app.integrations.data_platform import DataPlatformClient
from app.integrations.ml_platform import MLPlatformClient

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting Cognitive Orchestration Service")
    
    # Initialize components
    system_monitor = SystemMonitor(settings)
    ml_optimizer = MLOptimizer(settings)
    
    # Initialize integrations
    data_platform = DataPlatformClient(settings.data_platform_url)
    ml_platform = MLPlatformClient(settings.ml_platform_url)
    
    # Initialize orchestrator
    orchestrator = CognitiveOrchestrator(
        ml_optimizer=ml_optimizer,
        system_monitor=system_monitor,
        data_platform=data_platform,
        ml_platform=ml_platform,
        settings=settings
    )
    
    # Store in app state
    app.state.orchestrator = orchestrator
    app.state.system_monitor = system_monitor
    app.state.ml_optimizer = ml_optimizer
    
    # Start background tasks
    await orchestrator.start()
    await system_monitor.start()
    
    logger.info("Cognitive Orchestration Service started successfully")
    
    yield
    
    # Cleanup
    logger.info("Shutting down Cognitive Orchestration Service")
    await orchestrator.stop()
    await system_monitor.stop()
    await data_platform.close()
    await ml_platform.close()


app = FastAPI(
    title="Cognitive Orchestration Service",
    description="AI-driven orchestration for autonomous optimization",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(orchestration.router, prefix="/api/v1/orchestration", tags=["orchestration"])
app.include_router(optimization.router, prefix="/api/v1/optimization", tags=["optimization"])
app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["monitoring"])

# Mount Prometheus metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "cognitive-orchestration-service",
        "version": "1.0.0"
    }


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Cognitive Orchestration Service",
        "description": "AI-driven orchestration for autonomous optimization",
        "endpoints": [
            "/api/v1/orchestration",
            "/api/v1/optimization",
            "/api/v1/monitoring",
            "/metrics",
            "/health"
        ]
    } 