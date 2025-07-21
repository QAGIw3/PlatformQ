"""
Network Bandwidth Market Service
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
import consul

from .config import settings
from .api import (
    paths_router,
    bandwidth_router,
    circuits_router,
    pricing_router,
    latency_router
)
from .services import (
    PathRegistryService,
    BandwidthManagerService,
    CircuitManagerService,
    PricingEngineService
)
from .utils import BackgroundTaskManager
from . import core


# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Service instances
services = {}
background_task_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    logger.info("Starting Network Bandwidth Market Service")
    
    try:
        # Initialize services
        services["path_registry"] = PathRegistryService()
        await services["path_registry"].initialize()
        
        services["pricing_engine"] = PricingEngineService()
        await services["pricing_engine"].initialize()
        
        services["bandwidth_manager"] = BandwidthManagerService(
            services["path_registry"]
        )
        await services["bandwidth_manager"].initialize()
        
        services["circuit_manager"] = CircuitManagerService(
            services["path_registry"]
        )
        await services["circuit_manager"].initialize()
        
        # Set service instances for dependency injection
        core.dependencies.path_registry_service = services["path_registry"]
        core.dependencies.bandwidth_manager_service = services["bandwidth_manager"]
        core.dependencies.circuit_manager_service = services["circuit_manager"]
        core.dependencies.pricing_engine_service = services["pricing_engine"]
        
        # Initialize background tasks
        global background_task_manager
        background_task_manager = BackgroundTaskManager(
            services["path_registry"],
            services["bandwidth_manager"],
            services["circuit_manager"],
            services["pricing_engine"]
        )
        await background_task_manager.start()
        
        # Register with Consul
        await register_with_consul()
        
        logger.info("Network Bandwidth Market Service started successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        raise
    
    finally:
        # Cleanup
        logger.info("Shutting down Network Bandwidth Market Service")
        
        # Stop background tasks
        if background_task_manager:
            await background_task_manager.stop()
        
        # Cleanup services
        for service in services.values():
            if hasattr(service, 'cleanup'):
                await service.cleanup()
        
        # Deregister from Consul
        await deregister_from_consul()
        
        logger.info("Network Bandwidth Market Service stopped")


# Create FastAPI app
app = FastAPI(
    title=settings.SERVICE_NAME,
    version=settings.VERSION,
    description="Real-time marketplace for network bandwidth, circuits, and latency guarantees",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Setup Prometheus metrics
if settings.PROMETHEUS_ENABLED:
    instrumentator = Instrumentator()
    instrumentator.instrument(app).expose(app, endpoint="/metrics")

# Include routers
app.include_router(paths_router, prefix=settings.API_PREFIX)
app.include_router(bandwidth_router, prefix=settings.API_PREFIX)
app.include_router(circuits_router, prefix=settings.API_PREFIX)
app.include_router(pricing_router, prefix=settings.API_PREFIX)
app.include_router(latency_router, prefix=settings.API_PREFIX)


# Root endpoint
@app.get("/")
async def root():
    """Service information"""
    return {
        "service": settings.SERVICE_NAME,
        "version": settings.VERSION,
        "status": "healthy",
        "api_docs": "/docs"
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    health_status = {
        "status": "healthy",
        "service": settings.SERVICE_NAME,
        "version": settings.VERSION,
        "checks": {}
    }
    
    # Check service health
    try:
        # Check Ignite connection
        if services.get("path_registry") and services["path_registry"].ignite_client:
            health_status["checks"]["ignite"] = "healthy"
        else:
            health_status["checks"]["ignite"] = "unhealthy"
            health_status["status"] = "degraded"
        
        # Check Elasticsearch
        if services.get("path_registry") and services["path_registry"].es_client:
            if services["path_registry"].es_client.ping():
                health_status["checks"]["elasticsearch"] = "healthy"
            else:
                health_status["checks"]["elasticsearch"] = "unhealthy"
                health_status["status"] = "degraded"
        
        # Check background tasks
        if background_task_manager:
            active_tasks = sum(1 for task in background_task_manager.tasks if not task.done())
            health_status["checks"]["background_tasks"] = {
                "status": "healthy" if active_tasks > 0 else "unhealthy",
                "active_count": active_tasks
            }
        
    except Exception as e:
        logger.error(f"Health check error: {e}")
        health_status["status"] = "unhealthy"
        health_status["error"] = str(e)
    
    status_code = 200 if health_status["status"] == "healthy" else 503
    return health_status


# Ready check endpoint
@app.get("/ready")
async def ready_check():
    """Readiness check endpoint"""
    if all(service is not None for service in services.values()):
        return {"status": "ready"}
    else:
        raise HTTPException(status_code=503, detail="Service not ready")


async def register_with_consul():
    """Register service with Consul"""
    try:
        c = consul.Consul(
            host=settings.CONSUL_HOST,
            port=settings.CONSUL_PORT
        )
        
        # Register service
        c.agent.service.register(
            name=settings.CONSUL_SERVICE_NAME,
            service_id=f"{settings.CONSUL_SERVICE_NAME}-{settings.PORT}",
            address=settings.HOST,
            port=settings.PORT,
            tags=[
                "network-bandwidth",
                "marketplace",
                "api",
                f"version:{settings.VERSION}"
            ],
            check=consul.Check.http(
                f"http://{settings.HOST}:{settings.PORT}/health",
                interval=settings.CONSUL_HEALTH_CHECK_INTERVAL,
                timeout="5s"
            )
        )
        
        logger.info(f"Registered with Consul as {settings.CONSUL_SERVICE_NAME}")
        
    except Exception as e:
        logger.error(f"Failed to register with Consul: {e}")


async def deregister_from_consul():
    """Deregister service from Consul"""
    try:
        c = consul.Consul(
            host=settings.CONSUL_HOST,
            port=settings.CONSUL_PORT
        )
        
        c.agent.service.deregister(
            service_id=f"{settings.CONSUL_SERVICE_NAME}-{settings.PORT}"
        )
        
        logger.info("Deregistered from Consul")
        
    except Exception as e:
        logger.error(f"Failed to deregister from Consul: {e}")


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host=settings.HOST,
        port=settings.PORT,
        workers=settings.WORKERS,
        reload=True
    ) 