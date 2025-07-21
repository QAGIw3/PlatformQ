"""
Market Aggregator Service Main Application
"""
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
import asyncio
import consul

from .config import settings
from .api import bundles, arbitrage, market_comparison
from .aggregators.bundle_optimizer import BundleOptimizer
from .aggregators.arbitrage_detector import ArbitrageDetector
from .core.market_client import MarketClient
from . import core


# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Background tasks
arbitrage_monitor_task = None
resource_sync_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    logger.info("Starting Market Aggregator Service")
    
    try:
        # Initialize market client
        market_client = MarketClient()
        core.dependencies.market_client_instance = market_client
        
        # Initialize bundle optimizer
        bundle_optimizer = BundleOptimizer(market_client)
        core.dependencies.bundle_optimizer_instance = bundle_optimizer
        
        # Initialize arbitrage detector
        arbitrage_detector = ArbitrageDetector(market_client)
        await arbitrage_detector.initialize()
        core.dependencies.arbitrage_detector_instance = arbitrage_detector
        
        # Start background tasks
        global arbitrage_monitor_task, resource_sync_task
        
        if settings.ARBITRAGE_DETECTION_ENABLED:
            arbitrage_monitor_task = asyncio.create_task(
                arbitrage_detector.monitor_arbitrage_opportunities()
            )
            logger.info("Started arbitrage monitoring")
        
        resource_sync_task = asyncio.create_task(periodic_resource_sync())
        logger.info("Started resource synchronization")
        
        # Register with Consul
        await register_with_consul()
        
        logger.info("Market Aggregator Service started successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start service: {e}")
        raise
    
    finally:
        # Cleanup
        logger.info("Shutting down Market Aggregator Service")
        
        # Cancel background tasks
        if arbitrage_monitor_task:
            arbitrage_monitor_task.cancel()
            try:
                await arbitrage_monitor_task
            except asyncio.CancelledError:
                pass
        
        if resource_sync_task:
            resource_sync_task.cancel()
            try:
                await resource_sync_task
            except asyncio.CancelledError:
                pass
        
        # Cleanup services
        if core.dependencies.market_client_instance:
            await core.dependencies.market_client_instance.cleanup()
            
        if core.dependencies.arbitrage_detector_instance:
            await core.dependencies.arbitrage_detector_instance.cleanup()
        
        # Deregister from Consul
        await deregister_from_consul()
        
        logger.info("Market Aggregator Service stopped")


# Create FastAPI app
app = FastAPI(
    title=settings.SERVICE_NAME,
    version=settings.VERSION,
    description="Unified market aggregator for quantum, AI, and network compute resources",
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
app.include_router(bundles.router, prefix=settings.API_PREFIX)
app.include_router(arbitrage.router, prefix=settings.API_PREFIX)
app.include_router(market_comparison.router, prefix=settings.API_PREFIX)


# Root endpoint
@app.get("/")
async def root():
    """Service information"""
    return {
        "service": settings.SERVICE_NAME,
        "version": settings.VERSION,
        "status": "healthy",
        "api_docs": "/docs",
        "features": [
            "resource_bundling",
            "arbitrage_detection",
            "market_comparison",
            "workload_optimization"
        ]
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check service dependencies
        checks = {
            "market_client": core.dependencies.market_client_instance is not None,
            "bundle_optimizer": core.dependencies.bundle_optimizer_instance is not None,
            "arbitrage_detector": core.dependencies.arbitrage_detector_instance is not None
        }
        
        # Check external service connectivity
        if core.dependencies.market_client_instance:
            try:
                # Quick connectivity check
                await asyncio.wait_for(
                    core.dependencies.market_client_instance.client.get(
                        f"{settings.QUANTUM_MARKET_URL}/health"
                    ),
                    timeout=5.0
                )
                checks["quantum_market"] = True
            except:
                checks["quantum_market"] = False
        
        all_healthy = all(checks.values())
        
        return {
            "status": "healthy" if all_healthy else "degraded",
            "checks": checks,
            "features": {
                "bundle_optimization": settings.BUNDLE_OPTIMIZATION_ENABLED,
                "arbitrage_detection": settings.ARBITRAGE_DETECTION_ENABLED
            }
        }
        
    except Exception as e:
        logger.error(f"Health check error: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy")


# Ready check endpoint
@app.get("/ready")
async def ready_check():
    """Readiness check endpoint"""
    if all([
        core.dependencies.market_client_instance,
        core.dependencies.bundle_optimizer_instance,
        core.dependencies.arbitrage_detector_instance
    ]):
        return {"status": "ready"}
    else:
        raise HTTPException(status_code=503, detail="Service not ready")


# Service statistics endpoint
@app.get("/stats")
async def service_stats():
    """Get service statistics"""
    stats = {
        "active_bundles": 0,  # Would query from cache
        "active_arbitrage_opportunities": 0,
        "total_allocations_24h": 0,
        "total_arbitrage_profit_24h": 0
    }
    
    if core.dependencies.arbitrage_detector_instance:
        stats["active_arbitrage_opportunities"] = len(
            core.dependencies.arbitrage_detector_instance.active_opportunities
        )
    
    return stats


async def periodic_resource_sync():
    """Periodically sync resource availability and pricing"""
    while True:
        try:
            await asyncio.sleep(settings.RESOURCE_SYNC_INTERVAL)
            
            if core.dependencies.market_client_instance:
                # Sync quantum resources
                quantum_resources = await core.dependencies.market_client_instance.search_quantum_resources()
                logger.info(f"Synced {len(quantum_resources)} quantum resources")
                
                # Sync AI accelerators
                ai_resources = await core.dependencies.market_client_instance.search_ai_accelerators()
                logger.info(f"Synced {len(ai_resources)} AI accelerators")
                
                # Sync network paths
                network_paths = await core.dependencies.market_client_instance.get_network_paths()
                logger.info(f"Synced {len(network_paths)} network paths")
                
        except asyncio.CancelledError:
            logger.info("Resource sync task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in resource sync: {e}")
            await asyncio.sleep(60)  # Wait before retry


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
                "aggregator",
                "bundles",
                "arbitrage",
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