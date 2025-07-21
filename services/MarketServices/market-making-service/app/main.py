"""Market Making Service - Main Application"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from app.config import settings
from app.api import amm, pools, strategies, liquidity, mining, analytics, monitoring, compliant_pools, market_makers, options_amm
from app.core.dependencies import (
    get_ignite_client,
    get_pulsar_client,
    get_redis_client,
    init_services,
    cleanup_services
)
from app.core.events import EventPublisher
from app.monitoring import setup_monitoring, metrics_registry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle"""
    logger.info(f"Starting {settings.SERVICE_NAME}...")
    
    # Initialize services
    await init_services()
    
    # Setup monitoring
    setup_monitoring()
    
    logger.info(f"{settings.SERVICE_NAME} started successfully")
    
    yield
    
    # Cleanup
    logger.info(f"Shutting down {settings.SERVICE_NAME}...")
    await cleanup_services()
    logger.info(f"{settings.SERVICE_NAME} stopped")


# Create FastAPI app
app = FastAPI(
    title=settings.SERVICE_NAME,
    description="High-performance automated market making and liquidity provision service",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(amm.router, prefix="/api/v1/amm", tags=["AMM"])
app.include_router(pools.router, prefix="/api/v1/pools", tags=["Pools"])
app.include_router(strategies.router, prefix="/api/v1/strategies", tags=["Strategies"])
app.include_router(liquidity.router, prefix="/api/v1/liquidity", tags=["Liquidity"])
app.include_router(mining.router, prefix="/api/v1/mining", tags=["Mining"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["Analytics"])
app.include_router(monitoring.router, prefix="/api/v1/monitoring", tags=["Monitoring"])
app.include_router(compliant_pools.router, prefix="/api/v1/compliant-pools", tags=["Compliant Pools"])
app.include_router(market_makers.router, prefix="/api/v1/market-makers", tags=["Market Makers"])
app.include_router(options_amm.router, prefix="/api/v1/options-amm", tags=["Options AMM"])

# Mount metrics endpoint
metrics_app = make_asgi_app(registry=metrics_registry)
app.mount("/metrics", metrics_app)


@app.get("/")
async def root() -> Dict[str, str]:
    """Root endpoint"""
    return {
        "service": settings.SERVICE_NAME,
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
async def health() -> Dict[str, Any]:
    """Health check endpoint"""
    try:
        # Check dependencies
        ignite_status = await check_ignite_health()
        pulsar_status = await check_pulsar_health()
        redis_status = await check_redis_health()
        
        all_healthy = all([ignite_status, pulsar_status, redis_status])
        
        return {
            "status": "healthy" if all_healthy else "degraded",
            "service": settings.SERVICE_NAME,
            "dependencies": {
                "ignite": "healthy" if ignite_status else "unhealthy",
                "pulsar": "healthy" if pulsar_status else "unhealthy",
                "redis": "healthy" if redis_status else "unhealthy"
            }
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "service": settings.SERVICE_NAME,
            "error": str(e)
        }


@app.get("/ready")
async def readiness() -> Dict[str, str]:
    """Readiness check endpoint"""
    # Add readiness checks here
    return {"status": "ready"}


async def check_ignite_health() -> bool:
    """Check Ignite connection health"""
    try:
        client = await get_ignite_client()
        return client is not None
    except Exception:
        return False


async def check_pulsar_health() -> bool:
    """Check Pulsar connection health"""
    try:
        client = await get_pulsar_client()
        return client is not None
    except Exception:
        return False


async def check_redis_health() -> bool:
    """Check Redis connection health"""
    try:
        client = await get_redis_client()
        await client.ping()
        return True
    except Exception:
        return False


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.SERVICE_PORT,
        reload=settings.DEBUG
    ) 