"""Futures Service main application."""

import asyncio
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from app.api import futures
from app.config import Settings
from app.cache.ignite_manager import FuturesCacheManager
from app.core.funding_engine import FundingEngine
from app.core.settlement_engine import SettlementEngine
from app.dependencies import init_dependencies


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metrics
request_count = Counter("futures_requests_total", "Total requests", ["method", "endpoint"])
request_duration = Histogram("futures_request_duration_seconds", "Request duration")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management."""
    # Startup
    logger.info("Starting Futures Service...")
    
    # Initialize settings
    settings = Settings()
    
    # Initialize cache manager
    cache_manager = FuturesCacheManager(settings)
    await cache_manager.connect()
    
    # Initialize engines
    funding_engine = FundingEngine(settings, cache_manager)
    settlement_engine = SettlementEngine(settings, cache_manager)
    
    # Start engines
    await funding_engine.start()
    await settlement_engine.start()
    
    # Initialize dependencies
    init_dependencies(settings, cache_manager, funding_engine, settlement_engine)
    
    # Start funding cycles for existing perpetuals
    contracts = await cache_manager.get_active_contracts()
    for contract in contracts:
        if contract.contract_type == "perpetual":
            await funding_engine.start_funding_cycle(contract.symbol)
    
    logger.info("Futures Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Futures Service...")
    
    # Stop engines
    await funding_engine.stop()
    await settlement_engine.stop()
    
    # Disconnect from cache
    await cache_manager.disconnect()
    
    logger.info("Futures Service stopped")


# Create FastAPI app
app = FastAPI(
    title="Futures Service",
    description="High-performance futures trading service",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add middleware for metrics
@app.middleware("http")
async def track_metrics(request: Request, call_next):
    """Track request metrics."""
    method = request.method
    endpoint = request.url.path
    
    request_count.labels(method=method, endpoint=endpoint).inc()
    
    with request_duration.time():
        response = await call_next(request)
    
    return response

# Add Prometheus instrumentation
instrumentator = Instrumentator()
instrumentator.instrument(app).expose(app)

# Include routers
app.include_router(futures.router, prefix="/api/v1/futures", tags=["futures"])

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "futures-service"
    }

# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint."""
    return generate_latest()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 