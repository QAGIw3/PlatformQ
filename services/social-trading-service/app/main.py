"""Social Trading Service main application."""

import asyncio
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from app.api import social
from app.config import Settings
from app.copy.copy_executor import CopyTradingExecutor
from app.reputation.reputation_engine import ReputationEngine
from app.dependencies import init_dependencies


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metrics
request_count = Counter("social_trading_requests_total", "Total requests", ["method", "endpoint"])
request_duration = Histogram("social_trading_request_duration_seconds", "Request duration")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management."""
    # Startup
    logger.info("Starting Social Trading Service...")
    
    # Initialize settings
    settings = Settings()
    
    # Initialize components
    copy_executor = CopyTradingExecutor(settings)
    reputation_engine = ReputationEngine(settings)
    
    # Start components
    await copy_executor.start()
    await reputation_engine.start()
    
    # Initialize dependencies
    init_dependencies(settings, copy_executor, reputation_engine)
    
    logger.info("Social Trading Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Social Trading Service...")
    
    # Stop components
    await copy_executor.stop()
    await reputation_engine.stop()
    
    logger.info("Social Trading Service stopped")


# Create FastAPI app
app = FastAPI(
    title="Social Trading Service",
    description="Social trading platform with copy trading and reputation system",
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
app.include_router(social.router, prefix="/api/v1/social", tags=["social"])

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "social-trading-service"
    }

# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint."""
    return generate_latest()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 