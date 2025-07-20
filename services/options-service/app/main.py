"""Options Service main application."""

import asyncio
from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from app.config import Settings


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Metrics
request_count = Counter("options_requests_total", "Total requests", ["method", "endpoint"])
request_duration = Histogram("options_request_duration_seconds", "Request duration")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management."""
    # Startup
    logger.info("Starting Options Service...")
    
    # Initialize settings
    settings = Settings()
    
    # In production, initialize:
    # - Cache manager (Ignite)
    # - Volatility surface engine
    # - Options AMM
    # - Greeks calculator
    # - Pricing engines
    
    logger.info("Options Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Options Service...")
    logger.info("Options Service stopped")


# Create FastAPI app
app = FastAPI(
    title="Options Service",
    description="Options trading and pricing service",
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
# app.include_router(options.router, prefix="/api/v1/options", tags=["options"])

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "options-service"
    }

# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint."""
    return generate_latest()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 