"""Market Data Service main application."""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, generate_latest
from prometheus_fastapi_instrumentator import Instrumentator

from app.api import market_data, oracle_data
from app.config import MarketDataConfig
from app.core.aggregator import MarketDataAggregator
from app.websocket import market_stream

# Metrics
request_count = Counter("market_data_requests_total", "Total requests", ["method", "endpoint"])
request_duration = Histogram("market_data_request_duration_seconds", "Request duration")

# Global aggregator instance
aggregator: MarketDataAggregator = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management."""
    global aggregator
    
    # Startup
    settings = MarketDataConfig()
    # TODO: Initialize aggregator with proper dependencies
    # aggregator = MarketDataAggregator(settings, cache_manager, event_subscriber)
    # await aggregator.start()
    
    # Start background tasks
    background_tasks = []
    # for symbol in ["BTC/USD", "ETH/USD", "BNB/USD"]:
    #     task = asyncio.create_task(aggregator.build_candles(symbol))
    #     background_tasks.append(task)
    
    # Create WebSocket manager
    # app.state.ws_manager = market_stream.create_websocket_manager(aggregator)
    
    yield
    
    # Shutdown
    for task in background_tasks:
        task.cancel()
    
    # await aggregator.stop()


# Create FastAPI app
app = FastAPI(
    title="Market Data Service",
    description="Real-time market data aggregation and distribution",
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
app.include_router(market_data.router, prefix="/api/v1/market", tags=["market"])
app.include_router(oracle_data.router, prefix="/api/v1/oracle", tags=["oracle"])
app.include_router(market_stream.router, prefix="/ws", tags=["websocket"])

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "market-data-service",
        # "aggregator_running": aggregator is not None and aggregator._running
        "aggregator_running": False  # TODO: Fix when aggregator is properly initialized
    }

# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint."""
    return generate_latest()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 