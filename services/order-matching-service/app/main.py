from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
import time
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from prometheus_client.core import CollectorRegistry
from starlette.responses import Response

from .config import OrderMatchingConfig
from .core.matching_engine import MatchingEngine
from .api import orders, websocket
from .dependencies import get_matching_engine, get_config


# Prometheus metrics
registry = CollectorRegistry()
request_count = Counter(
    'oms_requests_total',
    'Total requests',
    ['method', 'endpoint', 'status'],
    registry=registry
)
request_duration = Histogram(
    'oms_request_duration_seconds',
    'Request duration',
    ['method', 'endpoint'],
    registry=registry
)
active_orders = Gauge(
    'oms_active_orders',
    'Number of active orders',
    ['market'],
    registry=registry
)
trades_total = Counter(
    'oms_trades_total',
    'Total trades executed',
    ['market'],
    registry=registry
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    config = get_config()
    matching_engine = get_matching_engine()
    
    # Initialize matching engine
    await matching_engine.initialize()
    
    # Start market data publisher
    market_data_task = asyncio.create_task(
        websocket.market_data_publisher(matching_engine)
    )
    
    print(f"Order Matching Service started on port {config.SERVICE_PORT}")
    print(f"WebSocket server on port {config.WEBSOCKET_PORT}")
    print(f"Metrics available on port {config.METRICS_PORT}")
    
    yield
    
    # Shutdown
    print("Shutting down Order Matching Service...")
    
    # Cancel background task
    market_data_task.cancel()
    
    # Shutdown matching engine
    await matching_engine.shutdown()
    
    print("Order Matching Service stopped")


# Create FastAPI app
app = FastAPI(
    title="Order Matching Service",
    description="High-performance order matching engine",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Middleware for metrics
@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    """Track request metrics"""
    start_time = time.time()
    
    # Process request
    response = await call_next(request)
    
    # Record metrics
    duration = time.time() - start_time
    request_count.labels(
        method=request.method,
        endpoint=request.url.path,
        status=response.status_code
    ).inc()
    request_duration.labels(
        method=request.method,
        endpoint=request.url.path
    ).observe(duration)
    
    return response


# Include routers
app.include_router(orders.router)
app.include_router(websocket.router)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Order Matching Service",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
async def health_check(matching_engine: MatchingEngine = Depends(get_matching_engine)):
    """Health check endpoint"""
    metrics = matching_engine.get_metrics()
    
    return {
        "status": "healthy",
        "timestamp": time.time_ns(),
        "metrics": {
            "orders_processed": metrics["orders_processed"],
            "trades_executed": metrics["trades_executed"],
            "active_markets": metrics["active_markets"],
            "latency_p99_ms": metrics["latency_p99_ms"]
        }
    }


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint"""
    # Update custom metrics
    matching_engine = get_matching_engine()
    
    # Update active orders per market
    for market_id, order_book in matching_engine.order_books.items():
        stats = order_book.get_stats()
        active_orders.labels(market=market_id).set(stats["total_orders"])
        trades_total.labels(market=market_id)._value.set(stats["trade_count"])
    
    # Generate metrics
    return Response(
        content=generate_latest(registry),
        media_type="text/plain"
    )


@app.get("/api/v1/markets")
async def list_markets(matching_engine: MatchingEngine = Depends(get_matching_engine)):
    """List all active markets"""
    markets = []
    
    for market_id in matching_engine.active_markets:
        order_book = matching_engine.order_books.get(market_id)
        if order_book:
            stats = order_book.get_stats()
            markets.append({
                "market_id": market_id,
                "total_orders": stats["total_orders"],
                "total_volume": stats["total_volume"],
                "trade_count": stats["trade_count"],
                "spread_bps": stats["spread_bps"]
            })
    
    return {
        "markets": markets,
        "count": len(markets)
    }


if __name__ == "__main__":
    import uvicorn
    
    config = OrderMatchingConfig()
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=config.SERVICE_PORT,
        log_level="info"
    ) 