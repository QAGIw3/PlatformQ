"""Trading Core Service - Unified trading engine for all product types."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from .config import Settings
from .state import IgniteStateManager
from .events import FlinkEventProcessor  
from .core import (
    MatchingEngine, MatchingAlgorithm,
    OrderManager, PositionManager, MarketManager
)
from .dependencies import init_dependencies, cleanup_dependencies
from .api import (
    orders_router, markets_router, positions_router,
    trades_router, websocket_router
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    logger.info("Starting Trading Core Service...")
    
    # Initialize settings
    settings = Settings()
    
    # Initialize state manager
    state_manager = IgniteStateManager(settings)
    await state_manager.connect()
    
    # Initialize event processor
    event_processor = FlinkEventProcessor(settings)
    await event_processor.initialize()
    
    # Initialize matching engine
    matching_engine = MatchingEngine(
        state_manager=state_manager,
        event_processor=event_processor,
        algorithm=MatchingAlgorithm.PRICE_TIME
    )
    await matching_engine.initialize()
    
    # Initialize managers
    order_manager = OrderManager(
        state_manager=state_manager,
        matching_engine=matching_engine,
        event_processor=event_processor
    )
    
    position_manager = PositionManager(
        state_manager=state_manager,
        event_processor=event_processor
    )
    
    market_manager = MarketManager(
        state_manager=state_manager,
        event_processor=event_processor
    )
    
    # Initialize dependencies
    init_dependencies(
        settings=settings,
        state_manager=state_manager,
        event_processor=event_processor,
        matching_engine=matching_engine,
        order_manager=order_manager,
        position_manager=position_manager,
        market_manager=market_manager
    )
    
    # Start background tasks
    asyncio.create_task(market_update_loop(market_manager))
    asyncio.create_task(position_monitoring_loop(position_manager))
    
    # Start Flink processing
    # asyncio.create_task(event_processor.start())
    
    logger.info("Trading Core Service started successfully")
    
    yield
    
    # Cleanup
    logger.info("Shutting down Trading Core Service...")
    await cleanup_dependencies()
    logger.info("Trading Core Service shut down")


# Create FastAPI application
app = FastAPI(
    title="Trading Core Service",
    description="Unified trading engine for all product types",
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


# Include routers
app.include_router(orders_router, prefix="/api/v1")
app.include_router(markets_router, prefix="/api/v1")
app.include_router(positions_router, prefix="/api/v1")
app.include_router(trades_router, prefix="/api/v1")
app.include_router(websocket_router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Trading Core Service",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "orders": "/api/v1/orders",
            "markets": "/api/v1/markets",
            "positions": "/api/v1/positions",
            "trades": "/api/v1/trades",
            "websocket": "/api/v1/ws/market"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    # Would check actual component health
    return {
        "status": "healthy",
        "components": {
            "state_manager": "connected",
            "event_processor": "running",
            "matching_engine": "operational",
            "websocket": "available"
        }
    }


@app.get("/metrics")
async def metrics():
    """Service metrics endpoint."""
    from .dependencies import (
        get_matching_engine, get_order_manager,
        get_position_manager, get_market_manager
    )
    
    matching_engine = get_matching_engine()
    order_manager = get_order_manager()
    position_manager = get_position_manager()
    market_manager = get_market_manager()
    
    return {
        "matching_engine": matching_engine.get_metrics(),
        "order_manager": order_manager.get_metrics(),
        "position_manager": position_manager.get_metrics(),
        "market_manager": market_manager.get_metrics()
    }


# Background tasks
async def market_update_loop(market_manager: MarketManager):
    """Periodically check circuit breakers and market status."""
    while True:
        try:
            await asyncio.sleep(30)  # Check every 30 seconds
            await market_manager.check_circuit_breakers()
        except Exception as e:
            logger.error(f"Error in market update loop: {e}")


async def position_monitoring_loop(position_manager: PositionManager):
    """Monitor positions for liquidations."""
    while True:
        try:
            await asyncio.sleep(60)  # Check every minute
            liquidations = await position_manager.check_liquidations()
            if liquidations:
                logger.warning(f"Found {len(liquidations)} positions for liquidation")
        except Exception as e:
            logger.error(f"Error in position monitoring loop: {e}")


# Exception handlers
@app.exception_handler(ValueError)
async def value_error_handler(request, exc):
    """Handle validation errors."""
    return JSONResponse(
        status_code=400,
        content={"detail": str(exc)}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc):
    """Handle general exceptions."""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )


if __name__ == "__main__":
    # Run with uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8020,
        reload=True,
        log_level="info"
    ) 