"""Trading Core Service - Unified high-performance trading engine."""

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from .config import Settings
from .state import IgniteStateManager
from .events import FlinkEventProcessor
from .core import (
    MatchingEngine, MatchingAlgorithm,
    OrderManager, PositionManager, MarketManager
)
from .integrations import DerivativesAdapter, ComputeMarketAdapter
from .integrations.market_intelligence_integration import MarketIntelligenceIntegration
from .api import orders, markets, positions, websocket, internal
from .dependencies import init_dependencies


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("Starting Trading Core Service...")
    
    # Initialize settings
    settings = Settings()
    
    # Initialize state manager
    state_manager = IgniteStateManager(settings)
    await state_manager.initialize()
    
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
    
    # Initialize core managers
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
    
    # Initialize adapters
    derivatives_adapter = DerivativesAdapter(
        matching_engine=matching_engine,
        state_manager=state_manager,
        event_processor=event_processor
    )
    
    compute_adapter = ComputeMarketAdapter(
        matching_engine=matching_engine,
        state_manager=state_manager,
        event_processor=event_processor
    )
    
    # Initialize market intelligence integration
    market_intelligence = MarketIntelligenceIntegration(
        matching_engine=matching_engine
    )
    await market_intelligence.initialize()
    
    # Store in app state for access in endpoints
    app.state.market_intelligence = market_intelligence
    
    # Initialize dependencies
    init_dependencies(
        order_manager=order_manager,
        position_manager=position_manager,
        market_manager=market_manager,
        matching_engine=matching_engine,
        state_manager=state_manager,
        event_processor=event_processor,
        derivatives_adapter=derivatives_adapter,
        compute_adapter=compute_adapter
    )
    
    # Create default markets
    await market_manager.create_default_markets()
    
    logger.info("Trading Core Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Trading Core Service...")
    
    await matching_engine.shutdown()
    await event_processor.stop()
    await state_manager.disconnect()
    
    logger.info("Trading Core Service stopped")


# Create FastAPI app
app = FastAPI(
    title="Trading Core Service",
    description="Unified high-performance trading engine with Flink and Ignite",
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

# Include API routers
app.include_router(orders.router, prefix="/api/v1")
app.include_router(markets.router, prefix="/api/v1")
app.include_router(positions.router, prefix="/api/v1")
app.include_router(websocket.router, prefix="/api/v1/ws")
app.include_router(internal.router)  # Internal API for service-to-service

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Trading Core Service",
        "version": "1.0.0",
        "status": "operational",
        "features": [
            "High-performance order matching",
            "Apache Flink event processing",
            "Apache Ignite distributed state",
            "WebSocket real-time feeds",
            "Sub-millisecond latency",
            "Circuit breaker protection",
            "Derivatives integration",
            "Compute market unification",
            "Market intelligence ML integration"
        ]
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "trading-core-service"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 