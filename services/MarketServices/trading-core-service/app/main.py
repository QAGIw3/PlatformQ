"""Trading Core Service - Unified high-performance trading engine."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from .config import Settings, settings
from .api import orders_router, markets_router, positions_router, trades_router, websocket_router
from .api.internal import router as internal_router
from .core.matching_engine import MatchingEngine, MatchingAlgorithm
from .core.order_manager import OrderManager
from .core.position_manager import PositionManager
from .core.market_manager import MarketManager
from .state.ignite_manager import IgniteStateManager
from .events.flink_processor import FlinkEventProcessor
from .integrations import DerivativesAdapter, ComputeMarketAdapter
from .integrations.platform_direct import PlatformDirectIntegration
from .dependencies import init_dependencies

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Global instances
state_manager = None
event_processor = None
matching_engine = None
order_manager = None
position_manager = None
market_manager = None
derivatives_adapter = None
compute_adapter = None
platform_integration = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global state_manager, event_processor, matching_engine
    global order_manager, position_manager, market_manager
    global derivatives_adapter, compute_adapter, platform_integration
    
    logger.info("Starting Trading Core Service...")
    
    try:
        # Initialize state manager
        state_manager = IgniteStateManager(settings)
        await state_manager.connect()
        logger.info("Connected to Ignite state manager")
        
        # Initialize event processor
        event_processor = FlinkEventProcessor(settings)
        await event_processor.initialize()
        logger.info("Initialized Flink event processor")
        
        # Initialize matching engine
        matching_engine = MatchingEngine(
            state_manager=state_manager,
            event_processor=event_processor,
            algorithm=MatchingAlgorithm.PRICE_TIME
        )
        await matching_engine.initialize()
        logger.info("Initialized matching engine")
        
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
        
        # Initialize platform direct integration
        if hasattr(settings, 'enable_direct_comm') and settings.enable_direct_comm:
            platform_integration = PlatformDirectIntegration(
                matching_engine=matching_engine,
                order_manager=order_manager,
                position_manager=position_manager,
                state_manager=state_manager
            )
            await platform_integration.initialize()
            logger.info("Initialized platform direct integration")
        
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
        
        # Start background tasks
        event_processor.start()
        
        logger.info("Trading Core Service started successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start Trading Core Service: {e}")
        raise
    finally:
        logger.info("Shutting down Trading Core Service...")
        
        # Cleanup
        if event_processor:
            await event_processor.stop()
        
        if matching_engine:
            await matching_engine.shutdown()
            
        if state_manager:
            await state_manager.disconnect()
            
        logger.info("Trading Core Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Trading Core Service",
    description="High-performance trading engine with real-time order matching",
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
app.include_router(orders_router, prefix=f"{settings.api_prefix}/orders", tags=["orders"])
app.include_router(markets_router, prefix=f"{settings.api_prefix}/markets", tags=["markets"])
app.include_router(positions_router, prefix=f"{settings.api_prefix}/positions", tags=["positions"])
app.include_router(trades_router, prefix=f"{settings.api_prefix}/trades", tags=["trades"])
app.include_router(websocket_router, prefix="/ws", tags=["websocket"])
app.include_router(internal_router, tags=["internal"])

# Instrument with Prometheus
Instrumentator().instrument(app).expose(app)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Trading Core Service",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "orders": f"{settings.api_prefix}/orders",
            "markets": f"{settings.api_prefix}/markets",
            "positions": f"{settings.api_prefix}/positions",
            "trades": f"{settings.api_prefix}/trades",
            "websocket": "/ws",
            "health": "/health",
            "metrics": "/metrics"
        }
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "Trading Core Service",
        "matching_engine": matching_engine.get_metrics() if matching_engine else None
    } 