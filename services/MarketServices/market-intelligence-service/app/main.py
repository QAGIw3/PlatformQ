"""Market Intelligence Service - Main application."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import Settings
from .api import insights
from .integrations.graph_data_integration import GraphDataIntegration
from .integrations.trading_core_integration import TradingCoreMarketIntelligence


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Global instances
graph_integration: GraphDataIntegration = None
trading_core_intel: TradingCoreMarketIntelligence = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    global graph_integration, trading_core_intel
    
    logger.info("Starting Market Intelligence Service...")
    
    # Initialize components
    settings = Settings()
    
    # Initialize integrations
    graph_integration = GraphDataIntegration()
    await graph_integration.initialize()
    
    trading_core_intel = TradingCoreMarketIntelligence()
    await trading_core_intel.initialize()
    
    # Store in app state
    app.state.graph_integration = graph_integration
    app.state.trading_core_intel = trading_core_intel
    app.state.settings = settings
    
    logger.info("Market Intelligence Service started successfully")
    
    yield
    
    logger.info("Shutting down Market Intelligence Service...")


# Create FastAPI application
app = FastAPI(
    title="Market Intelligence Service",
    description="Real-time market data and analytics with graph intelligence",
    version="2.0.0",
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

# Include API routers
app.include_router(insights.router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Market Intelligence Service",
        "version": "2.0.0",
        "status": "operational",
        "endpoints": {
            "insights": "/api/v1/insights",
            "manipulation_detection": "/api/v1/insights/manipulation/detect",
            "systemic_risk": "/api/v1/insights/systemic-risk",
            "trader_network": "/api/v1/insights/trader/{trader_id}/network",
            "correlations": "/api/v1/insights/correlations/{asset_id}",
            "health": "/health"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    health_status = {
        "status": "healthy",
        "integrations": {
            "graph_intelligence": graph_integration is not None,
            "trading_core": trading_core_intel is not None
        }
    }
    
    return health_status


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8022,
        reload=True,
        log_level="info"
    ) 