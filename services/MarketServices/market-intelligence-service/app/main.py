"""Market Intelligence Service - Main application."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import Settings


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    logger.info("Starting Market Intelligence Service...")
    
    # Initialize components
    settings = Settings()
    
    # Initialize analytics engine, oracle aggregator, etc.
    # Simplified for now
    
    logger.info("Market Intelligence Service started successfully")
    
    yield
    
    logger.info("Shutting down Market Intelligence Service...")


# Create FastAPI application
app = FastAPI(
    title="Market Intelligence Service",
    description="Real-time market data and analytics",
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


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Market Intelligence Service",
        "version": "1.0.0",
        "status": "operational",
        "endpoints": {
            "market_data": "/api/v1/market-data",
            "analytics": "/api/v1/analytics",
            "oracle": "/api/v1/oracle",
            "indicators": "/api/v1/indicators"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8022,
        reload=True,
        log_level="info"
    ) 