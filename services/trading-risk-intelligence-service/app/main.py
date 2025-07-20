"""
Trading Risk Intelligence Service

Specialized service for trading risk network analysis and systemic risk detection.
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
import logging
from typing import Optional
from datetime import datetime

from platformq_shared import (
    create_base_app,
    add_error_handlers
)

from .api.trading_risk_api import router as trading_risk_router
from .core.trading_risk_network import TradingRiskNetwork
from .vault_consul_integration import VaultConsulIntegration

logger = logging.getLogger(__name__)

# Service components
vault_consul = None
trading_risk_network = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan events"""
    global vault_consul, trading_risk_network
    
    logger.info("Starting Trading Risk Intelligence Service...")
    
    # Initialize Vault/Consul integration
    vault_consul = VaultConsulIntegration()
    await vault_consul.initialize()
    
    # Initialize trading risk network
    trading_risk_network = TradingRiskNetwork(
        gremlin_url=vault_consul.janusgraph_config.get('gremlin_url', 'ws://janusgraph:8182/gremlin')
    )
    
    # Store in app state
    app.state.vault_consul = vault_consul
    app.state.trading_risk_network = trading_risk_network
    
    logger.info("Trading Risk Intelligence Service started successfully")
    
    yield
    
    # Cleanup
    logger.info("Shutting down Trading Risk Intelligence Service...")
    await vault_consul.close()
    logger.info("Trading Risk Intelligence Service shutdown complete")


# Create FastAPI app
app = create_base_app(
    title="Trading Risk Intelligence Service",
    description="Specialized service for trading risk network analysis and systemic risk detection",
    version="1.0.0",
    lifespan=lifespan
)

# Add error handlers
add_error_handlers(app)

# Include routers
app.include_router(trading_risk_router, prefix="/api/v1/trading-risk")


@app.get("/")
def read_root():
    """Service information endpoint"""
    return {
        "service": "trading-risk-intelligence-service",
        "version": "1.0.0",
        "status": "operational",
        "description": "Trading risk network analysis and systemic risk detection",
        "features": [
            "risk-propagation-analysis",
            "systemic-risk-detection",
            "cascade-simulation",
            "risk-clustering",
            "trader-network-analysis"
        ]
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Check Vault/Consul connection
        vault_status = await vault_consul.check_health() if vault_consul else False
        
        # Check JanusGraph connection
        graph_status = trading_risk_network.check_connection() if trading_risk_network else False
        
        if vault_status and graph_status:
            return {
                "status": "healthy",
                "timestamp": datetime.utcnow().isoformat(),
                "checks": {
                    "vault_consul": "healthy",
                    "janusgraph": "healthy"
                }
            }
        else:
            return {
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "checks": {
                    "vault_consul": "healthy" if vault_status else "unhealthy",
                    "janusgraph": "healthy" if graph_status else "unhealthy"
                }
            }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e)
        } 