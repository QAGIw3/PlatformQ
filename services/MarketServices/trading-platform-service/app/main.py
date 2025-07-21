"""Trading Platform Service with Social Trading Features."""

import os
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from decimal import Decimal
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .shared.service_client import ServiceClient
from .dependencies import (
    get_trading_core_client,
    get_current_user,
    get_vault_consul,
    get_copy_executor,
    get_reputation_engine
)
from .social_trading.models import TraderProfile, CopyTradingRelation
from .social_trading.copy.fast_copy_executor import FastCopyExecutor
from .social_trading.reputation.reputation_engine import ReputationEngine
from .integrations.event_driven_trading import EventDrivenTradingIntegration, TradingEventType
from .vault_consul_integration import VaultConsulIntegration

# API Routers
from .api.unified_trading import router as unified_trading_router
from .social_trading.api.social import router as social_router
from .social_trading.api.automated_trading import router as automated_trading_router
from .social_trading.api.strategy_markets import router as strategy_markets_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
copy_executor = None
reputation_engine = None
event_integration = None
ignite_client = None
vault_consul = None


class Settings:
    # Apache Ignite
    ignite_host = os.getenv("IGNITE_HOST", "localhost")
    ignite_port = int(os.getenv("IGNITE_PORT", "10800"))
    
    # Apache Pulsar
    pulsar_url = os.getenv("PULSAR_URL", "pulsar://localhost:6650")
    
    # External services
    trading_core_url = os.getenv("TRADING_CORE_URL", "http://localhost:8020")
    risk_service_url = os.getenv("RISK_SERVICE_URL", "http://localhost:8004")
    
    # Direct Communication
    enable_direct_comm = os.getenv("ENABLE_DIRECT_COMM", "true").lower() == "true"
    copy_trade_batch_size = int(os.getenv("COPY_TRADE_BATCH_SIZE", "100"))
    copy_trade_batch_window_ms = int(os.getenv("COPY_TRADE_BATCH_WINDOW_MS", "10"))
    
    # Copy Trading Parameters
    max_copy_allocation = float(os.getenv("MAX_COPY_ALLOCATION", "0.5"))
    
    # Reputation System
    reputation_update_interval = int(os.getenv("REPUTATION_UPDATE_INTERVAL", "3600"))
    reputation_decay_rate = float(os.getenv("REPUTATION_DECAY_RATE", "0.95"))
    min_trades_for_reputation = int(os.getenv("MIN_TRADES_FOR_REPUTATION", "10"))
    
    # Social Features
    max_posts_per_day = int(os.getenv("MAX_POSTS_PER_DAY", "50"))


settings = Settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global copy_executor, reputation_engine, event_integration, ignite_client, vault_consul
    
    logger.info("Starting Trading Platform Service...")
    
    try:
        # Initialize Ignite client
        try:
            from pyignite import Client
            ignite_client = Client()
            ignite_client.connect(settings.ignite_host, settings.ignite_port)
            logger.info(f"Connected to Ignite at {settings.ignite_host}:{settings.ignite_port}")
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            # Continue without Ignite for now
            ignite_client = None
        
        # Initialize Vault/Consul integration
        vault_consul_config = {
            'vault_url': os.getenv('VAULT_URL', 'http://localhost:8200'),
            'vault_token': os.getenv('VAULT_TOKEN', 'dev-token'),
            'consul_url': os.getenv('CONSUL_URL', 'http://localhost:8500'),
            'service_name': 'trading-platform-service'
        }
        vault_consul = VaultConsulIntegration(vault_consul_config)
        await vault_consul.initialize()
        
        # Initialize event-driven trading integration
        event_integration = EventDrivenTradingIntegration(vault_consul_integration=vault_consul)
        await event_integration.initialize()
        
        # Initialize copy executor based on direct communication setting
        if settings.enable_direct_comm and ignite_client:
            logger.info("Initializing FastCopyExecutor with direct communication")
            copy_executor = FastCopyExecutor(ignite_client)
            await copy_executor.initialize()
        else:
            logger.info("Direct communication disabled or Ignite unavailable, using HTTP-based executor")
            # Fallback to regular executor if needed
            from .social_trading.copy.copy_executor import CopyTradingExecutor
            copy_executor = CopyTradingExecutor(settings)
            await copy_executor.start()
        
        # Initialize reputation engine
        reputation_engine = ReputationEngine(settings)
        await reputation_engine.start()
        
        # Register event handlers
        async def on_trade_executed(trade):
            await event_integration.process_trade_execution(trade)
        
        async def on_position_updated(position):
            await event_integration.process_position_update(position)
        
        event_integration.register_event_handler(
            TradingEventType.TRADE_EXECUTED,
            on_trade_executed
        )
        
        event_integration.register_event_handler(
            TradingEventType.POSITION_UPDATED,
            on_position_updated
        )
        
        # Store instances in app state
        app.state.copy_executor = copy_executor
        app.state.reputation_engine = reputation_engine
        app.state.event_integration = event_integration
        app.state.ignite_client = ignite_client
        app.state.vault_consul = vault_consul
        app.state.trading_core_client = ServiceClient(
            base_url=settings.trading_core_url,
            service_name="trading-core"
        )
        
        logger.info("Trading Platform Service started successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start Trading Platform Service: {e}")
        raise
    finally:
        logger.info("Shutting down Trading Platform Service...")
        
        # Cleanup
        if copy_executor:
            if hasattr(copy_executor, 'stop'):
                await copy_executor.stop()
        
        if reputation_engine:
            await reputation_engine.stop()
            
        if ignite_client:
            ignite_client.close()
            
        if vault_consul:
            await vault_consul.shutdown()
            
        logger.info("Trading Platform Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Trading Platform Service",
    description="Social trading platform with copy trading and automated strategies",
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
app.include_router(unified_trading_router, prefix="/api/v1/trading", tags=["trading"])
app.include_router(social_router, prefix="/api/v1/social", tags=["social"])
app.include_router(automated_trading_router, prefix="/api/v1/automated", tags=["automated"])
app.include_router(strategy_markets_router, prefix="/api/v1/strategy-markets", tags=["strategy-markets"])


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": "Trading Platform Service",
        "version": "1.0.0",
        "status": "running",
        "features": [
            "Unified Trading API",
            "Copy Trading",
            "Social Trading",
            "Automated Strategies",
            "Strategy Markets",
            "Reputation System"
        ],
        "endpoints": {
            "trading": "/api/v1/trading",
            "social": "/api/v1/social",
            "automated": "/api/v1/automated",
            "strategy_markets": "/api/v1/strategy-markets",
            "websocket": "/ws",
            "health": "/health"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    health_status = {
        "status": "healthy",
        "service": "Trading Platform Service",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "copy_executor": "healthy" if copy_executor else "unavailable",
            "reputation_engine": "healthy" if reputation_engine else "unavailable",
            "ignite": "connected" if ignite_client else "disconnected",
            "direct_comm": settings.enable_direct_comm
        }
    }
    
    # Check trading core connectivity
    try:
        if hasattr(app.state, 'trading_core_client'):
            # Simple connectivity check
            health_status["components"]["trading_core"] = "connected"
    except:
        health_status["components"]["trading_core"] = "disconnected"
        
    return health_status


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates."""
    await websocket.accept()
    try:
        while True:
            data = await websocket.receive_text()
            # Handle WebSocket messages
            await websocket.send_text(f"Echo: {data}")
    except WebSocketDisconnect:
        logger.info("Client disconnected")


# Additional endpoints from the original main.py can be added here as needed 