"""
Unified Trading Platform Service

Comprehensive trading platform combining social trading, copy trading, prediction markets,
and advanced market mechanisms for the PlatformQ ecosystem.
"""

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, List, Optional, Set, Any
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import logging
import json
import os

# Shared Components
from app.shared.order_matching import UnifiedMatchingEngine
from app.api import unified_trading

# Social Trading Components
from app.social_trading.trading.strategy_engine import StrategyEngine
from app.social_trading.trading.copy_executor import CopyTradingExecutor
from app.social_trading.reputation.reputation_engine import ReputationEngine
from app.social_trading.analytics.performance_tracker import PerformanceTracker
from app.social_trading.copy.portfolio_copier import PortfolioCopier
from app.social_trading.dao.trader_dao import TraderDAO

# Prediction Markets Components
from app.prediction_markets.markets.market_engine import MarketEngine
from app.prediction_markets.markets.conditional_engine import ConditionalMarketEngine
from app.prediction_markets.resolution.oracle_resolver import OracleResolver
from app.prediction_markets.liquidity.amm_pool import PredictionAMM
from app.prediction_markets.governance.market_dao import MarketGovernanceDAO

# API Routers
from app.social_trading.api import (
    strategies as social_strategies,
    copy_trading,
    reputation as social_reputation,
    analytics as social_analytics,
    social,
    strategy_markets,
    automated_trading
)
from app.prediction_markets.api import (
    markets as prediction_markets,
    conditional,
    resolution,
    liquidity,
    governance as prediction_governance
)

# Import Vault/Consul integration
from app.vault_consul_integration import VaultConsulIntegration

# Shared Components and Integrations
from app.integrations import (
    IgniteCache,
    PulsarEventPublisher,
    ElasticsearchClient,
    JanusGraphClient,
    BlockchainClient,
    DerivativesEngineClient,
    GraphIntelligenceClient,
    NeuromorphicClient,
    OracleAggregatorClient,
    SocialDataClient
)
from app.integrations.event_driven_trading import EventDrivenTradingIntegration

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
vault_consul: Optional[VaultConsulIntegration] = None
exchange_connectors: Dict[str, Any] = {}
strategy_engine: Optional[StrategyEngine] = None
copy_executor: Optional[CopyTradingExecutor] = None
reputation_engine: Optional[ReputationEngine] = None
performance_tracker: Optional[PerformanceTracker] = None
portfolio_copier: Optional[PortfolioCopier] = None
trader_dao: Optional[TraderDAO] = None
market_engine: Optional[MarketEngine] = None
conditional_engine: Optional[ConditionalMarketEngine] = None
oracle_resolver: Optional[OracleResolver] = None
prediction_amm: Optional[PredictionAMM] = None
market_dao: Optional[MarketGovernanceDAO] = None
matching_engine: Optional[UnifiedMatchingEngine] = None
websocket_manager: Set[WebSocket] = set()
event_driven_trading: Optional[EventDrivenTradingIntegration] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan with Vault/Consul integration"""
    global vault_consul, exchange_connectors
    global matching_engine, strategy_engine, copy_executor
    global reputation_engine, performance_tracker, portfolio_copier
    global trader_dao, market_engine, conditional_engine
    global oracle_resolver, prediction_amm, market_dao
    global event_driven_trading
    
    # Initialize Vault/Consul integration
    vault_consul = VaultConsulIntegration({
        "vault_addr": os.getenv("VAULT_ADDR", "http://vault:8200"),
        "vault_token": os.getenv("VAULT_TOKEN"),
        "consul_addr": os.getenv("CONSUL_ADDR", "http://consul:8500")
    })
    
    await vault_consul.initialize()
    
    # Register service with Consul
    await vault_consul.register_service(
        tags=["trading", "social-trading", "prediction-markets", "copy-trading"],
        meta={
            "version": "1.0.0",
            "exchanges": "binance,coinbase,kraken,ftx",
            "features": "social,copy,predictions,automated"
        }
    )
    
    # Initialize exchange connectors with secure credentials
    for exchange in ["binance", "coinbase", "kraken", "ftx"]:
        try:
            credentials = await vault_consul.get_exchange_credentials(exchange)
            # Initialize exchange connector with credentials
            # exchange_connectors[exchange] = ExchangeConnector(credentials)
        except Exception as e:
            logger.warning(f"Failed to initialize {exchange}: {e}")
    
    # Initialize components with secure configuration
    logger.info("Initializing Trading Platform components...")
    
    # Shared components with secure key management
    matching_engine = UnifiedMatchingEngine(
        vault_consul=vault_consul
    )
    
    # Social Trading components
    strategy_engine = StrategyEngine(
        vault_consul=vault_consul
    )
    copy_executor = CopyTradingExecutor(
        matching_engine=matching_engine,
        vault_consul=vault_consul
    )
    reputation_engine = ReputationEngine(
        vault_consul=vault_consul
    )
    performance_tracker = PerformanceTracker(
        vault_consul=vault_consul
    )
    portfolio_copier = PortfolioCopier(
        copy_executor=copy_executor,
        vault_consul=vault_consul
    )
    trader_dao = TraderDAO(
        vault_consul=vault_consul
    )
    
    # Prediction Markets components
    market_engine = MarketEngine(
        matching_engine=matching_engine,
        vault_consul=vault_consul
    )
    conditional_engine = ConditionalMarketEngine(
        market_engine=market_engine,
        vault_consul=vault_consul
    )
    oracle_resolver = OracleResolver(
        vault_consul=vault_consul
    )
    prediction_amm = PredictionAMM(
        vault_consul=vault_consul
    )
    market_dao = MarketGovernanceDAO(
        vault_consul=vault_consul
    )
    
    # Initialize event-driven trading integration
    event_driven_trading = EventDrivenTradingIntegration(
        vault_consul=vault_consul
    )
    await event_driven_trading.initialize()
    
    # Register event handlers for matching engine
    async def on_trade_executed(trade):
        await event_driven_trading.process_trade_execution(trade)
    
    async def on_position_updated(position):
        await event_driven_trading.process_position_update(position)
    
    # Hook into matching engine events
    matching_engine.on_trade_executed = on_trade_executed
    
    # Hook into copy trading events
    async def on_copy_trade_executed(copy_trade):
        # Create trading relationship in graph
        await event_driven_trading.add_trading_relationship(
            from_trader=copy_trade["follower_id"],
            to_trader=copy_trade["leader_id"],
            relationship_type="copy_trading",
            strength=0.8,
            exposure_amount=copy_trade["amount"]
        )
    
    copy_executor.on_copy_trade = on_copy_trade_executed
    
    # Start background tasks
    asyncio.create_task(monitor_exchange_health())
    asyncio.create_task(update_trading_metrics())
    asyncio.create_task(enforce_risk_limits())
    
    logger.info("Trading Platform Service initialized successfully")
    
    yield
    
    # Cleanup
    logger.info("Shutting down Trading Platform Service...")
    
    await vault_consul.deregister_service()
    await vault_consul.shutdown()
    
    logger.info("Trading Platform Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Unified Trading Platform Service",
    description="Comprehensive trading platform with social trading and prediction markets",
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

# Include Social Trading API routers
app.include_router(social_strategies.router, prefix="/api/v1/social", tags=["social-strategies"])
app.include_router(copy_trading.router, prefix="/api/v1/social", tags=["copy-trading"])
app.include_router(social_reputation.router, prefix="/api/v1/social", tags=["social-reputation"])
app.include_router(social_analytics.router, prefix="/api/v1/social", tags=["social-analytics"])
app.include_router(social.router, prefix="/api/v1/social", tags=["social"])
app.include_router(strategy_markets.router, prefix="/api/v1/social", tags=["strategy-markets"])
app.include_router(automated_trading.router, prefix="/api/v1/social", tags=["automated-trading"])

# Include Prediction Markets API routers
app.include_router(prediction_markets.router, prefix="/api/v1/prediction", tags=["prediction-markets"])
app.include_router(prediction_trading.router, prefix="/api/v1/prediction", tags=["prediction-trading"])
app.include_router(resolution.router, prefix="/api/v1/prediction", tags=["resolution"])
app.include_router(prediction_analytics.router, prefix="/api/v1/prediction", tags=["prediction-analytics"])
app.include_router(prediction_governance.router, prefix="/api/v1/prediction", tags=["prediction-governance"])

# Include Unified Trading API router
app.include_router(unified_trading.router, prefix="/api/v1", tags=["unified-trading"])

# Root endpoint
@app.get("/")
async def root():
    """Service information endpoint"""
    return {
        "service": "trading-platform-service",
        "version": "1.0.0",
        "status": "operational",
        "description": "Unified trading platform with social trading and prediction markets",
        "features": [
            # Social Trading Features
            "strategy-nfts",
            "copy-trading",
            "performance-tracking",
            "reputation-system",
            "social-feed",
            "automated-trading",
            "trader-dao",
            
            # Prediction Markets Features
            "binary-markets",
            "categorical-markets",
            "scalar-markets",
            "conditional-markets",
            "amm-liquidity",
            "oracle-resolution",
            "market-governance",
            
            # Shared Features
            "real-time-analytics",
            "blockchain-integration",
            "distributed-caching",
            "event-streaming"
        ],
        "components": {
            "social_trading": {
                "strategy_engine": strategy_engine is not None,
                "copy_executor": copy_executor is not None,
                "reputation_engine": reputation_engine is not None,
                "performance_tracker": performance_tracker is not None
            },
            "prediction_markets": {
                "market_engine": market_engine is not None,
                "conditional_engine": conditional_engine is not None,
                "oracle_resolver": oracle_resolver is not None,
                "prediction_amm": prediction_amm is not None
            },
            "shared": {
                "matching_engine": matching_engine is not None
            }
        }
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    """Enhanced health check with trading platform status"""
    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "checks": {}
    }
    
    # Check Vault/Consul
    if vault_consul:
        health["checks"]["vault"] = await vault_consul.check_vault_health()
        health["checks"]["consul"] = await vault_consul.check_consul_health()
    else:
        health["status"] = "unhealthy"
        health["checks"]["vault"] = {"status": "not_initialized"}
        health["checks"]["consul"] = {"status": "not_initialized"}
    
    # Check exchanges
    if exchange_connectors:
        exchange_health = {}
        for exchange in exchange_connectors:
            try:
                # Check exchange connectivity
                exchange_health[exchange] = {"status": "healthy"}
            except Exception:
                exchange_health[exchange] = {"status": "unhealthy"}
                health["status"] = "degraded"
        
        health["checks"]["exchanges"] = exchange_health
    
    # Check components
    component_health = {
        "matching_engine": "healthy" if matching_engine else "not_initialized",
        "strategy_engine": "healthy" if strategy_engine else "not_initialized",
        "market_engine": "healthy" if market_engine else "not_initialized"
    }
    
    health["checks"]["components"] = component_health
    
    return health


# WebSocket endpoint for real-time updates
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time trading updates"""
    await websocket.accept()
    websocket_manager.add(websocket)
    
    try:
        while True:
            # Keep connection alive and handle messages
            data = await websocket.receive_text()
            message = json.loads(data)
            
            # Route messages to appropriate handlers
            if message.get("type") == "subscribe":
                await handle_subscription(websocket, message)
            elif message.get("type") == "unsubscribe":
                await handle_unsubscription(websocket, message)
                
    except WebSocketDisconnect:
        websocket_manager.remove(websocket)
        

async def handle_subscription(websocket: WebSocket, message: dict):
    """Handle WebSocket subscriptions"""
    channel = message.get("channel")
    
    if channel == "social_trading":
        # Subscribe to social trading updates
        await strategy_engine.add_subscriber(websocket)
    elif channel == "prediction_markets":
        # Subscribe to prediction market updates
        await market_engine.add_subscriber(websocket)
    elif channel == "performance":
        # Subscribe to performance updates
        await performance_tracker.add_subscriber(websocket)
        

async def handle_unsubscription(websocket: WebSocket, message: dict):
    """Handle WebSocket unsubscriptions"""
    channel = message.get("channel")
    
    if channel == "social_trading":
        await strategy_engine.remove_subscriber(websocket)
    elif channel == "prediction_markets":
        await market_engine.remove_subscriber(websocket)
    elif channel == "performance":
        await performance_tracker.remove_subscriber(websocket) 

# Security and Order Management Endpoints

from pydantic import BaseModel

class OrderSignRequest(BaseModel):
    order_type: str  # market, limit, stop
    side: str  # buy, sell
    symbol: str
    quantity: str
    price: Optional[str] = None
    exchange: str
    trader_id: str

@app.post("/api/orders/sign")
async def sign_trading_order(request: OrderSignRequest):
    """Sign trading order for execution"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        # Validate risk limits
        validation = await vault_consul.validate_order_limits(
            {
                "market": request.symbol,
                "quantity": request.quantity,
                "price": request.price or "1"
            },
            request.trader_id
        )
        
        if not validation["valid"]:
            raise HTTPException(status_code=400, detail=validation["reason"])
        
        # Sign order
        order_data = request.dict()
        order_data["timestamp"] = int(datetime.utcnow().timestamp())
        
        signed_order = await vault_consul.sign_order(order_data, request.exchange)
        
        # Execute order through exchange connector
        # ... execution logic ...
        
        return signed_order
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/orders/verify")
async def verify_order_signature(signed_order: Dict[str, Any]):
    """Verify order signature"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        valid = await vault_consul.verify_order_signature(signed_order)
        return {"valid": valid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Exchange Credentials Management

@app.post("/api/exchanges/{exchange}/credentials")
async def update_exchange_credentials(
    exchange: str,
    credentials: Dict[str, str]
):
    """Update exchange API credentials"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        await vault_consul.store_exchange_credentials(exchange, credentials)
        
        # Reinitialize exchange connector
        # ... reconnection logic ...
        
        return {"status": "updated", "exchange": exchange}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Trading Strategy Encryption

class TradingStrategy(BaseModel):
    strategy_id: str
    name: str
    algorithm: Dict[str, Any]
    parameters: Dict[str, Any]
    risk_limits: Dict[str, Any]

@app.post("/api/strategies/encrypt")
async def encrypt_trading_strategy(strategy: TradingStrategy):
    """Encrypt trading strategy for secure storage"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        encrypted = await vault_consul.encrypt_strategy(
            strategy.dict(),
            strategy.strategy_id
        )
        
        return {
            "strategy_id": strategy.strategy_id,
            "encrypted": encrypted
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/strategies/{strategy_id}/decrypt")
async def decrypt_trading_strategy(
    strategy_id: str,
    encrypted_strategy: str
):
    """Decrypt trading strategy"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        decrypted = await vault_consul.decrypt_strategy(
            encrypted_strategy,
            strategy_id
        )
        
        return decrypted
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Risk Management

@app.get("/api/risk/limits/{trader_id}")
async def get_trader_risk_limits(trader_id: str, market: Optional[str] = None):
    """Get risk limits for trader"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        limits = await vault_consul.get_risk_limits(trader_id, market)
        return limits
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Settlement and Price Feeds

class SettlementRequest(BaseModel):
    trades: List[Dict[str, Any]]
    settlement_type: str = "default"
    total_value: str

@app.post("/api/settlement/sign")
async def sign_settlement_batch(request: SettlementRequest):
    """Sign settlement batch"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        settlement_data = {
            "trades": request.trades,
            "type": request.settlement_type,
            "total_value": request.total_value,
            "trade_count": len(request.trades)
        }
        
        signed = await vault_consul.sign_settlement(settlement_data)
        
        return signed
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/price-feeds/{provider}/auth")
async def get_price_feed_auth(provider: str):
    """Get authentication for price feed provider"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        auth = await vault_consul.get_price_feed_auth(provider)
        return auth
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Background Tasks

async def monitor_exchange_health():
    """Monitor exchange connectivity and health"""
    while True:
        try:
            if vault_consul and exchange_connectors:
                for exchange, connector in exchange_connectors.items():
                    try:
                        # Check exchange health
                        # health = await connector.check_health()
                        
                        # Store health status
                        await vault_consul.consul.kv.put(
                            f"trading/exchanges/{exchange}/health",
                            json.dumps({
                                "status": "healthy",
                                "timestamp": datetime.utcnow().isoformat()
                            })
                        )
                    except Exception as e:
                        logger.error(f"Exchange {exchange} unhealthy: {e}")
                        
            await asyncio.sleep(30)  # Check every 30 seconds
        except Exception as e:
            logger.error(f"Exchange monitoring error: {e}")
            await asyncio.sleep(60)

async def update_trading_metrics():
    """Update trading performance metrics"""
    while True:
        try:
            if vault_consul and performance_tracker:
                # Get all active traders
                traders = await trader_dao.get_active_traders()
                
                for trader in traders:
                    # Calculate metrics
                    metrics = await performance_tracker.calculate_metrics(
                        trader["id"]
                    )
                    
                    # Store metrics
                    await vault_consul.store_trading_metrics(
                        trader["id"],
                        metrics
                    )
                    
            await asyncio.sleep(300)  # Update every 5 minutes
        except Exception as e:
            logger.error(f"Metrics update error: {e}")
            await asyncio.sleep(60)

async def enforce_risk_limits():
    """Monitor and enforce risk limits"""
    while True:
        try:
            if vault_consul:
                # Check all active positions
                # Enforce daily loss limits
                # Cancel orders if limits exceeded
                pass
                
            await asyncio.sleep(60)  # Check every minute
        except Exception as e:
            logger.error(f"Risk enforcement error: {e}")
            await asyncio.sleep(60) 