"""
Unified Trading Platform Service

Comprehensive trading platform combining social trading, copy trading, prediction markets,
and advanced market mechanisms for the PlatformQ ecosystem.
"""

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, List, Optional, Set
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
import logging
import json

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
    trading as prediction_trading,
    resolution,
    analytics as prediction_analytics,
    governance as prediction_governance
)

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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup platform components"""
    global strategy_engine, copy_executor, reputation_engine, performance_tracker
    global portfolio_copier, trader_dao, market_engine, conditional_engine
    global oracle_resolver, prediction_amm, market_dao, matching_engine
    
    # Startup
    logger.info("Starting Unified Trading Platform Service...")
    
    # Initialize shared infrastructure
    ignite = IgniteCache()
    await ignite.connect()
    
    pulsar = PulsarEventPublisher()
    await pulsar.connect()
    
    elasticsearch = ElasticsearchClient()
    await elasticsearch.connect()
    
    janusgraph = JanusGraphClient()
    await janusgraph.connect()
    
    blockchain = BlockchainClient()
    await blockchain.connect()
    
    derivatives = DerivativesEngineClient()
    graph_intelligence = GraphIntelligenceClient()
    neuromorphic = NeuromorphicClient()
    oracle_aggregator = OracleAggregatorClient()
    social_data = SocialDataClient()
    
    # Initialize Unified Matching Engine
    logger.info("Initializing unified matching engine...")
    matching_engine = UnifiedMatchingEngine(
        ignite_client=ignite,
        pulsar_client=pulsar
    )
    
    # Store in app state for dependency injection
    app.state.matching_engine = matching_engine
    
    # Initialize Social Trading Components
    logger.info("Initializing social trading components...")
    
    strategy_engine = StrategyEngine(
        ignite_cache=ignite,
        pulsar_publisher=pulsar,
        neuromorphic_client=neuromorphic
    )
    
    copy_executor = CopyTradingExecutor(
        ignite_cache=ignite,
        derivatives_client=derivatives,
        blockchain_client=blockchain
    )
    
    reputation_engine = ReputationEngine(
        graph_client=janusgraph,
        ignite_cache=ignite,
        blockchain_client=blockchain
    )
    
    performance_tracker = PerformanceTracker(
        elasticsearch_client=elasticsearch,
        ignite_cache=ignite
    )
    
    portfolio_copier = PortfolioCopier(
        copy_executor=copy_executor,
        performance_tracker=performance_tracker,
        ignite_cache=ignite
    )
    
    trader_dao = TraderDAO(
        blockchain_client=blockchain,
        reputation_engine=reputation_engine
    )
    
    # Initialize Prediction Markets Components
    logger.info("Initializing prediction markets components...")
    
    market_engine = MarketEngine(
        ignite_cache=ignite,
        blockchain_client=blockchain,
        pulsar_publisher=pulsar
    )
    
    conditional_engine = ConditionalMarketEngine(
        market_engine=market_engine,
        graph_client=janusgraph
    )
    
    oracle_resolver = OracleResolver(
        oracle_aggregator=oracle_aggregator,
        blockchain_client=blockchain,
        ignite_cache=ignite
    )
    
    prediction_amm = PredictionAMM(
        ignite_cache=ignite,
        blockchain_client=blockchain
    )
    
    market_dao = MarketGovernanceDAO(
        blockchain_client=blockchain,
        ignite_cache=ignite
    )
    
    # Start background tasks
    asyncio.create_task(performance_tracker.start_monitoring())
    asyncio.create_task(reputation_engine.start_reputation_updates())
    asyncio.create_task(oracle_resolver.start_resolution_monitoring())
    asyncio.create_task(market_engine.start_market_monitoring())
    
    logger.info("Unified Trading Platform Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Unified Trading Platform Service...")
    
    # Stop background tasks
    await performance_tracker.stop_monitoring()
    await reputation_engine.stop_reputation_updates()
    await oracle_resolver.stop_resolution_monitoring()
    await market_engine.stop_market_monitoring()
    
    # Close connections
    await ignite.close()
    await pulsar.close()
    await elasticsearch.close()
    await janusgraph.close()
    await blockchain.close()
    
    logger.info("Unified Trading Platform Service shutdown complete")


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
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "components": {
            "social_trading": {
                "operational": all([
                    strategy_engine is not None,
                    copy_executor is not None,
                    reputation_engine is not None
                ])
            },
            "prediction_markets": {
                "operational": all([
                    market_engine is not None,
                    oracle_resolver is not None,
                    prediction_amm is not None
                ])
            }
        }
    }


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