from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, List, Optional
from decimal import Decimal
import logging
from datetime import datetime

from app.api import markets, trading, positions, analytics, lending, compute_futures, variance_swaps, monitoring_dashboard, partner_capacity, capacity_coordinator, risk_intelligence, asset_compute_nexus, compute_spot, burst_compute, compute_stablecoin, synthetic_derivatives
from app.engines.matching_engine import MatchingEngine
from app.engines.funding_engine import FundingEngine
from app.engines.settlement_engine import SettlementEngine
from app.engines.compute_futures_engine import ComputeFuturesEngine
from app.engines.partner_capacity_manager import PartnerCapacityManager
from app.engines.wholesale_arbitrage_engine import WholesaleArbitrageEngine
from app.engines.cross_service_capacity_coordinator import CrossServiceCapacityCoordinator
from app.engines.compute_spot_market import ComputeSpotMarket
from app.engines.burst_compute_derivatives import BurstComputeEngine
from app.engines.compute_stablecoin import ComputeStablecoinEngine
from app.engines.synthetic_derivatives_engine import SyntheticDerivativesEngine
from app.engines.margin_engine import MarginEngine
from app.collateral.multi_tier_engine import MultiTierCollateralEngine
from app.liquidation.partial_liquidator import PartialLiquidationEngine
from app.fees.dynamic_fee_engine import DynamicFeeEngine
from app.governance.market_dao import MarketCreationDAO
from app.integrations import (
    GraphIntelligenceClient,
    OracleAggregatorClient,
    DigitalAssetServiceClient,
    NeuromorphicServiceClient,
    VerifiableCredentialClient,
    PulsarEventPublisher,
    SeaTunnelClient
)
from app.integrations.ignite_optimized import get_optimized_cache
from app.database.connection_pool import get_db_pool
from app.integrations.graph_intelligence_integration import GraphIntelligenceIntegration
from app.integrations.asset_compute_nexus import AssetComputeNexus
from app.websocket.market_data import MarketDataWebSocket
from app.monitoring import PrometheusMetrics

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global instances
matching_engine: Optional[MatchingEngine] = None
funding_engine: Optional[FundingEngine] = None
settlement_engine: Optional[SettlementEngine] = None
compute_futures_engine: Optional[ComputeFuturesEngine] = None
partner_capacity_manager: Optional[PartnerCapacityManager] = None
wholesale_arbitrage_engine: Optional[WholesaleArbitrageEngine] = None
cross_service_coordinator: Optional[CrossServiceCapacityCoordinator] = None
compute_spot_market: Optional[ComputeSpotMarket] = None
burst_compute_engine: Optional[BurstComputeEngine] = None
compute_stablecoin_engine: Optional[ComputeStablecoinEngine] = None
synthetic_derivatives_engine: Optional[SyntheticDerivativesEngine] = None
collateral_engine: Optional[MultiTierCollateralEngine] = None
liquidation_engine: Optional[PartialLiquidationEngine] = None
fee_engine: Optional[DynamicFeeEngine] = None
market_dao: Optional[MarketCreationDAO] = None
websocket_manager: Optional[MarketDataWebSocket] = None
metrics: Optional[PrometheusMetrics] = None
graph_intelligence: Optional[GraphIntelligenceIntegration] = None
asset_compute_nexus: Optional[AssetComputeNexus] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle - startup and shutdown
    """
    # Startup
    logger.info("Starting Derivatives Engine Service...")
    
    # Initialize optimized cache first
    logger.info("Initializing optimized cache...")
    ignite = await get_optimized_cache()
    
    # Initialize database pools
    logger.info("Initializing database connection pools...")
    db_pool = await get_db_pool()
    
    pulsar = PulsarEventPublisher()
    await pulsar.connect()
    
    graph_client = GraphIntelligenceClient()
    oracle_client = OracleAggregatorClient()
    neuromorphic_client = NeuromorphicServiceClient()
    vc_client = VerifiableCredentialClient()
    asset_client = DigitalAssetServiceClient()
    seatunnel_client = SeaTunnelClient()
    
    # Initialize engines
    global matching_engine, funding_engine, settlement_engine, compute_futures_engine
    global partner_capacity_manager, wholesale_arbitrage_engine, cross_service_coordinator
    global collateral_engine, liquidation_engine, fee_engine
    global market_dao, websocket_manager, metrics, graph_intelligence, asset_compute_nexus
    global synthetic_derivatives_engine, compute_spot_market
    global burst_compute_engine, compute_stablecoin_engine
    
    # Collateral and risk engines
    collateral_engine = MultiTierCollateralEngine(ignite, graph_client, oracle_client, None, None)
    fee_engine = DynamicFeeEngine(graph_client, ignite)
    
    # Insurance pool integration
    from app.integrations import InsurancePoolClient
    insurance_pool = InsurancePoolClient()
    
    liquidation_engine = PartialLiquidationEngine(
        collateral_engine,
        insurance_pool,
        ignite,
        pulsar
    )
    
    # Trading engines
    matching_engine = MatchingEngine(
        neuromorphic_client,  # Use neuromorphic for ultra-fast matching
        ignite,
        pulsar
    )
    
    funding_engine = FundingEngine(
        oracle_client,
        ignite,
        pulsar
    )
    
    settlement_engine = SettlementEngine(
        collateral_engine,
        fee_engine,
        ignite,
        pulsar
    )
    
    # Partner capacity management
    partner_capacity_manager = PartnerCapacityManager(
        ignite,
        pulsar,
        oracle_client
    )
    
    # Compute futures engine with partner capacity support
    compute_futures_engine = ComputeFuturesEngine(
        ignite,
        pulsar,
        oracle_client,
        partner_capacity_manager
    )
    
    # Wholesale arbitrage engine
    wholesale_arbitrage_engine = WholesaleArbitrageEngine(
        ignite,
        pulsar,
        oracle_client,
        partner_capacity_manager,
        compute_futures_engine
    )
    
    # Cross-service capacity coordinator
    cross_service_coordinator = CrossServiceCapacityCoordinator(
        ignite,
        pulsar,
        partner_capacity_manager,
        compute_futures_engine
    )
    
    # Compute spot market
    global compute_spot_market
    compute_spot_market = ComputeSpotMarket(
        ignite,
        pulsar,
        oracle_client,
        partner_capacity_manager,
        cross_service_coordinator
    )
    
    # Set spot market instance in API module
    compute_spot.set_spot_market(compute_spot_market)
    
    # Create margin engine
    margin_engine = MarginEngine(
        graph_intelligence_client=graph_client,
        oracle_client=oracle_client,
        ignite_cache=ignite,
        pulsar_publisher=pulsar
    )
    await margin_engine.start()
    
    # Burst compute engine
    global burst_compute_engine
    burst_compute_engine = BurstComputeEngine(
        ignite,
        pulsar,
        oracle_client,
        partner_capacity_manager
    )
    
    # Set burst engine instance in API module
    burst_compute.set_burst_engine(burst_compute_engine)
    
    # Compute stablecoin engine
    global compute_stablecoin_engine
    from app.integrations import BlockchainEventBridgeClient
    blockchain_bridge = BlockchainEventBridgeClient()
    compute_stablecoin_engine = ComputeStablecoinEngine(
        ignite,
        pulsar,
        oracle_client,
        blockchain_bridge,
        collateral_engine
    )
    
    # Set stablecoin engine instance in API module
    compute_stablecoin.set_stablecoin_engine(compute_stablecoin_engine)
    
    # Synthetic derivatives engine
    synthetic_derivatives_engine = SyntheticDerivativesEngine(
        ignite,
        pulsar,
        oracle_client,
        collateral_engine
    )
    
    # Governance
    market_dao = MarketCreationDAO(
        graph_client,
        vc_client,
        ignite,
        pulsar
    )
    
    # WebSocket manager for real-time data
    websocket_manager = MarketDataWebSocket(ignite, pulsar)
    
    # Prometheus metrics
    metrics = PrometheusMetrics()
    
    # Initialize graph intelligence integration
    graph_intelligence = GraphIntelligenceIntegration(
        graph_service_url="http://graph-intelligence-service:8000",
        ignite_cache=ignite,
        pulsar_publisher=pulsar
    )
    
    # Set graph intelligence on collateral engine for risk-adjusted margins
    collateral_engine.graph_intelligence = graph_intelligence
    
    # Initialize asset-compute nexus
    asset_compute_nexus = AssetComputeNexus(
        digital_asset_url="http://digital-asset-service:8000",
        mlops_url="http://mlops-service:8000",
        ignite_cache=ignite,
        pulsar_publisher=pulsar
    )
    
    # Set asset-compute nexus on collateral engine for digital asset collateral
    collateral_engine.asset_compute_nexus = asset_compute_nexus
    
    # Start background tasks
    asyncio.create_task(matching_engine.start())
    asyncio.create_task(funding_engine.start_funding_calculation_loop())
    asyncio.create_task(settlement_engine.start_settlement_loop())
    asyncio.create_task(websocket_manager.start())
    asyncio.create_task(metrics.start_collection())
    asyncio.create_task(cross_service_coordinator.start())
    asyncio.create_task(synthetic_derivatives_engine.start_monitoring())
    
    # Initialize compute spot market monitoring
    await compute_spot_market.start_monitoring()
    
    # Initialize burst compute monitoring
    await burst_compute_engine.start_monitoring()
    
    # Initialize stablecoin engine monitoring
    await compute_stablecoin_engine.start_monitoring()
    
    logger.info("Derivatives Engine Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Derivatives Engine Service...")
    
    # Stop engines
    if matching_engine:
        await matching_engine.stop()
    if funding_engine:
        await funding_engine.stop()
    if settlement_engine:
        await settlement_engine.stop()
    if compute_futures_engine:
        await compute_futures_engine.stop()
    if partner_capacity_manager:
        await partner_capacity_manager.stop()
    if wholesale_arbitrage_engine:
        await wholesale_arbitrage_engine.stop()
    if cross_service_coordinator:
        await cross_service_coordinator.stop()
    if compute_spot_market:
        await compute_spot_market.stop()
    if burst_compute_engine:
        await burst_compute_engine.stop()
    if compute_stablecoin_engine:
        await compute_stablecoin_engine.stop()
    if synthetic_derivatives_engine:
        await synthetic_derivatives_engine.stop()
    if margin_engine:
        await margin_engine.stop()
    
    # Disconnect clients
    await pulsar.disconnect()
    await graph_client.disconnect()
    await oracle_client.disconnect()
    await neuromorphic_client.disconnect()
    await vc_client.disconnect()
    await asset_client.disconnect()
    
    logger.info("Derivatives Engine Service shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="Derivatives Engine Service", 
    version="2.0.0",
    description="Advanced derivatives trading engine with compute derivatives, burst compute, and synthetic derivatives",
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

# Include routers
app.include_router(markets.router, prefix="/api/v1/markets", tags=["markets"])
app.include_router(trading.router, prefix="/api/v1/trading", tags=["trading"])
app.include_router(positions.router, prefix="/api/v1/positions", tags=["positions"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
app.include_router(lending.router)  # Already has prefix in router definition
app.include_router(compute_futures.router)  # Compute futures trading
app.include_router(compute_spot.router)  # Compute spot market
app.include_router(burst_compute.router)  # Burst compute derivatives
app.include_router(compute_stablecoin.router)  # Compute-backed stablecoins
app.include_router(synthetic_derivatives.router)  # Synthetic derivatives
app.include_router(variance_swaps.router)  # Variance swaps trading
app.include_router(monitoring_dashboard.router)  # Monitoring dashboard
app.include_router(partner_capacity.router)  # Partner capacity management
app.include_router(capacity_coordinator.router)  # Cross-service capacity coordination
app.include_router(risk_intelligence.router)  # Risk intelligence with graph integration
app.include_router(asset_compute_nexus.router)  # Asset-compute-model nexus

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": "Derivatives Engine Service",
        "version": "2.0.0",
        "status": "operational",
        "features": [
            "Compute Futures Trading",
            "Compute Spot Market",
            "Burst Compute Derivatives",
            "Compute-backed Stablecoins",
            "Synthetic Derivatives",
            "Variance Swaps",
            "Partner Capacity Management",
            "Cross-service Capacity Coordination",
            "Wholesale Arbitrage",
            "Real-time Settlement",
            "Advanced Margin System",
            "Graph Intelligence Integration",
            "Digital Asset Collateral"
        ]
    }

# Health check endpoint
@app.get("/health")
async def health_check():
    try:
        # Check if engines are initialized
        engines_status = {
            "matching_engine": matching_engine is not None,
            "funding_engine": funding_engine is not None,
            "settlement_engine": settlement_engine is not None,
            "compute_futures_engine": compute_futures_engine is not None,
            "partner_capacity_manager": partner_capacity_manager is not None,
            "wholesale_arbitrage_engine": wholesale_arbitrage_engine is not None,
            "cross_service_coordinator": cross_service_coordinator is not None,
            "compute_spot_market": compute_spot_market is not None,
            "burst_compute_engine": burst_compute_engine is not None,
            "compute_stablecoin_engine": compute_stablecoin_engine is not None,
            "synthetic_derivatives_engine": synthetic_derivatives_engine is not None
        }
        
        all_healthy = all(engines_status.values())
        
        return {
            "status": "healthy" if all_healthy else "degraded",
            "engines": engines_status,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }

# WebSocket endpoint for real-time market data
@app.websocket("/ws/market/{market_id}")
async def websocket_market_data(websocket: WebSocket, market_id: str):
    """WebSocket endpoint for real-time market data"""
    await websocket.accept()
    
    if not websocket_manager:
        await websocket.close(code=1011, reason="Service not ready")
        return
    
    connection_id = None
    try:
        # Register connection
        connection_id = await websocket_manager.connect(websocket, market_id)
        
        # Keep connection alive
        while True:
            # Wait for messages from client (ping/pong or subscription updates)
            message = await websocket.receive_text()
            
            # Handle subscription updates
            if message.startswith("subscribe:"):
                data_type = message.split(":")[1]
                await websocket_manager.subscribe(connection_id, market_id, data_type)
            elif message.startswith("unsubscribe:"):
                data_type = message.split(":")[1]
                await websocket_manager.unsubscribe(connection_id, market_id, data_type)
            elif message == "ping":
                await websocket.send_text("pong")
                
    except WebSocketDisconnect:
        if connection_id:
            await websocket_manager.disconnect(connection_id)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        if connection_id:
            await websocket_manager.disconnect(connection_id)
        await websocket.close(code=1011, reason="Internal error")

# Performance monitoring endpoint
from app.analytics.performance_dashboard import create_performance_router
performance_router = create_performance_router(
    ignite_cache=None,  # Will be set during startup
    pulsar_publisher=None,  # Will be set during startup
    graph_intelligence=None  # Will be set during startup
)
app.include_router(performance_router)

# Add endpoint to update router dependencies after startup
@app.on_event("startup")
async def update_performance_router():
    """Update performance router with initialized dependencies"""
    if performance_router and matching_engine:
        # This would need proper implementation in the performance router
        pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 