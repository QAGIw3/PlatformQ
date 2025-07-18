from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio
from typing import Dict, List, Optional
from decimal import Decimal
import logging

from app.api import markets, trading, positions, analytics, compliant_pools, risk, lending, liquidity, options, market_makers, compute_futures, variance_swaps, structured_products, risk_limits, monitoring_dashboard
from app.engines.matching_engine import MatchingEngine
from app.engines.funding_engine import FundingEngine
from app.engines.settlement_engine import SettlementEngine
from app.engines.compute_futures_engine import ComputeFuturesEngine
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
    IgniteCache,
    SeaTunnelClient
)
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
collateral_engine: Optional[MultiTierCollateralEngine] = None
liquidation_engine: Optional[PartialLiquidationEngine] = None
fee_engine: Optional[DynamicFeeEngine] = None
market_dao: Optional[MarketCreationDAO] = None
websocket_manager: Optional[MarketDataWebSocket] = None
metrics: Optional[PrometheusMetrics] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifecycle - startup and shutdown
    """
    # Startup
    logger.info("Starting Derivatives Engine Service...")
    
    # Initialize clients
    ignite = IgniteCache()
    await ignite.connect()
    
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
    global collateral_engine, liquidation_engine, fee_engine
    global market_dao, websocket_manager, metrics
    
    # Collateral and risk engines
    collateral_engine = MultiTierCollateralEngine(ignite, graph_client, oracle_client)
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
    
    # Compute futures engine
    compute_futures_engine = ComputeFuturesEngine(
        ignite,
        pulsar,
        oracle_client
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
    
    # Start background tasks
    asyncio.create_task(matching_engine.start())
    asyncio.create_task(funding_engine.start_funding_calculation_loop())
    asyncio.create_task(liquidation_engine.start_monitoring_loop())
    asyncio.create_task(websocket_manager.start_broadcasting())
    asyncio.create_task(compute_futures_engine.start())
    
    # Setup data pipelines with SeaTunnel
    await setup_data_pipelines(seatunnel_client)
    
    logger.info("Derivatives Engine Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Derivatives Engine Service...")
    
    # Stop background tasks
    await matching_engine.stop()
    await funding_engine.stop()
    
    # Close connections
    await ignite.close()
    await pulsar.close()
    
    logger.info("Derivatives Engine Service shut down successfully")

async def setup_data_pipelines(seatunnel_client: SeaTunnelClient):
    """
    Setup SeaTunnel data pipelines for derivatives data
    """
    # Price data pipeline
    price_pipeline = {
        "name": "derivative_price_pipeline",
        "source": {
            "type": "pulsar",
            "topic": "price-updates",
            "subscription": "derivatives-price-consumer"
        },
        "transform": [
            {
                "type": "sql",
                "query": """
                    SELECT 
                        market_id,
                        price,
                        source_prices,
                        timestamp,
                        TUMBLE_START(timestamp, INTERVAL '1' MINUTE) as window_start
                    FROM source
                    GROUP BY TUMBLE(timestamp, INTERVAL '1' MINUTE), market_id
                """
            }
        ],
        "sink": [
            {
                "type": "cassandra",
                "table": "derivative_prices",
                "keyspace": "platformq"
            },
            {
                "type": "elasticsearch",
                "index": "derivative-prices"
            }
        ]
    }
    
    await seatunnel_client.create_job(price_pipeline)
    
    # Trade data pipeline
    trade_pipeline = {
        "name": "derivative_trade_pipeline",
        "source": {
            "type": "pulsar",
            "topic": "trade-events",
            "subscription": "derivatives-trade-consumer"
        },
        "transform": [
            {
                "type": "aggregate",
                "window": "1m",
                "aggregations": ["count", "sum", "avg", "min", "max"]
            }
        ],
        "sink": [
            {
                "type": "ignite",
                "cache": "trade_analytics"
            },
            {
                "type": "influxdb",
                "bucket": "derivatives_metrics"
            }
        ]
    }
    
    await seatunnel_client.create_job(trade_pipeline)

# Create FastAPI app
app = FastAPI(
    title="PlatformQ Derivatives Engine",
    description="Universal synthetic derivatives trading platform",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(markets.router, prefix="/api/v1/markets", tags=["markets"])
app.include_router(trading.router, prefix="/api/v1/trading", tags=["trading"])
app.include_router(positions.router, prefix="/api/v1/positions", tags=["positions"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])
app.include_router(compliant_pools.router)  # Already has prefix in router definition
app.include_router(risk.router)  # Already has prefix in router definition
app.include_router(lending.router)  # Already has prefix in router definition
app.include_router(liquidity.router)  # Already has prefix in router definition
app.include_router(options.router)  # Already has prefix in router definition
app.include_router(market_makers.router)  # Already has prefix in router definition
app.include_router(compute_futures.router)  # Compute futures trading
app.include_router(variance_swaps.router)  # Variance swaps trading
app.include_router(structured_products.router)  # Structured products
app.include_router(risk_limits.router)  # Risk limits management
app.include_router(monitoring_dashboard.router)  # Monitoring dashboard

# Health check
@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {
        "status": "healthy",
        "service": "derivatives-engine",
        "version": "1.0.0",
        "engines": {
            "matching": matching_engine is not None,
            "funding": funding_engine is not None,
            "settlement": settlement_engine is not None,
            "liquidation": liquidation_engine is not None
        }
    }

# WebSocket endpoint for real-time market data
@app.websocket("/ws/market-data/{market_id}")
async def websocket_market_data(websocket: WebSocket, market_id: str):
    """
    WebSocket endpoint for real-time market data streaming
    """
    await websocket_manager.connect(websocket, market_id)
    try:
        while True:
            # Keep connection alive and handle incoming messages
            data = await websocket.receive_text()
            # Handle subscription changes, etc.
            await websocket_manager.handle_message(websocket, data)
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket, market_id)

# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """
    Prometheus metrics endpoint
    """
    return metrics.generate_metrics()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 