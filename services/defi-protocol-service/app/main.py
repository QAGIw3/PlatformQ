"""
DeFi Protocol Service

Provides decentralized finance functionality including lending, borrowing,
yield farming, liquidity pools, and auction mechanisms.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional
import asyncio

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Counter, Histogram, Gauge
import time

from platformq_shared import (
    create_base_app,
    ConfigLoader,
    ErrorCode,
    AppException,
    get_current_user
)
from platformq_blockchain_common import (
    ConnectionPool,
    AdapterFactory,
    ChainType,
    ChainConfig
)

from .core.config import settings
from .core.defi_manager import DeFiManager
from .protocols.lending import LendingProtocol
from .protocols.auctions import AuctionProtocol
from .protocols.yield_farming import YieldFarmingProtocol
from .protocols.liquidity import LiquidityProtocol
from .services.risk_calculator import RiskCalculator
from .services.price_oracle import PriceOracle
from .api import lending, auctions, yield_farming, liquidity, analytics

logger = logging.getLogger(__name__)

# Metrics
DEFI_TRANSACTIONS = Counter(
    'defi_transactions_total', 
    'Total DeFi transactions',
    ['chain', 'protocol', 'operation']
)
TRANSACTION_LATENCY = Histogram(
    'defi_transaction_latency_seconds',
    'DeFi transaction latency',
    ['chain', 'protocol']
)
TVL_GAUGE = Gauge(
    'defi_tvl_usd',
    'Total Value Locked in USD',
    ['chain', 'protocol']
)
APY_GAUGE = Gauge(
    'defi_apy_percent',
    'Annual Percentage Yield',
    ['chain', 'pool']
)

# Global instances
connection_pool: Optional[ConnectionPool] = None
defi_manager: Optional[DeFiManager] = None
lending_protocol: Optional[LendingProtocol] = None
auction_protocol: Optional[AuctionProtocol] = None
yield_farming_protocol: Optional[YieldFarmingProtocol] = None
liquidity_protocol: Optional[LiquidityProtocol] = None
risk_calculator: Optional[RiskCalculator] = None
price_oracle: Optional[PriceOracle] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global connection_pool, defi_manager, lending_protocol
    global auction_protocol, yield_farming_protocol, liquidity_protocol
    global risk_calculator, price_oracle
    
    # Startup
    logger.info("Starting DeFi Protocol Service...")
    
    # Initialize configuration
    config_loader = ConfigLoader()
    config = config_loader.load_settings()
    
    # Initialize connection pool
    connection_pool = ConnectionPool(
        max_connections_per_chain=int(config.get("max_connections_per_chain", 5))
    )
    await connection_pool.initialize()
    
    # Register supported chains
    for chain_config in settings.SUPPORTED_CHAINS:
        chain_cfg = ChainConfig(
            chain_type=ChainType(chain_config["type"]),
            rpc_url=chain_config["rpc_url"],
            chain_id=chain_config.get("chain_id"),
            extras=chain_config.get("extras", {})
        )
        connection_pool.register_chain(chain_cfg)
    
    app.state.connection_pool = connection_pool
    
    # Initialize price oracle
    price_oracle = PriceOracle(
        providers=config.get("price_providers", ["coingecko", "chainlink"]),
        cache_ttl=int(config.get("price_cache_ttl", 300))
    )
    await price_oracle.initialize()
    app.state.price_oracle = price_oracle
    
    # Initialize risk calculator
    risk_calculator = RiskCalculator(
        price_oracle=price_oracle,
        volatility_window=int(config.get("volatility_window", 30))
    )
    app.state.risk_calculator = risk_calculator
    
    # Initialize DeFi manager
    defi_manager = DeFiManager(
        connection_pool=connection_pool,
        price_oracle=price_oracle,
        risk_calculator=risk_calculator
    )
    await defi_manager.initialize()
    app.state.defi_manager = defi_manager
    
    # Initialize protocols
    lending_protocol = LendingProtocol(defi_manager)
    await lending_protocol.initialize()
    app.state.lending_protocol = lending_protocol
    
    auction_protocol = AuctionProtocol(defi_manager)
    await auction_protocol.initialize()
    app.state.auction_protocol = auction_protocol
    
    yield_farming_protocol = YieldFarmingProtocol(defi_manager)
    await yield_farming_protocol.initialize()
    app.state.yield_farming_protocol = yield_farming_protocol
    
    liquidity_protocol = LiquidityProtocol(defi_manager)
    await liquidity_protocol.initialize()
    app.state.liquidity_protocol = liquidity_protocol
    
    # Start background tasks
    asyncio.create_task(defi_manager.monitor_positions())
    asyncio.create_task(price_oracle.update_prices_loop())
    asyncio.create_task(_update_metrics_loop())
    
    logger.info("DeFi Protocol Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down DeFi Protocol Service...")
    
    # Stop protocols
    if lending_protocol:
        await lending_protocol.shutdown()
    if auction_protocol:
        await auction_protocol.shutdown()
    if yield_farming_protocol:
        await yield_farming_protocol.shutdown()
    if liquidity_protocol:
        await liquidity_protocol.shutdown()
        
    # Stop core services
    if defi_manager:
        await defi_manager.shutdown()
    if price_oracle:
        await price_oracle.shutdown()
    if connection_pool:
        await connection_pool.close()
    
    logger.info("DeFi Protocol Service shutdown complete")


async def _update_metrics_loop():
    """Update DeFi metrics periodically"""
    while True:
        try:
            # Update TVL metrics
            tvl_data = await defi_manager.get_total_value_locked()
            for chain, protocols in tvl_data.items():
                for protocol, value in protocols.items():
                    TVL_GAUGE.labels(chain=chain, protocol=protocol).set(value)
            
            # Update APY metrics
            apy_data = await yield_farming_protocol.get_all_pool_apys()
            for pool_id, apy in apy_data.items():
                APY_GAUGE.labels(
                    chain=apy["chain"],
                    pool=pool_id
                ).set(apy["value"])
                
            await asyncio.sleep(60)  # Update every minute
            
        except Exception as e:
            logger.error(f"Error updating metrics: {e}")
            await asyncio.sleep(60)


# Create app
app = create_base_app(
    service_name="defi-protocol-service",
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
app.include_router(lending.router, prefix="/api/v1/lending", tags=["lending"])
app.include_router(auctions.router, prefix="/api/v1/auctions", tags=["auctions"])
app.include_router(yield_farming.router, prefix="/api/v1/yield-farming", tags=["yield-farming"])
app.include_router(liquidity.router, prefix="/api/v1/liquidity", tags=["liquidity"])
app.include_router(analytics.router, prefix="/api/v1/analytics", tags=["analytics"])

# Root endpoint
@app.get("/")
async def root():
    return {
        "service": "defi-protocol-service",
        "version": "1.0.0",
        "status": "operational",
        "features": [
            "lending-borrowing",
            "nft-auctions",
            "yield-farming",
            "liquidity-pools",
            "flash-loans",
            "price-oracles",
            "risk-management",
            "cross-chain-defi"
        ]
    }

# Health check endpoint
@app.get("/health")
async def health_check():
    try:
        # Check connection pool
        pool_stats = connection_pool.get_pool_stats()
        
        # Check price oracle
        oracle_status = await price_oracle.health_check()
        
        return {
            "status": "healthy",
            "connection_pool": pool_stats,
            "price_oracle": oracle_status,
            "timestamp": time.time()
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(status_code=503, detail="Service unhealthy") 