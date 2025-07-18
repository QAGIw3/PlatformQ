"""
Blockchain Gateway Service

Unified blockchain abstraction layer providing multi-chain support,
transaction management, smart contract interaction, and gas optimization.
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pyignite import Client as IgniteClient
import pulsar

from platformq_blockchain_common import ChainType, ChainConfig
from app.core import BlockchainGateway
from app.api.endpoints import router

logger = logging.getLogger(__name__)


def load_chain_configs():
    """Load blockchain configurations from environment"""
    configs = []
    
    # Ethereum configuration
    if os.getenv("ETHEREUM_RPC_URL"):
        configs.append(ChainConfig(
            chain_id=1,
            chain_type=ChainType.ETHEREUM,
            name="Ethereum Mainnet",
            rpc_url=os.getenv("ETHEREUM_RPC_URL"),
            ws_url=os.getenv("ETHEREUM_WS_URL"),
            explorer_url="https://etherscan.io",
            native_currency="ETH",
            contracts={
                "multicall": os.getenv("ETHEREUM_MULTICALL_ADDRESS", "0x5BA1e12693Dc8F9c48aAD8770482f4739bEeD696")
            }
        ))
    
    # Polygon configuration
    if os.getenv("POLYGON_RPC_URL"):
        configs.append(ChainConfig(
            chain_id=137,
            chain_type=ChainType.POLYGON,
            name="Polygon",
            rpc_url=os.getenv("POLYGON_RPC_URL"),
            ws_url=os.getenv("POLYGON_WS_URL"),
            explorer_url="https://polygonscan.com",
            native_currency="MATIC",
            contracts={
                "multicall": os.getenv("POLYGON_MULTICALL_ADDRESS", "0x11ce4B23bD875D7F5C6a31084f55fDe1e9A87507")
            }
        ))
    
    # Arbitrum configuration
    if os.getenv("ARBITRUM_RPC_URL"):
        configs.append(ChainConfig(
            chain_id=42161,
            chain_type=ChainType.ARBITRUM,
            name="Arbitrum One",
            rpc_url=os.getenv("ARBITRUM_RPC_URL"),
            ws_url=os.getenv("ARBITRUM_WS_URL"),
            explorer_url="https://arbiscan.io",
            native_currency="ETH"
        ))
    
    return configs


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    # Startup
    logger.info("Starting Blockchain Gateway Service...")
    
    # Initialize Ignite client
    ignite_client = IgniteClient()
    ignite_client.connect([os.getenv("IGNITE_HOST", "localhost:10800")])
    
    # Initialize Pulsar client
    pulsar_client = pulsar.Client(
        os.getenv("PULSAR_URL", "pulsar://localhost:6650")
    )
    
    # Create blockchain gateway
    gateway = BlockchainGateway(ignite_client, pulsar_client)
    await gateway.initialize()
    
    # Register blockchain configurations
    configs = load_chain_configs()
    for config in configs:
        gateway.register_chain(config)
        logger.info(f"Registered {config.name}")
    
    # Store in app state
    app.state.gateway = gateway
    app.state.ignite_client = ignite_client
    app.state.pulsar_client = pulsar_client
    
    logger.info("Blockchain Gateway Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Blockchain Gateway Service...")
    
    # Cleanup resources
    await gateway.shutdown()
    ignite_client.close()
    pulsar_client.close()
    
    logger.info("Blockchain Gateway Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Blockchain Gateway Service",
    description="Unified blockchain abstraction layer for PlatformQ",
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

# Include API router
app.include_router(router, prefix="/api/v1")

# Root endpoint
@app.get("/")
async def root():
    """Service information endpoint"""
    gateway = getattr(app.state, "gateway", None)
    
    return {
        "service": "blockchain-gateway-service",
        "version": "2.0.0",
        "status": "operational",
        "description": "Unified blockchain abstraction layer",
        "features": [
            "multi-chain-support",
            "connection-pooling",
            "transaction-management",
            "gas-optimization",
            "smart-contract-deployment",
            "event-streaming"
        ],
        "supported_chains": gateway.get_supported_chains() if gateway else []
    } 