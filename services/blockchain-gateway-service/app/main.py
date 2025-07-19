"""
Blockchain Gateway Service

Unified blockchain abstraction layer providing multi-chain support,
transaction management, smart contract interaction, gas optimization,
oracle aggregation, and cross-chain bridge functionality.
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pyignite import Client as IgniteClient
import pulsar
import httpx

from platformq_blockchain_common import ChainType, ChainConfig
from app.core import BlockchainGateway
from app.core.chain_manager import CrossChainManager
from app.core.gas_optimization_enhanced import GasOptimizationService
from app.core.bridges.cross_chain_bridge import CrossChainBridge
from app.oracle.price_aggregator import PriceAggregator
from app.compliance.kyc_aml_service import KYCAMLService
from app.execution.executor import ProposalExecutor
from app.api.endpoints import router
from app.api.credentials import router as credentials_router

logger = logging.getLogger(__name__)

# Global service instances
chain_manager: Optional[CrossChainManager] = None
gas_optimizer: Optional[GasOptimizationService] = None
cross_chain_bridge: Optional[CrossChainBridge] = None
price_aggregator: Optional[PriceAggregator] = None
kyc_aml_service: Optional[KYCAMLService] = None
proposal_executor: Optional[ProposalExecutor] = None


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
    
    # Solana configuration
    if os.getenv("SOLANA_RPC_URL"):
        configs.append(ChainConfig(
            chain_id=0,  # Solana doesn't use chain IDs
            chain_type=ChainType.SOLANA,
            name="Solana Mainnet",
            rpc_url=os.getenv("SOLANA_RPC_URL"),
            ws_url=os.getenv("SOLANA_WS_URL"),
            explorer_url="https://explorer.solana.com",
            native_currency="SOL"
        ))
    
    # Cosmos configuration
    if os.getenv("COSMOS_RPC_URL"):
        configs.append(ChainConfig(
            chain_id=0,  # Cosmos uses string IDs
            chain_type=ChainType.COSMOS,
            name="Cosmos Hub",
            rpc_url=os.getenv("COSMOS_RPC_URL"),
            ws_url=os.getenv("COSMOS_WS_URL"),
            explorer_url="https://www.mintscan.io/cosmos",
            native_currency="ATOM"
        ))
    
    return configs


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global chain_manager, gas_optimizer, cross_chain_bridge, price_aggregator
    global kyc_aml_service, proposal_executor
    
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
    
    # Initialize service components
    chain_manager = CrossChainManager(ignite_client, pulsar_client)
    gas_optimizer = GasOptimizationService(ignite_client)
    cross_chain_bridge = CrossChainBridge(chain_manager, gas_optimizer)
    price_aggregator = PriceAggregator(
        cache_client=ignite_client,
        pulsar_client=pulsar_client
    )
    kyc_aml_service = KYCAMLService(ignite_client)
    proposal_executor = ProposalExecutor(
        chain_manager=chain_manager,
        gas_optimizer=gas_optimizer
    )
    
    # Initialize all services
    await chain_manager.initialize()
    await price_aggregator.initialize()
    await cross_chain_bridge.initialize()
    
    # Store in app state
    app.state.gateway = gateway
    app.state.chain_manager = chain_manager
    app.state.gas_optimizer = gas_optimizer
    app.state.cross_chain_bridge = cross_chain_bridge
    app.state.price_aggregator = price_aggregator
    app.state.kyc_aml_service = kyc_aml_service
    app.state.proposal_executor = proposal_executor
    app.state.ignite_client = ignite_client
    app.state.pulsar_client = pulsar_client
    
    logger.info("Blockchain Gateway Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Blockchain Gateway Service...")
    
    # Cleanup resources
    await gateway.shutdown()
    await chain_manager.shutdown()
    await price_aggregator.shutdown()
    await cross_chain_bridge.shutdown()
    
    ignite_client.close()
    pulsar_client.close()
    
    logger.info("Blockchain Gateway Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Blockchain Gateway Service",
    description="Unified blockchain abstraction layer for PlatformQ with oracle aggregation and cross-chain capabilities",
    version="3.0.0",
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
app.include_router(router, prefix="/api/v1")
app.include_router(credentials_router, prefix="/api/v1")

# Root endpoint
@app.get("/")
async def root():
    """Service information endpoint"""
    gateway = getattr(app.state, "gateway", None)
    chain_manager = getattr(app.state, "chain_manager", None)
    
    return {
        "service": "blockchain-gateway-service",
        "version": "3.0.0",
        "status": "operational",
        "description": "Unified blockchain abstraction layer with oracle aggregation and cross-chain capabilities",
        "features": [
            "multi-chain-support",
            "connection-pooling",
            "transaction-management",
            "gas-optimization",
            "smart-contract-deployment",
            "event-streaming",
            "oracle-aggregation",
            "cross-chain-bridge",
            "kyc-aml-compliance",
            "proposal-execution"
        ],
        "supported_chains": gateway.get_supported_chains() if gateway else [],
        "active_chains": chain_manager.get_active_chains() if chain_manager else []
    } 