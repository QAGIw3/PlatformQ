"""
DAO Governance Service

Decentralized governance system for PlatformQ with multi-chain support,
various voting strategies, reputation management, and proposal execution.
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pyignite import Client as IgniteClient
import pulsar
import httpx

from app.core import GovernanceManager
from app.api.endpoints import router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting DAO Governance Service...")
    
    # Initialize Ignite client
    ignite_client = IgniteClient()
    ignite_client.connect([os.getenv("IGNITE_HOST", "localhost:10800")])
    
    # Initialize Pulsar client
    pulsar_client = pulsar.Client(
        os.getenv("PULSAR_URL", "pulsar://localhost:6650")
    )
    
    # Create HTTP client for blockchain gateway
    blockchain_gateway_url = os.getenv(
        "BLOCKCHAIN_GATEWAY_URL",
        "http://blockchain-gateway-service:8000"
    )
    blockchain_gateway = httpx.AsyncClient(
        base_url=blockchain_gateway_url,
        timeout=30.0
    )
    
    # Create governance manager
    governance_manager = GovernanceManager(
        ignite_client=ignite_client,
        pulsar_client=pulsar_client,
        blockchain_gateway=blockchain_gateway
    )
    
    await governance_manager.initialize()
    
    # Store in app state
    app.state.governance_manager = governance_manager
    app.state.ignite_client = ignite_client
    app.state.pulsar_client = pulsar_client
    app.state.blockchain_gateway = blockchain_gateway
    
    logger.info("DAO Governance Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down DAO Governance Service...")
    
    await governance_manager.shutdown()
    await blockchain_gateway.aclose()
    ignite_client.close()
    pulsar_client.close()
    
    logger.info("DAO Governance Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="DAO Governance Service",
    description="Decentralized governance for PlatformQ",
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
    return {
        "service": "dao-governance-service",
        "version": "2.0.0",
        "status": "operational",
        "description": "Decentralized governance system",
        "features": [
            "multi-chain-proposals",
            "voting-strategies",
            "reputation-management",
            "proposal-execution",
            "cross-chain-governance"
        ]
    } 