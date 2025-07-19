"""
Unified Governance Service

Comprehensive governance system for PlatformQ including DAO governance, proposals management,
multi-chain support, voting strategies, reputation management, and proposal execution.
"""

import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pyignite import Client as IgniteClient
import pulsar
import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core import GovernanceManager
from app.api.endpoints import router as governance_router
from app.api.proposal_endpoints import router as proposals_router
from app.proposals.messaging.pulsar_consumer import start_consumer, stop_consumer
from platformq.shared.nextcloud_client import NextcloudClient

logger = logging.getLogger(__name__)

# Database setup for proposals
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:password@localhost/governance"
)
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting Unified Governance Service...")
    
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
    
    # Initialize Nextcloud client for proposals
    nextcloud_client = NextcloudClient(
        nextcloud_url=os.getenv("NEXTCLOUD_URL", "http://nextcloud:80"),
        admin_user=os.getenv("NEXTCLOUD_USER", "admin"),
        admin_pass=os.getenv("NEXTCLOUD_PASSWORD", "admin")
    )
    
    # Create governance manager
    governance_manager = GovernanceManager(
        ignite_client=ignite_client,
        pulsar_client=pulsar_client,
        blockchain_gateway=blockchain_gateway,
        db_session=SessionLocal
    )
    
    await governance_manager.initialize()
    
    # Start Pulsar consumer for proposals
    start_consumer()
    
    # Store in app state
    app.state.governance_manager = governance_manager
    app.state.ignite_client = ignite_client
    app.state.pulsar_client = pulsar_client
    app.state.blockchain_gateway = blockchain_gateway
    app.state.nextcloud_client = nextcloud_client
    app.state.db_session = SessionLocal
    
    logger.info("Unified Governance Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Unified Governance Service...")
    
    # Stop Pulsar consumer
    stop_consumer()
    
    await governance_manager.shutdown()
    await blockchain_gateway.aclose()
    ignite_client.close()
    pulsar_client.close()
    
    logger.info("Unified Governance Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Unified Governance Service",
    description="Comprehensive governance system for PlatformQ including DAO governance and proposals management",
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
app.include_router(governance_router, prefix="/api/v1", tags=["governance"])
app.include_router(proposals_router, prefix="/api/v1", tags=["proposals"])

# Root endpoint
@app.get("/")
async def root():
    """Service information endpoint"""
    governance_manager = getattr(app.state, "governance_manager", None)
    
    return {
        "service": "governance-service",
        "version": "3.0.0",
        "status": "operational",
        "description": "Unified governance system with DAO and proposals management",
        "features": [
            "dao-governance",
            "proposal-management",
            "multi-chain-proposals",
            "voting-strategies",
            "reputation-management",
            "proposal-execution",
            "cross-chain-governance",
            "document-management",
            "nextcloud-integration"
        ],
        "voting_strategies": governance_manager.get_available_voting_strategies() if governance_manager else []
    } 