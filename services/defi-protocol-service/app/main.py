"""
DeFi Protocol Service

Provides decentralized finance functionality including lending, borrowing,
yield farming, liquidity pools, and auction mechanisms.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
import asyncio
import os
from decimal import Decimal
from datetime import datetime
import json

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
from .protocols.insurance import InsuranceProtocol
from .services.risk_calculator import RiskCalculator
from .services.price_oracle import PriceOracle
from .api import lending, auctions, yield_farming, liquidity, analytics, insurance, infrastructure, infrastructure_amm, infrastructure_risk, infrastructure_insurance, staking, vaults, derivatives

# Import Vault/Consul integration
from .vault_consul_integration import VaultConsulIntegration

logger = logging.getLogger(__name__)

# Metrics
DEFI_TRANSACTIONS = Counter(
    'defi_transactions_total', 
    'Total DeFi transactions',
    ['chain', 'protocol', 'operation']
)
TRANSACTION_LATENCY = Histogram(
    'defi_transaction_duration_seconds',
    'DeFi transaction duration',
    ['chain', 'protocol', 'operation']
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
price_oracle: Optional[PriceOracle] = None
risk_calculator: Optional[RiskCalculator] = None
vault_consul: Optional[VaultConsulIntegration] = None

# Protocol instances with secure key management
lending_protocols: Dict[str, LendingProtocol] = {}
auction_protocols: Dict[str, AuctionProtocol] = {}
yield_protocols: Dict[str, YieldFarmingProtocol] = {}
liquidity_protocols: Dict[str, LiquidityProtocol] = {}
insurance_protocol: Optional[InsuranceProtocol] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan with Vault/Consul integration"""
    global connection_pool, defi_manager, price_oracle, risk_calculator, vault_consul
    global lending_protocols, auction_protocols, yield_protocols, liquidity_protocols, insurance_protocol
    
    logger.info("Starting DeFi Protocol Service...")
    
    # Initialize Vault/Consul integration
    vault_consul = VaultConsulIntegration({
        "vault_addr": os.getenv("VAULT_ADDR", "http://vault:8200"),
        "vault_token": os.getenv("VAULT_TOKEN"),
        "consul_addr": os.getenv("CONSUL_ADDR", "http://consul:8500")
    })
    
    await vault_consul.initialize()
    
    # Register service with Consul
    await vault_consul.register_service(
        tags=["defi", "lending", "amm", "yield-farming"],
        meta={
            "version": "1.0.0",
            "supported_chains": "ethereum,polygon,arbitrum,optimism"
        }
    )
    
    # Initialize blockchain connection pool with secure keys
    connection_pool = ConnectionPool()
    
    # Add chain configurations with deployment keys
    for chain in ["ethereum", "polygon", "arbitrum", "optimism"]:
        deployment_key = await vault_consul.get_contract_deployment_key(chain)
        
        config = ChainConfig(
            chain_type=ChainType[chain.upper()],
            rpc_url=settings.get_chain_rpc(chain),
            private_key=deployment_key["private_key"],
            contract_addresses=settings.get_chain_contracts(chain)
        )
        await connection_pool.add_chain(config)
    
    # Initialize price oracle
    price_oracle = PriceOracle(
        providers=settings.PRICE_PROVIDERS,
        cache_ttl=settings.PRICE_CACHE_TTL
    )
    await price_oracle.initialize()
    
    # Initialize risk calculator
    risk_calculator = RiskCalculator(
        price_oracle=price_oracle,
        volatility_window=settings.VOLATILITY_WINDOW
    )
    
    # Initialize DeFi manager
    defi_manager = DeFiManager(
        connection_pool=connection_pool,
        price_oracle=price_oracle,
        risk_calculator=risk_calculator
    )
    
    # Initialize protocols with secure configuration
    protocol_params = await vault_consul.get_protocol_parameters()
    
    # Initialize lending protocols
    for chain in ["ethereum", "polygon", "arbitrum", "optimism"]:
        lending_protocols[chain] = LendingProtocol(
            defi_manager=defi_manager
        )
        await lending_protocols[chain].initialize()
    
    # Initialize auction protocols
    for chain in ["ethereum", "polygon", "arbitrum", "optimism"]:
        auction_protocols[chain] = AuctionProtocol(
            defi_manager=defi_manager
        )
        await auction_protocols[chain].initialize()
    
    # Initialize yield farming protocols
    for chain in ["ethereum", "polygon", "arbitrum", "optimism"]:
        yield_protocols[chain] = YieldFarmingProtocol(
            defi_manager=defi_manager
        )
        await yield_protocols[chain].initialize()
    
    # Initialize liquidity protocols
    for chain in ["ethereum", "polygon", "arbitrum", "optimism"]:
        liquidity_protocols[chain] = LiquidityProtocol(
            defi_manager=defi_manager
        )
        await liquidity_protocols[chain].initialize()
    
    # Initialize insurance protocol (single instance across all chains)
    insurance_protocol = InsuranceProtocol(
        defi_manager=defi_manager,
        lending_protocol=None,  # Will be set after initialization
        yield_protocol=None     # Will be set after initialization
    )
    await insurance_protocol.initialize()
    
    # Now link insurance with lending and yield protocols
    insurance_protocol.lending_protocol = lending_protocols
    insurance_protocol.yield_protocol = yield_protocols
    
    # Update lending protocols to use insurance
    for chain, lending in lending_protocols.items():
        lending.insurance_protocol = insurance_protocol
    
    # Update risk calculator to use insurance
    risk_calculator.insurance_protocol = insurance_protocol
    
    # Store insurance protocol in app state for API access
    app.state.insurance_protocol = insurance_protocol
    
    # Start background tasks
    asyncio.create_task(monitor_protocol_health())
    asyncio.create_task(update_oracle_prices())
    asyncio.create_task(monitor_governance_proposals())
    
    logger.info("DeFi Protocol Service started successfully")
    
    yield
    
    # Cleanup
    logger.info("Shutting down DeFi Protocol Service...")
    
    # Shutdown protocols
    if insurance_protocol:
        await insurance_protocol.shutdown()
    
    for protocol in lending_protocols.values():
        await protocol.shutdown()
    
    for protocol in auction_protocols.values():
        await protocol.shutdown()
    
    for protocol in yield_protocols.values():
        await protocol.shutdown()
    
    for protocol in liquidity_protocols.values():
        await protocol.shutdown()
    
    # Shutdown core components
    if price_oracle:
        await price_oracle.shutdown()
    
    if defi_manager:
        await defi_manager.shutdown()
    
    if connection_pool:
        await connection_pool.close()
    
    if vault_consul:
        await vault_consul.deregister_service()
        await vault_consul.shutdown()
    
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
app.include_router(insurance.router, prefix="/api/v1/insurance", tags=["insurance"])
app.include_router(infrastructure.router, prefix="/api/v1/infrastructure", tags=["infrastructure"])
app.include_router(infrastructure_amm.router, prefix="/api/v1/infrastructure/amm", tags=["infrastructure-amm"])
app.include_router(infrastructure_risk.router, prefix="/api/v1/infrastructure/risk", tags=["infrastructure-risk"])
app.include_router(infrastructure_insurance.router, prefix="/api/v1/infrastructure/insurance", tags=["infrastructure-insurance"])
app.include_router(staking.router, prefix="/api/v1", tags=["staking"])
app.include_router(vaults.router, prefix="/api/v1", tags=["vaults"])
app.include_router(derivatives.router, prefix="/api/v1", tags=["derivatives"])

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
            "insurance-pools",
            "flash-loans",
            "price-oracles",
            "risk-management",
            "cross-chain-defi",
            "staking-delegation",
            "infrastructure-vaults",
            "options-perpetuals",
            "derivatives-amm"
        ]
    }

# Oracle and Governance Endpoints

from pydantic import BaseModel

class OracleDataRequest(BaseModel):
    data_type: str  # price, volume, liquidity
    asset_pair: str
    value: str
    timestamp: Optional[int] = None

@app.post("/api/oracle/sign")
async def sign_oracle_data(request: OracleDataRequest):
    """Sign oracle data for on-chain verification"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        oracle_data = {
            "pair": request.asset_pair,
            "value": request.value,
            "decimals": 18,
            "timestamp": request.timestamp or int(datetime.utcnow().timestamp())
        }
        
        signed_data = await vault_consul.sign_oracle_data(
            oracle_data,
            request.data_type
        )
        
        return signed_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/oracle/verify")
async def verify_oracle_signature(signed_data: Dict[str, Any]):
    """Verify oracle signature"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        valid = await vault_consul.verify_oracle_signature(signed_data)
        return {"valid": valid}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class GovernanceProposal(BaseModel):
    proposal_type: str  # parameter_change, upgrade, emergency
    title: str
    description: str
    changes: Dict[str, Any]
    execution_delay: int = 172800  # 48 hours

@app.post("/api/governance/propose")
async def create_governance_proposal(proposal: GovernanceProposal):
    """Create and sign governance proposal"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        signed_proposal = await vault_consul.sign_governance_proposal(
            proposal.dict()
        )
        
        return signed_proposal
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Cross-chain Bridge Endpoints

class BridgeTransfer(BaseModel):
    source_chain: str
    target_chain: str
    token_address: str
    amount: str
    recipient: str
    user_address: str

@app.post("/api/bridge/initiate")
async def initiate_bridge_transfer(transfer: BridgeTransfer):
    """Initiate cross-chain bridge transfer"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        # Validate transaction limits
        validation = await vault_consul.validate_transaction_limits(
            "bridge",
            Decimal(transfer.amount),
            transfer.user_address
        )
        
        if not validation["valid"]:
            raise HTTPException(status_code=400, detail=validation["reason"])
        
        # Get bridge validator key
        validator_key = await vault_consul.get_bridge_validator_key(
            f"{transfer.source_chain}-{transfer.target_chain}",
            transfer.source_chain
        )
        
        # Sign bridge message
        bridge_message = {
            "token": transfer.token_address,
            "amount": transfer.amount,
            "recipient": transfer.recipient,
            "user": transfer.user_address
        }
        
        signed_message = await vault_consul.sign_cross_chain_message(
            bridge_message,
            transfer.source_chain,
            transfer.target_chain
        )
        
        # Initiate transfer on source chain
        # ... blockchain interaction code ...
        
        return {
            "transfer_id": signed_message["message_hash"],
            "status": "initiated",
            "signed_message": signed_message
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Protocol Configuration Endpoints

@app.get("/api/protocol/parameters")
async def get_protocol_parameters():
    """Get current protocol parameters"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        params = await vault_consul.get_protocol_parameters()
        return params
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/protocol/flash-loan-config")
async def get_flash_loan_config():
    """Get flash loan protection configuration"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        config = await vault_consul.get_flash_loan_protection_config()
        return config
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Yield Strategy Management

class YieldStrategy(BaseModel):
    strategy_id: str
    protocol: str
    assets: List[str]
    weights: List[float]
    expected_apy: str
    risk_level: str

@app.post("/api/strategies/create")
async def create_yield_strategy(strategy: YieldStrategy):
    """Create new yield farming strategy"""
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    try:
        await vault_consul.store_yield_strategy(
            strategy.strategy_id,
            strategy.dict()
        )
        
        return {
            "strategy_id": strategy.strategy_id,
            "status": "created"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Background Tasks

async def monitor_protocol_health():
    """Monitor protocol health and TVL"""
    while True:
        try:
            if defi_manager and vault_consul:
                for chain in connection_pool.chains:
                    # Get TVL for each protocol
                    lending_tvl = await lending_protocols[chain].get_tvl()
                    liquidity_tvl = await liquidity_protocols[chain].get_tvl()
                    
                    # Update metrics
                    TVL_GAUGE.labels(chain=chain, protocol="lending").set(lending_tvl)
                    TVL_GAUGE.labels(chain=chain, protocol="liquidity").set(liquidity_tvl)
                    
                    # Store in Consul for monitoring
                    await vault_consul.consul.kv.put(
                        f"defi/metrics/{chain}/tvl",
                        json.dumps({
                            "lending": str(lending_tvl),
                            "liquidity": str(liquidity_tvl),
                            "timestamp": datetime.utcnow().isoformat()
                        })
                    )
            
            await asyncio.sleep(60)  # Update every minute
        except Exception as e:
            logger.error(f"Protocol monitoring error: {e}")
            await asyncio.sleep(60)

async def update_oracle_prices():
    """Update oracle price feeds"""
    while True:
        try:
            if price_oracle and vault_consul:
                # Get price updates
                prices = await price_oracle.get_latest_prices()
                
                # Sign and submit to chain
                for asset_pair, price_data in prices.items():
                    signed_price = await vault_consul.sign_oracle_data(
                        price_data,
                        "price"
                    )
                    
                    # Submit to blockchain
                    # ... blockchain interaction code ...
                    
            await asyncio.sleep(30)  # Update every 30 seconds
        except Exception as e:
            logger.error(f"Oracle update error: {e}")
            await asyncio.sleep(60)

async def monitor_governance_proposals():
    """Monitor and execute governance proposals"""
    while True:
        try:
            if vault_consul:
                # Check for proposals ready to execute
                _, proposals = await vault_consul.consul.kv.get(
                    "defi/governance/proposals",
                    recurse=True
                )
                
                if proposals:
                    for proposal_kv in proposals:
                        if proposal_kv["Value"]:
                            proposal = json.loads(proposal_kv["Value"])
                            
                            # Check if timelock passed
                            proposed_at = datetime.fromisoformat(
                                proposal["proposed_at"]
                            )
                            
                            if (datetime.utcnow() - proposed_at).total_seconds() > proposal.get("execution_delay", 172800):
                                # Execute proposal
                                logger.info(f"Executing proposal: {proposal['title']}")
                                # ... execution logic ...
                                
            await asyncio.sleep(300)  # Check every 5 minutes
        except Exception as e:
            logger.error(f"Governance monitoring error: {e}")
            await asyncio.sleep(60)

# Enhanced Health Check

@app.get("/health")
async def health_check():
    """Enhanced health check with protocol status"""
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
    
    # Check blockchain connections
    if connection_pool:
        chain_health = {}
        for chain in connection_pool.chains:
            try:
                # Check connection
                adapter = await connection_pool.get_adapter(chain)
                chain_health[chain] = {"status": "healthy"}
            except Exception:
                chain_health[chain] = {"status": "unhealthy"}
                health["status"] = "degraded"
        
        health["checks"]["chains"] = chain_health
    
    # Check protocols
    if defi_manager:
        health["checks"]["protocols"] = {
            "lending": len(lending_protocols),
            "liquidity": len(liquidity_protocols),
            "yield": len(yield_protocols),
            "auction": len(auction_protocols),
            "insurance": 1 if insurance_protocol else 0
        }
    
    return health 