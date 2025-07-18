"""
Compute Marketplace Service

Web3/DeFi-native marketplace for compute resources with fractional ownership,
trust-based pricing, and specialized support for federated learning.
"""

import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum
import uuid

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
import asyncio
import httpx
from pyignite import Client as IgniteClient

from platformq_shared import (
    create_base_app,
    ErrorCode,
    AppException,
    EventProcessor,
    get_pulsar_client,
    get_ignite_client
)
from platformq_shared.event_publisher import EventPublisher
from platformq_shared.database import get_db

from .models import (
    ComputeOffering,
    ComputePurchase,
    ResourceType,
    OfferingStatus,
    PurchaseStatus,
    Priority,
    ComputeTokenHolder,
    ComputeLiquidityPosition,
    ComputeDerivative
)
from .repository import (
    ComputeOfferingRepository,
    ComputePurchaseRepository,
    ResourceAvailabilityRepository,
    PricingRuleRepository
)
from .event_processors import DatasetMarketplaceEventProcessor

# Import new components
from .neuromorphic_matcher import NeuromorphicComputeMatcher, ComputeRequirement
from .defi.compute_tokenizer import ComputeTokenizer
from .trust_pricing_engine import TrustPricingEngine
from .fl_compute_pool import FederatedLearningComputePool, FLComputeCapabilities
from .config import settings

# Setup logging
logger = logging.getLogger(__name__)

# Global instances
event_processor = None
neuromorphic_matcher = None
compute_tokenizer = None
trust_pricing_engine = None
fl_compute_pool = None
ignite_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global event_processor, neuromorphic_matcher, compute_tokenizer
    global trust_pricing_engine, fl_compute_pool, ignite_client
    
    # Startup
    logger.info("Initializing Web3/DeFi Compute Marketplace Service...")
    
    # Initialize Ignite client
    ignite_client = await get_ignite_client()
    
    # Initialize event processor
    pulsar_client = get_pulsar_client()
    event_publisher = EventPublisher(pulsar_client)
    
    event_processor = DatasetMarketplaceEventProcessor(
        pulsar_client=pulsar_client,
        service_name="compute-marketplace-service"
    )
    
    # Initialize neuromorphic matcher for ultra-fast matching
    neuromorphic_matcher = NeuromorphicComputeMatcher(
        neuromorphic_url=settings.neuromorphic_service_url,
        ignite_client=ignite_client,
        event_publisher=event_publisher,
        graph_intelligence_url=settings.graph_intelligence_url
    )
    await neuromorphic_matcher.initialize()
    
    # Initialize Web3/DeFi tokenizer
    compute_tokenizer = ComputeTokenizer(
        web3_provider=settings.web3_provider_url,
        contract_addresses={
            "token_factory": settings.token_factory_address,
            "amm_router": settings.amm_router_address,
            "lending_pool": settings.lending_pool_address,
            "price_oracle": settings.price_oracle_address
        },
        ignite_client=ignite_client,
        event_publisher=event_publisher,
        vc_service_url=settings.vc_service_url,
        private_key=settings.operator_private_key  # For automated operations
    )
    
    # Initialize trust-based pricing engine
    trust_pricing_engine = TrustPricingEngine(
        graph_intelligence_url=settings.graph_intelligence_url,
        ignite_client=ignite_client,
        event_publisher=event_publisher,
        quantum_optimization_url=settings.quantum_optimization_url
    )
    
    # Initialize federated learning compute pool
    fl_compute_pool = FederatedLearningComputePool(
        fl_service_url=settings.federated_learning_url,
        ignite_client=ignite_client,
        event_publisher=event_publisher,
        vc_service_url=settings.vc_service_url,
        neuromorphic_url=settings.neuromorphic_service_url
    )
    await fl_compute_pool.initialize()
    
    # Start event processor
    await event_processor.start()
    
    logger.info("Web3/DeFi Compute Marketplace Service initialized successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Compute Marketplace Service...")
    
    if event_processor:
        await event_processor.stop()
    
    if neuromorphic_matcher:
        await neuromorphic_matcher.close()
        
    if compute_tokenizer:
        await compute_tokenizer.close()
        
    if trust_pricing_engine:
        await trust_pricing_engine.close()
        
    if fl_compute_pool:
        await fl_compute_pool.close()
    
    logger.info("Compute Marketplace Service shutdown complete")

# Create FastAPI app
app = create_base_app(
    title="Web3/DeFi Compute Marketplace",
    description="Decentralized marketplace for compute resources with fractional ownership and DeFi primitives",
    version="2.0.0",
    lifespan=lifespan,
    event_processors=[event_processor] if event_processor else []
)

# Pydantic models for API requests/responses
class ComputeOfferingRequest(BaseModel):
    provider_id: str
    resource_type: str  # "cpu", "gpu", "tpu"
    resource_specs: Dict[str, Any]
    availability_hours: List[int]  # Hours of day available
    min_duration_minutes: int
    max_duration_minutes: Optional[int]
    price_per_hour: float
    location: str
    tags: List[str] = []
    # Web3/DeFi options
    enable_tokenization: bool = False
    tokenization_hours: Optional[int] = None  # Hours to tokenize
    enable_fractional: bool = True
    min_fraction_hours: float = 0.1


class TokenizationRequest(BaseModel):
    offering_id: str
    total_hours: int
    min_fraction_hours: float = 0.1
    create_liquidity_pool: bool = False
    paired_token: Optional[str] = "USDC"
    initial_liquidity: Optional[Dict[str, float]] = None


class LiquidityPoolRequest(BaseModel):
    token_address: str
    paired_token: str = "USDC"
    compute_hours: float
    paired_amount: float


class FLComputeOfferingRequest(BaseModel):
    base_offering_id: str
    provider_id: str
    capabilities: Dict[str, Any]  # FLComputeCapabilities as dict
    premium_percentage: float = 20.0


class ComputeMatchRequest(BaseModel):
    resource_type: str
    min_memory_gb: float
    min_vcpus: int
    gpu_type: Optional[str] = None
    location_preference: Optional[str] = None
    max_latency_ms: Optional[int] = None
    max_price_per_hour: Optional[float] = None
    duration_minutes: int = 60
    priority: str = "normal"
    use_trust_pricing: bool = True


@app.get("/")
async def root():
    return {
        "service": "Web3/DeFi Compute Marketplace",
        "status": "operational",
        "features": [
            "fractional_ownership",
            "compute_tokenization",
            "amm_liquidity_pools",
            "trust_based_pricing",
            "neuromorphic_matching",
            "federated_learning_support"
        ]
    }


@app.post("/api/v1/offerings", response_model=Dict[str, Any])
async def create_compute_offering(
    request: ComputeOfferingRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    tenant_id: str = Depends(lambda: "default-tenant")  # TODO: Get from auth
):
    """Create a new compute offering with optional tokenization"""
    try:
        # Initialize repositories
        offering_repo = ComputeOfferingRepository(db)
        
        # Create offering
        offering = offering_repo.create({
            "offering_id": str(uuid.uuid4()),
            "tenant_id": tenant_id,
            "provider_id": request.provider_id,
            "resource_type": request.resource_type,
            "resource_specs": request.resource_specs,
            "availability_hours": request.availability_hours,
            "min_duration_minutes": request.min_duration_minutes,
            "max_duration_minutes": request.max_duration_minutes,
            "price_per_hour": request.price_per_hour,
            "location": request.location,
            "tags": request.tags,
            "status": OfferingStatus.ACTIVE,
            "is_tokenized": request.enable_tokenization,
            "fractionalization_enabled": request.enable_fractional,
            "min_fraction_size": request.min_fraction_hours
        })
        
        response_data = {
            "offering_id": offering.offering_id,
            "status": "active",
            "created_at": offering.created_at.isoformat()
        }
        
        # Tokenize if requested
        if request.enable_tokenization and request.tokenization_hours:
            background_tasks.add_task(
                tokenize_offering_async,
                offering,
                request.tokenization_hours,
                request.min_fraction_hours
            )
            response_data["tokenization_status"] = "pending"
        
        # Register with neuromorphic matcher
        background_tasks.add_task(
            register_with_neuromorphic,
            offering
        )
        
        return response_data
        
    except Exception as e:
        logger.error(f"Error creating compute offering: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/match", response_model=List[Dict[str, Any]])
async def match_compute_resources(
    request: ComputeMatchRequest,
    db: Session = Depends(get_db),
    buyer_id: str = Depends(lambda: "buyer-123")  # TODO: Get from auth
):
    """Ultra-fast compute matching using neuromorphic processing"""
    try:
        # Convert to neuromorphic requirement
        requirement = ComputeRequirement(
            resource_type=request.resource_type,
            min_memory_gb=request.min_memory_gb,
            min_vcpus=request.min_vcpus,
            gpu_type=request.gpu_type,
            location_preference=request.location_preference,
            max_latency_ms=request.max_latency_ms,
            max_price_per_hour=request.max_price_per_hour,
            duration_minutes=request.duration_minutes,
            priority=request.priority
        )
        
        # Get matches using neuromorphic matcher
        matches = await neuromorphic_matcher.match_compute_request(
            requirement,
            max_results=10
        )
        
        # Apply trust-based pricing if enabled
        if request.use_trust_pricing:
            enhanced_matches = []
            for provider, score in matches:
                pricing_result = await trust_pricing_engine.calculate_trust_adjusted_price(
                    base_price=provider.price_per_hour,
                    provider_id=provider.provider_id,
                    buyer_id=buyer_id,
                    resource_type=request.resource_type,
                    duration_minutes=request.duration_minutes
                )
                
                enhanced_matches.append({
                    "provider": {
                        "provider_id": provider.provider_id,
                        "offering_id": provider.offering_id,
                        "resource_type": provider.resource_type,
                        "resource_specs": provider.resource_specs,
                        "location": provider.location,
                        "base_price_per_hour": provider.price_per_hour,
                        "trust_adjusted_price": pricing_result["final_price"],
                        "discount_percentage": pricing_result["discount_percentage"]
                    },
                    "match_score": score,
                    "trust_relationship": pricing_result["trust_relationship"],
                    "pricing_adjustments": pricing_result["adjustments"]
                })
            
            return enhanced_matches
        else:
            # Return basic matches
            return [
                {
                    "provider": {
                        "provider_id": provider.provider_id,
                        "offering_id": provider.offering_id,
                        "resource_type": provider.resource_type,
                        "resource_specs": provider.resource_specs,
                        "location": provider.location,
                        "price_per_hour": provider.price_per_hour
                    },
                    "match_score": score
                }
                for provider, score in matches
            ]
        
    except Exception as e:
        logger.error(f"Error matching compute resources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/tokenize", response_model=Dict[str, Any])
async def tokenize_compute_offering(
    request: TokenizationRequest,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db)
):
    """Tokenize compute hours into tradeable tokens"""
    try:
        # Get offering
        offering_repo = ComputeOfferingRepository(db)
        offering = offering_repo.get_by_offering_id(request.offering_id)
        
        if not offering:
            raise HTTPException(status_code=404, detail="Offering not found")
        
        # Tokenize
        result = await compute_tokenizer.tokenize_compute_offering(
            offering,
            request.total_hours,
            request.min_fraction_hours
        )
        
        # Create liquidity pool if requested
        if request.create_liquidity_pool and request.initial_liquidity:
            background_tasks.add_task(
                create_liquidity_pool_async,
                result["token_address"],
                request.paired_token,
                request.initial_liquidity
            )
            result["liquidity_pool_status"] = "pending"
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error tokenizing compute offering: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/liquidity/add", response_model=Dict[str, Any])
async def add_liquidity(
    request: LiquidityPoolRequest
):
    """Add liquidity to compute token AMM pool"""
    try:
        result = await compute_tokenizer.create_liquidity_pool(
            request.token_address,
            request.paired_token,
            request.compute_hours,
            request.paired_amount
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error adding liquidity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/lending/enable", response_model=Dict[str, Any])
async def enable_lending(
    token_address: str,
    collateral_factor: float = 0.75
):
    """Enable lending/borrowing for compute tokens"""
    try:
        result = await compute_tokenizer.enable_lending(
            token_address,
            collateral_factor
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error enabling lending: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/fl/offerings", response_model=Dict[str, Any])
async def create_fl_compute_offering(
    request: FLComputeOfferingRequest,
    db: Session = Depends(get_db)
):
    """Create federated learning optimized compute offering"""
    try:
        # Get base offering
        offering_repo = ComputeOfferingRepository(db)
        base_offering = offering_repo.get_by_offering_id(request.base_offering_id)
        
        if not base_offering:
            raise HTTPException(status_code=404, detail="Base offering not found")
        
        # Convert capabilities
        fl_capabilities = FLComputeCapabilities(**request.capabilities)
        
        # Create FL offering
        result = await fl_compute_pool.create_fl_compute_offering(
            request.provider_id,
            base_offering,
            fl_capabilities,
            request.premium_percentage
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error creating FL compute offering: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/fl/sessions/{session_id}/join", response_model=Dict[str, Any])
async def join_fl_session(
    session_id: str,
    provider_id: str,
    allocated_resources: Dict[str, Any]
):
    """Join federated learning session as compute provider"""
    try:
        result = await fl_compute_pool.join_fl_session(
            session_id,
            provider_id,
            allocated_resources
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Error joining FL session: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/offerings/{offering_id}/token", response_model=Dict[str, Any])
async def get_token_info(
    offering_id: str,
    db: Session = Depends(get_db)
):
    """Get tokenization info for an offering"""
    try:
        offering_repo = ComputeOfferingRepository(db)
        offering = offering_repo.get_by_offering_id(offering_id)
        
        if not offering:
            raise HTTPException(status_code=404, detail="Offering not found")
        
        if not offering.is_tokenized:
            raise HTTPException(status_code=400, detail="Offering is not tokenized")
        
        # Get token info from blockchain
        # This is simplified - in production query actual blockchain
        return {
            "offering_id": offering_id,
            "token_address": offering.token_address,
            "total_supply": offering.total_supply,
            "circulating_supply": offering.circulating_supply,
            "liquidity_pool": offering.liquidity_pool_address,
            "current_price": await get_current_token_price(offering.token_address)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting token info: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/analytics/defi", response_model=Dict[str, Any])
async def get_defi_analytics():
    """Get DeFi analytics for compute marketplace"""
    try:
        # Get metrics from various sources
        total_value_locked = await calculate_total_value_locked()
        active_pools = await get_active_liquidity_pools()
        lending_stats = await get_lending_statistics()
        
        return {
            "total_value_locked": total_value_locked,
            "active_liquidity_pools": len(active_pools),
            "total_compute_hours_tokenized": await get_total_tokenized_hours(),
            "lending_stats": lending_stats,
            "top_pools": active_pools[:5],
            "neuromorphic_matching_stats": neuromorphic_matcher.get_performance_stats(),
            "trust_pricing_stats": trust_pricing_engine.get_metrics()
        }
        
    except Exception as e:
        logger.error(f"Error getting DeFi analytics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Async helper functions
async def tokenize_offering_async(offering, hours, min_fraction):
    """Async tokenization task"""
    try:
        result = await compute_tokenizer.tokenize_compute_offering(
            offering,
            hours,
            min_fraction
        )
        logger.info(f"Successfully tokenized offering {offering.offering_id}")
    except Exception as e:
        logger.error(f"Failed to tokenize offering: {e}")


async def register_with_neuromorphic(offering):
    """Register offering with neuromorphic matcher"""
    # Implementation would update neuromorphic index
    pass


async def create_liquidity_pool_async(token_address, paired_token, liquidity):
    """Async liquidity pool creation"""
    try:
        result = await compute_tokenizer.create_liquidity_pool(
            token_address,
            paired_token,
            liquidity["compute_hours"],
            liquidity["paired_amount"]
        )
        logger.info(f"Successfully created liquidity pool for {token_address}")
    except Exception as e:
        logger.error(f"Failed to create liquidity pool: {e}")


async def get_current_token_price(token_address: str) -> float:
    """Get current token price from AMM"""
    # In production, query actual AMM
    return 1.0  # Placeholder


async def calculate_total_value_locked() -> float:
    """Calculate total value locked in DeFi protocols"""
    # In production, aggregate from various sources
    return 1000000.0  # Placeholder


async def get_active_liquidity_pools() -> List[Dict[str, Any]]:
    """Get active liquidity pools"""
    # In production, query from blockchain
    return []  # Placeholder


async def get_lending_statistics() -> Dict[str, Any]:
    """Get lending protocol statistics"""
    # In production, query lending protocol
    return {
        "total_supplied": 0,
        "total_borrowed": 0,
        "average_apy": 0
    }


async def get_total_tokenized_hours() -> int:
    """Get total compute hours tokenized"""
    # In production, sum from all tokenized offerings
    return 0  # Placeholder


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 