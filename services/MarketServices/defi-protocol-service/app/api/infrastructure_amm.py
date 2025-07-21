"""
Infrastructure AMM API endpoints

Provides endpoints for AMM pools trading infrastructure resource tokens.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
import logging

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from platformq_shared import get_current_user
from ..protocols.infrastructure_amm import InfrastructureAMMProtocol
from ..models import ResourceType, ServiceTier

logger = logging.getLogger(__name__)

router = APIRouter()


# Request/Response Models
class CreatePoolRequest(BaseModel):
    """Request to create a new AMM pool"""
    resource_token_id: int = Field(..., description="Resource token ID")
    payment_token: str = Field(..., description="Payment token address (e.g., USDC)")
    initial_resource_amount: int = Field(..., gt=0, description="Initial resource token liquidity")
    initial_payment_amount: Decimal = Field(..., gt=0, description="Initial payment token liquidity")
    fee_rate: int = Field(default=30, ge=1, le=1000, description="Fee rate in basis points (30 = 0.3%)")


class AddLiquidityRequest(BaseModel):
    """Request to add liquidity to a pool"""
    pool_id: int = Field(..., description="AMM pool ID")
    resource_amount: int = Field(..., gt=0, description="Amount of resource tokens to add")
    max_payment_amount: Decimal = Field(..., gt=0, description="Maximum payment tokens to add")
    slippage_tolerance: Decimal = Field(default=Decimal("0.01"), description="Slippage tolerance (0.01 = 1%)")


class RemoveLiquidityRequest(BaseModel):
    """Request to remove liquidity from a pool"""
    pool_id: int = Field(..., description="AMM pool ID")
    lp_token_amount: Decimal = Field(..., gt=0, description="Amount of LP tokens to burn")
    min_resource_amount: int = Field(default=0, description="Minimum resource tokens to receive")
    min_payment_amount: Decimal = Field(default=Decimal("0"), description="Minimum payment tokens to receive")


class SwapRequest(BaseModel):
    """Request to swap tokens"""
    pool_id: int = Field(..., description="AMM pool ID")
    input_is_resource: bool = Field(..., description="True if swapping resource for payment, false otherwise")
    input_amount: Decimal = Field(..., gt=0, description="Amount of input tokens")
    min_output_amount: Decimal = Field(..., gt=0, description="Minimum output tokens to receive")


class PoolResponse(BaseModel):
    """AMM pool details"""
    pool_id: int
    resource_token_id: int
    payment_token: str
    resource_reserves: int
    payment_reserves: Decimal
    total_lp_tokens: Decimal
    lp_token_address: str
    fee_rate: Decimal
    price: Decimal  # Payment tokens per resource token
    volume_24h: Decimal
    fees_24h: Decimal
    apy: Decimal


class LiquidityResponse(BaseModel):
    """Response for liquidity operations"""
    pool_id: int
    resource_amount: int
    payment_amount: Decimal
    lp_tokens_minted: Optional[Decimal] = None
    lp_tokens_burned: Optional[Decimal] = None
    share_percentage: Decimal
    tx_hash: str


class SwapResponse(BaseModel):
    """Response for swap operations"""
    pool_id: int
    input_token: str
    output_token: str
    input_amount: Decimal
    output_amount: Decimal
    fee_amount: Decimal
    price_impact: Decimal
    effective_price: Decimal
    tx_hash: str


# Initialize protocol
amm_protocol = None


async def get_amm_protocol() -> InfrastructureAMMProtocol:
    """Get infrastructure AMM protocol instance"""
    global amm_protocol
    if not amm_protocol:
        from ..main import defi_manager
        amm_protocol = InfrastructureAMMProtocol(
            defi_manager.blockchain_pool,
            defi_manager.config
        )
        await amm_protocol.initialize()
    return amm_protocol


@router.post("/pools/create", response_model=PoolResponse)
async def create_pool(
    request: CreatePoolRequest,
    current_user: Dict = Depends(get_current_user)
) -> PoolResponse:
    """
    Create a new AMM pool for resource tokens
    
    - Requires initial liquidity for both tokens
    - Sets the initial price ratio
    - Mints LP tokens to the creator
    """
    protocol = await get_amm_protocol()
    
    try:
        result = await protocol.create_pool(
            creator=current_user["wallet_address"],
            resource_token_id=request.resource_token_id,
            payment_token=request.payment_token,
            initial_resource_amount=request.initial_resource_amount,
            initial_payment_amount=request.initial_payment_amount,
            fee_rate=request.fee_rate
        )
        
        return PoolResponse(
            pool_id=result["pool_id"],
            resource_token_id=request.resource_token_id,
            payment_token=request.payment_token,
            resource_reserves=request.initial_resource_amount,
            payment_reserves=request.initial_payment_amount,
            total_lp_tokens=result["lp_tokens_minted"],
            lp_token_address=result["lp_token_address"],
            fee_rate=Decimal(request.fee_rate) / Decimal(10000),
            price=request.initial_payment_amount / request.initial_resource_amount,
            volume_24h=Decimal("0"),
            fees_24h=Decimal("0"),
            apy=Decimal("0")
        )
        
    except Exception as e:
        logger.error(f"Error creating pool: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/pools/{pool_id}/add-liquidity", response_model=LiquidityResponse)
async def add_liquidity(
    pool_id: int,
    request: AddLiquidityRequest,
    current_user: Dict = Depends(get_current_user)
) -> LiquidityResponse:
    """
    Add liquidity to an existing pool
    
    - Adds tokens in proportion to current reserves
    - Mints LP tokens based on share of pool
    - Applies slippage protection
    """
    protocol = await get_amm_protocol()
    
    try:
        result = await protocol.add_liquidity(
            pool_id=pool_id,
            provider=current_user["wallet_address"],
            resource_amount=request.resource_amount,
            max_payment_amount=request.max_payment_amount,
            slippage_tolerance=request.slippage_tolerance
        )
        
        return LiquidityResponse(
            pool_id=pool_id,
            resource_amount=result["resource_added"],
            payment_amount=result["payment_added"],
            lp_tokens_minted=result["lp_tokens_minted"],
            share_percentage=result["share_percentage"],
            tx_hash=result["tx_hash"]
        )
        
    except Exception as e:
        logger.error(f"Error adding liquidity: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/pools/{pool_id}/remove-liquidity", response_model=LiquidityResponse)
async def remove_liquidity(
    pool_id: int,
    request: RemoveLiquidityRequest,
    current_user: Dict = Depends(get_current_user)
) -> LiquidityResponse:
    """
    Remove liquidity from a pool
    
    - Burns LP tokens
    - Returns proportional share of both tokens
    - Includes accumulated fees
    """
    protocol = await get_amm_protocol()
    
    try:
        result = await protocol.remove_liquidity(
            pool_id=pool_id,
            provider=current_user["wallet_address"],
            lp_token_amount=request.lp_token_amount,
            min_resource_amount=request.min_resource_amount,
            min_payment_amount=request.min_payment_amount
        )
        
        return LiquidityResponse(
            pool_id=pool_id,
            resource_amount=result["resource_returned"],
            payment_amount=result["payment_returned"],
            lp_tokens_burned=request.lp_token_amount,
            share_percentage=result["remaining_share"],
            tx_hash=result["tx_hash"]
        )
        
    except Exception as e:
        logger.error(f"Error removing liquidity: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/pools/{pool_id}/swap", response_model=SwapResponse)
async def swap_tokens(
    pool_id: int,
    request: SwapRequest,
    current_user: Dict = Depends(get_current_user)
) -> SwapResponse:
    """
    Swap tokens in an AMM pool
    
    - Uses constant product formula (x * y = k)
    - Applies time decay pricing for resources
    - Includes fee deduction
    - Provides slippage protection
    """
    protocol = await get_amm_protocol()
    
    try:
        result = await protocol.swap(
            pool_id=pool_id,
            trader=current_user["wallet_address"],
            input_is_resource=request.input_is_resource,
            input_amount=request.input_amount,
            min_output_amount=request.min_output_amount
        )
        
        return SwapResponse(
            pool_id=pool_id,
            input_token=result["input_token"],
            output_token=result["output_token"],
            input_amount=request.input_amount,
            output_amount=result["output_amount"],
            fee_amount=result["fee_amount"],
            price_impact=result["price_impact"],
            effective_price=result["effective_price"],
            tx_hash=result["tx_hash"]
        )
        
    except Exception as e:
        logger.error(f"Error swapping tokens: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/pools", response_model=List[PoolResponse])
async def list_pools(
    resource_type: Optional[ResourceType] = None,
    payment_token: Optional[str] = None,
    active_only: bool = True
) -> List[PoolResponse]:
    """List all AMM pools with optional filters"""
    protocol = await get_amm_protocol()
    
    try:
        pools = await protocol.list_pools(
            resource_type=resource_type,
            payment_token=payment_token,
            active_only=active_only
        )
        
        return [
            PoolResponse(
                pool_id=pool["pool_id"],
                resource_token_id=pool["resource_token_id"],
                payment_token=pool["payment_token"],
                resource_reserves=pool["resource_reserves"],
                payment_reserves=pool["payment_reserves"],
                total_lp_tokens=pool["total_lp_tokens"],
                lp_token_address=pool["lp_token_address"],
                fee_rate=pool["fee_rate"],
                price=pool["price"],
                volume_24h=pool["volume_24h"],
                fees_24h=pool["fees_24h"],
                apy=pool["apy"]
            )
            for pool in pools
        ]
        
    except Exception as e:
        logger.error(f"Error listing pools: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pools/{pool_id}", response_model=PoolResponse)
async def get_pool_details(pool_id: int) -> PoolResponse:
    """Get detailed information about a specific pool"""
    protocol = await get_amm_protocol()
    
    try:
        pool = await protocol.get_pool_details(pool_id)
        
        return PoolResponse(
            pool_id=pool["pool_id"],
            resource_token_id=pool["resource_token_id"],
            payment_token=pool["payment_token"],
            resource_reserves=pool["resource_reserves"],
            payment_reserves=pool["payment_reserves"],
            total_lp_tokens=pool["total_lp_tokens"],
            lp_token_address=pool["lp_token_address"],
            fee_rate=pool["fee_rate"],
            price=pool["price"],
            volume_24h=pool["volume_24h"],
            fees_24h=pool["fees_24h"],
            apy=pool["apy"]
        )
        
    except Exception as e:
        logger.error(f"Error getting pool details: {e}")
        raise HTTPException(status_code=404, detail="Pool not found")


@router.get("/pools/{pool_id}/quote")
async def get_swap_quote(
    pool_id: int,
    input_is_resource: bool,
    input_amount: Decimal
) -> Dict[str, Any]:
    """
    Get a quote for a potential swap
    
    - Shows expected output amount
    - Calculates price impact
    - Includes fee calculation
    - No actual transaction
    """
    protocol = await get_amm_protocol()
    
    try:
        quote = await protocol.get_swap_quote(
            pool_id=pool_id,
            input_is_resource=input_is_resource,
            input_amount=input_amount
        )
        
        return quote
        
    except Exception as e:
        logger.error(f"Error getting swap quote: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/user/{address}/positions")
async def get_user_positions(
    address: str,
    current_user: Dict = Depends(get_current_user)
) -> List[Dict[str, Any]]:
    """Get all liquidity positions for a user"""
    # Verify user is querying their own positions or has permission
    if address.lower() != current_user["wallet_address"].lower():
        raise HTTPException(status_code=403, detail="Not authorized")
    
    protocol = await get_amm_protocol()
    
    try:
        positions = await protocol.get_user_positions(address)
        return positions
        
    except Exception as e:
        logger.error(f"Error getting user positions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/amm")
async def get_amm_stats() -> Dict[str, Any]:
    """Get overall AMM protocol statistics"""
    protocol = await get_amm_protocol()
    
    try:
        stats = await protocol.get_protocol_stats()
        
        return {
            "total_pools": stats["total_pools"],
            "total_volume_24h": stats["total_volume_24h"],
            "total_fees_24h": stats["total_fees_24h"],
            "total_value_locked": stats["tvl"],
            "top_pools": stats["top_pools"],
            "resource_breakdown": stats["resource_breakdown"]
        }
        
    except Exception as e:
        logger.error(f"Error getting AMM stats: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 