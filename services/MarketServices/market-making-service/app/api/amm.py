"""AMM API endpoints."""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Annotated, Any
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.dependencies import get_ignite_client, get_pulsar_client, get_redis_client
from app.core.events import publish_event, EventType
from app.monitoring import pool_operations, swap_volume, liquidity_gauge
from app.config import settings
from app.pools.concentrated_liquidity import ConcentratedLiquidityPool
from app.pools.stableswap import StableSwapPool
from app.amm.dynamic_fee_manager import DynamicFeeManager

# Mock models and enums for compatibility
class PoolType:
    CONSTANT_PRODUCT = "constant_product"
    STABLE_SWAP = "stable_swap"
    CONCENTRATED = "concentrated"

class SwapDirection:
    TOKEN0_TO_TOKEN1 = "token0_to_token1"
    TOKEN1_TO_TOKEN0 = "token1_to_token0"


router = APIRouter()


# Request/Response Models

class CreatePoolRequest(BaseModel):
    """Request to create a new AMM pool."""
    pool_type: PoolType
    base_asset: str
    quote_asset: str
    initial_price: Optional[Decimal] = Field(None, gt=0)
    fee_bps: Optional[int] = Field(None, ge=1, le=1000)
    tick_spacing: Optional[int] = None  # For concentrated liquidity
    amplification: Optional[int] = None  # For stableswap


class AddLiquidityRequest(BaseModel):
    """Request to add liquidity to a pool."""
    pool_id: str
    base_amount: Decimal = Field(..., gt=0)
    quote_amount: Decimal = Field(..., gt=0)
    tick_lower: Optional[int] = None  # For concentrated liquidity
    tick_upper: Optional[int] = None  # For concentrated liquidity
    min_lp_tokens: Optional[Decimal] = None


class RemoveLiquidityRequest(BaseModel):
    """Request to remove liquidity from a pool."""
    pool_id: str
    position_id: str
    lp_tokens: Optional[Decimal] = None  # None = remove all
    percentage: Optional[float] = Field(None, gt=0, le=100)  # Alternative to lp_tokens
    min_base_amount: Optional[Decimal] = None
    min_quote_amount: Optional[Decimal] = None


class SwapQuoteRequest(BaseModel):
    """Request for swap quote."""
    pool_id: str
    direction: SwapDirection
    amount_in: Decimal = Field(..., gt=0)


class ExecuteSwapRequest(BaseModel):
    """Request to execute a swap."""
    pool_id: str
    direction: SwapDirection
    amount_in: Decimal = Field(..., gt=0)
    min_amount_out: Optional[Decimal] = Field(None, gt=0)
    deadline: Optional[datetime] = None


# Pool Management

@router.post("/pools", response_model=LiquidityPool)
async def create_pool(
    request: CreatePoolRequest,
    user_id: Annotated[str, Depends(lambda: "mock_user")],
    settings: Annotated[Settings, Depends(lambda: settings)],
    concentrated_amm: Annotated[ConcentratedLiquidityAMM, Depends(lambda: None)],
    stableswap_amm: Annotated[StableSwapAMM, Depends(lambda: None)],
    fee_manager: Annotated[DynamicFeeManager, Depends(lambda: None)],
    pool_manager = Depends(lambda: None)
):
    """Create a new AMM pool."""
    # Generate pool ID
    pool_id = f"{request.base_asset}-{request.quote_asset}-{request.pool_type.value}-{uuid.uuid4().hex[:8]}"
    
    # Check if similar pool exists
    existing = await pool_manager.find_pool(
        base_asset=request.base_asset,
        quote_asset=request.quote_asset,
        pool_type=request.pool_type
    )
    if existing:
        raise HTTPException(
            status_code=400,
            detail=f"Pool already exists: {existing.pool_id}"
        )
    
    # Create pool based on type
    if request.pool_type == PoolType.CONCENTRATED:
        if not request.initial_price:
            raise HTTPException(
                status_code=400,
                detail="Initial price required for concentrated liquidity pool"
            )
        pool = concentrated_amm.create_pool(
            pool_id=pool_id,
            base_asset=request.base_asset,
            quote_asset=request.quote_asset,
            initial_price=request.initial_price,
            fee_bps=request.fee_bps or settings.base_fee_bps
        )
    elif request.pool_type == PoolType.STABLESWAP:
        pool = stableswap_amm.create_pool(
            pool_id=pool_id,
            base_asset=request.base_asset,
            quote_asset=request.quote_asset,
            amplification=request.amplification
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported pool type: {request.pool_type}"
        )
    
    # Initialize fee manager for pool
    await fee_manager.initialize_pool(pool_id, pool.base_fee_bps)
    
    # Store pool
    await pool_manager.store_pool(pool)
    
    return pool


@router.get("/pools/{pool_id}", response_model=LiquidityPool)
async def get_pool(
    pool_id: str,
    pool_manager = Depends(lambda: None)
):
    """Get pool details."""
    pool = await pool_manager.get_pool(pool_id)
    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")
    return pool


@router.get("/pools", response_model=List[LiquidityPool])
async def list_pools(
    base_asset: Optional[str] = None,
    quote_asset: Optional[str] = None,
    pool_type: Optional[PoolType] = None,
    active_only: bool = True,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    pool_manager = Depends(lambda: None)
):
    """List AMM pools with filters."""
    pools = await pool_manager.list_pools(
        base_asset=base_asset,
        quote_asset=quote_asset,
        pool_type=pool_type,
        active_only=active_only,
        limit=limit,
        offset=offset
    )
    return pools


# Liquidity Management

@router.post("/liquidity/add")
async def add_liquidity(
    request: AddLiquidityRequest,
    user_id: Annotated[str, Depends(lambda: "mock_user")],
    concentrated_amm: Annotated[ConcentratedLiquidityAMM, Depends(lambda: None)],
    stableswap_amm: Annotated[StableSwapAMM, Depends(lambda: None)],
    pool_manager = Depends(lambda: None)
):
    """Add liquidity to a pool."""
    # Get pool
    pool = await pool_manager.get_pool(request.pool_id)
    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")
    
    # Create or get position
    position = await pool_manager.get_or_create_position(
        pool_id=request.pool_id,
        provider=user_id
    )
    
    # Add liquidity based on pool type
    if pool.pool_type == PoolType.CONCENTRATED:
        if request.tick_lower is None or request.tick_upper is None:
            raise HTTPException(
                status_code=400,
                detail="Tick range required for concentrated liquidity"
            )
        
        base_amount, quote_amount = await concentrated_amm.add_liquidity(
            pool=pool,
            position=position,
            tick_lower=request.tick_lower,
            tick_upper=request.tick_upper,
            liquidity_amount=request.base_amount  # Simplified
        )
        
        result = {
            "position_id": position.position_id,
            "base_deposited": base_amount,
            "quote_deposited": quote_amount,
            "liquidity": position.liquidity,
            "tick_lower": request.tick_lower,
            "tick_upper": request.tick_upper
        }
        
    elif pool.pool_type == PoolType.STABLESWAP:
        base_deposited, quote_deposited, lp_tokens = await stableswap_amm.add_liquidity(
            pool=pool,
            position=position,
            base_amount=request.base_amount,
            quote_amount=request.quote_amount,
            min_lp_tokens=request.min_lp_tokens
        )
        
        result = {
            "position_id": position.position_id,
            "base_deposited": base_deposited,
            "quote_deposited": quote_deposited,
            "lp_tokens": lp_tokens
        }
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported pool type: {pool.pool_type}"
        )
    
    # Update pool in storage
    await pool_manager.update_pool(pool)
    await pool_manager.update_position(position)
    
    return result


@router.post("/liquidity/remove")
async def remove_liquidity(
    request: RemoveLiquidityRequest,
    user_id: Annotated[str, Depends(lambda: "mock_user")],
    concentrated_amm: Annotated[ConcentratedLiquidityAMM, Depends(lambda: None)],
    stableswap_amm: Annotated[StableSwapAMM, Depends(lambda: None)],
    pool_manager = Depends(lambda: None)
):
    """Remove liquidity from a pool."""
    # Get pool and position
    pool = await pool_manager.get_pool(request.pool_id)
    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")
    
    position = await pool_manager.get_position(request.position_id)
    if not position:
        raise HTTPException(status_code=404, detail="Position not found")
    
    # Check ownership
    if position.provider != user_id:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Calculate amount to remove
    if request.lp_tokens:
        lp_tokens_to_remove = request.lp_tokens
    elif request.percentage:
        lp_tokens_to_remove = position.liquidity * Decimal(str(request.percentage / 100))
    else:
        lp_tokens_to_remove = position.liquidity  # Remove all
    
    # Remove liquidity based on pool type
    if pool.pool_type == PoolType.CONCENTRATED:
        base_amount, quote_amount, fees_base, fees_quote = await concentrated_amm.remove_liquidity(
            pool=pool,
            position=position,
            liquidity_amount=lp_tokens_to_remove
        )
        
        result = {
            "base_amount": base_amount,
            "quote_amount": quote_amount,
            "fees_collected_base": fees_base,
            "fees_collected_quote": fees_quote,
            "remaining_liquidity": position.liquidity
        }
        
    elif pool.pool_type == PoolType.STABLESWAP:
        base_amount, quote_amount = await stableswap_amm.remove_liquidity(
            pool=pool,
            position=position,
            lp_tokens=lp_tokens_to_remove,
            min_base=request.min_base_amount,
            min_quote=request.min_quote_amount
        )
        
        result = {
            "base_amount": base_amount,
            "quote_amount": quote_amount,
            "remaining_lp_tokens": position.liquidity
        }
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported pool type: {pool.pool_type}"
        )
    
    # Update storage
    await pool_manager.update_pool(pool)
    await pool_manager.update_position(position)
    
    return result


@router.get("/liquidity/positions/{user_id}", response_model=List[LiquidityPosition])
async def get_user_positions(
    user_id: str,
    pool_manager = Depends(lambda: None)
):
    """Get all liquidity positions for a user."""
    positions = await pool_manager.get_user_positions(user_id)
    return positions


# Swaps

@router.post("/swap/quote")
async def get_swap_quote(
    request: SwapQuoteRequest,
    concentrated_amm: Annotated[ConcentratedLiquidityAMM, Depends(lambda: None)],
    stableswap_amm: Annotated[StableSwapAMM, Depends(lambda: None)],
    pool_manager = Depends(lambda: None)
):
    """Get a quote for a swap without executing."""
    # Get pool
    pool = await pool_manager.get_pool(request.pool_id)
    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")
    
    # Create a copy to simulate swap
    pool_copy = pool.copy(deep=True)
    
    # Simulate swap based on pool type
    if pool.pool_type == PoolType.CONCENTRATED:
        result = await concentrated_amm.swap(
            pool=pool_copy,
            direction=request.direction,
            amount_in=request.amount_in
        )
    elif pool.pool_type == PoolType.STABLESWAP:
        result = await stableswap_amm.swap(
            pool=pool_copy,
            direction=request.direction,
            amount_in=request.amount_in
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported pool type: {pool.pool_type}"
        )
    
    return {
        "amount_out": result.amount_out,
        "price_impact": result.price_impact,
        "execution_price": result.execution_price,
        "fee": result.fee_paid,
        "slippage_warning": result.price_impact > Decimal("0.01")  # > 1%
    }


@router.post("/swap/execute")
async def execute_swap(
    request: ExecuteSwapRequest,
    user_id: Annotated[str, Depends(lambda: "mock_user")],
    concentrated_amm: Annotated[ConcentratedLiquidityAMM, Depends(lambda: None)],
    stableswap_amm: Annotated[StableSwapAMM, Depends(lambda: None)],
    pool_manager = Depends(lambda: None)
):
    """Execute a swap."""
    # Check deadline
    if request.deadline and datetime.utcnow() > request.deadline:
        raise HTTPException(status_code=400, detail="Transaction deadline exceeded")
    
    # Get pool
    pool = await pool_manager.get_pool(request.pool_id)
    if not pool:
        raise HTTPException(status_code=404, detail="Pool not found")
    
    # Execute swap based on pool type
    if pool.pool_type == PoolType.CONCENTRATED:
        result = await concentrated_amm.swap(
            pool=pool,
            direction=request.direction,
            amount_in=request.amount_in,
            min_amount_out=request.min_amount_out
        )
    elif pool.pool_type == PoolType.STABLESWAP:
        result = await stableswap_amm.swap(
            pool=pool,
            direction=request.direction,
            amount_in=request.amount_in,
            min_amount_out=request.min_amount_out
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported pool type: {pool.pool_type}"
        )
    
    # Set trader
    result.trader = user_id
    
    # Update pool
    await pool_manager.update_pool(pool)
    
    # Store swap record
    await pool_manager.store_swap(result)
    
    return {
        "swap_id": result.swap_id,
        "amount_in": result.amount_in,
        "amount_out": result.amount_out,
        "fee_paid": result.fee_paid,
        "execution_price": result.execution_price,
        "price_impact": result.price_impact
    }


# Pool Metrics

@router.get("/pools/{pool_id}/metrics", response_model=PoolMetrics)
async def get_pool_metrics(
    pool_id: str,
    period: str = Query("daily", regex="^(hourly|daily|weekly)$"),
    pool_manager = Depends(lambda: None)
):
    """Get pool metrics and analytics."""
    metrics = await pool_manager.get_pool_metrics(pool_id, period)
    if not metrics:
        raise HTTPException(status_code=404, detail="Metrics not found")
    return metrics


@router.get("/pools/{pool_id}/fee-history")
async def get_fee_history(
    pool_id: str,
    fee_manager: Annotated[Any, Depends(lambda: None)],
    hours: int = Query(24, ge=1, le=168)  # Max 1 week
):
    """Get fee history for a pool."""
    history = await fee_manager.get_fee_history(pool_id, hours)
    
    return {
        "pool_id": pool_id,
        "current_fee_bps": fee_manager.get_current_fee(pool_id),
        "history": [
            {
                "timestamp": timestamp.isoformat(),
                "fee_bps": fee
            }
            for timestamp, fee in history
        ]
    } 