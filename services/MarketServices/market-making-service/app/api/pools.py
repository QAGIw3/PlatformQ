"""Pools API endpoints for Market Making Service"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.dependencies import get_ignite_client, get_redis_client, get_service_clients
from app.core.events import publish_event, EventType
from app.monitoring import pool_operations, swap_volume, liquidity_gauge, fee_revenue
from app.amm.concentrated_liquidity import ConcentratedLiquidityAMM
from app.amm.liquidity_pool import LiquidityPool
from app.pools.stableswap import StableSwapPool
from app.config import settings

router = APIRouter()


class CreatePoolRequest(BaseModel):
    """Request to create a new liquidity pool"""
    token_a: str = Field(..., description="First token address")
    token_b: str = Field(..., description="Second token address")
    pool_type: str = Field(..., pattern="^(constant_product|stableswap|concentrated)$")
    fee_tier: Decimal = Field(..., ge=0.0001, le=0.1, description="Fee tier (0.01% to 10%)")
    initial_liquidity_a: Decimal = Field(..., gt=0, description="Initial liquidity for token A")
    initial_liquidity_b: Decimal = Field(..., gt=0, description="Initial liquidity for token B")
    
    # Concentrated liquidity specific
    tick_spacing: Optional[int] = Field(None, description="Tick spacing for concentrated liquidity")
    initial_price: Optional[Decimal] = Field(None, gt=0, description="Initial price for concentrated liquidity")
    
    # Compliance settings
    compliance_enabled: bool = Field(default=False, description="Enable compliance features")
    min_kyc_tier: Optional[int] = Field(None, ge=1, le=3, description="Minimum KYC tier required")
    allowed_jurisdictions: Optional[List[str]] = Field(None, description="Allowed jurisdictions")
    blocked_jurisdictions: Optional[List[str]] = Field(None, description="Blocked jurisdictions")


class AddLiquidityRequest(BaseModel):
    """Request to add liquidity to a pool"""
    pool_id: str = Field(..., description="Pool ID")
    amount_a: Decimal = Field(..., gt=0, description="Amount of token A")
    amount_b: Decimal = Field(..., gt=0, description="Amount of token B")
    
    # Concentrated liquidity specific
    tick_lower: Optional[int] = Field(None, description="Lower tick for concentrated position")
    tick_upper: Optional[int] = Field(None, description="Upper tick for concentrated position")
    
    slippage_tolerance: Decimal = Field(default=0.01, ge=0, le=0.1, description="Slippage tolerance")


class RemoveLiquidityRequest(BaseModel):
    """Request to remove liquidity from a pool"""
    pool_id: str = Field(..., description="Pool ID")
    liquidity_percentage: Decimal = Field(..., gt=0, le=100, description="Percentage to remove")
    position_id: Optional[str] = Field(None, description="Specific position ID for concentrated liquidity")


class SwapRequest(BaseModel):
    """Request to execute a swap"""
    pool_id: str = Field(..., description="Pool ID")
    token_in: str = Field(..., description="Token to swap from")
    amount_in: Decimal = Field(..., gt=0, description="Amount to swap")
    min_amount_out: Decimal = Field(..., ge=0, description="Minimum amount to receive")
    recipient: Optional[str] = Field(None, description="Recipient address (defaults to sender)")


class PoolResponse(BaseModel):
    """Pool information response"""
    pool_id: str
    token_a: str
    token_b: str
    pool_type: str
    fee_tier: str
    reserve_a: str
    reserve_b: str
    total_liquidity: str
    current_price: str
    volume_24h: str
    fees_24h: str
    apy: str
    created_at: str
    compliance_enabled: bool
    
    # Additional fields for concentrated liquidity
    tick_current: Optional[int] = None
    liquidity_current: Optional[str] = None


@router.post("/create", response_model=PoolResponse)
async def create_pool(
    request: CreatePoolRequest,
    user_id: str = Depends(lambda: "mock_user")  # Replace with actual auth
):
    """Create a new liquidity pool"""
    try:
        pool_operations.labels(
            operation='create',
            pool_type=request.pool_type,
            status='started'
        ).inc()
        
        # Generate pool ID
        pool_id = f"{request.token_a}_{request.token_b}_{request.pool_type}_{int(datetime.utcnow().timestamp())}"
        
        # Store pool data in Ignite
        ignite = await get_ignite_client()
        pool_cache = await ignite.get_or_create_cache("pools")
        
        pool_data = {
            "pool_id": pool_id,
            "token_a": request.token_a,
            "token_b": request.token_b,
            "pool_type": request.pool_type,
            "fee_tier": str(request.fee_tier),
            "reserve_a": str(request.initial_liquidity_a),
            "reserve_b": str(request.initial_liquidity_b),
            "total_liquidity": str(request.initial_liquidity_a + request.initial_liquidity_b),
            "current_price": str(request.initial_liquidity_b / request.initial_liquidity_a),
            "volume_24h": "0",
            "fees_24h": "0",
            "created_at": datetime.utcnow().isoformat(),
            "creator": user_id,
            "compliance_enabled": request.compliance_enabled,
            "compliance_settings": {
                "min_kyc_tier": request.min_kyc_tier,
                "allowed_jurisdictions": request.allowed_jurisdictions or [],
                "blocked_jurisdictions": request.blocked_jurisdictions or []
            } if request.compliance_enabled else None
        }
        
        # Add pool-type specific data
        if request.pool_type == "concentrated":
            pool_data.update({
                "tick_spacing": request.tick_spacing or 60,
                "tick_current": 0,
                "liquidity_current": str(request.initial_liquidity_a * request.initial_liquidity_b).split('.')[0]
            })
        
        await pool_cache.put(pool_id, pool_data)
        
        # Update liquidity gauge
        liquidity_gauge.labels(
            pool_id=pool_id,
            pool_type=request.pool_type
        ).set(float(pool_data["total_liquidity"]))
        
        # Publish event
        await publish_event(
            EventType.POOL_CREATED,
            {
                "pool_id": pool_id,
                "pool_type": request.pool_type,
                "tokens": [request.token_a, request.token_b],
                "initial_liquidity": pool_data["total_liquidity"]
            },
            user_id=user_id
        )
        
        pool_operations.labels(
            operation='create',
            pool_type=request.pool_type,
            status='success'
        ).inc()
        
        return PoolResponse(
            pool_id=pool_id,
            token_a=request.token_a,
            token_b=request.token_b,
            pool_type=request.pool_type,
            fee_tier=str(request.fee_tier),
            reserve_a=pool_data["reserve_a"],
            reserve_b=pool_data["reserve_b"],
            total_liquidity=pool_data["total_liquidity"],
            current_price=pool_data["current_price"],
            volume_24h="0",
            fees_24h="0",
            apy="0",
            created_at=pool_data["created_at"],
            compliance_enabled=request.compliance_enabled,
            tick_current=pool_data.get("tick_current"),
            liquidity_current=pool_data.get("liquidity_current")
        )
        
    except Exception as e:
        pool_operations.labels(
            operation='create',
            pool_type=request.pool_type,
            status='error'
        ).inc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[PoolResponse])
async def list_pools(
    pool_type: Optional[str] = Query(None, pattern="^(constant_product|stableswap|concentrated)$"),
    token: Optional[str] = Query(None, description="Filter by token"),
    active_only: bool = Query(True, description="Show only active pools"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List available liquidity pools"""
    try:
        ignite = await get_ignite_client()
        pool_cache = await ignite.get_or_create_cache("pools")
        
        # In production, use proper SQL queries
        # For now, fetch all and filter in memory
        pools = []
        async for pool_id, pool_data in pool_cache.scan():
            # Apply filters
            if pool_type and pool_data["pool_type"] != pool_type:
                continue
            if token and token not in [pool_data["token_a"], pool_data["token_b"]]:
                continue
                
            # Calculate APY (simplified)
            volume_24h = float(pool_data.get("volume_24h", 0))
            total_liquidity = float(pool_data.get("total_liquidity", 1))
            fee_tier = float(pool_data.get("fee_tier", 0.003))
            
            daily_fees = volume_24h * fee_tier
            daily_apy = (daily_fees / total_liquidity) if total_liquidity > 0 else 0
            apy = daily_apy * 365 * 100  # Annualized percentage
            
            pools.append(PoolResponse(
                pool_id=pool_id,
                token_a=pool_data["token_a"],
                token_b=pool_data["token_b"],
                pool_type=pool_data["pool_type"],
                fee_tier=pool_data["fee_tier"],
                reserve_a=pool_data["reserve_a"],
                reserve_b=pool_data["reserve_b"],
                total_liquidity=pool_data["total_liquidity"],
                current_price=pool_data["current_price"],
                volume_24h=pool_data["volume_24h"],
                fees_24h=str(daily_fees),
                apy=f"{apy:.2f}",
                created_at=pool_data["created_at"],
                compliance_enabled=pool_data.get("compliance_enabled", False),
                tick_current=pool_data.get("tick_current"),
                liquidity_current=pool_data.get("liquidity_current")
            ))
        
        # Apply pagination
        pools = pools[offset:offset + limit]
        
        return pools
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{pool_id}", response_model=PoolResponse)
async def get_pool(pool_id: str):
    """Get pool details"""
    try:
        ignite = await get_ignite_client()
        pool_cache = await ignite.get_or_create_cache("pools")
        
        pool_data = await pool_cache.get(pool_id)
        if not pool_data:
            raise HTTPException(status_code=404, detail="Pool not found")
        
        # Calculate APY
        volume_24h = float(pool_data.get("volume_24h", 0))
        total_liquidity = float(pool_data.get("total_liquidity", 1))
        fee_tier = float(pool_data.get("fee_tier", 0.003))
        
        daily_fees = volume_24h * fee_tier
        daily_apy = (daily_fees / total_liquidity) if total_liquidity > 0 else 0
        apy = daily_apy * 365 * 100
        
        return PoolResponse(
            pool_id=pool_id,
            token_a=pool_data["token_a"],
            token_b=pool_data["token_b"],
            pool_type=pool_data["pool_type"],
            fee_tier=pool_data["fee_tier"],
            reserve_a=pool_data["reserve_a"],
            reserve_b=pool_data["reserve_b"],
            total_liquidity=pool_data["total_liquidity"],
            current_price=pool_data["current_price"],
            volume_24h=pool_data["volume_24h"],
            fees_24h=str(daily_fees),
            apy=f"{apy:.2f}",
            created_at=pool_data["created_at"],
            compliance_enabled=pool_data.get("compliance_enabled", False),
            tick_current=pool_data.get("tick_current"),
            liquidity_current=pool_data.get("liquidity_current")
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{pool_id}/add-liquidity")
async def add_liquidity(
    pool_id: str,
    request: AddLiquidityRequest,
    user_id: str = Depends(lambda: "mock_user")
):
    """Add liquidity to a pool"""
    try:
        pool_operations.labels(
            operation='add_liquidity',
            pool_type='unknown',  # Will be updated
            status='started'
        ).inc()
        
        # Get pool data
        ignite = await get_ignite_client()
        pool_cache = await ignite.get_or_create_cache("pools")
        
        pool_data = await pool_cache.get(pool_id)
        if not pool_data:
            raise HTTPException(status_code=404, detail="Pool not found")
        
        # Check compliance if enabled
        if pool_data.get("compliance_enabled"):
            # In production, verify user compliance
            pass
        
        # Update reserves (simplified - in production use proper AMM math)
        pool_data["reserve_a"] = str(Decimal(pool_data["reserve_a"]) + request.amount_a)
        pool_data["reserve_b"] = str(Decimal(pool_data["reserve_b"]) + request.amount_b)
        pool_data["total_liquidity"] = str(
            Decimal(pool_data["reserve_a"]) + Decimal(pool_data["reserve_b"])
        )
        
        await pool_cache.put(pool_id, pool_data)
        
        # Update metrics
        liquidity_gauge.labels(
            pool_id=pool_id,
            pool_type=pool_data["pool_type"]
        ).set(float(pool_data["total_liquidity"]))
        
        # Publish event
        await publish_event(
            EventType.LIQUIDITY_ADDED,
            {
                "pool_id": pool_id,
                "user_id": user_id,
                "amount_a": str(request.amount_a),
                "amount_b": str(request.amount_b),
                "new_total_liquidity": pool_data["total_liquidity"]
            },
            user_id=user_id
        )
        
        pool_operations.labels(
            operation='add_liquidity',
            pool_type=pool_data["pool_type"],
            status='success'
        ).inc()
        
        return {
            "success": True,
            "pool_id": pool_id,
            "new_reserve_a": pool_data["reserve_a"],
            "new_reserve_b": pool_data["reserve_b"],
            "lp_tokens_minted": str(request.amount_a + request.amount_b)  # Simplified
        }
        
    except HTTPException:
        raise
    except Exception as e:
        pool_operations.labels(
            operation='add_liquidity',
            pool_type='unknown',
            status='error'
        ).inc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{pool_id}/remove-liquidity")
async def remove_liquidity(
    pool_id: str,
    request: RemoveLiquidityRequest,
    user_id: str = Depends(lambda: "mock_user")
):
    """Remove liquidity from a pool"""
    try:
        pool_operations.labels(
            operation='remove_liquidity',
            pool_type='unknown',
            status='started'
        ).inc()
        
        # Get pool data
        ignite = await get_ignite_client()
        pool_cache = await ignite.get_or_create_cache("pools")
        
        pool_data = await pool_cache.get(pool_id)
        if not pool_data:
            raise HTTPException(status_code=404, detail="Pool not found")
        
        # Calculate amounts to remove (simplified)
        factor = request.liquidity_percentage / 100
        amount_a = Decimal(pool_data["reserve_a"]) * factor
        amount_b = Decimal(pool_data["reserve_b"]) * factor
        
        # Update reserves
        pool_data["reserve_a"] = str(Decimal(pool_data["reserve_a"]) - amount_a)
        pool_data["reserve_b"] = str(Decimal(pool_data["reserve_b"]) - amount_b)
        pool_data["total_liquidity"] = str(
            Decimal(pool_data["reserve_a"]) + Decimal(pool_data["reserve_b"])
        )
        
        await pool_cache.put(pool_id, pool_data)
        
        # Update metrics
        liquidity_gauge.labels(
            pool_id=pool_id,
            pool_type=pool_data["pool_type"]
        ).set(float(pool_data["total_liquidity"]))
        
        # Publish event
        await publish_event(
            EventType.LIQUIDITY_REMOVED,
            {
                "pool_id": pool_id,
                "user_id": user_id,
                "amount_a": str(amount_a),
                "amount_b": str(amount_b),
                "percentage": str(request.liquidity_percentage),
                "new_total_liquidity": pool_data["total_liquidity"]
            },
            user_id=user_id
        )
        
        pool_operations.labels(
            operation='remove_liquidity',
            pool_type=pool_data["pool_type"],
            status='success'
        ).inc()
        
        return {
            "success": True,
            "pool_id": pool_id,
            "amount_a_received": str(amount_a),
            "amount_b_received": str(amount_b),
            "new_reserve_a": pool_data["reserve_a"],
            "new_reserve_b": pool_data["reserve_b"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        pool_operations.labels(
            operation='remove_liquidity',
            pool_type='unknown',
            status='error'
        ).inc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{pool_id}/swap")
async def execute_swap(
    pool_id: str,
    request: SwapRequest,
    user_id: str = Depends(lambda: "mock_user")
):
    """Execute a swap in a pool"""
    try:
        pool_operations.labels(
            operation='swap',
            pool_type='unknown',
            status='started'
        ).inc()
        
        # Get pool data
        ignite = await get_ignite_client()
        pool_cache = await ignite.get_or_create_cache("pools")
        
        pool_data = await pool_cache.get(pool_id)
        if not pool_data:
            raise HTTPException(status_code=404, detail="Pool not found")
        
        # Determine which token is being swapped
        if request.token_in == pool_data["token_a"]:
            token_out = pool_data["token_b"]
            reserve_in = Decimal(pool_data["reserve_a"])
            reserve_out = Decimal(pool_data["reserve_b"])
        elif request.token_in == pool_data["token_b"]:
            token_out = pool_data["token_a"]
            reserve_in = Decimal(pool_data["reserve_b"])
            reserve_out = Decimal(pool_data["reserve_a"])
        else:
            raise HTTPException(status_code=400, detail="Invalid token_in")
        
        # Calculate output amount (constant product formula with fee)
        fee_tier = Decimal(pool_data["fee_tier"])
        amount_in_with_fee = request.amount_in * (1 - fee_tier)
        amount_out = (amount_in_with_fee * reserve_out) / (reserve_in + amount_in_with_fee)
        
        # Check slippage
        if amount_out < request.min_amount_out:
            raise HTTPException(status_code=400, detail="Slippage too high")
        
        # Update reserves
        if request.token_in == pool_data["token_a"]:
            pool_data["reserve_a"] = str(reserve_in + request.amount_in)
            pool_data["reserve_b"] = str(reserve_out - amount_out)
        else:
            pool_data["reserve_b"] = str(reserve_in + request.amount_in)
            pool_data["reserve_a"] = str(reserve_out - amount_out)
        
        # Update price
        pool_data["current_price"] = str(
            Decimal(pool_data["reserve_b"]) / Decimal(pool_data["reserve_a"])
        )
        
        # Update volume
        current_volume = Decimal(pool_data.get("volume_24h", "0"))
        pool_data["volume_24h"] = str(current_volume + request.amount_in)
        
        await pool_cache.put(pool_id, pool_data)
        
        # Update metrics
        swap_volume.labels(
            pool_id=pool_id,
            token_in=request.token_in,
            token_out=token_out
        ).inc(float(request.amount_in))
        
        fee_amount = request.amount_in * fee_tier
        fee_revenue.labels(
            pool_id=pool_id,
            fee_tier=str(fee_tier)
        ).inc(float(fee_amount))
        
        # Publish event
        await publish_event(
            EventType.SWAP_EXECUTED,
            {
                "pool_id": pool_id,
                "user_id": user_id,
                "token_in": request.token_in,
                "token_out": token_out,
                "amount_in": str(request.amount_in),
                "amount_out": str(amount_out),
                "fee_amount": str(fee_amount),
                "new_price": pool_data["current_price"]
            },
            user_id=user_id
        )
        
        pool_operations.labels(
            operation='swap',
            pool_type=pool_data["pool_type"],
            status='success'
        ).inc()
        
        return {
            "success": True,
            "pool_id": pool_id,
            "token_in": request.token_in,
            "token_out": token_out,
            "amount_in": str(request.amount_in),
            "amount_out": str(amount_out),
            "fee_amount": str(fee_amount),
            "execution_price": str(amount_out / request.amount_in),
            "new_price": pool_data["current_price"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        pool_operations.labels(
            operation='swap',
            pool_type='unknown',
            status='error'
        ).inc()
        raise HTTPException(status_code=500, detail=str(e)) 