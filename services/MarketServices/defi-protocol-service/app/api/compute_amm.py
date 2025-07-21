"""
Compute Resource AMM API Endpoints

Provides RESTful API for the Automated Market Maker (AMM) protocol.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime, timedelta
from collections import defaultdict
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pydantic import BaseModel, Field

from ..dependencies import (
    get_compute_amm,
    verify_api_key,
    get_current_user
)
from ..protocols import (
    PoolType,
    SwapDirection
)

router = APIRouter(prefix="/api/v1/compute-amm", tags=["compute-amm"])


# Request models

class CreatePoolRequest(BaseModel):
    """Request to create a new liquidity pool"""
    token0: str = Field(..., description="First token address")
    token1: str = Field(..., description="Second token address")
    pool_type: PoolType = Field(PoolType.VOLATILE, description="Pool type")
    initial_price: Optional[Decimal] = Field(None, description="Initial price ratio (token1/token0)")
    fee_tier: Optional[Decimal] = Field(None, ge=0, le=0.1, description="Custom fee tier")


class AddLiquidityRequest(BaseModel):
    """Request to add liquidity to a pool"""
    pool_address: str = Field(..., description="Pool address")
    amount0: Decimal = Field(..., gt=0, description="Amount of token0")
    amount1: Decimal = Field(..., gt=0, description="Amount of token1")
    min_amount0: Optional[Decimal] = Field(None, ge=0, description="Minimum token0 to add")
    min_amount1: Optional[Decimal] = Field(None, ge=0, description="Minimum token1 to add")
    deadline_minutes: int = Field(10, ge=1, le=60, description="Transaction deadline in minutes")


class RemoveLiquidityRequest(BaseModel):
    """Request to remove liquidity from a pool"""
    pool_address: str = Field(..., description="Pool address")
    lp_tokens: Decimal = Field(..., gt=0, description="Amount of LP tokens to burn")
    min_amount0: Optional[Decimal] = Field(None, ge=0, description="Minimum token0 to receive")
    min_amount1: Optional[Decimal] = Field(None, ge=0, description="Minimum token1 to receive")
    deadline_minutes: int = Field(10, ge=1, le=60, description="Transaction deadline in minutes")


class SwapRequest(BaseModel):
    """Request to execute a swap"""
    token_in: str = Field(..., description="Token to swap from")
    token_out: str = Field(..., description="Token to swap to")
    amount: Decimal = Field(..., gt=0, description="Amount (input or output based on direction)")
    direction: SwapDirection = Field(SwapDirection.EXACT_IN, description="Swap direction")
    max_slippage: Decimal = Field(0.01, ge=0, le=0.1, description="Maximum slippage tolerance")
    deadline_minutes: int = Field(10, ge=1, le=60, description="Transaction deadline in minutes")


# Pool endpoints

@router.post("/pools/create")
async def create_pool(
    request: CreatePoolRequest,
    amm: Any = Depends(get_compute_amm),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Create a new liquidity pool"""
    try:
        result = await amm.create_pool(
            token0=request.token0,
            token1=request.token1,
            pool_type=request.pool_type,
            initial_price=request.initial_price,
            fee_tier=request.fee_tier
        )
        
        return {
            "success": True,
            "pool": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/pools/{pool_address}")
async def get_pool_info(
    pool_address: str,
    amm: Any = Depends(get_compute_amm)
) -> Dict[str, Any]:
    """Get detailed information about a pool"""
    try:
        return await amm.get_pool_info(pool_address)
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/pools")
async def list_pools(
    token0: Optional[str] = Query(None, description="Filter by token0"),
    token1: Optional[str] = Query(None, description="Filter by token1"),
    pool_type: Optional[PoolType] = Query(None, description="Filter by pool type"),
    min_tvl: Optional[Decimal] = Query(None, ge=0, description="Minimum TVL in USD"),
    amm: Any = Depends(get_compute_amm)
) -> Dict[str, Any]:
    """List all pools with optional filters"""
    try:
        # Get all pools
        all_pools = []
        
        for pool_address, pool_data in amm._pools.items():
            pool_info = await amm.get_pool_info(pool_address)
            
            # Apply filters
            if token0 and pool_info['token0'] != token0:
                continue
            if token1 and pool_info['token1'] != token1:
                continue
            if pool_type and pool_info['pool_type'] != pool_type:
                continue
            if min_tvl and pool_info['tvl_usd'] < min_tvl:
                continue
            
            all_pools.append(pool_info)
        
        # Sort by TVL
        all_pools.sort(key=lambda p: p['tvl_usd'], reverse=True)
        
        return {
            "pools": all_pools,
            "total": len(all_pools)
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Liquidity endpoints

@router.post("/liquidity/add")
async def add_liquidity(
    request: AddLiquidityRequest,
    amm: Any = Depends(get_compute_amm),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Add liquidity to a pool"""
    try:
        # Calculate deadline
        deadline = int((datetime.utcnow() + timedelta(minutes=request.deadline_minutes)).timestamp())
        
        result = await amm.add_liquidity(
            pool_address=request.pool_address,
            amount0=request.amount0,
            amount1=request.amount1,
            min_amount0=request.min_amount0,
            min_amount1=request.min_amount1,
            recipient=user['address'],
            deadline=deadline
        )
        
        return {
            "success": True,
            "liquidity": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/liquidity/remove")
async def remove_liquidity(
    request: RemoveLiquidityRequest,
    amm: Any = Depends(get_compute_amm),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Remove liquidity from a pool"""
    try:
        # Calculate deadline
        deadline = int((datetime.utcnow() + timedelta(minutes=request.deadline_minutes)).timestamp())
        
        result = await amm.remove_liquidity(
            pool_address=request.pool_address,
            lp_tokens=request.lp_tokens,
            min_amount0=request.min_amount0,
            min_amount1=request.min_amount1,
            recipient=user['address'],
            deadline=deadline
        )
        
        return {
            "success": True,
            "removal": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/liquidity/positions/{user_address}")
async def get_user_positions(
    user_address: str,
    amm: Any = Depends(get_compute_amm)
) -> Dict[str, Any]:
    """Get all liquidity positions for a user"""
    try:
        positions = await amm.get_user_positions(user_address)
        
        # Calculate totals
        total_value_usd = sum(p['current_value']['usd'] for p in positions)
        total_fees_earned = sum(p['estimated_fees_earned'] for p in positions)
        
        return {
            "positions": positions,
            "summary": {
                "total_positions": len(positions),
                "total_value_usd": total_value_usd,
                "total_fees_earned": total_fees_earned,
                "average_il": sum(p['impermanent_loss'] for p in positions) / len(positions) if positions else 0
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Swap endpoints

@router.post("/swap")
async def execute_swap(
    request: SwapRequest,
    amm: Any = Depends(get_compute_amm),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Execute a swap transaction"""
    try:
        # Calculate deadline
        deadline = int((datetime.utcnow() + timedelta(minutes=request.deadline_minutes)).timestamp())
        
        result = await amm.swap(
            token_in=request.token_in,
            token_out=request.token_out,
            amount=request.amount,
            direction=request.direction,
            max_slippage=request.max_slippage,
            recipient=user['address'],
            deadline=deadline
        )
        
        return {
            "success": True,
            "swap": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/swap/quote")
async def get_quote(
    token_in: str = Body(..., description="Token to swap from"),
    token_out: str = Body(..., description="Token to swap to"),
    amount_in: Decimal = Body(..., gt=0, description="Amount to swap"),
    amm: Any = Depends(get_compute_amm)
) -> Dict[str, Any]:
    """Get a swap quote without executing"""
    try:
        return await amm.get_quote(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in
        )
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/swap/routes")
async def find_swap_routes(
    token_in: str = Query(..., description="Token to swap from"),
    token_out: str = Query(..., description="Token to swap to"),
    amount_in: Decimal = Query(..., gt=0, description="Amount to swap"),
    max_hops: int = Query(3, ge=1, le=4, description="Maximum number of hops"),
    amm: Any = Depends(get_compute_amm)
) -> Dict[str, Any]:
    """Find all possible swap routes between two tokens"""
    try:
        # This would implement more complex routing logic
        # For now, return the standard quote
        quote = await amm.get_quote(
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in
        )
        
        routes = []
        if quote.get('path'):
            routes.append({
                'path': quote['path'],
                'pools': quote.get('pools', []),
                'amount_out': quote.get('amount_out', 0),
                'price_impact': quote.get('price_impact', 0),
                'gas_estimate': 150000 * len(quote.get('pools', []))  # Rough estimate
            })
        
        return {
            "routes": routes,
            "best_route": routes[0] if routes else None,
            "token_in": token_in,
            "token_out": token_out,
            "amount_in": amount_in
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Analytics endpoints

@router.get("/analytics/volume")
async def get_volume_analytics(
    period_hours: int = Query(24, ge=1, le=720, description="Period in hours"),
    pool_address: Optional[str] = Query(None, description="Filter by pool"),
    amm: Any = Depends(get_compute_amm)
) -> Dict[str, Any]:
    """Get trading volume analytics"""
    try:
        total_volume = Decimal("0")
        pool_volumes = []
        
        for address, pool_data in amm._pools.items():
            if pool_address and address != pool_address:
                continue
            
            pool_info = await amm.get_pool_info(address)
            
            pool_volumes.append({
                'pool_address': address,
                'token0': pool_info['token0'],
                'token1': pool_info['token1'],
                'volume_24h': pool_info['volume_24h'],
                'fees_24h': pool_info['fees_24h'],
                'utilization': pool_info['utilization']
            })
            
            total_volume += pool_info['volume_24h']
        
        # Sort by volume
        pool_volumes.sort(key=lambda p: p['volume_24h'], reverse=True)
        
        return {
            "period_hours": period_hours,
            "total_volume": total_volume,
            "pools": pool_volumes,
            "top_pool": pool_volumes[0] if pool_volumes else None
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/analytics/liquidity")
async def get_liquidity_analytics(
    amm: Any = Depends(get_compute_amm)
) -> Dict[str, Any]:
    """Get liquidity analytics across all pools"""
    try:
        total_tvl = Decimal("0")
        resource_tvls = defaultdict(Decimal)
        pool_stats = []
        
        for address in amm._pools:
            pool_info = await amm.get_pool_info(address)
            
            total_tvl += pool_info['tvl_usd']
            
            # Track TVL by resource type
            for token in [pool_info['token0'], pool_info['token1']]:
                if 'quantum' in token.lower():
                    resource_tvls['quantum'] += pool_info['tvl_usd'] / 2
                elif 'ai' in token.lower():
                    resource_tvls['ai'] += pool_info['tvl_usd'] / 2
                elif 'network' in token.lower():
                    resource_tvls['network'] += pool_info['tvl_usd'] / 2
            
            pool_stats.append({
                'pool_address': address,
                'tvl_usd': pool_info['tvl_usd'],
                'apr': pool_info['apr'],
                'utilization': pool_info['utilization']
            })
        
        # Calculate average metrics
        avg_apr = sum(p['apr'] for p in pool_stats) / len(pool_stats) if pool_stats else 0
        avg_utilization = sum(p['utilization'] for p in pool_stats) / len(pool_stats) if pool_stats else 0
        
        return {
            "total_tvl_usd": total_tvl,
            "tvl_by_resource": dict(resource_tvls),
            "pool_count": len(pool_stats),
            "average_apr": avg_apr,
            "average_utilization": avg_utilization * 100,  # As percentage
            "top_pools_by_tvl": sorted(pool_stats, key=lambda p: p['tvl_usd'], reverse=True)[:10]
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/analytics/price-impact")
async def analyze_price_impact(
    token_in: str = Query(..., description="Token to swap from"),
    token_out: str = Query(..., description="Token to swap to"),
    test_amounts: List[Decimal] = Query([100, 1000, 10000], description="Test amounts"),
    amm: Any = Depends(get_compute_amm)
) -> Dict[str, Any]:
    """Analyze price impact for different swap amounts"""
    try:
        impact_analysis = []
        
        for amount in test_amounts:
            quote = await amm.get_quote(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount
            )
            
            if quote.get('amount_out'):
                impact_analysis.append({
                    'amount_in': amount,
                    'amount_out': quote['amount_out'],
                    'execution_price': quote['execution_price'],
                    'price_impact': quote['price_impact'],
                    'route': quote.get('route', 'Direct')
                })
        
        return {
            "token_in": token_in,
            "token_out": token_out,
            "impact_analysis": impact_analysis,
            "recommendation": "Use smaller amounts" if any(
                a['price_impact'] > 5 for a in impact_analysis
            ) else "Price impact acceptable"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Health check

@router.get("/health")
async def amm_health(
    amm: Any = Depends(get_compute_amm)
) -> Dict[str, Any]:
    """Check AMM health and statistics"""
    
    try:
        pool_count = len(amm._pools)
        total_tvl = Decimal("0")
        total_volume = Decimal("0")
        
        for pool in amm._pools.values():
            info = await amm.get_pool_info(pool['address'])
            total_tvl += info['tvl_usd']
            total_volume += info['volume_24h']
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow(),
            "statistics": {
                "pool_count": pool_count,
                "total_tvl_usd": float(total_tvl),
                "volume_24h": float(total_volume),
                "active_pools": pool_count
            }
        }
        
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": datetime.utcnow()
        } 