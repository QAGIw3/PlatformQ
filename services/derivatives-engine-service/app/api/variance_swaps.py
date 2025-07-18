"""
Variance Swaps API

Endpoints for trading variance and volatility swaps.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field

from app.auth import get_current_user
from app.engines.variance_swap_engine import VarianceSwapEngine, VarianceSwap, VolatilitySwap

router = APIRouter(
    prefix="/api/v1/variance-swaps",
    tags=["variance_swaps"]
)

# Initialize engine (would be injected in practice)
variance_engine = None


class CreateVarianceSwapRequest(BaseModel):
    """Request to create a variance swap"""
    underlying: str = Field(..., description="Underlying asset")
    notional: Decimal = Field(..., gt=0, description="Vega notional")
    strike_volatility: Decimal = Field(..., gt=0, le=5, description="Strike vol (0-500%)")
    maturity_days: int = Field(..., ge=7, le=365, description="Days to maturity")
    observation_frequency: str = Field(default="daily", pattern="^(daily|hourly|tick)$")
    side: str = Field(..., pattern="^(buy|sell)$", description="Buy = long vol, Sell = short vol")


class CreateVolatilitySwapRequest(BaseModel):
    """Request to create a volatility swap"""
    underlying: str = Field(..., description="Underlying asset")
    notional: Decimal = Field(..., gt=0, description="Direct vol notional")
    strike_volatility: Decimal = Field(..., gt=0, le=5, description="Strike vol")
    maturity_days: int = Field(..., ge=7, le=365)
    side: str = Field(..., pattern="^(buy|sell)$")


@router.post("/variance/create")
async def create_variance_swap(
    request: CreateVarianceSwapRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create a new variance swap
    """
    try:
        # Determine counterparty (would match with another user in practice)
        if request.side == "buy":
            buyer_id = current_user["sub"]
            seller_id = "market_maker"  # Automated market maker
        else:
            buyer_id = "market_maker"
            seller_id = current_user["sub"]
            
        swap = await variance_engine.create_variance_swap(
            buyer_id=buyer_id,
            seller_id=seller_id,
            underlying=request.underlying,
            notional=request.notional,
            strike_volatility=request.strike_volatility,
            maturity_days=request.maturity_days,
            observation_frequency=request.observation_frequency
        )
        
        # Calculate initial margin
        margin = await variance_engine._calculate_initial_margin(swap)
        user_margin = margin["buyer"] if request.side == "buy" else margin["seller"]
        
        return {
            "success": True,
            "swap_id": swap.swap_id,
            "details": {
                "underlying": swap.underlying,
                "notional": str(swap.notional),
                "strike_volatility": f"{swap.strike_volatility:.2%}",
                "strike_variance": f"{swap.strike_variance:.4f}",
                "maturity": swap.maturity_date.isoformat(),
                "observation_frequency": swap.observation_frequency,
                "side": request.side
            },
            "margin_requirement": str(user_margin),
            "estimated_daily_observations": 1 if request.observation_frequency == "daily" else 24
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create variance swap: {e}")
        raise HTTPException(status_code=500, detail="Failed to create swap")


@router.post("/volatility/create")
async def create_volatility_swap(
    request: CreateVolatilitySwapRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create a new volatility swap with convexity adjustment
    """
    try:
        if request.side == "buy":
            buyer_id = current_user["sub"]
            seller_id = "market_maker"
        else:
            buyer_id = "market_maker"
            seller_id = current_user["sub"]
            
        vol_swap = await variance_engine.create_volatility_swap(
            buyer_id=buyer_id,
            seller_id=seller_id,
            underlying=request.underlying,
            notional=request.notional,
            strike_volatility=request.strike_volatility,
            maturity_days=request.maturity_days
        )
        
        return {
            "success": True,
            "swap_id": vol_swap.swap_id,
            "variance_swap_id": vol_swap.variance_swap_id,
            "details": {
                "underlying": vol_swap.underlying,
                "notional": str(vol_swap.notional),
                "strike_volatility": f"{vol_swap.strike_volatility:.2%}",
                "convexity_adjustment": f"{vol_swap.convexity_adjustment:.4f}",
                "effective_strike": f"{(vol_swap.strike_volatility + vol_swap.convexity_adjustment):.2%}",
                "maturity": vol_swap.maturity_date.isoformat(),
                "side": request.side
            }
        }
        
    except Exception as e:
        logger.error(f"Failed to create volatility swap: {e}")
        raise HTTPException(status_code=500, detail="Failed to create swap")


@router.get("/market-overview")
async def get_variance_swap_market_overview(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get overview of variance swap market
    """
    overview = await variance_engine.get_market_overview()
    
    return {
        "market_stats": overview,
        "popular_underlyings": [
            {"asset": "BTC", "avg_strike_vol": "75%", "avg_realized_vol": "72%"},
            {"asset": "ETH", "avg_strike_vol": "85%", "avg_realized_vol": "80%"},
            {"asset": "SOL", "avg_strike_vol": "95%", "avg_realized_vol": "88%"}
        ],
        "vol_term_structure": {
            "7d": {"strike": "70%", "bid": "68%", "ask": "72%"},
            "30d": {"strike": "75%", "bid": "73%", "ask": "77%"},
            "90d": {"strike": "80%", "bid": "78%", "ask": "82%"}
        }
    }


@router.get("/swap/{swap_id}")
async def get_swap_details(
    swap_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get details of a specific swap
    """
    details = await variance_engine.get_swap_details(swap_id)
    
    if not details:
        raise HTTPException(status_code=404, detail="Swap not found")
        
    # Add real-time mark-to-market
    swap = variance_engine.variance_swaps.get(swap_id)
    if swap:
        current_pnl = await variance_engine._calculate_swap_pnl(swap)
        details["mark_to_market"] = {
            "current_pnl": str(current_pnl),
            "pnl_percentage": f"{(current_pnl / swap.notional):.2%}",
            "last_update": datetime.utcnow().isoformat()
        }
        
    return details


@router.get("/positions")
async def get_user_variance_positions(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get user's variance swap positions
    """
    user_id = current_user["sub"]
    positions = []
    
    # Get variance swaps
    for swap_id, swap in variance_engine.variance_swaps.items():
        if swap.buyer_id == user_id or swap.seller_id == user_id:
            side = "buy" if swap.buyer_id == user_id else "sell"
            current_pnl = await variance_engine._calculate_swap_pnl(swap)
            
            # Adjust P&L sign based on side
            if side == "sell":
                current_pnl = -current_pnl
                
            positions.append({
                "swap_id": swap_id,
                "type": "variance",
                "underlying": swap.underlying,
                "side": side,
                "notional": str(swap.notional),
                "strike_volatility": f"{swap.strike_volatility:.2%}",
                "realized_volatility": f"{np.sqrt(float(swap.realized_variance)):.2%}",
                "current_pnl": str(current_pnl),
                "time_to_maturity": (swap.maturity_date - datetime.utcnow()).days,
                "is_active": swap.is_active
            })
            
    # Calculate portfolio statistics
    total_pnl = sum(Decimal(p["current_pnl"]) for p in positions)
    total_notional = sum(
        variance_engine.variance_swaps[p["swap_id"]].notional 
        for p in positions
    )
    
    return {
        "positions": positions,
        "portfolio_summary": {
            "total_positions": len(positions),
            "total_notional": str(total_notional),
            "total_pnl": str(total_pnl),
            "average_strike_vol": f"{np.mean([float(p['strike_volatility'].rstrip('%')) for p in positions]):.1f}%",
            "average_realized_vol": f"{np.mean([float(p['realized_volatility'].rstrip('%')) for p in positions]):.1f}%"
        }
    }


@router.get("/historical-volatility/{underlying}")
async def get_historical_volatility(
    underlying: str,
    lookback_days: int = Query(default=30, ge=7, le=365),
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get historical volatility for an underlying
    """
    hist_vol = await variance_engine._get_historical_volatility(underlying, lookback_days)
    vol_of_vol = await variance_engine._get_vol_of_vol(underlying, lookback_days)
    
    return {
        "underlying": underlying,
        "lookback_days": lookback_days,
        "historical_volatility": f"{hist_vol:.2%}",
        "volatility_of_volatility": f"{vol_of_vol:.2%}",
        "percentile_ranks": {
            "30d": "75th",  # Mock data
            "90d": "60th",
            "1y": "50th"
        },
        "volatility_regime": "medium" if hist_vol < Decimal("0.8") else "high"
    }


@router.post("/close/{swap_id}")
async def close_swap_early(
    swap_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Close a swap early (novation to market maker)
    """
    swap = variance_engine.variance_swaps.get(swap_id)
    if not swap:
        raise HTTPException(status_code=404, detail="Swap not found")
        
    user_id = current_user["sub"]
    if swap.buyer_id != user_id and swap.seller_id != user_id:
        raise HTTPException(status_code=403, detail="Not your swap")
        
    if not swap.is_active:
        raise HTTPException(status_code=400, detail="Swap already settled")
        
    # Calculate current value
    current_pnl = await variance_engine._calculate_swap_pnl(swap)
    
    # Novation fee (1% of notional)
    novation_fee = swap.notional * Decimal("0.01")
    
    # Net amount to receive/pay
    if swap.buyer_id == user_id:
        net_amount = current_pnl - novation_fee
    else:
        net_amount = -current_pnl - novation_fee
        
    # Mark as inactive (would process novation in practice)
    swap.is_active = False
    
    return {
        "success": True,
        "swap_id": swap_id,
        "settlement": {
            "current_pnl": str(current_pnl),
            "novation_fee": str(novation_fee),
            "net_amount": str(net_amount),
            "settlement_time": datetime.utcnow().isoformat()
        }
    }


@router.get("/implied-variance-curve/{underlying}")
async def get_implied_variance_curve(
    underlying: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get implied variance term structure
    """
    # In practice, would calculate from option prices
    return {
        "underlying": underlying,
        "curve_date": datetime.utcnow().isoformat(),
        "term_structure": [
            {"days": 7, "implied_variance": 0.49, "implied_vol": "70%"},
            {"days": 30, "implied_variance": 0.5625, "implied_vol": "75%"},
            {"days": 60, "implied_variance": 0.6084, "implied_vol": "78%"},
            {"days": 90, "implied_variance": 0.64, "implied_vol": "80%"},
            {"days": 180, "implied_variance": 0.6724, "implied_vol": "82%"}
        ],
        "atm_skew": {
            "30d": -0.02,  # Negative skew
            "90d": -0.015
        }
    }


import logging
import numpy as np

logger = logging.getLogger(__name__) 