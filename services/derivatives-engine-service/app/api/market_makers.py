"""
Market Maker API

Endpoints for market maker incentives, rewards, and liquidity mining programs.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field

from app.liquidity.market_maker_incentives import (
    MarketMakerIncentives,
    MakerTier
)
from app.models.order import Order
from app.models.market import Market
from app.auth import get_current_user
from app.integrations import IgniteCache as IgniteClient, PulsarEventPublisher as PulsarClient

router = APIRouter(
    prefix="/api/v1/market-makers",
    tags=["market-makers"]
)

# Initialize market maker incentives
ignite_client = IgniteClient()
pulsar_client = PulsarClient()
mm_incentives = MarketMakerIncentives(ignite_client, pulsar_client)


class LiquidityMiningProgramRequest(BaseModel):
    """Request to create liquidity mining program"""
    market_id: str = Field(..., description="Market ID for the program")
    duration_days: int = Field(..., ge=7, le=365, description="Program duration in days")
    total_rewards: Decimal = Field(..., gt=0, description="Total rewards to distribute")
    reward_token: str = Field(..., description="Token used for rewards")
    volume_weight: Decimal = Field(default=Decimal("0.4"), ge=0, le=1)
    depth_weight: Decimal = Field(default=Decimal("0.3"), ge=0, le=1)
    uptime_weight: Decimal = Field(default=Decimal("0.3"), ge=0, le=1)


@router.post("/register")
async def register_market_maker(
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Register as a market maker"""
    try:
        # Initialize maker metrics if not exists
        if user.user_id not in mm_incentives.maker_metrics:
            # Create initial metrics (will be done in record_maker_order)
            return {
                "maker_id": user.user_id,
                "status": "registered",
                "initial_tier": MakerTier.BRONZE.value,
                "tier_requirements": mm_incentives.tier_requirements[MakerTier.BRONZE].__dict__
            }
        else:
            # Already registered
            stats = await mm_incentives.get_maker_stats(user.user_id)
            return {
                "maker_id": user.user_id,
                "status": "already_registered",
                "current_tier": stats["current_tier"],
                "metrics": stats["metrics"]
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_maker_stats(
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Get current stats for authenticated market maker"""
    try:
        stats = await mm_incentives.get_maker_stats(user.user_id)
        
        if not stats:
            raise HTTPException(
                status_code=404,
                detail="Market maker not found. Please register first."
            )
        
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/{maker_id}")
async def get_maker_stats_by_id(
    maker_id: str,
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Get stats for a specific market maker (public data only)"""
    try:
        stats = await mm_incentives.get_maker_stats(maker_id)
        
        if not stats:
            raise HTTPException(
                status_code=404,
                detail="Market maker not found"
            )
        
        # Return public data only
        return {
            "maker_id": maker_id,
            "current_tier": stats["current_tier"],
            "metrics": {
                "volume_24h": stats["metrics"]["volume_24h"],
                "volume_30d": stats["metrics"]["volume_30d"],
                "depth_score": stats["metrics"]["depth_score"],
                "uptime_percent": stats["metrics"]["uptime_percent"]
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/leaderboard")
async def get_leaderboard(
    metric: str = Query(
        default="volume_30d",
        regex="^(volume_24h|volume_30d|depth_score|uptime_percent|total_rebates_earned|total_bonuses_earned)$"
    ),
    limit: int = Query(default=10, ge=1, le=100)
) -> Dict[str, Any]:
    """Get market maker leaderboard"""
    try:
        leaderboard = await mm_incentives.get_leaderboard(metric, limit)
        
        return {
            "metric": metric,
            "leaderboard": leaderboard,
            "timestamp": datetime.now()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tiers")
async def get_tier_requirements() -> Dict[str, Any]:
    """Get requirements for all market maker tiers"""
    try:
        tiers = {}
        for tier, requirements in mm_incentives.tier_requirements.items():
            tiers[tier.value] = {
                "min_volume_30d": str(requirements.min_volume_30d),
                "min_depth_score": str(requirements.min_depth_score),
                "min_uptime_percent": str(requirements.min_uptime_percent),
                "maker_rebate_bps": str(requirements.maker_rebate_bps),
                "bonus_multiplier": str(requirements.bonus_multiplier)
            }
        
        return {
            "tiers": tiers,
            "tier_order": [
                MakerTier.BRONZE.value,
                MakerTier.SILVER.value,
                MakerTier.GOLD.value,
                MakerTier.PLATINUM.value,
                MakerTier.DIAMOND.value
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/liquidity-mining/create")
async def create_liquidity_mining_program(
    request: LiquidityMiningProgramRequest,
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Create a new liquidity mining program (admin only)"""
    try:
        # Check admin privileges
        if not user.is_admin:
            raise HTTPException(status_code=403, detail="Admin access required")
        
        # Validate weights sum to 1
        weight_sum = request.volume_weight + request.depth_weight + request.uptime_weight
        if abs(weight_sum - Decimal("1")) > Decimal("0.01"):
            raise HTTPException(
                status_code=400,
                detail="Weights must sum to 1.0"
            )
        
        result = await mm_incentives.create_liquidity_mining_program(
            market_id=request.market_id,
            duration_days=request.duration_days,
            total_rewards=request.total_rewards,
            reward_token=request.reward_token
        )
        
        if result.status.value != "completed":
            raise HTTPException(
                status_code=400,
                detail=result.error or "Failed to create program"
            )
        
        return {
            "program_id": result.data["program_id"],
            "market_id": request.market_id,
            "start_time": datetime.now(),
            "end_time": datetime.now().timestamp() + (request.duration_days * 86400),
            "total_rewards": str(request.total_rewards),
            "reward_token": request.reward_token
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/liquidity-mining/programs")
async def get_liquidity_mining_programs(
    active_only: bool = Query(default=True),
    market_id: Optional[str] = None
) -> Dict[str, Any]:
    """Get liquidity mining programs"""
    try:
        programs = []
        current_time = datetime.now()
        
        for program_id, program in mm_incentives.mining_programs.items():
            # Filter by active status
            if active_only and program.end_time < current_time:
                continue
            
            # Filter by market
            if market_id and program.market_id != market_id:
                continue
            
            programs.append({
                "program_id": program_id,
                "market_id": program.market_id,
                "start_time": program.start_time,
                "end_time": program.end_time,
                "total_rewards": str(program.total_rewards),
                "distributed_rewards": str(program.distributed_rewards),
                "remaining_rewards": str(program.total_rewards - program.distributed_rewards),
                "reward_token": program.reward_token,
                "participants": len(program.participants),
                "is_active": program.end_time > current_time
            })
        
        return {
            "programs": programs,
            "total_programs": len(programs)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/liquidity-mining/rewards")
async def get_mining_rewards(
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Get liquidity mining rewards for authenticated user"""
    try:
        rewards = []
        total_pending = Decimal("0")
        total_claimed = Decimal("0")
        
        # Check each program
        for program_id, program in mm_incentives.mining_programs.items():
            if user.user_id in program.participants:
                # Calculate user's share (simplified)
                # In production, would track actual contributions
                user_share = Decimal("0.1")  # Mock 10% share
                program_rewards = program.distributed_rewards * user_share
                
                rewards.append({
                    "program_id": program_id,
                    "market_id": program.market_id,
                    "reward_token": program.reward_token,
                    "pending_rewards": str(program_rewards),
                    "claimed_rewards": "0",  # Would track claims
                    "last_update": datetime.now()
                })
                
                total_pending += program_rewards
        
        return {
            "rewards": rewards,
            "total_pending": str(total_pending),
            "total_claimed": str(total_claimed)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/start")
async def start_market_maker_incentives(
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Start market maker incentives system (admin only)"""
    try:
        # Check admin privileges
        if not user.is_admin:
            raise HTTPException(status_code=403, detail="Admin access required")
        
        await mm_incentives.start()
        
        return {
            "status": "started",
            "tiers": len(mm_incentives.tier_requirements),
            "active_makers": len(mm_incentives.maker_metrics)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stop")
async def stop_market_maker_incentives(
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Stop market maker incentives system (admin only)"""
    try:
        # Check admin privileges
        if not user.is_admin:
            raise HTTPException(status_code=403, detail="Admin access required")
        
        await mm_incentives.stop()
        
        return {
            "status": "stopped"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/performance/snapshot")
async def get_performance_snapshot(
    timestamp: Optional[datetime] = None
) -> Dict[str, Any]:
    """Get market maker performance snapshot"""
    try:
        if timestamp:
            # Find closest snapshot
            closest_snapshot = None
            min_diff = float('inf')
            
            for snapshot in mm_incentives.performance_snapshots:
                diff = abs((snapshot["timestamp"] - timestamp).total_seconds())
                if diff < min_diff:
                    min_diff = diff
                    closest_snapshot = snapshot
            
            if not closest_snapshot:
                raise HTTPException(
                    status_code=404,
                    detail="No snapshot found near requested time"
                )
            
            return closest_snapshot
        else:
            # Return latest snapshot
            if not mm_incentives.performance_snapshots:
                raise HTTPException(
                    status_code=404,
                    detail="No snapshots available"
                )
            
            return mm_incentives.performance_snapshots[-1]
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 