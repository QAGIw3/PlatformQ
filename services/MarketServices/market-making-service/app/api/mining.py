"""Liquidity Mining API endpoints"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime, timedelta
from enum import Enum

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.dependencies import get_ignite_client, get_redis_client, get_service_clients
from app.core.events import publish_event, EventType
from app.monitoring import mining_rewards, liquidity_gauge
from app.config import settings

router = APIRouter()


class RewardToken(str, Enum):
    """Available reward tokens"""
    PLATFORM = "PLATFORM"
    USDC = "USDC"
    ETH = "ETH"
    CUSTOM = "CUSTOM"


class ProgramStatus(str, Enum):
    """Mining program status"""
    PENDING = "pending"
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class CreateMiningProgramRequest(BaseModel):
    """Request to create liquidity mining program"""
    pool_id: str = Field(..., description="Target pool ID")
    duration_days: int = Field(..., ge=7, le=365, description="Program duration")
    total_rewards: Decimal = Field(..., gt=0, description="Total rewards to distribute")
    reward_token: RewardToken = Field(..., description="Token used for rewards")
    
    # Weight distribution
    volume_weight: Decimal = Field(default=0.4, ge=0, le=1, description="Weight for volume")
    depth_weight: Decimal = Field(default=0.3, ge=0, le=1, description="Weight for depth")
    uptime_weight: Decimal = Field(default=0.3, ge=0, le=1, description="Weight for uptime")
    
    # Optional parameters
    min_liquidity: Optional[Decimal] = Field(None, gt=0, description="Minimum liquidity to qualify")
    boost_multipliers: Optional[Dict[str, Decimal]] = Field(None, description="Boost factors")
    start_delay_hours: int = Field(default=0, ge=0, le=168, description="Hours before start")


class ClaimRewardsRequest(BaseModel):
    """Request to claim rewards"""
    program_ids: Optional[List[str]] = Field(None, description="Specific programs to claim from")
    recipient: Optional[str] = Field(None, description="Alternative recipient address")


class MiningProgramResponse(BaseModel):
    """Mining program information"""
    program_id: str
    pool_id: str
    status: str
    total_rewards: str
    distributed_rewards: str
    remaining_rewards: str
    reward_token: str
    start_time: str
    end_time: str
    participants: int
    current_apy: str
    weights: Dict[str, str]
    created_at: str


@router.post("/programs", response_model=MiningProgramResponse)
async def create_mining_program(
    request: CreateMiningProgramRequest,
    user_id: str = Depends(lambda: "mock_user")
):
    """Create a new liquidity mining program"""
    try:
        # Validate weights sum to 1
        total_weight = request.volume_weight + request.depth_weight + request.uptime_weight
        if abs(total_weight - 1) > 0.001:
            raise HTTPException(status_code=400, detail="Weights must sum to 1")
        
        # Generate program ID
        program_id = f"mining_{request.pool_id}_{int(datetime.utcnow().timestamp())}"
        
        # Calculate timing
        start_time = datetime.utcnow() + timedelta(hours=request.start_delay_hours)
        end_time = start_time + timedelta(days=request.duration_days)
        
        # Store program data
        ignite = await get_ignite_client()
        program_cache = await ignite.get_or_create_cache("mining_programs")
        
        program_data = {
            "program_id": program_id,
            "pool_id": request.pool_id,
            "status": ProgramStatus.PENDING.value if request.start_delay_hours > 0 else ProgramStatus.ACTIVE.value,
            "creator": user_id,
            "total_rewards": str(request.total_rewards),
            "distributed_rewards": "0",
            "remaining_rewards": str(request.total_rewards),
            "reward_token": request.reward_token.value,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "participants": 0,
            "weights": {
                "volume": str(request.volume_weight),
                "depth": str(request.depth_weight),
                "uptime": str(request.uptime_weight)
            },
            "min_liquidity": str(request.min_liquidity) if request.min_liquidity else None,
            "boost_multipliers": request.boost_multipliers,
            "created_at": datetime.utcnow().isoformat(),
            "daily_rate": str(request.total_rewards / request.duration_days)
        }
        
        await program_cache.put(program_id, program_data)
        
        # Calculate initial APY (simplified)
        # In production, would fetch pool TVL
        estimated_tvl = Decimal("1000000")  # Mock $1M TVL
        annual_rewards = (request.total_rewards / request.duration_days) * 365
        current_apy = (annual_rewards / estimated_tvl) * 100
        
        # Publish event
        await publish_event(
            EventType.MINING_PROGRAM_CREATED,
            {
                "program_id": program_id,
                "pool_id": request.pool_id,
                "total_rewards": str(request.total_rewards),
                "duration_days": request.duration_days,
                "reward_token": request.reward_token.value
            },
            user_id=user_id
        )
        
        return MiningProgramResponse(
            program_id=program_id,
            pool_id=request.pool_id,
            status=program_data["status"],
            total_rewards=program_data["total_rewards"],
            distributed_rewards="0",
            remaining_rewards=program_data["remaining_rewards"],
            reward_token=request.reward_token.value,
            start_time=program_data["start_time"],
            end_time=program_data["end_time"],
            participants=0,
            current_apy=f"{current_apy:.2f}",
            weights=program_data["weights"],
            created_at=program_data["created_at"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/programs", response_model=List[MiningProgramResponse])
async def list_mining_programs(
    pool_id: Optional[str] = Query(None, description="Filter by pool"),
    status: Optional[ProgramStatus] = Query(None, description="Filter by status"),
    active_only: bool = Query(True, description="Show only active programs"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List liquidity mining programs"""
    try:
        ignite = await get_ignite_client()
        program_cache = await ignite.get_or_create_cache("mining_programs")
        
        programs = []
        async for prog_id, prog_data in program_cache.scan():
            # Apply filters
            if pool_id and prog_data["pool_id"] != pool_id:
                continue
            if status and prog_data["status"] != status.value:
                continue
            if active_only and prog_data["status"] != ProgramStatus.ACTIVE.value:
                continue
            
            # Calculate current APY (mock)
            remaining = Decimal(prog_data["remaining_rewards"])
            if prog_data["status"] == ProgramStatus.ACTIVE.value and remaining > 0:
                days_left = (datetime.fromisoformat(prog_data["end_time"]) - datetime.utcnow()).days
                if days_left > 0:
                    annual_rate = (remaining / days_left) * 365
                    current_apy = (annual_rate / Decimal("1000000")) * 100  # Mock TVL
                else:
                    current_apy = 0
            else:
                current_apy = 0
            
            programs.append(MiningProgramResponse(
                program_id=prog_id,
                pool_id=prog_data["pool_id"],
                status=prog_data["status"],
                total_rewards=prog_data["total_rewards"],
                distributed_rewards=prog_data["distributed_rewards"],
                remaining_rewards=prog_data["remaining_rewards"],
                reward_token=prog_data["reward_token"],
                start_time=prog_data["start_time"],
                end_time=prog_data["end_time"],
                participants=prog_data.get("participants", 0),
                current_apy=f"{current_apy:.2f}",
                weights=prog_data["weights"],
                created_at=prog_data["created_at"]
            ))
        
        # Apply pagination
        programs = programs[offset:offset + limit]
        
        return programs
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/rewards/{user_id}")
async def get_user_rewards(
    user_id: str,
    program_id: Optional[str] = Query(None, description="Filter by program"),
    include_claimed: bool = Query(False, description="Include already claimed rewards")
):
    """Get user's mining rewards"""
    try:
        ignite = await get_ignite_client()
        rewards_cache = await ignite.get_or_create_cache("user_rewards")
        
        # Get user rewards key
        user_key = f"rewards_{user_id}"
        user_rewards = await rewards_cache.get(user_key) or {}
        
        rewards_list = []
        total_unclaimed = Decimal("0")
        
        for prog_id, reward_data in user_rewards.items():
            if program_id and prog_id != program_id:
                continue
            
            if not include_claimed and reward_data.get("claimed", False):
                continue
            
            rewards_list.append({
                "program_id": prog_id,
                "earned": reward_data["earned"],
                "claimed": reward_data.get("claimed_amount", "0"),
                "unclaimed": str(Decimal(reward_data["earned"]) - Decimal(reward_data.get("claimed_amount", "0"))),
                "reward_token": reward_data.get("reward_token", "PLATFORM"),
                "last_update": reward_data.get("last_update", datetime.utcnow().isoformat())
            })
            
            if not reward_data.get("claimed", False):
                total_unclaimed += Decimal(reward_data["earned"]) - Decimal(reward_data.get("claimed_amount", "0"))
        
        return {
            "user_id": user_id,
            "rewards": rewards_list,
            "total_unclaimed": str(total_unclaimed),
            "total_earned": str(sum(Decimal(r["earned"]) for r in rewards_list))
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/claim")
async def claim_rewards(
    request: ClaimRewardsRequest,
    user_id: str = Depends(lambda: "mock_user")
):
    """Claim accumulated rewards"""
    try:
        ignite = await get_ignite_client()
        rewards_cache = await ignite.get_or_create_cache("user_rewards")
        
        # Get user rewards
        user_key = f"rewards_{user_id}"
        user_rewards = await rewards_cache.get(user_key) or {}
        
        if not user_rewards:
            raise HTTPException(status_code=404, detail="No rewards to claim")
        
        # Filter programs to claim
        programs_to_claim = request.program_ids or list(user_rewards.keys())
        
        total_claimed = {}
        claimed_programs = []
        
        for prog_id in programs_to_claim:
            if prog_id not in user_rewards:
                continue
                
            reward_data = user_rewards[prog_id]
            earned = Decimal(reward_data["earned"])
            already_claimed = Decimal(reward_data.get("claimed_amount", "0"))
            to_claim = earned - already_claimed
            
            if to_claim <= 0:
                continue
            
            # Update claimed amount
            reward_data["claimed_amount"] = str(earned)
            reward_data["last_claimed"] = datetime.utcnow().isoformat()
            
            token = reward_data.get("reward_token", "PLATFORM")
            total_claimed[token] = total_claimed.get(token, Decimal("0")) + to_claim
            claimed_programs.append(prog_id)
            
            # Update metrics
            mining_rewards.labels(
                program_id=prog_id,
                token=token
            ).inc(float(to_claim))
        
        if not claimed_programs:
            raise HTTPException(status_code=400, detail="No unclaimed rewards")
        
        # Save updated rewards
        await rewards_cache.put(user_key, user_rewards)
        
        # Publish event
        await publish_event(
            EventType.REWARDS_CLAIMED,
            {
                "user_id": user_id,
                "programs": claimed_programs,
                "amounts": {k: str(v) for k, v in total_claimed.items()},
                "recipient": request.recipient or user_id
            },
            user_id=user_id
        )
        
        return {
            "success": True,
            "claimed": {k: str(v) for k, v in total_claimed.items()},
            "programs": claimed_programs,
            "recipient": request.recipient or user_id,
            "tx_hash": "0x" + "b" * 64  # Mock transaction hash
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/leaderboard/{program_id}")
async def get_mining_leaderboard(
    program_id: str,
    metric: str = Query("total_earned", pattern="^(total_earned|volume|liquidity|score)$"),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get mining program leaderboard"""
    try:
        # In production, aggregate from actual participation data
        # For now, return mock leaderboard
        leaderboard = []
        
        for i in range(min(10, limit)):
            leaderboard.append({
                "rank": i + 1,
                "user_id": f"user_{i + 1}",
                "address": "0x" + f"{i+1:040x}",
                "metrics": {
                    "total_earned": str(10000 - (i * 1000)),
                    "volume_24h": str(50000 - (i * 5000)),
                    "liquidity_provided": str(100000 - (i * 10000)),
                    "uptime_percent": 99.5 - (i * 0.5),
                    "score": 1000 - (i * 100)
                },
                "boost_multiplier": "1.0",
                "share_percent": f"{(10 - i):.2f}"
            })
        
        return {
            "program_id": program_id,
            "metric": metric,
            "leaderboard": leaderboard,
            "total_participants": 100,
            "updated_at": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_mining_stats():
    """Get overall mining statistics"""
    try:
        ignite = await get_ignite_client()
        program_cache = await ignite.get_or_create_cache("mining_programs")
        
        # Aggregate statistics
        total_programs = 0
        active_programs = 0
        total_distributed = Decimal("0")
        total_remaining = Decimal("0")
        
        async for prog_id, prog_data in program_cache.scan():
            total_programs += 1
            if prog_data["status"] == ProgramStatus.ACTIVE.value:
                active_programs += 1
            total_distributed += Decimal(prog_data["distributed_rewards"])
            total_remaining += Decimal(prog_data["remaining_rewards"])
        
        return {
            "total_programs": total_programs,
            "active_programs": active_programs,
            "total_value_locked": "10000000",  # Mock TVL
            "total_rewards_distributed": str(total_distributed),
            "total_rewards_remaining": str(total_remaining),
            "unique_participants": 1234,  # Mock
            "average_apy": "45.67",  # Mock
            "top_pools": [
                {
                    "pool_id": "ETH_USDC_constant_product",
                    "tvl": "5000000",
                    "apy": "65.43",
                    "participants": 456
                }
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 