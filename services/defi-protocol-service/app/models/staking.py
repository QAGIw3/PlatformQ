"""
Staking Models

Data models for resource staking and delegation.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field

from platformq_shared.models import ResourceType, ServiceTier


class StakeStatus(str, Enum):
    """Status of a stake"""
    ACTIVE = "active"
    UNLOCKED = "unlocked"
    WITHDRAWN = "withdrawn"
    SLASHED = "slashed"


class StakingPool(BaseModel):
    """Staking pool information"""
    pool_id: int
    token_id: int
    total_staked: int
    reward_per_token: int
    min_stake_amount: int
    is_lp: bool
    lp_token_address: Optional[str] = None
    created_at: datetime
    last_update: Optional[datetime] = None
    reward_rate: Optional[int] = None
    period_finish: Optional[datetime] = None


class DelegationPool(BaseModel):
    """Delegation pool information"""
    pool_id: int
    operator: str
    total_delegated: int
    operator_fee: int  # Basis points
    min_delegation: int
    accepting_delegations: bool
    metadata: str
    performance_score: int  # 0-100
    created_at: datetime
    last_slash_time: Optional[datetime] = None


class UserStake(BaseModel):
    """User stake information"""
    stake_id: int
    user: str
    pool_id: int
    amount: int
    lock_end_time: datetime
    status: StakeStatus
    created_at: datetime
    last_claim_time: Optional[datetime] = None
    is_delegated: bool = False
    delegation_pool_id: Optional[int] = None
    withdrawn_at: Optional[datetime] = None
    slashed_amount: Optional[int] = None


# Request/Response Models

class CreateStakingPoolRequest(BaseModel):
    """Request to create a staking pool"""
    token_id: int = Field(..., description="Resource token ID (0 for LP)")
    min_stake_amount: int = Field(..., gt=0)
    is_lp: bool = Field(default=False)
    lp_token_address: Optional[str] = None


class CreateDelegationPoolRequest(BaseModel):
    """Request to create a delegation pool"""
    operator_fee: int = Field(..., ge=0, le=2000, description="Fee in basis points (max 20%)")
    min_delegation: int = Field(..., gt=0)
    metadata: str = Field(..., max_length=1000, description="Pool description/strategy")


class StakeRequest(BaseModel):
    """Request to stake tokens"""
    pool_id: int = Field(..., ge=1)
    amount: int = Field(..., gt=0)
    lock_duration: int = Field(..., ge=86400, description="Lock duration in seconds (min 1 day)")


class DelegateStakeRequest(BaseModel):
    """Request to delegate a stake"""
    stake_id: int = Field(..., ge=1)
    delegation_pool_id: int = Field(..., ge=1)


class WithdrawRequest(BaseModel):
    """Request to withdraw stake"""
    stake_id: int = Field(..., ge=1)


class ClaimRewardsRequest(BaseModel):
    """Request to claim rewards"""
    stake_id: int = Field(..., ge=1)


class AutoCompoundRequest(BaseModel):
    """Request to enable/disable auto-compound"""
    enable: bool


class ExecuteCompoundRequest(BaseModel):
    """Request to execute compound"""
    user_address: str
    stake_ids: List[int]


class UpdateDelegationFeeRequest(BaseModel):
    """Request to update delegation pool fee"""
    pool_id: int = Field(..., ge=1)
    new_fee: int = Field(..., ge=0, le=2000)


# Response Models

class StakingPoolResponse(BaseModel):
    """Staking pool details"""
    pool_id: int
    token_id: int
    total_staked: int
    min_stake_amount: int
    is_lp: bool
    lp_token_address: Optional[str]
    reward_rate: int
    apy: float
    total_rewards: int


class DelegationPoolInfo(BaseModel):
    """Delegation pool information"""
    pool_id: int
    operator: str
    total_delegated: int
    operator_fee: int
    min_delegation: int
    accepting_delegations: bool
    metadata: str
    performance_score: int
    total_rewards_earned: int
    delegator_count: int


class UserStakeResponse(BaseModel):
    """User stake details"""
    stake_id: int
    pool_id: int
    amount: int
    lock_end_time: datetime
    status: str
    is_delegated: bool
    delegation_pool_id: Optional[int]
    pending_rewards: int
    claimable: bool
    time_until_unlock: Optional[int]


class StakingStats(BaseModel):
    """Overall staking statistics"""
    total_staked: int
    total_rewards_distributed: int
    active_stakers: int
    total_pools: int
    total_delegation_pools: int
    average_apy: float


class StakeResponse(BaseModel):
    """Response for stake operation"""
    stake_id: int
    tx_hash: str
    amount: int
    lock_end_time: str
    estimated_apy: float


class DelegateResponse(BaseModel):
    """Response for delegation operation"""
    tx_hash: str
    stake_id: int
    delegation_pool_id: int
    operator_fee: int
    operator_address: str


class WithdrawResponse(BaseModel):
    """Response for withdrawal operation"""
    tx_hash: str
    amount: int
    rewards_claimed: int


class ClaimRewardsResponse(BaseModel):
    """Response for claim rewards operation"""
    tx_hash: str
    rewards: int
    claimed_at: str


class CompoundResponse(BaseModel):
    """Response for compound operation"""
    tx_hash: str
    total_compounded: int
    stakes_compounded: int
    new_balances: Dict[int, int]  # stake_id -> new amount


class SlashingEvent(BaseModel):
    """Slashing event details"""
    stake_id: int
    user: str
    amount_slashed: int
    reason: str
    timestamp: datetime
    delegation_pool_id: Optional[int] 