"""
Staking API endpoints

Handles resource token staking, delegation, and rewards.
"""

from typing import List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status
from datetime import datetime

from platformq_shared.auth import get_current_user
from ..dependencies import get_staking_protocol
from ..models import (
    CreateStakingPoolRequest, CreateDelegationPoolRequest,
    StakeRequest, DelegateStakeRequest, StakeWithdrawRequest,
    ClaimRewardsRequest, AutoCompoundRequest, ExecuteCompoundRequest,
    UpdateDelegationFeeRequest, StakingPoolResponse, DelegationPoolInfo,
    UserStakeResponse, StakingStats, StakeResponse, DelegateResponse,
    StakeWithdrawResponse, ClaimRewardsResponse, CompoundResponse
)
from ..protocols.staking_protocol import StakingProtocol

router = APIRouter(prefix="/staking", tags=["staking"])


@router.post("/pools", response_model=Dict[str, Any])
async def create_staking_pool(
    request: CreateStakingPoolRequest,
    current_user: dict = Depends(get_current_user),
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """
    Create a new staking pool.
    
    Requires OPERATOR role.
    """
    # Check permissions
    if "OPERATOR" not in current_user.get("roles", []):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operator role required"
        )
    
    try:
        result = await protocol.create_staking_pool(
            token_id=request.token_id,
            min_stake_amount=request.min_stake_amount,
            is_lp=request.is_lp,
            lp_token_address=request.lp_token_address,
            operator_address=current_user["wallet_address"]
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/delegation-pools", response_model=Dict[str, Any])
async def create_delegation_pool(
    request: CreateDelegationPoolRequest,
    current_user: dict = Depends(get_current_user),
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Create a delegation pool for professional operators."""
    try:
        result = await protocol.create_delegation_pool(
            operator_address=current_user["wallet_address"],
            operator_fee=request.operator_fee,
            min_delegation=request.min_delegation,
            metadata=request.metadata
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/stake", response_model=StakeResponse)
async def stake_tokens(
    request: StakeRequest,
    current_user: dict = Depends(get_current_user),
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Stake tokens in a pool."""
    try:
        result = await protocol.stake(
            user_address=current_user["wallet_address"],
            pool_id=request.pool_id,
            amount=request.amount,
            lock_duration=request.lock_duration
        )
        
        # Get pool info for APY
        pool_stats = await protocol._get_pool_info(request.pool_id)
        
        return StakeResponse(
            stake_id=result["stake_id"],
            tx_hash=result["tx_hash"],
            amount=result["amount"],
            lock_end_time=result["lock_end_time"],
            estimated_apy=8.5  # Mock APY for now
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/delegate", response_model=DelegateResponse)
async def delegate_stake(
    request: DelegateStakeRequest,
    current_user: dict = Depends(get_current_user),
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Delegate a stake to an operator pool."""
    try:
        result = await protocol.delegate_stake(
            user_address=current_user["wallet_address"],
            stake_id=request.stake_id,
            delegation_pool_id=request.delegation_pool_id
        )
        
        # Get delegation pool info
        pool_info = await protocol._get_delegation_pool_info(request.delegation_pool_id)
        
        return DelegateResponse(
            tx_hash=result["tx_hash"],
            stake_id=result["stake_id"],
            delegation_pool_id=result["delegation_pool_id"],
            operator_fee=result["operator_fee"],
            operator_address=pool_info["operator"]
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/withdraw", response_model=StakeWithdrawResponse)
async def withdraw_stake(
    request: StakeWithdrawRequest,
    current_user: dict = Depends(get_current_user),
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Withdraw staked tokens."""
    try:
        result = await protocol.withdraw(
            user_address=current_user["wallet_address"],
            stake_id=request.stake_id
        )
        
        return StakeWithdrawResponse(
            tx_hash=result["tx_hash"],
            amount=result["amount"],
            rewards_claimed=result.get("rewards_claimed", 0)
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/claim-rewards", response_model=ClaimRewardsResponse)
async def claim_rewards(
    request: ClaimRewardsRequest,
    current_user: dict = Depends(get_current_user),
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Claim rewards for a stake."""
    try:
        result = await protocol.claim_rewards(
            user_address=current_user["wallet_address"],
            stake_id=request.stake_id
        )
        
        return ClaimRewardsResponse(
            tx_hash=result["tx_hash"],
            rewards=result["rewards"],
            claimed_at=result["claimed_at"]
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/auto-compound", response_model=Dict[str, Any])
async def set_auto_compound(
    request: AutoCompoundRequest,
    current_user: dict = Depends(get_current_user),
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Enable or disable auto-compounding."""
    try:
        result = await protocol.enable_auto_compound(
            user_address=current_user["wallet_address"],
            enable=request.enable
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.post("/compound", response_model=CompoundResponse)
async def execute_compound(
    request: ExecuteCompoundRequest,
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """
    Execute auto-compound for a user.
    
    This is typically called by keepers or automation.
    """
    try:
        result = await protocol.execute_auto_compound(
            user_address=request.user_address,
            stake_ids=request.stake_ids
        )
        
        # Get new balances
        new_balances = {}
        for stake_id in request.stake_ids:
            stakes = await protocol.get_user_stakes(request.user_address)
            for stake in stakes:
                if stake["stake_id"] == stake_id:
                    new_balances[stake_id] = stake["amount"]
                    break
        
        return CompoundResponse(
            tx_hash=result["tx_hash"],
            total_compounded=result["total_compounded"],
            stakes_compounded=result["stakes_compounded"],
            new_balances=new_balances
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.put("/delegation-pools/{pool_id}/fee", response_model=Dict[str, Any])
async def update_delegation_fee(
    pool_id: int,
    request: UpdateDelegationFeeRequest,
    current_user: dict = Depends(get_current_user),
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Update delegation pool operator fee."""
    try:
        result = await protocol.update_delegation_pool_fee(
            operator_address=current_user["wallet_address"],
            pool_id=pool_id,
            new_fee=request.new_fee
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )


@router.get("/stats", response_model=StakingStats)
async def get_staking_stats(
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Get overall staking statistics."""
    try:
        stats = await protocol.get_staking_stats()
        return stats
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/pools", response_model=List[StakingPoolResponse])
async def get_staking_pools(
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Get all staking pools."""
    # Mock response for now
    pools = []
    for pool_id in range(1, 5):
        try:
            pool_info = await protocol._get_pool_info(pool_id)
            pools.append(StakingPoolResponse(
                pool_id=pool_id,
                token_id=pool_info["token_id"],
                total_staked=pool_info["total_staked"],
                min_stake_amount=pool_info["min_stake_amount"],
                is_lp=pool_info["is_lp"],
                lp_token_address=pool_info.get("lp_token_address"),
                reward_rate=1000,  # Mock
                apy=8.5,  # Mock
                total_rewards=100000  # Mock
            ))
        except:
            break
    
    return pools


@router.get("/delegation-pools", response_model=List[DelegationPoolInfo])
async def get_delegation_pools(
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Get all delegation pools."""
    try:
        pools = await protocol.get_delegation_pools()
        return pools
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/user/stakes", response_model=List[UserStakeResponse])
async def get_user_stakes(
    current_user: dict = Depends(get_current_user),
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Get all stakes for the current user."""
    try:
        stakes = await protocol.get_user_stakes(current_user["wallet_address"])
        
        # Convert to response format
        responses = []
        for stake in stakes:
            time_until_unlock = None
            if stake["lock_end_time"] > datetime.utcnow():
                time_until_unlock = int(
                    (stake["lock_end_time"] - datetime.utcnow()).total_seconds()
                )
            
            responses.append(UserStakeResponse(
                stake_id=stake["stake_id"],
                pool_id=stake["pool_id"],
                amount=stake["amount"],
                lock_end_time=stake["lock_end_time"],
                status="active" if time_until_unlock else "unlocked",
                is_delegated=stake["is_delegated"],
                delegation_pool_id=stake.get("delegation_pool_id"),
                pending_rewards=stake["pending_rewards"],
                claimable=stake["pending_rewards"] > 0,
                time_until_unlock=time_until_unlock
            ))
        
        return responses
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/user/{user_address}/stakes", response_model=List[UserStakeResponse])
async def get_user_stakes_by_address(
    user_address: str,
    protocol: StakingProtocol = Depends(get_staking_protocol)
):
    """Get all stakes for a specific user."""
    try:
        stakes = await protocol.get_user_stakes(user_address)
        
        # Convert to response format
        responses = []
        for stake in stakes:
            time_until_unlock = None
            if stake["lock_end_time"] > datetime.utcnow():
                time_until_unlock = int(
                    (stake["lock_end_time"] - datetime.utcnow()).total_seconds()
                )
            
            responses.append(UserStakeResponse(
                stake_id=stake["stake_id"],
                pool_id=stake["pool_id"],
                amount=stake["amount"],
                lock_end_time=stake["lock_end_time"],
                status="active" if time_until_unlock else "unlocked",
                is_delegated=stake["is_delegated"],
                delegation_pool_id=stake.get("delegation_pool_id"),
                pending_rewards=stake["pending_rewards"],
                claimable=stake["pending_rewards"] > 0,
                time_until_unlock=time_until_unlock
            ))
        
        return responses
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) 