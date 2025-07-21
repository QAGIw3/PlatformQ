"""
Insurance API endpoints for DeFi protocol service.
"""

import logging
from typing import Dict, Any, Optional, List
from decimal import Decimal
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from platformq_shared import get_current_user
from ..protocols.insurance import InsuranceProtocol
from ..models.insurance import RiskTier, ClaimStatus
from ..core.defi_manager import DEFI_TRANSACTIONS, TRANSACTION_LATENCY
import time

logger = logging.getLogger(__name__)

router = APIRouter()


class StakeLiquidityRequest(BaseModel):
    """Request model for staking liquidity in insurance pool"""
    chain: str = Field(..., description="Blockchain identifier")
    amount: float = Field(..., gt=0, description="Amount to stake")
    tier: str = Field(..., description="Risk tier: stable, balanced, or aggressive")
    lock_period_days: int = Field(0, ge=0, le=365, description="Optional lock period for bonus APY")


class InsuranceClaimRequest(BaseModel):
    """Request model for submitting insurance claim"""
    chain: str = Field(..., description="Blockchain identifier")
    claim_type: str = Field(..., description="Type of claim: liquidation, hack, impermanent_loss")
    reference_id: str = Field(..., description="Reference ID (loan_id, pool_id, etc)")
    amount: float = Field(..., gt=0, description="Amount to claim")
    evidence: Dict[str, Any] = Field(default_factory=dict, description="Supporting evidence")


class UnstakeRequest(BaseModel):
    """Request model for unstaking from insurance pool"""
    chain: str = Field(..., description="Blockchain identifier")
    position_id: str = Field(..., description="Stake position ID")
    amount: Optional[float] = Field(None, description="Amount to unstake (None for full)")


def get_insurance_protocol(request) -> InsuranceProtocol:
    """Dependency to get insurance protocol instance"""
    return request.app.state.insurance_protocol


@router.post("/stake")
async def stake_liquidity(
    request: StakeLiquidityRequest,
    current_user: Dict = Depends(get_current_user),
    insurance_protocol: InsuranceProtocol = Depends(get_insurance_protocol)
):
    """
    Stake liquidity in insurance pool for rewards.
    
    Choose from three risk tiers:
    - Stable: Low risk/reward (5% base APY)
    - Balanced: Medium risk/reward (12% base APY)
    - Aggressive: High risk/reward (25% base APY)
    
    Lock for longer periods to earn bonus APY.
    """
    try:
        start_time = time.time()
        
        # Convert tier string to enum
        try:
            tier = RiskTier(request.tier.lower())
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid tier. Must be one of: {[t.value for t in RiskTier]}"
            )
        
        # Stake liquidity
        result = await insurance_protocol.stake_liquidity(
            chain=request.chain,
            user=current_user["wallet_address"],
            amount=Decimal(str(request.amount)),
            tier=tier,
            lock_period_days=request.lock_period_days
        )
        
        # Track metrics
        duration = time.time() - start_time
        DEFI_TRANSACTIONS.labels(
            chain=request.chain,
            protocol="insurance",
            operation="stake"
        ).inc()
        TRANSACTION_LATENCY.labels(
            chain=request.chain,
            protocol="insurance"
        ).observe(duration)
        
        return {
            "position_id": result["position_id"],
            "transaction_hash": result["tx_hash"],
            "gas_used": result["gas_used"],
            "apy": result["apy"],
            "lock_until": result["lock_until"],
            "tier": tier.value
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error staking liquidity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/unstake")
async def unstake_liquidity(
    request: UnstakeRequest,
    current_user: Dict = Depends(get_current_user),
    insurance_protocol: InsuranceProtocol = Depends(get_insurance_protocol)
):
    """
    Unstake liquidity from insurance pool.
    
    Note: Positions cannot be unstaked while locked.
    """
    try:
        start_time = time.time()
        
        # Check if position is locked
        position = await insurance_protocol.get_position(
            request.position_id,
            current_user["wallet_address"]
        )
        
        if position and position.is_locked:
            raise HTTPException(
                status_code=400,
                detail=f"Position is locked until {position.lock_until.isoformat()}"
            )
        
        # Unstake
        result = await insurance_protocol.unstake_liquidity(
            chain=request.chain,
            user=current_user["wallet_address"],
            position_id=request.position_id,
            amount=Decimal(str(request.amount)) if request.amount else None
        )
        
        # Track metrics
        duration = time.time() - start_time
        DEFI_TRANSACTIONS.labels(
            chain=request.chain,
            protocol="insurance",
            operation="unstake"
        ).inc()
        TRANSACTION_LATENCY.labels(
            chain=request.chain,
            protocol="insurance"
        ).observe(duration)
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error unstaking liquidity: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/claim-rewards/{position_id}")
async def claim_rewards(
    position_id: str,
    chain: str,
    current_user: Dict = Depends(get_current_user),
    insurance_protocol: InsuranceProtocol = Depends(get_insurance_protocol)
):
    """
    Claim accumulated staking rewards.
    
    Rewards are calculated based on:
    - Pool APY (dynamic based on utilization)
    - Lock bonus (if position is locked)
    - Time staked
    """
    try:
        start_time = time.time()
        
        # Claim rewards
        result = await insurance_protocol.claim_rewards(
            chain=chain,
            user=current_user["wallet_address"],
            position_id=position_id
        )
        
        # Track metrics
        duration = time.time() - start_time
        DEFI_TRANSACTIONS.labels(
            chain=chain,
            protocol="insurance",
            operation="claim_rewards"
        ).inc()
        TRANSACTION_LATENCY.labels(
            chain=chain,
            protocol="insurance"
        ).observe(duration)
        
        return result
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error claiming rewards: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/claims/submit")
async def submit_insurance_claim(
    request: InsuranceClaimRequest,
    current_user: Dict = Depends(get_current_user),
    insurance_protocol: InsuranceProtocol = Depends(get_insurance_protocol)
):
    """
    Submit an insurance claim for coverage.
    
    Supported claim types:
    - liquidation: Cover losses from lending liquidations
    - hack: Cover losses from protocol hacks (requires governance approval)
    - impermanent_loss: Cover IL from liquidity provision
    """
    try:
        start_time = time.time()
        
        # Validate claim type
        valid_types = ["liquidation", "hack", "impermanent_loss"]
        if request.claim_type not in valid_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid claim type. Must be one of: {valid_types}"
            )
        
        # Submit claim
        result = await insurance_protocol.submit_claim(
            chain=request.chain,
            claimant=current_user["wallet_address"],
            claim_type=request.claim_type,
            reference_id=request.reference_id,
            amount=Decimal(str(request.amount)),
            evidence=request.evidence
        )
        
        # Track metrics
        duration = time.time() - start_time
        DEFI_TRANSACTIONS.labels(
            chain=request.chain,
            protocol="insurance",
            operation="submit_claim"
        ).inc()
        TRANSACTION_LATENCY.labels(
            chain=request.chain,
            protocol="insurance"
        ).observe(duration)
        
        return {
            "claim_id": result["claim_id"],
            "status": "submitted",
            "estimated_processing_time": "1-3 blocks for liquidations, 24-48 hours for other claims"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error submitting claim: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pools/stats")
async def get_pool_statistics(
    chain: Optional[str] = Query(None, description="Filter by chain"),
    insurance_protocol: InsuranceProtocol = Depends(get_insurance_protocol)
):
    """
    Get comprehensive insurance pool statistics.
    
    Returns TVL, APYs, utilization rates, and coverage metrics for each tier.
    """
    try:
        stats = await insurance_protocol.get_pool_stats(chain)
        
        # Add global metrics
        if not chain:
            total_tvl = Decimal("0")
            for chain_stats in stats.values():
                total_tvl += Decimal(chain_stats["total_tvl"])
            
            stats["global"] = {
                "total_tvl": str(total_tvl),
                "chains": len(stats)
            }
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting pool stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pools/apy")
async def get_current_apys(
    chain: str,
    insurance_protocol: InsuranceProtocol = Depends(get_insurance_protocol)
):
    """
    Get current APYs for all insurance pool tiers.
    
    APYs are dynamic and based on pool utilization.
    """
    try:
        apys = {}
        
        for tier in RiskTier:
            apy = await insurance_protocol.calculate_current_apy(chain, tier)
            apys[tier.value] = {
                "current_apy": str(apy),
                "base_apy": str(insurance_protocol.tiers[tier]["base_apy"]),
                "tier_config": {
                    "name": insurance_protocol.tiers[tier]["name"],
                    "min_stake": str(insurance_protocol.tiers[tier]["min_stake"]),
                    "max_leverage_covered": insurance_protocol.tiers[tier]["max_leverage_covered"]
                }
            }
        
        return apys
        
    except Exception as e:
        logger.error(f"Error getting APYs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/positions")
async def get_user_positions(
    chain: Optional[str] = Query(None, description="Filter by chain"),
    current_user: Dict = Depends(get_current_user),
    insurance_protocol: InsuranceProtocol = Depends(get_insurance_protocol)
):
    """
    Get user's insurance stake positions.
    """
    try:
        positions = await insurance_protocol.get_user_positions(
            user=current_user["wallet_address"],
            chain=chain
        )
        
        return {
            "positions": [
                {
                    "id": pos.id,
                    "chain": pos.chain,
                    "tier": pos.tier.value,
                    "amount": str(pos.amount),
                    "rewards_earned": str(pos.rewards_earned),
                    "staked_at": pos.staked_at.isoformat(),
                    "lock_until": pos.lock_until.isoformat() if pos.lock_until else None,
                    "is_locked": pos.is_locked,
                    "effective_apy": str(pos.effective_apy),
                    "current_value": str(pos.current_value)
                }
                for pos in positions
            ],
            "total_staked": str(sum(p.amount for p in positions)),
            "total_rewards": str(sum(p.rewards_earned for p in positions))
        }
        
    except Exception as e:
        logger.error(f"Error getting positions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/claims/{claim_id}")
async def get_claim_status(
    claim_id: str,
    insurance_protocol: InsuranceProtocol = Depends(get_insurance_protocol)
):
    """
    Get status and details of an insurance claim.
    """
    try:
        claim = await insurance_protocol.get_claim(claim_id)
        
        if not claim:
            raise HTTPException(status_code=404, detail="Claim not found")
        
        return {
            "id": claim.id,
            "status": claim.status.value,
            "claim_type": claim.claim_type,
            "amount_claimed": str(claim.amount_claimed),
            "amount_approved": str(claim.amount_approved) if claim.amount_approved else None,
            "submitted_at": claim.submitted_at.isoformat(),
            "processed_at": claim.processed_at.isoformat() if claim.processed_at else None,
            "processing_time": claim.processing_time,
            "processors": claim.processors
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting claim: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/coverage/available")
async def get_available_coverage(
    chain: str,
    market_type: str,
    leverage: int = Query(1, ge=1, le=100),
    insurance_protocol: InsuranceProtocol = Depends(get_insurance_protocol)
):
    """
    Check available insurance coverage for a market/leverage combination.
    """
    try:
        coverage = await insurance_protocol.get_available_coverage(
            chain=chain,
            market_type=market_type,
            leverage=leverage
        )
        
        return {
            "market_type": market_type,
            "leverage": leverage,
            "covering_pools": [
                {
                    "tier": tier.value,
                    "available_coverage": str(coverage[tier]),
                    "coverage_ratio": str(insurance_protocol.tiers[tier]["coverage_ratio"])
                }
                for tier in coverage.keys()
            ],
            "total_available": str(sum(coverage.values()))
        }
        
    except Exception as e:
        logger.error(f"Error getting coverage: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 