"""
Compliant Pools API

Endpoints for creating and managing DeFi pools with compliance requirements.
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.integrations.compliance_bridge import (
    ComplianceBridge,
    ComplianceConfig,
    ComplianceLevel,
    PoolRestriction
)
from app.models.liquidity_pool import PoolConfig
from app.auth import get_current_user
from platformq_shared import ProcessingStatus

router = APIRouter(
    prefix="/api/v1/compliant-pools",
    tags=["compliant-pools"]
)

# Initialize compliance bridge
compliance_bridge = ComplianceBridge()


class CreateCompliantPoolRequest(BaseModel):
    """Request to create a compliant liquidity pool"""
    token_a: str
    token_b: str
    fee_tier: Decimal = Field(..., ge=0.0001, le=0.1)
    initial_liquidity_a: Decimal = Field(..., gt=0)
    initial_liquidity_b: Decimal = Field(..., gt=0)
    concentrated_liquidity: bool = True
    
    # Compliance requirements
    min_kyc_tier: int = Field(..., ge=1, le=3)
    allowed_jurisdictions: List[str] = Field(default_factory=list)
    blocked_jurisdictions: List[str] = Field(default_factory=list)
    max_position_size: Optional[Decimal] = Field(None, gt=0)
    require_accredited: bool = False
    cooldown_hours: Optional[int] = Field(None, ge=0)
    require_zkp: bool = True


class CheckComplianceRequest(BaseModel):
    """Request to check user compliance for pool action"""
    pool_id: str
    action: str = Field(..., pattern="^(deposit|withdraw|swap|provide_liquidity)$")
    amount: Optional[Decimal] = Field(None, gt=0)


class JurisdictionPoolsRequest(BaseModel):
    """Request to create pools for multiple jurisdictions"""
    token_a: str
    token_b: str
    fee_tier: Decimal = Field(..., ge=0.0001, le=0.1)
    initial_liquidity_a: Decimal = Field(..., gt=0)
    initial_liquidity_b: Decimal = Field(..., gt=0)
    jurisdictions: List[str] = Field(..., min_items=1, max_items=10)


@router.post("/create")
async def create_compliant_pool(
    request: CreateCompliantPoolRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create a new DeFi pool with compliance requirements
    
    This creates a liquidity pool that automatically enforces KYC and
    jurisdiction requirements for all participants.
    """
    try:
        # Create pool configuration
        pool_config = PoolConfig(
            token_a=request.token_a,
            token_b=request.token_b,
            fee_tier=request.fee_tier,
            initial_liquidity_a=request.initial_liquidity_a,
            initial_liquidity_b=request.initial_liquidity_b,
            concentrated_liquidity=request.concentrated_liquidity,
            metadata={
                "created_by": current_user["user_id"],
                "compliance_enabled": True
            }
        )
        
        # Create compliance configuration
        compliance_config = ComplianceConfig(
            min_kyc_tier=request.min_kyc_tier,
            allowed_jurisdictions=request.allowed_jurisdictions,
            blocked_jurisdictions=request.blocked_jurisdictions,
            max_position_size=request.max_position_size,
            require_accredited=request.require_accredited,
            cooldown_period=timedelta(hours=request.cooldown_hours) if request.cooldown_hours else None,
            require_zkp=request.require_zkp
        )
        
        # Create compliant pool
        result = await compliance_bridge.create_compliant_pool(
            tenant_id=current_user["tenant_id"],
            pool_config=pool_config,
            compliance_config=compliance_config
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "pool": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/check-compliance")
async def check_user_compliance(
    request: CheckComplianceRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Check if current user meets compliance requirements for pool action
    
    Returns detailed compliance check results including KYC status,
    jurisdiction checks, and position limits.
    """
    try:
        result = await compliance_bridge.check_user_compliance(
            tenant_id=current_user["tenant_id"],
            user_id=current_user["user_id"],
            pool_id=request.pool_id,
            action=request.action,
            amount=request.amount
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "compliance": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jurisdiction-pools")
async def create_jurisdiction_pools(
    request: JurisdictionPoolsRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create separate compliant pools for different jurisdictions
    
    This automatically creates pools with appropriate compliance
    requirements for each specified jurisdiction.
    """
    try:
        # Create base pool configuration
        base_config = PoolConfig(
            token_a=request.token_a,
            token_b=request.token_b,
            fee_tier=request.fee_tier,
            initial_liquidity_a=request.initial_liquidity_a,
            initial_liquidity_b=request.initial_liquidity_b,
            concentrated_liquidity=True,
            metadata={
                "created_by": current_user["user_id"],
                "multi_jurisdiction": True
            }
        )
        
        # Create pools for each jurisdiction
        result = await compliance_bridge.create_jurisdiction_pools(
            tenant_id=current_user["tenant_id"],
            base_config=base_config,
            jurisdictions=request.jurisdictions
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "result": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pool/{pool_id}/requirements")
async def get_pool_requirements(
    pool_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get compliance requirements for a specific pool
    """
    try:
        requirements = await compliance_bridge._get_pool_requirements(
            tenant_id=current_user["tenant_id"],
            pool_id=pool_id
        )
        
        return {
            "status": "success",
            "pool_id": pool_id,
            "requirements": requirements
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/report/{pool_id}")
async def generate_compliance_report(
    pool_id: str,
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Generate compliance report for a pool
    
    Returns detailed compliance analytics including transaction counts,
    jurisdiction breakdown, and suspicious activity flags.
    """
    try:
        result = await compliance_bridge.generate_compliance_report(
            tenant_id=current_user["tenant_id"],
            pool_id=pool_id,
            start_date=start_date,
            end_date=end_date
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "report": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jurisdiction-requirements/{jurisdiction}")
async def get_jurisdiction_requirements(
    jurisdiction: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get regulatory requirements for a specific jurisdiction
    """
    try:
        requirements = await compliance_bridge._get_jurisdiction_requirements(
            jurisdiction.upper()
        )
        
        return {
            "status": "success",
            "jurisdiction": jurisdiction.upper(),
            "requirements": requirements
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/user/compliance-status")
async def get_user_compliance_status(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current user's compliance status
    """
    try:
        status = await compliance_bridge._get_user_compliance_status(
            tenant_id=current_user["tenant_id"],
            user_id=current_user["user_id"]
        )
        
        # Check if user has ZKP credentials
        has_zkp = {}
        for tier in [1, 2, 3]:
            has_zkp[f"tier_{tier}"] = await compliance_bridge._check_zkp_credentials(
                tenant_id=current_user["tenant_id"],
                user_id=current_user["user_id"],
                required_tier=tier
            )
        
        return {
            "status": "success",
            "user_id": current_user["user_id"],
            "compliance_status": status,
            "zkp_credentials": has_zkp
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/available-pools")
async def get_available_pools(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get all pools available to the current user based on their compliance status
    """
    try:
        # Get user's compliance status
        user_compliance = await compliance_bridge._get_user_compliance_status(
            tenant_id=current_user["tenant_id"],
            user_id=current_user["user_id"]
        )
        
        # This would query all pools and filter based on user's compliance
        # For now, return mock data
        available_pools = [
            {
                "pool_id": "COMPLIANT_WETH_USDC_1234",
                "tokens": ["WETH", "USDC"],
                "min_kyc_tier": 1,
                "jurisdictions": ["US", "UK", "EU"],
                "user_eligible": user_compliance.get("kyc_tier", 0) >= 1
            },
            {
                "pool_id": "COMPLIANT_WBTC_USDT_5678",
                "tokens": ["WBTC", "USDT"],
                "min_kyc_tier": 2,
                "jurisdictions": ["US"],
                "require_accredited": True,
                "user_eligible": (
                    user_compliance.get("kyc_tier", 0) >= 2 and
                    user_compliance.get("is_accredited", False)
                )
            }
        ]
        
        # Filter to only eligible pools
        eligible_pools = [p for p in available_pools if p["user_eligible"]]
        
        return {
            "status": "success",
            "total_pools": len(available_pools),
            "eligible_pools": len(eligible_pools),
            "pools": eligible_pools,
            "user_kyc_tier": user_compliance.get("kyc_tier", 0),
            "user_jurisdiction": user_compliance.get("jurisdiction", "unknown")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 