"""
Compute Resource DeFi API endpoints

Provides endpoints for vaults, lending, and derivatives on compute resources.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime, timedelta
from fastapi import APIRouter, HTTPException, Depends, Query, Body
from pydantic import BaseModel, Field

from ..dependencies import (
    get_compute_vault_protocol,
    get_compute_lending_protocol,
    get_compute_derivatives_protocol,
    get_compute_insurance_protocol,
    verify_api_key,
    get_current_user
)
from ..protocols import (
    ComputeResourceType,
    ComputeStrategyType,
    ComputeCollateralType,
    ComputeLoanType,
    ComputeDerivativeType,
    ComputeOptionType,
    InsuranceCoverageType,
    ClaimStatus,
    RiskLevel
)

router = APIRouter(prefix="/api/v1/compute-defi", tags=["compute-defi"])


# Request models

class CreateComputeVaultRequest(BaseModel):
    """Request to create a compute resource vault"""
    resource_type: ComputeResourceType
    name: str = Field(..., min_length=3, max_length=50)
    symbol: str = Field(..., min_length=2, max_length=10)
    strategies: List[ComputeStrategyType]
    management_fee: int = Field(200, ge=0, le=1000)  # Basis points
    performance_fee: int = Field(1500, ge=0, le=5000)  # Basis points
    min_deposit: int = Field(100, gt=0)


class DepositComputeResourcesRequest(BaseModel):
    """Request to deposit compute resources into vault"""
    vault_address: str
    resource_ids: List[int]
    amounts: List[int]
    lock_period_days: int = Field(0, ge=0, le=365)


class BorrowComputeRequest(BaseModel):
    """Request to borrow compute resources"""
    pool_address: str
    resource_ids: List[int]
    amounts: List[int]
    duration_hours: int = Field(..., gt=0, le=8760)
    collateral_type: ComputeCollateralType
    collateral_amount: Decimal = Field(..., gt=0)
    loan_type: ComputeLoanType = ComputeLoanType.SPOT_COMPUTE


class CreateComputeFutureRequest(BaseModel):
    """Request to create compute future contract"""
    resource_type: str
    resource_specs: Dict[str, Any]
    quantity: int = Field(..., gt=0)
    delivery_days: int = Field(..., gt=0, le=365)
    settlement_type: str = Field("physical", regex="^(physical|cash)$")


class CreateComputeOptionRequest(BaseModel):
    """Request to create compute option contract"""
    resource_type: str
    resource_specs: Dict[str, Any]
    option_type: ComputeOptionType
    strike_price: Decimal = Field(..., gt=0)
    quantity: int = Field(..., gt=0)
    expiration_days: int = Field(..., gt=0, le=365)
    american_style: bool = False


class HedgePortfolioRequest(BaseModel):
    """Request to hedge compute portfolio"""
    portfolio: List[Dict[str, Any]]
    hedge_objective: str = Field("delta_neutral", regex="^(delta_neutral|vega_neutral|tail_risk)$")
    constraints: Optional[Dict[str, Any]] = None


class CreateInsurancePoolRequest(BaseModel):
    """Request to create insurance pool"""
    resource_type: str
    coverage_type: InsuranceCoverageType
    initial_capital: Decimal = Field(..., gt=0)
    target_size: Decimal = Field(..., gt=0)
    reserve_ratio: Decimal = Field(0.2, ge=0.1, le=0.5)


class PurchasePolicyRequest(BaseModel):
    """Request to purchase insurance policy"""
    pool_id: str
    resource_ids: List[int]
    coverage_amount: Decimal = Field(..., gt=0)
    coverage_period_days: int = Field(..., gt=0, le=365)
    deductible_override: Optional[Decimal] = Field(None, ge=0, le=0.5)
    bundle_discount: bool = False


class FileClaimRequest(BaseModel):
    """Request to file insurance claim"""
    policy_id: str
    claim_type: str
    incident_data: Dict[str, Any]
    requested_amount: Decimal = Field(..., gt=0)
    evidence_hashes: List[str] = Field(default_factory=list)


# Vault endpoints

@router.post("/vaults/create")
async def create_compute_vault(
    request: CreateComputeVaultRequest,
    vault_protocol: Any = Depends(get_compute_vault_protocol),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Create a new compute resource vault"""
    try:
        result = await vault_protocol.create_compute_vault(
            resource_type=request.resource_type,
            name=request.name,
            symbol=request.symbol,
            strategies=request.strategies,
            management_fee=request.management_fee,
            performance_fee=request.performance_fee,
            min_deposit=request.min_deposit
        )
        
        return {
            "success": True,
            "vault": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/vaults/deposit")
async def deposit_compute_resources(
    request: DepositComputeResourcesRequest,
    vault_protocol: Any = Depends(get_compute_vault_protocol),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Deposit compute resources into vault"""
    try:
        if len(request.resource_ids) != len(request.amounts):
            raise ValueError("Resource IDs and amounts length mismatch")
        
        result = await vault_protocol.deposit_compute_resources(
            vault_address=request.vault_address,
            user_address=user['address'],
            resource_ids=request.resource_ids,
            amounts=request.amounts,
            lock_period_days=request.lock_period_days
        )
        
        return {
            "success": True,
            "deposit": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/vaults/{vault_address}/harvest")
async def harvest_vault_yields(
    vault_address: str,
    vault_protocol: Any = Depends(get_compute_vault_protocol),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Harvest yields from vault strategies"""
    try:
        result = await vault_protocol.harvest_yields(vault_address)
        
        return {
            "success": True,
            "harvest": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/vaults/{vault_address}/performance")
async def get_vault_performance(
    vault_address: str,
    days: int = Query(30, ge=1, le=365),
    vault_protocol: Any = Depends(get_compute_vault_protocol)
) -> Dict[str, Any]:
    """Get vault performance metrics"""
    try:
        # In production, would fetch from protocol
        # Mock response for demonstration
        return {
            "vault_address": vault_address,
            "period_days": days,
            "total_return": "15.3%",
            "annualized_return": "186.2%",
            "sharpe_ratio": 2.1,
            "max_drawdown": "8.5%",
            "strategies": {
                "market_arbitrage": {
                    "profit": "50000",
                    "trades": 127
                },
                "bundle_optimization": {
                    "savings": "25000",
                    "bundles_created": 43
                }
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Lending endpoints

@router.post("/lending/pools/create")
async def create_compute_lending_pool(
    resource_type: str,
    initial_liquidity: Decimal,
    reserve_factor: int = Query(1000, ge=0, le=5000),
    enable_quality_scoring: bool = True,
    lending_protocol: Any = Depends(get_compute_lending_protocol),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Create a lending pool for compute resources"""
    try:
        result = await lending_protocol.create_compute_lending_pool(
            resource_type=resource_type,
            initial_liquidity=initial_liquidity,
            reserve_factor=reserve_factor,
            enable_quality_scoring=enable_quality_scoring
        )
        
        return {
            "success": True,
            "pool": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/lending/borrow")
async def borrow_compute_resources(
    request: BorrowComputeRequest,
    lending_protocol: Any = Depends(get_compute_lending_protocol),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Borrow compute resources against collateral"""
    try:
        result = await lending_protocol.borrow_compute_resources(
            pool_address=request.pool_address,
            borrower=user['address'],
            resource_ids=request.resource_ids,
            amounts=request.amounts,
            duration_hours=request.duration_hours,
            collateral_type=request.collateral_type,
            collateral_amount=request.collateral_amount,
            loan_type=request.loan_type
        )
        
        return {
            "success": True,
            "loan": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/lending/liquidate/{loan_id}")
async def liquidate_compute_loan(
    loan_id: str,
    lending_protocol: Any = Depends(get_compute_lending_protocol),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Liquidate an undercollateralized compute loan"""
    try:
        result = await lending_protocol.liquidate_compute_loan(
            loan_id=loan_id,
            liquidator=user['address']
        )
        
        return {
            "success": True,
            "liquidation": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/lending/pools/{pool_address}/stats")
async def get_lending_pool_stats(
    pool_address: str,
    lending_protocol: Any = Depends(get_compute_lending_protocol)
) -> Dict[str, Any]:
    """Get lending pool statistics"""
    try:
        # In production, would fetch from protocol
        # Mock response for demonstration
        return {
            "pool_address": pool_address,
            "resource_type": "quantum",
            "total_liquidity": "1000000",
            "available_liquidity": "650000",
            "total_borrowed": "350000",
            "utilization_rate": "35%",
            "current_borrow_rate": "8.5%",
            "current_supply_rate": "5.2%",
            "active_loans": 23,
            "total_collateral_value": "525000"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Derivatives endpoints

@router.post("/derivatives/futures/create")
async def create_compute_future(
    request: CreateComputeFutureRequest,
    derivatives_protocol: Any = Depends(get_compute_derivatives_protocol),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Create a future contract for compute resources"""
    try:
        delivery_date = datetime.utcnow() + timedelta(days=request.delivery_days)
        
        result = await derivatives_protocol.create_compute_future(
            resource_type=request.resource_type,
            resource_specs=request.resource_specs,
            quantity=request.quantity,
            delivery_date=delivery_date,
            settlement_type=request.settlement_type
        )
        
        return {
            "success": True,
            "future": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/derivatives/options/create")
async def create_compute_option(
    request: CreateComputeOptionRequest,
    derivatives_protocol: Any = Depends(get_compute_derivatives_protocol),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Create an option contract for compute resources"""
    try:
        expiration_date = datetime.utcnow() + timedelta(days=request.expiration_days)
        
        result = await derivatives_protocol.create_compute_option(
            resource_type=request.resource_type,
            resource_specs=request.resource_specs,
            option_type=request.option_type,
            strike_price=request.strike_price,
            quantity=request.quantity,
            expiration_date=expiration_date,
            american_style=request.american_style
        )
        
        return {
            "success": True,
            "option": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/derivatives/hedge")
async def hedge_compute_portfolio(
    request: HedgePortfolioRequest,
    derivatives_protocol: Any = Depends(get_compute_derivatives_protocol),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Generate hedge recommendations for compute portfolio"""
    try:
        result = await derivatives_protocol.hedge_compute_portfolio(
            portfolio=request.portfolio,
            hedge_objective=request.hedge_objective,
            constraints=request.constraints
        )
        
        return {
            "success": True,
            "hedge_recommendation": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/derivatives/pricing/{contract_id}")
async def get_derivative_pricing(
    contract_id: str,
    contract_type: str = Query(..., regex="^(future|option)$"),
    derivatives_protocol: Any = Depends(get_compute_derivatives_protocol)
) -> Dict[str, Any]:
    """Get current pricing for derivative contract"""
    try:
        # In production, would fetch from protocol
        # Mock response for demonstration
        if contract_type == "option":
            return {
                "contract_id": contract_id,
                "type": "option",
                "spot_price": "500",
                "strike_price": "550",
                "premium": "45.67",
                "implied_volatility": "0.35",
                "greeks": {
                    "delta": "0.42",
                    "gamma": "0.008",
                    "vega": "1.25",
                    "theta": "-0.85"
                },
                "time_to_expiry_days": 30
            }
        else:
            return {
                "contract_id": contract_id,
                "type": "future",
                "spot_price": "500",
                "future_price": "515",
                "basis": "15",
                "open_interest": "2500",
                "volume_24h": "750",
                "time_to_delivery_days": 45
            }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Insurance endpoints

@router.post("/insurance/pools/create")
async def create_insurance_pool(
    request: CreateInsurancePoolRequest,
    insurance_protocol: Any = Depends(get_compute_insurance_protocol),
    api_key: str = Depends(verify_api_key)
) -> Dict[str, Any]:
    """Create an insurance pool for compute resources"""
    try:
        result = await insurance_protocol.create_insurance_pool(
            resource_type=request.resource_type,
            coverage_type=request.coverage_type,
            initial_capital=request.initial_capital,
            target_size=request.target_size,
            reserve_ratio=request.reserve_ratio
        )
        
        return {
            "success": True,
            "pool": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/insurance/policies/purchase")
async def purchase_insurance_policy(
    request: PurchasePolicyRequest,
    insurance_protocol: Any = Depends(get_compute_insurance_protocol),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Purchase insurance policy for compute resources"""
    try:
        result = await insurance_protocol.purchase_policy(
            pool_id=request.pool_id,
            policyholder=user['address'],
            resource_ids=request.resource_ids,
            coverage_amount=request.coverage_amount,
            coverage_period_days=request.coverage_period_days,
            deductible_override=request.deductible_override,
            bundle_discount=request.bundle_discount
        )
        
        return {
            "success": True,
            "policy": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/insurance/claims/file")
async def file_insurance_claim(
    request: FileClaimRequest,
    insurance_protocol: Any = Depends(get_compute_insurance_protocol),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """File an insurance claim"""
    try:
        result = await insurance_protocol.file_claim(
            policy_id=request.policy_id,
            claim_type=request.claim_type,
            incident_data=request.incident_data,
            requested_amount=request.requested_amount,
            evidence_hashes=request.evidence_hashes
        )
        
        return {
            "success": True,
            "claim": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/insurance/liquidity/provide")
async def provide_insurance_liquidity(
    pool_id: str,
    amount: Decimal = Body(..., gt=0),
    insurance_protocol: Any = Depends(get_compute_insurance_protocol),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Provide liquidity to insurance pool"""
    try:
        result = await insurance_protocol.provide_liquidity(
            pool_id=pool_id,
            provider=user['address'],
            amount=amount
        )
        
        return {
            "success": True,
            "liquidity": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/insurance/stake/slashing")
async def stake_for_slashing_insurance(
    stake_amount: Decimal = Body(..., gt=0),
    resource_type: str = Body(...),
    coverage_multiplier: Decimal = Body(10, ge=1, le=100),
    insurance_protocol: Any = Depends(get_compute_insurance_protocol),
    user: Dict[str, Any] = Depends(get_current_user)
) -> Dict[str, Any]:
    """Stake tokens to get slashing insurance"""
    try:
        result = await insurance_protocol.stake_for_slashing_insurance(
            provider_address=user['address'],
            stake_amount=stake_amount,
            resource_type=resource_type,
            coverage_multiplier=coverage_multiplier
        )
        
        return {
            "success": True,
            "stake_insurance": result
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/insurance/policies/{policy_id}")
async def get_policy_details(
    policy_id: str,
    insurance_protocol: Any = Depends(get_compute_insurance_protocol)
) -> Dict[str, Any]:
    """Get insurance policy details"""
    try:
        # In production, would fetch from protocol
        # Mock response for demonstration
        return {
            "policy_id": policy_id,
            "coverage_type": "quality_degradation",
            "coverage_amount": "100000",
            "premium": "2500",
            "deductible": "0.05",
            "start_date": "2024-01-15T00:00:00Z",
            "end_date": "2025-01-15T00:00:00Z",
            "status": "active",
            "claims": [],
            "risk_assessment": {
                "risk_level": "medium",
                "risk_score": 45.2
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/insurance/claims/{claim_id}")
async def get_claim_status(
    claim_id: str,
    insurance_protocol: Any = Depends(get_compute_insurance_protocol)
) -> Dict[str, Any]:
    """Get insurance claim status"""
    try:
        # In production, would fetch from protocol
        # Mock response for demonstration
        return {
            "claim_id": claim_id,
            "policy_id": "policy_123",
            "status": "investigating",
            "requested_amount": "25000",
            "deductible_amount": "1250",
            "claimable_amount": "23750",
            "filed_at": "2024-01-20T10:30:00Z",
            "investigation_eta": "24-48 hours",
            "investigation_notes": [
                "Quality degradation confirmed via oracle",
                "Awaiting final review"
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/insurance/pools/{pool_id}/stats")
async def get_insurance_pool_stats(
    pool_id: str,
    insurance_protocol: Any = Depends(get_compute_insurance_protocol)
) -> Dict[str, Any]:
    """Get insurance pool statistics"""
    try:
        # In production, would fetch from protocol
        # Mock response for demonstration
        return {
            "pool_id": pool_id,
            "resource_type": "quantum",
            "coverage_type": "quality_degradation",
            "total_capital": "5000000",
            "available_capital": "4500000",
            "active_coverage": "8000000",
            "coverage_ratio": "1.78",
            "total_premiums_collected": "125000",
            "total_claims_paid": "45000",
            "loss_ratio": "0.36",
            "active_policies": 156,
            "apy": "8.5%"
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# Analytics endpoints

@router.get("/analytics/market-overview")
async def get_compute_defi_overview(
    vault_protocol: Any = Depends(get_compute_vault_protocol),
    lending_protocol: Any = Depends(get_compute_lending_protocol),
    derivatives_protocol: Any = Depends(get_compute_derivatives_protocol)
) -> Dict[str, Any]:
    """Get overview of compute DeFi markets"""
    try:
        # In production, would aggregate from protocols
        # Mock response for demonstration
        return {
            "total_value_locked": {
                "vaults": "25000000",
                "lending": "15000000",
                "derivatives": "10000000",
                "total": "50000000"
            },
            "active_positions": {
                "vault_depositors": 1250,
                "active_loans": 89,
                "open_futures": 234,
                "open_options": 567
            },
            "yields": {
                "average_vault_apy": "18.5%",
                "average_lending_apy": "7.2%",
                "highest_vault_apy": "45.3%",
                "highest_lending_apy": "12.8%"
            },
            "volume_24h": {
                "vault_deposits": "2500000",
                "loans_originated": "1800000",
                "derivatives_traded": "5200000"
            },
            "resource_breakdown": {
                "quantum": {
                    "tvl": "20000000",
                    "utilization": "78%"
                },
                "ai": {
                    "tvl": "18000000",
                    "utilization": "65%"
                },
                "network": {
                    "tvl": "12000000",
                    "utilization": "82%"
                }
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/analytics/arbitrage-opportunities")
async def get_arbitrage_opportunities(
    min_profit_margin: Decimal = Query(0.02, ge=0, le=1),
    resource_type: Optional[str] = None,
    vault_protocol: Any = Depends(get_compute_vault_protocol)
) -> Dict[str, Any]:
    """Get current arbitrage opportunities in compute DeFi"""
    try:
        # In production, would scan markets for opportunities
        # Mock response for demonstration
        opportunities = [
            {
                "type": "spot_futures_arbitrage",
                "resource": "quantum",
                "spot_price": "500",
                "futures_price": "525",
                "profit_margin": "0.05",
                "estimated_profit": "2500",
                "risk_score": "low"
            },
            {
                "type": "quality_arbitrage",
                "resource": "ai",
                "low_quality_price": "80",
                "high_quality_price": "85",
                "quality_differential": "15",
                "profit_margin": "0.08",
                "estimated_profit": "1200",
                "risk_score": "medium"
            }
        ]
        
        if resource_type:
            opportunities = [o for o in opportunities if o["resource"] == resource_type]
        
        opportunities = [
            o for o in opportunities 
            if Decimal(o["profit_margin"]) >= min_profit_margin
        ]
        
        return {
            "opportunities": opportunities,
            "total_potential_profit": sum(
                Decimal(o["estimated_profit"]) for o in opportunities
            ),
            "timestamp": datetime.utcnow()
        }
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e)) 