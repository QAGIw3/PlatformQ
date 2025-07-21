"""
Infrastructure DeFi API endpoints

Provides endpoints for infrastructure-backed lending and resource token operations.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import logging

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field, validator

from platformq_shared import get_current_user
from ..protocols.infrastructure_lending import InfrastructureLendingProtocol
from ..services.resource_valuation import ResourceValuationService
from ..models import ResourceType, ServiceTier

logger = logging.getLogger(__name__)

router = APIRouter()


# Request/Response Models
class ResourceLoanRequest(BaseModel):
    """Request to create a loan with resource tokens as collateral"""
    resource_token_id: int = Field(..., description="Resource token ID to use as collateral")
    amount: int = Field(..., gt=0, description="Amount of resource tokens to collateralize")
    loan_amount: Decimal = Field(..., gt=0, description="Amount to borrow in payment token")
    duration_days: int = Field(..., ge=1, le=365, description="Loan duration in days")
    payment_token: str = Field(..., description="Token address to borrow (e.g., USDC)")
    
    @validator('loan_amount')
    def validate_loan_amount(cls, v):
        if v > Decimal('1000000'):
            raise ValueError("Loan amount exceeds maximum")
        return v


class ResourceValuationRequest(BaseModel):
    """Request to value resource tokens"""
    resource_type: ResourceType
    service_tier: ServiceTier
    region: str = Field(default="us-east-1")
    amount: int = Field(..., gt=0)
    valid_until: datetime


class LoanResponse(BaseModel):
    """Loan creation response"""
    loan_id: int
    borrower: str
    collateral_token_id: int
    collateral_amount: int
    collateral_value: Decimal
    loan_amount: Decimal
    interest_rate: Decimal
    total_due: Decimal
    duration: int
    expires_at: datetime
    status: str
    tx_hash: str


class ResourceValuationResponse(BaseModel):
    """Resource valuation response"""
    resource_type: ResourceType
    service_tier: ServiceTier
    region: str
    amount: int
    base_price_per_unit: Decimal
    time_decay_factor: Decimal
    total_value: Decimal
    max_loan_amount: Decimal
    ltv_ratio: Decimal
    expires_in_days: int


# Initialize services
lending_protocol = None
valuation_service = None


async def get_lending_protocol() -> InfrastructureLendingProtocol:
    """Get infrastructure lending protocol instance"""
    global lending_protocol
    if not lending_protocol:
        from ..main import defi_manager
        lending_protocol = InfrastructureLendingProtocol(
            defi_manager.blockchain_pool,
            defi_manager.config
        )
        await lending_protocol.initialize()
    return lending_protocol


async def get_valuation_service() -> ResourceValuationService:
    """Get resource valuation service instance"""
    global valuation_service
    if not valuation_service:
        from ..main import price_oracle
        valuation_service = ResourceValuationService(price_oracle)
    return valuation_service


@router.post("/loans/create", response_model=LoanResponse)
async def create_resource_loan(
    request: ResourceLoanRequest,
    background_tasks: BackgroundTasks,
    current_user: Dict = Depends(get_current_user)
) -> LoanResponse:
    """
    Create a loan using resource tokens as collateral
    
    - Resource tokens are locked as collateral
    - Loan amount is determined by LTV ratio
    - Interest rates based on resource volatility
    - Automatic liquidation if collateral value drops
    """
    protocol = await get_lending_protocol()
    
    try:
        # Create loan
        result = await protocol.create_resource_loan(
            borrower=current_user["wallet_address"],
            resource_token_id=request.resource_token_id,
            amount=request.amount,
            loan_amount=request.loan_amount,
            duration_seconds=request.duration_days * 86400,
            payment_token=request.payment_token
        )
        
        # Schedule monitoring for liquidation
        background_tasks.add_task(
            protocol.monitor_loan_health,
            result["loan_id"]
        )
        
        return LoanResponse(
            loan_id=result["loan_id"],
            borrower=current_user["wallet_address"],
            collateral_token_id=request.resource_token_id,
            collateral_amount=request.amount,
            collateral_value=result["collateral_value"],
            loan_amount=request.loan_amount,
            interest_rate=result["interest_rate"],
            total_due=result["total_due"],
            duration=request.duration_days * 86400,
            expires_at=datetime.utcnow() + timedelta(days=request.duration_days),
            status="active",
            tx_hash=result["tx_hash"]
        )
        
    except Exception as e:
        logger.error(f"Error creating resource loan: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/loans/{loan_id}")
async def get_loan_details(
    loan_id: int,
    current_user: Dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """Get details of a specific loan"""
    protocol = await get_lending_protocol()
    
    try:
        loan = await protocol.get_loan_details(loan_id)
        
        # Check if user is authorized to view loan
        if loan["borrower"].lower() != current_user["wallet_address"].lower():
            raise HTTPException(status_code=403, detail="Not authorized to view this loan")
        
        return loan
        
    except Exception as e:
        logger.error(f"Error getting loan details: {e}")
        raise HTTPException(status_code=404, detail="Loan not found")


@router.post("/loans/{loan_id}/repay")
async def repay_loan(
    loan_id: int,
    amount: Optional[Decimal] = None,
    current_user: Dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Repay a loan partially or fully
    
    - If amount is not specified, repays the full amount
    - Returns collateral proportionally for partial repayments
    - Updates loan status upon full repayment
    """
    protocol = await get_lending_protocol()
    
    try:
        result = await protocol.repay_loan(
            loan_id=loan_id,
            payer=current_user["wallet_address"],
            amount=amount
        )
        
        return {
            "loan_id": loan_id,
            "amount_repaid": result["amount_repaid"],
            "remaining_debt": result["remaining_debt"],
            "collateral_returned": result["collateral_returned"],
            "status": result["status"],
            "tx_hash": result["tx_hash"]
        }
        
    except Exception as e:
        logger.error(f"Error repaying loan: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/loans/{loan_id}/revalue")
async def revalue_collateral(
    loan_id: int,
    background_tasks: BackgroundTasks,
    current_user: Dict = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Trigger collateral revaluation for a loan
    
    - Recalculates collateral value based on current prices
    - May trigger liquidation if undercollateralized
    - Anyone can call this to maintain protocol health
    """
    protocol = await get_lending_protocol()
    
    try:
        result = await protocol.revalue_collateral(loan_id)
        
        # If undercollateralized, schedule liquidation
        if result["health_factor"] < Decimal("1.0"):
            background_tasks.add_task(
                protocol.liquidate_loan,
                loan_id
            )
        
        return {
            "loan_id": loan_id,
            "old_value": result["old_value"],
            "new_value": result["new_value"],
            "health_factor": result["health_factor"],
            "at_risk": result["health_factor"] < Decimal("1.2"),
            "tx_hash": result["tx_hash"]
        }
        
    except Exception as e:
        logger.error(f"Error revaluing collateral: {e}")
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/valuate", response_model=ResourceValuationResponse)
async def valuate_resources(
    request: ResourceValuationRequest,
    current_user: Dict = Depends(get_current_user)
) -> ResourceValuationResponse:
    """
    Get current valuation for resource tokens
    
    - Calculates value based on oracle prices
    - Applies time decay for expiring resources
    - Shows maximum loan amount based on LTV
    """
    valuation = await get_valuation_service()
    
    try:
        result = await valuation.calculate_value(
            resource_type=request.resource_type,
            service_tier=request.service_tier,
            region=request.region,
            amount=request.amount,
            valid_until=request.valid_until
        )
        
        return ResourceValuationResponse(
            resource_type=request.resource_type,
            service_tier=request.service_tier,
            region=request.region,
            amount=request.amount,
            base_price_per_unit=result["base_price"],
            time_decay_factor=result["time_decay_factor"],
            total_value=result["total_value"],
            max_loan_amount=result["max_loan_amount"],
            ltv_ratio=result["ltv_ratio"],
            expires_in_days=result["days_until_expiry"]
        )
        
    except Exception as e:
        logger.error(f"Error valuating resources: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/loans/my-loans")
async def get_my_loans(
    status: Optional[str] = None,
    current_user: Dict = Depends(get_current_user)
) -> List[Dict[str, Any]]:
    """Get all loans for the current user"""
    protocol = await get_lending_protocol()
    
    try:
        loans = await protocol.get_user_loans(
            user=current_user["wallet_address"],
            status=status
        )
        
        return loans
        
    except Exception as e:
        logger.error(f"Error getting user loans: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/lending")
async def get_lending_stats() -> Dict[str, Any]:
    """Get infrastructure lending protocol statistics"""
    protocol = await get_lending_protocol()
    
    try:
        stats = await protocol.get_protocol_stats()
        
        return {
            "total_loans": stats["total_loans"],
            "active_loans": stats["active_loans"],
            "total_value_locked": stats["tvl"],
            "total_borrowed": stats["total_borrowed"],
            "average_ltv": stats["average_ltv"],
            "liquidation_rate": stats["liquidation_rate"],
            "resource_breakdown": stats["resource_breakdown"]
        }
        
    except Exception as e:
        logger.error(f"Error getting lending stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ltv-ratios")
async def get_ltv_ratios() -> Dict[str, Dict[str, float]]:
    """Get current LTV ratios for all resource types and tiers"""
    protocol = await get_lending_protocol()
    
    try:
        return await protocol.get_ltv_ratios()
        
    except Exception as e:
        logger.error(f"Error getting LTV ratios: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 