"""
Derivative Lending API

Endpoints for risk-based lending using derivative positions as collateral.
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field

from app.lending.derivative_lending import (
    DerivativeLending,
    LoanTerms,
    LoanStatus
)
from app.auth import get_current_user
from platformq_shared import ProcessingStatus

router = APIRouter(
    prefix="/api/v1/lending",
    tags=["lending"]
)

# Initialize lending engine
lending_engine = DerivativeLending()


class LoanRequest(BaseModel):
    """Request for a new loan"""
    loan_amount: Decimal = Field(..., gt=0, description="Amount to borrow")
    duration_days: int = Field(..., ge=7, le=365, description="Loan duration in days")
    collateral_positions: List[str] = Field(..., min_items=1, description="Position IDs to use as collateral")


class RepaymentRequest(BaseModel):
    """Request to repay a loan"""
    loan_id: str
    repayment_amount: Decimal = Field(..., gt=0)


class AddCollateralRequest(BaseModel):
    """Request to add collateral to existing loan"""
    loan_id: str
    additional_positions: List[str] = Field(..., min_items=1)


@router.post("/request-loan")
async def request_loan(
    request: LoanRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Request a loan using derivative positions as collateral
    
    The interest rate and maximum loan amount are determined by:
    - Risk score of collateral positions
    - Loan duration
    - Current market conditions
    """
    try:
        result = await lending_engine.request_loan(
            tenant_id=current_user["tenant_id"],
            borrower_id=current_user["user_id"],
            loan_amount=request.loan_amount,
            duration_days=request.duration_days,
            collateral_positions=request.collateral_positions
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "data": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/loans")
async def list_loans(
    status: Optional[LoanStatus] = None,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    List user's loans
    """
    try:
        user_loans = []
        
        for loan_id, loan in lending_engine.active_loans.items():
            if loan.borrower_id == current_user["user_id"]:
                if status is None or loan.status == status:
                    user_loans.append(lending_engine._serialize_loan(loan))
        
        return {
            "status": "success",
            "loans": user_loans,
            "total": len(user_loans)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/loans/{loan_id}")
async def get_loan_details(
    loan_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get detailed information about a specific loan
    """
    try:
        loan = lending_engine.active_loans.get(loan_id)
        
        if not loan:
            raise HTTPException(status_code=404, detail="Loan not found")
        
        # Verify ownership
        if loan.borrower_id != current_user["user_id"]:
            raise HTTPException(status_code=403, detail="Unauthorized")
        
        # Get current health status
        health_result = await lending_engine.monitor_loan_health(loan_id)
        
        return {
            "status": "success",
            "loan": lending_engine._serialize_loan(loan),
            "health_status": health_result.data if health_result.status == ProcessingStatus.SUCCESS else None,
            "repayment_schedule": lending_engine._generate_repayment_schedule(loan)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/repay")
async def repay_loan(
    request: RepaymentRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Make a loan repayment
    
    Payments are applied first to accrued interest, then to principal.
    """
    try:
        result = await lending_engine.repay_loan(
            tenant_id=current_user["tenant_id"],
            borrower_id=current_user["user_id"],
            loan_id=request.loan_id,
            repayment_amount=request.repayment_amount
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "repayment": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/add-collateral")
async def add_collateral(
    request: AddCollateralRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Add additional collateral to improve loan health
    
    Useful for resolving margin calls or improving loan terms.
    """
    try:
        result = await lending_engine.add_collateral(
            tenant_id=current_user["tenant_id"],
            borrower_id=current_user["user_id"],
            loan_id=request.loan_id,
            additional_positions=request.additional_positions
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "data": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opportunities")
async def get_lending_opportunities(
    position_ids: Optional[List[str]] = None,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Analyze user's positions for lending opportunities
    
    Shows potential loan amounts and estimated interest rates.
    """
    try:
        # If no positions specified, use all user positions
        if not position_ids:
            # This would fetch all user positions
            position_ids = ["pos_1", "pos_2"]  # Mock data
        
        result = await lending_engine.get_lending_opportunities(
            tenant_id=current_user["tenant_id"],
            user_positions=position_ids
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "opportunities": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/loan-terms")
async def get_loan_terms(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current loan terms and parameters
    """
    try:
        terms = LoanTerms()
        
        return {
            "status": "success",
            "terms": {
                "min_duration_days": terms.min_duration.days,
                "max_duration_days": terms.max_duration.days,
                "min_loan_amount": str(terms.min_loan_amount),
                "max_loan_amount": str(terms.max_loan_amount),
                "base_interest_rate": str(terms.base_interest_rate),
                "max_interest_rate": str(terms.max_interest_rate),
                "initial_margin_requirement": str(terms.initial_margin),
                "maintenance_margin": str(terms.maintenance_margin),
                "liquidation_discount": str(terms.liquidation_discount),
                "margin_call_grace_period_hours": terms.liquidation_delay.total_seconds() / 3600
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/interest-rates")
async def get_interest_rate_curve(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current interest rate curve based on risk scores
    """
    try:
        rate_curve = []
        
        for risk_score, rate in lending_engine.rate_curve.items():
            rate_curve.append({
                "min_risk_score": risk_score,
                "interest_rate": str(rate),
                "apr_percentage": f"{float(rate) * 100:.1f}%"
            })
        
        return {
            "status": "success",
            "rate_curve": rate_curve,
            "description": "Interest rates are adjusted based on loan duration and LTV ratio"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health-monitor/{loan_id}")
async def monitor_loan_health(
    loan_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get real-time health status of a loan
    
    Monitors collateral value, LTV ratio, and margin requirements.
    """
    try:
        loan = lending_engine.active_loans.get(loan_id)
        
        if not loan:
            raise HTTPException(status_code=404, detail="Loan not found")
        
        # Verify ownership
        if loan.borrower_id != current_user["user_id"]:
            raise HTTPException(status_code=403, detail="Unauthorized")
        
        # Get health status
        result = await lending_engine.monitor_loan_health(loan_id)
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "loan_id": loan_id,
                "health": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def get_loan_history(
    limit: int = 50,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get user's loan history
    """
    try:
        user_history = []
        
        for event in lending_engine.loan_history[-limit:]:
            if event.get("borrower_id") == current_user["user_id"]:
                user_history.append({
                    "loan_id": event["loan_id"],
                    "action": event["action"],
                    "timestamp": event["timestamp"].isoformat(),
                    "details": event["details"]
                })
        
        return {
            "status": "success",
            "history": user_history,
            "total": len(user_history)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 


@router.get("/loan/{loan_id}/repayment-schedule")
async def get_repayment_schedule(
    loan_id: str,
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """Get repayment schedule for a loan"""
    try:
        loan = lending_engine.active_loans.get(loan_id)
        if not loan:
            raise HTTPException(status_code=404, detail="Loan not found")
        
        if loan.borrower_id != user.user_id:
            raise HTTPException(status_code=403, detail="Unauthorized")
        
        schedule = lending_engine._generate_repayment_schedule(loan)
        
        return {
            "loan_id": loan_id,
            "schedule": schedule
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============ Flash Loan Endpoints ============

class FlashLoanRequest(BaseModel):
    """Request for a flash loan"""
    asset: str = Field(..., description="Asset to borrow")
    amount: Decimal = Field(..., gt=0, description="Amount to borrow")
    callback_contract: str = Field(..., description="Contract to execute with borrowed funds")
    callback_data: Dict[str, Any] = Field(default_factory=dict, description="Data to pass to callback")


class FlashLoanArbitrageRequest(BaseModel):
    """Request for flash loan arbitrage"""
    borrow_asset: str = Field(..., description="Asset to borrow for arbitrage")
    borrow_amount: Decimal = Field(..., gt=0, description="Amount to borrow")
    target_markets: List[str] = Field(..., min_items=2, description="Markets to arbitrage between")
    expected_profit: Decimal = Field(..., gt=0, description="Expected profit from arbitrage")


@router.post("/flash-loan")
async def execute_flash_loan(
    request: FlashLoanRequest,
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Execute a flash loan - borrow and repay within the same transaction
    
    Flash loans allow borrowing large amounts with no collateral, but must be
    repaid within the same transaction plus a 0.09% fee.
    """
    try:
        result = await lending_engine.execute_flash_loan(
            borrower_id=user.user_id,
            asset=request.asset,
            amount=request.amount,
            callback_contract=request.callback_contract,
            callback_data=request.callback_data
        )
        
        if result.status != ProcessingStatus.COMPLETED:
            raise HTTPException(
                status_code=400, 
                detail=result.error or "Flash loan execution failed"
            )
        
        return {
            "success": True,
            "flash_loan": result.data["flash_loan"],
            "fee_paid": result.data["fee_paid"]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/flash-loan/arbitrage")
async def execute_flash_loan_arbitrage(
    request: FlashLoanArbitrageRequest,
    user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Execute flash loan for arbitrage opportunity
    
    Automatically executes arbitrage strategy using flash borrowed funds.
    """
    try:
        strategy = {
            "borrow_asset": request.borrow_asset,
            "borrow_amount": str(request.borrow_amount),
            "target_markets": request.target_markets,
            "expected_profit": str(request.expected_profit)
        }
        
        result = await lending_engine.execute_flash_loan_arbitrage(
            arbitrageur_id=user.user_id,
            strategy=strategy
        )
        
        if result.status != ProcessingStatus.COMPLETED:
            raise HTTPException(
                status_code=400,
                detail=result.error or "Arbitrage execution failed"
            )
        
        return {
            "success": True,
            "flash_loan": result.data.get("flash_loan"),
            "arbitrage_profit": result.data.get("arbitrage_profit", "0"),
            "net_profit": result.data.get("net_profit", "0")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/flash-loan/fee")
async def get_flash_loan_fee(
    asset: str,
    amount: Decimal
) -> Dict[str, Any]:
    """
    Get flash loan fee for given asset and amount
    
    Fee is dynamic based on pool utilization.
    """
    try:
        fee = await lending_engine.get_flash_loan_fee(asset, amount)
        fee_rate = fee / amount if amount > 0 else lending_engine.flash_loan_fee_rate
        
        return {
            "asset": asset,
            "amount": str(amount),
            "fee": str(fee),
            "fee_rate": str(fee_rate),
            "fee_percentage": str(fee_rate * 100)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/flash-loan/available-liquidity")
async def get_available_liquidity(asset: str) -> Dict[str, Any]:
    """Get available liquidity for flash loans"""
    try:
        available = await lending_engine._get_available_liquidity(asset)
        total = await lending_engine._get_pool_balance(asset)
        utilization = await lending_engine._get_pool_utilization(asset)
        
        return {
            "asset": asset,
            "available_liquidity": str(available),
            "total_liquidity": str(total),
            "utilization_rate": str(utilization),
            "utilization_percentage": str(utilization * 100)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/flash-loan/stats")
async def get_flash_loan_stats() -> Dict[str, Any]:
    """Get flash loan statistics"""
    try:
        metrics = lending_engine.metrics
        
        return {
            "total_flash_loans": metrics.get("flash_loans_executed", 0),
            "total_volume": str(metrics.get("flash_loan_volume", Decimal("0"))),
            "total_fees_collected": str(metrics.get("flash_fees_collected", Decimal("0"))),
            "average_loan_size": str(
                metrics.get("flash_loan_volume", Decimal("0")) / 
                max(1, metrics.get("flash_loans_executed", 1))
            ),
            "recent_flash_loans": metrics.get("flash_loans", [])[-10:]  # Last 10
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 