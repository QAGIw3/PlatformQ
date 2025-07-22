"""Value at Risk (VaR) API endpoints."""

import logging
from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from ..core import VaRCalculator
from ..models import VaRResult, VaRParameters
from ..dependencies import get_var_calculator, get_state_manager
from ..auth import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/var", tags=["Value at Risk"])


class VaRRequest(BaseModel):
    """Request for VaR calculation."""
    confidence_level: float = Field(0.95, ge=0.9, le=0.99, description="Confidence level")
    time_horizon_days: int = Field(1, ge=1, le=30, description="Time horizon in days")
    method: str = Field("historical", pattern="^(historical|parametric|monte_carlo)$")
    lookback_days: Optional[int] = Field(30, ge=10, le=365, description="Historical data lookback")


class VaRResponse(BaseModel):
    """VaR calculation response."""
    portfolio_id: str
    var_amount: str
    var_percentage: str
    cvar_amount: str
    cvar_percentage: str
    confidence_level: float
    time_horizon_days: int
    method: str
    positions_included: int
    calculation_time: str
    timestamp: str


class VaRBacktestResponse(BaseModel):
    """VaR backtest results."""
    portfolio_id: str
    period_start: str
    period_end: str
    var_breaches: int
    expected_breaches: int
    breach_percentage: float
    kupiec_test: Dict[str, Any]
    christoffersen_test: Dict[str, Any]
    is_valid: bool


@router.get("/{portfolio_id}", response_model=VaRResponse)
async def calculate_var(
    portfolio_id: str,
    confidence_level: float = Query(0.95, ge=0.9, le=0.99),
    time_horizon_days: int = Query(1, ge=1, le=30),
    method: str = Query("historical", pattern="^(historical|parametric|monte_carlo)$"),
    current_user: Dict = Depends(get_current_user),
    var_calculator: VaRCalculator = Depends(get_var_calculator),
    state_manager = Depends(get_state_manager)
) -> VaRResponse:
    """Calculate Value at Risk for a portfolio."""
    # Get portfolio
    portfolio = await state_manager.get_portfolio(portfolio_id)
    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")
    
    # Check authorization
    if portfolio.get("user_id") != current_user["user_id"] and "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get positions
    positions = await state_manager.get_portfolio_positions(portfolio_id)
    if not positions:
        return VaRResponse(
            portfolio_id=portfolio_id,
            var_amount="0",
            var_percentage="0",
            cvar_amount="0",
            cvar_percentage="0",
            confidence_level=confidence_level,
            time_horizon_days=time_horizon_days,
            method=method,
            positions_included=0,
            calculation_time="0",
            timestamp=datetime.utcnow().isoformat()
        )
    
    # Get market data
    market_ids = list(set(p.get("market_id") for p in positions))
    market_data = await state_manager.get_market_data_batch(market_ids)
    
    # Start timer
    start_time = datetime.utcnow()
    
    # Calculate portfolio VaR
    var_result = await var_calculator.calculate_portfolio_var(
        positions=positions,
        market_data=market_data,
        confidence_level=confidence_level,
        time_horizon_days=time_horizon_days,
        method=method
    )
    
    # Calculate CVaR (Conditional VaR)
    cvar_result = await var_calculator.calculate_portfolio_cvar(
        positions=positions,
        market_data=market_data,
        confidence_level=confidence_level,
        time_horizon_days=time_horizon_days,
        method=method
    )
    
    # Calculate time
    calc_time = (datetime.utcnow() - start_time).total_seconds()
    
    return VaRResponse(
        portfolio_id=portfolio_id,
        var_amount=str(var_result["var_amount"]),
        var_percentage=str(var_result["var_percentage"]),
        cvar_amount=str(cvar_result["cvar_amount"]),
        cvar_percentage=str(cvar_result["cvar_percentage"]),
        confidence_level=confidence_level,
        time_horizon_days=time_horizon_days,
        method=method,
        positions_included=len(positions),
        calculation_time=f"{calc_time:.3f}",
        timestamp=datetime.utcnow().isoformat()
    )


@router.post("/calculate", response_model=VaRResponse)
async def calculate_custom_var(
    request: VaRRequest,
    positions: List[Dict[str, Any]],
    current_user: Dict = Depends(get_current_user),
    var_calculator: VaRCalculator = Depends(get_var_calculator),
    state_manager = Depends(get_state_manager)
) -> VaRResponse:
    """Calculate VaR for a custom portfolio."""
    if not positions:
        raise HTTPException(status_code=400, detail="No positions provided")
    
    # Get market data
    market_ids = list(set(p.get("market_id") for p in positions))
    market_data = await state_manager.get_market_data_batch(market_ids)
    
    # Start timer
    start_time = datetime.utcnow()
    
    # Calculate VaR
    var_result = await var_calculator.calculate_portfolio_var(
        positions=positions,
        market_data=market_data,
        confidence_level=request.confidence_level,
        time_horizon_days=request.time_horizon_days,
        method=request.method,
        lookback_days=request.lookback_days
    )
    
    # Calculate CVaR
    cvar_result = await var_calculator.calculate_portfolio_cvar(
        positions=positions,
        market_data=market_data,
        confidence_level=request.confidence_level,
        time_horizon_days=request.time_horizon_days,
        method=request.method,
        lookback_days=request.lookback_days
    )
    
    # Calculate time
    calc_time = (datetime.utcnow() - start_time).total_seconds()
    
    return VaRResponse(
        portfolio_id=f"custom_{current_user['user_id']}_{datetime.utcnow().timestamp()}",
        var_amount=str(var_result["var_amount"]),
        var_percentage=str(var_result["var_percentage"]),
        cvar_amount=str(cvar_result["cvar_amount"]),
        cvar_percentage=str(cvar_result["cvar_percentage"]),
        confidence_level=request.confidence_level,
        time_horizon_days=request.time_horizon_days,
        method=request.method,
        positions_included=len(positions),
        calculation_time=f"{calc_time:.3f}",
        timestamp=datetime.utcnow().isoformat()
    )


@router.get("/history/{portfolio_id}")
async def get_var_history(
    portfolio_id: str,
    days: int = Query(30, ge=1, le=365),
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> List[Dict[str, Any]]:
    """Get historical VaR calculations."""
    # Get portfolio
    portfolio = await state_manager.get_portfolio(portfolio_id)
    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")
    
    # Check authorization
    if portfolio.get("user_id") != current_user["user_id"] and "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get historical VaR
    history = await state_manager.get_var_history(
        portfolio_id=portfolio_id,
        start_date=datetime.utcnow() - timedelta(days=days),
        end_date=datetime.utcnow()
    )
    
    return [
        {
            "date": record["date"].isoformat(),
            "var_amount": str(record["var_amount"]),
            "var_percentage": str(record["var_percentage"]),
            "cvar_amount": str(record.get("cvar_amount", 0)),
            "portfolio_value": str(record["portfolio_value"]),
            "actual_pnl": str(record.get("actual_pnl", 0)),
            "breach": record.get("breach", False)
        }
        for record in history
    ]


@router.post("/backtest/{portfolio_id}", response_model=VaRBacktestResponse)
async def backtest_var(
    portfolio_id: str,
    start_date: datetime = Query(..., description="Backtest start date"),
    end_date: datetime = Query(..., description="Backtest end date"),
    confidence_level: float = Query(0.95, ge=0.9, le=0.99),
    current_user: Dict = Depends(get_current_user),
    var_calculator: VaRCalculator = Depends(get_var_calculator),
    state_manager = Depends(get_state_manager)
) -> VaRBacktestResponse:
    """Backtest VaR model performance."""
    # Get portfolio
    portfolio = await state_manager.get_portfolio(portfolio_id)
    if not portfolio:
        raise HTTPException(status_code=404, detail="Portfolio not found")
    
    # Check authorization
    if portfolio.get("user_id") != current_user["user_id"] and "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Run backtest
    backtest_result = await var_calculator.backtest_var(
        portfolio_id=portfolio_id,
        start_date=start_date,
        end_date=end_date,
        confidence_level=confidence_level
    )
    
    # Perform statistical tests
    kupiec_test = var_calculator.kupiec_test(
        breaches=backtest_result["breaches"],
        total_observations=backtest_result["total_days"],
        confidence_level=confidence_level
    )
    
    christoffersen_test = var_calculator.christoffersen_test(
        breach_sequence=backtest_result["breach_sequence"],
        confidence_level=confidence_level
    )
    
    # Determine validity
    is_valid = kupiec_test["p_value"] > 0.05 and christoffersen_test["p_value"] > 0.05
    
    return VaRBacktestResponse(
        portfolio_id=portfolio_id,
        period_start=start_date.isoformat(),
        period_end=end_date.isoformat(),
        var_breaches=backtest_result["breaches"],
        expected_breaches=backtest_result["expected_breaches"],
        breach_percentage=backtest_result["breach_percentage"],
        kupiec_test=kupiec_test,
        christoffersen_test=christoffersen_test,
        is_valid=is_valid
    )


@router.get("/limits")
async def get_var_limits(
    user_id: Optional[str] = Query(None, description="Filter by user"),
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> List[Dict[str, Any]]:
    """Get VaR limits configuration."""
    # Check authorization
    if user_id and user_id != current_user["user_id"] and "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Get limits
    if user_id:
        limits = await state_manager.get_var_limits(user_id)
    else:
        # Risk managers can see all limits
        if "risk_manager" not in current_user.get("roles", []):
            user_id = current_user["user_id"]
            limits = await state_manager.get_var_limits(user_id)
        else:
            limits = await state_manager.get_all_var_limits()
    
    return [
        {
            "user_id": limit.get("user_id"),
            "portfolio_id": limit.get("portfolio_id"),
            "var_limit": str(limit.get("var_limit")),
            "cvar_limit": str(limit.get("cvar_limit")),
            "confidence_level": limit.get("confidence_level"),
            "time_horizon_days": limit.get("time_horizon_days"),
            "current_var": str(limit.get("current_var", 0)),
            "utilization": str(limit.get("utilization", 0)),
            "status": limit.get("status", "active")
        }
        for limit in limits
    ]


@router.put("/limits/{portfolio_id}")
async def update_var_limits(
    portfolio_id: str,
    var_limit: Decimal = Query(..., gt=0, description="VaR limit"),
    cvar_limit: Optional[Decimal] = Query(None, gt=0, description="CVaR limit"),
    current_user: Dict = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Update VaR limits for a portfolio."""
    # Risk managers only
    if "risk_manager" not in current_user.get("roles", []):
        raise HTTPException(status_code=403, detail="Risk manager access required")
    
    # Update limits
    limits = {
        "var_limit": var_limit,
        "cvar_limit": cvar_limit or var_limit * Decimal("1.2"),
        "updated_by": current_user["user_id"],
        "updated_at": datetime.utcnow()
    }
    
    await state_manager.update_var_limits(portfolio_id, limits)
    
    return {
        "portfolio_id": portfolio_id,
        "var_limit": str(var_limit),
        "cvar_limit": str(limits["cvar_limit"]),
        "status": "updated",
        "timestamp": datetime.utcnow().isoformat()
    } 