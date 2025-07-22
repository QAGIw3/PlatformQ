"""Risk assessment API endpoints."""

from typing import Dict, List, Optional, Any
from datetime import datetime
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from platformq_shared import (
    get_current_user,
    AuthenticatedUser,
    UserRole,
    monitor_operation
)
from ..dependencies import (
    get_risk_calculator,
    get_state_manager,
    get_settings
)
from ..models.risk import RiskLevel

router = APIRouter(prefix="/risk", tags=["risk"])


class PositionRiskRequest(BaseModel):
    """Request for position risk calculation"""
    position_id: str
    market_id: str
    size: Decimal
    entry_price: Decimal
    current_price: Decimal
    collateral: Decimal
    leverage: Decimal = Decimal("1")


class PositionRiskResponse(BaseModel):
    """Response for position risk"""
    position_id: str
    risk_level: str
    metrics: Dict[str, str]
    alerts: List[Dict[str, Any]]
    timestamp: str


class PortfolioRiskResponse(BaseModel):
    """Response for portfolio risk"""
    user_id: str
    total_positions: int
    total_value: str
    total_collateral: str
    total_unrealized_pnl: str
    margin_usage: str
    portfolio_var: str
    portfolio_leverage: str
    max_concentration: str
    risk_score: int
    alerts: List[Dict[str, Any]]
    timestamp: str


@router.get("/portfolio/{user_id}", response_model=PortfolioRiskResponse)
async def get_portfolio_risk(
    user_id: str,
    refresh: bool = Query(False, description="Force refresh calculation"),
    current_user: AuthenticatedUser = Depends(get_current_user),
    risk_calculator = Depends(get_risk_calculator),
    state_manager = Depends(get_state_manager)
) -> PortfolioRiskResponse:
    """Get portfolio risk metrics for a user."""
    # Check authorization
    if current_user.user_id != user_id and UserRole.RISK_MANAGER not in current_user.roles:
        raise HTTPException(status_code=403, detail="Not authorized to view this portfolio")
    
    # Check cache first
    if not refresh:
        cached_risk = await state_manager.get_portfolio_risk(user_id)
        if cached_risk and (datetime.utcnow() - cached_risk.timestamp).seconds < 60:
            return _format_portfolio_risk(cached_risk)
    
    # Get positions and market data
    positions = await state_manager.get_user_positions(user_id)
    if not positions:
        return PortfolioRiskResponse(
            user_id=user_id,
            total_positions=0,
            total_value="0",
            total_collateral="0",
            total_unrealized_pnl="0",
            margin_usage="0",
            portfolio_var="0",
            portfolio_leverage="0",
            max_concentration="0",
            risk_score=0,
            alerts=[],
            timestamp=datetime.utcnow().isoformat()
        )
    
    # Get market data for all positions
    market_ids = list(set(p.get("market_id") for p in positions))
    market_data = await state_manager.get_market_data_batch(market_ids)
    
    # Calculate portfolio risk
    portfolio_risk = await risk_calculator.calculate_portfolio_risk(
        user_id=user_id,
        positions=positions,
        market_data=market_data
    )
    
    # Cache the result
    await state_manager.cache_portfolio_risk(user_id, portfolio_risk)
    
    # Publish risk event if there are alerts
    if portfolio_risk.alerts:
        await state_manager.publish_risk_alert(user_id, portfolio_risk)
    
    return _format_portfolio_risk(portfolio_risk)


@router.post("/position", response_model=PositionRiskResponse)
async def calculate_position_risk(
    request: PositionRiskRequest,
    current_user: AuthenticatedUser = Depends(get_current_user),
    risk_calculator = Depends(get_risk_calculator)
) -> PositionRiskResponse:
    """Calculate risk for a single position."""
    # Calculate position risk
    risk_metrics = await risk_calculator.calculate_position_risk(
        size=request.size,
        entry_price=request.entry_price,
        current_price=request.current_price,
        collateral=request.collateral,
        leverage=request.leverage
    )
    
    # Determine risk level
    risk_level = _determine_risk_level(risk_metrics)
    
    # Generate alerts
    alerts = _generate_position_alerts(risk_metrics, risk_level)
    
    return PositionRiskResponse(
        position_id=request.position_id,
        risk_level=risk_level.value,
        metrics={
            "unrealized_pnl": str(risk_metrics.get("unrealized_pnl", 0)),
            "margin_ratio": str(risk_metrics.get("margin_ratio", 0)),
            "liquidation_price": str(risk_metrics.get("liquidation_price", 0)),
            "position_value": str(risk_metrics.get("position_value", 0)),
            "effective_leverage": str(risk_metrics.get("effective_leverage", 0))
        },
        alerts=alerts,
        timestamp=datetime.utcnow().isoformat()
    )


@router.get("/exposure/{market_id}")
async def get_market_exposure(
    market_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Get total exposure for a specific market."""
    # Only risk managers can view total market exposure
    if UserRole.RISK_MANAGER not in current_user.roles:
        raise HTTPException(status_code=403, detail="Risk manager access required")
    
    exposure = await state_manager.get_market_exposure(market_id)
    
    return {
        "market_id": market_id,
        "total_long_exposure": str(exposure.get("long", 0)),
        "total_short_exposure": str(exposure.get("short", 0)),
        "net_exposure": str(exposure.get("net", 0)),
        "open_interest": str(exposure.get("open_interest", 0)),
        "largest_position": str(exposure.get("largest_position", 0)),
        "concentration_ratio": str(exposure.get("concentration_ratio", 0)),
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/alerts")
async def get_risk_alerts(
    status: Optional[str] = Query(None, pattern="^(active|resolved|all)$"),
    severity: Optional[str] = Query(None, pattern="^(low|medium|high|critical)$"),
    limit: int = Query(100, ge=1, le=1000),
    current_user: AuthenticatedUser = Depends(get_current_user),
    state_manager = Depends(get_state_manager)
) -> Dict[str, Any]:
    """Get risk alerts."""
    # Filter alerts based on user role
    if UserRole.RISK_MANAGER in current_user.roles:
        # Risk managers see all alerts
        alerts = await state_manager.get_all_alerts(status, severity, limit)
    else:
        # Users only see their own alerts
        alerts = await state_manager.get_user_alerts(
            current_user.user_id, status, severity, limit
        )
    
    return {
        "alerts": [_format_alert(a) for a in alerts],
        "total": len(alerts),
        "timestamp": datetime.utcnow().isoformat()
    }


# Helper functions
def _format_portfolio_risk(portfolio_risk) -> PortfolioRiskResponse:
    """Format portfolio risk for response"""
    return PortfolioRiskResponse(
        user_id=portfolio_risk.user_id,
        total_positions=portfolio_risk.total_positions,
        total_value=str(portfolio_risk.total_value),
        total_collateral=str(portfolio_risk.total_collateral),
        total_unrealized_pnl=str(portfolio_risk.total_unrealized_pnl),
        margin_usage=str(portfolio_risk.margin_usage),
        portfolio_var=str(portfolio_risk.portfolio_var),
        portfolio_leverage=str(portfolio_risk.portfolio_leverage),
        max_concentration=str(portfolio_risk.max_concentration),
        risk_score=portfolio_risk.risk_score,
        alerts=[_format_alert(a) for a in portfolio_risk.alerts],
        timestamp=portfolio_risk.timestamp.isoformat()
    )


def _determine_risk_level(risk_metrics: Dict) -> RiskLevel:
    """Determine risk level based on metrics"""
    margin_ratio = float(risk_metrics.get("margin_ratio", 0))
    
    if margin_ratio < 1.5:
        return RiskLevel.CRITICAL
    elif margin_ratio < 2.0:
        return RiskLevel.HIGH
    elif margin_ratio < 3.0:
        return RiskLevel.MEDIUM
    else:
        return RiskLevel.LOW


def _generate_position_alerts(risk_metrics: Dict, risk_level: RiskLevel) -> List[Dict]:
    """Generate alerts based on position risk"""
    alerts = []
    
    margin_ratio = float(risk_metrics.get("margin_ratio", 0))
    if margin_ratio < 1.5:
        alerts.append({
            "type": "margin_call",
            "severity": "critical",
            "message": f"Margin ratio critical: {margin_ratio:.2f}",
            "action_required": True
        })
    elif margin_ratio < 2.0:
        alerts.append({
            "type": "margin_warning",
            "severity": "high",
            "message": f"Margin ratio low: {margin_ratio:.2f}",
            "action_required": False
        })
    
    return alerts


def _format_alert(alert) -> Dict[str, Any]:
    """Format alert for response"""
    return {
        "alert_id": alert.alert_id,
        "type": alert.alert_type,
        "severity": alert.severity,
        "message": alert.message,
        "metadata": alert.metadata,
        "created_at": alert.created_at.isoformat(),
        "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None
    } 