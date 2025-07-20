"""Risk management API endpoints"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from decimal import Decimal
from typing import Optional, List, Dict
from datetime import datetime

from platformq_trading_common.risk.models import RiskLimits
from ..core.risk_monitor import RiskMonitor, MonitoringResult
from ..dependencies import get_risk_monitor, get_current_user


router = APIRouter(prefix="/api/v1/risk", tags=["risk"])


class SetRiskLimitsRequest(BaseModel):
    """Request to set risk limits for a trader"""
    max_position_size: Decimal = Field(..., gt=0)
    max_leverage: Decimal = Field(..., ge=1)
    max_loss_per_trade: Decimal = Field(..., gt=0)
    max_daily_loss: Decimal = Field(..., gt=0)
    max_open_positions: int = Field(..., ge=1)
    min_margin_level: Decimal = Field(..., ge=100)  # Minimum 100%
    concentration_limit: Decimal = Field(..., ge=0, le=100)  # Percentage


class RiskCheckResponse(BaseModel):
    """Risk check response"""
    trader_id: str
    timestamp: datetime
    margin_level: Decimal
    margin_used: Decimal
    free_margin: Decimal
    equity: Decimal
    health_status: str
    alerts: List[Dict]
    violations: List[Dict]
    actions_required: List[Dict]
    
    # Risk metrics
    var_95: Decimal
    total_exposure: Decimal
    net_exposure: Decimal
    largest_position_pct: float


class PortfolioRiskResponse(BaseModel):
    """Portfolio risk analysis response"""
    trader_id: str
    positions: List[Dict]
    total_value: Decimal
    margin_level: Decimal
    risk_metrics: Dict
    stress_test_results: Optional[Dict] = None


@router.post("/limits/{trader_id}")
async def set_risk_limits(
    trader_id: str,
    request: SetRiskLimitsRequest,
    risk_monitor: RiskMonitor = Depends(get_risk_monitor),
    current_user: Dict = Depends(get_current_user)
):
    """Set risk limits for a trader"""
    try:
        # Verify permissions
        if current_user["user_id"] != trader_id and "admin" not in current_user.get("roles", []):
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Create risk limits
        limits = RiskLimits(
            max_position_size=request.max_position_size,
            max_leverage=request.max_leverage,
            max_loss_per_trade=request.max_loss_per_trade,
            max_daily_loss=request.max_daily_loss,
            max_open_positions=request.max_open_positions,
            min_margin_level=request.min_margin_level,
            concentration_limit=request.concentration_limit
        )
        
        # Add trader to monitoring with limits
        await risk_monitor.add_trader_monitoring(trader_id, limits)
        
        return {
            "success": True,
            "message": f"Risk limits set for trader {trader_id}",
            "limits": {
                "max_position_size": str(limits.max_position_size),
                "max_leverage": str(limits.max_leverage),
                "max_loss_per_trade": str(limits.max_loss_per_trade),
                "max_daily_loss": str(limits.max_daily_loss),
                "max_open_positions": limits.max_open_positions,
                "min_margin_level": str(limits.min_margin_level),
                "concentration_limit": str(limits.concentration_limit)
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/check/{trader_id}", response_model=RiskCheckResponse)
async def check_trader_risk(
    trader_id: str,
    risk_monitor: RiskMonitor = Depends(get_risk_monitor),
    current_user: Dict = Depends(get_current_user)
):
    """Check current risk status for a trader"""
    try:
        # Verify permissions
        if current_user["user_id"] != trader_id and "admin" not in current_user.get("roles", []):
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Check if trader is monitored
        if trader_id not in risk_monitor.monitored_traders:
            # Add to monitoring with default limits
            await risk_monitor.add_trader_monitoring(trader_id)
        
        # Perform risk check
        result: MonitoringResult = await risk_monitor.check_trader_risk(trader_id)
        
        return RiskCheckResponse(
            trader_id=trader_id,
            timestamp=result.timestamp,
            margin_level=result.margin_status.margin_level,
            margin_used=result.margin_status.margin_used,
            free_margin=result.margin_status.free_margin,
            equity=result.margin_status.equity,
            health_status=result.margin_status.health_status,
            alerts=result.alerts,
            violations=list(result.violations.keys()),
            actions_required=result.actions_required,
            var_95=result.risk_metrics.var_95,
            total_exposure=result.risk_metrics.total_exposure,
            net_exposure=result.risk_metrics.net_exposure,
            largest_position_pct=result.risk_metrics.largest_position_pct
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{trader_id}", response_model=PortfolioRiskResponse)
async def get_portfolio_risk(
    trader_id: str,
    include_stress_test: bool = Query(default=False),
    risk_monitor: RiskMonitor = Depends(get_risk_monitor),
    current_user: Dict = Depends(get_current_user)
):
    """Get detailed portfolio risk analysis"""
    try:
        # Verify permissions
        if current_user["user_id"] != trader_id and "admin" not in current_user.get("roles", []):
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Get portfolio
        portfolio = risk_monitor.trader_portfolios.get(trader_id)
        if not portfolio:
            await risk_monitor._refresh_trader_portfolio(trader_id)
            portfolio = risk_monitor.trader_portfolios.get(trader_id)
        
        if not portfolio:
            raise HTTPException(status_code=404, detail="Portfolio not found")
        
        # Get risk metrics
        risk_metrics = await risk_monitor._calculate_risk_metrics(portfolio)
        margin_status = risk_monitor._calculate_margin_status(portfolio)
        
        # Prepare positions data
        positions_data = []
        for position in portfolio.positions:
            positions_data.append({
                "position_id": position.position_id,
                "market_id": position.market_id,
                "side": position.side.value,
                "size": str(position.size),
                "entry_price": str(position.entry_price),
                "mark_price": str(position.mark_price),
                "unrealized_pnl": str(position.unrealized_pnl),
                "margin_used": str(position.margin_used),
                "leverage": str(position.leverage)
            })
        
        response = PortfolioRiskResponse(
            trader_id=trader_id,
            positions=positions_data,
            total_value=portfolio.total_value,
            margin_level=margin_status.margin_level,
            risk_metrics={
                "var_95": str(risk_metrics.var_95),
                "cvar_95": str(risk_metrics.cvar_95),
                "sharpe_ratio": risk_metrics.sharpe_ratio,
                "max_drawdown": risk_metrics.max_drawdown,
                "total_exposure": str(risk_metrics.total_exposure),
                "net_exposure": str(risk_metrics.net_exposure),
                "largest_position_pct": risk_metrics.largest_position_pct
            }
        )
        
        # TODO: Add stress test results if requested
        if include_stress_test:
            response.stress_test_results = {
                "scenarios": [],
                "worst_case_loss": "0"
            }
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
async def get_risk_alerts(
    severity: Optional[str] = Query(None, pattern="^(low|medium|high|critical)$"),
    limit: int = Query(default=100, ge=1, le=1000),
    risk_monitor: RiskMonitor = Depends(get_risk_monitor),
    current_user: Dict = Depends(get_current_user)
):
    """Get risk alerts for the user or all users (admin)"""
    try:
        alerts = []
        
        # Determine which traders to check
        if "admin" in current_user.get("roles", []):
            trader_ids = list(risk_monitor.monitored_traders)
        else:
            trader_ids = [current_user["user_id"]]
        
        # Collect alerts
        for trader_id in trader_ids:
            state = risk_monitor.trader_states.get(trader_id)
            if state:
                for alert in state.active_alerts:
                    if not severity or alert.get("level").value == severity:
                        alerts.append({
                            "trader_id": trader_id,
                            "timestamp": state.last_check.isoformat(),
                            **alert
                        })
        
        # Sort by timestamp and limit
        alerts.sort(key=lambda x: x["timestamp"], reverse=True)
        alerts = alerts[:limit]
        
        return {
            "alerts": alerts,
            "count": len(alerts),
            "filter": {"severity": severity} if severity else {}
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/monitoring/start/{trader_id}")
async def start_monitoring(
    trader_id: str,
    risk_monitor: RiskMonitor = Depends(get_risk_monitor),
    current_user: Dict = Depends(get_current_user)
):
    """Start risk monitoring for a trader"""
    try:
        # Verify permissions
        if current_user["user_id"] != trader_id and "admin" not in current_user.get("roles", []):
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Add to monitoring
        await risk_monitor.add_trader_monitoring(trader_id)
        
        return {
            "success": True,
            "message": f"Started monitoring trader {trader_id}"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/monitoring/stop/{trader_id}")
async def stop_monitoring(
    trader_id: str,
    risk_monitor: RiskMonitor = Depends(get_risk_monitor),
    current_user: Dict = Depends(get_current_user)
):
    """Stop risk monitoring for a trader"""
    try:
        # Verify permissions
        if current_user["user_id"] != trader_id and "admin" not in current_user.get("roles", []):
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Remove from monitoring
        await risk_monitor.remove_trader_monitoring(trader_id)
        
        return {
            "success": True,
            "message": f"Stopped monitoring trader {trader_id}"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/monitoring/status")
async def get_monitoring_status(
    risk_monitor: RiskMonitor = Depends(get_risk_monitor),
    current_user: Dict = Depends(get_current_user)
):
    """Get monitoring status"""
    try:
        # Admin can see all, users only see their own
        if "admin" in current_user.get("roles", []):
            monitored = list(risk_monitor.monitored_traders)
        else:
            user_id = current_user["user_id"]
            monitored = [user_id] if user_id in risk_monitor.monitored_traders else []
        
        # Get status for each monitored trader
        traders_status = []
        for trader_id in monitored:
            state = risk_monitor.trader_states.get(trader_id)
            if state:
                traders_status.append({
                    "trader_id": trader_id,
                    "last_check": state.last_check.isoformat(),
                    "health_status": state.margin_status.health_status,
                    "margin_level": str(state.margin_status.margin_level),
                    "alert_count": len(state.active_alerts),
                    "has_critical_alerts": state.has_critical_alerts
                })
        
        return {
            "monitored_traders": len(monitored),
            "traders": traders_status
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 