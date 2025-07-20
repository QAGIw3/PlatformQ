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
    
    # ML assessment
    ml_assessment: Optional[Dict] = None


class PortfolioRiskResponse(BaseModel):
    """Portfolio risk analysis response"""
    trader_id: str
    positions: List[Dict]
    total_value: Decimal
    margin_level: Decimal
    risk_metrics: Dict
    stress_test_results: Optional[Dict] = None


class MarketRiskRequest(BaseModel):
    """Request for market risk assessment"""
    market_id: str
    include_predictions: bool = True
    include_recommendations: bool = True


class MarketRiskResponse(BaseModel):
    """Market risk assessment response"""
    market_id: str
    timestamp: datetime
    current_volatility: str
    predicted_volatility: str
    anomaly_score: float
    var_95: str
    var_99: str
    liquidity_score: str
    risk_level: str
    warnings: List[str]
    recommended_params: Dict[str, str]


class PositionRiskRequest(BaseModel):
    """Request for position risk assessment"""
    position_id: str
    include_stress_tests: bool = True
    include_ml_predictions: bool = True


class PositionRiskResponse(BaseModel):
    """Position risk assessment response"""
    position_id: str
    liquidation_probability: str
    expected_shortfall: str
    margin_utilization: str
    health_factor: str
    risk_score: float
    stress_test_results: List[Dict]
    recommendations: List[str]
    market_risk: Dict


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
            largest_position_pct=result.risk_metrics.largest_position_pct,
            ml_assessment=result.ml_assessment
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


@router.post("/market/assess", response_model=MarketRiskResponse)
async def assess_market_risk(
    request: MarketRiskRequest,
    risk_monitor: RiskMonitor = Depends(get_risk_monitor),
    current_user: Dict = Depends(get_current_user)
):
    """Get ML-based market risk assessment"""
    try:
        # Get market data
        market_data = await risk_monitor._get_enriched_market_data(request.market_id)
        if not market_data:
            raise HTTPException(status_code=404, detail="Market data not available")
        
        # Get ML assessment
        market_risk = await risk_monitor.ml_engine.assess_market_risk(
            request.market_id,
            market_data
        )
        
        return MarketRiskResponse(
            market_id=market_risk.market_id,
            timestamp=market_risk.timestamp,
            current_volatility=str(market_risk.current_volatility),
            predicted_volatility=str(market_risk.predicted_volatility),
            anomaly_score=market_risk.anomaly_score,
            var_95=str(market_risk.var_95),
            var_99=str(market_risk.var_99),
            liquidity_score=str(market_risk.liquidity_score),
            risk_level=market_risk.risk_level,
            warnings=market_risk.warnings if request.include_predictions else [],
            recommended_params={k: str(v) for k, v in market_risk.recommended_params.items()} if request.include_recommendations else {}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/position/assess", response_model=PositionRiskResponse)
async def assess_position_risk(
    request: PositionRiskRequest,
    risk_monitor: RiskMonitor = Depends(get_risk_monitor),
    current_user: Dict = Depends(get_current_user)
):
    """Get ML-based position risk assessment"""
    try:
        # Find position in portfolios
        position_dict = None
        position = None
        
        for portfolio in risk_monitor.trader_portfolios.values():
            for pos in portfolio.positions:
                if pos.position_id == request.position_id:
                    position = pos
                    position_dict = risk_monitor._position_to_dict(pos)
                    break
            if position_dict:
                break
        
        if not position_dict:
            raise HTTPException(status_code=404, detail="Position not found")
        
        # Verify permissions
        if current_user["user_id"] != position.trader_id and "admin" not in current_user.get("roles", []):
            raise HTTPException(status_code=403, detail="Insufficient permissions")
        
        # Get market data
        market_data = await risk_monitor._get_enriched_market_data(position.market_id)
        if not market_data:
            raise HTTPException(status_code=404, detail="Market data not available")
        
        # Get ML assessments
        market_risk = await risk_monitor.ml_engine.assess_market_risk(
            position.market_id,
            market_data
        )
        
        position_risk = await risk_monitor.ml_engine.assess_position_risk(
            position_dict,
            market_risk
        )
        
        return PositionRiskResponse(
            position_id=position_risk.position_id,
            liquidation_probability=str(position_risk.liquidation_probability),
            expected_shortfall=str(position_risk.expected_shortfall),
            margin_utilization=str(position_risk.margin_utilization),
            health_factor=str(position_risk.health_factor),
            risk_score=position_risk.risk_score,
            stress_test_results=position_risk.stress_test_results if request.include_stress_tests else [],
            recommendations=position_risk.recommendations if request.include_ml_predictions else [],
            market_risk=market_risk.to_dict()
        )
        
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