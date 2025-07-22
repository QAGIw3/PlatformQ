"""Real-time risk monitoring API endpoints."""

import asyncio
import logging
from typing import Optional, List, Dict
from datetime import datetime
from decimal import Decimal
from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

from platformq_shared import (
    get_current_user,
    require_roles,
    UserRole,
    AuthenticatedUser,
    monitor_operation
)

from ..dependencies import get_risk_monitor, get_settings
from ..core.risk_monitor import RiskMonitor, MonitoringResult
from ..models.risk import (
    RiskLimitsRequest,
    RiskCheckResponse,
    MarketRiskAssessmentResponse,
    PositionRiskAssessmentResponse,
    RiskMonitoringStatusResponse
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/monitoring", tags=["monitoring"])


@router.post("/users/{user_id}/start")
@monitor_operation("monitoring_start")
async def start_monitoring(
    user_id: str,
    limits: Optional[RiskLimitsRequest] = None,
    current_user: AuthenticatedUser = Depends(get_current_user),
    risk_monitor: RiskMonitor = Depends(get_risk_monitor)
):
    """Start monitoring a user's risk."""
    # Check permissions
    if current_user.user_id != user_id and UserRole.ADMIN not in current_user.roles:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    # Convert limits to dict if provided
    limits_dict = limits.dict() if limits else None
    
    # Add user to monitoring
    await risk_monitor.add_user_monitoring(user_id, limits_dict)
    
    return {"status": "monitoring_started", "user_id": user_id}


@router.post("/users/{user_id}/stop")
@monitor_operation("monitoring_stop")
async def stop_monitoring(
    user_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
    risk_monitor: RiskMonitor = Depends(get_risk_monitor)
):
    """Stop monitoring a user's risk."""
    # Check permissions
    if current_user.user_id != user_id and UserRole.ADMIN not in current_user.roles:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    await risk_monitor.remove_user_monitoring(user_id)
    
    return {"status": "monitoring_stopped", "user_id": user_id}


@router.get("/users/{user_id}/check", response_model=RiskCheckResponse)
@monitor_operation("risk_check")
async def check_user_risk(
    user_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
    risk_monitor: RiskMonitor = Depends(get_risk_monitor)
):
    """Check current risk status for a user."""
    # Check permissions
    if current_user.user_id != user_id and UserRole.ADMIN not in current_user.roles:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    
    # Check if user is monitored
    if user_id not in risk_monitor.monitored_users:
        # Add to monitoring with default limits
        await risk_monitor.add_user_monitoring(user_id)
    
    # Perform risk check
    try:
        result: MonitoringResult = await risk_monitor.check_user_risk(user_id)
        
        return RiskCheckResponse(
            user_id=user_id,
            timestamp=result.timestamp,
            margin_level=result.margin_status.margin_level,
            margin_used=result.margin_status.margin_used,
            free_margin=result.margin_status.free_margin,
            equity=result.margin_status.equity,
            health_status=result.margin_status.health_status,
            alerts=result.alerts,
            violations=[v["type"] for v in result.violations],
            actions_required=result.actions_required,
            var_95=result.risk_metrics.get("var_95"),
            total_exposure=result.risk_metrics.get("total_exposure"),
            net_exposure=result.risk_metrics.get("net_exposure"),
            largest_position_pct=result.risk_metrics.get("largest_position_pct"),
            ml_assessment=result.ml_assessment
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status", response_model=RiskMonitoringStatusResponse)
@require_roles([UserRole.ADMIN])
async def get_monitoring_status(
    risk_monitor: RiskMonitor = Depends(get_risk_monitor)
):
    """Get overall monitoring status."""
    # Calculate stats
    total_positions = 0
    total_margin_used = 0
    users_at_risk = 0
    ml_predictions_count = 0
    
    for user_id in risk_monitor.monitored_users:
        portfolio = risk_monitor.user_portfolios.get(user_id)
        if portfolio:
            total_positions += len(portfolio.get("positions", []))
            total_margin_used += float(portfolio.get("total_margin_used", 0))
            
        state = risk_monitor.user_states.get(user_id)
        if state and (state.has_high_alerts or state.has_critical_alerts):
            users_at_risk += 1
    
    # Count ML predictions in cache
    ml_predictions_count = len(risk_monitor.market_data_cache)
    
    return RiskMonitoringStatusResponse(
        monitored_users=len(risk_monitor.monitored_users),
        total_positions=total_positions,
        total_margin_used=str(Decimal(str(total_margin_used))),
        users_at_risk=users_at_risk,
        ml_predictions_active=ml_predictions_count,
        cache_status={
            "price_cache": len(risk_monitor.price_cache),
            "portfolio_cache": len(risk_monitor.user_portfolios),
            "market_data_cache": len(risk_monitor.market_data_cache)
        },
        ml_engine_status={
            "models_loaded": sum(1 for m in risk_monitor.ml_engine.models.values() if m is not None),
            "features_tracked": 9  # Number of features in ML model
        }
    )


@router.post("/market/{market_id}/assess", response_model=MarketRiskAssessmentResponse)
@monitor_operation("market_risk_assessment")
async def assess_market_risk(
    market_id: str,
    current_user: AuthenticatedUser = Depends(get_current_user),
    risk_monitor: RiskMonitor = Depends(get_risk_monitor)
):
    """Get ML-based market risk assessment."""
    # Get enriched market data
    market_data = await risk_monitor._get_enriched_market_data(market_id)
    
    if not market_data:
        raise HTTPException(status_code=404, detail="Market data not available")
    
    # Get ML assessment
    market_risk = await risk_monitor.ml_engine.assess_market_risk(market_id, market_data)
    
    return MarketRiskAssessmentResponse(
        market_id=market_id,
        timestamp=datetime.utcnow(),
        risk_assessment=market_risk.to_dict(),
        recommended_parameters={k: str(v) for k, v in market_risk.recommended_params.items()},
        warnings=market_risk.warnings
    )


@router.post("/position/{position_id}/assess", response_model=PositionRiskAssessmentResponse)
@monitor_operation("position_risk_assessment")
async def assess_position_risk(
    position_id: str,
    position_data: Dict,
    current_user: AuthenticatedUser = Depends(get_current_user),
    risk_monitor: RiskMonitor = Depends(get_risk_monitor)
):
    """Get ML-based position risk assessment."""
    # Get market data
    market_id = position_data.get("market_id")
    if not market_id:
        raise HTTPException(status_code=400, detail="market_id required in position_data")
    
    market_data = await risk_monitor._get_enriched_market_data(market_id)
    if not market_data:
        raise HTTPException(status_code=404, detail="Market data not available")
    
    # Get market risk assessment
    market_risk = await risk_monitor.ml_engine.assess_market_risk(market_id, market_data)
    
    # Get position risk assessment
    position_risk = await risk_monitor.ml_engine.assess_position_risk(
        position_data,
        market_risk,
        {"historical_liquidation_rate": 0}  # Default user profile
    )
    
    return PositionRiskAssessmentResponse(
        position_id=position_id,
        timestamp=datetime.utcnow(),
        risk_assessment=position_risk.to_dict(),
        stress_test_results=position_risk.stress_test_results,
        recommendations=position_risk.recommendations
    )


@router.websocket("/ws/{user_id}")
async def websocket_monitoring(
    websocket: WebSocket,
    user_id: str,
    risk_monitor: RiskMonitor = Depends(get_risk_monitor)
):
    """WebSocket endpoint for real-time risk updates."""
    await websocket.accept()
    
    # Add user to monitoring if not already
    if user_id not in risk_monitor.monitored_users:
        await risk_monitor.add_user_monitoring(user_id)
    
    try:
        while True:
            # Check user risk
            try:
                result = await risk_monitor.check_user_risk(user_id)
                
                # Send update
                await websocket.send_json({
                    "type": "risk_update",
                    "timestamp": result.timestamp.isoformat(),
                    "margin_level": float(result.margin_status.margin_level),
                    "health_status": result.margin_status.health_status,
                    "alerts": result.alerts,
                    "violations": [v["type"] for v in result.violations]
                })
                
            except Exception as e:
                await websocket.send_json({
                    "type": "error",
                    "message": str(e)
                })
            
            # Wait before next update
            await asyncio.sleep(risk_monitor.settings.RISK_CALCULATION_INTERVAL_SECONDS)
            
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await websocket.close()


# Additional monitoring endpoints

@router.get("/alerts")
@require_roles([UserRole.ADMIN])
async def get_all_alerts(
    severity: Optional[str] = Query(None, description="Filter by severity"),
    limit: int = Query(100, le=1000),
    risk_monitor: RiskMonitor = Depends(get_risk_monitor)
):
    """Get all active alerts across monitored users."""
    all_alerts = []
    
    for user_id, state in risk_monitor.user_states.items():
        for alert in state.active_alerts:
            if severity is None or alert.get("severity") == severity:
                all_alerts.append({
                    "user_id": user_id,
                    "timestamp": state.timestamp.isoformat(),
                    **alert
                })
    
    # Sort by timestamp and severity
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    all_alerts.sort(
        key=lambda x: (
            severity_order.get(x.get("severity", "low"), 3),
            x["timestamp"]
        )
    )
    
    return all_alerts[:limit]


@router.get("/metrics")
async def get_metrics(
    risk_monitor: RiskMonitor = Depends(get_risk_monitor)
):
    """Get Prometheus metrics."""
    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
    from fastapi.responses import Response
    
    metrics = generate_latest()
    return Response(content=metrics, media_type=CONTENT_TYPE_LATEST) 