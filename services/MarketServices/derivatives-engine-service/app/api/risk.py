"""
Risk Management API

Endpoints for oracle-powered real-time risk calculations and monitoring.
"""

from fastapi import APIRouter, HTTPException, Depends, WebSocket, WebSocketDisconnect
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.risk.oracle_risk_engine import OracleRiskEngine, RiskMetric
from app.models.position import Position
from app.auth import get_current_user
from platformq_shared import ProcessingStatus
import asyncio
import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/risk",
    tags=["risk"]
)

# Initialize risk engine
risk_engine = OracleRiskEngine()

# Store WebSocket connections for real-time monitoring
active_connections: Dict[str, List[WebSocket]] = {}


class CalculatePortfolioRiskRequest(BaseModel):
    """Request to calculate portfolio risk"""
    position_ids: Optional[List[str]] = Field(None, description="Specific positions to analyze, or all if empty")


class MarginRequirementsRequest(BaseModel):
    """Request to calculate margin requirements"""
    position_ids: List[str] = Field(..., min_items=1)
    stress_scenario: Optional[str] = Field(None, pattern="^(market_crash|flash_crash|black_swan)$")


class LiquidationPredictionRequest(BaseModel):
    """Request to predict liquidation scenarios"""
    position_ids: List[str] = Field(..., min_items=1)
    time_horizons: Optional[List[int]] = Field([1, 4, 24], description="Time horizons in hours")


@router.post("/portfolio-risk")
async def calculate_portfolio_risk(
    request: CalculatePortfolioRiskRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Calculate comprehensive risk metrics for user's portfolio
    
    Uses real-time oracle price feeds to calculate VaR, CVaR, Sharpe ratio,
    liquidation risks, and other key metrics.
    """
    try:
        # Get user positions
        positions = await _get_user_positions(
            current_user["tenant_id"],
            current_user["user_id"],
            request.position_ids
        )
        
        if not positions:
            return {
                "status": "success",
                "message": "No positions found",
                "risk_profile": None
            }
        
        # Calculate risk
        result = await risk_engine.calculate_portfolio_risk(
            tenant_id=current_user["tenant_id"],
            user_id=current_user["user_id"],
            positions=positions
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


@router.post("/margin-requirements")
async def calculate_margin_requirements(
    request: MarginRequirementsRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Calculate dynamic margin requirements based on real-time risk
    
    Takes into account current market volatility, correlations, and
    optional stress scenarios.
    """
    try:
        # Get positions
        positions = await _get_user_positions(
            current_user["tenant_id"],
            current_user["user_id"],
            request.position_ids
        )
        
        if not positions:
            raise HTTPException(status_code=404, detail="Positions not found")
        
        # Calculate margins
        result = await risk_engine.calculate_margin_requirements(
            tenant_id=current_user["tenant_id"],
            positions=positions,
            stress_scenario=request.stress_scenario
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "margins": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/liquidation-prediction")
async def predict_liquidation_scenarios(
    request: LiquidationPredictionRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Predict liquidation scenarios using ML and oracle data
    
    Analyzes potential liquidation risks across different time horizons
    and provides hedging recommendations.
    """
    try:
        # Get positions
        positions = await _get_user_positions(
            current_user["tenant_id"],
            current_user["user_id"],
            request.position_ids
        )
        
        if not positions:
            raise HTTPException(status_code=404, detail="Positions not found")
        
        # Predict liquidation scenarios
        result = await risk_engine.predict_liquidation_scenarios(
            tenant_id=current_user["tenant_id"],
            positions=positions,
            time_horizons=request.time_horizons
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "predictions": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/real-time-metrics")
async def get_real_time_risk_metrics(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current real-time risk metrics for user's portfolio
    """
    try:
        # Get all user positions
        positions = await _get_user_positions(
            current_user["tenant_id"],
            current_user["user_id"],
            None
        )
        
        if not positions:
            return {
                "status": "success",
                "message": "No positions",
                "metrics": None
            }
        
        # Get quick risk snapshot
        result = await risk_engine.calculate_portfolio_risk(
            tenant_id=current_user["tenant_id"],
            user_id=current_user["user_id"],
            positions=positions
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            risk_profile = result.data["risk_profile"]
            
            return {
                "status": "success",
                "metrics": {
                    "risk_score": risk_profile["risk_score"],
                    "margin_ratio": risk_profile["margin_ratio"],
                    "var_95": risk_profile["var_95"],
                    "alerts": len(risk_profile["alerts"]),
                    "critical_alerts": len([
                        a for a in risk_profile["alerts"]
                        if a.get("type") == "critical"
                    ]),
                    "timestamp": risk_profile["timestamp"]
                }
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.websocket("/ws/monitor")
async def monitor_risk_websocket(
    websocket: WebSocket,
    token: str
):
    """
    WebSocket endpoint for real-time risk monitoring
    
    Streams risk updates every 10 seconds or when significant changes occur.
    """
    await websocket.accept()
    
    try:
        # Verify token and get user
        user = await _verify_websocket_token(token)
        if not user:
            await websocket.close(code=1008, reason="Invalid token")
            return
        
        user_id = user["user_id"]
        
        # Add to active connections
        if user_id not in active_connections:
            active_connections[user_id] = []
        active_connections[user_id].append(websocket)
        
        # Start monitoring
        async def risk_callback(risk_data):
            """Send risk updates to client"""
            try:
                await websocket.send_json({
                    "type": "risk_update",
                    "data": risk_data,
                    "timestamp": datetime.utcnow().isoformat()
                })
            except:
                pass
        
        # Monitor risk in real-time
        monitoring_task = asyncio.create_task(
            risk_engine.monitor_real_time_risk(
                tenant_id=user["tenant_id"],
                user_id=user_id,
                callback=risk_callback
            )
        )
        
        # Keep connection alive
        while True:
            try:
                # Wait for client messages (ping/pong)
                message = await websocket.receive_text()
                if message == "ping":
                    await websocket.send_text("pong")
            except WebSocketDisconnect:
                break
                
    except Exception as e:
        logger.error(f"WebSocket error: {str(e)}")
    finally:
        # Clean up
        if user_id in active_connections:
            active_connections[user_id].remove(websocket)
            if not active_connections[user_id]:
                del active_connections[user_id]
        
        # Cancel monitoring task
        if 'monitoring_task' in locals():
            monitoring_task.cancel()


@router.get("/risk-alerts")
async def get_risk_alerts(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current risk alerts for user's portfolio
    """
    try:
        # Get positions and calculate risk
        positions = await _get_user_positions(
            current_user["tenant_id"],
            current_user["user_id"],
            None
        )
        
        if not positions:
            return {
                "status": "success",
                "alerts": []
            }
        
        result = await risk_engine.calculate_portfolio_risk(
            tenant_id=current_user["tenant_id"],
            user_id=current_user["user_id"],
            positions=positions
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            alerts = result.data["risk_profile"]["alerts"]
            
            # Sort by severity
            severity_order = {"critical": 0, "warning": 1, "info": 2}
            alerts.sort(key=lambda x: severity_order.get(x.get("type", "info"), 3))
            
            return {
                "status": "success",
                "alerts": alerts,
                "total": len(alerts),
                "critical": len([a for a in alerts if a.get("type") == "critical"]),
                "warnings": len([a for a in alerts if a.get("type") == "warning"])
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/historical-risk/{days}")
async def get_historical_risk_metrics(
    days: int,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get historical risk metrics for the specified number of days
    """
    try:
        # This would fetch historical risk data from storage
        # For now, return mock data
        
        historical_data = []
        for i in range(days):
            historical_data.append({
                "date": (datetime.utcnow() - timedelta(days=days-i)).isoformat(),
                "risk_score": 50 + (i % 30),  # Mock variation
                "var_95": str(10000 + (i * 100)),
                "margin_ratio": str(Decimal("0.5") + Decimal(str(i * 0.01))),
                "alerts": i % 5  # Mock alert count
            })
        
        return {
            "status": "success",
            "days": days,
            "data": historical_data
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stress-test")
async def run_stress_test(
    scenario: str = "market_crash",
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Run stress test on portfolio using predefined scenarios
    """
    try:
        # Get positions
        positions = await _get_user_positions(
            current_user["tenant_id"],
            current_user["user_id"],
            None
        )
        
        if not positions:
            return {
                "status": "success",
                "message": "No positions to stress test"
            }
        
        # Calculate stress margins
        result = await risk_engine.calculate_margin_requirements(
            tenant_id=current_user["tenant_id"],
            positions=positions,
            stress_scenario=scenario
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            stress_data = result.data
            
            # Calculate impact
            base_margin = float(sum(stress_data["base_margins"].values()))
            stress_margin = float(stress_data["total_required"])
            margin_increase = (
                (stress_margin - base_margin) / base_margin * 100
                if base_margin > 0 else 0
            )
            
            return {
                "status": "success",
                "scenario": scenario,
                "results": {
                    "base_margin_required": base_margin,
                    "stress_margin_required": stress_margin,
                    "margin_increase_percent": margin_increase,
                    "positions_affected": len(stress_data["stress_margins"]),
                    "market_conditions": stress_data["market_conditions"]
                }
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def _get_user_positions(
    tenant_id: str,
    user_id: str,
    position_ids: Optional[List[str]]
) -> List[Position]:
    """Get user positions from database"""
    # This would fetch from database
    # For now, return mock positions
    
    mock_positions = [
        Position(
            position_id="pos_1",
            user_id=user_id,
            asset_id="BTC-USD",
            quantity=Decimal("0.5"),
            entry_price=Decimal("45000"),
            current_price=Decimal("47000"),
            initial_margin=Decimal("5000"),
            used_margin=Decimal("4000"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("1000"),
            created_at=datetime.utcnow()
        ),
        Position(
            position_id="pos_2",
            user_id=user_id,
            asset_id="ETH-USD",
            quantity=Decimal("10"),
            entry_price=Decimal("3000"),
            current_price=Decimal("3200"),
            initial_margin=Decimal("3000"),
            used_margin=Decimal("2800"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("2000"),
            created_at=datetime.utcnow()
        )
    ]
    
    if position_ids:
        return [p for p in mock_positions if p.position_id in position_ids]
    return mock_positions


async def _verify_websocket_token(token: str) -> Optional[Dict[str, Any]]:
    """Verify WebSocket authentication token"""
    # This would verify JWT token
    # For now, return mock user
    return {
        "user_id": "user_123",
        "tenant_id": "tenant_1"
    } 