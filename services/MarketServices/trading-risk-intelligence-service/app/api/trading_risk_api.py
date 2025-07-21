"""
Trading Risk API Endpoints

Provides graph-based risk analysis for trading activities.
"""

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field

from ..core.trading_risk_network import (
    TradingRiskNetwork, 
    RiskPropagationType,
    TraderRiskLevel,
    RiskPropagationResult
)
from platformq_shared.api.deps import get_current_tenant_and_user

router = APIRouter(prefix="/trading-risk", tags=["trading-risk"])


class TraderRiskUpdate(BaseModel):
    """Trader risk update request"""
    trader_id: str = Field(..., description="Trader ID")
    risk_score: float = Field(..., ge=0, le=1, description="Risk score (0-1)")
    exposure: float = Field(..., description="Total exposure")
    leverage: float = Field(..., ge=0, description="Current leverage")
    margin_utilization: float = Field(..., ge=0, le=1, description="Margin utilization (0-1)")
    position_count: int = Field(..., ge=0, description="Number of open positions")
    liquidity: float = Field(..., description="Available liquidity")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class TradingRelationship(BaseModel):
    """Trading relationship between traders"""
    from_trader: str = Field(..., description="Source trader ID")
    to_trader: str = Field(..., description="Target trader ID")
    relationship_type: str = Field(..., description="Type of relationship")
    strength: float = Field(..., ge=0, le=1, description="Relationship strength (0-1)")
    exposure_amount: float = Field(..., description="Exposure amount")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")


class RiskPropagationRequest(BaseModel):
    """Risk propagation analysis request"""
    source_trader: str = Field(..., description="Source trader ID")
    risk_event: Dict[str, Any] = Field(..., description="Risk event details")


class CascadeSimulationRequest(BaseModel):
    """Cascade failure simulation request"""
    failing_trader: str = Field(..., description="Trader ID that fails")
    failure_type: str = Field("liquidation", description="Type of failure")


def get_risk_network(request) -> TradingRiskNetwork:
    """Get risk network instance"""
    return request.app.state.trading_risk_network


@router.post("/traders/{trader_id}/risk")
async def update_trader_risk(
    trader_id: str,
    risk_update: TraderRiskUpdate,
    background_tasks: BackgroundTasks,
    risk_network: TradingRiskNetwork = Depends(get_risk_network),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Update trader risk profile"""
    try:
        # Prepare risk metrics
        risk_metrics = {
            'risk_score': risk_update.risk_score,
            'exposure': str(risk_update.exposure),
            'leverage': risk_update.leverage,
            'margin_utilization': risk_update.margin_utilization,
            'position_count': risk_update.position_count,
            'liquidity': str(risk_update.liquidity),
            'tenant_id': context['tenant_id']
        }
        
        if risk_update.metadata:
            risk_metrics.update(risk_update.metadata)
        
        # Update in background
        background_tasks.add_task(
            risk_network.update_trader_risk,
            trader_id,
            risk_metrics
        )
        
        return {
            "status": "accepted",
            "trader_id": trader_id,
            "risk_score": risk_update.risk_score,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/relationships")
async def add_trading_relationship(
    relationship: TradingRelationship,
    background_tasks: BackgroundTasks,
    risk_network: TradingRiskNetwork = Depends(get_risk_network),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Add or update trading relationship"""
    try:
        # Validate relationship type
        try:
            rel_type = RiskPropagationType(relationship.relationship_type)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid relationship type. Must be one of: {[t.value for t in RiskPropagationType]}"
            )
        
        # Prepare metadata
        metadata = {
            'strength': relationship.strength,
            'exposure_amount': str(relationship.exposure_amount),
            'tenant_id': context['tenant_id']
        }
        
        if relationship.metadata:
            metadata.update(relationship.metadata)
        
        # Add relationship in background
        background_tasks.add_task(
            risk_network.add_trading_relationship,
            relationship.from_trader,
            relationship.to_trader,
            rel_type,
            metadata
        )
        
        return {
            "status": "accepted",
            "from_trader": relationship.from_trader,
            "to_trader": relationship.to_trader,
            "relationship_type": relationship.relationship_type,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/analyze/propagation")
async def analyze_risk_propagation(
    request: RiskPropagationRequest,
    risk_network: TradingRiskNetwork = Depends(get_risk_network),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Analyze risk propagation from a source trader"""
    try:
        # Run propagation analysis
        result = await risk_network.analyze_risk_propagation(
            request.source_trader,
            request.risk_event
        )
        
        return {
            "source_trader": request.source_trader,
            "affected_traders": result.affected_traders,
            "total_affected": len(result.affected_traders),
            "total_exposure": str(result.total_exposure),
            "cascade_depth": result.cascade_depth,
            "systemic_risk_score": result.systemic_risk_score,
            "mitigation_actions": result.mitigation_actions,
            "propagation_paths": result.propagation_paths[:10],  # Limit to top 10
            "analysis_timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/clusters/risk")
async def detect_risk_clusters(
    risk_network: TradingRiskNetwork = Depends(get_risk_network),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Detect clusters of high-risk traders"""
    try:
        clusters = await risk_network.detect_risk_clusters()
        
        return {
            "cluster_count": len(clusters),
            "clusters": [
                {
                    "cluster_id": idx,
                    "traders": list(cluster),
                    "size": len(cluster)
                }
                for idx, cluster in enumerate(clusters)
            ],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/traders/{trader_id}/systemic-importance")
async def get_trader_systemic_importance(
    trader_id: str,
    risk_network: TradingRiskNetwork = Depends(get_risk_network),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Calculate systemic importance of a trader"""
    try:
        importance = await risk_network.calculate_trader_systemic_importance(trader_id)
        
        # Determine importance level
        if importance > 0.8:
            level = "critical"
        elif importance > 0.6:
            level = "high"
        elif importance > 0.4:
            level = "medium"
        else:
            level = "low"
        
        return {
            "trader_id": trader_id,
            "systemic_importance": importance,
            "importance_level": level,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/simulate/cascade")
async def simulate_cascade_failure(
    request: CascadeSimulationRequest,
    risk_network: TradingRiskNetwork = Depends(get_risk_network),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Simulate cascade effects of trader failure"""
    try:
        # Validate failure type
        valid_failure_types = ["liquidation", "default", "margin_call"]
        if request.failure_type not in valid_failure_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid failure type. Must be one of: {valid_failure_types}"
            )
        
        # Run simulation
        results = await risk_network.simulate_cascade_failure(
            request.failing_trader,
            request.failure_type
        )
        
        return {
            "simulation": results,
            "summary": {
                "initial_failure": request.failing_trader,
                "failure_type": request.failure_type,
                "total_affected": results.get('total_affected', 0),
                "total_losses": str(results.get('total_losses', 0)),
                "cascade_waves": len(results.get('waves', [])),
                "recommendations": results.get('recommendations', [])
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/network/stats")
async def get_risk_network_stats(
    risk_network: TradingRiskNetwork = Depends(get_risk_network),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get overall risk network statistics"""
    try:
        # Get network statistics
        total_traders = risk_network.g.V().hasLabel('trader').count().next()
        total_relationships = risk_network.g.E().count().next()
        
        # Risk distribution
        risk_distribution = {}
        for level in TraderRiskLevel:
            threshold = risk_network.risk_thresholds[level]
            count = risk_network.g.V().hasLabel('trader').has(
                'risk_score', P.gte(threshold)
            ).count().next()
            risk_distribution[level.value] = count
        
        # Get top systemic traders
        top_systemic = risk_network.g.V().hasLabel('trader').order().by(
            __.inE().count(), T.desc
        ).limit(10).values('trader_id').toList()
        
        return {
            "network_stats": {
                "total_traders": total_traders,
                "total_relationships": total_relationships,
                "average_connections": total_relationships / max(total_traders, 1) * 2
            },
            "risk_distribution": risk_distribution,
            "top_systemic_traders": top_systemic,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/alerts/configure")
async def configure_risk_alerts(
    thresholds: Dict[str, float],
    risk_network: TradingRiskNetwork = Depends(get_risk_network),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Configure risk alert thresholds"""
    try:
        # Update thresholds
        for level_str, threshold in thresholds.items():
            try:
                level = TraderRiskLevel(level_str)
                if 0 <= threshold <= 1:
                    risk_network.risk_thresholds[level] = threshold
                else:
                    raise ValueError(f"Threshold must be between 0 and 1")
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
        
        return {
            "status": "updated",
            "thresholds": {
                level.value: threshold 
                for level, threshold in risk_network.risk_thresholds.items()
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 