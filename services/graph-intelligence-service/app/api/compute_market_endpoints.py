"""
Compute Market Intelligence API Endpoints
"""

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal

from ..api.deps import get_current_tenant_and_user
from ..compute_market_insights import ComputeMarketIntelligence

router = APIRouter()


class TrustAdjustedMarginRequest(BaseModel):
    """Request for trust-adjusted margin calculation"""
    participant_id: str = Field(..., description="Market participant ID")
    base_margin: float = Field(..., description="Base margin requirement")


class RiskMitigationRequest(BaseModel):
    """Request for risk mitigation recommendations"""
    participant_id: str = Field(..., description="Market participant ID")
    exposure: float = Field(..., description="Current exposure amount")


class MarketImpactRequest(BaseModel):
    """Request for market impact prediction"""
    order_type: str = Field(..., description="Order type (BUY/SELL)")
    order_size: float = Field(..., description="Order size")
    resource_type: str = Field("GPU", description="Resource type")


@router.get("/market/insights", response_model=Dict[str, Any])
async def get_market_insights(
    compute_market_intelligence: ComputeMarketIntelligence = Depends(lambda: router.app.state.compute_market_intelligence),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get comprehensive market structure insights"""
    try:
        insight = await compute_market_intelligence.analyze_market_structure()
        
        return {
            "timestamp": insight.timestamp.isoformat(),
            "market_concentration": insight.market_concentration,
            "liquidity_score": insight.liquidity_score,
            "volatility_estimate": insight.volatility_estimate,
            "network_resilience": insight.network_resilience,
            "systemic_risk_score": insight.systemic_risk_score,
            "dominant_participants": insight.dominant_participants,
            "market_inefficiencies": insight.market_inefficiencies,
            "arbitrage_opportunities": insight.arbitrage_opportunities,
            "risk_clusters": [list(cluster) for cluster in insight.risk_clusters],
            "market_health": _calculate_market_health(insight)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/market/trust-adjusted-margin", response_model=Dict[str, Any])
async def calculate_trust_adjusted_margin(
    request: TrustAdjustedMarginRequest,
    compute_market_intelligence: ComputeMarketIntelligence = Depends(lambda: router.app.state.compute_market_intelligence),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Calculate trust-adjusted margin requirements for a participant"""
    try:
        result = await compute_market_intelligence.calculate_trust_adjusted_margin(
            participant_id=request.participant_id,
            base_margin=Decimal(str(request.base_margin))
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/market/risk-mitigation", response_model=List[Dict[str, Any]])
async def get_risk_mitigation_recommendations(
    request: RiskMitigationRequest,
    compute_market_intelligence: ComputeMarketIntelligence = Depends(lambda: router.app.state.compute_market_intelligence),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get risk mitigation recommendations for a participant"""
    try:
        recommendations = await compute_market_intelligence.recommend_risk_mitigation(
            participant_id=request.participant_id,
            exposure=Decimal(str(request.exposure))
        )
        
        return recommendations
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/market/impact-prediction", response_model=Dict[str, Any])
async def predict_market_impact(
    request: MarketImpactRequest,
    compute_market_intelligence: ComputeMarketIntelligence = Depends(lambda: router.app.state.compute_market_intelligence),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Predict market impact of a large order"""
    try:
        impact = await compute_market_intelligence.predict_market_impact(
            order_type=request.order_type,
            order_size=Decimal(str(request.order_size)),
            resource_type=request.resource_type
        )
        
        return impact
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/market/risk-assessment/{participant_id}", response_model=Dict[str, Any])
async def get_participant_risk_assessment(
    participant_id: str,
    compute_market_intelligence: ComputeMarketIntelligence = Depends(lambda: router.app.state.compute_market_intelligence),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get comprehensive risk assessment for a market participant"""
    try:
        # This would query the graph database for detailed participant info
        # For now, return a sample assessment
        return {
            "participant_id": participant_id,
            "timestamp": datetime.utcnow().isoformat(),
            "risk_scores": {
                "counterparty_risk": 0.3,
                "operational_risk": 0.2,
                "market_risk": 0.4,
                "liquidity_risk": 0.25,
                "systemic_risk": 0.15
            },
            "overall_risk": "medium",
            "credit_rating": "BB+",
            "exposure_limits": {
                "max_single_contract": 100000,
                "max_total_exposure": 500000,
                "max_uncollateralized": 20000
            },
            "monitoring_level": "standard",
            "last_review": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/market/network-analysis", response_model=Dict[str, Any])
async def get_market_network_analysis(
    compute_market_intelligence: ComputeMarketIntelligence = Depends(lambda: router.app.state.compute_market_intelligence),
    context: dict = Depends(get_current_tenant_and_user)
):
    """Get network analysis of market participants and relationships"""
    try:
        # This would analyze the trading network
        # For now, return sample analysis
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "network_metrics": {
                "total_participants": 342,
                "active_participants": 187,
                "total_relationships": 1523,
                "average_degree": 8.9,
                "clustering_coefficient": 0.42,
                "network_diameter": 6
            },
            "centrality_analysis": {
                "most_connected": ["participant_123", "participant_456"],
                "highest_betweenness": ["participant_789", "participant_012"],
                "highest_eigenvector": ["participant_345", "participant_678"]
            },
            "community_detection": {
                "number_of_communities": 8,
                "largest_community_size": 89,
                "modularity_score": 0.67
            },
            "vulnerability_analysis": {
                "critical_nodes": ["participant_123", "participant_789"],
                "bridge_relationships": 12,
                "resilience_score": 0.78
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _calculate_market_health(insight) -> str:
    """Calculate overall market health score"""
    # Weighted health calculation
    health_score = (
        (1 - insight.market_concentration) * 0.25 +  # Lower concentration is better
        insight.liquidity_score * 0.25 +
        (1 - insight.volatility_estimate) * 0.2 +  # Lower volatility is better
        insight.network_resilience * 0.2 +
        (1 - insight.systemic_risk_score) * 0.1  # Lower systemic risk is better
    )
    
    if health_score > 0.8:
        return "excellent"
    elif health_score > 0.6:
        return "good"
    elif health_score > 0.4:
        return "fair"
    else:
        return "poor" 