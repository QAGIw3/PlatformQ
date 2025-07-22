"""Market Intelligence Insights API."""

from typing import Dict, Any, List, Optional
from datetime import datetime
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from ..integrations.graph_data_integration import GraphDataIntegration
from ..integrations.trading_core_integration import TradingCoreMarketIntelligence

router = APIRouter(prefix="/insights", tags=["Market Insights"])


class MarketInsightRequest(BaseModel):
    """Request for market insights."""
    market_id: str = Field(..., description="Market identifier")
    include_network_analysis: bool = Field(True, description="Include network analysis")
    include_manipulation_detection: bool = Field(True, description="Include manipulation detection")


class SystemicRiskRequest(BaseModel):
    """Request for systemic risk analysis."""
    market_ids: List[str] = Field(..., description="List of market IDs")
    time_window_hours: int = Field(24, description="Time window for analysis")


# Initialize integrations
graph_integration = GraphDataIntegration()
trading_core_intel = TradingCoreMarketIntelligence()


@router.on_event("startup")
async def startup_event():
    """Initialize integrations on startup."""
    await graph_integration.initialize()
    await trading_core_intel.initialize()


@router.get("/{market_id}")
async def get_market_insight(
    market_id: str,
    include_network: bool = Query(True, description="Include network analysis"),
    include_ml: bool = Query(True, description="Include ML predictions")
) -> Dict[str, Any]:
    """Get comprehensive market insights."""
    try:
        # Get base market insight
        insight = await trading_core_intel.get_market_insight(market_id)
        
        # Add network analysis if requested
        if include_network:
            # Get top traders in this market
            market_data = await trading_core_intel._fetch_market_data(market_id)
            if market_data and "recent_trades" in market_data:
                traders = set()
                for trade in market_data["recent_trades"]:
                    traders.add(trade.get("buyer_id"))
                    traders.add(trade.get("seller_id"))
                
                # Get network insights for top traders
                network_insights = []
                for trader_id in list(traders)[:10]:  # Top 10 traders
                    trader_network = await graph_integration.get_trader_network_insights(trader_id)
                    if trader_network:
                        network_insights.append({
                            "trader_id": trader_id,
                            "influence_score": trader_network.get("influence_score", 0),
                            "copy_risk": trader_network.get("copy_risk", 0)
                        })
                
                insight.network_analysis = {
                    "active_traders": len(traders),
                    "top_influencers": sorted(
                        network_insights, 
                        key=lambda x: x["influence_score"], 
                        reverse=True
                    )[:5]
                }
        
        return insight
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/manipulation/detect")
async def detect_market_manipulation(
    request: MarketInsightRequest
) -> Dict[str, Any]:
    """Detect potential market manipulation."""
    try:
        from datetime import timedelta
        
        manipulations = await graph_integration.detect_market_manipulation(
            market_id=request.market_id,
            time_window=timedelta(hours=24)
        )
        
        return {
            "market_id": request.market_id,
            "timestamp": datetime.utcnow().isoformat(),
            "manipulation_detected": len(manipulations) > 0,
            "patterns": manipulations,
            "risk_score": min(len(manipulations) * 0.2, 1.0)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/systemic-risk")
async def analyze_systemic_risk(
    request: SystemicRiskRequest
) -> Dict[str, Any]:
    """Analyze systemic risk across markets."""
    try:
        risk_analysis = await graph_integration.analyze_systemic_risk(
            market_ids=request.market_ids
        )
        
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "markets_analyzed": len(request.market_ids),
            "risk_analysis": risk_analysis,
            "overall_risk_level": _calculate_overall_risk_level(risk_analysis)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trader/{trader_id}/network")
async def get_trader_network_insights(
    trader_id: str,
    include_cliques: bool = Query(True, description="Include trading cliques")
) -> Dict[str, Any]:
    """Get network insights for a specific trader."""
    try:
        insights = await graph_integration.get_trader_network_insights(trader_id)
        
        if not include_cliques and "clique_membership" in insights:
            # Remove clique data if not requested
            insights.pop("clique_membership")
            insights.pop("manipulation_risk", None)
        
        return {
            "trader_id": trader_id,
            "timestamp": datetime.utcnow().isoformat(),
            "network_insights": insights
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/correlations/{asset_id}")
async def get_asset_correlations(
    asset_id: str,
    correlated_assets: List[str] = Query(..., description="Assets to check correlation with"),
    time_period: str = Query("30d", description="Time period for correlation")
) -> Dict[str, Any]:
    """Get correlation data for assets."""
    try:
        correlations = await graph_integration.get_asset_correlation_graph(
            asset_ids=[asset_id] + correlated_assets,
            time_period=time_period
        )
        
        return correlations
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _calculate_overall_risk_level(risk_analysis: Dict[str, Any]) -> str:
    """Calculate overall risk level from analysis."""
    connectivity = risk_analysis.get("connectivity", 0)
    
    if connectivity < 0.3:
        return "low"
    elif connectivity < 0.6:
        return "moderate"
    elif connectivity < 0.8:
        return "high"
    else:
        return "critical" 