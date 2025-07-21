"""
Arbitrage API Routes
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional

from ..models.aggregation import (
    ArbitrageSearchRequest, ArbitrageResponse,
    ArbitrageOpportunity, ArbitrageExecution
)
from ..aggregators.arbitrage_detector import ArbitrageDetector
from ..core.dependencies import get_arbitrage_detector
from ..config import settings


router = APIRouter(prefix="/arbitrage", tags=["Arbitrage"])


@router.post("/search", response_model=ArbitrageResponse)
async def search_arbitrage(
    request: ArbitrageSearchRequest,
    arbitrage_detector: ArbitrageDetector = Depends(get_arbitrage_detector)
):
    """Search for arbitrage opportunities"""
    try:
        opportunities = await arbitrage_detector.search_arbitrage_opportunities(request)
        
        # Calculate total potential profit
        total_potential_profit = sum(opp.potential_profit for opp in opportunities)
        
        # Generate recommendations
        recommendations = []
        if opportunities:
            # Sort by profit/risk ratio
            sorted_opps = sorted(
                opportunities,
                key=lambda x: x.potential_profit / (x.risk_score + 0.1),
                reverse=True
            )
            
            for opp in sorted_opps[:3]:  # Top 3
                recommendations.append({
                    "opportunity_id": opp.opportunity_id,
                    "action": f"Buy {opp.quantity} units in {opp.market_a}, sell in {opp.market_b}",
                    "expected_profit": opp.potential_profit,
                    "risk_level": "low" if opp.risk_score < 0.3 else "medium" if opp.risk_score < 0.6 else "high",
                    "urgency": "high" if (opp.expires_at - opp.timestamp).total_seconds() < 3600 else "medium"
                })
        
        # Market analysis
        market_analysis = {
            "total_opportunities": len(opportunities),
            "avg_profit_margin": sum(opp.profit_margin for opp in opportunities) / len(opportunities) if opportunities else 0,
            "market_efficiency": 1 - (len(opportunities) / 100),  # Fewer opportunities = more efficient
            "best_resource_type": max(
                [(rt, sum(1 for o in opportunities if o.resource_type == rt)) 
                 for rt in set(o.resource_type for o in opportunities)],
                key=lambda x: x[1]
            )[0].value if opportunities else None
        }
        
        return ArbitrageResponse(
            opportunities=opportunities,
            total_potential_profit=total_potential_profit,
            recommended_executions=recommendations,
            market_analysis=market_analysis
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opportunities", response_model=List[ArbitrageOpportunity])
async def get_active_opportunities(
    resource_type: Optional[str] = Query(None, description="Filter by resource type"),
    min_profit: Optional[float] = Query(None, description="Minimum profit threshold"),
    max_risk: Optional[float] = Query(None, description="Maximum risk score"),
    arbitrage_detector: ArbitrageDetector = Depends(get_arbitrage_detector)
):
    """Get currently active arbitrage opportunities"""
    try:
        # Get all active opportunities
        opportunities = list(arbitrage_detector.active_opportunities.values())
        
        # Apply filters
        if resource_type:
            opportunities = [o for o in opportunities if o.resource_type.value == resource_type]
        
        if min_profit is not None:
            opportunities = [o for o in opportunities if o.potential_profit >= min_profit]
        
        if max_risk is not None:
            opportunities = [o for o in opportunities if o.risk_score <= max_risk]
        
        # Sort by profit potential
        opportunities.sort(key=lambda x: x.potential_profit, reverse=True)
        
        return opportunities
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opportunities/{opportunity_id}", response_model=ArbitrageOpportunity)
async def get_opportunity_details(
    opportunity_id: str,
    arbitrage_detector: ArbitrageDetector = Depends(get_arbitrage_detector)
):
    """Get details of a specific arbitrage opportunity"""
    try:
        opportunity = arbitrage_detector.active_opportunities.get(opportunity_id)
        
        if not opportunity:
            # Check cache
            cached = arbitrage_detector.arbitrage_cache.get(opportunity_id)
            if cached:
                opportunity = ArbitrageOpportunity(**cached)
            else:
                raise HTTPException(status_code=404, detail="Opportunity not found")
        
        return opportunity
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/execute/{opportunity_id}", response_model=ArbitrageExecution)
async def execute_arbitrage(
    opportunity_id: str,
    auto_execute: bool = Query(False, description="Execute without confirmation"),
    arbitrage_detector: ArbitrageDetector = Depends(get_arbitrage_detector)
):
    """Execute an arbitrage opportunity"""
    try:
        # Get opportunity details
        opportunity = arbitrage_detector.active_opportunities.get(opportunity_id)
        if not opportunity:
            raise HTTPException(status_code=404, detail="Opportunity not found or expired")
        
        # Check safety limits
        if opportunity.potential_profit > settings.MAX_ARBITRAGE_VALUE:
            raise HTTPException(
                status_code=400,
                detail=f"Profit exceeds safety limit of {settings.MAX_ARBITRAGE_VALUE}"
            )
        
        if opportunity.risk_score > 0.8:
            if not auto_execute:
                raise HTTPException(
                    status_code=400,
                    detail="High risk opportunity requires auto_execute=true confirmation"
                )
        
        # Execute arbitrage
        execution = await arbitrage_detector.execute_arbitrage(opportunity_id)
        
        return execution
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/executions", response_model=List[ArbitrageExecution])
async def get_execution_history(
    limit: int = Query(100, le=1000),
    success_only: bool = Query(False),
    arbitrage_detector: ArbitrageDetector = Depends(get_arbitrage_detector)
):
    """Get arbitrage execution history"""
    try:
        # In production, would query from database
        # For now, get from cache
        executions = []
        
        # Would implement proper querying
        # cache_keys = arbitrage_detector.arbitrage_cache.get_keys("exec_*")
        
        if success_only:
            executions = [e for e in executions if e.success]
        
        # Sort by execution time
        executions.sort(key=lambda x: x.executed_at, reverse=True)
        
        return executions[:limit]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats")
async def get_arbitrage_stats(
    time_period_hours: int = Query(24, description="Time period for stats"),
    arbitrage_detector: ArbitrageDetector = Depends(get_arbitrage_detector)
):
    """Get arbitrage statistics"""
    try:
        # Calculate stats (simplified - in production would query from database)
        active_opportunities = len(arbitrage_detector.active_opportunities)
        
        # Mock stats for demonstration
        stats = {
            "time_period_hours": time_period_hours,
            "active_opportunities": active_opportunities,
            "total_opportunities_found": active_opportunities * 10,  # Mock
            "total_executions": 50,  # Mock
            "successful_executions": 45,  # Mock
            "failed_executions": 5,  # Mock
            "total_profit_captured": 12500.0,  # Mock
            "average_profit_per_execution": 250.0,  # Mock
            "success_rate": 0.9,  # Mock
            "most_profitable_type": "price_differential",
            "most_active_resource": "quantum",
            "average_execution_time_ms": 3500,  # Mock
            "opportunities_by_type": {
                "price_differential": 40,
                "quality_arbitrage": 25,
                "time_arbitrage": 20,
                "cross_market": 15
            },
            "profit_by_resource": {
                "quantum": 6000.0,
                "ai": 4500.0,
                "network": 2000.0
            }
        }
        
        return stats
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/settings")
async def update_arbitrage_settings(
    min_profit_margin: Optional[float] = None,
    max_risk_score: Optional[float] = None,
    auto_execute_enabled: Optional[bool] = None,
    max_auto_execute_value: Optional[float] = None
):
    """Update arbitrage detection settings"""
    try:
        updates = {}
        
        if min_profit_margin is not None:
            settings.ARBITRAGE_MIN_PROFIT_MARGIN = min_profit_margin
            updates["min_profit_margin"] = min_profit_margin
        
        if max_risk_score is not None:
            # Would update in settings
            updates["max_risk_score"] = max_risk_score
        
        if auto_execute_enabled is not None:
            # Would update auto-execution settings
            updates["auto_execute_enabled"] = auto_execute_enabled
        
        if max_auto_execute_value is not None:
            settings.MAX_ARBITRAGE_VALUE = max_auto_execute_value
            updates["max_auto_execute_value"] = max_auto_execute_value
        
        return {
            "status": "updated",
            "updates": updates,
            "current_settings": {
                "min_profit_margin": settings.ARBITRAGE_MIN_PROFIT_MARGIN,
                "execution_delay": settings.ARBITRAGE_EXECUTION_DELAY,
                "max_arbitrage_value": settings.MAX_ARBITRAGE_VALUE
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 