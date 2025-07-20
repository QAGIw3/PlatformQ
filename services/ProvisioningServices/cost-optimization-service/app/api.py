"""API endpoints for Cost Optimization Service"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any

from fastapi import APIRouter, HTTPException, Query, Depends, Body
from pydantic import BaseModel, Field

from platformq_cost_common import (
    CostAnalysis,
    CostRecommendation,
    Budget,
    BudgetAlert,
    ResourceCost
)
from platformq_resource_common import ResourceMetrics

from .cost_analyzer import CostAnalyzer
from .recommendation_engine import RecommendationEngine
from .budget_manager import BudgetManager
from .repository import CostRepository

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/v1", tags=["cost-optimization"])

# Dependency injection
repository = CostRepository()
cost_analyzer = CostAnalyzer(repository)
recommendation_engine = RecommendationEngine(repository)
budget_manager = BudgetManager(repository)


# Request/Response models
class CostAnalysisRequest(BaseModel):
    """Request for cost analysis"""
    tenant_id: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    include_recommendations: bool = True
    include_anomalies: bool = True


class CostAnalysisResponse(BaseModel):
    """Response for cost analysis"""
    analysis: CostAnalysis
    recommendations: Optional[List[CostRecommendation]] = None


class BudgetRequest(BaseModel):
    """Request for creating/updating budget"""
    name: str
    amount: float = Field(gt=0)
    period: str = Field(pattern="^(daily|weekly|monthly|quarterly|yearly|custom)$")
    alert_thresholds: List[int] = Field(default=[50, 75, 90, 100])
    resource_filters: Optional[Dict[str, str]] = None
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None


class BudgetResponse(BaseModel):
    """Response for budget operations"""
    budget_id: str
    message: str


class RecommendationUpdateRequest(BaseModel):
    """Request to update recommendation status"""
    status: str = Field(pattern="^(pending|acknowledged|implemented|dismissed)$")
    notes: Optional[str] = None


# Cost Analysis endpoints
@router.post("/cost-analysis", response_model=CostAnalysisResponse)
async def analyze_costs(request: CostAnalysisRequest):
    """Analyze costs for a tenant"""
    try:
        # Default to last 24 hours if not specified
        end_date = request.end_date or datetime.now(timezone.utc)
        start_date = request.start_date or (end_date - timedelta(hours=24))
        
        # Perform cost analysis
        analysis = await cost_analyzer.analyze_costs(
            tenant_id=request.tenant_id,
            start_date=start_date,
            end_date=end_date
        )
        
        recommendations = None
        if request.include_recommendations:
            # Get resource metrics for recommendations
            # In production, this would fetch actual metrics
            resource_metrics = []  # Mock for now
            
            recommendations = await recommendation_engine.generate_recommendations(
                tenant_id=request.tenant_id,
                cost_analysis=analysis,
                resource_metrics=resource_metrics
            )
            
        # Check budgets and generate alerts
        alerts = await budget_manager.check_budgets(
            tenant_id=request.tenant_id,
            cost_analysis=analysis
        )
        
        return CostAnalysisResponse(
            analysis=analysis,
            recommendations=recommendations
        )
        
    except Exception as e:
        logger.error(f"Error analyzing costs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cost-analysis/{tenant_id}/{analysis_id}")
async def get_cost_analysis(tenant_id: str, analysis_id: str):
    """Get specific cost analysis by ID"""
    try:
        analysis = await repository.get_cost_analysis(tenant_id, analysis_id)
        if not analysis:
            raise HTTPException(status_code=404, detail="Analysis not found")
            
        return {"analysis": analysis}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting cost analysis: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cost-history/{tenant_id}")
async def get_cost_history(
    tenant_id: str,
    days: int = Query(default=30, ge=1, le=365)
):
    """Get cost history for tenant"""
    try:
        history = await repository.get_cost_history(tenant_id, days)
        return {"history": history}
        
    except Exception as e:
        logger.error(f"Error getting cost history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Recommendation endpoints
@router.get("/recommendations/{tenant_id}")
async def get_recommendations(
    tenant_id: str,
    status: Optional[str] = Query(None, pattern="^(pending|acknowledged|implemented|dismissed)$"),
    recommendation_type: Optional[str] = Query(None)
):
    """Get recommendations for tenant"""
    try:
        recommendations = await repository.get_recommendations(
            tenant_id=tenant_id,
            status=status,
            recommendation_type=recommendation_type
        )
        
        return {"recommendations": recommendations}
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/recommendations/{tenant_id}/{recommendation_id}")
async def update_recommendation(
    tenant_id: str,
    recommendation_id: str,
    update: RecommendationUpdateRequest
):
    """Update recommendation status"""
    try:
        # In production, this would update the recommendation in the database
        return {
            "message": "Recommendation updated",
            "recommendation_id": recommendation_id,
            "new_status": update.status
        }
        
    except Exception as e:
        logger.error(f"Error updating recommendation: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Budget endpoints
@router.post("/budgets/{tenant_id}", response_model=BudgetResponse)
async def create_budget(tenant_id: str, budget: BudgetRequest):
    """Create a new budget"""
    try:
        budget_id = await budget_manager.create_budget(
            tenant_id=tenant_id,
            name=budget.name,
            amount=budget.amount,
            period=budget.period,
            alert_thresholds=budget.alert_thresholds,
            resource_filters=budget.resource_filters,
            start_date=budget.start_date,
            end_date=budget.end_date
        )
        
        return BudgetResponse(
            budget_id=budget_id,
            message="Budget created successfully"
        )
        
    except Exception as e:
        logger.error(f"Error creating budget: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/budgets/{tenant_id}")
async def get_budgets(tenant_id: str):
    """Get all budgets for tenant"""
    try:
        budgets = await repository.get_budgets(tenant_id)
        return {"budgets": budgets}
        
    except Exception as e:
        logger.error(f"Error getting budgets: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/budgets/{tenant_id}/{budget_id}")
async def get_budget(tenant_id: str, budget_id: str):
    """Get specific budget"""
    try:
        budget = await repository.get_budget(tenant_id, budget_id)
        if not budget:
            raise HTTPException(status_code=404, detail="Budget not found")
            
        # Get current status
        status = await budget_manager.get_budget_status(tenant_id, budget_id)
        
        return {
            "budget": budget,
            "status": status
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting budget: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/budgets/{tenant_id}/{budget_id}")
async def update_budget(
    tenant_id: str,
    budget_id: str,
    updates: Dict[str, Any] = Body(...)
):
    """Update budget"""
    try:
        await budget_manager.update_budget(tenant_id, budget_id, updates)
        
        return {
            "message": "Budget updated successfully",
            "budget_id": budget_id
        }
        
    except Exception as e:
        logger.error(f"Error updating budget: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/budgets/{tenant_id}/{budget_id}")
async def delete_budget(tenant_id: str, budget_id: str):
    """Delete budget"""
    try:
        await budget_manager.delete_budget(tenant_id, budget_id)
        
        return {
            "message": "Budget deleted successfully",
            "budget_id": budget_id
        }
        
    except Exception as e:
        logger.error(f"Error deleting budget: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Alert endpoints
@router.get("/alerts/{tenant_id}")
async def get_budget_alerts(
    tenant_id: str,
    days: int = Query(default=7, ge=1, le=90)
):
    """Get budget alerts for tenant"""
    try:
        # In production, this would query alerts from the database
        alerts = []
        
        return {"alerts": alerts}
        
    except Exception as e:
        logger.error(f"Error getting alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Cost breakdown endpoints
@router.get("/cost-breakdown/{tenant_id}")
async def get_cost_breakdown(
    tenant_id: str,
    group_by: str = Query(default="resource_type", pattern="^(resource_type|provider|service|tag:.+)$"),
    days: int = Query(default=7, ge=1, le=90)
):
    """Get cost breakdown by various dimensions"""
    try:
        # In production, this would aggregate costs by the specified dimension
        breakdown = {}
        
        return {
            "breakdown": breakdown,
            "group_by": group_by,
            "period_days": days
        }
        
    except Exception as e:
        logger.error(f"Error getting cost breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Resource cost endpoints
@router.get("/resource-costs/{tenant_id}")
async def get_resource_costs(
    tenant_id: str,
    resource_type: Optional[str] = None,
    provider: Optional[str] = None,
    days: int = Query(default=1, ge=1, le=30)
):
    """Get detailed resource costs"""
    try:
        # In production, this would query resource costs with filters
        costs = []
        
        return {
            "resource_costs": costs,
            "filters": {
                "resource_type": resource_type,
                "provider": provider,
                "days": days
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting resource costs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Summary endpoints
@router.get("/summary/{tenant_id}")
async def get_cost_summary(tenant_id: str):
    """Get cost optimization summary for tenant"""
    try:
        # Get latest analysis
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(hours=24)
        
        analysis = await cost_analyzer.analyze_costs(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date
        )
        
        # Get pending recommendations
        recommendations = await repository.get_recommendations(
            tenant_id=tenant_id,
            status="pending"
        )
        
        # Get active budgets
        budgets = await repository.get_budgets(tenant_id)
        
        # Calculate potential savings
        total_potential_savings = sum(
            r.estimated_monthly_savings for r in recommendations
            if r.estimated_monthly_savings > 0
        )
        
        return {
            "summary": {
                "current_daily_cost": analysis.total_cost,
                "projected_monthly_cost": analysis.total_cost * 30,
                "total_potential_savings": total_potential_savings,
                "pending_recommendations": len(recommendations),
                "active_budgets": len(budgets),
                "cost_trend": analysis.trends[0] if analysis.trends else None,
                "top_cost_drivers": analysis.breakdown[:3] if analysis.breakdown else []
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting cost summary: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 