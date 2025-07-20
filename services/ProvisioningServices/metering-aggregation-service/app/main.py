"""Metering Aggregation Service

Aggregates usage data from CloudKitty and OpenMeter to provide unified cost
and usage analytics, replacing the legacy cost-optimization-service.
"""

import os
import logging
from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from decimal import Decimal

from fastapi import FastAPI, HTTPException, Depends, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

from platformq_cost_common.models import (
    CostAnalysis,
    CostBreakdown,
    CostTrend,
    BudgetAlert,
    CostRecommendation,
    CostRecommendationType
)
from platformq_shared.security import get_current_user_from_trusted_header as get_current_user

from .aggregator import MetricsAggregator
from .analyzer import CostAnalyzer
from .recommender import CostRecommender
from .config import Settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
settings: Optional[Settings] = None
aggregator: Optional[MetricsAggregator] = None
analyzer: Optional[CostAnalyzer] = None
recommender: Optional[CostRecommender] = None


# Request/Response models
class CostSummaryRequest(BaseModel):
    """Request for cost summary"""
    start_date: datetime
    end_date: datetime
    group_by: List[str] = Field(default_factory=lambda: ["service", "plan"])
    include_forecast: bool = False


class UsageMetricsRequest(BaseModel):
    """Request for usage metrics"""
    metric_type: str = Field(..., description="Metric type (compute_hours, storage_gb_hours, etc.)")
    start_date: datetime
    end_date: datetime
    group_by: List[str] = Field(default_factory=list)


class BudgetSetRequest(BaseModel):
    """Request to set budget"""
    monthly_limit: Decimal = Field(..., gt=0)
    alert_thresholds: List[int] = Field(default_factory=lambda: [50, 80, 90, 100])
    currency: str = "USD"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    global settings, aggregator, analyzer, recommender
    
    # Load configuration
    settings = Settings()
    
    # Initialize components
    aggregator = MetricsAggregator(settings)
    analyzer = CostAnalyzer(settings)
    recommender = CostRecommender(settings)
    
    # Initialize connections
    await aggregator.initialize()
    await analyzer.initialize()
    
    logger.info("Metering Aggregation Service started")
    
    yield
    
    # Cleanup
    await aggregator.close()
    await analyzer.close()
    
    logger.info("Metering Aggregation Service stopped")


app = FastAPI(
    title="Metering Aggregation Service",
    description="Unified cost and usage analytics for Platform Q",
    version="2.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Health check endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "metering-aggregation-service"}


@app.get("/ready")
async def readiness_check():
    """Readiness check endpoint"""
    ready = True
    details = {}
    
    # Check CloudKitty connection
    if settings.cloudkitty_enabled:
        details["cloudkitty"] = "ready" if await aggregator.check_cloudkitty_health() else "not ready"
        if details["cloudkitty"] == "not ready":
            ready = False
    
    # Check OpenMeter connection
    if settings.openmeter_enabled:
        details["openmeter"] = "ready" if await aggregator.check_openmeter_health() else "not ready"
        if details["openmeter"] == "not ready":
            ready = False
    
    return {
        "ready": ready,
        "details": details
    }


# Cost Analysis Endpoints
@app.get("/api/v1/tenants/{tenant_id}/cost/summary", response_model=CostAnalysis)
async def get_cost_summary(
    tenant_id: str = Path(..., description="Tenant ID"),
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    group_by: List[str] = Query(default=["service", "plan"]),
    include_forecast: bool = Query(default=False),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get cost summary for a tenant"""
    
    # Verify access
    if current_user["tenant_id"] != tenant_id and not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        # Get cost data from CloudKitty
        cost_data = await aggregator.get_cloudkitty_summary(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
            group_by=group_by
        )
        
        # Analyze costs
        analysis = await analyzer.analyze_costs(
            tenant_id=tenant_id,
            cost_data=cost_data,
            include_forecast=include_forecast
        )
        
        return analysis
        
    except Exception as e:
        logger.error(f"Error getting cost summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/tenants/{tenant_id}/cost/breakdown", response_model=CostBreakdown)
async def get_cost_breakdown(
    tenant_id: str = Path(..., description="Tenant ID"),
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get detailed cost breakdown for a tenant"""
    
    # Verify access
    if current_user["tenant_id"] != tenant_id and not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        breakdown = await analyzer.get_cost_breakdown(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date
        )
        
        return breakdown
        
    except Exception as e:
        logger.error(f"Error getting cost breakdown: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/tenants/{tenant_id}/cost/trends", response_model=List[CostTrend])
async def get_cost_trends(
    tenant_id: str = Path(..., description="Tenant ID"),
    period: str = Query("daily", regex="^(hourly|daily|weekly|monthly)$"),
    lookback_days: int = Query(30, ge=1, le=365),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get cost trends for a tenant"""
    
    # Verify access
    if current_user["tenant_id"] != tenant_id and not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        end_date = datetime.utcnow()
        start_date = end_date - timedelta(days=lookback_days)
        
        trends = await analyzer.get_cost_trends(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
            period=period
        )
        
        return trends
        
    except Exception as e:
        logger.error(f"Error getting cost trends: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Usage Metrics Endpoints
@app.get("/api/v1/tenants/{tenant_id}/usage/metrics")
async def get_usage_metrics(
    tenant_id: str = Path(..., description="Tenant ID"),
    metric_type: str = Query(..., description="Metric type"),
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    group_by: List[str] = Query(default=[]),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get usage metrics from OpenMeter"""
    
    # Verify access
    if current_user["tenant_id"] != tenant_id and not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        metrics = await aggregator.get_openmeter_metrics(
            meter_slug=metric_type,
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
            group_by=group_by or None
        )
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error getting usage metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/tenants/{tenant_id}/usage/realtime")
async def get_realtime_usage(
    tenant_id: str = Path(..., description="Tenant ID"),
    metric_type: str = Query(..., description="Metric type"),
    window_minutes: int = Query(5, ge=1, le=60),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get real-time usage data"""
    
    # Verify access
    if current_user["tenant_id"] != tenant_id and not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=window_minutes)
        
        realtime_data = await aggregator.get_realtime_metrics(
            meter_slug=metric_type,
            tenant_id=tenant_id,
            start_time=start_time,
            end_time=end_time
        )
        
        return realtime_data
        
    except Exception as e:
        logger.error(f"Error getting realtime usage: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Budget Management Endpoints
@app.get("/api/v1/tenants/{tenant_id}/budget")
async def get_budget(
    tenant_id: str = Path(..., description="Tenant ID"),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get budget configuration for a tenant"""
    
    # Verify access
    if current_user["tenant_id"] != tenant_id and not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        budget = await analyzer.get_budget(tenant_id)
        return budget
        
    except Exception as e:
        logger.error(f"Error getting budget: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/api/v1/tenants/{tenant_id}/budget")
async def set_budget(
    tenant_id: str = Path(..., description="Tenant ID"),
    request: BudgetSetRequest = ...,
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Set budget for a tenant"""
    
    # Verify access (admin only)
    if not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        await analyzer.set_budget(
            tenant_id=tenant_id,
            monthly_limit=request.monthly_limit,
            alert_thresholds=request.alert_thresholds,
            currency=request.currency
        )
        
        return {"message": "Budget updated successfully"}
        
    except Exception as e:
        logger.error(f"Error setting budget: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/tenants/{tenant_id}/budget/alerts", response_model=List[BudgetAlert])
async def get_budget_alerts(
    tenant_id: str = Path(..., description="Tenant ID"),
    active_only: bool = Query(True),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get budget alerts for a tenant"""
    
    # Verify access
    if current_user["tenant_id"] != tenant_id and not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        alerts = await analyzer.get_budget_alerts(
            tenant_id=tenant_id,
            active_only=active_only
        )
        
        return alerts
        
    except Exception as e:
        logger.error(f"Error getting budget alerts: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Cost Optimization Endpoints
@app.get("/api/v1/tenants/{tenant_id}/recommendations", response_model=List[CostRecommendation])
async def get_cost_recommendations(
    tenant_id: str = Path(..., description="Tenant ID"),
    min_savings_percent: float = Query(5.0, ge=0, le=100),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get cost optimization recommendations"""
    
    # Verify access
    if current_user["tenant_id"] != tenant_id and not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        # Get current usage patterns
        usage_data = await aggregator.get_usage_patterns(tenant_id)
        
        # Generate recommendations
        recommendations = await recommender.generate_recommendations(
            tenant_id=tenant_id,
            usage_data=usage_data,
            min_savings_percent=min_savings_percent
        )
        
        return recommendations
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Hierarchical Tenant Endpoints
@app.get("/api/v1/resellers/{reseller_id}/cost/summary")
async def get_reseller_cost_summary(
    reseller_id: str = Path(..., description="Reseller ID"),
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    group_by_customer: bool = Query(True),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get aggregated cost summary for a reseller"""
    
    # Verify access (reseller admin only)
    if current_user.get("reseller_id") != reseller_id or not current_user.get("is_reseller_admin"):
        raise HTTPException(status_code=403, detail="Reseller admin access required")
    
    try:
        summary = await analyzer.get_reseller_summary(
            reseller_id=reseller_id,
            start_date=start_date,
            end_date=end_date,
            group_by_customer=group_by_customer
        )
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting reseller summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/customers/{customer_id}/cost/summary")
async def get_customer_cost_summary(
    customer_id: str = Path(..., description="Customer ID"),
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    group_by_tenant: bool = Query(True),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Get aggregated cost summary for a customer"""
    
    # Verify access (customer admin only)
    if current_user.get("customer_id") != customer_id or not current_user.get("is_customer_admin"):
        raise HTTPException(status_code=403, detail="Customer admin access required")
    
    try:
        summary = await analyzer.get_customer_summary(
            customer_id=customer_id,
            start_date=start_date,
            end_date=end_date,
            group_by_tenant=group_by_tenant
        )
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting customer summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Export Endpoints
@app.get("/api/v1/tenants/{tenant_id}/cost/export")
async def export_cost_data(
    tenant_id: str = Path(..., description="Tenant ID"),
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    format: str = Query("csv", regex="^(csv|json|xlsx)$"),
    current_user: Dict[str, Any] = Depends(get_current_user)
):
    """Export cost data in various formats"""
    
    # Verify access
    if current_user["tenant_id"] != tenant_id and not current_user.get("is_admin"):
        raise HTTPException(status_code=403, detail="Access denied")
    
    try:
        export_data = await analyzer.export_cost_data(
            tenant_id=tenant_id,
            start_date=start_date,
            end_date=end_date,
            format=format
        )
        
        # Return appropriate response based on format
        if format == "csv":
            from fastapi.responses import Response
            return Response(
                content=export_data,
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename=costs_{tenant_id}.csv"}
            )
        elif format == "xlsx":
            from fastapi.responses import Response
            return Response(
                content=export_data,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                headers={"Content-Disposition": f"attachment; filename=costs_{tenant_id}.xlsx"}
            )
        else:
            return export_data
            
    except Exception as e:
        logger.error(f"Error exporting cost data: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Metrics endpoint
@app.get("/metrics")
async def get_metrics():
    """Prometheus metrics endpoint"""
    # This would be implemented with prometheus_client
    return Response(content="", media_type="text/plain")


if __name__ == "__main__":
    port = int(os.getenv("SERVICE_PORT", "8091"))
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=port,
        reload=True
    ) 