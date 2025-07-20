from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import logging

from ..models.analytics_models import (
    AnalyticsQuery, ChainMetrics, WalletAnalytics,
    TokenAnalytics, AnalyticsReport, Alert,
    TimeInterval, MetricType, TimeSeries
)
from ..core.analytics_engine import AnalyticsEngine


router = APIRouter(prefix="/analytics", tags=["Analytics"])
logger = logging.getLogger(__name__)


# Dependency to get analytics engine
async def get_analytics_engine() -> AnalyticsEngine:
    """Get analytics engine instance"""
    from ..main import analytics_engine
    return analytics_engine


@router.post("/query", response_model=Dict[str, Any])
async def query_analytics(
    query: AnalyticsQuery,
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    Execute analytics query
    
    Query blockchain data with specified metrics, chains, and time range
    """
    try:
        result = await analytics_engine.query_analytics(query)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error executing analytics query: {e}")
        raise HTTPException(status_code=500, detail="Failed to execute query")


@router.get("/chains/{chain}/metrics", response_model=ChainMetrics)
async def get_chain_metrics(
    chain: str,
    timestamp: Optional[datetime] = Query(None),
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    Get current metrics for a blockchain
    
    Returns transaction count, volume, gas metrics, and network statistics
    """
    try:
        metrics = await analytics_engine.get_chain_metrics(chain, timestamp)
        return metrics
    except Exception as e:
        logger.error(f"Error getting chain metrics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get chain metrics")


@router.get("/wallets/{address}", response_model=WalletAnalytics)
async def get_wallet_analytics(
    address: str,
    chain: str = Query(..., description="Blockchain name"),
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    Get analytics for a specific wallet address
    
    Returns balance, transaction history, and interaction metrics
    """
    try:
        analytics = await analytics_engine.get_wallet_analytics(address, chain)
        return analytics
    except Exception as e:
        logger.error(f"Error getting wallet analytics: {e}")
        raise HTTPException(status_code=500, detail="Failed to get wallet analytics")


@router.post("/reports/generate", response_model=AnalyticsReport)
async def generate_report(
    name: str,
    report_type: str = Query(..., description="Type of report (daily, weekly, monthly, custom)"),
    chains: List[str] = Query(...),
    metrics: List[str] = Query(...),
    start_date: datetime = Query(...),
    end_date: datetime = Query(...),
    format: str = Query("json", description="Report format (json, html, pdf)"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    Generate analytics report
    
    Creates a comprehensive report with specified metrics and visualizations
    """
    try:
        # For PDF/HTML reports, generate in background
        if format in ['pdf', 'html']:
            report = AnalyticsReport(
                name=name,
                report_type=report_type,
                start_date=start_date,
                end_date=end_date,
                chains=chains,
                metrics=metrics,
                format=format
            )
            
            background_tasks.add_task(
                analytics_engine.generate_report,
                name, report_type, chains, metrics,
                start_date, end_date, format
            )
            
            return report
        else:
            # Generate JSON report synchronously
            report = await analytics_engine.generate_report(
                name, report_type, chains, metrics,
                start_date, end_date, format
            )
            return report
            
    except Exception as e:
        logger.error(f"Error generating report: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate report")


@router.get("/reports/{report_id}", response_model=AnalyticsReport)
async def get_report(
    report_id: str,
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    Get generated report by ID
    """
    # Load from cache
    report_data = await analytics_engine.reports_cache.get(report_id)
    if not report_data:
        raise HTTPException(status_code=404, detail="Report not found")
    
    import json
    return AnalyticsReport(**json.loads(report_data))


@router.post("/alerts", response_model=Alert)
async def create_alert(
    alert: Alert,
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    Create analytics alert
    
    Set up alerts for metric thresholds
    """
    try:
        created_alert = await analytics_engine.create_alert(alert)
        return created_alert
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating alert: {e}")
        raise HTTPException(status_code=500, detail="Failed to create alert")


@router.get("/alerts", response_model=List[Alert])
async def list_alerts(
    chain: Optional[str] = Query(None),
    is_active: Optional[bool] = Query(None),
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    List analytics alerts
    """
    alerts = list(analytics_engine.active_alerts.values())
    
    # Apply filters
    if chain:
        alerts = [a for a in alerts if a.chain == chain]
    if is_active is not None:
        alerts = [a for a in alerts if a.is_active == is_active]
    
    return alerts


@router.delete("/alerts/{alert_id}")
async def delete_alert(
    alert_id: str,
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    Delete an alert
    """
    if alert_id not in analytics_engine.active_alerts:
        raise HTTPException(status_code=404, detail="Alert not found")
    
    # Mark as inactive
    alert = analytics_engine.active_alerts[alert_id]
    alert.is_active = False
    await analytics_engine.alerts_cache.put(alert_id, alert.json())
    del analytics_engine.active_alerts[alert_id]
    
    return {"message": "Alert deleted"}


@router.get("/metrics/timeseries")
async def get_timeseries_data(
    metric: str = Query(..., description="Metric name"),
    chain: str = Query(..., description="Blockchain name"),
    start_time: datetime = Query(...),
    end_time: datetime = Query(...),
    interval: TimeInterval = Query(TimeInterval.ONE_HOUR),
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    Get time series data for a specific metric
    """
    try:
        # Create query
        query = AnalyticsQuery(
            metric_type=MetricType(metric),
            chains=[chain],
            start_time=start_time,
            end_time=end_time,
            interval=interval
        )
        
        result = await analytics_engine.query_analytics(query)
        
        # Extract time series data
        if chain in result['data'] and 'metrics' in result['data'][chain]:
            metrics = result['data'][chain]['metrics']
            if metric in metrics:
                return metrics[metric]
        
        return {
            'metric': metric,
            'chain': chain,
            'data_points': []
        }
        
    except Exception as e:
        logger.error(f"Error getting time series data: {e}")
        raise HTTPException(status_code=500, detail="Failed to get time series data")


@router.get("/insights")
async def get_insights(
    chains: List[str] = Query(...),
    lookback_hours: int = Query(24, ge=1, le=168),
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    Get analytics insights
    
    Returns AI-generated insights based on recent data
    """
    try:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=lookback_hours)
        
        all_insights = []
        
        for chain in chains:
            # Query multiple metrics
            for metric_type in [
                MetricType.TRANSACTION_COUNT,
                MetricType.GAS_PRICE,
                MetricType.TRANSACTION_VOLUME
            ]:
                query = AnalyticsQuery(
                    metric_type=metric_type,
                    chains=[chain],
                    start_time=start_time,
                    end_time=end_time,
                    interval=TimeInterval.ONE_HOUR
                )
                
                result = await analytics_engine.query_analytics(query)
                all_insights.extend(result.get('insights', []))
        
        # Sort by severity
        severity_order = {'high': 0, 'medium': 1, 'low': 2, 'info': 3}
        all_insights.sort(key=lambda x: severity_order.get(x.get('severity', 'info'), 4))
        
        return {
            'insights': all_insights[:20],  # Top 20 insights
            'generated_at': datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error getting insights: {e}")
        raise HTTPException(status_code=500, detail="Failed to get insights")


@router.get("/trends/{metric}")
async def get_metric_trends(
    metric: str,
    chains: List[str] = Query(...),
    period_days: int = Query(7, ge=1, le=90),
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    Get trend analysis for a metric across chains
    """
    try:
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(days=period_days)
        
        trends = {}
        
        for chain in chains:
            query = AnalyticsQuery(
                metric_type=MetricType(metric),
                chains=[chain],
                start_time=start_time,
                end_time=end_time,
                interval=TimeInterval.ONE_DAY
            )
            
            result = await analytics_engine.query_analytics(query)
            
            if chain in result['data']:
                chain_data = result['data'][chain]
                summary = chain_data.get('summary', {})
                
                # Calculate trend
                if metric in summary:
                    metric_summary = summary[metric]
                    trends[chain] = {
                        'current': metric_summary.get('latest'),
                        'average': metric_summary.get('average'),
                        'min': metric_summary.get('min'),
                        'max': metric_summary.get('max'),
                        'change_percent': None  # TODO: Calculate percentage change
                    }
        
        return {
            'metric': metric,
            'period_days': period_days,
            'trends': trends
        }
        
    except Exception as e:
        logger.error(f"Error getting metric trends: {e}")
        raise HTTPException(status_code=500, detail="Failed to get trends")


@router.get("/health")
async def health_check(
    analytics_engine: AnalyticsEngine = Depends(get_analytics_engine)
):
    """
    Health check endpoint
    
    Returns service health and component statuses
    """
    component_health = {
        'redis': analytics_engine.redis_client is not None,
        'mongodb': analytics_engine.mongodb_client is not None,
        'ignite': analytics_engine.ignite_client is not None,
        'analyzers': len(analytics_engine.analyzers) > 0
    }
    
    return {
        'status': 'healthy' if all(component_health.values()) else 'degraded',
        'components': component_health,
        'active_alerts': len(analytics_engine.active_alerts)
    } 