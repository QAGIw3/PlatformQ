"""
Model monitoring API endpoints
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from enum import Enum

router = APIRouter()


class MonitorType(str, Enum):
    DATA_DRIFT = "data_drift"
    CONCEPT_DRIFT = "concept_drift"
    PERFORMANCE = "performance"
    PREDICTION_DRIFT = "prediction_drift"
    FEATURE_DRIFT = "feature_drift"


class AlertSeverity(str, Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class MonitorConfig(BaseModel):
    """Monitor configuration"""
    monitor_type: MonitorType
    model_id: str = Field(..., description="Model to monitor")
    deployment_id: Optional[str] = Field(None, description="Specific deployment")
    reference_dataset: Optional[str] = Field(None, description="Reference dataset ID")
    thresholds: Dict[str, float] = Field(default_factory=dict)
    check_frequency: str = Field("hourly", description="hourly, daily, weekly, realtime")
    alert_channels: List[str] = Field(default_factory=lambda: ["email", "dashboard"])


class MonitorCreate(BaseModel):
    """Create monitor request"""
    name: str = Field(..., description="Monitor name")
    description: Optional[str] = Field(None)
    config: MonitorConfig
    enabled: bool = Field(True)
    tags: List[str] = Field(default_factory=list)


class MonitorResponse(BaseModel):
    """Monitor response"""
    monitor_id: str
    name: str
    description: Optional[str]
    config: MonitorConfig
    enabled: bool
    created_at: datetime
    last_check: Optional[datetime]
    status: str  # healthy, warning, critical
    tags: List[str]


class DriftReport(BaseModel):
    """Drift analysis report"""
    report_id: str
    monitor_id: str
    drift_type: MonitorType
    drift_score: float
    features_affected: List[str]
    statistics: Dict[str, Any]
    recommendations: List[str]
    generated_at: datetime


class Alert(BaseModel):
    """Monitoring alert"""
    alert_id: str
    monitor_id: str
    severity: AlertSeverity
    title: str
    message: str
    details: Dict[str, Any]
    created_at: datetime
    acknowledged: bool
    acknowledged_by: Optional[str]
    acknowledged_at: Optional[datetime]


@router.post("/monitors", response_model=MonitorResponse)
async def create_monitor(
    monitor: MonitorCreate,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Create a new model monitor"""
    monitor_id = f"monitor_{datetime.utcnow().timestamp()}"
    
    return MonitorResponse(
        monitor_id=monitor_id,
        name=monitor.name,
        description=monitor.description,
        config=monitor.config,
        enabled=monitor.enabled,
        created_at=datetime.utcnow(),
        last_check=None,
        status="healthy",
        tags=monitor.tags
    )


@router.get("/monitors", response_model=List[MonitorResponse])
async def list_monitors(
    tenant_id: str = Query(..., description="Tenant ID"),
    model_id: Optional[str] = Query(None),
    monitor_type: Optional[MonitorType] = Query(None),
    status: Optional[str] = Query(None),
    enabled: Optional[bool] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List model monitors"""
    return []


@router.get("/monitors/{monitor_id}", response_model=MonitorResponse)
async def get_monitor(
    monitor_id: str,
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Get monitor details"""
    raise HTTPException(status_code=404, detail="Monitor not found")


@router.patch("/monitors/{monitor_id}")
async def update_monitor(
    monitor_id: str,
    enabled: Optional[bool] = Query(None),
    config_update: Optional[Dict[str, Any]] = None,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Update monitor configuration"""
    return {
        "monitor_id": monitor_id,
        "status": "updated",
        "updated_at": datetime.utcnow()
    }


@router.delete("/monitors/{monitor_id}")
async def delete_monitor(
    monitor_id: str,
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Delete a monitor"""
    return {"status": "deleted", "monitor_id": monitor_id}


@router.get("/monitors/{monitor_id}/reports", response_model=List[DriftReport])
async def get_drift_reports(
    monitor_id: str,
    tenant_id: str = Query(..., description="Tenant ID"),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0)
):
    """Get drift analysis reports"""
    return [
        DriftReport(
            report_id="report_123",
            monitor_id=monitor_id,
            drift_type=MonitorType.DATA_DRIFT,
            drift_score=0.15,
            features_affected=["feature_1", "feature_3"],
            statistics={
                "ks_statistic": 0.15,
                "wasserstein_distance": 0.08,
                "population_stability_index": 0.12
            },
            recommendations=[
                "Consider retraining the model",
                "Review feature engineering for affected features"
            ],
            generated_at=datetime.utcnow()
        )
    ]


@router.get("/alerts", response_model=List[Alert])
async def list_alerts(
    tenant_id: str = Query(..., description="Tenant ID"),
    monitor_id: Optional[str] = Query(None),
    severity: Optional[AlertSeverity] = Query(None),
    acknowledged: Optional[bool] = Query(None),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0)
):
    """List monitoring alerts"""
    return [
        Alert(
            alert_id="alert_123",
            monitor_id="monitor_456",
            severity=AlertSeverity.WARNING,
            title="Data Drift Detected",
            message="Significant drift detected in 3 features",
            details={
                "drift_score": 0.25,
                "features": ["age", "income", "location"],
                "samples_affected": 1500
            },
            created_at=datetime.utcnow(),
            acknowledged=False,
            acknowledged_by=None,
            acknowledged_at=None
        )
    ]


@router.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: str,
    notes: Optional[str] = Query(None, description="Acknowledgment notes"),
    tenant_id: str = Query(..., description="Tenant ID"),
    user_id: str = Query(..., description="User ID")
):
    """Acknowledge an alert"""
    return {
        "alert_id": alert_id,
        "acknowledged": True,
        "acknowledged_by": user_id,
        "acknowledged_at": datetime.utcnow(),
        "notes": notes
    }


@router.get("/models/{model_id}/metrics")
async def get_model_metrics(
    model_id: str,
    tenant_id: str = Query(..., description="Tenant ID"),
    metric_names: Optional[List[str]] = Query(None),
    deployment_id: Optional[str] = Query(None),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    granularity: str = Query("hour", description="minute, hour, day, week")
):
    """Get model performance metrics over time"""
    return {
        "model_id": model_id,
        "deployment_id": deployment_id,
        "metrics": {
            "accuracy": {
                "values": [
                    {"timestamp": datetime.utcnow() - timedelta(hours=i), "value": 0.95 - i*0.01}
                    for i in range(24)
                ],
                "current": 0.94,
                "trend": "decreasing"
            },
            "latency_p99": {
                "values": [
                    {"timestamp": datetime.utcnow() - timedelta(hours=i), "value": 25.5 + i*0.5}
                    for i in range(24)
                ],
                "current": 37.5,
                "trend": "increasing"
            }
        },
        "time_range": {
            "start": start_time or datetime.utcnow() - timedelta(days=1),
            "end": end_time or datetime.utcnow()
        }
    }


@router.post("/models/{model_id}/analyze-drift")
async def analyze_drift(
    model_id: str,
    current_data: Dict[str, Any] = Query(..., description="Current data sample"),
    reference_data: Optional[Dict[str, Any]] = Query(None, description="Reference data"),
    tenant_id: str = Query(..., description="Tenant ID")
):
    """Perform drift analysis on demand"""
    analysis_id = f"drift_analysis_{datetime.utcnow().timestamp()}"
    
    return {
        "analysis_id": analysis_id,
        "model_id": model_id,
        "drift_detected": True,
        "drift_score": 0.18,
        "drift_type": "data_drift",
        "features_drifted": {
            "feature_1": {"score": 0.25, "test": "ks_test", "p_value": 0.03},
            "feature_3": {"score": 0.15, "test": "chi_squared", "p_value": 0.08}
        },
        "recommendations": [
            "Monitor feature_1 closely",
            "Consider retraining if drift continues"
        ]
    }


@router.get("/dashboard-summary")
async def get_monitoring_dashboard(
    tenant_id: str = Query(..., description="Tenant ID"),
    time_range: str = Query("24h", description="Time range (1h, 24h, 7d, 30d)")
):
    """Get monitoring dashboard summary"""
    return {
        "summary": {
            "total_models_monitored": 15,
            "active_alerts": 3,
            "models_with_drift": 2,
            "average_model_performance": 0.92,
            "total_predictions_24h": 150000
        },
        "alerts_by_severity": {
            "critical": 0,
            "warning": 3,
            "info": 5
        },
        "top_issues": [
            {
                "model_id": "model_123",
                "issue": "Performance degradation",
                "severity": "warning",
                "duration": "2 hours"
            }
        ],
        "recent_events": [
            {
                "timestamp": datetime.utcnow(),
                "event": "Drift detected in model_456",
                "severity": "warning"
            }
        ],
        "time_range": time_range
    } 