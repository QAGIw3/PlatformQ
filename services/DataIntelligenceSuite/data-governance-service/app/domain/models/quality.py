"""
Quality domain models extending data-intelligence-common
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any, Union
from enum import Enum

# Import from common library
from data_intelligence_common.models.base_models import BaseModel
from data_intelligence_common.models.data_models import (
    DataQualityMetric,
    DataQualityDimension,
    DataQualityRule,
    ValidationResult
)
from data_intelligence_common.core.processing.quality_processor import (
    QualityCheckType,
    QualityCheckResult,
    DataQualityProfile
)


class QualityRuleType(str, Enum):
    """Extended quality rule types"""
    # Basic rules from common
    COMPLETENESS = "completeness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    
    # Advanced rules
    REFERENTIAL_INTEGRITY = "referential_integrity"
    BUSINESS_RULE = "business_rule"
    STATISTICAL = "statistical"
    ML_BASED = "ml_based"
    CUSTOM_SQL = "custom_sql"
    CUSTOM_PYTHON = "custom_python"


class RemediationStrategy(str, Enum):
    """Remediation strategies"""
    AUTO_FIX = "auto_fix"
    MANUAL_REVIEW = "manual_review"
    QUARANTINE = "quarantine"
    ALERT_ONLY = "alert_only"
    ROLLBACK = "rollback"
    CUSTOM = "custom"


@dataclass
class QualityRuleDefinition(DataQualityRule):
    """Extended quality rule definition"""
    rule_type: QualityRuleType = QualityRuleType.VALIDITY
    
    # Advanced configuration
    ml_model_id: Optional[str] = None
    statistical_params: Dict[str, Any] = field(default_factory=dict)
    custom_code: Optional[str] = None
    
    # Remediation
    remediation_strategy: RemediationStrategy = RemediationStrategy.ALERT_ONLY
    remediation_config: Dict[str, Any] = field(default_factory=dict)
    auto_fix_enabled: bool = False
    
    # Scheduling
    schedule_cron: Optional[str] = None
    priority: int = 0
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)
    
    # Governance
    owner: Optional[str] = None
    approved_by: Optional[str] = None
    compliance_tags: List[str] = field(default_factory=list)


@dataclass
class QualityCheckRequest(BaseModel):
    """Quality check request"""
    entity_id: str
    entity_type: str = "dataset"
    
    # Check configuration
    rule_ids: Optional[List[str]] = None
    check_types: Optional[List[QualityCheckType]] = None
    
    # Sampling
    sample_size: Optional[int] = None
    sample_percentage: Optional[float] = None
    
    # Options
    async_execution: bool = False
    store_results: bool = True
    trigger_remediation: bool = True
    
    # Context
    triggered_by: Optional[str] = None
    correlation_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class EnhancedQualityProfile(DataQualityProfile):
    """Enhanced quality profile with governance features"""
    # Governance
    data_classification: Optional[str] = None
    sensitivity_level: Optional[str] = None
    retention_period_days: Optional[int] = None
    
    # Advanced profiling
    statistical_summary: Dict[str, Any] = field(default_factory=dict)
    anomaly_scores: Dict[str, float] = field(default_factory=dict)
    
    # ML insights
    predicted_quality_score: Optional[float] = None
    quality_trend: Optional[str] = None
    risk_indicators: List[str] = field(default_factory=list)
    
    # Relationships
    related_entities: List[str] = field(default_factory=list)
    upstream_quality_scores: Dict[str, float] = field(default_factory=dict)
    
    # Recommendations
    improvement_suggestions: List[Dict[str, Any]] = field(default_factory=list)
    optimization_opportunities: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class QualityIncident(BaseModel):
    """Quality incident tracking"""
    incident_id: str
    entity_id: str
    rule_id: str
    
    # Incident details
    severity: str  # critical, high, medium, low
    impact_score: float
    affected_records: int
    
    # Detection
    detected_at: datetime = field(default_factory=datetime.utcnow)
    detection_method: str = "rule_based"
    
    # Status
    status: str = "open"  # open, investigating, resolved, closed
    assigned_to: Optional[str] = None
    
    # Resolution
    resolution_type: Optional[str] = None
    resolved_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None
    
    # Impact
    downstream_impacts: List[Dict[str, Any]] = field(default_factory=list)
    business_impact: Optional[str] = None
    
    # Root cause
    root_cause: Optional[str] = None
    contributing_factors: List[str] = field(default_factory=list)


@dataclass
class RemediationAction(BaseModel):
    """Remediation action"""
    action_id: str
    incident_id: str
    action_type: RemediationStrategy
    
    # Action details
    description: str
    automated: bool = True
    
    # Execution
    status: str = "pending"  # pending, running, completed, failed
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Results
    records_affected: int = 0
    records_fixed: int = 0
    success_rate: Optional[float] = None
    
    # Validation
    validation_required: bool = True
    validated_by: Optional[str] = None
    validation_result: Optional[bool] = None
    
    # Audit
    executed_by: Optional[str] = None
    execution_log: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class QualityMetricHistory(BaseModel):
    """Quality metric history"""
    entity_id: str
    metric_type: str
    
    # Time series data
    timestamps: List[datetime] = field(default_factory=list)
    values: List[float] = field(default_factory=list)
    
    # Statistics
    mean_value: Optional[float] = None
    std_deviation: Optional[float] = None
    trend: Optional[str] = None  # improving, stable, degrading
    
    # Anomalies
    anomaly_points: List[int] = field(default_factory=list)
    anomaly_scores: List[float] = field(default_factory=list)
    
    # Forecasting
    forecast_values: List[float] = field(default_factory=list)
    forecast_confidence: List[float] = field(default_factory=list)
    
    # Metadata
    aggregation_level: str = "daily"
    last_updated: datetime = field(default_factory=datetime.utcnow)


@dataclass
class QualityDashboard(BaseModel):
    """Quality dashboard configuration"""
    dashboard_id: str
    name: str
    description: Optional[str] = None
    
    # Widgets
    widgets: List[Dict[str, Any]] = field(default_factory=list)
    
    # Filters
    default_filters: Dict[str, Any] = field(default_factory=dict)
    available_filters: List[str] = field(default_factory=list)
    
    # Layout
    layout_config: Dict[str, Any] = field(default_factory=dict)
    
    # Sharing
    owner: str
    shared_with: List[str] = field(default_factory=list)
    is_public: bool = False
    
    # Refresh
    auto_refresh: bool = True
    refresh_interval_seconds: int = 300
    
    # Alerts
    alert_rules: List[Dict[str, Any]] = field(default_factory=list)
    notification_channels: List[str] = field(default_factory=list) 