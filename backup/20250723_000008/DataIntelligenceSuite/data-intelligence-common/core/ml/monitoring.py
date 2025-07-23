"""
ML model monitoring and drift detection.

Provides monitoring for model performance, data drift, and prediction quality.
"""

import uuid
from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
import numpy as np
from collections import defaultdict, deque
import statistics

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger, MetricsCollector

logger = StructuredLogger.get_logger(__name__)


class DriftType(str, Enum):
    """Types of drift"""
    DATA_DRIFT = "data_drift"
    CONCEPT_DRIFT = "concept_drift"
    PREDICTION_DRIFT = "prediction_drift"
    PERFORMANCE_DRIFT = "performance_drift"


class AlertSeverity(str, Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class MonitoringMetric(str, Enum):
    """Monitoring metric types"""
    ACCURACY = "accuracy"
    PRECISION = "precision"
    RECALL = "recall"
    F1_SCORE = "f1_score"
    AUC_ROC = "auc_roc"
    LATENCY = "latency"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    DRIFT_SCORE = "drift_score"
    CUSTOM = "custom"


@dataclass
class DriftResult:
    """Drift detection result"""
    drift_type: DriftType
    drift_score: float
    is_drifted: bool
    threshold: float
    
    # Details
    feature_scores: Optional[Dict[str, float]] = None
    statistical_test: Optional[str] = None
    p_value: Optional[float] = None
    
    # Metadata
    reference_window: Optional[Tuple[datetime, datetime]] = None
    current_window: Optional[Tuple[datetime, datetime]] = None
    sample_size: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "drift_type": self.drift_type.value,
            "drift_score": self.drift_score,
            "is_drifted": self.is_drifted,
            "threshold": self.threshold,
            "feature_scores": self.feature_scores,
            "statistical_test": self.statistical_test,
            "p_value": self.p_value,
            "reference_window": [
                self.reference_window[0].isoformat(),
                self.reference_window[1].isoformat()
            ] if self.reference_window else None,
            "current_window": [
                self.current_window[0].isoformat(),
                self.current_window[1].isoformat()
            ] if self.current_window else None,
            "sample_size": self.sample_size
        }


@dataclass
class PerformanceMetric:
    """Model performance metric"""
    metric_type: MonitoringMetric
    value: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Context
    model_id: Optional[str] = None
    model_version: Optional[str] = None
    
    # Aggregation info
    window_start: Optional[datetime] = None
    window_end: Optional[datetime] = None
    sample_count: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "metric_type": self.metric_type.value,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "model_id": self.model_id,
            "model_version": self.model_version,
            "window_start": self.window_start.isoformat() if self.window_start else None,
            "window_end": self.window_end.isoformat() if self.window_end else None,
            "sample_count": self.sample_count
        }


@dataclass
class MonitoringAlert:
    """Monitoring alert"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    alert_type: str = ""
    severity: AlertSeverity = AlertSeverity.WARNING
    
    # Alert details
    title: str = ""
    message: str = ""
    metric_value: Optional[float] = None
    threshold_value: Optional[float] = None
    
    # Context
    model_id: Optional[str] = None
    model_version: Optional[str] = None
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None
    is_resolved: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "alert_type": self.alert_type,
            "severity": self.severity.value,
            "title": self.title,
            "message": self.message,
            "metric_value": self.metric_value,
            "threshold_value": self.threshold_value,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "created_at": self.created_at.isoformat(),
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "is_resolved": self.is_resolved
        }


@dataclass
class MonitoringConfig:
    """Monitoring configuration"""
    # Performance monitoring
    performance_metrics: List[MonitoringMetric] = field(default_factory=list)
    performance_window_minutes: int = 60
    performance_aggregation: str = "mean"  # mean, median, p95, etc.
    
    # Drift detection
    drift_detection_enabled: bool = True
    drift_window_size: int = 1000
    drift_threshold: float = 0.1
    drift_method: str = "ks_test"  # ks_test, chi2, psi, etc.
    
    # Alerting
    alert_enabled: bool = True
    alert_thresholds: Dict[str, float] = field(default_factory=dict)
    alert_cooldown_minutes: int = 30
    
    # Data retention
    metrics_retention_days: int = 30
    raw_data_retention_days: int = 7


class ModelMonitor:
    """
    ML model monitoring system.
    
    Features:
    - Performance tracking
    - Data drift detection
    - Prediction drift monitoring
    - Alert management
    - Metric aggregation
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        self.metrics = metrics_collector
        
        # Storage
        self._configs: Dict[str, MonitoringConfig] = {}
        self._performance_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self._alerts: Dict[str, List[MonitoringAlert]] = defaultdict(list)
        
        # Reference data for drift detection
        self._reference_data: Dict[str, Dict[str, Any]] = {}
        self._feature_statistics: Dict[str, Dict[str, Any]] = {}
        
        # Alert tracking
        self._last_alert_time: Dict[str, datetime] = {}
        
    def configure_monitoring(
        self,
        model_id: str,
        version: str,
        config: MonitoringConfig
    ):
        """Configure monitoring for model"""
        key = f"{model_id}:{version}"
        self._configs[key] = config
        
        logger.info(f"Configured monitoring for model {key}")
        
    def record_prediction(
        self,
        model_id: str,
        version: str,
        features: Dict[str, Any],
        prediction: Any,
        actual: Optional[Any] = None,
        latency_ms: Optional[float] = None
    ):
        """Record prediction for monitoring"""
        key = f"{model_id}:{version}"
        config = self._configs.get(key)
        
        if not config:
            return
            
        # Record performance metrics
        if latency_ms is not None:
            self._record_metric(
                key,
                MonitoringMetric.LATENCY,
                latency_ms
            )
            
        # Check for drift if enabled
        if config.drift_detection_enabled:
            drift_result = self._check_drift(key, features)
            if drift_result and drift_result.is_drifted:
                self._create_drift_alert(key, drift_result)
                
        # Store for future analysis
        if actual is not None:
            # Calculate accuracy metrics when ground truth is available
            self._update_performance_metrics(key, prediction, actual)
            
    def _record_metric(
        self,
        model_key: str,
        metric_type: MonitoringMetric,
        value: float
    ):
        """Record performance metric"""
        metric = PerformanceMetric(
            metric_type=metric_type,
            value=value,
            model_id=model_key.split(":")[0],
            model_version=model_key.split(":")[1]
        )
        
        self._performance_metrics[model_key].append(metric)
        
        # Update Prometheus metrics if available
        if self.metrics:
            self.metrics.observe_histogram(
                f"ml_model_{metric_type.value}",
                value,
                labels={"model": model_key}
            )
            
        # Check thresholds
        config = self._configs.get(model_key)
        if config and config.alert_enabled:
            threshold_key = f"{metric_type.value}_threshold"
            if threshold_key in config.alert_thresholds:
                threshold = config.alert_thresholds[threshold_key]
                if value > threshold:
                    self._create_performance_alert(model_key, metric_type, value, threshold)
                    
    def _check_drift(
        self,
        model_key: str,
        features: Dict[str, Any]
    ) -> Optional[DriftResult]:
        """Check for data drift"""
        config = self._configs.get(model_key)
        if not config or not config.drift_detection_enabled:
            return None
            
        # Get reference data
        reference = self._reference_data.get(model_key)
        if not reference:
            return None
            
        # Perform drift detection based on method
        if config.drift_method == "ks_test":
            return self._ks_drift_test(model_key, features, reference, config)
        elif config.drift_method == "chi2":
            return self._chi2_drift_test(model_key, features, reference, config)
        elif config.drift_method == "psi":
            return self._psi_drift_test(model_key, features, reference, config)
        else:
            return None
            
    def _ks_drift_test(
        self,
        model_key: str,
        current_features: Dict[str, Any],
        reference_data: Dict[str, Any],
        config: MonitoringConfig
    ) -> DriftResult:
        """Kolmogorov-Smirnov drift test"""
        from scipy import stats
        
        feature_scores = {}
        max_drift = 0.0
        
        # Test each numeric feature
        for feature_name, current_value in current_features.items():
            if feature_name not in reference_data.get("features", {}):
                continue
                
            ref_values = reference_data["features"][feature_name]
            
            # Only test numeric features
            if isinstance(current_value, (int, float)) and ref_values:
                # Get recent values for current distribution
                recent_values = self._get_recent_feature_values(model_key, feature_name)
                if len(recent_values) < 30:  # Need sufficient samples
                    continue
                    
                # Perform KS test
                statistic, p_value = stats.ks_2samp(ref_values, recent_values)
                
                feature_scores[feature_name] = statistic
                max_drift = max(max_drift, statistic)
                
        # Overall drift score
        drift_score = max_drift if feature_scores else 0.0
        is_drifted = drift_score > config.drift_threshold
        
        return DriftResult(
            drift_type=DriftType.DATA_DRIFT,
            drift_score=drift_score,
            is_drifted=is_drifted,
            threshold=config.drift_threshold,
            feature_scores=feature_scores,
            statistical_test="ks_test"
        )
        
    def _chi2_drift_test(
        self,
        model_key: str,
        current_features: Dict[str, Any],
        reference_data: Dict[str, Any],
        config: MonitoringConfig
    ) -> DriftResult:
        """Chi-squared drift test for categorical features"""
        from scipy import stats
        
        feature_scores = {}
        max_drift = 0.0
        
        # Test each categorical feature
        for feature_name, current_value in current_features.items():
            if feature_name not in reference_data.get("features", {}):
                continue
                
            ref_distribution = reference_data["features"][feature_name]
            
            # Only test categorical features
            if isinstance(ref_distribution, dict):  # Stored as value counts
                # Get recent value distribution
                recent_distribution = self._get_recent_categorical_distribution(
                    model_key, feature_name
                )
                
                if not recent_distribution:
                    continue
                    
                # Align categories
                all_categories = set(ref_distribution.keys()) | set(recent_distribution.keys())
                ref_counts = [ref_distribution.get(cat, 0) for cat in all_categories]
                recent_counts = [recent_distribution.get(cat, 0) for cat in all_categories]
                
                # Perform chi-squared test
                if sum(recent_counts) > 0:
                    statistic, p_value = stats.chisquare(recent_counts, ref_counts)
                    
                    # Normalize by degrees of freedom
                    normalized_stat = statistic / (len(all_categories) - 1)
                    feature_scores[feature_name] = normalized_stat
                    max_drift = max(max_drift, normalized_stat)
                    
        # Overall drift score
        drift_score = max_drift if feature_scores else 0.0
        is_drifted = drift_score > config.drift_threshold
        
        return DriftResult(
            drift_type=DriftType.DATA_DRIFT,
            drift_score=drift_score,
            is_drifted=is_drifted,
            threshold=config.drift_threshold,
            feature_scores=feature_scores,
            statistical_test="chi2"
        )
        
    def _psi_drift_test(
        self,
        model_key: str,
        current_features: Dict[str, Any],
        reference_data: Dict[str, Any],
        config: MonitoringConfig
    ) -> DriftResult:
        """Population Stability Index (PSI) drift test"""
        feature_scores = {}
        max_psi = 0.0
        
        for feature_name, current_value in current_features.items():
            if feature_name not in reference_data.get("features", {}):
                continue
                
            ref_stats = reference_data["features"][feature_name]
            
            # Calculate PSI
            psi = self._calculate_psi(
                ref_stats.get("distribution", {}),
                self._get_recent_distribution(model_key, feature_name)
            )
            
            if psi is not None:
                feature_scores[feature_name] = psi
                max_psi = max(max_psi, psi)
                
        # Overall drift score
        drift_score = max_psi if feature_scores else 0.0
        is_drifted = drift_score > config.drift_threshold
        
        return DriftResult(
            drift_type=DriftType.DATA_DRIFT,
            drift_score=drift_score,
            is_drifted=is_drifted,
            threshold=config.drift_threshold,
            feature_scores=feature_scores,
            statistical_test="psi"
        )
        
    def _calculate_psi(
        self,
        reference_dist: Dict[str, float],
        current_dist: Dict[str, float]
    ) -> Optional[float]:
        """Calculate Population Stability Index"""
        if not reference_dist or not current_dist:
            return None
            
        psi = 0.0
        
        # Get all bins
        all_bins = set(reference_dist.keys()) | set(current_dist.keys())
        
        for bin_name in all_bins:
            ref_pct = reference_dist.get(bin_name, 0.001)  # Avoid log(0)
            curr_pct = current_dist.get(bin_name, 0.001)
            
            psi += (curr_pct - ref_pct) * np.log(curr_pct / ref_pct)
            
        return psi
        
    def _update_performance_metrics(
        self,
        model_key: str,
        predictions: Union[Any, List[Any]],
        actuals: Union[Any, List[Any]]
    ):
        """Update performance metrics with ground truth"""
        # Convert to lists if single values
        if not isinstance(predictions, list):
            predictions = [predictions]
        if not isinstance(actuals, list):
            actuals = [actuals]
            
        # Calculate metrics based on prediction type
        # Binary classification
        if all(isinstance(p, (int, float)) and p in [0, 1] for p in predictions):
            correct = sum(1 for p, a in zip(predictions, actuals) if p == a)
            accuracy = correct / len(predictions)
            
            self._record_metric(model_key, MonitoringMetric.ACCURACY, accuracy)
            
            # Calculate precision/recall if binary
            true_positives = sum(1 for p, a in zip(predictions, actuals) if p == 1 and a == 1)
            false_positives = sum(1 for p, a in zip(predictions, actuals) if p == 1 and a == 0)
            false_negatives = sum(1 for p, a in zip(predictions, actuals) if p == 0 and a == 1)
            
            if true_positives + false_positives > 0:
                precision = true_positives / (true_positives + false_positives)
                self._record_metric(model_key, MonitoringMetric.PRECISION, precision)
                
            if true_positives + false_negatives > 0:
                recall = true_positives / (true_positives + false_negatives)
                self._record_metric(model_key, MonitoringMetric.RECALL, recall)
                
                if precision + recall > 0:
                    f1 = 2 * (precision * recall) / (precision + recall)
                    self._record_metric(model_key, MonitoringMetric.F1_SCORE, f1)
                    
    def _create_drift_alert(
        self,
        model_key: str,
        drift_result: DriftResult
    ):
        """Create drift alert"""
        # Check cooldown
        if not self._should_create_alert(model_key, "drift"):
            return
            
        alert = MonitoringAlert(
            alert_type="drift_detected",
            severity=AlertSeverity.WARNING,
            title=f"Data drift detected for model {model_key}",
            message=f"Drift score {drift_result.drift_score:.3f} exceeds threshold {drift_result.threshold}",
            metric_value=drift_result.drift_score,
            threshold_value=drift_result.threshold,
            model_id=model_key.split(":")[0],
            model_version=model_key.split(":")[1]
        )
        
        self._alerts[model_key].append(alert)
        self._last_alert_time[f"{model_key}:drift"] = datetime.utcnow()
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="monitoring.alert.created",
                source="model_monitor",
                data=alert.to_dict()
            ))
            
        logger.warning(f"Drift alert created: {alert.title}")
        
    def _create_performance_alert(
        self,
        model_key: str,
        metric_type: MonitoringMetric,
        value: float,
        threshold: float
    ):
        """Create performance alert"""
        # Check cooldown
        alert_key = f"{metric_type.value}_threshold"
        if not self._should_create_alert(model_key, alert_key):
            return
            
        alert = MonitoringAlert(
            alert_type="performance_degradation",
            severity=AlertSeverity.ERROR,
            title=f"Performance degradation for model {model_key}",
            message=f"{metric_type.value} {value:.3f} exceeds threshold {threshold}",
            metric_value=value,
            threshold_value=threshold,
            model_id=model_key.split(":")[0],
            model_version=model_key.split(":")[1]
        )
        
        self._alerts[model_key].append(alert)
        self._last_alert_time[f"{model_key}:{alert_key}"] = datetime.utcnow()
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="monitoring.alert.created",
                source="model_monitor",
                data=alert.to_dict()
            ))
            
        logger.error(f"Performance alert created: {alert.title}")
        
    def _should_create_alert(
        self,
        model_key: str,
        alert_type: str
    ) -> bool:
        """Check if alert should be created based on cooldown"""
        config = self._configs.get(model_key)
        if not config:
            return False
            
        last_alert_key = f"{model_key}:{alert_type}"
        last_alert_time = self._last_alert_time.get(last_alert_key)
        
        if last_alert_time:
            cooldown = timedelta(minutes=config.alert_cooldown_minutes)
            if datetime.utcnow() - last_alert_time < cooldown:
                return False
                
        return True
        
    def set_reference_data(
        self,
        model_id: str,
        version: str,
        features: List[Dict[str, Any]],
        predictions: Optional[List[Any]] = None
    ):
        """Set reference data for drift detection"""
        key = f"{model_id}:{version}"
        
        # Calculate feature statistics
        feature_stats = {}
        
        for feature_name in features[0].keys():
            values = [f.get(feature_name) for f in features]
            
            # Numeric features
            if all(isinstance(v, (int, float)) for v in values if v is not None):
                feature_stats[feature_name] = values
            # Categorical features
            else:
                value_counts = defaultdict(int)
                for v in values:
                    if v is not None:
                        value_counts[str(v)] += 1
                        
                # Convert to distribution
                total = sum(value_counts.values())
                distribution = {
                    k: v / total for k, v in value_counts.items()
                }
                feature_stats[feature_name] = distribution
                
        self._reference_data[key] = {
            "features": feature_stats,
            "sample_size": len(features),
            "created_at": datetime.utcnow()
        }
        
        logger.info(f"Set reference data for model {key} with {len(features)} samples")
        
    def get_metrics(
        self,
        model_id: str,
        version: str,
        metric_type: Optional[MonitoringMetric] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> List[PerformanceMetric]:
        """Get performance metrics"""
        key = f"{model_id}:{version}"
        metrics = list(self._performance_metrics.get(key, []))
        
        # Filter by type
        if metric_type:
            metrics = [m for m in metrics if m.metric_type == metric_type]
            
        # Filter by time
        if start_time:
            metrics = [m for m in metrics if m.timestamp >= start_time]
        if end_time:
            metrics = [m for m in metrics if m.timestamp <= end_time]
            
        return metrics
        
    def get_alerts(
        self,
        model_id: Optional[str] = None,
        version: Optional[str] = None,
        severity: Optional[AlertSeverity] = None,
        is_resolved: Optional[bool] = None
    ) -> List[MonitoringAlert]:
        """Get monitoring alerts"""
        alerts = []
        
        # Get alerts for specific model or all
        if model_id and version:
            key = f"{model_id}:{version}"
            alerts = list(self._alerts.get(key, []))
        else:
            for alert_list in self._alerts.values():
                alerts.extend(alert_list)
                
        # Filter by severity
        if severity:
            alerts = [a for a in alerts if a.severity == severity]
            
        # Filter by resolution status
        if is_resolved is not None:
            alerts = [a for a in alerts if a.is_resolved == is_resolved]
            
        return alerts
        
    def resolve_alert(self, alert_id: str):
        """Resolve an alert"""
        for alert_list in self._alerts.values():
            for alert in alert_list:
                if alert.id == alert_id:
                    alert.is_resolved = True
                    alert.resolved_at = datetime.utcnow()
                    
                    # Publish event
                    if self.event_bus:
                        self.event_bus.publish(Event(
                            type="monitoring.alert.resolved",
                            source="model_monitor",
                            data={"alert_id": alert_id}
                        ))
                        
                    logger.info(f"Resolved alert: {alert_id}")
                    return
                    
    def get_monitoring_summary(
        self,
        model_id: str,
        version: str,
        window_hours: int = 24
    ) -> Dict[str, Any]:
        """Get monitoring summary for model"""
        key = f"{model_id}:{version}"
        config = self._configs.get(key)
        
        # Time window
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=window_hours)
        
        # Get metrics
        metrics = self.get_metrics(model_id, version, start_time=start_time)
        
        # Calculate aggregates
        metric_summary = {}
        for metric_type in MonitoringMetric:
            type_metrics = [m for m in metrics if m.metric_type == metric_type]
            if type_metrics:
                values = [m.value for m in type_metrics]
                metric_summary[metric_type.value] = {
                    "count": len(values),
                    "mean": statistics.mean(values),
                    "min": min(values),
                    "max": max(values),
                    "latest": type_metrics[-1].value
                }
                
        # Get alerts
        alerts = self.get_alerts(model_id, version)
        recent_alerts = [
            a for a in alerts
            if a.created_at >= start_time
        ]
        
        # Drift status
        drift_status = "unknown"
        if config and config.drift_detection_enabled:
            recent_drift_alerts = [
                a for a in recent_alerts
                if a.alert_type == "drift_detected"
            ]
            drift_status = "drifted" if recent_drift_alerts else "stable"
            
        return {
            "model_id": model_id,
            "version": version,
            "window_hours": window_hours,
            "metrics": metric_summary,
            "total_predictions": sum(
                m.get("count", 0) for m in metric_summary.values()
            ),
            "alerts": {
                "total": len(recent_alerts),
                "by_severity": {
                    s.value: len([a for a in recent_alerts if a.severity == s])
                    for s in AlertSeverity
                },
                "unresolved": len([a for a in recent_alerts if not a.is_resolved])
            },
            "drift_status": drift_status,
            "monitoring_enabled": config is not None
        }
        
    def _get_recent_feature_values(
        self,
        model_key: str,
        feature_name: str,
        limit: int = 1000
    ) -> List[float]:
        """Get recent feature values (placeholder)"""
        # In production, would retrieve from feature store
        return []
        
    def _get_recent_categorical_distribution(
        self,
        model_key: str,
        feature_name: str
    ) -> Dict[str, int]:
        """Get recent categorical distribution (placeholder)"""
        # In production, would retrieve from feature store
        return {}
        
    def _get_recent_distribution(
        self,
        model_key: str,
        feature_name: str
    ) -> Dict[str, float]:
        """Get recent distribution (placeholder)"""
        # In production, would retrieve from feature store
        return {} 