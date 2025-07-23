"""
Anomaly Detection for Data Quality.
"""

import asyncio
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class AnomalyType(str, Enum):
    """Types of anomalies."""
    OUTLIER = "outlier"
    DRIFT = "drift"
    PATTERN = "pattern"
    STRUCTURAL = "structural"
    TEMPORAL = "temporal"
    MULTIVARIATE = "multivariate"


class DetectionMethod(str, Enum):
    """Anomaly detection methods."""
    ISOLATION_FOREST = "isolation_forest"
    ZSCORE = "zscore"
    IQR = "iqr"
    DBSCAN = "dbscan"
    AUTOENCODER = "autoencoder"
    PROPHET = "prophet"
    CUSTOM = "custom"


@dataclass
class AnomalyScore:
    """Anomaly score for a data point."""
    value: float
    confidence: float
    method: DetectionMethod
    threshold: float
    is_anomaly: bool


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""
    dataset_id: str
    detection_id: str
    timestamp: datetime
    anomaly_type: AnomalyType
    method: DetectionMethod
    
    # Results
    total_records: int
    anomaly_count: int
    anomaly_rate: float
    anomaly_indices: List[int]
    anomaly_scores: List[AnomalyScore]
    
    # Analysis
    severity: str  # low, medium, high, critical
    confidence: float
    
    # Metadata
    execution_time_ms: float
    parameters: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


class AnomalyDetector:
    """
    Advanced anomaly detection for data quality.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        
        # Detection models
        self.models: Dict[str, Any] = {}
        self.scalers: Dict[str, StandardScaler] = {}
        
        # Detection history
        self.detection_history: List[AnomalyResult] = []
        
        # Configuration
        self.contamination_rate = 0.1  # Expected anomaly rate
        self.min_samples = 100
        
        logger.info("Anomaly Detector initialized")
        
    async def initialize(self):
        """Initialize anomaly detector."""
        # Subscribe to events
        await self.event_bus.subscribe("quality.anomaly.detect", self._handle_detection_request)
        
        logger.info("Anomaly Detector ready")
        
    async def detect_anomalies(
        self,
        data: Union[pd.DataFrame, np.ndarray],
        dataset_id: str,
        anomaly_type: AnomalyType = AnomalyType.OUTLIER,
        method: Optional[DetectionMethod] = None,
        columns: Optional[List[str]] = None,
        **kwargs
    ) -> AnomalyResult:
        """
        Detect anomalies in data.
        
        Args:
            data: Data to analyze
            dataset_id: Dataset identifier
            anomaly_type: Type of anomaly to detect
            method: Detection method (auto-selected if None)
            columns: Specific columns to analyze
            **kwargs: Additional parameters for detection method
            
        Returns:
            AnomalyResult with detected anomalies
        """
        start_time = datetime.utcnow()
        detection_id = f"anomaly_{dataset_id}_{start_time.timestamp()}"
        
        # Convert to DataFrame if needed
        if isinstance(data, np.ndarray):
            data = pd.DataFrame(data)
        
        # Filter columns if specified
        if columns:
            data = data[columns]
        
        # Select detection method if not specified
        if method is None:
            method = self._select_method(anomaly_type, data)
        
        # Detect anomalies based on type and method
        if anomaly_type == AnomalyType.OUTLIER:
            result = await self._detect_outliers(data, dataset_id, method, **kwargs)
        elif anomaly_type == AnomalyType.DRIFT:
            result = await self._detect_drift(data, dataset_id, method, **kwargs)
        elif anomaly_type == AnomalyType.MULTIVARIATE:
            result = await self._detect_multivariate(data, dataset_id, method, **kwargs)
        elif anomaly_type == AnomalyType.TEMPORAL:
            result = await self._detect_temporal(data, dataset_id, method, **kwargs)
        else:
            # Default to outlier detection
            result = await self._detect_outliers(data, dataset_id, method, **kwargs)
        
        # Calculate execution time
        execution_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        # Update result
        result.detection_id = detection_id
        result.timestamp = start_time
        result.execution_time_ms = execution_time
        result.dataset_id = dataset_id
        result.anomaly_type = anomaly_type
        result.method = method
        
        # Determine severity
        result.severity = self._calculate_severity(result.anomaly_rate, result.confidence)
        
        # Store result
        self.detection_history.append(result)
        
        # Cache result
        await self.cache_manager.set(
            f"quality:anomaly:{detection_id}",
            self._serialize_result(result),
            ttl=86400  # 24 hours
        )
        
        # Publish event
        await self.event_bus.publish("quality.anomaly.detected", {
            "detection_id": detection_id,
            "dataset_id": dataset_id,
            "anomaly_type": anomaly_type.value,
            "anomaly_count": result.anomaly_count,
            "severity": result.severity
        })
        
        logger.info(
            f"Anomaly detection complete for {dataset_id}: "
            f"{result.anomaly_count} anomalies found ({result.anomaly_rate:.1%})"
        )
        
        return result
        
    async def _detect_outliers(
        self,
        data: pd.DataFrame,
        dataset_id: str,
        method: DetectionMethod,
        **kwargs
    ) -> AnomalyResult:
        """Detect outlier anomalies."""
        if method == DetectionMethod.ISOLATION_FOREST:
            return await self._detect_isolation_forest(data, dataset_id, **kwargs)
        elif method == DetectionMethod.ZSCORE:
            return await self._detect_zscore(data, dataset_id, **kwargs)
        elif method == DetectionMethod.IQR:
            return await self._detect_iqr(data, dataset_id, **kwargs)
        else:
            # Default to Isolation Forest
            return await self._detect_isolation_forest(data, dataset_id, **kwargs)
            
    async def _detect_isolation_forest(
        self,
        data: pd.DataFrame,
        dataset_id: str,
        contamination: Optional[float] = None,
        n_estimators: int = 100,
        **kwargs
    ) -> AnomalyResult:
        """Detect anomalies using Isolation Forest."""
        # Prepare data
        numeric_data = data.select_dtypes(include=[np.number])
        if numeric_data.empty:
            raise ValueError("No numeric columns found for anomaly detection")
        
        # Handle missing values
        numeric_data = numeric_data.fillna(numeric_data.mean())
        
        # Scale data
        scaler_key = f"{dataset_id}_isolation"
        if scaler_key not in self.scalers:
            self.scalers[scaler_key] = StandardScaler()
        
        scaled_data = self.scalers[scaler_key].fit_transform(numeric_data)
        
        # Train Isolation Forest
        contamination = contamination or self.contamination_rate
        model = IsolationForest(
            contamination=contamination,
            n_estimators=n_estimators,
            random_state=42,
            **kwargs
        )
        
        # Fit and predict
        predictions = model.fit_predict(scaled_data)
        anomaly_scores = model.score_samples(scaled_data)
        
        # Convert to anomaly indices
        anomaly_mask = predictions == -1
        anomaly_indices = np.where(anomaly_mask)[0].tolist()
        
        # Create scores
        scores = []
        threshold = np.percentile(anomaly_scores, contamination * 100)
        
        for i, score in enumerate(anomaly_scores):
            scores.append(AnomalyScore(
                value=float(score),
                confidence=abs(score - threshold) / abs(threshold),
                method=DetectionMethod.ISOLATION_FOREST,
                threshold=float(threshold),
                is_anomaly=predictions[i] == -1
            ))
        
        # Create result
        result = AnomalyResult(
            dataset_id=dataset_id,
            detection_id="",  # Will be set by caller
            timestamp=datetime.utcnow(),
            anomaly_type=AnomalyType.OUTLIER,
            method=DetectionMethod.ISOLATION_FOREST,
            total_records=len(data),
            anomaly_count=len(anomaly_indices),
            anomaly_rate=len(anomaly_indices) / len(data),
            anomaly_indices=anomaly_indices,
            anomaly_scores=scores,
            severity="",  # Will be calculated by caller
            confidence=0.8,  # Isolation Forest confidence
            execution_time_ms=0,  # Will be set by caller
            parameters={
                "contamination": contamination,
                "n_estimators": n_estimators
            }
        )
        
        return result
        
    async def _detect_zscore(
        self,
        data: pd.DataFrame,
        dataset_id: str,
        threshold: float = 3.0,
        **kwargs
    ) -> AnomalyResult:
        """Detect anomalies using Z-score method."""
        numeric_data = data.select_dtypes(include=[np.number])
        if numeric_data.empty:
            raise ValueError("No numeric columns found for anomaly detection")
        
        anomaly_indices = []
        scores = []
        
        for col in numeric_data.columns:
            col_data = numeric_data[col].dropna()
            if len(col_data) == 0:
                continue
            
            # Calculate z-scores
            mean = col_data.mean()
            std = col_data.std()
            
            if std > 0:
                z_scores = np.abs((col_data - mean) / std)
                col_anomalies = z_scores > threshold
                
                # Get indices
                col_indices = col_data[col_anomalies].index.tolist()
                anomaly_indices.extend(col_indices)
                
                # Create scores for this column
                for idx, z_score in zip(col_data.index, z_scores):
                    scores.append(AnomalyScore(
                        value=float(z_score),
                        confidence=min(z_score / threshold, 1.0) if z_score > threshold else 0.0,
                        method=DetectionMethod.ZSCORE,
                        threshold=threshold,
                        is_anomaly=z_score > threshold
                    ))
        
        # Remove duplicates
        anomaly_indices = list(set(anomaly_indices))
        
        result = AnomalyResult(
            dataset_id=dataset_id,
            detection_id="",
            timestamp=datetime.utcnow(),
            anomaly_type=AnomalyType.OUTLIER,
            method=DetectionMethod.ZSCORE,
            total_records=len(data),
            anomaly_count=len(anomaly_indices),
            anomaly_rate=len(anomaly_indices) / len(data),
            anomaly_indices=anomaly_indices,
            anomaly_scores=scores,
            severity="",
            confidence=0.9,  # Z-score confidence
            execution_time_ms=0,
            parameters={"threshold": threshold}
        )
        
        return result
        
    async def _detect_iqr(
        self,
        data: pd.DataFrame,
        dataset_id: str,
        multiplier: float = 1.5,
        **kwargs
    ) -> AnomalyResult:
        """Detect anomalies using IQR method."""
        numeric_data = data.select_dtypes(include=[np.number])
        if numeric_data.empty:
            raise ValueError("No numeric columns found for anomaly detection")
        
        anomaly_indices = []
        scores = []
        
        for col in numeric_data.columns:
            col_data = numeric_data[col].dropna()
            if len(col_data) == 0:
                continue
            
            # Calculate IQR
            q1 = col_data.quantile(0.25)
            q3 = col_data.quantile(0.75)
            iqr = q3 - q1
            
            # Calculate bounds
            lower_bound = q1 - multiplier * iqr
            upper_bound = q3 + multiplier * iqr
            
            # Find anomalies
            col_anomalies = (col_data < lower_bound) | (col_data > upper_bound)
            col_indices = col_data[col_anomalies].index.tolist()
            anomaly_indices.extend(col_indices)
            
            # Create scores
            for idx, value in zip(col_data.index, col_data):
                if value < lower_bound:
                    distance = lower_bound - value
                    score = distance / iqr if iqr > 0 else 0
                elif value > upper_bound:
                    distance = value - upper_bound
                    score = distance / iqr if iqr > 0 else 0
                else:
                    score = 0
                
                scores.append(AnomalyScore(
                    value=float(score),
                    confidence=min(score, 1.0),
                    method=DetectionMethod.IQR,
                    threshold=multiplier,
                    is_anomaly=score > 0
                ))
        
        # Remove duplicates
        anomaly_indices = list(set(anomaly_indices))
        
        result = AnomalyResult(
            dataset_id=dataset_id,
            detection_id="",
            timestamp=datetime.utcnow(),
            anomaly_type=AnomalyType.OUTLIER,
            method=DetectionMethod.IQR,
            total_records=len(data),
            anomaly_count=len(anomaly_indices),
            anomaly_rate=len(anomaly_indices) / len(data),
            anomaly_indices=anomaly_indices,
            anomaly_scores=scores,
            severity="",
            confidence=0.85,  # IQR confidence
            execution_time_ms=0,
            parameters={"multiplier": multiplier}
        )
        
        return result
        
    async def _detect_drift(
        self,
        data: pd.DataFrame,
        dataset_id: str,
        method: DetectionMethod,
        reference_data: Optional[pd.DataFrame] = None,
        **kwargs
    ) -> AnomalyResult:
        """Detect data drift."""
        # This would implement drift detection between current and reference data
        # For now, return a placeholder
        return AnomalyResult(
            dataset_id=dataset_id,
            detection_id="",
            timestamp=datetime.utcnow(),
            anomaly_type=AnomalyType.DRIFT,
            method=method,
            total_records=len(data),
            anomaly_count=0,
            anomaly_rate=0.0,
            anomaly_indices=[],
            anomaly_scores=[],
            severity="low",
            confidence=0.0,
            execution_time_ms=0
        )
        
    async def _detect_multivariate(
        self,
        data: pd.DataFrame,
        dataset_id: str,
        method: DetectionMethod,
        **kwargs
    ) -> AnomalyResult:
        """Detect multivariate anomalies."""
        # Use PCA + Isolation Forest for multivariate detection
        numeric_data = data.select_dtypes(include=[np.number])
        if numeric_data.empty:
            raise ValueError("No numeric columns found")
        
        # Handle missing values
        numeric_data = numeric_data.fillna(numeric_data.mean())
        
        # Apply PCA for dimensionality reduction
        n_components = min(numeric_data.shape[1], 10)
        pca = PCA(n_components=n_components)
        pca_data = pca.fit_transform(numeric_data)
        
        # Apply Isolation Forest on PCA components
        pca_df = pd.DataFrame(pca_data)
        result = await self._detect_isolation_forest(pca_df, dataset_id, **kwargs)
        
        # Update result type
        result.anomaly_type = AnomalyType.MULTIVARIATE
        result.metadata["pca_explained_variance"] = pca.explained_variance_ratio_.tolist()
        
        return result
        
    async def _detect_temporal(
        self,
        data: pd.DataFrame,
        dataset_id: str,
        method: DetectionMethod,
        time_column: Optional[str] = None,
        **kwargs
    ) -> AnomalyResult:
        """Detect temporal anomalies."""
        # This would implement time-series anomaly detection
        # For now, return a placeholder
        return AnomalyResult(
            dataset_id=dataset_id,
            detection_id="",
            timestamp=datetime.utcnow(),
            anomaly_type=AnomalyType.TEMPORAL,
            method=method,
            total_records=len(data),
            anomaly_count=0,
            anomaly_rate=0.0,
            anomaly_indices=[],
            anomaly_scores=[],
            severity="low",
            confidence=0.0,
            execution_time_ms=0
        )
        
    def _select_method(self, anomaly_type: AnomalyType, data: pd.DataFrame) -> DetectionMethod:
        """Auto-select detection method based on data characteristics."""
        numeric_cols = data.select_dtypes(include=[np.number]).columns
        
        if anomaly_type == AnomalyType.OUTLIER:
            if len(numeric_cols) > 5:
                return DetectionMethod.ISOLATION_FOREST
            else:
                return DetectionMethod.ZSCORE
        elif anomaly_type == AnomalyType.MULTIVARIATE:
            return DetectionMethod.ISOLATION_FOREST
        else:
            return DetectionMethod.ZSCORE
            
    def _calculate_severity(self, anomaly_rate: float, confidence: float) -> str:
        """Calculate anomaly severity."""
        score = anomaly_rate * confidence
        
        if score > 0.3:
            return "critical"
        elif score > 0.2:
            return "high"
        elif score > 0.1:
            return "medium"
        else:
            return "low"
            
    def _serialize_result(self, result: AnomalyResult) -> Dict[str, Any]:
        """Serialize result for caching."""
        return {
            "dataset_id": result.dataset_id,
            "detection_id": result.detection_id,
            "timestamp": result.timestamp.isoformat(),
            "anomaly_type": result.anomaly_type.value,
            "method": result.method.value,
            "total_records": result.total_records,
            "anomaly_count": result.anomaly_count,
            "anomaly_rate": result.anomaly_rate,
            "severity": result.severity,
            "confidence": result.confidence,
            "execution_time_ms": result.execution_time_ms,
            "parameters": result.parameters
        }
        
    async def _handle_detection_request(self, event_data: Dict[str, Any]):
        """Handle anomaly detection request."""
        try:
            dataset_id = event_data["dataset_id"]
            data = event_data["data"]
            anomaly_type = AnomalyType(event_data.get("anomaly_type", "outlier"))
            
            result = await self.detect_anomalies(data, dataset_id, anomaly_type)
            
            # Publish result
            await self.event_bus.publish("quality.anomaly.result", self._serialize_result(result))
            
        except Exception as e:
            logger.error(f"Error handling detection request: {e}")
            await self.event_bus.publish("quality.anomaly.error", {
                "error": str(e),
                "event_data": event_data
            })
            
    async def get_anomaly_patterns(
        self,
        dataset_id: str,
        time_window: Optional[timedelta] = None
    ) -> Dict[str, Any]:
        """Analyze anomaly patterns over time."""
        # Filter history by dataset and time
        relevant_results = [
            r for r in self.detection_history
            if r.dataset_id == dataset_id
        ]
        
        if time_window:
            cutoff = datetime.utcnow() - time_window
            relevant_results = [
                r for r in relevant_results
                if r.timestamp >= cutoff
            ]
        
        if not relevant_results:
            return {"patterns": [], "trends": {}}
        
        # Analyze patterns
        patterns = {
            "average_anomaly_rate": np.mean([r.anomaly_rate for r in relevant_results]),
            "anomaly_rate_trend": self._calculate_trend([r.anomaly_rate for r in relevant_results]),
            "common_indices": self._find_common_indices(relevant_results),
            "severity_distribution": self._calculate_severity_distribution(relevant_results)
        }
        
        return patterns
        
    def _calculate_trend(self, values: List[float]) -> str:
        """Calculate trend direction."""
        if len(values) < 2:
            return "stable"
        
        # Simple linear regression
        x = np.arange(len(values))
        slope = np.polyfit(x, values, 1)[0]
        
        if slope > 0.01:
            return "increasing"
        elif slope < -0.01:
            return "decreasing"
        else:
            return "stable"
            
    def _find_common_indices(self, results: List[AnomalyResult]) -> List[int]:
        """Find indices that appear as anomalies frequently."""
        from collections import Counter
        
        all_indices = []
        for result in results:
            all_indices.extend(result.anomaly_indices)
        
        # Count occurrences
        counter = Counter(all_indices)
        
        # Return indices that appear in >50% of results
        threshold = len(results) * 0.5
        return [idx for idx, count in counter.items() if count > threshold]
        
    def _calculate_severity_distribution(self, results: List[AnomalyResult]) -> Dict[str, float]:
        """Calculate distribution of severity levels."""
        severities = [r.severity for r in results]
        total = len(severities)
        
        if total == 0:
            return {}
        
        distribution = {}
        for severity in ["low", "medium", "high", "critical"]:
            count = severities.count(severity)
            distribution[severity] = count / total
        
        return distribution 