"""
Real-time Model Monitoring and Drift Detection Flink Job

This job monitors ML model predictions in real-time, detecting:
- Data drift: changes in input data distribution
- Concept drift: changes in the relationship between features and target
- Model performance degradation
- Prediction anomalies
"""

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, Schema
from pyflink.datastream.functions import ProcessFunction, KeyedProcessFunction, AggregateFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, MapStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows
from pyflink.common.time import Time, Duration
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.datastream.connectors.pulsar import FlinkPulsarSource, FlinkPulsarSink, PulsarSourceBuilder, PulsarSinkBuilder
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import RuntimeContext
import json
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Optional
from scipy import stats
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PredictionData:
    """Container for prediction data"""
    def __init__(self):
        self.model_name: str = ""
        self.model_version: str = ""
        self.tenant_id: str = ""
        self.prediction_id: str = ""
        self.timestamp: datetime = None
        self.features: Dict[str, Any] = {}
        self.prediction: Any = None
        self.actual_label: Optional[Any] = None
        self.prediction_confidence: Optional[float] = None
        self.latency_ms: Optional[float] = None


class ModelMonitoringState:
    """State for monitoring model performance"""
    def __init__(self):
        self.baseline_statistics: Dict[str, Dict[str, float]] = {}
        self.current_window_data: List[Dict[str, Any]] = []
        self.drift_scores: List[float] = []
        self.performance_metrics: Dict[str, List[float]] = {
            "accuracy": [],
            "precision": [],
            "recall": [],
            "f1": []
        }
        self.alert_history: List[Dict[str, Any]] = []
        self.last_baseline_update: Optional[datetime] = None


class DataDriftDetector(ProcessFunction):
    """Detects data drift in model inputs"""
    
    def __init__(self, sensitivity: float = 0.05):
        self.sensitivity = sensitivity
        self.state_descriptor = None
        self.monitoring_state = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize state"""
        self.state_descriptor = ValueStateDescriptor(
            "monitoring_state",
            ModelMonitoringState
        )
        self.monitoring_state = runtime_context.get_state(self.state_descriptor)
        
    def process_element(self, value: Dict[str, Any], ctx: ProcessFunction.Context):
        """Process prediction data and detect drift"""
        # Parse prediction data
        prediction_data = self._parse_prediction_data(value)
        
        # Get or initialize state
        state = self.monitoring_state.value()
        if state is None:
            state = ModelMonitoringState()
            
        # Add to current window
        state.current_window_data.append({
            "features": prediction_data.features,
            "prediction": prediction_data.prediction,
            "timestamp": prediction_data.timestamp
        })
        
        # Process window if enough data
        if len(state.current_window_data) >= 100:  # Window size
            drift_detected, drift_results = self._detect_drift(state)
            
            if drift_detected:
                # Create a structured event for retraining
                retraining_event = {
                    "event_type": "RETRAINING_REQUIRED",
                    "reason": "DATA_DRIFT_DETECTED",
                    "model_name": prediction_data.model_name,
                    "model_version": prediction_data.model_version,
                    "tenant_id": prediction_data.tenant_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "drift_details": drift_results,
                    "severity": self._calculate_severity(drift_results)
                }
                yield retraining_event
                
                # Update alert history
                state.alert_history.append(alert)
                
            # Update baseline if no drift or periodic update
            if not drift_detected or self._should_update_baseline(state):
                self._update_baseline(state)
                
            # Clear window
            state.current_window_data = []
            
        # Update state
        self.monitoring_state.update(state)
        
    def _parse_prediction_data(self, value: Dict[str, Any]) -> PredictionData:
        """Parse raw prediction data"""
        data = PredictionData()
        data.model_name = value.get("model_name", "")
        data.model_version = value.get("model_version", "")
        data.tenant_id = value.get("tenant_id", "")
        data.prediction_id = value.get("prediction_id", "")
        data.timestamp = datetime.fromisoformat(value.get("timestamp", datetime.utcnow().isoformat()))
        data.features = value.get("features", {})
        data.prediction = value.get("prediction")
        data.prediction_confidence = value.get("confidence")
        data.latency_ms = value.get("latency_ms")
        return data
        
    def _detect_drift(self, state: ModelMonitoringState) -> Tuple[bool, Dict[str, Any]]:
        """Detect drift using statistical tests"""
        if not state.baseline_statistics:
            # No baseline yet
            return False, {}
            
        drift_results = {}
        drift_detected = False
        
        # Extract current window features
        current_features = pd.DataFrame([d["features"] for d in state.current_window_data])
        
        for feature_name in current_features.columns:
            if feature_name not in state.baseline_statistics:
                continue
                
            baseline_stats = state.baseline_statistics[feature_name]
            current_values = current_features[feature_name].dropna()
            
            if len(current_values) < 10:
                continue
                
            # Perform drift detection based on feature type
            if current_values.dtype in ['float64', 'int64']:
                # Numerical feature - use Kolmogorov-Smirnov test
                drift_score, p_value = self._ks_test(
                    current_values,
                    baseline_stats
                )
                
                if p_value < self.sensitivity:
                    drift_detected = True
                    drift_results[feature_name] = {
                        "test": "KS",
                        "drift_score": drift_score,
                        "p_value": p_value,
                        "baseline_mean": baseline_stats.get("mean"),
                        "current_mean": float(current_values.mean())
                    }
            else:
                # Categorical feature - use chi-square test
                drift_score, p_value = self._chi_square_test(
                    current_values,
                    baseline_stats
                )
                
                if p_value < self.sensitivity:
                    drift_detected = True
                    drift_results[feature_name] = {
                        "test": "Chi-Square",
                        "drift_score": drift_score,
                        "p_value": p_value
                    }
                    
        return drift_detected, drift_results
        
    def _ks_test(self, current_values: pd.Series, baseline_stats: Dict[str, float]) -> Tuple[float, float]:
        """Kolmogorov-Smirnov test for numerical features"""
        # Generate baseline distribution from stats
        baseline_mean = baseline_stats.get("mean", 0)
        baseline_std = baseline_stats.get("std", 1)
        baseline_size = baseline_stats.get("count", 1000)
        
        # Approximate baseline distribution
        baseline_samples = np.random.normal(baseline_mean, baseline_std, int(baseline_size))
        
        # Perform KS test
        ks_statistic, p_value = stats.ks_2samp(current_values, baseline_samples)
        
        return ks_statistic, p_value
        
    def _chi_square_test(self, current_values: pd.Series, baseline_stats: Dict[str, Any]) -> Tuple[float, float]:
        """Chi-square test for categorical features"""
        # Get value counts
        current_counts = current_values.value_counts()
        baseline_probs = baseline_stats.get("value_probs", {})
        
        if not baseline_probs:
            return 0.0, 1.0
            
        # Align categories
        all_categories = set(current_counts.index) | set(baseline_probs.keys())
        
        observed = []
        expected = []
        total_count = len(current_values)
        
        for category in all_categories:
            observed.append(current_counts.get(category, 0))
            expected_prob = baseline_probs.get(category, 0.001)  # Small probability for unseen categories
            expected.append(expected_prob * total_count)
            
        # Perform chi-square test
        chi2_statistic, p_value = stats.chisquare(observed, expected)
        
        return chi2_statistic, p_value
        
    def _calculate_severity(self, drift_results: Dict[str, Any]) -> str:
        """Calculate drift severity"""
        if not drift_results:
            return "LOW"
            
        # Count features with significant drift
        significant_features = sum(1 for r in drift_results.values() if r["p_value"] < 0.01)
        total_features = len(drift_results)
        
        drift_ratio = significant_features / total_features if total_features > 0 else 0
        
        if drift_ratio > 0.5:
            return "HIGH"
        elif drift_ratio > 0.2:
            return "MEDIUM"
        else:
            return "LOW"
            
    def _should_update_baseline(self, state: ModelMonitoringState) -> bool:
        """Check if baseline should be updated"""
        if state.last_baseline_update is None:
            return True
            
        # Update baseline every 7 days
        days_since_update = (datetime.utcnow() - state.last_baseline_update).days
        return days_since_update >= 7
        
    def _update_baseline(self, state: ModelMonitoringState):
        """Update baseline statistics"""
        if not state.current_window_data:
            return
            
        # Calculate new baseline statistics
        features_df = pd.DataFrame([d["features"] for d in state.current_window_data])
        
        state.baseline_statistics = {}
        
        for feature_name in features_df.columns:
            values = features_df[feature_name].dropna()
            
            if len(values) == 0:
                continue
                
            if values.dtype in ['float64', 'int64']:
                # Numerical feature statistics
                state.baseline_statistics[feature_name] = {
                    "mean": float(values.mean()),
                    "std": float(values.std()),
                    "min": float(values.min()),
                    "max": float(values.max()),
                    "count": len(values)
                }
            else:
                # Categorical feature statistics
                value_counts = values.value_counts(normalize=True)
                state.baseline_statistics[feature_name] = {
                    "value_probs": value_counts.to_dict(),
                    "unique_count": len(value_counts),
                    "count": len(values)
                }
                
        state.last_baseline_update = datetime.utcnow()


class ModelPerformanceMonitor(KeyedProcessFunction):
    """Monitor model performance metrics"""
    
    def __init__(self):
        self.performance_state = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize state"""
        self.performance_state = runtime_context.get_state(
            ValueStateDescriptor("performance_metrics", Dict[str, Any])
        )
        
    def process_element(self, value: Dict[str, Any], ctx: KeyedProcessFunction.Context):
        """Process predictions with ground truth"""
        # Only process if we have ground truth
        if "actual_label" not in value or value["actual_label"] is None:
            return
            
        # Get current state
        metrics = self.performance_state.value() or {
            "total_predictions": 0,
            "correct_predictions": 0,
            "prediction_counts": {},
            "confusion_matrix": {}
        }
        
        prediction = value["prediction"]
        actual = value["actual_label"]
        
        # Update metrics
        metrics["total_predictions"] += 1
        
        if prediction == actual:
            metrics["correct_predictions"] += 1
            
        # Update confusion matrix
        key = f"{actual}_{prediction}"
        metrics["confusion_matrix"][key] = metrics["confusion_matrix"].get(key, 0) + 1
        
        # Calculate performance metrics every 100 predictions
        if metrics["total_predictions"] % 100 == 0:
            accuracy = metrics["correct_predictions"] / metrics["total_predictions"]
            
            # Check for performance degradation
            if accuracy < 0.8:  # Threshold
                retraining_event = {
                    "event_type": "RETRAINING_REQUIRED",
                    "reason": "PERFORMANCE_DEGRADATION",
                    "model_name": value["model_name"],
                    "model_version": value["model_version"],
                    "tenant_id": value["tenant_id"],
                    "timestamp": datetime.utcnow().isoformat(),
                    "metrics": {
                        "current_accuracy": accuracy,
                        "total_predictions": metrics["total_predictions"]
                    },
                    "severity": "HIGH" if accuracy < 0.7 else "MEDIUM"
                }
                yield retraining_event
                
        # Update state
        self.performance_state.update(metrics)


class PredictionAnomalyDetector(ProcessFunction):
    """Detect anomalous predictions"""
    
    def __init__(self, anomaly_threshold: float = 3.0):
        self.anomaly_threshold = anomaly_threshold
        self.prediction_history = None
        
    def open(self, runtime_context: RuntimeContext):
        """Initialize state"""
        self.prediction_history = runtime_context.get_state(
            ListStateDescriptor("prediction_history", float)
        )
        
    def process_element(self, value: Dict[str, Any], ctx: ProcessFunction.Context):
        """Detect prediction anomalies"""
        if "prediction_confidence" not in value:
            return
            
        confidence = value["prediction_confidence"]
        
        # Get historical confidences
        history = list(self.prediction_history.get() or [])
        
        if len(history) > 50:
            # Calculate z-score
            mean_confidence = np.mean(history)
            std_confidence = np.std(history)
            
            if std_confidence > 0:
                z_score = abs(confidence - mean_confidence) / std_confidence
                
                if z_score > self.anomaly_threshold:
                    alert = {
                        "alert_type": "PREDICTION_ANOMALY", 
                        "model_name": value["model_name"],
                        "model_version": value["model_version"],
                        "tenant_id": value["tenant_id"],
                        "timestamp": datetime.utcnow().isoformat(),
                        "prediction_id": value["prediction_id"],
                        "confidence": confidence,
                        "expected_confidence": mean_confidence,
                        "z_score": z_score,
                        "severity": "MEDIUM"
                    }
                    
                    yield alert
                    
        # Update history
        history.append(confidence)
        if len(history) > 1000:
            history = history[-1000:]  # Keep last 1000
            
        self.prediction_history.update(history)


def create_model_monitoring_job():
    """Create and configure the model monitoring Flink job"""
    
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(4)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    
    # Configure checkpointing
    env.enable_checkpointing(30000)  # 30 seconds
    
    # Create table environment
    t_env = StreamTableEnvironment.create(env)
    
    # Configure Pulsar source for prediction events
    pulsar_source = FlinkPulsarSource.builder() \
        .set_service_url(settings.pulsar_url) \
        .set_admin_url(settings.pulsar_admin_url) \
        .set_subscription_name("model-monitoring-subscription") \
        .set_topics(["model-predictions", "model-predictions-with-labels"]) \
        .set_deserialization_schema(SimpleStringSchema()) \
        .build()
    
    # Create data stream with watermarks
    prediction_stream = env.add_source(pulsar_source) \
        .map(lambda x: json.loads(x)) \
        .assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
            .with_timestamp_assigner(lambda x: int(datetime.fromisoformat(x['timestamp']).timestamp() * 1000))
        )
    
    # Branch 1: Data drift detection
    drift_alerts = prediction_stream \
        .key_by(lambda x: f"{x['model_name']}_{x['model_version']}") \
        .process(DataDriftDetector(sensitivity=0.05))
    
    # Branch 2: Performance monitoring (for predictions with labels)
    performance_alerts = prediction_stream \
        .filter(lambda x: "actual_label" in x) \
        .key_by(lambda x: f"{x['model_name']}_{x['model_version']}") \
        .process(ModelPerformanceMonitor())
    
    # Branch 3: Anomaly detection
    anomaly_alerts = prediction_stream \
        .process(PredictionAnomalyDetector(anomaly_threshold=3.0))
    
    # Union the retraining events
    retraining_requests = drift_alerts.union(performance_alerts)
    
    # Configure Pulsar sink for retraining requests
    retraining_sink = FlinkPulsarSink.builder() \
        .set_service_url(settings.pulsar_url) \
        .set_admin_url(settings.pulsar_admin_url) \
        .set_topic("model-retraining-requests") \
        .set_serialization_schema(SimpleStringSchema()) \
        .build()
    
    # Write retraining requests to sink
    retraining_requests.map(lambda x: json.dumps(x)).add_sink(retraining_sink)
    
    # Also write to monitoring metrics sink for dashboards
    t_env.execute_sql("""
        CREATE TABLE monitoring_metrics_sink (
            model_name STRING,
            model_version STRING, 
            tenant_id STRING,
            metric_type STRING,
            metric_name STRING,
            metric_value DOUBLE,
            timestamp TIMESTAMP(3),
            PRIMARY KEY (model_name, model_version, tenant_id, metric_type, metric_name) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/metrics',
            'table-name' = 'model_monitoring_metrics',
            'username' = 'metrics_user',
            'password' = 'metrics_password'
        )
    """)
    
    # Execute job
    env.execute("Model Monitoring and Drift Detection Job")


if __name__ == "__main__":
    create_model_monitoring_job() 