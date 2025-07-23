"""
Model Monitoring and Drift Detection
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import numpy as np
from scipy import stats
import logging

from pyignite import AsyncClient
from prometheus_client import Counter, Histogram, Gauge
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score

from .config import settings
from ..integrations.event_driven_ml import EventDrivenMLIntegration, MLEventType

logger = logging.getLogger(__name__)

# Prometheus metrics
drift_detected_counter = Counter('ml_drift_detected_total', 'Total drift detections', ['model_id', 'drift_type'])
performance_gauge = Gauge('ml_model_performance', 'Model performance score', ['model_id', 'metric'])
prediction_histogram = Histogram('ml_prediction_distribution', 'Prediction value distribution', ['model_id'])
monitoring_errors_counter = Counter('ml_monitoring_errors_total', 'Total monitoring errors')


class DriftType:
    """Types of drift detection"""
    COVARIATE = "covariate"  # Input distribution drift
    CONCEPT = "concept"      # Output/concept drift
    PREDICTION = "prediction" # Prediction distribution drift
    PERFORMANCE = "performance" # Performance degradation


class ModelMonitor:
    """Monitors model performance and detects drift"""
    
    def __init__(self, cache_client: Optional[AsyncClient] = None):
        self.cache_client = cache_client
        self.event_integration: Optional[EventDrivenMLIntegration] = None
        self._monitoring_tasks: Dict[str, asyncio.Task] = {}
        self._baseline_stats: Dict[str, Dict] = {}
        self._performance_history: Dict[str, List] = {}
        self._alert_cooldowns: Dict[str, datetime] = {}
        
    async def initialize(self, event_integration: EventDrivenMLIntegration):
        """Initialize the model monitor"""
        self.event_integration = event_integration
        logger.info("Model Monitor initialized")
        
    async def start_monitoring(self, model_id: str, config: Dict[str, Any]):
        """Start monitoring a model"""
        if model_id in self._monitoring_tasks:
            logger.warning(f"Model {model_id} is already being monitored")
            return
            
        # Store baseline statistics
        if "baseline_stats" in config:
            self._baseline_stats[model_id] = config["baseline_stats"]
            
        # Start monitoring task
        task = asyncio.create_task(self._monitor_model(model_id, config))
        self._monitoring_tasks[model_id] = task
        
        logger.info(f"Started monitoring for model {model_id}")
        
        # Publish event
        if self.event_integration:
            await self.event_integration.publish_event(
                MLEventType.MONITORING_STARTED,
                {
                    "model_id": model_id,
                    "monitoring_config": config,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
    async def stop_monitoring(self, model_id: str):
        """Stop monitoring a model"""
        if model_id in self._monitoring_tasks:
            self._monitoring_tasks[model_id].cancel()
            del self._monitoring_tasks[model_id]
            logger.info(f"Stopped monitoring for model {model_id}")
            
    async def _monitor_model(self, model_id: str, config: Dict[str, Any]):
        """Main monitoring loop for a model"""
        check_interval = config.get("check_interval", settings.drift_check_interval)
        
        try:
            while True:
                await asyncio.sleep(check_interval)
                
                # Check for drift
                drift_results = await self.check_drift(model_id)
                
                # Check performance
                performance_results = await self.check_performance(model_id)
                
                # Process results
                await self._process_monitoring_results(
                    model_id, drift_results, performance_results
                )
                
        except asyncio.CancelledError:
            logger.info(f"Monitoring cancelled for model {model_id}")
            raise
        except Exception as e:
            logger.error(f"Error monitoring model {model_id}: {e}")
            monitoring_errors_counter.inc()
            
    async def check_drift(self, model_id: str) -> Dict[str, Any]:
        """Check for various types of drift"""
        results = {
            "drift_detected": False,
            "drift_types": [],
            "details": {}
        }
        
        try:
            # Get recent predictions
            recent_data = await self._get_recent_predictions(model_id)
            if not recent_data:
                return results
                
            baseline = self._baseline_stats.get(model_id, {})
            if not baseline:
                logger.warning(f"No baseline stats for model {model_id}")
                return results
                
            # Check covariate drift (input distribution)
            if "input_stats" in baseline:
                covariate_drift = await self._check_covariate_drift(
                    recent_data.get("inputs", []),
                    baseline["input_stats"]
                )
                if covariate_drift["detected"]:
                    results["drift_detected"] = True
                    results["drift_types"].append(DriftType.COVARIATE)
                    results["details"]["covariate"] = covariate_drift
                    
            # Check prediction drift
            if "prediction_stats" in baseline:
                prediction_drift = await self._check_prediction_drift(
                    recent_data.get("predictions", []),
                    baseline["prediction_stats"]
                )
                if prediction_drift["detected"]:
                    results["drift_detected"] = True
                    results["drift_types"].append(DriftType.PREDICTION)
                    results["details"]["prediction"] = prediction_drift
                    
            # Update metrics
            if results["drift_detected"]:
                for drift_type in results["drift_types"]:
                    drift_detected_counter.labels(
                        model_id=model_id,
                        drift_type=drift_type
                    ).inc()
                    
        except Exception as e:
            logger.error(f"Error checking drift for model {model_id}: {e}")
            monitoring_errors_counter.inc()
            
        return results
        
    async def _check_covariate_drift(
        self, 
        recent_inputs: List[np.ndarray], 
        baseline_stats: Dict
    ) -> Dict[str, Any]:
        """Check for covariate drift using statistical tests"""
        if not recent_inputs:
            return {"detected": False}
            
        # Convert to numpy array
        recent_array = np.array(recent_inputs)
        
        # Perform Kolmogorov-Smirnov test for each feature
        drift_detected = False
        feature_drifts = []
        
        for feature_idx in range(recent_array.shape[1]):
            feature_data = recent_array[:, feature_idx]
            baseline_mean = baseline_stats.get(f"feature_{feature_idx}_mean", 0)
            baseline_std = baseline_stats.get(f"feature_{feature_idx}_std", 1)
            
            # Generate baseline distribution (assuming normal)
            baseline_samples = np.random.normal(
                baseline_mean, baseline_std, size=len(feature_data)
            )
            
            # KS test
            ks_stat, p_value = stats.ks_2samp(feature_data, baseline_samples)
            
            if p_value < 0.05:  # Significant drift
                drift_detected = True
                feature_drifts.append({
                    "feature_index": feature_idx,
                    "ks_statistic": float(ks_stat),
                    "p_value": float(p_value),
                    "current_mean": float(np.mean(feature_data)),
                    "baseline_mean": float(baseline_mean)
                })
                
        return {
            "detected": drift_detected,
            "feature_drifts": feature_drifts,
            "total_features": recent_array.shape[1],
            "drifted_features": len(feature_drifts)
        }
        
    async def _check_prediction_drift(
        self,
        recent_predictions: List[float],
        baseline_stats: Dict
    ) -> Dict[str, Any]:
        """Check for prediction distribution drift"""
        if not recent_predictions:
            return {"detected": False}
            
        recent_array = np.array(recent_predictions)
        
        # Calculate current statistics
        current_mean = np.mean(recent_array)
        current_std = np.std(recent_array)
        
        baseline_mean = baseline_stats.get("mean", 0)
        baseline_std = baseline_stats.get("std", 1)
        
        # Check if mean has shifted significantly (2 std)
        mean_shift = abs(current_mean - baseline_mean)
        drift_detected = mean_shift > 2 * baseline_std
        
        # Update prediction distribution histogram
        for pred in recent_predictions:
            prediction_histogram.labels(model_id="model").observe(pred)
            
        return {
            "detected": drift_detected,
            "current_mean": float(current_mean),
            "baseline_mean": float(baseline_mean),
            "mean_shift": float(mean_shift),
            "shift_std_units": float(mean_shift / baseline_std) if baseline_std > 0 else 0
        }
        
    async def check_performance(self, model_id: str) -> Dict[str, Any]:
        """Check model performance metrics"""
        results = {
            "performance_degraded": False,
            "metrics": {},
            "threshold": settings.performance_threshold
        }
        
        try:
            # Get recent predictions with ground truth
            recent_results = await self._get_recent_results(model_id)
            if not recent_results:
                return results
                
            y_true = recent_results.get("ground_truth", [])
            y_pred = recent_results.get("predictions", [])
            
            if not y_true or len(y_true) != len(y_pred):
                return results
                
            # Calculate metrics
            metrics = {
                "accuracy": accuracy_score(y_true, y_pred),
                "precision": precision_score(y_true, y_pred, average='weighted', zero_division=0),
                "recall": recall_score(y_true, y_pred, average='weighted', zero_division=0),
                "f1_score": f1_score(y_true, y_pred, average='weighted', zero_division=0)
            }
            
            results["metrics"] = metrics
            
            # Update Prometheus metrics
            for metric_name, value in metrics.items():
                performance_gauge.labels(
                    model_id=model_id,
                    metric=metric_name
                ).set(value)
                
            # Check if performance is below threshold
            if metrics["accuracy"] < settings.performance_threshold:
                results["performance_degraded"] = True
                
            # Store in history
            if model_id not in self._performance_history:
                self._performance_history[model_id] = []
            self._performance_history[model_id].append({
                "timestamp": datetime.utcnow(),
                "metrics": metrics
            })
            
            # Keep only last 100 entries
            self._performance_history[model_id] = self._performance_history[model_id][-100:]
            
        except Exception as e:
            logger.error(f"Error checking performance for model {model_id}: {e}")
            monitoring_errors_counter.inc()
            
        return results
        
    async def _process_monitoring_results(
        self,
        model_id: str,
        drift_results: Dict[str, Any],
        performance_results: Dict[str, Any]
    ):
        """Process monitoring results and trigger alerts if needed"""
        alerts_triggered = []
        
        # Check if we're in cooldown period
        if model_id in self._alert_cooldowns:
            if datetime.utcnow() < self._alert_cooldowns[model_id]:
                return
                
        # Process drift detection
        if drift_results.get("drift_detected"):
            alerts_triggered.append({
                "type": "drift",
                "severity": "high",
                "details": drift_results
            })
            
            # Publish drift event
            if self.event_integration:
                await self.event_integration.publish_event(
                    MLEventType.DRIFT_DETECTED,
                    {
                        "model_id": model_id,
                        "drift_types": drift_results["drift_types"],
                        "details": drift_results["details"],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
        # Process performance degradation
        if performance_results.get("performance_degraded"):
            alerts_triggered.append({
                "type": "performance",
                "severity": "critical",
                "details": performance_results
            })
            
            # Publish performance event
            if self.event_integration:
                await self.event_integration.publish_event(
                    MLEventType.PERFORMANCE_DEGRADED,
                    {
                        "model_id": model_id,
                        "metrics": performance_results["metrics"],
                        "threshold": performance_results["threshold"],
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
        # Set cooldown if alerts were triggered
        if alerts_triggered:
            self._alert_cooldowns[model_id] = (
                datetime.utcnow() + timedelta(minutes=settings.alert_cooldown_minutes)
            )
            
    async def _get_recent_predictions(self, model_id: str) -> Dict[str, List]:
        """Get recent predictions from cache or storage"""
        if not self.cache_client:
            return {}
            
        try:
            # Get from cache
            cache_key = f"predictions:{model_id}:recent"
            cached_data = await self.cache_client.get(cache_key)
            
            if cached_data:
                return cached_data
                
            # TODO: Implement fetching from storage if not in cache
            return {}
            
        except Exception as e:
            logger.error(f"Error getting recent predictions: {e}")
            return {}
            
    async def _get_recent_results(self, model_id: str) -> Dict[str, List]:
        """Get recent prediction results with ground truth"""
        if not self.cache_client:
            return {}
            
        try:
            cache_key = f"results:{model_id}:recent"
            cached_data = await self.cache_client.get(cache_key)
            
            if cached_data:
                return cached_data
                
            # TODO: Implement fetching from storage if not in cache
            return {}
            
        except Exception as e:
            logger.error(f"Error getting recent results: {e}")
            return {}
            
    async def get_monitoring_status(self, model_id: str) -> Dict[str, Any]:
        """Get current monitoring status for a model"""
        return {
            "model_id": model_id,
            "is_monitored": model_id in self._monitoring_tasks,
            "has_baseline": model_id in self._baseline_stats,
            "performance_history_count": len(self._performance_history.get(model_id, [])),
            "in_alert_cooldown": model_id in self._alert_cooldowns
        }
        
    async def set_baseline_stats(self, model_id: str, stats: Dict[str, Any]):
        """Set baseline statistics for a model"""
        self._baseline_stats[model_id] = stats
        logger.info(f"Set baseline stats for model {model_id}")
        
    async def get_performance_history(
        self, 
        model_id: str, 
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get performance history for a model"""
        history = self._performance_history.get(model_id, [])
        return history[-limit:] if history else [] 