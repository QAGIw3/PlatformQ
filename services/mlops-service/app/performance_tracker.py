"""
Model Performance Tracker

Real-time accuracy monitoring for purchased/deployed models
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import asyncio
import httpx
from dataclasses import dataclass
from collections import defaultdict
import numpy as np
from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)

# Metrics
MODEL_PREDICTIONS = Counter('model_predictions_total', 'Total predictions', ['model_name', 'version', 'tenant_id'])
MODEL_ACCURACY = Gauge('model_accuracy', 'Current model accuracy', ['model_name', 'version', 'tenant_id'])
PREDICTION_LATENCY = Histogram('model_prediction_latency_seconds', 'Prediction latency', ['model_name', 'version'])
DRIFT_SCORE = Gauge('model_drift_score', 'Model drift score', ['model_name', 'version', 'drift_type'])

@dataclass
class PerformanceMetrics:
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    latency_p50: float
    latency_p95: float
    latency_p99: float
    prediction_count: int
    error_rate: float
    drift_score: float
    timestamp: datetime


class ModelPerformanceTracker:
    """Tracks model performance metrics in real-time"""
    
    def __init__(self, 
                 elasticsearch_url: str = "http://elasticsearch:9200",
                 monitoring_interval: int = 300):  # 5 minutes
        self.elasticsearch_url = elasticsearch_url
        self.monitoring_interval = monitoring_interval
        self.performance_history: Dict[str, List[PerformanceMetrics]] = defaultdict(list)
        self.prediction_buffer: Dict[str, List[Dict]] = defaultdict(list)
        self.ground_truth_buffer: Dict[str, List[Dict]] = defaultdict(list)
        self._monitoring_tasks: Dict[str, asyncio.Task] = {}
        
    async def start_monitoring(self, model_id: str, model_name: str, version: str, tenant_id: str):
        """Start monitoring a model's performance"""
        key = f"{tenant_id}:{model_name}:{version}"
        
        if key in self._monitoring_tasks:
            logger.warning(f"Already monitoring {key}")
            return
            
        task = asyncio.create_task(self._monitor_loop(key, model_id, model_name, version, tenant_id))
        self._monitoring_tasks[key] = task
        logger.info(f"Started monitoring {key}")
        
    async def stop_monitoring(self, model_name: str, version: str, tenant_id: str):
        """Stop monitoring a model"""
        key = f"{tenant_id}:{model_name}:{version}"
        
        if key in self._monitoring_tasks:
            self._monitoring_tasks[key].cancel()
            del self._monitoring_tasks[key]
            logger.info(f"Stopped monitoring {key}")
            
    async def record_prediction(self, 
                              model_name: str, 
                              version: str, 
                              tenant_id: str,
                              input_data: Dict[str, Any],
                              prediction: Any,
                              latency_ms: float,
                              prediction_id: str):
        """Record a model prediction"""
        key = f"{tenant_id}:{model_name}:{version}"
        
        self.prediction_buffer[key].append({
            "prediction_id": prediction_id,
            "timestamp": datetime.utcnow(),
            "input_data": input_data,
            "prediction": prediction,
            "latency_ms": latency_ms
        })
        
        # Update metrics
        MODEL_PREDICTIONS.labels(model_name=model_name, version=version, tenant_id=tenant_id).inc()
        PREDICTION_LATENCY.labels(model_name=model_name, version=version).observe(latency_ms / 1000)
        
        # Store in Elasticsearch
        await self._store_prediction(key, prediction_id, input_data, prediction, latency_ms)
        
    async def record_ground_truth(self,
                                model_name: str,
                                version: str,
                                tenant_id: str,
                                prediction_id: str,
                                ground_truth: Any):
        """Record ground truth for a prediction"""
        key = f"{tenant_id}:{model_name}:{version}"
        
        self.ground_truth_buffer[key].append({
            "prediction_id": prediction_id,
            "ground_truth": ground_truth,
            "timestamp": datetime.utcnow()
        })
        
    async def _monitor_loop(self, key: str, model_id: str, model_name: str, version: str, tenant_id: str):
        """Main monitoring loop for a model"""
        while True:
            try:
                await asyncio.sleep(self.monitoring_interval)
                
                # Calculate performance metrics
                metrics = await self._calculate_metrics(key)
                
                if metrics:
                    self.performance_history[key].append(metrics)
                    
                    # Update Prometheus metrics
                    MODEL_ACCURACY.labels(
                        model_name=model_name, 
                        version=version, 
                        tenant_id=tenant_id
                    ).set(metrics.accuracy)
                    
                    DRIFT_SCORE.labels(
                        model_name=model_name,
                        version=version,
                        drift_type="feature"
                    ).set(metrics.drift_score)
                    
                    # Check for performance degradation
                    if await self._check_degradation(key, metrics):
                        await self._trigger_alert(model_name, version, tenant_id, metrics)
                        
                    # Store metrics
                    await self._store_metrics(key, metrics)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in monitoring loop for {key}: {e}")
                
    async def _calculate_metrics(self, key: str) -> Optional[PerformanceMetrics]:
        """Calculate performance metrics from buffers"""
        predictions = self.prediction_buffer.get(key, [])
        ground_truths = self.ground_truth_buffer.get(key, [])
        
        if not predictions:
            return None
            
        # Match predictions with ground truth
        matched_data = []
        gt_dict = {gt["prediction_id"]: gt["ground_truth"] for gt in ground_truths}
        
        for pred in predictions:
            if pred["prediction_id"] in gt_dict:
                matched_data.append({
                    "prediction": pred["prediction"],
                    "ground_truth": gt_dict[pred["prediction_id"]],
                    "latency_ms": pred["latency_ms"]
                })
                
        if not matched_data:
            # Only latency metrics available
            latencies = [p["latency_ms"] for p in predictions]
            return PerformanceMetrics(
                accuracy=0.0,
                precision=0.0,
                recall=0.0,
                f1_score=0.0,
                latency_p50=np.percentile(latencies, 50),
                latency_p95=np.percentile(latencies, 95),
                latency_p99=np.percentile(latencies, 99),
                prediction_count=len(predictions),
                error_rate=0.0,
                drift_score=0.0,
                timestamp=datetime.utcnow()
            )
            
        # Calculate accuracy metrics
        correct = sum(1 for m in matched_data if m["prediction"] == m["ground_truth"])
        accuracy = correct / len(matched_data)
        
        # Calculate other metrics (simplified for binary classification)
        # In production, implement proper multi-class metrics
        latencies = [m["latency_ms"] for m in matched_data]
        
        # Calculate drift score (simplified)
        drift_score = await self._calculate_drift(key, predictions)
        
        # Clear old data from buffers
        self._clean_buffers(key)
        
        return PerformanceMetrics(
            accuracy=accuracy,
            precision=accuracy,  # Simplified
            recall=accuracy,     # Simplified
            f1_score=accuracy,   # Simplified
            latency_p50=np.percentile(latencies, 50),
            latency_p95=np.percentile(latencies, 95),
            latency_p99=np.percentile(latencies, 99),
            prediction_count=len(matched_data),
            error_rate=1.0 - accuracy,
            drift_score=drift_score,
            timestamp=datetime.utcnow()
        )
        
    async def _calculate_drift(self, key: str, predictions: List[Dict]) -> float:
        """Calculate feature drift score"""
        # Simplified drift calculation
        # In production, implement proper statistical tests (KS, PSI, etc.)
        if len(self.performance_history[key]) < 2:
            return 0.0
            
        # Compare current distribution with historical
        # This is a placeholder - implement proper drift detection
        return np.random.uniform(0, 0.3)
        
    async def _check_degradation(self, key: str, current_metrics: PerformanceMetrics) -> bool:
        """Check if model performance has degraded"""
        history = self.performance_history[key]
        
        if len(history) < 5:
            return False
            
        # Check if accuracy dropped by more than 5%
        baseline_accuracy = np.mean([m.accuracy for m in history[-10:-5]])
        current_accuracy = current_metrics.accuracy
        
        if baseline_accuracy - current_accuracy > 0.05:
            return True
            
        # Check if drift score is high
        if current_metrics.drift_score > 0.7:
            return True
            
        # Check if error rate increased significantly
        baseline_error = np.mean([m.error_rate for m in history[-10:-5]])
        if current_metrics.error_rate > baseline_error * 1.5:
            return True
            
        return False
        
    async def _trigger_alert(self, model_name: str, version: str, tenant_id: str, metrics: PerformanceMetrics):
        """Trigger alert for performance degradation"""
        alert_data = {
            "model_name": model_name,
            "version": version,
            "tenant_id": tenant_id,
            "alert_type": "performance_degradation",
            "metrics": {
                "accuracy": metrics.accuracy,
                "error_rate": metrics.error_rate,
                "drift_score": metrics.drift_score,
                "latency_p95": metrics.latency_p95
            },
            "timestamp": metrics.timestamp.isoformat()
        }
        
        # Send to monitoring service
        async with httpx.AsyncClient() as client:
            try:
                await client.post(
                    "http://monitoring-service:8000/api/v1/alerts",
                    json=alert_data
                )
            except Exception as e:
                logger.error(f"Failed to send alert: {e}")
                
    async def _store_prediction(self, key: str, prediction_id: str, input_data: Dict, prediction: Any, latency_ms: float):
        """Store prediction in Elasticsearch"""
        doc = {
            "model_key": key,
            "prediction_id": prediction_id,
            "input_data": input_data,
            "prediction": prediction,
            "latency_ms": latency_ms,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        async with httpx.AsyncClient() as client:
            try:
                await client.post(
                    f"{self.elasticsearch_url}/model-predictions/_doc",
                    json=doc
                )
            except Exception as e:
                logger.error(f"Failed to store prediction: {e}")
                
    async def _store_metrics(self, key: str, metrics: PerformanceMetrics):
        """Store metrics in Elasticsearch"""
        doc = {
            "model_key": key,
            "accuracy": metrics.accuracy,
            "precision": metrics.precision,
            "recall": metrics.recall,
            "f1_score": metrics.f1_score,
            "latency_p50": metrics.latency_p50,
            "latency_p95": metrics.latency_p95,
            "latency_p99": metrics.latency_p99,
            "prediction_count": metrics.prediction_count,
            "error_rate": metrics.error_rate,
            "drift_score": metrics.drift_score,
            "timestamp": metrics.timestamp.isoformat()
        }
        
        async with httpx.AsyncClient() as client:
            try:
                await client.post(
                    f"{self.elasticsearch_url}/model-performance/_doc",
                    json=doc
                )
            except Exception as e:
                logger.error(f"Failed to store metrics: {e}")
                
    def _clean_buffers(self, key: str):
        """Clean old data from buffers"""
        cutoff_time = datetime.utcnow() - timedelta(hours=1)
        
        # Clean prediction buffer
        self.prediction_buffer[key] = [
            p for p in self.prediction_buffer[key]
            if p["timestamp"] > cutoff_time
        ]
        
        # Clean ground truth buffer
        self.ground_truth_buffer[key] = [
            gt for gt in self.ground_truth_buffer[key]
            if gt["timestamp"] > cutoff_time
        ]
        
    async def get_performance_report(self, model_name: str, version: str, tenant_id: str) -> Dict[str, Any]:
        """Get performance report for a model"""
        key = f"{tenant_id}:{model_name}:{version}"
        history = self.performance_history.get(key, [])
        
        if not history:
            return {"status": "no_data"}
            
        recent_metrics = history[-1]
        
        # Calculate trends
        if len(history) > 10:
            accuracy_trend = np.polyfit(range(10), [m.accuracy for m in history[-10:]], 1)[0]
            drift_trend = np.polyfit(range(10), [m.drift_score for m in history[-10:]], 1)[0]
        else:
            accuracy_trend = 0
            drift_trend = 0
            
        return {
            "current_metrics": {
                "accuracy": recent_metrics.accuracy,
                "precision": recent_metrics.precision,
                "recall": recent_metrics.recall,
                "f1_score": recent_metrics.f1_score,
                "error_rate": recent_metrics.error_rate,
                "drift_score": recent_metrics.drift_score
            },
            "latency_metrics": {
                "p50": recent_metrics.latency_p50,
                "p95": recent_metrics.latency_p95,
                "p99": recent_metrics.latency_p99
            },
            "trends": {
                "accuracy_trend": accuracy_trend,
                "drift_trend": drift_trend
            },
            "prediction_count": recent_metrics.prediction_count,
            "last_updated": recent_metrics.timestamp.isoformat()
        } 