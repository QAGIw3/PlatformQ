"""
ML-Driven Anomaly Detection for Simulations

Real-time anomaly detection using machine learning models
integrated with MLOps service for continuous improvement.
"""

import asyncio
import json
import logging
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import httpx
from pydantic import BaseModel, Field
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import joblib
import pandas as pd

from pyignite import Client as IgniteClient
from platformq_shared.event_publisher import EventPublisher
import mlflow
import mlflow.sklearn

logger = logging.getLogger(__name__)


class AnomalyDetectionConfig(BaseModel):
    """Configuration for anomaly detection"""
    detection_method: str = Field(default="isolation_forest", description="isolation_forest, autoencoder, lstm")
    sensitivity: float = Field(default=0.1, description="Anomaly threshold (0-1)")
    window_size: int = Field(default=100, description="Number of data points for detection window")
    feature_columns: List[str] = Field(default=[], description="Features to monitor")
    retrain_interval: int = Field(default=3600, description="Seconds between model retraining")
    min_samples: int = Field(default=1000, description="Minimum samples for training")


class SimulationAnomalyDetector:
    """ML-based anomaly detector for simulation metrics"""
    
    def __init__(self,
                 ignite_client: IgniteClient,
                 event_publisher: EventPublisher,
                 mlops_service_url: str,
                 seatunnel_service_url: str):
        self.ignite = ignite_client
        self.event_publisher = event_publisher
        self.mlops_url = mlops_service_url
        self.seatunnel_url = seatunnel_service_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Caches
        self.metrics_cache = self.ignite.get_or_create_cache("simulation_metrics_realtime")
        self.anomaly_cache = self.ignite.get_or_create_cache("simulation_anomalies")
        self.model_cache = self.ignite.get_or_create_cache("anomaly_detection_models")
        
        # ML components
        self.models: Dict[str, Any] = {}
        self.scalers: Dict[str, StandardScaler] = {}
        self.training_data: Dict[str, List[Dict[str, Any]]] = {}
        
        # MLflow setup
        mlflow.set_tracking_uri(f"{mlops_service_url}/mlflow")
        
    async def detect_anomalies(self,
                             simulation_id: str,
                             metrics: Dict[str, Any],
                             config: AnomalyDetectionConfig) -> Dict[str, Any]:
        """Detect anomalies in simulation metrics"""
        try:
            # Prepare features
            features = self._extract_features(metrics, config.feature_columns)
            
            if not features:
                return {"anomaly_detected": False, "reason": "No features to analyze"}
                
            # Get or create model for this simulation type
            model_key = f"{simulation_id}_model"
            
            if model_key not in self.models:
                await self._load_or_create_model(simulation_id, config)
                
            if model_key not in self.models:
                # Not enough data yet
                self._collect_training_data(simulation_id, features)
                return {"anomaly_detected": False, "reason": "Collecting training data"}
                
            # Scale features
            scaler = self.scalers.get(model_key)
            if scaler:
                features_scaled = scaler.transform([features])
            else:
                features_scaled = [features]
                
            # Detect anomalies
            model = self.models[model_key]
            prediction = model.predict(features_scaled)
            anomaly_score = model.score_samples(features_scaled)[0]
            
            is_anomaly = prediction[0] == -1
            
            if is_anomaly:
                anomaly_details = await self._analyze_anomaly(
                    simulation_id,
                    metrics,
                    features,
                    anomaly_score,
                    config
                )
                
                # Store anomaly
                await self._store_anomaly(simulation_id, anomaly_details)
                
                # Trigger alerts and actions
                await self._handle_anomaly(simulation_id, anomaly_details)
                
                return {
                    "anomaly_detected": True,
                    "anomaly_score": float(anomaly_score),
                    "details": anomaly_details
                }
            else:
                # Update training data for continuous learning
                self._collect_training_data(simulation_id, features)
                
                return {
                    "anomaly_detected": False,
                    "anomaly_score": float(anomaly_score)
                }
                
        except Exception as e:
            logger.error(f"Error detecting anomalies: {e}")
            return {"anomaly_detected": False, "error": str(e)}
            
    async def _load_or_create_model(self,
                                  simulation_id: str,
                                  config: AnomalyDetectionConfig):
        """Load existing model or create new one"""
        model_key = f"{simulation_id}_model"
        
        try:
            # Try to load from MLflow
            model_name = f"anomaly_detector_{simulation_id}"
            
            try:
                # Get latest model version
                client = mlflow.MlflowClient()
                model_version = client.get_latest_versions(
                    model_name,
                    stages=["Production"]
                )[0]
                
                # Load model
                model_uri = f"models:/{model_name}/{model_version.version}"
                model = mlflow.sklearn.load_model(model_uri)
                
                self.models[model_key] = model
                
                # Load scaler if exists
                scaler_uri = f"{model_uri}/scaler"
                try:
                    scaler = joblib.load(mlflow.artifacts.download_artifacts(scaler_uri))
                    self.scalers[model_key] = scaler
                except:
                    pass
                    
                logger.info(f"Loaded anomaly detection model for {simulation_id}")
                
            except Exception as e:
                # No existing model, check if we have enough training data
                training_key = f"{simulation_id}_training"
                
                if training_key in self.training_data:
                    data = self.training_data[training_key]
                    
                    if len(data) >= config.min_samples:
                        # Train new model
                        await self._train_model(simulation_id, data, config)
                        
        except Exception as e:
            logger.error(f"Error loading/creating model: {e}")
            
    async def _train_model(self,
                         simulation_id: str,
                         training_data: List[Dict[str, Any]],
                         config: AnomalyDetectionConfig):
        """Train anomaly detection model"""
        try:
            model_key = f"{simulation_id}_model"
            
            # Convert to numpy array
            X = np.array(training_data)
            
            # Create and fit scaler
            scaler = StandardScaler()
            X_scaled = scaler.fit_transform(X)
            
            # Train model based on method
            if config.detection_method == "isolation_forest":
                model = IsolationForest(
                    contamination=config.sensitivity,
                    random_state=42,
                    n_estimators=100
                )
            else:
                # Default to Isolation Forest
                model = IsolationForest(
                    contamination=config.sensitivity,
                    random_state=42
                )
                
            model.fit(X_scaled)
            
            # Store in memory
            self.models[model_key] = model
            self.scalers[model_key] = scaler
            
            # Log to MLflow
            with mlflow.start_run():
                mlflow.log_param("simulation_id", simulation_id)
                mlflow.log_param("detection_method", config.detection_method)
                mlflow.log_param("sensitivity", config.sensitivity)
                mlflow.log_param("training_samples", len(X))
                
                # Log model
                mlflow.sklearn.log_model(
                    model,
                    "model",
                    registered_model_name=f"anomaly_detector_{simulation_id}"
                )
                
                # Save scaler as artifact
                joblib.dump(scaler, "scaler.pkl")
                mlflow.log_artifact("scaler.pkl", "model")
                
                # Calculate and log metrics
                train_scores = model.score_samples(X_scaled)
                mlflow.log_metric("mean_anomaly_score", np.mean(train_scores))
                mlflow.log_metric("std_anomaly_score", np.std(train_scores))
                
            logger.info(f"Trained anomaly detection model for {simulation_id}")
            
            # Clear training data to save memory
            training_key = f"{simulation_id}_training"
            if training_key in self.training_data:
                del self.training_data[training_key]
                
        except Exception as e:
            logger.error(f"Error training model: {e}")
            
    def _extract_features(self,
                        metrics: Dict[str, Any],
                        feature_columns: List[str]) -> List[float]:
        """Extract features from metrics"""
        if not feature_columns:
            # Auto-detect numeric features
            feature_columns = [
                k for k, v in metrics.items()
                if isinstance(v, (int, float)) and not k.startswith("_")
            ]
            
        features = []
        for col in feature_columns:
            value = metrics.get(col, 0)
            if isinstance(value, (int, float)):
                features.append(float(value))
            else:
                features.append(0.0)
                
        return features
        
    def _collect_training_data(self,
                             simulation_id: str,
                             features: List[float]):
        """Collect training data for model updates"""
        training_key = f"{simulation_id}_training"
        
        if training_key not in self.training_data:
            self.training_data[training_key] = []
            
        self.training_data[training_key].append(features)
        
        # Limit memory usage
        if len(self.training_data[training_key]) > 10000:
            self.training_data[training_key] = self.training_data[training_key][-5000:]
            
    async def _analyze_anomaly(self,
                             simulation_id: str,
                             metrics: Dict[str, Any],
                             features: List[float],
                             anomaly_score: float,
                             config: AnomalyDetectionConfig) -> Dict[str, Any]:
        """Analyze detected anomaly for root cause"""
        try:
            # Get historical data for comparison
            historical_data = await self._get_historical_metrics(simulation_id)
            
            # Feature importance analysis
            feature_importance = self._calculate_feature_importance(
                features,
                historical_data,
                config.feature_columns
            )
            
            # Anomaly classification
            anomaly_type = self._classify_anomaly(metrics, historical_data)
            
            # Impact assessment
            impact = self._assess_impact(metrics, anomaly_type)
            
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "anomaly_score": float(anomaly_score),
                "anomaly_type": anomaly_type,
                "feature_importance": feature_importance,
                "impact": impact,
                "metrics": metrics,
                "recommended_actions": self._get_recommendations(anomaly_type, impact)
            }
            
        except Exception as e:
            logger.error(f"Error analyzing anomaly: {e}")
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "anomaly_score": float(anomaly_score),
                "error": str(e)
            }
            
    async def _get_historical_metrics(self,
                                    simulation_id: str,
                                    hours: int = 24) -> List[Dict[str, Any]]:
        """Get historical metrics from data lake via SeaTunnel"""
        try:
            # Create SeaTunnel job to fetch historical data
            job_request = {
                "job_name": f"anomaly_historical_{simulation_id}",
                "job_type": "batch",
                "source": {
                    "type": "minio",
                    "bucket": "simulation-metrics",
                    "path": f"silver/simulations/{simulation_id}/*",
                    "format": "parquet"
                },
                "sink": {
                    "type": "ignite",
                    "cache": f"historical_metrics_{simulation_id}"
                },
                "transformations": [
                    {
                        "type": "filter",
                        "condition": f"timestamp > now() - interval '{hours} hours'"
                    }
                ],
                "execution_mode": "sync"
            }
            
            response = await self.http_client.post(
                f"{self.seatunnel_url}/api/v1/jobs",
                json=job_request
            )
            
            if response.status_code == 201:
                job_result = response.json()
                
                # Wait for job completion (simplified)
                await asyncio.sleep(5)
                
                # Read from Ignite cache
                cache = self.ignite.get_or_create_cache(f"historical_metrics_{simulation_id}")
                
                data = []
                query = cache.scan()
                for _, value in query:
                    data.append(value)
                    
                return data
            else:
                logger.error(f"Failed to create SeaTunnel job: {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting historical metrics: {e}")
            return []
            
    def _calculate_feature_importance(self,
                                    current_features: List[float],
                                    historical_data: List[Dict[str, Any]],
                                    feature_names: List[str]) -> Dict[str, float]:
        """Calculate which features contributed most to anomaly"""
        if not historical_data or not feature_names:
            return {}
            
        try:
            # Calculate z-scores for each feature
            historical_features = []
            for data in historical_data:
                features = self._extract_features(data, feature_names)
                if features:
                    historical_features.append(features)
                    
            if not historical_features:
                return {}
                
            historical_array = np.array(historical_features)
            means = np.mean(historical_array, axis=0)
            stds = np.std(historical_array, axis=0)
            
            importance = {}
            for i, (feature_name, value, mean, std) in enumerate(
                zip(feature_names, current_features, means, stds)
            ):
                if std > 0:
                    z_score = abs(value - mean) / std
                    importance[feature_name] = float(z_score)
                else:
                    importance[feature_name] = 0.0
                    
            # Normalize
            total = sum(importance.values())
            if total > 0:
                importance = {k: v/total for k, v in importance.items()}
                
            return importance
            
        except Exception as e:
            logger.error(f"Error calculating feature importance: {e}")
            return {}
            
    def _classify_anomaly(self,
                        metrics: Dict[str, Any],
                        historical_data: List[Dict[str, Any]]) -> str:
        """Classify the type of anomaly"""
        # Simple classification based on metrics
        convergence_rate = metrics.get("convergence_rate", 1.0)
        error_rate = metrics.get("error_rate", 0.0)
        resource_usage = metrics.get("resource_usage", 0.0)
        
        if convergence_rate < 0.1:
            return "convergence_failure"
        elif error_rate > 0.5:
            return "high_error_rate"
        elif resource_usage > 0.95:
            return "resource_exhaustion"
        elif metrics.get("agent_count", 0) == 0:
            return "agent_failure"
        else:
            return "performance_degradation"
            
    def _assess_impact(self,
                     metrics: Dict[str, Any],
                     anomaly_type: str) -> Dict[str, Any]:
        """Assess the impact of the anomaly"""
        severity_scores = {
            "convergence_failure": 0.9,
            "high_error_rate": 0.8,
            "resource_exhaustion": 0.7,
            "agent_failure": 0.9,
            "performance_degradation": 0.5
        }
        
        severity = severity_scores.get(anomaly_type, 0.5)
        
        # Adjust based on metrics
        if metrics.get("simulation_time", 0) > 3600:  # Long-running simulation
            severity *= 1.2
            
        return {
            "severity": min(severity, 1.0),
            "affected_components": self._identify_affected_components(anomaly_type),
            "estimated_recovery_time": self._estimate_recovery_time(anomaly_type),
            "business_impact": "high" if severity > 0.7 else "medium" if severity > 0.4 else "low"
        }
        
    def _identify_affected_components(self, anomaly_type: str) -> List[str]:
        """Identify components affected by anomaly"""
        component_map = {
            "convergence_failure": ["optimizer", "parameters"],
            "high_error_rate": ["agents", "interactions"],
            "resource_exhaustion": ["compute", "memory"],
            "agent_failure": ["agents", "initialization"],
            "performance_degradation": ["general"]
        }
        return component_map.get(anomaly_type, ["unknown"])
        
    def _estimate_recovery_time(self, anomaly_type: str) -> int:
        """Estimate recovery time in seconds"""
        recovery_times = {
            "convergence_failure": 1800,  # 30 minutes
            "high_error_rate": 600,       # 10 minutes
            "resource_exhaustion": 300,    # 5 minutes
            "agent_failure": 900,          # 15 minutes
            "performance_degradation": 1200 # 20 minutes
        }
        return recovery_times.get(anomaly_type, 600)
        
    def _get_recommendations(self,
                           anomaly_type: str,
                           impact: Dict[str, Any]) -> List[str]:
        """Get recommended actions for anomaly"""
        recommendations = {
            "convergence_failure": [
                "Trigger ML-based parameter optimization",
                "Reduce simulation time step",
                "Check boundary conditions"
            ],
            "high_error_rate": [
                "Review agent interaction rules",
                "Enable debug logging",
                "Reduce agent count temporarily"
            ],
            "resource_exhaustion": [
                "Scale up compute resources",
                "Enable agent culling",
                "Optimize memory usage"
            ],
            "agent_failure": [
                "Restart failed agents",
                "Check initialization parameters",
                "Review agent definitions"
            ],
            "performance_degradation": [
                "Profile simulation performance",
                "Enable caching",
                "Review recent changes"
            ]
        }
        
        base_recommendations = recommendations.get(anomaly_type, ["Investigate anomaly"])
        
        # Add severity-based recommendations
        if impact["severity"] > 0.8:
            base_recommendations.insert(0, "URGENT: Consider pausing simulation")
            
        return base_recommendations
        
    async def _store_anomaly(self,
                           simulation_id: str,
                           anomaly_details: Dict[str, Any]):
        """Store anomaly in cache and trigger persistence"""
        try:
            anomaly_id = f"anomaly-{simulation_id}-{datetime.utcnow().timestamp()}"
            
            anomaly_record = {
                "anomaly_id": anomaly_id,
                "simulation_id": simulation_id,
                **anomaly_details
            }
            
            # Store in Ignite
            self.anomaly_cache.put(anomaly_id, anomaly_record)
            
            # Publish event for persistence
            await self.event_publisher.publish_event(
                f"platformq/analytics/anomaly-events",
                "SimulationAnomalyDetected",
                anomaly_record
            )
            
        except Exception as e:
            logger.error(f"Error storing anomaly: {e}")
            
    async def _handle_anomaly(self,
                            simulation_id: str,
                            anomaly_details: Dict[str, Any]):
        """Handle detected anomaly with appropriate actions"""
        try:
            severity = anomaly_details.get("impact", {}).get("severity", 0)
            anomaly_type = anomaly_details.get("anomaly_type")
            
            # High severity - immediate action
            if severity > 0.8:
                # Notify simulation service
                await self._notify_simulation_service(simulation_id, anomaly_details)
                
                # Trigger automatic remediation
                if anomaly_type == "convergence_failure":
                    await self._trigger_ml_optimization(simulation_id)
                elif anomaly_type == "resource_exhaustion":
                    await self._request_resource_scaling(simulation_id)
                    
            # Medium severity - alert and monitor
            elif severity > 0.5:
                # Send notification
                await self._send_anomaly_notification(simulation_id, anomaly_details)
                
            # Always trigger model retraining if needed
            await self._check_model_performance(simulation_id)
            
        except Exception as e:
            logger.error(f"Error handling anomaly: {e}")
            
    async def _notify_simulation_service(self,
                                       simulation_id: str,
                                       anomaly_details: Dict[str, Any]):
        """Notify simulation service of critical anomaly"""
        try:
            notification = {
                "simulation_id": simulation_id,
                "anomaly_type": anomaly_details.get("anomaly_type"),
                "severity": anomaly_details.get("impact", {}).get("severity"),
                "recommended_actions": anomaly_details.get("recommended_actions", [])
            }
            
            response = await self.http_client.post(
                f"http://simulation-service:8000/api/v1/simulations/{simulation_id}/anomaly",
                json=notification
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to notify simulation service: {response.text}")
                
        except Exception as e:
            logger.error(f"Error notifying simulation service: {e}")
            
    async def _trigger_ml_optimization(self, simulation_id: str):
        """Trigger ML-based parameter optimization"""
        try:
            # Publish event to trigger federated learning
            await self.event_publisher.publish_event(
                f"platformq/ml/optimization-requests",
                "SimulationOptimizationRequest",
                {
                    "simulation_id": simulation_id,
                    "reason": "anomaly_detected",
                    "urgency": "high",
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
        except Exception as e:
            logger.error(f"Error triggering ML optimization: {e}")
            
    async def _request_resource_scaling(self, simulation_id: str):
        """Request resource scaling for simulation"""
        try:
            # Call provisioning service
            scaling_request = {
                "resource_type": "simulation_compute",
                "resource_id": simulation_id,
                "action": "scale_up",
                "factor": 1.5,
                "reason": "anomaly_resource_exhaustion"
            }
            
            response = await self.http_client.post(
                "http://provisioning-service:8000/api/v1/resources/scale",
                json=scaling_request
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to request scaling: {response.text}")
                
        except Exception as e:
            logger.error(f"Error requesting resource scaling: {e}")
            
    async def _send_anomaly_notification(self,
                                       simulation_id: str,
                                       anomaly_details: Dict[str, Any]):
        """Send anomaly notification"""
        try:
            notification = {
                "type": "simulation_anomaly",
                "simulation_id": simulation_id,
                "severity": anomaly_details.get("impact", {}).get("severity"),
                "anomaly_type": anomaly_details.get("anomaly_type"),
                "message": f"Anomaly detected in simulation {simulation_id}: {anomaly_details.get('anomaly_type')}",
                "details": anomaly_details
            }
            
            response = await self.http_client.post(
                "http://notification-service:8000/api/v1/notifications",
                json=notification
            )
            
            if response.status_code != 201:
                logger.error(f"Failed to send notification: {response.text}")
                
        except Exception as e:
            logger.error(f"Error sending notification: {e}")
            
    async def _check_model_performance(self, simulation_id: str):
        """Check if model needs retraining"""
        model_key = f"{simulation_id}_model"
        
        # Track false positive rate
        # If too many false positives, trigger retraining
        # This is simplified - real implementation would track metrics
        
    async def continuous_monitoring(self):
        """Continuous monitoring loop"""
        while True:
            try:
                # Check all active simulations
                sim_cache = self.ignite.get_or_create_cache("active_simulations")
                
                query = sim_cache.scan()
                for sim_id, sim_data in query:
                    # Get latest metrics
                    metrics = self.metrics_cache.get(f"{sim_id}_latest")
                    
                    if metrics:
                        # Run anomaly detection
                        config = AnomalyDetectionConfig(
                            feature_columns=sim_data.get("monitored_features", []),
                            sensitivity=sim_data.get("anomaly_sensitivity", 0.1)
                        )
                        
                        result = await self.detect_anomalies(sim_id, metrics, config)
                        
                        if result.get("anomaly_detected"):
                            logger.warning(f"Anomaly detected in simulation {sim_id}")
                            
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error in continuous monitoring: {e}")
                await asyncio.sleep(30)
                
    async def close(self):
        """Clean up resources"""
        await self.http_client.aclose() 