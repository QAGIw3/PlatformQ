"""
ML Optimizer

Uses machine learning to optimize workflow and pipeline execution.
"""

import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
import numpy as np

from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import joblib

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration
from pyignite import AsyncClient

logger = StructuredLogger.get_logger(__name__)


class OptimizationTarget(Enum):
    """Optimization targets"""
    COST = "cost"
    PERFORMANCE = "performance"
    BALANCED = "balanced"
    RELIABILITY = "reliability"
    THROUGHPUT = "throughput"


class MLOptimizer:
    """
    ML-driven optimization for workflows and pipelines
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        
        # ML models
        self.resource_model: Optional[RandomForestRegressor] = None
        self.performance_model: Optional[RandomForestRegressor] = None
        self.anomaly_detector: Optional[IsolationForest] = None
        self.cost_model: Optional[RandomForestRegressor] = None
        
        # Feature scalers
        self.resource_scaler = StandardScaler()
        self.performance_scaler = StandardScaler()
        
        # Ignite client for caching
        self.ignite_client: Optional[AsyncClient] = None
        
        # Historical data
        self.execution_history: List[Dict[str, Any]] = []
        
        # Configuration
        self.config = {
            "optimization_interval": 300,  # 5 minutes
            "learning_rate": 0.001,
            "model_update_threshold": 0.05,
            "lookback_days": 30,
            "min_samples_for_training": 100,
            "anomaly_contamination": 0.1
        }
        
        # Metrics
        self.metrics = {
            "predictions_made": 0,
            "model_updates": 0,
            "optimization_improvements": 0,
            "anomalies_detected": 0
        }
    
    async def initialize(self):
        """Initialize ML optimizer"""
        logger.info("initializing_ml_optimizer")
        
        # Load configuration
        await self._load_configuration()
        
        # Initialize Ignite client
        self.ignite_client = AsyncClient()
        await self.ignite_client.connect("ignite", 10800)
        
        # Load historical data
        await self._load_historical_data()
        
        # Train initial models
        await self._train_models()
        
        # Start optimization loop
        asyncio.create_task(self._optimization_loop())
        
        logger.info("ml_optimizer_initialized")
    
    async def cleanup(self):
        """Cleanup resources"""
        # Save models
        await self._save_models()
        
        if self.ignite_client:
            await self.ignite_client.close()
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/ml-optimizer")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def optimize_workflow(self, workflow_config: Dict[str, Any], 
                              target: OptimizationTarget = OptimizationTarget.BALANCED) -> Dict[str, Any]:
        """
        Optimize workflow configuration using ML
        
        Args:
            workflow_config: Current workflow configuration
            target: Optimization target
            
        Returns:
            Optimized configuration with recommendations
        """
        # Extract features from workflow
        features = self._extract_workflow_features(workflow_config)
        
        # Predict resource requirements
        resource_prediction = await self._predict_resources(features)
        
        # Predict performance metrics
        performance_prediction = await self._predict_performance(features)
        
        # Generate optimization recommendations
        recommendations = self._generate_recommendations(
            workflow_config,
            resource_prediction,
            performance_prediction,
            target
        )
        
        # Update metrics
        self.metrics["predictions_made"] += 1
        
        # Emit event
        await self.event_bus.publish(
            "orchestration.optimization.completed",
            {
                "workflow_name": workflow_config.get("name"),
                "target": target.value,
                "recommendations": len(recommendations["changes"]),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return recommendations
    
    async def predict_resource_needs(self, pipeline_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Predict resource requirements for pipeline
        
        Args:
            pipeline_config: Pipeline configuration
            
        Returns:
            Resource predictions
        """
        # Extract features
        features = self._extract_pipeline_features(pipeline_config)
        
        # Make predictions
        if self.resource_model:
            try:
                features_scaled = self.resource_scaler.transform([features])
                predictions = self.resource_model.predict(features_scaled)[0]
                
                return {
                    "cpu": max(1, int(predictions[0])),
                    "memory_gb": max(1, int(predictions[1])),
                    "storage_gb": max(10, int(predictions[2])),
                    "execution_time_minutes": max(1, int(predictions[3])),
                    "confidence": 0.85  # Placeholder confidence score
                }
            except Exception as e:
                logger.error(f"Error predicting resources: {e}")
        
        # Default predictions
        return {
            "cpu": 2,
            "memory_gb": 4,
            "storage_gb": 20,
            "execution_time_minutes": 30,
            "confidence": 0.5
        }
    
    async def detect_anomalies(self, execution_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """
        Detect anomalies in execution metrics
        
        Args:
            execution_metrics: Execution metrics
            
        Returns:
            Anomaly detection results
        """
        if not self.anomaly_detector:
            return {"is_anomaly": False, "score": 0.0}
        
        try:
            # Extract features from metrics
            features = [
                execution_metrics.get("execution_time", 0),
                execution_metrics.get("cpu_usage", 0),
                execution_metrics.get("memory_usage", 0),
                execution_metrics.get("error_rate", 0),
                execution_metrics.get("records_processed", 0)
            ]
            
            # Predict anomaly
            prediction = self.anomaly_detector.predict([features])[0]
            anomaly_score = self.anomaly_detector.score_samples([features])[0]
            
            is_anomaly = prediction == -1
            
            if is_anomaly:
                self.metrics["anomalies_detected"] += 1
                
                # Emit event
                await self.event_bus.publish(
                    "orchestration.anomaly.detected",
                    {
                        "metrics": execution_metrics,
                        "score": float(anomaly_score),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
            
            return {
                "is_anomaly": is_anomaly,
                "score": float(anomaly_score),
                "threshold": -0.5  # Anomaly threshold
            }
            
        except Exception as e:
            logger.error(f"Error detecting anomaly: {e}")
            return {"is_anomaly": False, "score": 0.0}
    
    async def learn_from_execution(self, execution_data: Dict[str, Any]):
        """
        Learn from execution results
        
        Args:
            execution_data: Execution results and metrics
        """
        # Add to history
        self.execution_history.append({
            **execution_data,
            "timestamp": datetime.utcnow()
        })
        
        # Limit history size
        max_history = 10000
        if len(self.execution_history) > max_history:
            self.execution_history = self.execution_history[-max_history:]
        
        # Cache in Ignite
        if self.ignite_client:
            cache = await self.ignite_client.get_or_create_cache("ml_execution_history")
            await cache.put(execution_data["id"], execution_data)
        
        # Check if models need retraining
        if len(self.execution_history) >= self.config["min_samples_for_training"]:
            if len(self.execution_history) % 100 == 0:  # Retrain every 100 executions
                asyncio.create_task(self._train_models())
    
    async def _train_models(self):
        """Train ML models"""
        if len(self.execution_history) < self.config["min_samples_for_training"]:
            logger.info("Not enough data for training models")
            return
        
        logger.info("Training ML models")
        
        try:
            # Prepare training data
            X_resource, y_resource = self._prepare_resource_training_data()
            X_performance, y_performance = self._prepare_performance_training_data()
            X_anomaly = self._prepare_anomaly_training_data()
            
            # Train resource prediction model
            if len(X_resource) > 10:
                X_train, X_test, y_train, y_test = train_test_split(
                    X_resource, y_resource, test_size=0.2, random_state=42
                )
                
                self.resource_model = RandomForestRegressor(
                    n_estimators=100,
                    max_depth=10,
                    random_state=42
                )
                
                X_train_scaled = self.resource_scaler.fit_transform(X_train)
                self.resource_model.fit(X_train_scaled, y_train)
                
                # Evaluate model
                X_test_scaled = self.resource_scaler.transform(X_test)
                score = self.resource_model.score(X_test_scaled, y_test)
                logger.info(f"Resource model R² score: {score:.3f}")
            
            # Train performance prediction model
            if len(X_performance) > 10:
                X_train, X_test, y_train, y_test = train_test_split(
                    X_performance, y_performance, test_size=0.2, random_state=42
                )
                
                self.performance_model = RandomForestRegressor(
                    n_estimators=100,
                    max_depth=10,
                    random_state=42
                )
                
                X_train_scaled = self.performance_scaler.fit_transform(X_train)
                self.performance_model.fit(X_train_scaled, y_train)
                
                # Evaluate model
                X_test_scaled = self.performance_scaler.transform(X_test)
                score = self.performance_model.score(X_test_scaled, y_test)
                logger.info(f"Performance model R² score: {score:.3f}")
            
            # Train anomaly detector
            if len(X_anomaly) > 10:
                self.anomaly_detector = IsolationForest(
                    contamination=self.config["anomaly_contamination"],
                    random_state=42
                )
                self.anomaly_detector.fit(X_anomaly)
                logger.info("Anomaly detector trained")
            
            # Update metrics
            self.metrics["model_updates"] += 1
            
            # Save models
            await self._save_models()
            
        except Exception as e:
            logger.error(f"Error training models: {e}")
    
    def _extract_workflow_features(self, workflow_config: Dict[str, Any]) -> List[float]:
        """Extract features from workflow configuration"""
        steps = workflow_config.get("steps", [])
        
        features = [
            len(steps),  # Number of steps
            sum(1 for s in steps if s.get("type") == "transform"),  # Transform steps
            sum(1 for s in steps if s.get("type") == "aggregate"),  # Aggregate steps
            len(workflow_config.get("dependencies", {})),  # Dependencies
            1 if workflow_config.get("schedule") else 0,  # Scheduled
            workflow_config.get("retry_policy", {}).get("max_retries", 3),  # Max retries
            1 if workflow_config.get("type") == "streaming" else 0,  # Streaming
            workflow_config.get("timeout", 3600) / 3600  # Timeout in hours
        ]
        
        return features
    
    def _extract_pipeline_features(self, pipeline_config: Dict[str, Any]) -> List[float]:
        """Extract features from pipeline configuration"""
        steps = pipeline_config.get("steps", [])
        
        features = [
            len(steps),  # Number of steps
            sum(1 for s in steps if s.get("type") == "extract"),  # Extract steps
            sum(1 for s in steps if s.get("type") == "transform"),  # Transform steps
            sum(1 for s in steps if s.get("type") == "load"),  # Load steps
            pipeline_config.get("batch_size", 1000) / 1000,  # Batch size (thousands)
            1 if pipeline_config.get("type") == "streaming" else 0,  # Streaming
            len(pipeline_config.get("data_sources", [])),  # Number of sources
            pipeline_config.get("expected_records", 10000) / 10000  # Expected records (ten thousands)
        ]
        
        return features
    
    async def _predict_resources(self, features: List[float]) -> Dict[str, Any]:
        """Predict resource requirements"""
        if self.resource_model:
            try:
                features_scaled = self.resource_scaler.transform([features])
                predictions = self.resource_model.predict(features_scaled)[0]
                
                return {
                    "cpu": max(1, int(predictions[0])),
                    "memory_gb": max(1, int(predictions[1])),
                    "storage_gb": max(10, int(predictions[2]))
                }
            except Exception as e:
                logger.error(f"Error predicting resources: {e}")
        
        # Default resources
        return {"cpu": 2, "memory_gb": 4, "storage_gb": 20}
    
    async def _predict_performance(self, features: List[float]) -> Dict[str, Any]:
        """Predict performance metrics"""
        if self.performance_model:
            try:
                features_scaled = self.performance_scaler.transform([features])
                predictions = self.performance_model.predict(features_scaled)[0]
                
                return {
                    "execution_time": max(1, int(predictions[0])),
                    "throughput": max(100, int(predictions[1])),
                    "success_rate": min(1.0, max(0.0, predictions[2]))
                }
            except Exception as e:
                logger.error(f"Error predicting performance: {e}")
        
        # Default performance
        return {"execution_time": 30, "throughput": 1000, "success_rate": 0.95}
    
    def _generate_recommendations(self, config: Dict[str, Any], 
                                resources: Dict[str, Any],
                                performance: Dict[str, Any],
                                target: OptimizationTarget) -> Dict[str, Any]:
        """Generate optimization recommendations"""
        recommendations = {
            "target": target.value,
            "predicted_resources": resources,
            "predicted_performance": performance,
            "changes": [],
            "impact": {}
        }
        
        # Resource recommendations based on target
        if target == OptimizationTarget.COST:
            # Optimize for cost - use minimum resources
            if resources["cpu"] > 2:
                recommendations["changes"].append({
                    "type": "resource",
                    "field": "cpu",
                    "current": config.get("resources", {}).get("cpu", 4),
                    "recommended": 2,
                    "reason": "Reduce CPU to minimize cost"
                })
        
        elif target == OptimizationTarget.PERFORMANCE:
            # Optimize for performance - use more resources
            recommendations["changes"].append({
                "type": "resource",
                "field": "cpu",
                "current": config.get("resources", {}).get("cpu", 2),
                "recommended": resources["cpu"],
                "reason": "Increase CPU for better performance"
            })
            
            recommendations["changes"].append({
                "type": "resource",
                "field": "memory_gb",
                "current": config.get("resources", {}).get("memory_gb", 4),
                "recommended": resources["memory_gb"],
                "reason": "Increase memory for better performance"
            })
        
        elif target == OptimizationTarget.BALANCED:
            # Balance cost and performance
            cpu_recommendation = min(resources["cpu"], 4)
            memory_recommendation = min(resources["memory_gb"], 8)
            
            recommendations["changes"].append({
                "type": "resource",
                "field": "cpu",
                "current": config.get("resources", {}).get("cpu", 2),
                "recommended": cpu_recommendation,
                "reason": "Balanced CPU allocation"
            })
        
        # Parallelization recommendations
        if len(config.get("steps", [])) > 5 and performance["execution_time"] > 60:
            recommendations["changes"].append({
                "type": "parallelization",
                "field": "parallel_steps",
                "current": 1,
                "recommended": min(4, len(config.get("steps", [])) // 2),
                "reason": "Parallelize independent steps"
            })
        
        # Batch size recommendations
        current_batch = config.get("batch_size", 1000)
        if performance["throughput"] < 5000 and current_batch < 5000:
            recommendations["changes"].append({
                "type": "configuration",
                "field": "batch_size",
                "current": current_batch,
                "recommended": min(10000, current_batch * 2),
                "reason": "Increase batch size for better throughput"
            })
        
        # Calculate impact
        recommendations["impact"] = {
            "estimated_cost_change": self._calculate_cost_impact(recommendations["changes"]),
            "estimated_performance_change": self._calculate_performance_impact(recommendations["changes"]),
            "confidence": 0.75
        }
        
        return recommendations
    
    def _calculate_cost_impact(self, changes: List[Dict[str, Any]]) -> str:
        """Calculate cost impact of changes"""
        cpu_change = 0
        memory_change = 0
        
        for change in changes:
            if change["type"] == "resource":
                if change["field"] == "cpu":
                    cpu_change = change["recommended"] - change["current"]
                elif change["field"] == "memory_gb":
                    memory_change = change["recommended"] - change["current"]
        
        # Simple cost calculation (example)
        cost_change = cpu_change * 0.05 + memory_change * 0.01  # $/hour
        
        if cost_change > 0:
            return f"+${cost_change:.2f}/hour"
        elif cost_change < 0:
            return f"-${abs(cost_change):.2f}/hour"
        else:
            return "No change"
    
    def _calculate_performance_impact(self, changes: List[Dict[str, Any]]) -> str:
        """Calculate performance impact of changes"""
        performance_boost = 0
        
        for change in changes:
            if change["type"] == "resource":
                if change["field"] == "cpu":
                    performance_boost += (change["recommended"] - change["current"]) * 10
                elif change["field"] == "memory_gb":
                    performance_boost += (change["recommended"] - change["current"]) * 5
            elif change["type"] == "parallelization":
                performance_boost += change["recommended"] * 20
            elif change["type"] == "configuration" and change["field"] == "batch_size":
                performance_boost += 15
        
        if performance_boost > 0:
            return f"+{performance_boost}% faster"
        elif performance_boost < 0:
            return f"{abs(performance_boost)}% slower"
        else:
            return "No change"
    
    def _prepare_resource_training_data(self) -> Tuple[List[List[float]], List[List[float]]]:
        """Prepare training data for resource prediction"""
        X = []
        y = []
        
        for execution in self.execution_history:
            if "config" in execution and "metrics" in execution:
                features = self._extract_workflow_features(execution["config"])
                
                resources = [
                    execution["metrics"].get("cpu_used", 2),
                    execution["metrics"].get("memory_gb_used", 4),
                    execution["metrics"].get("storage_gb_used", 20)
                ]
                
                X.append(features)
                y.append(resources)
        
        return X, y
    
    def _prepare_performance_training_data(self) -> Tuple[List[List[float]], List[List[float]]]:
        """Prepare training data for performance prediction"""
        X = []
        y = []
        
        for execution in self.execution_history:
            if "config" in execution and "metrics" in execution:
                features = self._extract_workflow_features(execution["config"])
                
                performance = [
                    execution["metrics"].get("execution_time", 30),
                    execution["metrics"].get("throughput", 1000),
                    execution["metrics"].get("success_rate", 0.95)
                ]
                
                X.append(features)
                y.append(performance)
        
        return X, y
    
    def _prepare_anomaly_training_data(self) -> List[List[float]]:
        """Prepare training data for anomaly detection"""
        X = []
        
        for execution in self.execution_history:
            if "metrics" in execution:
                features = [
                    execution["metrics"].get("execution_time", 0),
                    execution["metrics"].get("cpu_usage", 0),
                    execution["metrics"].get("memory_usage", 0),
                    execution["metrics"].get("error_rate", 0),
                    execution["metrics"].get("records_processed", 0)
                ]
                
                X.append(features)
        
        return X
    
    async def _save_models(self):
        """Save trained models"""
        try:
            if self.resource_model:
                joblib.dump(self.resource_model, "/tmp/resource_model.pkl")
                joblib.dump(self.resource_scaler, "/tmp/resource_scaler.pkl")
            
            if self.performance_model:
                joblib.dump(self.performance_model, "/tmp/performance_model.pkl")
                joblib.dump(self.performance_scaler, "/tmp/performance_scaler.pkl")
            
            if self.anomaly_detector:
                joblib.dump(self.anomaly_detector, "/tmp/anomaly_detector.pkl")
            
            logger.info("Models saved successfully")
            
        except Exception as e:
            logger.error(f"Error saving models: {e}")
    
    async def _load_historical_data(self):
        """Load historical execution data"""
        if self.ignite_client:
            try:
                cache = await self.ignite_client.get_or_create_cache("ml_execution_history")
                # Load recent execution history
                # This is a simplified version - in production, you'd query with proper filters
                logger.info("Historical data loaded")
            except Exception as e:
                logger.error(f"Error loading historical data: {e}")
    
    async def _optimization_loop(self):
        """Continuous optimization loop"""
        while True:
            try:
                await asyncio.sleep(self.config["optimization_interval"])
                
                # Retrain models periodically
                if len(self.execution_history) >= self.config["min_samples_for_training"]:
                    await self._train_models()
                
                # Analyze recent executions for optimization opportunities
                recent_executions = self.execution_history[-100:]
                
                if recent_executions:
                    # Calculate optimization metrics
                    avg_cpu_usage = np.mean([e["metrics"].get("cpu_usage", 0) for e in recent_executions])
                    avg_memory_usage = np.mean([e["metrics"].get("memory_usage", 0) for e in recent_executions])
                    
                    # Check if optimization is needed
                    if avg_cpu_usage < 30:  # Under-utilized
                        self.metrics["optimization_improvements"] += 1
                        logger.info("CPU under-utilized, optimization opportunity detected")
                    
                    if avg_memory_usage < 40:  # Under-utilized
                        self.metrics["optimization_improvements"] += 1
                        logger.info("Memory under-utilized, optimization opportunity detected")
                
            except Exception as e:
                logger.error(f"Error in optimization loop: {e}")
    
    async def get_optimization_metrics(self) -> Dict[str, Any]:
        """Get optimization metrics"""
        return {
            **self.metrics,
            "models_trained": {
                "resource_model": self.resource_model is not None,
                "performance_model": self.performance_model is not None,
                "anomaly_detector": self.anomaly_detector is not None
            },
            "training_samples": len(self.execution_history),
            "last_model_update": datetime.utcnow().isoformat()  # Placeholder
        } 