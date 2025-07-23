"""
ML-driven pipeline optimization engine
"""

import asyncio
import json
import numpy as np
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict

from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import joblib

from platformq_shared.logging import get_logger
from pyignite import AsyncClient
from ..core.config import settings

logger = get_logger(__name__)


class OptimizationTarget(str, Enum):
    """Optimization targets"""
    COST = "cost"
    PERFORMANCE = "performance"
    BALANCED = "balanced"
    RELIABILITY = "reliability"
    THROUGHPUT = "throughput"


class MLPipelineOptimizer:
    """ML-driven optimization for pipelines and workflows"""
    
    def __init__(self):
        self.ignite_client: Optional[AsyncClient] = None
        self.models: Dict[str, Any] = {}
        self.scalers: Dict[str, StandardScaler] = {}
        self.optimization_history: List[Dict[str, Any]] = []
        self.feature_importance: Dict[str, Dict[str, float]] = {}
        self.anomaly_detector: Optional[IsolationForest] = None
        
    async def initialize(self):
        """Initialize the ML optimizer"""
        logger.info("Initializing ML pipeline optimizer")
        
        # Initialize Ignite client
        self.ignite_client = AsyncClient()
        await self.ignite_client.connect(settings.ignite_host, settings.ignite_port)
        
        # Load pre-trained models if available
        await self._load_models()
        
        # Initialize anomaly detector
        self.anomaly_detector = IsolationForest(
            contamination=0.1,
            random_state=42
        )
        
        # Start optimization loop
        asyncio.create_task(self._optimization_loop())
        
        logger.info("ML pipeline optimizer initialized")
        
    async def cleanup(self):
        """Cleanup resources"""
        if self.ignite_client:
            await self.ignite_client.close()
            
    async def _load_models(self):
        """Load pre-trained optimization models"""
        try:
            # Resource prediction model
            if settings.ml_model_path and (settings.ml_model_path / "resource_predictor.pkl").exists():
                self.models['resource_predictor'] = joblib.load(
                    settings.ml_model_path / "resource_predictor.pkl"
                )
                self.scalers['resource_predictor'] = joblib.load(
                    settings.ml_model_path / "resource_scaler.pkl"
                )
                logger.info("Loaded pre-trained resource prediction model")
                
            # Performance prediction model
            if settings.ml_model_path and (settings.ml_model_path / "performance_predictor.pkl").exists():
                self.models['performance_predictor'] = joblib.load(
                    settings.ml_model_path / "performance_predictor.pkl"
                )
                self.scalers['performance_predictor'] = joblib.load(
                    settings.ml_model_path / "performance_scaler.pkl"
                )
                logger.info("Loaded pre-trained performance prediction model")
                
        except Exception as e:
            logger.error(f"Failed to load models: {e}")
            
    async def optimize_workflow(self, 
                              workflow: Dict[str, Any],
                              target: OptimizationTarget = OptimizationTarget.BALANCED,
                              constraints: Optional[Dict[str, Any]] = None,
                              historical_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Optimize workflow configuration using ML"""
        logger.info(f"Optimizing workflow {workflow.get('name', 'unknown')} for {target}")
        
        # Extract features from workflow
        features = await self._extract_workflow_features(workflow, historical_data)
        
        # Get optimization recommendations
        recommendations = {
            "workflow_id": workflow.get('id'),
            "workflow_name": workflow.get('name'),
            "target": target,
            "timestamp": datetime.utcnow().isoformat(),
            "original_config": workflow.copy(),
            "optimized_config": {},
            "predicted_improvements": {},
            "confidence": 0.0
        }
        
        # Predict resource requirements
        if 'resource_predictor' in self.models:
            resource_pred = await self._predict_resources(features)
            recommendations['resource_allocation'] = resource_pred
            
        # Predict performance
        if 'performance_predictor' in self.models:
            perf_pred = await self._predict_performance(features)
            recommendations['performance_prediction'] = perf_pred
            
        # Generate optimized configuration
        optimized_config = await self._generate_optimized_config(
            workflow, target, constraints, features
        )
        recommendations['optimized_config'] = optimized_config
        
        # Calculate predicted improvements
        improvements = await self._calculate_improvements(
            workflow, optimized_config, target
        )
        recommendations['predicted_improvements'] = improvements
        
        # Calculate confidence score
        recommendations['confidence'] = await self._calculate_confidence(
            features, historical_data
        )
        
        # Store optimization history
        self.optimization_history.append(recommendations)
        
        # Cache results
        if self.ignite_client:
            cache = await self.ignite_client.get_or_create_cache("optimizations")
            await cache.put(
                f"opt_{workflow.get('id')}_{datetime.utcnow().isoformat()}",
                json.dumps(recommendations)
            )
        
        return recommendations
        
    async def _extract_workflow_features(self, 
                                       workflow: Dict[str, Any],
                                       historical_data: Optional[List[Dict[str, Any]]]) -> np.ndarray:
        """Extract features from workflow for ML models"""
        features = []
        
        # Basic workflow features
        features.extend([
            len(workflow.get('steps', [])),  # Number of steps
            len(workflow.get('dependencies', [])),  # Number of dependencies
            workflow.get('retry_count', 3),  # Retry configuration
            workflow.get('timeout', 3600),  # Timeout setting
            1 if workflow.get('parallel_execution', False) else 0,  # Parallelization
        ])
        
        # Step complexity features
        steps = workflow.get('steps', [])
        if steps:
            step_types = [s.get('type', '') for s in steps]
            features.extend([
                sum(1 for t in step_types if t == 'transform'),  # Transform steps
                sum(1 for t in step_types if t == 'ml_training'),  # ML steps
                sum(1 for t in step_types if t == 'quality_check'),  # Quality steps
                max([s.get('resources', {}).get('cpu', 1) for s in steps]),  # Max CPU
                max([s.get('resources', {}).get('memory', 1) for s in steps]),  # Max memory
            ])
        else:
            features.extend([0, 0, 0, 1, 1])
            
        # Historical performance features
        if historical_data:
            recent_runs = historical_data[-10:]  # Last 10 runs
            if recent_runs:
                features.extend([
                    np.mean([r.get('duration', 0) for r in recent_runs]),  # Avg duration
                    np.std([r.get('duration', 0) for r in recent_runs]),  # Duration variance
                    sum(1 for r in recent_runs if r.get('status') == 'failed') / len(recent_runs),  # Failure rate
                    np.mean([r.get('resource_usage', {}).get('cpu', 0) for r in recent_runs]),  # Avg CPU usage
                    np.mean([r.get('resource_usage', {}).get('memory', 0) for r in recent_runs]),  # Avg memory usage
                ])
            else:
                features.extend([0, 0, 0, 0, 0])
        else:
            features.extend([0, 0, 0, 0, 0])
            
        return np.array(features).reshape(1, -1)
        
    async def _predict_resources(self, features: np.ndarray) -> Dict[str, Any]:
        """Predict resource requirements"""
        try:
            model = self.models.get('resource_predictor')
            scaler = self.scalers.get('resource_predictor')
            
            if model and scaler:
                # Scale features
                scaled_features = scaler.transform(features)
                
                # Predict resources
                predictions = model.predict(scaled_features)
                
                return {
                    "cpu": float(predictions[0][0]),
                    "memory": float(predictions[0][1]),
                    "storage": float(predictions[0][2]) if predictions.shape[1] > 2 else 0,
                    "predicted": True
                }
            else:
                # Default resource allocation
                return {
                    "cpu": 2.0,
                    "memory": 4.0,
                    "storage": 10.0,
                    "predicted": False
                }
                
        except Exception as e:
            logger.error(f"Resource prediction failed: {e}")
            return {"cpu": 2.0, "memory": 4.0, "storage": 10.0, "predicted": False}
            
    async def _predict_performance(self, features: np.ndarray) -> Dict[str, Any]:
        """Predict workflow performance"""
        try:
            model = self.models.get('performance_predictor')
            scaler = self.scalers.get('performance_predictor')
            
            if model and scaler:
                # Scale features
                scaled_features = scaler.transform(features)
                
                # Predict performance metrics
                predictions = model.predict(scaled_features)
                
                return {
                    "estimated_duration": float(predictions[0][0]),
                    "success_probability": min(float(predictions[0][1]), 1.0) if predictions.shape[1] > 1 else 0.95,
                    "throughput": float(predictions[0][2]) if predictions.shape[1] > 2 else 100,
                    "predicted": True
                }
            else:
                # Default predictions
                return {
                    "estimated_duration": 300.0,
                    "success_probability": 0.95,
                    "throughput": 100.0,
                    "predicted": False
                }
                
        except Exception as e:
            logger.error(f"Performance prediction failed: {e}")
            return {
                "estimated_duration": 300.0,
                "success_probability": 0.95,
                "throughput": 100.0,
                "predicted": False
            }
            
    async def _generate_optimized_config(self, 
                                       workflow: Dict[str, Any],
                                       target: OptimizationTarget,
                                       constraints: Optional[Dict[str, Any]],
                                       features: np.ndarray) -> Dict[str, Any]:
        """Generate optimized workflow configuration"""
        optimized = workflow.copy()
        
        # Apply target-specific optimizations
        if target == OptimizationTarget.COST:
            # Optimize for cost
            optimized['resources'] = {
                "cpu": min(workflow.get('resources', {}).get('cpu', 2) * 0.8, 
                          constraints.get('max_cpu', float('inf')) if constraints else float('inf')),
                "memory": min(workflow.get('resources', {}).get('memory', 4) * 0.8,
                            constraints.get('max_memory', float('inf')) if constraints else float('inf'))
            }
            optimized['parallel_execution'] = False  # Sequential execution is cheaper
            optimized['retry_count'] = 2  # Fewer retries
            
        elif target == OptimizationTarget.PERFORMANCE:
            # Optimize for performance
            optimized['resources'] = {
                "cpu": min(workflow.get('resources', {}).get('cpu', 2) * 1.5,
                          constraints.get('max_cpu', float('inf')) if constraints else float('inf')),
                "memory": min(workflow.get('resources', {}).get('memory', 4) * 1.5,
                            constraints.get('max_memory', float('inf')) if constraints else float('inf'))
            }
            optimized['parallel_execution'] = True  # Enable parallelization
            optimized['cache_intermediate'] = True  # Cache intermediate results
            
        elif target == OptimizationTarget.RELIABILITY:
            # Optimize for reliability
            optimized['retry_count'] = 5  # More retries
            optimized['retry_delay'] = 120  # Longer retry delay
            optimized['timeout'] = workflow.get('timeout', 3600) * 1.5  # Increase timeout
            optimized['checkpoint_frequency'] = 'high'  # Frequent checkpoints
            
        elif target == OptimizationTarget.THROUGHPUT:
            # Optimize for throughput
            optimized['batch_size'] = workflow.get('batch_size', 100) * 2
            optimized['parallel_workers'] = min(8, constraints.get('max_workers', 8) if constraints else 8)
            optimized['queue_size'] = 1000
            
        else:  # BALANCED
            # Balanced optimization
            optimized['resources'] = {
                "cpu": workflow.get('resources', {}).get('cpu', 2),
                "memory": workflow.get('resources', {}).get('memory', 4)
            }
            optimized['parallel_execution'] = len(workflow.get('steps', [])) > 5
            optimized['retry_count'] = 3
            optimized['cache_intermediate'] = True
            
        # Apply ML-based adjustments if models are available
        if self.models:
            # Detect if workflow is anomalous
            if self.anomaly_detector:
                is_anomaly = self.anomaly_detector.predict(features)
                if is_anomaly[0] == -1:  # Anomaly detected
                    logger.warning(f"Anomalous workflow detected, applying conservative settings")
                    optimized['monitoring_level'] = 'high'
                    optimized['alert_threshold'] = 'sensitive'
                    
        return optimized
        
    async def _calculate_improvements(self, 
                                    original: Dict[str, Any],
                                    optimized: Dict[str, Any],
                                    target: OptimizationTarget) -> Dict[str, float]:
        """Calculate predicted improvements"""
        improvements = {}
        
        # Resource improvements
        original_cpu = original.get('resources', {}).get('cpu', 2)
        optimized_cpu = optimized.get('resources', {}).get('cpu', 2)
        improvements['cpu_change'] = (optimized_cpu - original_cpu) / original_cpu
        
        original_memory = original.get('resources', {}).get('memory', 4)
        optimized_memory = optimized.get('resources', {}).get('memory', 4)
        improvements['memory_change'] = (optimized_memory - original_memory) / original_memory
        
        # Performance improvements (estimated)
        if target == OptimizationTarget.PERFORMANCE:
            improvements['estimated_speedup'] = 1.2 if optimized.get('parallel_execution') else 1.0
            improvements['estimated_cost_increase'] = 0.3  # 30% cost increase
        elif target == OptimizationTarget.COST:
            improvements['estimated_speedup'] = 0.9  # 10% slower
            improvements['estimated_cost_savings'] = 0.25  # 25% cost savings
        else:
            improvements['estimated_speedup'] = 1.1
            improvements['estimated_cost_change'] = 0.0
            
        return improvements
        
    async def _calculate_confidence(self, 
                                  features: np.ndarray,
                                  historical_data: Optional[List[Dict[str, Any]]]) -> float:
        """Calculate confidence score for optimization"""
        confidence = 0.5  # Base confidence
        
        # Increase confidence if we have ML models
        if self.models:
            confidence += 0.2
            
        # Increase confidence based on historical data
        if historical_data and len(historical_data) > 10:
            confidence += min(0.2, len(historical_data) / 100)
            
        # Decrease confidence for anomalous workflows
        if self.anomaly_detector:
            is_anomaly = self.anomaly_detector.predict(features)
            if is_anomaly[0] == -1:
                confidence *= 0.7
                
        return min(confidence, 0.95)  # Cap at 95%
        
    async def train_models(self, training_data: List[Dict[str, Any]]):
        """Train optimization models on historical data"""
        logger.info(f"Training models on {len(training_data)} samples")
        
        if len(training_data) < settings.ml_min_training_samples:
            logger.warning(f"Insufficient training data: {len(training_data)} < {settings.ml_min_training_samples}")
            return
            
        try:
            # Prepare training data
            X = []
            y_resources = []
            y_performance = []
            
            for sample in training_data:
                # Extract features
                features = await self._extract_workflow_features(
                    sample['workflow'],
                    sample.get('historical_data', [])
                )
                X.append(features.flatten())
                
                # Extract targets
                y_resources.append([
                    sample['actual_resources']['cpu'],
                    sample['actual_resources']['memory'],
                    sample['actual_resources'].get('storage', 0)
                ])
                
                y_performance.append([
                    sample['actual_duration'],
                    1.0 if sample['status'] == 'success' else 0.0,
                    sample.get('throughput', 100)
                ])
                
            X = np.array(X)
            y_resources = np.array(y_resources)
            y_performance = np.array(y_performance)
            
            # Train resource prediction model
            X_train, X_test, y_train_res, y_test_res = train_test_split(
                X, y_resources, test_size=0.2, random_state=42
            )
            
            resource_scaler = StandardScaler()
            X_train_scaled = resource_scaler.fit_transform(X_train)
            X_test_scaled = resource_scaler.transform(X_test)
            
            resource_model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42
            )
            resource_model.fit(X_train_scaled, y_train_res)
            
            # Evaluate resource model
            resource_score = resource_model.score(X_test_scaled, y_test_res)
            logger.info(f"Resource prediction model R² score: {resource_score:.3f}")
            
            # Store resource model
            self.models['resource_predictor'] = resource_model
            self.scalers['resource_predictor'] = resource_scaler
            
            # Train performance prediction model
            performance_scaler = StandardScaler()
            X_train_scaled_perf = performance_scaler.fit_transform(X_train)
            
            performance_model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42
            )
            performance_model.fit(X_train_scaled_perf, y_train_res)
            
            # Store performance model
            self.models['performance_predictor'] = performance_model
            self.scalers['performance_predictor'] = performance_scaler
            
            # Train anomaly detector
            self.anomaly_detector.fit(X)
            
            # Save models if path is configured
            if settings.ml_model_path:
                joblib.dump(resource_model, settings.ml_model_path / "resource_predictor.pkl")
                joblib.dump(resource_scaler, settings.ml_model_path / "resource_scaler.pkl")
                joblib.dump(performance_model, settings.ml_model_path / "performance_predictor.pkl")
                joblib.dump(performance_scaler, settings.ml_model_path / "performance_scaler.pkl")
                logger.info("Models saved to disk")
                
        except Exception as e:
            logger.error(f"Model training failed: {e}")
            
    async def get_recommendations(self, workflow_id: str) -> List[Dict[str, Any]]:
        """Get optimization recommendations for a workflow"""
        recommendations = [
            opt for opt in self.optimization_history
            if opt.get('workflow_id') == workflow_id
        ]
        
        # Sort by timestamp (most recent first)
        recommendations.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        
        return recommendations[:10]  # Return last 10 recommendations
        
    async def predict_resources(self, 
                              workflow: Dict[str, Any],
                              context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Predict resource requirements for a workflow"""
        features = await self._extract_workflow_features(workflow, None)
        return await self._predict_resources(features)
        
    async def detect_anomalies(self, workflows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Detect anomalous workflows"""
        if not self.anomaly_detector:
            return []
            
        anomalies = []
        
        for workflow in workflows:
            features = await self._extract_workflow_features(workflow, None)
            is_anomaly = self.anomaly_detector.predict(features)
            
            if is_anomaly[0] == -1:
                anomalies.append({
                    "workflow_id": workflow.get('id'),
                    "workflow_name": workflow.get('name'),
                    "anomaly_score": self.anomaly_detector.score_samples(features)[0],
                    "detected_at": datetime.utcnow().isoformat()
                })
                
        return anomalies
        
    async def _optimization_loop(self):
        """Background optimization loop"""
        while True:
            try:
                # Collect recent execution data
                if self.ignite_client:
                    cache = await self.ignite_client.get_or_create_cache("executions")
                    recent_executions = []
                    
                    # Get recent executions (simplified - real implementation would query properly)
                    # This is a placeholder for the actual data collection logic
                    
                    # Retrain models if enough new data
                    if len(recent_executions) >= settings.ml_retrain_threshold:
                        await self.train_models(recent_executions)
                        
            except Exception as e:
                logger.error(f"Optimization loop error: {e}")
                
            await asyncio.sleep(settings.optimization_interval) 