"""
ML Optimizer

Machine learning models for workflow optimization
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import optuna
import joblib
import structlog

logger = structlog.get_logger()


@dataclass
class OptimizationResult:
    """Result of optimization"""
    optimizations: Dict[str, Any]
    expected_improvement: float
    confidence: float
    reasoning: List[str]


@dataclass
class ResourcePrediction:
    """Resource usage prediction"""
    cpu: float
    memory: float
    duration: float
    cost: float
    confidence: float


class PerformancePredictor(nn.Module):
    """Neural network for performance prediction"""
    
    def __init__(self, input_dim: int, hidden_dim: int = 128):
        super().__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim // 2)
        self.fc3 = nn.Linear(hidden_dim // 2, 4)  # cpu, memory, duration, cost
        self.dropout = nn.Dropout(0.2)
        self.relu = nn.ReLU()
        
    def forward(self, x):
        x = self.relu(self.fc1(x))
        x = self.dropout(x)
        x = self.relu(self.fc2(x))
        x = self.dropout(x)
        return self.fc3(x)


class MLOptimizer:
    """Machine learning based optimizer"""
    
    def __init__(self, settings):
        self.settings = settings
        
        # Models
        self.performance_model = None
        self.resource_predictor = None
        self.anomaly_detector = None
        
        # Scalers
        self.feature_scaler = StandardScaler()
        self.target_scaler = StandardScaler()
        
        # Optimization history
        self.optimization_history = []
        
    async def load_performance_model(self):
        """Load or initialize performance model"""
        try:
            # Try to load existing model
            self.performance_model = joblib.load("models/performance_model.pkl")
            logger.info("Loaded existing performance model")
        except:
            # Initialize new model
            self.performance_model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=42
            )
            logger.info("Initialized new performance model")
            
        return self.performance_model
        
    async def load_resource_predictor(self):
        """Load or initialize resource predictor"""
        try:
            # Try to load existing model
            self.resource_predictor = torch.load("models/resource_predictor.pt")
            logger.info("Loaded existing resource predictor")
        except:
            # Initialize new model
            self.resource_predictor = PerformancePredictor(input_dim=20)
            logger.info("Initialized new resource predictor")
            
        return self.resource_predictor
        
    async def initialize_anomaly_detector(self):
        """Initialize anomaly detection model"""
        from sklearn.ensemble import IsolationForest
        
        self.anomaly_detector = IsolationForest(
            contamination=0.1,
            random_state=42
        )
        
        return self
        
    async def predict_optimal_config(self,
                                   pipeline_config: Dict[str, Any],
                                   historical_performance: pd.DataFrame,
                                   current_system_state: Dict[str, Any],
                                   business_constraints: Dict[str, Any]) -> OptimizationResult:
        """
        Predict optimal configuration for a pipeline
        
        Args:
            pipeline_config: Current pipeline configuration
            historical_performance: Historical performance data
            current_system_state: Current system metrics
            business_constraints: Business rules and constraints
            
        Returns:
            Optimization result with recommendations
        """
        try:
            # Extract features
            features = self._extract_features(
                pipeline_config,
                current_system_state,
                historical_performance
            )
            
            # Define optimization objective
            def objective(trial):
                # Suggest hyperparameters
                parallelism = trial.suggest_int("parallelism", 1, 32)
                batch_size = trial.suggest_int("batch_size", 100, 10000, step=100)
                cache_ttl = trial.suggest_int("cache_ttl", 60, 3600)
                cpu_request = trial.suggest_float("cpu", 0.5, 16.0)
                memory_request = trial.suggest_float("memory", 1.0, 64.0)
                
                # Create configuration
                config = {
                    "parallelism": parallelism,
                    "batch_size": batch_size,
                    "cache_ttl": cache_ttl,
                    "resources": {
                        "cpu": cpu_request,
                        "memory": memory_request
                    }
                }
                
                # Predict performance
                predicted_perf = self._predict_performance(features, config)
                
                # Calculate objective (weighted sum)
                cost = predicted_perf["cost"] * business_constraints["cost_weight"]
                performance = (1.0 / predicted_perf["duration"]) * business_constraints["performance_weight"]
                reliability = predicted_perf["success_rate"] * business_constraints["reliability_weight"]
                
                return -(cost + performance + reliability)
                
            # Run optimization
            study = optuna.create_study(direction="maximize")
            study.optimize(objective, n_trials=50, timeout=30)
            
            # Get best configuration
            best_params = study.best_params
            
            # Calculate expected improvement
            current_perf = self._estimate_current_performance(pipeline_config, historical_performance)
            optimal_perf = self._predict_performance(features, best_params)
            
            improvement = self._calculate_improvement(current_perf, optimal_perf)
            
            # Generate reasoning
            reasoning = self._generate_reasoning(
                pipeline_config,
                best_params,
                improvement
            )
            
            return OptimizationResult(
                optimizations=best_params,
                expected_improvement=improvement,
                confidence=0.85,  # Based on model confidence
                reasoning=reasoning
            )
            
        except Exception as e:
            logger.error(f"Optimization failed: {e}")
            # Return conservative optimization
            return OptimizationResult(
                optimizations={},
                expected_improvement=0.0,
                confidence=0.0,
                reasoning=["Optimization failed, using default configuration"]
            )
            
    async def predict_resources(self,
                              workflow_config: Dict[str, Any],
                              historical_usage: pd.DataFrame,
                              time_horizon: int) -> ResourcePrediction:
        """Predict resource usage for a workflow"""
        try:
            # Prepare features
            features = self._prepare_resource_features(
                workflow_config,
                historical_usage,
                time_horizon
            )
            
            # Make prediction
            if self.resource_predictor:
                self.resource_predictor.eval()
                with torch.no_grad():
                    features_tensor = torch.FloatTensor(features).unsqueeze(0)
                    predictions = self.resource_predictor(features_tensor)
                    
                cpu, memory, duration, cost = predictions[0].numpy()
                
                return ResourcePrediction(
                    cpu=max(0.1, cpu),
                    memory=max(0.5, memory),
                    duration=max(60, duration),
                    cost=max(0.01, cost),
                    confidence=0.8
                )
            else:
                # Fallback to statistical prediction
                return self._statistical_resource_prediction(historical_usage)
                
        except Exception as e:
            logger.error(f"Resource prediction failed: {e}")
            return ResourcePrediction(
                cpu=2.0,
                memory=4.0,
                duration=300,
                cost=1.0,
                confidence=0.3
            )
            
    async def optimize_workflow(self,
                              workflow_config: Dict[str, Any],
                              system_state: Dict[str, Any],
                              constraints: Dict[str, Any]) -> OptimizationResult:
        """Optimize a workflow configuration"""
        optimizations = {}
        reasoning = []
        
        # Analyze parallelization opportunity
        parallel_opt = self._analyze_parallelization(workflow_config, system_state)
        if parallel_opt["improvement"] > 0.1:
            optimizations["parallelism"] = parallel_opt["value"]
            reasoning.append(f"Increased parallelism to {parallel_opt['value']} for {parallel_opt['improvement']*100:.1f}% improvement")
            
        # Analyze resource allocation
        resource_opt = self._analyze_resources(workflow_config, system_state)
        if resource_opt["improvement"] > 0.05:
            optimizations["resources"] = resource_opt["value"]
            reasoning.append(f"Optimized resources for {resource_opt['improvement']*100:.1f}% cost reduction")
            
        # Analyze caching strategy
        cache_opt = self._analyze_caching(workflow_config)
        if cache_opt["improvement"] > 0.15:
            optimizations["caching"] = cache_opt["value"]
            reasoning.append(f"Enabled caching for {cache_opt['improvement']*100:.1f}% performance gain")
            
        # Calculate total improvement
        total_improvement = sum([
            parallel_opt.get("improvement", 0),
            resource_opt.get("improvement", 0),
            cache_opt.get("improvement", 0)
        ])
        
        return OptimizationResult(
            optimizations=optimizations,
            expected_improvement=total_improvement,
            confidence=0.75,
            reasoning=reasoning
        )
        
    async def update_performance_model(self,
                                     workflow_id: str,
                                     execution_metrics: Dict[str, float],
                                     optimizations: Optional[List[str]]):
        """Update performance model with new execution data"""
        # Store execution data
        self.optimization_history.append({
            "workflow_id": workflow_id,
            "metrics": execution_metrics,
            "optimizations": optimizations,
            "timestamp": datetime.utcnow()
        })
        
        # Retrain if enough new data
        if len(self.optimization_history) % 50 == 0:
            await self.retrain_performance_model(self.optimization_history)
            
    async def retrain_performance_model(self, executions: List[Any]):
        """Retrain performance model with new data"""
        try:
            # Prepare training data
            X, y = self._prepare_training_data(executions)
            
            if len(X) < 10:
                logger.warning("Not enough data for retraining")
                return
                
            # Split data
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, random_state=42
            )
            
            # Fit scalers
            X_train_scaled = self.feature_scaler.fit_transform(X_train)
            y_train_scaled = self.target_scaler.fit_transform(y_train)
            
            # Train model
            self.performance_model.fit(X_train_scaled, y_train_scaled)
            
            # Evaluate
            score = self.performance_model.score(
                self.feature_scaler.transform(X_test),
                self.target_scaler.transform(y_test)
            )
            
            logger.info(f"Retrained performance model, R2 score: {score:.3f}")
            
            # Save model
            joblib.dump(self.performance_model, "models/performance_model.pkl")
            
        except Exception as e:
            logger.error(f"Failed to retrain model: {e}")
            
    async def retrain_resource_predictor(self, executions: List[Any]):
        """Retrain resource prediction model"""
        try:
            # Prepare data
            X, y = self._prepare_resource_training_data(executions)
            
            if len(X) < 50:
                return
                
            # Convert to tensors
            X_tensor = torch.FloatTensor(X)
            y_tensor = torch.FloatTensor(y)
            
            # Create data loader
            dataset = torch.utils.data.TensorDataset(X_tensor, y_tensor)
            loader = torch.utils.data.DataLoader(dataset, batch_size=32, shuffle=True)
            
            # Training setup
            optimizer = torch.optim.Adam(self.resource_predictor.parameters(), lr=0.001)
            criterion = nn.MSELoss()
            
            # Train
            self.resource_predictor.train()
            for epoch in range(50):
                total_loss = 0
                for batch_x, batch_y in loader:
                    optimizer.zero_grad()
                    outputs = self.resource_predictor(batch_x)
                    loss = criterion(outputs, batch_y)
                    loss.backward()
                    optimizer.step()
                    total_loss += loss.item()
                    
                if epoch % 10 == 0:
                    logger.debug(f"Epoch {epoch}, Loss: {total_loss/len(loader):.4f}")
                    
            # Save model
            torch.save(self.resource_predictor, "models/resource_predictor.pt")
            logger.info("Retrained resource predictor")
            
        except Exception as e:
            logger.error(f"Failed to retrain resource predictor: {e}")
            
    async def analyze_patterns(self,
                             executions: List[Any],
                             metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze system-wide patterns"""
        patterns = {}
        
        try:
            # Analyze temporal patterns
            temporal = self._analyze_temporal_patterns(executions)
            patterns["temporal"] = temporal
            
            # Analyze resource patterns
            resource = self._analyze_resource_patterns(executions, metrics)
            patterns["resource"] = resource
            
            # Analyze failure patterns
            failure = self._analyze_failure_patterns(executions)
            patterns["failure"] = failure
            
            # Generate insights
            patterns["insights"] = self._generate_pattern_insights(patterns)
            
        except Exception as e:
            logger.error(f"Pattern analysis failed: {e}")
            
        return patterns
        
    def detect_anomalies(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Detect anomalies in system metrics"""
        anomalies = []
        
        try:
            # Prepare features
            features = self._metrics_to_features(metrics)
            
            if self.anomaly_detector and hasattr(self.anomaly_detector, 'predict'):
                # Predict anomalies
                predictions = self.anomaly_detector.predict([features])
                
                if predictions[0] == -1:  # Anomaly detected
                    anomalies.append({
                        "type": "system_anomaly",
                        "severity": "high",
                        "metrics": metrics,
                        "timestamp": datetime.utcnow()
                    })
                    
        except Exception as e:
            logger.error(f"Anomaly detection failed: {e}")
            
        return anomalies
        
    # Private helper methods
    
    def _extract_features(self,
                         pipeline_config: Dict[str, Any],
                         system_state: Dict[str, Any],
                         historical_perf: pd.DataFrame) -> np.ndarray:
        """Extract features for optimization"""
        features = []
        
        # Pipeline features
        features.extend([
            len(pipeline_config.get("steps", [])),
            pipeline_config.get("parallelism", 1),
            pipeline_config.get("batch_size", 1000),
            1 if pipeline_config.get("cache_enabled", False) else 0
        ])
        
        # System state features
        features.extend([
            system_state.get("cpu_usage", 0),
            system_state.get("memory_usage", 0),
            system_state.get("network_usage", 0),
            system_state.get("active_workflows", 0)
        ])
        
        # Historical features
        if not historical_perf.empty:
            features.extend([
                historical_perf["duration"].mean(),
                historical_perf["duration"].std(),
                historical_perf["cpu_usage"].mean() if "cpu_usage" in historical_perf else 0,
                historical_perf["memory_usage"].mean() if "memory_usage" in historical_perf else 0
            ])
        else:
            features.extend([300, 100, 2.0, 4.0])  # Defaults
            
        return np.array(features)
        
    def _predict_performance(self,
                           features: np.ndarray,
                           config: Dict[str, Any]) -> Dict[str, float]:
        """Predict performance for a configuration"""
        # Modify features based on config
        modified_features = features.copy()
        modified_features[1] = config.get("parallelism", 1)
        modified_features[2] = config.get("batch_size", 1000)
        
        if self.performance_model:
            # Use ML model
            try:
                scaled_features = self.feature_scaler.transform([modified_features])
                prediction = self.performance_model.predict(scaled_features)[0]
                scaled_pred = self.target_scaler.inverse_transform([prediction])[0]
                
                return {
                    "duration": max(60, scaled_pred[0]),
                    "cost": max(0.1, scaled_pred[1]),
                    "success_rate": min(1.0, max(0.5, scaled_pred[2])),
                    "resource_efficiency": min(1.0, max(0.3, scaled_pred[3]))
                }
            except:
                pass
                
        # Fallback to heuristic prediction
        base_duration = 300
        duration = base_duration / np.sqrt(config.get("parallelism", 1))
        cost = config["resources"]["cpu"] * 0.1 + config["resources"]["memory"] * 0.05
        
        return {
            "duration": duration,
            "cost": cost,
            "success_rate": 0.95,
            "resource_efficiency": 0.7
        }
        
    def _analyze_parallelization(self,
                               workflow_config: Dict[str, Any],
                               system_state: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze parallelization opportunities"""
        current_parallel = workflow_config.get("parallelism", 1)
        available_cores = system_state.get("available_cores", 16)
        
        # Calculate optimal parallelism
        step_count = len(workflow_config.get("steps", []))
        data_size = workflow_config.get("estimated_data_size", 1000000)
        
        # Heuristic: balance between overhead and speedup
        optimal = min(
            available_cores,
            max(1, int(np.sqrt(data_size / 100000))),
            step_count * 2
        )
        
        improvement = (optimal - current_parallel) / max(current_parallel, 1) * 0.3
        
        return {
            "value": optimal,
            "improvement": max(0, improvement),
            "reason": "parallelization_analysis"
        }
        
    def _analyze_resources(self,
                         workflow_config: Dict[str, Any],
                         system_state: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze resource optimization opportunities"""
        current_cpu = workflow_config.get("resources", {}).get("cpu", 2.0)
        current_memory = workflow_config.get("resources", {}).get("memory", 4.0)
        
        # Analyze actual usage patterns (simplified)
        usage_ratio = system_state.get("resource_usage_ratio", 0.5)
        
        optimal_cpu = current_cpu * usage_ratio * 1.2  # 20% buffer
        optimal_memory = current_memory * usage_ratio * 1.2
        
        cost_reduction = (1 - usage_ratio) * 0.3
        
        return {
            "value": {
                "cpu": round(optimal_cpu, 1),
                "memory": round(optimal_memory, 1)
            },
            "improvement": cost_reduction,
            "reason": "resource_optimization"
        }
        
    def _analyze_caching(self, workflow_config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze caching opportunities"""
        cache_enabled = workflow_config.get("cache_enabled", False)
        repeated_operations = workflow_config.get("repeated_operations", 0)
        
        if not cache_enabled and repeated_operations > 2:
            return {
                "value": {
                    "enabled": True,
                    "ttl": 3600,
                    "strategy": "LRU"
                },
                "improvement": 0.25,
                "reason": "repeated_operations_detected"
            }
            
        return {"value": {}, "improvement": 0, "reason": "no_caching_benefit"} 