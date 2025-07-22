"""
MLOps Manager for Unified ML Platform

Provides comprehensive MLOps capabilities including:
- CI/CD for ML models
- Automated retraining pipelines
- A/B testing and canary deployments
- Model versioning and rollback
- Performance monitoring and alerts
- Compliance and governance
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import mlflow
import httpx
import hashlib
from collections import defaultdict

logger = logging.getLogger(__name__)


class DeploymentStrategy(str, Enum):
    """Model deployment strategies"""
    BLUE_GREEN = "blue_green"
    CANARY = "canary"
    SHADOW = "shadow"
    AB_TEST = "ab_test"
    ROLLING = "rolling"


class ModelStage(str, Enum):
    """Model lifecycle stages"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    ARCHIVED = "archived"


class AlertSeverity(str, Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ModelVersion:
    """Model version information"""
    model_name: str
    version: str
    stage: ModelStage
    created_at: datetime
    metrics: Dict[str, float]
    tags: Dict[str, str] = field(default_factory=dict)
    artifacts: Dict[str, str] = field(default_factory=dict)
    deployment_config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeploymentConfig:
    """Model deployment configuration"""
    model_name: str
    version: str
    strategy: DeploymentStrategy
    target_environment: str
    replicas: int = 1
    cpu_request: str = "100m"
    cpu_limit: str = "1000m"
    memory_request: str = "256Mi"
    memory_limit: str = "1Gi"
    gpu_count: int = 0
    auto_scaling: bool = True
    min_replicas: int = 1
    max_replicas: int = 10
    target_cpu_utilization: int = 80
    health_check_path: str = "/health"
    readiness_path: str = "/ready"


@dataclass
class ABTestConfig:
    """A/B test configuration"""
    test_id: str
    model_a: ModelVersion
    model_b: ModelVersion
    traffic_split: float = 0.5  # Percentage to model B
    duration_hours: int = 24
    success_metrics: List[str] = field(default_factory=lambda: ["accuracy", "latency"])
    minimum_samples: int = 1000
    confidence_level: float = 0.95


@dataclass
class MonitoringAlert:
    """Monitoring alert"""
    alert_id: str
    model_name: str
    model_version: str
    severity: AlertSeverity
    metric_name: str
    current_value: float
    threshold: float
    message: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    resolved: bool = False


@dataclass
class RetrainingTrigger:
    """Conditions that trigger model retraining"""
    trigger_type: str  # "drift", "schedule", "performance", "data_volume"
    threshold: float
    check_interval: timedelta
    last_checked: datetime = field(default_factory=datetime.utcnow)
    last_triggered: Optional[datetime] = None


class ModelMonitor:
    """Monitors deployed models"""
    
    def __init__(self):
        self.metrics_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.alerts: List[MonitoringAlert] = []
        self.thresholds: Dict[str, Dict[str, float]] = {}
        
    def record_metrics(self, model_name: str, version: str, metrics: Dict[str, float]):
        """Record model metrics"""
        entry = {
            "timestamp": datetime.utcnow(),
            "version": version,
            "metrics": metrics
        }
        self.metrics_history[model_name].append(entry)
        
        # Check thresholds
        self._check_thresholds(model_name, version, metrics)
        
    def _check_thresholds(self, model_name: str, version: str, metrics: Dict[str, float]):
        """Check if metrics violate thresholds"""
        if model_name not in self.thresholds:
            return
            
        for metric_name, threshold in self.thresholds[model_name].items():
            if metric_name in metrics:
                current_value = metrics[metric_name]
                
                # Check if threshold violated
                if metric_name.endswith("_min") and current_value < threshold:
                    self._create_alert(model_name, version, metric_name, current_value, threshold, "below minimum")
                elif metric_name.endswith("_max") and current_value > threshold:
                    self._create_alert(model_name, version, metric_name, current_value, threshold, "above maximum")
                    
    def _create_alert(self, model_name: str, version: str, metric_name: str, 
                     current_value: float, threshold: float, violation_type: str):
        """Create a monitoring alert"""
        alert = MonitoringAlert(
            alert_id=f"{model_name}_{metric_name}_{datetime.utcnow().timestamp()}",
            model_name=model_name,
            model_version=version,
            severity=AlertSeverity.WARNING,
            metric_name=metric_name,
            current_value=current_value,
            threshold=threshold,
            message=f"Metric {metric_name} is {violation_type} threshold: {current_value} vs {threshold}"
        )
        self.alerts.append(alert)
        logger.warning(f"Alert created: {alert.message}")
        
    def get_recent_alerts(self, hours: int = 24) -> List[MonitoringAlert]:
        """Get recent alerts"""
        cutoff = datetime.utcnow() - timedelta(hours=hours)
        return [a for a in self.alerts if a.timestamp > cutoff and not a.resolved]


class ABTestManager:
    """Manages A/B tests for models"""
    
    def __init__(self):
        self.active_tests: Dict[str, ABTestConfig] = {}
        self.test_results: Dict[str, Dict[str, Any]] = {}
        
    async def start_ab_test(self, config: ABTestConfig) -> str:
        """Start a new A/B test"""
        self.active_tests[config.test_id] = config
        self.test_results[config.test_id] = {
            "start_time": datetime.utcnow(),
            "model_a_metrics": defaultdict(list),
            "model_b_metrics": defaultdict(list),
            "model_a_count": 0,
            "model_b_count": 0
        }
        
        logger.info(f"Started A/B test {config.test_id}")
        return config.test_id
        
    def route_request(self, test_id: str) -> str:
        """Route request to model A or B based on traffic split"""
        if test_id not in self.active_tests:
            raise ValueError(f"No active test with ID {test_id}")
            
        config = self.active_tests[test_id]
        
        # Random routing based on traffic split
        if np.random.random() < config.traffic_split:
            return "model_b"
        else:
            return "model_a"
            
    def record_result(self, test_id: str, model: str, metrics: Dict[str, float]):
        """Record test result"""
        if test_id not in self.test_results:
            return
            
        results = self.test_results[test_id]
        
        if model == "model_a":
            results["model_a_count"] += 1
            for metric, value in metrics.items():
                results["model_a_metrics"][metric].append(value)
        else:
            results["model_b_count"] += 1
            for metric, value in metrics.items():
                results["model_b_metrics"][metric].append(value)
                
    def analyze_test(self, test_id: str) -> Dict[str, Any]:
        """Analyze A/B test results"""
        if test_id not in self.test_results:
            return {}
            
        results = self.test_results[test_id]
        config = self.active_tests.get(test_id)
        
        if not config:
            return {}
            
        analysis = {
            "test_id": test_id,
            "duration_hours": (datetime.utcnow() - results["start_time"]).total_seconds() / 3600,
            "model_a_samples": results["model_a_count"],
            "model_b_samples": results["model_b_count"],
            "metrics_comparison": {}
        }
        
        # Compare metrics
        for metric in config.success_metrics:
            if metric in results["model_a_metrics"] and metric in results["model_b_metrics"]:
                a_values = results["model_a_metrics"][metric]
                b_values = results["model_b_metrics"][metric]
                
                if a_values and b_values:
                    # Calculate statistics
                    a_mean = np.mean(a_values)
                    b_mean = np.mean(b_values)
                    
                    # Simple t-test (in production, use proper statistical tests)
                    improvement = (b_mean - a_mean) / a_mean * 100
                    
                    analysis["metrics_comparison"][metric] = {
                        "model_a_mean": a_mean,
                        "model_b_mean": b_mean,
                        "improvement_percent": improvement,
                        "significant": abs(improvement) > 5  # Simplified
                    }
                    
        # Determine winner
        winning_metrics = sum(1 for m in analysis["metrics_comparison"].values() 
                            if m["improvement_percent"] > 0)
        
        if winning_metrics > len(config.success_metrics) / 2:
            analysis["winner"] = "model_b"
        else:
            analysis["winner"] = "model_a"
            
        return analysis


class MLOpsManager:
    """
    Main MLOps manager for model lifecycle and operations
    """
    
    def __init__(
        self,
        model_registry,
        training_orchestrator,
        serving_engine,
        enable_auto_retraining: bool = True,
        enable_ab_testing: bool = True
    ):
        self.model_registry = model_registry
        self.training_orchestrator = training_orchestrator
        self.serving_engine = serving_engine
        self.enable_auto_retraining = enable_auto_retraining
        self.enable_ab_testing = enable_ab_testing
        
        self.model_versions: Dict[str, List[ModelVersion]] = defaultdict(list)
        self.deployments: Dict[str, DeploymentConfig] = {}
        self.retraining_triggers: Dict[str, List[RetrainingTrigger]] = defaultdict(list)
        
        self.monitor = ModelMonitor()
        self.ab_test_manager = ABTestManager()
        
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
    async def initialize(self):
        """Initialize MLOps manager"""
        # Set up monitoring thresholds
        self._setup_default_thresholds()
        
        logger.info("MLOps Manager initialized")
        
    def _setup_default_thresholds(self):
        """Set up default monitoring thresholds"""
        self.monitor.thresholds["default"] = {
            "accuracy_min": 0.8,
            "latency_max": 100.0,  # ms
            "error_rate_max": 0.05,
            "drift_score_max": 0.1
        }
        
    async def register_model_version(
        self,
        model_name: str,
        model: Any,
        metrics: Dict[str, float],
        tags: Optional[Dict[str, str]] = None,
        stage: ModelStage = ModelStage.DEVELOPMENT
    ) -> ModelVersion:
        """Register a new model version"""
        # Generate version
        version = self._generate_version(model_name)
        
        # Create model version
        model_version = ModelVersion(
            model_name=model_name,
            version=version,
            stage=stage,
            created_at=datetime.utcnow(),
            metrics=metrics,
            tags=tags or {}
        )
        
        # Register with MLflow
        with mlflow.start_run():
            mlflow.log_metrics(metrics)
            
            if tags:
                mlflow.set_tags(tags)
                
            # Log model
            if hasattr(model, "save"):
                mlflow.pytorch.log_model(model, "model")
            else:
                mlflow.sklearn.log_model(model, "model")
                
            model_version.artifacts["mlflow_run_id"] = mlflow.active_run().info.run_id
            
        # Store version
        self.model_versions[model_name].append(model_version)
        
        logger.info(f"Registered model {model_name} version {version}")
        return model_version
        
    def _generate_version(self, model_name: str) -> str:
        """Generate next version number"""
        existing_versions = self.model_versions.get(model_name, [])
        if not existing_versions:
            return "1.0.0"
            
        # Parse latest version and increment
        latest = sorted(existing_versions, key=lambda v: v.created_at)[-1]
        major, minor, patch = map(int, latest.version.split('.'))
        
        # Simple increment logic
        return f"{major}.{minor}.{patch + 1}"
        
    async def promote_model(
        self,
        model_name: str,
        version: str,
        target_stage: ModelStage
    ) -> bool:
        """Promote model to different stage"""
        model_version = self._get_model_version(model_name, version)
        if not model_version:
            return False
            
        # Validate promotion
        if not self._validate_promotion(model_version, target_stage):
            logger.warning(f"Model {model_name}:{version} failed promotion validation")
            return False
            
        # Update stage
        model_version.stage = target_stage
        
        # If promoting to production, set up monitoring
        if target_stage == ModelStage.PRODUCTION:
            await self._setup_production_monitoring(model_name, version)
            
        logger.info(f"Promoted model {model_name}:{version} to {target_stage}")
        return True
        
    def _validate_promotion(self, model_version: ModelVersion, target_stage: ModelStage) -> bool:
        """Validate if model can be promoted"""
        # Check metrics meet minimum requirements
        min_accuracy = 0.8
        if model_version.metrics.get("accuracy", 0) < min_accuracy:
            return False
            
        # Check if model has been tested
        if target_stage == ModelStage.PRODUCTION and model_version.stage != ModelStage.STAGING:
            return False
            
        return True
        
    async def deploy_model(
        self,
        model_name: str,
        version: str,
        config: DeploymentConfig
    ) -> Dict[str, Any]:
        """Deploy model to serving infrastructure"""
        model_version = self._get_model_version(model_name, version)
        if not model_version:
            raise ValueError(f"Model {model_name}:{version} not found")
            
        # Prepare deployment
        deployment_spec = self._create_deployment_spec(model_version, config)
        
        # Deploy based on strategy
        if config.strategy == DeploymentStrategy.BLUE_GREEN:
            result = await self._deploy_blue_green(deployment_spec)
        elif config.strategy == DeploymentStrategy.CANARY:
            result = await self._deploy_canary(deployment_spec)
        elif config.strategy == DeploymentStrategy.AB_TEST:
            result = await self._deploy_ab_test(deployment_spec)
        else:
            result = await self._deploy_rolling(deployment_spec)
            
        # Store deployment config
        self.deployments[f"{model_name}:{version}"] = config
        
        # Start monitoring
        await self._start_deployment_monitoring(model_name, version)
        
        return result
        
    def _create_deployment_spec(
        self,
        model_version: ModelVersion,
        config: DeploymentConfig
    ) -> Dict[str, Any]:
        """Create deployment specification"""
        return {
            "name": f"{model_version.model_name}-{model_version.version}",
            "image": f"ml-models/{model_version.model_name}:{model_version.version}",
            "replicas": config.replicas,
            "resources": {
                "requests": {
                    "cpu": config.cpu_request,
                    "memory": config.memory_request
                },
                "limits": {
                    "cpu": config.cpu_limit,
                    "memory": config.memory_limit
                }
            },
            "env": {
                "MODEL_NAME": model_version.model_name,
                "MODEL_VERSION": model_version.version,
                "MLFLOW_RUN_ID": model_version.artifacts.get("mlflow_run_id", "")
            },
            "autoscaling": {
                "enabled": config.auto_scaling,
                "minReplicas": config.min_replicas,
                "maxReplicas": config.max_replicas,
                "targetCPUUtilizationPercentage": config.target_cpu_utilization
            }
        }
        
    async def _deploy_blue_green(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Blue-green deployment"""
        # Deploy to green environment
        green_result = await self.serving_engine.deploy_model(spec, environment="green")
        
        # Run smoke tests
        if await self._run_smoke_tests(spec["name"], "green"):
            # Switch traffic to green
            await self.serving_engine.switch_traffic(spec["name"], "green")
            
            # Decommission blue
            await self.serving_engine.remove_deployment(spec["name"], "blue")
            
            return {"status": "success", "strategy": "blue_green", "environment": "green"}
        else:
            # Rollback
            await self.serving_engine.remove_deployment(spec["name"], "green")
            return {"status": "failed", "strategy": "blue_green", "reason": "smoke tests failed"}
            
    async def _deploy_canary(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Canary deployment"""
        # Deploy canary with small traffic percentage
        canary_spec = spec.copy()
        canary_spec["name"] = f"{spec['name']}-canary"
        canary_spec["replicas"] = 1
        
        await self.serving_engine.deploy_model(canary_spec, traffic_percentage=10)
        
        # Monitor canary
        canary_healthy = await self._monitor_canary(canary_spec["name"], duration_minutes=30)
        
        if canary_healthy:
            # Gradually increase traffic
            for percentage in [25, 50, 75, 100]:
                await self.serving_engine.update_traffic(canary_spec["name"], percentage)
                await asyncio.sleep(300)  # 5 minutes between increases
                
                if not await self._monitor_canary(canary_spec["name"], duration_minutes=5):
                    # Rollback
                    await self.serving_engine.update_traffic(canary_spec["name"], 0)
                    return {"status": "failed", "strategy": "canary", "rollback_at": percentage}
                    
            return {"status": "success", "strategy": "canary"}
        else:
            await self.serving_engine.remove_deployment(canary_spec["name"])
            return {"status": "failed", "strategy": "canary", "reason": "canary unhealthy"}
            
    async def _deploy_ab_test(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """A/B test deployment"""
        if not self.enable_ab_testing:
            return await self._deploy_rolling(spec)
            
        # Get current production model
        current_model = await self._get_current_production_model(spec["name"])
        
        if current_model:
            # Create A/B test
            test_config = ABTestConfig(
                test_id=f"test_{spec['name']}_{datetime.utcnow().timestamp()}",
                model_a=current_model,
                model_b=ModelVersion(
                    model_name=spec["env"]["MODEL_NAME"],
                    version=spec["env"]["MODEL_VERSION"],
                    stage=ModelStage.STAGING,
                    created_at=datetime.utcnow(),
                    metrics={}
                ),
                traffic_split=0.2  # 20% to new model initially
            )
            
            test_id = await self.ab_test_manager.start_ab_test(test_config)
            
            # Deploy both models
            await self.serving_engine.deploy_model(spec, traffic_percentage=20)
            
            return {"status": "success", "strategy": "ab_test", "test_id": test_id}
        else:
            # No current model, deploy normally
            return await self._deploy_rolling(spec)
            
    async def _deploy_rolling(self, spec: Dict[str, Any]) -> Dict[str, Any]:
        """Rolling deployment"""
        result = await self.serving_engine.deploy_model(spec)
        return {"status": "success", "strategy": "rolling", "result": result}
        
    async def _run_smoke_tests(self, model_name: str, environment: str) -> bool:
        """Run smoke tests on deployed model"""
        try:
            # Send test requests
            test_endpoint = f"http://{model_name}.{environment}/predict"
            test_data = {"input": [[1.0, 2.0, 3.0, 4.0]]}  # Example
            
            response = await self.http_client.post(test_endpoint, json=test_data)
            
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Smoke test failed: {e}")
            return False
            
    async def _monitor_canary(self, model_name: str, duration_minutes: int) -> bool:
        """Monitor canary deployment health"""
        start_time = datetime.utcnow()
        errors = 0
        requests = 0
        
        while (datetime.utcnow() - start_time).total_seconds() < duration_minutes * 60:
            # Get metrics
            metrics = await self.serving_engine.get_metrics(model_name)
            
            requests += metrics.get("request_count", 0)
            errors += metrics.get("error_count", 0)
            
            # Check error rate
            if requests > 100 and errors / requests > 0.05:
                return False
                
            await asyncio.sleep(10)  # Check every 10 seconds
            
        return True
        
    def _get_model_version(self, model_name: str, version: str) -> Optional[ModelVersion]:
        """Get specific model version"""
        versions = self.model_versions.get(model_name, [])
        for v in versions:
            if v.version == version:
                return v
        return None
        
    async def _get_current_production_model(self, model_name: str) -> Optional[ModelVersion]:
        """Get current production model"""
        versions = self.model_versions.get(model_name, [])
        prod_versions = [v for v in versions if v.stage == ModelStage.PRODUCTION]
        
        if prod_versions:
            return sorted(prod_versions, key=lambda v: v.created_at)[-1]
        return None
        
    async def setup_retraining_trigger(
        self,
        model_name: str,
        trigger_type: str,
        threshold: float,
        check_interval: timedelta
    ):
        """Set up automatic retraining trigger"""
        trigger = RetrainingTrigger(
            trigger_type=trigger_type,
            threshold=threshold,
            check_interval=check_interval
        )
        
        self.retraining_triggers[model_name].append(trigger)
        logger.info(f"Set up retraining trigger for {model_name}: {trigger_type}")
        
    async def check_retraining_triggers(self, model_name: str) -> bool:
        """Check if model needs retraining"""
        triggers = self.retraining_triggers.get(model_name, [])
        
        for trigger in triggers:
            if datetime.utcnow() - trigger.last_checked < trigger.check_interval:
                continue
                
            trigger.last_checked = datetime.utcnow()
            
            if trigger.trigger_type == "drift":
                if await self._check_drift_trigger(model_name, trigger.threshold):
                    trigger.last_triggered = datetime.utcnow()
                    return True
                    
            elif trigger.trigger_type == "performance":
                if await self._check_performance_trigger(model_name, trigger.threshold):
                    trigger.last_triggered = datetime.utcnow()
                    return True
                    
            elif trigger.trigger_type == "schedule":
                if trigger.last_triggered is None or \
                   datetime.utcnow() - trigger.last_triggered > timedelta(hours=trigger.threshold):
                    trigger.last_triggered = datetime.utcnow()
                    return True
                    
        return False
        
    async def _check_drift_trigger(self, model_name: str, threshold: float) -> bool:
        """Check for data drift"""
        # Get recent predictions
        metrics = self.monitor.metrics_history.get(model_name, [])
        if not metrics:
            return False
            
        recent_metrics = [m for m in metrics if m["timestamp"] > datetime.utcnow() - timedelta(hours=1)]
        
        if recent_metrics:
            drift_scores = [m["metrics"].get("drift_score", 0) for m in recent_metrics]
            avg_drift = np.mean(drift_scores)
            return avg_drift > threshold
            
        return False
        
    async def _check_performance_trigger(self, model_name: str, threshold: float) -> bool:
        """Check for performance degradation"""
        metrics = self.monitor.metrics_history.get(model_name, [])
        if len(metrics) < 2:
            return False
            
        # Compare recent performance to baseline
        recent = [m for m in metrics if m["timestamp"] > datetime.utcnow() - timedelta(hours=1)]
        baseline = [m for m in metrics if m["timestamp"] < datetime.utcnow() - timedelta(days=7)]
        
        if recent and baseline:
            recent_accuracy = np.mean([m["metrics"].get("accuracy", 0) for m in recent])
            baseline_accuracy = np.mean([m["metrics"].get("accuracy", 0) for m in baseline])
            
            degradation = (baseline_accuracy - recent_accuracy) / baseline_accuracy
            return degradation > threshold
            
        return False
        
    async def trigger_retraining(self, model_name: str) -> Dict[str, Any]:
        """Trigger model retraining"""
        logger.info(f"Triggering retraining for {model_name}")
        
        # Get latest model version
        current_model = await self._get_current_production_model(model_name)
        if not current_model:
            return {"status": "failed", "reason": "No production model found"}
            
        # Create retraining job
        retraining_config = {
            "model_name": model_name,
            "base_version": current_model.version,
            "retraining_reason": "automated_trigger",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Submit to training orchestrator
        job_id = await self.training_orchestrator.submit_training_job(retraining_config)
        
        return {
            "status": "submitted",
            "job_id": job_id,
            "model_name": model_name,
            "base_version": current_model.version
        }
        
    async def _setup_production_monitoring(self, model_name: str, version: str):
        """Set up monitoring for production model"""
        # Set custom thresholds
        self.monitor.thresholds[model_name] = {
            "accuracy_min": 0.85,
            "latency_max": 50.0,
            "error_rate_max": 0.02,
            "drift_score_max": 0.05
        }
        
        # Set up retraining triggers
        await self.setup_retraining_trigger(
            model_name,
            "drift",
            threshold=0.1,
            check_interval=timedelta(hours=1)
        )
        
        await self.setup_retraining_trigger(
            model_name,
            "performance",
            threshold=0.05,  # 5% degradation
            check_interval=timedelta(hours=6)
        )
        
    async def _start_deployment_monitoring(self, model_name: str, version: str):
        """Start monitoring deployed model"""
        # This would integrate with Prometheus/Grafana
        logger.info(f"Started monitoring for {model_name}:{version}")
        
    async def monitor_deployments(self):
        """Background task to monitor all deployments"""
        while True:
            try:
                for deployment_key, config in self.deployments.items():
                    model_name, version = deployment_key.split(":")
                    
                    # Get metrics from serving engine
                    metrics = await self.serving_engine.get_metrics(f"{model_name}-{version}")
                    
                    # Record metrics
                    self.monitor.record_metrics(model_name, version, metrics)
                    
                    # Check retraining triggers
                    if self.enable_auto_retraining:
                        if await self.check_retraining_triggers(model_name):
                            await self.trigger_retraining(model_name)
                            
                # Check A/B tests
                for test_id in list(self.ab_test_manager.active_tests.keys()):
                    config = self.ab_test_manager.active_tests[test_id]
                    
                    # Check if test should end
                    results = self.ab_test_manager.test_results[test_id]
                    duration = (datetime.utcnow() - results["start_time"]).total_seconds() / 3600
                    
                    if duration >= config.duration_hours or \
                       (results["model_a_count"] + results["model_b_count"]) >= config.minimum_samples:
                        # Analyze and end test
                        analysis = self.ab_test_manager.analyze_test(test_id)
                        
                        # Deploy winner
                        if analysis.get("winner") == "model_b":
                            await self.serving_engine.update_traffic(
                                f"{config.model_b.model_name}-{config.model_b.version}",
                                100
                            )
                            
                        # Clean up test
                        del self.ab_test_manager.active_tests[test_id]
                        
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in deployment monitoring: {e}")
                await asyncio.sleep(60)
                
    def get_model_lineage(self, model_name: str) -> List[Dict[str, Any]]:
        """Get model lineage and evolution"""
        versions = self.model_versions.get(model_name, [])
        
        lineage = []
        for version in sorted(versions, key=lambda v: v.created_at):
            lineage.append({
                "version": version.version,
                "stage": version.stage.value,
                "created_at": version.created_at.isoformat(),
                "metrics": version.metrics,
                "tags": version.tags,
                "deployments": [
                    d for k, d in self.deployments.items()
                    if k.startswith(f"{model_name}:{version.version}")
                ]
            })
            
        return lineage
        
    async def rollback_model(self, model_name: str, target_version: str) -> bool:
        """Rollback model to previous version"""
        model_version = self._get_model_version(model_name, target_version)
        if not model_version:
            return False
            
        # Get current deployment config
        current_deployment = None
        for key, config in self.deployments.items():
            if key.startswith(f"{model_name}:"):
                current_deployment = config
                break
                
        if not current_deployment:
            return False
            
        # Deploy target version with same config
        rollback_config = current_deployment
        rollback_config.version = target_version
        
        await self.deploy_model(model_name, target_version, rollback_config)
        
        logger.info(f"Rolled back {model_name} to version {target_version}")
        return True
        
    async def shutdown(self):
        """Shutdown MLOps manager"""
        await self.http_client.aclose()
        logger.info("MLOps Manager shut down") 