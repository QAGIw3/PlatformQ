"""
Feedback Loop for Automated Model Retraining

This module implements the feedback loop that:
1. Listens to model monitoring alerts
2. Evaluates retraining criteria
3. Triggers automated retraining workflows
4. Manages A/B testing for new models
"""

import logging
import json
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum
import pulsar
from pulsar.schema import AvroSchema
import httpx

from platformq.shared.event_publisher import EventPublisher
from .retraining_strategies import (
    RetrainingStrategy, 
    DriftBasedStrategy,
    PerformanceBasedStrategy,
    ScheduledStrategy,
    CombinedStrategy
)

logger = logging.getLogger(__name__)


class AlertType(Enum):
    """Types of monitoring alerts"""
    DATA_DRIFT = "DATA_DRIFT"
    PERFORMANCE_DEGRADATION = "PERFORMANCE_DEGRADATION"
    PREDICTION_ANOMALY = "PREDICTION_ANOMALY"
    CONCEPT_DRIFT = "CONCEPT_DRIFT"


class RetrainingTrigger:
    """Represents a retraining trigger event"""
    def __init__(self, 
                 model_name: str,
                 model_version: str,
                 tenant_id: str,
                 trigger_reason: str,
                 alert_data: Dict[str, Any],
                 priority: str = "MEDIUM"):
        self.model_name = model_name
        self.model_version = model_version
        self.tenant_id = tenant_id
        self.trigger_reason = trigger_reason
        self.alert_data = alert_data
        self.priority = priority
        self.timestamp = datetime.utcnow()
        

class FeedbackLoopManager:
    """Manages the ML feedback loop"""
    
    def __init__(self, 
                 pulsar_url: str,
                 mlflow_url: str,
                 workflow_service_url: str,
                 event_publisher: EventPublisher):
        self.pulsar_url = pulsar_url
        self.mlflow_url = mlflow_url
        self.workflow_service_url = workflow_service_url
        self.event_publisher = event_publisher
        
        # Initialize strategies
        self.strategies = {
            "drift": DriftBasedStrategy(),
            "performance": PerformanceBasedStrategy(),
            "scheduled": ScheduledStrategy(),
            "combined": CombinedStrategy()
        }
        
        # Track recent alerts to prevent alert storms
        self.recent_alerts: Dict[str, List[Dict[str, Any]]] = {}
        self.retraining_history: Dict[str, datetime] = {}
        
        # Configuration
        self.min_retraining_interval = timedelta(hours=24)
        self.alert_aggregation_window = timedelta(minutes=30)
        
    async def start(self):
        """Start the feedback loop"""
        logger.info("Starting ML feedback loop manager")
        
        # Create Pulsar client
        self.pulsar_client = pulsar.Client(self.pulsar_url)
        
        # Subscribe to monitoring alerts
        self.alert_consumer = self.pulsar_client.subscribe(
            topic='model-monitoring-alerts',
            subscription_name='feedback-loop-subscription',
            schema=AvroSchema(Dict[str, Any])
        )
        
        # Start processing alerts
        await self.process_alerts()
        
    async def process_alerts(self):
        """Process monitoring alerts and trigger retraining"""
        while True:
            try:
                # Receive alert
                msg = self.alert_consumer.receive(timeout_millis=1000)
                alert = msg.value()
                
                # Process alert
                await self.handle_alert(alert)
                
                # Acknowledge message
                self.alert_consumer.acknowledge(msg)
                
            except Exception as e:
                if "timeout" not in str(e).lower():
                    logger.error(f"Error processing alert: {e}")
                await asyncio.sleep(0.1)
                
    async def handle_alert(self, alert: Dict[str, Any]):
        """Handle a monitoring alert"""
        alert_type = AlertType(alert.get("alert_type"))
        model_key = f"{alert['model_name']}_{alert['model_version']}"
        
        # Add to recent alerts
        if model_key not in self.recent_alerts:
            self.recent_alerts[model_key] = []
            
        self.recent_alerts[model_key].append({
            "alert": alert,
            "timestamp": datetime.utcnow()
        })
        
        # Clean old alerts
        self._clean_old_alerts(model_key)
        
        # Check if we should trigger retraining
        if await self.should_trigger_retraining(model_key, alert):
            await self.trigger_retraining(alert)
            
    def _clean_old_alerts(self, model_key: str):
        """Remove alerts older than aggregation window"""
        cutoff_time = datetime.utcnow() - self.alert_aggregation_window
        self.recent_alerts[model_key] = [
            a for a in self.recent_alerts[model_key]
            if a["timestamp"] > cutoff_time
        ]
        
    async def should_trigger_retraining(self, model_key: str, alert: Dict[str, Any]) -> bool:
        """Determine if retraining should be triggered"""
        # Check minimum interval
        if model_key in self.retraining_history:
            time_since_last = datetime.utcnow() - self.retraining_history[model_key]
            if time_since_last < self.min_retraining_interval:
                logger.info(f"Skipping retraining for {model_key} - too soon since last retraining")
                return False
                
        # Get appropriate strategy
        strategy = self._select_strategy(alert)
        
        # Aggregate recent alerts
        recent_alerts = [a["alert"] for a in self.recent_alerts[model_key]]
        
        # Check strategy
        return strategy.should_retrain(recent_alerts, alert)
        
    def _select_strategy(self, alert: Dict[str, Any]) -> RetrainingStrategy:
        """Select appropriate retraining strategy"""
        alert_type = AlertType(alert.get("alert_type"))
        
        if alert_type == AlertType.DATA_DRIFT:
            return self.strategies["drift"]
        elif alert_type == AlertType.PERFORMANCE_DEGRADATION:
            return self.strategies["performance"]
        else:
            return self.strategies["combined"]
            
    async def trigger_retraining(self, alert: Dict[str, Any]):
        """Trigger model retraining workflow"""
        model_name = alert["model_name"]
        model_version = alert["model_version"]
        tenant_id = alert["tenant_id"]
        
        trigger = RetrainingTrigger(
            model_name=model_name,
            model_version=model_version,
            tenant_id=tenant_id,
            trigger_reason=alert["alert_type"],
            alert_data=alert,
            priority=alert.get("severity", "MEDIUM")
        )
        
        logger.info(f"Triggering retraining for {model_name} v{model_version}")
        
        # Create retraining workflow
        workflow_config = self._create_workflow_config(trigger)
        
        # Submit workflow
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.workflow_service_url}/api/v1/workflows",
                json=workflow_config,
                headers={"X-Tenant-ID": tenant_id}
            )
            
            if response.status_code == 201:
                workflow_id = response.json()["workflow_id"]
                logger.info(f"Created retraining workflow {workflow_id}")
                
                # Publish event
                await self._publish_retraining_event(trigger, workflow_id)
                
                # Update history
                model_key = f"{model_name}_{model_version}"
                self.retraining_history[model_key] = datetime.utcnow()
            else:
                logger.error(f"Failed to create workflow: {response.text}")
                
    def _create_workflow_config(self, trigger: RetrainingTrigger) -> Dict[str, Any]:
        """Create workflow configuration for retraining"""
        return {
            "workflow_type": "MODEL_RETRAINING",
            "name": f"Retrain {trigger.model_name} - {trigger.trigger_reason}",
            "priority": trigger.priority,
            "config": {
                "model_name": trigger.model_name,
                "current_version": trigger.model_version,
                "trigger_reason": trigger.trigger_reason,
                "alert_data": trigger.alert_data,
                "training_config": {
                    "use_latest_data": True,
                    "incremental_training": trigger.alert_data.get("severity") != "HIGH",
                    "validation_split": 0.2,
                    "hyperparameter_tuning": True
                },
                "evaluation_config": {
                    "compare_with_current": True,
                    "min_improvement": 0.02,  # 2% improvement required
                    "test_duration_hours": 24  # A/B test duration
                }
            },
            "steps": [
                {
                    "name": "prepare_data",
                    "type": "SPARK_JOB",
                    "config": {
                        "job_class": "com.platformq.ml.DataPreparation",
                        "args": ["--model", trigger.model_name, "--latest"]
                    }
                },
                {
                    "name": "train_model",
                    "type": "SPARK_JOB", 
                    "config": {
                        "job_class": "com.platformq.ml.ModelTraining",
                        "args": ["--model", trigger.model_name, "--auto-tune"]
                    }
                },
                {
                    "name": "evaluate_model",
                    "type": "FUNCTION",
                    "config": {
                        "function": "evaluate_retrained_model",
                        "args": {
                            "new_model_uri": "${train_model.output.model_uri}",
                            "current_model": f"{trigger.model_name}:{trigger.model_version}"
                        }
                    }
                },
                {
                    "name": "deploy_canary",
                    "type": "FUNCTION",
                    "config": {
                        "function": "deploy_model_canary",
                        "args": {
                            "model_uri": "${train_model.output.model_uri}",
                            "traffic_percentage": 10
                        }
                    },
                    "condition": "${evaluate_model.output.improvement} > 0.02"
                }
            ]
        }
        
    async def _publish_retraining_event(self, trigger: RetrainingTrigger, workflow_id: str):
        """Publish retraining triggered event"""
        event = {
            "event_type": "MODEL_RETRAINING_TRIGGERED",
            "model_name": trigger.model_name,
            "model_version": trigger.model_version,
            "tenant_id": trigger.tenant_id,
            "workflow_id": workflow_id,
            "trigger_reason": trigger.trigger_reason,
            "timestamp": trigger.timestamp.isoformat()
        }
        
        self.event_publisher.publish(
            topic=f"persistent://platformq/{trigger.tenant_id}/model-retraining-events",
            schema_path="model_retraining_triggered.avsc",
            data=event
        )


class AutoMLOptimizer:
    """Automated hyperparameter optimization for retraining"""
    
    def __init__(self, mlflow_client):
        self.mlflow_client = mlflow_client
        
    async def optimize_hyperparameters(self, 
                                     model_name: str,
                                     training_data_uri: str,
                                     base_params: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize hyperparameters using Bayesian optimization"""
        # This would integrate with tools like Optuna or Ray Tune
        # For now, return enhanced parameters
        optimized_params = base_params.copy()
        
        # Example optimization logic
        if "learning_rate" in base_params:
            # Search around current value
            optimized_params["learning_rate_search"] = [
                base_params["learning_rate"] * 0.5,
                base_params["learning_rate"],
                base_params["learning_rate"] * 2.0
            ]
            
        return optimized_params


class ABTestManager:
    """Manages A/B testing for new model versions"""
    
    def __init__(self, monitoring_service_url: str):
        self.monitoring_service_url = monitoring_service_url
        
    async def start_ab_test(self,
                          model_name: str,
                          version_a: str,
                          version_b: str,
                          traffic_split: float = 0.1,
                          duration_hours: int = 24) -> str:
        """Start A/B test between two model versions"""
        test_config = {
            "model_name": model_name,
            "version_a": version_a,
            "version_b": version_b,
            "traffic_split": {
                "a": 1.0 - traffic_split,
                "b": traffic_split
            },
            "duration_hours": duration_hours,
            "metrics_to_track": [
                "prediction_accuracy",
                "prediction_latency",
                "prediction_confidence",
                "business_metrics"
            ],
            "success_criteria": {
                "min_improvement": 0.02,
                "max_latency_increase": 0.1,
                "min_confidence": 0.8
            }
        }
        
        # This would call the monitoring service to set up A/B test
        # Return test ID
        return f"ab_test_{model_name}_{version_b}"
        
    async def evaluate_ab_test(self, test_id: str) -> Dict[str, Any]:
        """Evaluate A/B test results"""
        # This would fetch metrics from monitoring service
        # For now, return mock results
        return {
            "test_id": test_id,
            "status": "completed",
            "winner": "version_b",
            "improvement": 0.03,
            "confidence_level": 0.95
        } 