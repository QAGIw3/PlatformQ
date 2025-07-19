"""
Event handlers for Unified ML Platform Service
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
import json

from platformq_event_framework import BaseEventProcessor, EventMetrics

logger = logging.getLogger(__name__)


class UnifiedMLEventHandler(BaseEventProcessor):
    """
    Event handler for ML platform events.
    
    Handles training requests, inference requests, federated learning events,
    retraining requests, and anomaly detection events.
    """
    
    def __init__(self,
                 service_name: str,
                 pulsar_url: str,
                 metrics: EventMetrics,
                 model_registry,
                 training_orchestrator,
                 serving_engine,
                 federated_coordinator,
                 mlops_manager):
        super().__init__(service_name, pulsar_url, metrics)
        self.model_registry = model_registry
        self.training_orchestrator = training_orchestrator
        self.serving_engine = serving_engine
        self.federated_coordinator = federated_coordinator
        self.mlops_manager = mlops_manager
        
    async def handle_training_request(self, event: Dict[str, Any]) -> None:
        """Handle ML training request events"""
        try:
            logger.info(f"Processing training request: {event.get('request_id')}")
            
            # Extract training configuration
            config = event.get('training_config', {})
            tenant_id = event.get('tenant_id')
            user_id = event.get('user_id')
            
            # Submit to training orchestrator
            training_job = await self.training_orchestrator.submit_job(
                config=config,
                tenant_id=tenant_id,
                user_id=user_id,
                metadata=event.get('metadata', {})
            )
            
            # Publish training started event
            await self.publish_event(
                f"persistent://platformq/{tenant_id}/ml-training-started-events",
                {
                    "job_id": training_job.job_id,
                    "request_id": event.get('request_id'),
                    "status": "started",
                    "started_at": datetime.utcnow().isoformat()
                }
            )
            
            self.metrics.record_success("training_request")
            
        except Exception as e:
            logger.error(f"Error handling training request: {e}")
            self.metrics.record_failure("training_request", str(e))
            
            # Publish failure event
            await self.publish_event(
                f"persistent://platformq/{event.get('tenant_id')}/ml-training-failed-events",
                {
                    "request_id": event.get('request_id'),
                    "error": str(e),
                    "failed_at": datetime.utcnow().isoformat()
                }
            )
            
    async def handle_inference_request(self, event: Dict[str, Any]) -> None:
        """Handle ML inference request events"""
        try:
            logger.info(f"Processing inference request: {event.get('request_id')}")
            
            deployment_id = event.get('deployment_id')
            input_data = event.get('input_data')
            tenant_id = event.get('tenant_id')
            
            # Route to serving engine
            prediction = await self.serving_engine.predict(
                deployment_id=deployment_id,
                input_data=input_data,
                tenant_id=tenant_id
            )
            
            # Publish prediction result
            await self.publish_event(
                f"persistent://platformq/{tenant_id}/ml-prediction-completed-events",
                {
                    "request_id": event.get('request_id'),
                    "deployment_id": deployment_id,
                    "prediction": prediction,
                    "completed_at": datetime.utcnow().isoformat()
                }
            )
            
            self.metrics.record_success("inference_request")
            
        except Exception as e:
            logger.error(f"Error handling inference request: {e}")
            self.metrics.record_failure("inference_request", str(e))
            
    async def handle_federated_event(self, event: Dict[str, Any]) -> None:
        """Handle federated learning events"""
        try:
            event_type = event.get('event_type')
            logger.info(f"Processing federated event: {event_type}")
            
            if event_type == 'session_created':
                # New federated session created
                session_id = event.get('session_id')
                await self.federated_coordinator.activate_session(session_id)
                
            elif event_type == 'participant_joined':
                # Participant joined session
                session_id = event.get('session_id')
                participant_id = event.get('participant_id')
                await self.federated_coordinator.add_participant(
                    session_id, participant_id
                )
                
            elif event_type == 'round_completed':
                # Training round completed
                session_id = event.get('session_id')
                round_number = event.get('round_number')
                await self.federated_coordinator.finalize_round(
                    session_id, round_number
                )
                
            self.metrics.record_success(f"federated_{event_type}")
            
        except Exception as e:
            logger.error(f"Error handling federated event: {e}")
            self.metrics.record_failure("federated_event", str(e))
            
    async def handle_retraining_request(self, event: Dict[str, Any]) -> None:
        """Handle model retraining request events"""
        try:
            logger.info(f"Processing retraining request: {event.get('model_id')}")
            
            model_id = event.get('model_id')
            trigger_reason = event.get('trigger_reason')
            tenant_id = event.get('tenant_id')
            
            # Check if retraining is needed
            should_retrain = await self.mlops_manager.evaluate_retraining_need(
                model_id=model_id,
                trigger_reason=trigger_reason,
                metrics=event.get('current_metrics', {})
            )
            
            if should_retrain:
                # Trigger retraining
                retraining_job = await self.mlops_manager.trigger_retraining(
                    model_id=model_id,
                    config=event.get('retraining_config', {}),
                    tenant_id=tenant_id
                )
                
                # Publish retraining started event
                await self.publish_event(
                    f"persistent://platformq/{tenant_id}/ml-retraining-started-events",
                    {
                        "model_id": model_id,
                        "job_id": retraining_job.job_id,
                        "trigger_reason": trigger_reason,
                        "started_at": datetime.utcnow().isoformat()
                    }
                )
            else:
                logger.info(f"Retraining not needed for model {model_id}")
                
            self.metrics.record_success("retraining_request")
            
        except Exception as e:
            logger.error(f"Error handling retraining request: {e}")
            self.metrics.record_failure("retraining_request", str(e))
            
    async def handle_anomaly_detection(self, event: Dict[str, Any]) -> None:
        """Handle anomaly detection events"""
        try:
            logger.info(f"Processing anomaly detection: {event.get('detector_id')}")
            
            detector_id = event.get('detector_id')
            data_stream = event.get('data_stream')
            tenant_id = event.get('tenant_id')
            
            # Process through neuromorphic engine if available
            if hasattr(self, 'neuromorphic_engine'):
                anomalies = await self.neuromorphic_engine.detect_anomalies(
                    detector_id=detector_id,
                    data_stream=data_stream
                )
                
                if anomalies:
                    # Publish anomaly detected event
                    await self.publish_event(
                        f"persistent://platformq/{tenant_id}/anomalies-detected-events",
                        {
                            "detector_id": detector_id,
                            "anomalies": anomalies,
                            "detected_at": datetime.utcnow().isoformat()
                        }
                    )
                    
            self.metrics.record_success("anomaly_detection")
            
        except Exception as e:
            logger.error(f"Error handling anomaly detection: {e}")
            self.metrics.record_failure("anomaly_detection", str(e))
            
    async def handle_model_deployed(self, event: Dict[str, Any]) -> None:
        """Handle model deployment events"""
        try:
            logger.info(f"Processing model deployment: {event.get('deployment_id')}")
            
            deployment_id = event.get('deployment_id')
            model_id = event.get('model_id')
            tenant_id = event.get('tenant_id')
            
            # Register deployment in serving engine
            await self.serving_engine.register_deployment(
                deployment_id=deployment_id,
                model_id=model_id,
                config=event.get('deployment_config', {})
            )
            
            # Start monitoring
            await self.mlops_manager.start_deployment_monitoring(
                deployment_id=deployment_id,
                model_id=model_id,
                tenant_id=tenant_id
            )
            
            self.metrics.record_success("model_deployed")
            
        except Exception as e:
            logger.error(f"Error handling model deployment: {e}")
            self.metrics.record_failure("model_deployed", str(e))
            
    async def handle_drift_detected(self, event: Dict[str, Any]) -> None:
        """Handle drift detection events"""
        try:
            logger.info(f"Processing drift detection: {event.get('model_id')}")
            
            model_id = event.get('model_id')
            drift_type = event.get('drift_type')
            drift_score = event.get('drift_score')
            tenant_id = event.get('tenant_id')
            
            # Evaluate action needed
            action = await self.mlops_manager.evaluate_drift_action(
                model_id=model_id,
                drift_type=drift_type,
                drift_score=drift_score
            )
            
            if action == "retrain":
                # Trigger retraining
                await self.handle_retraining_request({
                    "model_id": model_id,
                    "trigger_reason": f"{drift_type}_drift",
                    "tenant_id": tenant_id,
                    "current_metrics": {"drift_score": drift_score}
                })
            elif action == "alert":
                # Send alert
                await self.publish_event(
                    f"persistent://platformq/{tenant_id}/ml-drift-alert-events",
                    {
                        "model_id": model_id,
                        "drift_type": drift_type,
                        "drift_score": drift_score,
                        "severity": "warning",
                        "alerted_at": datetime.utcnow().isoformat()
                    }
                )
                
            self.metrics.record_success("drift_detected")
            
        except Exception as e:
            logger.error(f"Error handling drift detection: {e}")
            self.metrics.record_failure("drift_detected", str(e))
            
    async def handle_experiment_completed(self, event: Dict[str, Any]) -> None:
        """Handle experiment completion events"""
        try:
            logger.info(f"Processing experiment completion: {event.get('experiment_id')}")
            
            experiment_id = event.get('experiment_id')
            best_run_id = event.get('best_run_id')
            metrics = event.get('best_metrics', {})
            tenant_id = event.get('tenant_id')
            
            # Register best model if meets criteria
            if metrics.get('accuracy', 0) > 0.9:  # Example threshold
                model_id = await self.model_registry.register_from_experiment(
                    experiment_id=experiment_id,
                    run_id=best_run_id,
                    tenant_id=tenant_id
                )
                
                # Publish model registered event
                await self.publish_event(
                    f"persistent://platformq/{tenant_id}/ml-model-registered-events",
                    {
                        "model_id": model_id,
                        "experiment_id": experiment_id,
                        "run_id": best_run_id,
                        "metrics": metrics,
                        "registered_at": datetime.utcnow().isoformat()
                    }
                )
                
            self.metrics.record_success("experiment_completed")
            
        except Exception as e:
            logger.error(f"Error handling experiment completion: {e}")
            self.metrics.record_failure("experiment_completed", str(e)) 