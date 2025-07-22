"""
Event-Driven ML Integration

Integrates ML platform with event-driven architecture for:
- Model lifecycle event publishing
- Lineage tracking in graph database
- Training data management in data lake
- Real-time metrics and monitoring
"""

import logging
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime
from enum import Enum
import asyncio
import json

from platformq_shared import ServiceClient
import httpx

logger = logging.getLogger(__name__)


class MLEventType(Enum):
    """ML event types matching Event Router Service"""
    TRAINING_STARTED = "training_started"
    TRAINING_COMPLETED = "training_completed"
    TRAINING_FAILED = "training_failed"
    MODEL_REGISTERED = "model_registered"
    MODEL_DEPLOYED = "model_deployed"
    MODEL_RETIRED = "model_retired"
    INFERENCE_REQUEST = "inference_request"
    INFERENCE_RESULT = "inference_result"
    FEATURE_COMPUTED = "feature_computed"
    DRIFT_DETECTED = "drift_detected"
    EXPERIMENT_CREATED = "experiment_created"
    EXPERIMENT_COMPLETED = "experiment_completed"
    FEDERATED_ROUND_STARTED = "federated_round_started"
    FEDERATED_ROUND_COMPLETED = "federated_round_completed"


class EventDrivenMLIntegration:
    """Integrates ML platform with event-driven architecture"""
    
    def __init__(self, vault_consul_integration=None):
        self.vault_consul = vault_consul_integration
        
        # Service clients
        self.event_router_client = ServiceClient(
            service_name="event-router-service",
            circuit_breaker_threshold=5,
            rate_limit=1000.0
        )
        
        self.graph_intelligence_client = ServiceClient(
            service_name="graph-intelligence-service", 
            circuit_breaker_threshold=5,
            rate_limit=200.0
        )
        
        self.data_platform_client = ServiceClient(
            service_name="data-platform-service",
            circuit_breaker_threshold=5,
            rate_limit=500.0
        )
        
        # Event handlers
        self.event_handlers: Dict[MLEventType, List[Callable]] = {
            event_type: [] for event_type in MLEventType
        }
        
        # Metrics
        self.events_published = 0
        self.events_failed = 0
        self.lineage_updates = 0
        self.data_lake_operations = 0
        
    async def initialize(self):
        """Initialize integration"""
        logger.info("Initializing event-driven ML integration")
        
        # Register default handlers
        self._register_default_handlers()
        
        # Start background tasks
        asyncio.create_task(self._monitor_integration_health())
        
    def _register_default_handlers(self):
        """Register default event handlers"""
        # Training lifecycle handlers
        self.register_event_handler(
            MLEventType.TRAINING_COMPLETED,
            self._handle_training_completed
        )
        self.register_event_handler(
            MLEventType.MODEL_REGISTERED,
            self._handle_model_registered
        )
        self.register_event_handler(
            MLEventType.DRIFT_DETECTED,
            self._handle_drift_detected
        )
        
    def register_event_handler(self, event_type: MLEventType, handler: Callable):
        """Register handler for specific ML event type"""
        self.event_handlers[event_type].append(handler)
        logger.info(f"Registered ML handler for {event_type.value}")
        
    async def publish_ml_event(self, event_type: MLEventType, event_data: Dict[str, Any]) -> bool:
        """Publish ML event to event router"""
        try:
            # Prepare event
            event = {
                "event_type": event_type.value,
                "timestamp": datetime.utcnow().isoformat(),
                **event_data
            }
            
            # Route to appropriate endpoint based on event type
            endpoint_map = {
                MLEventType.TRAINING_STARTED: "/api/v1/ml-events/training-events",
                MLEventType.TRAINING_COMPLETED: "/api/v1/ml-events/training-events",
                MLEventType.TRAINING_FAILED: "/api/v1/ml-events/training-events",
                MLEventType.MODEL_REGISTERED: "/api/v1/ml-events/training-events",
                MLEventType.MODEL_DEPLOYED: "/api/v1/ml-events/training-events",
                MLEventType.MODEL_RETIRED: "/api/v1/ml-events/training-events",
                MLEventType.INFERENCE_REQUEST: "/api/v1/ml-events/inference-events",
                MLEventType.INFERENCE_RESULT: "/api/v1/ml-events/inference-events",
                MLEventType.FEATURE_COMPUTED: "/api/v1/ml-events/feature-events",
                MLEventType.DRIFT_DETECTED: "/api/v1/ml-events/drift-events",
                MLEventType.EXPERIMENT_CREATED: "/api/v1/ml-events/experiment-events",
                MLEventType.EXPERIMENT_COMPLETED: "/api/v1/ml-events/experiment-events",
                MLEventType.FEDERATED_ROUND_STARTED: "/api/v1/ml-events/federated-events",
                MLEventType.FEDERATED_ROUND_COMPLETED: "/api/v1/ml-events/federated-events"
            }
            
            endpoint = endpoint_map.get(event_type)
            if not endpoint:
                logger.error(f"No endpoint mapped for event type: {event_type}")
                return False
                
            # Publish to event router
            response = await self.event_router_client.post(endpoint, json=event)
            
            if response.status_code == 200:
                self.events_published += 1
                logger.info(f"Published ML event: {event_type.value}")
                
                # Execute local handlers
                for handler in self.event_handlers[event_type]:
                    asyncio.create_task(handler(event))
                    
                return True
            else:
                self.events_failed += 1
                logger.error(f"Failed to publish ML event: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error publishing ML event: {e}")
            self.events_failed += 1
            return False
            
    async def _handle_training_completed(self, event: Dict[str, Any]):
        """Handle training completed event"""
        try:
            # Update model lineage
            await self.update_model_lineage(event)
            
            # Save training artifacts to data lake
            await self.save_training_artifacts(event)
            
            # Track metrics
            await self.track_training_metrics(event)
            
        except Exception as e:
            logger.error(f"Error handling training completed event: {e}")
            
    async def _handle_model_registered(self, event: Dict[str, Any]):
        """Handle model registered event"""
        try:
            # Create lineage node
            await self.create_model_lineage_node(event)
            
            # Update model registry
            await self.update_model_registry(event)
            
        except Exception as e:
            logger.error(f"Error handling model registered event: {e}")
            
    async def _handle_drift_detected(self, event: Dict[str, Any]):
        """Handle drift detected event"""
        try:
            # Analyze impact
            impact = await self.analyze_drift_impact(event)
            
            # Trigger retraining if needed
            if impact.get("requires_retraining", False):
                await self.trigger_model_retraining(event)
                
        except Exception as e:
            logger.error(f"Error handling drift detected event: {e}")
            
    async def update_model_lineage(self, event: Dict[str, Any]) -> bool:
        """Update model lineage in graph database"""
        try:
            model_metadata = event.get("model_metadata", {})
            
            # Add model to lineage graph
            model_node = {
                "model_id": model_metadata.get("model_id"),
                "name": model_metadata.get("model_name"),
                "version": model_metadata.get("version"),
                "algorithm": model_metadata.get("algorithm"),
                "framework": model_metadata.get("framework"),
                "metrics": model_metadata.get("metrics", {}),
                "parameters": model_metadata.get("parameters", {}),
                "tags": model_metadata.get("tags", [])
            }
            
            response = await self.graph_intelligence_client.post(
                "/api/v1/ml-lineage/models",
                json=model_node
            )
            
            if response.status_code == 200:
                self.lineage_updates += 1
                
                # Add relationships
                await self._add_lineage_relationships(model_metadata)
                
                return True
            else:
                logger.error(f"Failed to update model lineage: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating model lineage: {e}")
            return False
            
    async def _add_lineage_relationships(self, model_metadata: Dict[str, Any]):
        """Add lineage relationships for model"""
        try:
            model_id = model_metadata.get("model_id")
            
            # Add dataset relationship
            if model_metadata.get("dataset_id"):
                await self.graph_intelligence_client.post(
                    "/api/v1/ml-lineage/relationships",
                    json={
                        "from_id": model_id,
                        "to_id": model_metadata["dataset_id"],
                        "relationship_type": "trained_on",
                        "metadata": {"training_date": datetime.utcnow().isoformat()}
                    }
                )
                
            # Add parent model relationship
            if model_metadata.get("parent_model_id"):
                await self.graph_intelligence_client.post(
                    "/api/v1/ml-lineage/relationships",
                    json={
                        "from_id": model_id,
                        "to_id": model_metadata["parent_model_id"],
                        "relationship_type": "derived_from",
                        "metadata": {"derivation_type": "fine_tuning"}
                    }
                )
                
            # Add experiment relationship
            if model_metadata.get("experiment_id"):
                await self.graph_intelligence_client.post(
                    "/api/v1/ml-lineage/relationships",
                    json={
                        "from_id": model_id,
                        "to_id": model_metadata["experiment_id"],
                        "relationship_type": "part_of_experiment",
                        "metadata": {}
                    }
                )
                
        except Exception as e:
            logger.error(f"Error adding lineage relationships: {e}")
            
    async def save_training_artifacts(self, event: Dict[str, Any]) -> bool:
        """Save training artifacts to data lake"""
        try:
            model_metadata = event.get("model_metadata", {})
            
            # Save model artifact metadata
            artifact_metadata = {
                "name": model_metadata.get("model_name"),
                "version": model_metadata.get("version"),
                "algorithm": model_metadata.get("algorithm"),
                "framework": model_metadata.get("framework"),
                "training_dataset_id": model_metadata.get("dataset_id"),
                "feature_set_id": model_metadata.get("feature_set_id", ""),
                "metrics": model_metadata.get("metrics", {}),
                "parameters": model_metadata.get("parameters", {})
            }
            
            # Note: Actual model file would be uploaded separately
            # This is just metadata tracking
            
            self.data_lake_operations += 1
            logger.info(f"Saved training artifacts for model: {model_metadata.get('model_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Error saving training artifacts: {e}")
            return False
            
    async def track_training_metrics(self, event: Dict[str, Any]) -> bool:
        """Track training metrics for monitoring"""
        try:
            model_metadata = event.get("model_metadata", {})
            metrics = model_metadata.get("metrics", {})
            
            # Publish metrics event
            metrics_event = {
                "model_id": model_metadata.get("model_id"),
                "model_name": model_metadata.get("model_name"),
                "metrics": metrics,
                "timestamp": datetime.utcnow().isoformat(),
                "training_duration": event.get("duration_seconds", 0),
                "resource_usage": event.get("resource_usage", {})
            }
            
            # Could send to monitoring service or store in time series DB
            logger.info(f"Tracked metrics for model: {model_metadata.get('model_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Error tracking training metrics: {e}")
            return False
            
    async def create_model_lineage_node(self, event: Dict[str, Any]) -> bool:
        """Create model node in lineage graph"""
        try:
            # Similar to update_model_lineage but for initial creation
            return await self.update_model_lineage(event)
            
        except Exception as e:
            logger.error(f"Error creating model lineage node: {e}")
            return False
            
    async def update_model_registry(self, event: Dict[str, Any]) -> bool:
        """Update model registry with new model"""
        try:
            # In a real implementation, this would update MLflow or similar
            logger.info(f"Updated model registry for: {event.get('model_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating model registry: {e}")
            return False
            
    async def analyze_drift_impact(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze impact of detected drift"""
        try:
            # Query lineage to find affected models and deployments
            model_id = event.get("model_id")
            
            response = await self.graph_intelligence_client.post(
                "/api/v1/ml-lineage/impact-analysis",
                json={
                    "artifact_id": model_id,
                    "change_type": "drift"
                }
            )
            
            if response.status_code == 200:
                impact = response.json().get("impact", {})
                
                # Determine if retraining is needed
                drift_score = event.get("drift_score", 0)
                if drift_score > 0.7 or impact.get("risk_level") == "critical":
                    impact["requires_retraining"] = True
                    
                return impact
            else:
                logger.error(f"Failed to analyze drift impact: {response.text}")
                return {}
                
        except Exception as e:
            logger.error(f"Error analyzing drift impact: {e}")
            return {}
            
    async def trigger_model_retraining(self, event: Dict[str, Any]) -> bool:
        """Trigger model retraining based on drift"""
        try:
            # Publish retraining request event
            retraining_event = {
                "event_type": "model_retraining_requested",
                "model_id": event.get("model_id"),
                "model_version": event.get("model_version"),
                "drift_score": event.get("drift_score"),
                "reason": "drift_detected",
                "priority": "high" if event.get("drift_score", 0) > 0.8 else "medium",
                "requested_at": datetime.utcnow().isoformat()
            }
            
            # In a real implementation, this would trigger actual retraining
            logger.info(f"Triggered retraining for model: {event.get('model_id')}")
            return True
            
        except Exception as e:
            logger.error(f"Error triggering model retraining: {e}")
            return False
            
    async def ingest_training_data(self, dataset_name: str, data_path: str) -> Optional[str]:
        """Ingest training data into ML data lake"""
        try:
            # Upload data to data platform
            with open(data_path, 'rb') as f:
                files = {'file': (dataset_name, f, 'application/octet-stream')}
                data = {
                    'dataset_name': dataset_name,
                    'source': 'ml_platform'
                }
                
                response = await self.data_platform_client.post(
                    "/api/v1/ml-lake/datasets/ingest",
                    files=files,
                    data=data
                )
                
            if response.status_code == 200:
                dataset_id = response.json().get("dataset_id")
                self.data_lake_operations += 1
                logger.info(f"Ingested training data: {dataset_id}")
                return dataset_id
            else:
                logger.error(f"Failed to ingest training data: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error ingesting training data: {e}")
            return None
            
    async def create_feature_set(self, dataset_id: str, feature_config: Dict[str, Any]) -> Optional[str]:
        """Create feature set from dataset"""
        try:
            response = await self.data_platform_client.post(
                "/api/v1/ml-lake/features/engineer",
                json={
                    "dataset_id": dataset_id,
                    **feature_config
                }
            )
            
            if response.status_code == 200:
                feature_set_id = response.json().get("feature_set_id")
                self.data_lake_operations += 1
                logger.info(f"Created feature set: {feature_set_id}")
                return feature_set_id
            else:
                logger.error(f"Failed to create feature set: {response.text}")
                return None
                
        except Exception as e:
            logger.error(f"Error creating feature set: {e}")
            return None
            
    async def get_model_lineage(self, model_id: str, depth: int = 3) -> Dict[str, Any]:
        """Get model lineage from graph database"""
        try:
            response = await self.graph_intelligence_client.get(
                f"/api/v1/ml-lineage/models/{model_id}/lineage",
                params={"depth": depth}
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Failed to get model lineage: {response.text}")
                return {}
                
        except Exception as e:
            logger.error(f"Error getting model lineage: {e}")
            return {}
            
    async def find_similar_models(self, model_id: str, threshold: float = 0.7) -> List[Dict[str, Any]]:
        """Find similar models based on lineage"""
        try:
            response = await self.graph_intelligence_client.post(
                "/api/v1/ml-lineage/similarity-search",
                json={
                    "model_id": model_id,
                    "similarity_threshold": threshold
                }
            )
            
            if response.status_code == 200:
                return response.json().get("similar_models", [])
            else:
                logger.error(f"Failed to find similar models: {response.text}")
                return []
                
        except Exception as e:
            logger.error(f"Error finding similar models: {e}")
            return []
            
    async def track_predictions(self, model_id: str, predictions: List[Dict[str, Any]],
                              metadata: Dict[str, Any] = None) -> bool:
        """Track model predictions for monitoring"""
        try:
            response = await self.data_platform_client.post(
                "/api/v1/ml-lake/predictions/track",
                json={
                    "model_id": model_id,
                    "predictions": predictions,
                    "request_metadata": metadata or {}
                }
            )
            
            if response.status_code == 200:
                self.data_lake_operations += 1
                return True
            else:
                logger.error(f"Failed to track predictions: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Error tracking predictions: {e}")
            return False
            
    async def _monitor_integration_health(self):
        """Monitor integration health and metrics"""
        while True:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Log metrics
                logger.info(f"ML Integration metrics - "
                          f"Events published: {self.events_published}, "
                          f"Events failed: {self.events_failed}, "
                          f"Lineage updates: {self.lineage_updates}, "
                          f"Data lake operations: {self.data_lake_operations}")
                
                # Check service health
                services = [
                    ("event-router", self.event_router_client),
                    ("graph-intelligence", self.graph_intelligence_client),
                    ("data-platform", self.data_platform_client)
                ]
                
                for service_name, client in services:
                    try:
                        # Simple health check
                        response = await client.get("/health", timeout=5.0)
                        if response.status_code != 200:
                            logger.warning(f"{service_name} health check failed")
                    except Exception as e:
                        logger.error(f"{service_name} is unreachable: {e}")
                        
            except Exception as e:
                logger.error(f"Error in integration health monitor: {e}")
                
    def get_metrics(self) -> Dict[str, int]:
        """Get integration metrics"""
        return {
            "events_published": self.events_published,
            "events_failed": self.events_failed,
            "lineage_updates": self.lineage_updates,
            "data_lake_operations": self.data_lake_operations
        } 