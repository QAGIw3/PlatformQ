"""
ML Events API Router

Handles routing and processing of ML-related events including:
- Model training lifecycle events
- Inference requests and results
- Feature pipeline events
- Model monitoring and drift detection
- Experiment tracking events
"""

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from typing import Dict, List, Optional, Any, Union
from pydantic import BaseModel, Field
from datetime import datetime
from decimal import Decimal
from enum import Enum
import asyncio
import logging

from ..core.event_router import EventRouter
from ..core.schemas import Event, RoutingRule
from ..monitoring.dlq_monitor import DLQMonitor

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/ml-events", tags=["ML Events"])


class MLEventType(Enum):
    """Types of ML events"""
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


class ModelMetadata(BaseModel):
    """Model metadata"""
    model_id: str
    model_name: str
    version: str
    algorithm: str
    framework: str
    metrics: Dict[str, float]
    parameters: Dict[str, Any]
    dataset_id: Optional[str] = None
    experiment_id: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class TrainingEvent(BaseModel):
    """Training lifecycle event"""
    event_type: MLEventType
    model_metadata: ModelMetadata
    training_id: str
    timestamp: datetime
    duration_seconds: Optional[int] = None
    resource_usage: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


class InferenceEvent(BaseModel):
    """Model inference event"""
    event_type: MLEventType
    model_id: str
    model_version: str
    request_id: str
    timestamp: datetime
    input_data: Optional[Dict[str, Any]] = None
    prediction: Optional[Any] = None
    confidence: Optional[float] = None
    latency_ms: Optional[int] = None


class FeatureEvent(BaseModel):
    """Feature computation event"""
    event_type: MLEventType
    feature_set_id: str
    feature_names: List[str]
    entity_id: str
    timestamp: datetime
    values: Dict[str, Any]
    computation_time_ms: int


class DriftEvent(BaseModel):
    """Model drift detection event"""
    event_type: MLEventType
    model_id: str
    model_version: str
    drift_type: str  # "data_drift", "concept_drift", "performance_drift"
    timestamp: datetime
    drift_score: float
    baseline_metrics: Dict[str, float]
    current_metrics: Dict[str, float]
    affected_features: List[str]


class ExperimentEvent(BaseModel):
    """ML experiment event"""
    event_type: MLEventType
    experiment_id: str
    experiment_name: str
    timestamp: datetime
    parameters: Dict[str, Any]
    metrics: Optional[Dict[str, float]] = None
    artifacts: Optional[List[str]] = None
    status: str  # "running", "completed", "failed"


class FederatedLearningEvent(BaseModel):
    """Federated learning event"""
    event_type: MLEventType
    federation_id: str
    round_number: int
    timestamp: datetime
    participant_count: int
    global_model_id: Optional[str] = None
    aggregation_method: str
    metrics: Optional[Dict[str, float]] = None


class MLEventRouter:
    """Routes ML events to appropriate destinations"""
    
    def __init__(self, event_router: EventRouter, dlq_monitor: Optional[DLQMonitor] = None):
        self.event_router = event_router
        self.dlq_monitor = dlq_monitor
        
        # ML-specific routing rules
        self.ml_routing_rules = {
            MLEventType.TRAINING_STARTED: ["ml-training-lifecycle", "ml-resource-monitoring"],
            MLEventType.TRAINING_COMPLETED: ["ml-training-lifecycle", "model-registry", "ml-lineage"],
            MLEventType.TRAINING_FAILED: ["ml-training-lifecycle", "ml-alerts"],
            MLEventType.MODEL_REGISTERED: ["model-registry", "ml-lineage", "ml-governance"],
            MLEventType.MODEL_DEPLOYED: ["model-serving", "ml-monitoring", "ml-alerts"],
            MLEventType.MODEL_RETIRED: ["model-registry", "ml-governance"],
            MLEventType.INFERENCE_REQUEST: ["model-serving", "ml-monitoring"],
            MLEventType.INFERENCE_RESULT: ["ml-monitoring", "ml-analytics"],
            MLEventType.FEATURE_COMPUTED: ["feature-store", "ml-lineage"],
            MLEventType.DRIFT_DETECTED: ["ml-monitoring", "ml-alerts", "ml-retraining"],
            MLEventType.EXPERIMENT_CREATED: ["ml-experiments", "ml-lineage"],
            MLEventType.EXPERIMENT_COMPLETED: ["ml-experiments", "model-registry"],
            MLEventType.FEDERATED_ROUND_STARTED: ["federated-learning", "ml-monitoring"],
            MLEventType.FEDERATED_ROUND_COMPLETED: ["federated-learning", "model-registry"]
        }
        
        # Enrichment functions
        self.enrichment_functions = {
            MLEventType.TRAINING_COMPLETED: self._enrich_training_completed,
            MLEventType.INFERENCE_REQUEST: self._enrich_inference_request,
            MLEventType.DRIFT_DETECTED: self._enrich_drift_detection
        }
        
    async def _enrich_training_completed(self, event: TrainingEvent) -> Dict[str, Any]:
        """Enrich training completed event with additional context"""
        enriched = event.dict()
        
        # Add cost calculation
        if event.resource_usage:
            gpu_hours = event.resource_usage.get("gpu_hours", 0)
            cpu_hours = event.resource_usage.get("cpu_hours", 0)
            enriched["estimated_cost"] = (gpu_hours * 2.5) + (cpu_hours * 0.1)  # Example pricing
            
        # Add model ranking
        if event.model_metadata.metrics:
            primary_metric = event.model_metadata.metrics.get("accuracy", 0)
            enriched["model_rank"] = self._calculate_model_rank(primary_metric)
            
        return enriched
        
    async def _enrich_inference_request(self, event: InferenceEvent) -> Dict[str, Any]:
        """Enrich inference request with model metadata"""
        enriched = event.dict()
        
        # Add model metadata (would normally fetch from registry)
        enriched["model_framework"] = "tensorflow"  # Example
        enriched["expected_latency_ms"] = 50
        enriched["cost_per_inference"] = 0.0001
        
        return enriched
        
    async def _enrich_drift_detection(self, event: DriftEvent) -> Dict[str, Any]:
        """Enrich drift event with recommended actions"""
        enriched = event.dict()
        
        # Add severity and recommendations
        if event.drift_score > 0.7:
            enriched["severity"] = "critical"
            enriched["recommended_action"] = "immediate_retraining"
        elif event.drift_score > 0.5:
            enriched["severity"] = "warning"
            enriched["recommended_action"] = "schedule_retraining"
        else:
            enriched["severity"] = "info"
            enriched["recommended_action"] = "monitor"
            
        return enriched
        
    def _calculate_model_rank(self, primary_metric: float) -> str:
        """Calculate model rank based on primary metric"""
        if primary_metric >= 0.95:
            return "champion"
        elif primary_metric >= 0.90:
            return "challenger"
        elif primary_metric >= 0.85:
            return "candidate"
        else:
            return "experimental"
            
    async def route_ml_event(self, event: Union[TrainingEvent, InferenceEvent, FeatureEvent, 
                                                DriftEvent, ExperimentEvent, FederatedLearningEvent]) -> Dict[str, Any]:
        """Route ML event to appropriate destinations"""
        event_type = event.event_type
        destinations = self.ml_routing_rules.get(event_type, ["ml-default"])
        
        # Apply enrichment if available
        enrichment_func = self.enrichment_functions.get(event_type)
        if enrichment_func:
            event_data = await enrichment_func(event)
        else:
            event_data = event.dict()
            
        # Create routing event
        routing_event = Event(
            event_id=f"ml-{event.timestamp.timestamp()}-{event_type.value}",
            event_type=f"ml.{event_type.value}",
            source="ml-platform-service",
            timestamp=event.timestamp,
            data=event_data,
            metadata={
                "ml_event_type": event_type.value,
                "destinations": destinations
            }
        )
        
        # Route to each destination
        results = {}
        for destination in destinations:
            try:
                result = await self.event_router.route_event(routing_event, destination)
                results[destination] = {"status": "success", "result": result}
            except Exception as e:
                logger.error(f"Failed to route ML event to {destination}: {e}")
                results[destination] = {"status": "failed", "error": str(e)}
                
                # Send to DLQ if available
                if self.dlq_monitor:
                    await self.dlq_monitor.send_to_dlq(routing_event, str(e))
                    
        return {
            "event_id": routing_event.event_id,
            "routed_to": results,
            "timestamp": datetime.utcnow()
        }


# Initialize router instance
ml_router_instance = None


def get_ml_router(event_router: EventRouter = Depends(lambda: router.app.state.event_router),
                  dlq_monitor: Optional[DLQMonitor] = Depends(lambda: getattr(router.app.state, 'dlq_monitor', None))) -> MLEventRouter:
    """Get ML router instance"""
    global ml_router_instance
    if not ml_router_instance:
        ml_router_instance = MLEventRouter(event_router, dlq_monitor)
    return ml_router_instance


@router.post("/training-events")
async def submit_training_event(event: TrainingEvent, 
                               ml_router: MLEventRouter = Depends(get_ml_router)) -> Dict[str, Any]:
    """Submit a training lifecycle event"""
    return await ml_router.route_ml_event(event)


@router.post("/inference-events")
async def submit_inference_event(event: InferenceEvent,
                                ml_router: MLEventRouter = Depends(get_ml_router)) -> Dict[str, Any]:
    """Submit an inference event"""
    return await ml_router.route_ml_event(event)


@router.post("/feature-events")
async def submit_feature_event(event: FeatureEvent,
                              ml_router: MLEventRouter = Depends(get_ml_router)) -> Dict[str, Any]:
    """Submit a feature computation event"""
    return await ml_router.route_ml_event(event)


@router.post("/drift-events")
async def submit_drift_event(event: DriftEvent,
                            ml_router: MLEventRouter = Depends(get_ml_router)) -> Dict[str, Any]:
    """Submit a drift detection event"""
    return await ml_router.route_ml_event(event)


@router.post("/experiment-events")
async def submit_experiment_event(event: ExperimentEvent,
                                 ml_router: MLEventRouter = Depends(get_ml_router)) -> Dict[str, Any]:
    """Submit an experiment event"""
    return await ml_router.route_ml_event(event)


@router.post("/federated-events")
async def submit_federated_event(event: FederatedLearningEvent,
                                ml_router: MLEventRouter = Depends(get_ml_router)) -> Dict[str, Any]:
    """Submit a federated learning event"""
    return await ml_router.route_ml_event(event)


@router.post("/batch-events")
async def submit_batch_ml_events(events: List[Union[TrainingEvent, InferenceEvent, FeatureEvent,
                                                   DriftEvent, ExperimentEvent, FederatedLearningEvent]],
                                 ml_router: MLEventRouter = Depends(get_ml_router),
                                 background_tasks: BackgroundTasks = BackgroundTasks()) -> Dict[str, Any]:
    """Submit multiple ML events in batch"""
    # Process in background
    background_tasks.add_task(process_batch_ml_events, events, ml_router)
    
    return {
        "status": "accepted",
        "event_count": len(events),
        "message": "Events queued for processing"
    }


async def process_batch_ml_events(events: List[Any], ml_router: MLEventRouter):
    """Process batch ML events asynchronously"""
    for event in events:
        try:
            await ml_router.route_ml_event(event)
        except Exception as e:
            logger.error(f"Failed to process ML event: {e}")


@router.get("/routing-rules")
async def get_ml_routing_rules(ml_router: MLEventRouter = Depends(get_ml_router)) -> Dict[str, List[str]]:
    """Get current ML event routing rules"""
    return {
        event_type.value: destinations 
        for event_type, destinations in ml_router.ml_routing_rules.items()
    }


@router.put("/routing-rules/{event_type}")
async def update_ml_routing_rule(event_type: MLEventType,
                                destinations: List[str],
                                ml_router: MLEventRouter = Depends(get_ml_router)) -> Dict[str, Any]:
    """Update routing rule for specific ML event type"""
    ml_router.ml_routing_rules[event_type] = destinations
    return {
        "event_type": event_type.value,
        "destinations": destinations,
        "updated_at": datetime.utcnow()
    } 