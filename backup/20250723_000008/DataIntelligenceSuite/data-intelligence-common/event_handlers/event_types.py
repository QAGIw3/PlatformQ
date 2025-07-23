"""Event types and priorities for DataIntelligence services."""

from enum import Enum


class EventPriority(Enum):
    """Event priority levels"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class EventType(Enum):
    """Standard event types for DataIntelligence"""
    # Data events
    DATA_INGESTED = "data.ingested"
    DATA_PROCESSED = "data.processed"
    DATA_QUALITY_CHECK = "data.quality.check"
    DATA_VALIDATED = "data.validated"
    DATA_TRANSFORMED = "data.transformed"
    
    # ML events
    MODEL_TRAINED = "model.trained"
    MODEL_DEPLOYED = "model.deployed"
    MODEL_PREDICTION = "model.prediction"
    MODEL_EVALUATION = "model.evaluation"
    MODEL_DRIFT_DETECTED = "model.drift.detected"
    
    # Pipeline events
    PIPELINE_STARTED = "pipeline.started"
    PIPELINE_COMPLETED = "pipeline.completed"
    PIPELINE_FAILED = "pipeline.failed"
    PIPELINE_STAGE_COMPLETED = "pipeline.stage.completed"
    
    # Service events
    SERVICE_STARTED = "service.started"
    SERVICE_STOPPED = "service.stopped"
    SERVICE_HEALTH_CHECK = "service.health.check"
    SERVICE_ERROR = "service.error"
    
    # Query events
    QUERY_EXECUTED = "query.executed"
    QUERY_OPTIMIZED = "query.optimized"
    QUERY_CACHED = "query.cached"
    
    # Workflow events
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED = "workflow.failed"
    WORKFLOW_TASK_COMPLETED = "workflow.task.completed"
    
    # Custom events
    CUSTOM = "custom" 