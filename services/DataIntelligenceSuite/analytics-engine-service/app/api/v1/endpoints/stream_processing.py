"""
Stream Processing API endpoints.
"""

from typing import Dict, Any, List, Optional
from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field

from app.core.config import settings
from app.engines.stream import (
    StreamProcessor,
    StreamConfig,
    ProcessingMode,
    WindowType,
    PatternDetector,
    Pattern,
    PatternType,
    PatternCondition,
    PatternElement,
    StateManager,
    CheckpointConfig,
    StateBackend,
    create_sequence_pattern,
    create_threshold_pattern
)
from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.integrations import IgniteClient

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter()

# Global instances (would be dependency injected in production)
_stream_processor = None
_pattern_detector = None
_state_manager = None


def get_stream_processor() -> StreamProcessor:
    """Get or create stream processor instance."""
    global _stream_processor
    if _stream_processor is None:
        event_bus = EventBus()
        cache_manager = CacheManager()
        ignite_client = IgniteClient() if settings.IGNITE_URL else None
        
        config = StreamConfig()
        _stream_processor = StreamProcessor(
            event_bus=event_bus,
            cache_manager=cache_manager,
            ignite_client=ignite_client,
            config=config
        )
    
    return _stream_processor


def get_pattern_detector() -> PatternDetector:
    """Get or create pattern detector instance."""
    global _pattern_detector
    if _pattern_detector is None:
        _pattern_detector = PatternDetector()
    
    return _pattern_detector


def get_state_manager() -> StateManager:
    """Get or create state manager instance."""
    global _state_manager
    if _state_manager is None:
        cache_manager = CacheManager()
        ignite_client = IgniteClient() if settings.IGNITE_URL else None
        
        checkpoint_config = CheckpointConfig(
            state_backend=StateBackend.IGNITE if ignite_client else StateBackend.MEMORY
        )
        
        _state_manager = StateManager(
            checkpoint_config=checkpoint_config,
            cache_manager=cache_manager,
            ignite_client=ignite_client
        )
    
    return _state_manager


# Request/Response models
class TransformationConfig(BaseModel):
    """Transformation configuration."""
    type: str = Field(..., description="Type: filter, map, flatmap, keyby, aggregate")
    condition: Optional[Dict[str, Any]] = Field(default=None, description="Filter condition")
    mapping: Optional[Dict[str, str]] = Field(default=None, description="Field mappings")
    field: Optional[str] = Field(default=None, description="Field for keyby/flatmap")


class WindowConfig(BaseModel):
    """Window configuration."""
    type: WindowType = Field(..., description="Window type")
    size: int = Field(..., description="Window size in milliseconds")
    slide: Optional[int] = Field(default=None, description="Slide interval for sliding windows")
    aggregations: Optional[List[Dict[str, Any]]] = Field(default=None, description="Aggregations to apply")


class JobCreateRequest(BaseModel):
    """Request to create a streaming job."""
    job_id: str = Field(..., description="Unique job identifier")
    name: str = Field(..., description="Job name")
    source_topic: str = Field(..., description="Source event topic")
    sink_topic: str = Field(..., description="Sink event topic")
    transformations: List[TransformationConfig] = Field(..., description="Transformations to apply")
    window_config: Optional[WindowConfig] = Field(default=None, description="Window configuration")
    parallelism: Optional[int] = Field(default=None, ge=1, le=32)


class EventProcessRequest(BaseModel):
    """Request to process an event."""
    job_id: str = Field(..., description="Job to process event with")
    event: Dict[str, Any] = Field(..., description="Event data")


class PatternConditionModel(BaseModel):
    """Pattern condition model."""
    field: str
    operator: str
    value: Any


class PatternElementModel(BaseModel):
    """Pattern element model."""
    name: str
    conditions: List[PatternConditionModel]
    quantifier: Optional[str] = None
    within_seconds: Optional[int] = None


class PatternCreateRequest(BaseModel):
    """Request to create a pattern."""
    pattern_id: str = Field(..., description="Unique pattern identifier")
    name: str = Field(..., description="Pattern name")
    pattern_type: PatternType = Field(..., description="Pattern type")
    elements: List[PatternElementModel] = Field(..., description="Pattern elements")
    within_seconds: Optional[int] = Field(default=None, description="Overall time window in seconds")


class ThresholdPatternRequest(BaseModel):
    """Request to create a threshold pattern."""
    pattern_id: str
    name: str
    field: str
    threshold: float
    count: int
    within_seconds: int


class StateUpdateRequest(BaseModel):
    """Request to update state."""
    operator_id: str
    key: str
    value: Any
    state_type: str = "value"  # value, list, map


class JobResponse(BaseModel):
    """Job creation response."""
    job_id: str
    name: str
    status: str
    created_at: str
    source_topic: str
    sink_topic: str


class ProcessingResponse(BaseModel):
    """Event processing response."""
    processed: bool
    output: Optional[Dict[str, Any]]
    latency_ms: float


class PatternResponse(BaseModel):
    """Pattern creation response."""
    pattern_id: str
    name: str
    pattern_type: str
    element_count: int


class StateResponse(BaseModel):
    """State response."""
    operator_id: str
    key: str
    value: Any
    state_type: str


# API Endpoints
@router.post("/jobs", response_model=JobResponse)
async def create_job(
    request: JobCreateRequest,
    background_tasks: BackgroundTasks,
    processor: StreamProcessor = Depends(get_stream_processor)
):
    """
    Create a new streaming job.
    
    Streaming jobs process events in real-time with transformations
    and optional windowing for aggregations.
    """
    try:
        # Initialize processor if needed
        if not hasattr(processor, '_initialized'):
            await processor.initialize()
            processor._initialized = True
        
        # Convert transformations
        transformations = [t.dict() for t in request.transformations]
        
        # Convert window config
        window_config = request.window_config.dict() if request.window_config else None
        
        # Create job
        result = await processor.create_job(
            job_id=request.job_id,
            name=request.name,
            source_topic=request.source_topic,
            sink_topic=request.sink_topic,
            transformations=transformations,
            window_config=window_config,
            parallelism=request.parallelism
        )
        
        return JobResponse(**result)
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/process", response_model=ProcessingResponse)
async def process_event(
    request: EventProcessRequest,
    processor: StreamProcessor = Depends(get_stream_processor)
):
    """
    Process a single event through a job.
    
    Useful for testing and debugging stream processing logic.
    """
    try:
        import time
        start_time = time.time()
        
        output = await processor.process_event(
            job_id=request.job_id,
            event=request.event
        )
        
        latency_ms = (time.time() - start_time) * 1000
        
        return ProcessingResponse(
            processed=output is not None,
            output=output,
            latency_ms=latency_ms
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error processing event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}", response_model=Dict[str, Any])
async def get_job_status(
    job_id: str,
    processor: StreamProcessor = Depends(get_stream_processor)
):
    """Get job status and metrics."""
    try:
        status = await processor.get_job_status(job_id)
        return status
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/jobs/{job_id}")
async def cancel_job(
    job_id: str,
    processor: StreamProcessor = Depends(get_stream_processor)
):
    """Cancel a streaming job."""
    try:
        await processor.cancel_job(job_id)
        return {"message": f"Job {job_id} cancelled"}
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error cancelling job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Pattern Detection endpoints
@router.post("/patterns", response_model=PatternResponse)
async def create_pattern(
    request: PatternCreateRequest,
    detector: PatternDetector = Depends(get_pattern_detector)
):
    """
    Create a complex event pattern.
    
    Patterns detect sequences, conjunctions, or temporal relationships
    between events in real-time streams.
    """
    try:
        # Convert elements
        elements = []
        for elem in request.elements:
            conditions = [
                PatternCondition(
                    field=c.field,
                    operator=c.operator,
                    value=c.value
                )
                for c in elem.conditions
            ]
            
            element = PatternElement(
                name=elem.name,
                conditions=conditions,
                quantifier=elem.quantifier,
                within=timedelta(seconds=elem.within_seconds) if elem.within_seconds else None
            )
            elements.append(element)
        
        # Create pattern
        pattern = Pattern(
            pattern_id=request.pattern_id,
            name=request.name,
            pattern_type=request.pattern_type,
            elements=elements,
            within=timedelta(seconds=request.within_seconds) if request.within_seconds else None
        )
        
        # Register pattern
        detector.register_pattern(pattern)
        
        return PatternResponse(
            pattern_id=pattern.pattern_id,
            name=pattern.name,
            pattern_type=pattern.pattern_type.value,
            element_count=len(pattern.elements)
        )
        
    except Exception as e:
        logger.error(f"Error creating pattern: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/patterns/threshold", response_model=PatternResponse)
async def create_threshold_pattern_endpoint(
    request: ThresholdPatternRequest,
    detector: PatternDetector = Depends(get_pattern_detector)
):
    """
    Create a threshold breach pattern.
    
    Detects when a field exceeds a threshold N times within a time window.
    """
    try:
        pattern = create_threshold_pattern(
            pattern_id=request.pattern_id,
            name=request.name,
            field=request.field,
            threshold=request.threshold,
            count=request.count,
            within=timedelta(seconds=request.within_seconds)
        )
        
        detector.register_pattern(pattern)
        
        return PatternResponse(
            pattern_id=pattern.pattern_id,
            name=pattern.name,
            pattern_type=pattern.pattern_type.value,
            element_count=len(pattern.elements)
        )
        
    except Exception as e:
        logger.error(f"Error creating threshold pattern: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/patterns/{pattern_id}")
async def delete_pattern(
    pattern_id: str,
    detector: PatternDetector = Depends(get_pattern_detector)
):
    """Delete a pattern."""
    try:
        detector.unregister_pattern(pattern_id)
        return {"message": f"Pattern {pattern_id} deleted"}
        
    except Exception as e:
        logger.error(f"Error deleting pattern: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/patterns/stats", response_model=Dict[str, Any])
async def get_pattern_stats(
    pattern_id: Optional[str] = None,
    detector: PatternDetector = Depends(get_pattern_detector)
):
    """Get pattern detection statistics."""
    try:
        stats = detector.get_pattern_stats(pattern_id)
        return stats
        
    except Exception as e:
        logger.error(f"Error getting pattern stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# State Management endpoints
@router.post("/state", response_model=StateResponse)
async def update_state(
    request: StateUpdateRequest,
    state_manager: StateManager = Depends(get_state_manager)
):
    """Update operator state."""
    try:
        # Initialize if needed
        if not hasattr(state_manager, '_initialized'):
            await state_manager.initialize()
            state_manager._initialized = True
        
        if request.state_type == "value":
            await state_manager.update_value_state(
                request.operator_id,
                request.key,
                request.value
            )
        elif request.state_type == "list":
            await state_manager.append_to_list_state(
                request.operator_id,
                request.key,
                request.value
            )
        # Add other state types as needed
        
        return StateResponse(
            operator_id=request.operator_id,
            key=request.key,
            value=request.value,
            state_type=request.state_type
        )
        
    except Exception as e:
        logger.error(f"Error updating state: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/state/{operator_id}/{key}", response_model=StateResponse)
async def get_state(
    operator_id: str,
    key: str,
    state_manager: StateManager = Depends(get_state_manager)
):
    """Get operator state."""
    try:
        value = await state_manager.get_value_state(operator_id, key)
        
        if value is None:
            raise HTTPException(status_code=404, detail="State not found")
        
        return StateResponse(
            operator_id=operator_id,
            key=key,
            value=value,
            state_type="value"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting state: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/checkpoint")
async def trigger_checkpoint(
    state_manager: StateManager = Depends(get_state_manager)
):
    """Manually trigger state checkpoint."""
    try:
        result = await state_manager.trigger_checkpoint()
        return result
        
    except Exception as e:
        logger.error(f"Error triggering checkpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/checkpoint/{checkpoint_id}/restore")
async def restore_checkpoint(
    checkpoint_id: str,
    state_manager: StateManager = Depends(get_state_manager)
):
    """Restore state from checkpoint."""
    try:
        await state_manager.restore_from_checkpoint(checkpoint_id)
        return {"message": f"State restored from checkpoint {checkpoint_id}"}
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error restoring checkpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics", response_model=Dict[str, Any])
async def get_stream_metrics(
    processor: StreamProcessor = Depends(get_stream_processor),
    state_manager: StateManager = Depends(get_state_manager)
):
    """Get stream processing metrics."""
    try:
        # Get processor metrics
        processor_metrics = {
            "events_processed": processor.metrics.events_processed,
            "events_per_second": processor.metrics.events_per_second,
            "processing_latency_ms": processor.metrics.processing_latency_ms,
            "watermark_lag_ms": processor.metrics.watermark_lag_ms,
            "errors_count": processor.metrics.errors_count,
            "active_jobs": len(processor.jobs),
            "active_windows": len(processor.windows)
        }
        
        # Get state metrics
        state_metrics = state_manager.get_state_metrics()
        
        return {
            "processor": processor_metrics,
            "state": state_metrics
        }
        
    except Exception as e:
        logger.error(f"Error getting metrics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Utility endpoints
@router.get("/window-types", response_model=List[str])
async def get_window_types():
    """Get available window types."""
    return [wt.value for wt in WindowType]


@router.get("/pattern-types", response_model=List[str])
async def get_pattern_types():
    """Get available pattern types."""
    return [pt.value for pt in PatternType]


@router.get("/processing-modes", response_model=List[str])
async def get_processing_modes():
    """Get available processing modes."""
    return [pm.value for pm in ProcessingMode] 