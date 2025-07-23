"""Event data models for DataIntelligence services."""

from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass, field
import uuid
from pulsar.schema import Record

from .event_types import EventType, EventPriority


@dataclass
class EventMetadata:
    """Standard metadata for all events"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = EventType.CUSTOM.value
    source_service: str = ""
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    correlation_id: Optional[str] = None
    user_id: Optional[str] = None
    tenant_id: Optional[str] = None
    priority: int = EventPriority.NORMAL.value
    version: str = "1.0"
    tags: List[str] = field(default_factory=list)
    

@dataclass
class DataEvent(Record):
    """Base event for data-related events"""
    metadata: EventMetadata
    dataset_id: str
    dataset_name: str
    operation: str
    record_count: Optional[int] = None
    size_bytes: Optional[int] = None
    schema_version: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ModelEvent(Record):
    """Base event for ML model events"""
    metadata: EventMetadata
    model_id: str
    model_name: str
    model_version: str
    model_type: str
    operation: str
    metrics: Dict[str, float] = field(default_factory=dict)
    parameters: Dict[str, Any] = field(default_factory=dict)
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineEvent(Record):
    """Base event for pipeline events"""
    metadata: EventMetadata
    pipeline_id: str
    pipeline_name: str
    stage: Optional[str] = None
    status: str = "unknown"
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryEvent(Record):
    """Base event for query events"""
    metadata: EventMetadata
    query_id: str
    query_type: str
    query_text: Optional[str] = None
    execution_time_ms: Optional[float] = None
    rows_returned: Optional[int] = None
    cache_hit: bool = False
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ServiceEvent(Record):
    """Base event for service lifecycle events"""
    metadata: EventMetadata
    service_name: str
    service_version: str
    status: str
    message: Optional[str] = None
    health_metrics: Dict[str, Any] = field(default_factory=dict)
    attributes: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowEvent(Record):
    """Base event for workflow events"""
    metadata: EventMetadata
    workflow_id: str
    workflow_name: str
    task_id: Optional[str] = None
    task_name: Optional[str] = None
    status: str
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    attributes: Dict[str, Any] = field(default_factory=dict) 