"""
Unified event models for DataIntelligenceSuite.

Provides specialized event types built on top of base event model.
"""

from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import uuid

from .base import (
    Event, EventPriority, EventCategory, EventStatus, EventType,
    EventProcessingMode, EventDeliveryMode
)


@dataclass
class SystemEvent(Event):
    """System-level event"""
    def __init__(self, **kwargs):
        super().__init__(category=EventCategory.SYSTEM, **kwargs)


@dataclass
class DataEvent(Event):
    """Data-related event"""
    dataset_id: Optional[str] = None
    table_name: Optional[str] = None
    row_count: Optional[int] = None
    
    def __init__(self, **kwargs):
        dataset_id = kwargs.pop('dataset_id', None)
        table_name = kwargs.pop('table_name', None)
        row_count = kwargs.pop('row_count', None)
        super().__init__(category=EventCategory.DATA, **kwargs)
        self.dataset_id = dataset_id
        self.table_name = table_name
        self.row_count = row_count


@dataclass
class ModelEvent(Event):
    """Model-related event"""
    model_id: Optional[str] = None
    model_name: Optional[str] = None
    model_version: Optional[str] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    
    def __init__(self, **kwargs):
        model_id = kwargs.pop('model_id', None)
        model_name = kwargs.pop('model_name', None)
        model_version = kwargs.pop('model_version', None)
        metrics = kwargs.pop('metrics', {})
        super().__init__(category=EventCategory.MODEL, **kwargs)
        self.model_id = model_id
        self.model_name = model_name
        self.model_version = model_version
        self.metrics = metrics


@dataclass
class ProcessingEvent(Event):
    """Processing-related event"""
    job_id: Optional[str] = None
    pipeline_id: Optional[str] = None
    stage: Optional[str] = None
    progress: Optional[float] = None
    
    def __init__(self, **kwargs):
        job_id = kwargs.pop('job_id', None)
        pipeline_id = kwargs.pop('pipeline_id', None)
        stage = kwargs.pop('stage', None)
        progress = kwargs.pop('progress', None)
        super().__init__(category=EventCategory.PROCESSING, **kwargs)
        self.job_id = job_id
        self.pipeline_id = pipeline_id
        self.stage = stage
        self.progress = progress


@dataclass
class AuditEvent(Event):
    """Audit event"""
    user_id: Optional[str] = None
    action: Optional[str] = None
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    changes: Dict[str, Any] = field(default_factory=dict)
    
    def __init__(self, **kwargs):
        user_id = kwargs.pop('user_id', None)
        action = kwargs.pop('action', None)
        resource_type = kwargs.pop('resource_type', None)
        resource_id = kwargs.pop('resource_id', None)
        changes = kwargs.pop('changes', {})
        super().__init__(category=EventCategory.AUDIT, **kwargs)
        self.user_id = user_id
        self.action = action
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.changes = changes


@dataclass
class WorkflowEvent(Event):
    """Workflow event"""
    workflow_id: Optional[str] = None
    workflow_name: Optional[str] = None
    step_id: Optional[str] = None
    step_name: Optional[str] = None
    
    def __init__(self, **kwargs):
        workflow_id = kwargs.pop('workflow_id', None)
        workflow_name = kwargs.pop('workflow_name', None)
        step_id = kwargs.pop('step_id', None)
        step_name = kwargs.pop('step_name', None)
        super().__init__(category=EventCategory.WORKFLOW, **kwargs)
        self.workflow_id = workflow_id
        self.workflow_name = workflow_name
        self.step_id = step_id
        self.step_name = step_name


@dataclass
class SecurityEvent(Event):
    """Security event"""
    user_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    risk_score: Optional[float] = None
    
    def __init__(self, **kwargs):
        user_id = kwargs.pop('user_id', None)
        ip_address = kwargs.pop('ip_address', None)
        user_agent = kwargs.pop('user_agent', None)
        risk_score = kwargs.pop('risk_score', None)
        super().__init__(category=EventCategory.SECURITY, **kwargs)
        self.user_id = user_id
        self.ip_address = ip_address
        self.user_agent = user_agent
        self.risk_score = risk_score


@dataclass
class NotificationEvent(Event):
    """Notification event"""
    recipient: Optional[str] = None
    channel: Optional[str] = None
    template_id: Optional[str] = None
    
    def __init__(self, **kwargs):
        recipient = kwargs.pop('recipient', None)
        channel = kwargs.pop('channel', None)
        template_id = kwargs.pop('template_id', None)
        super().__init__(category=EventCategory.NOTIFICATION, **kwargs)
        self.recipient = recipient
        self.channel = channel
        self.template_id = template_id


@dataclass
class IntegrationEvent(Event):
    """Integration event"""
    integration_id: Optional[str] = None
    external_system: Optional[str] = None
    operation: Optional[str] = None
    
    def __init__(self, **kwargs):
        integration_id = kwargs.pop('integration_id', None)
        external_system = kwargs.pop('external_system', None)
        operation = kwargs.pop('operation', None)
        super().__init__(category=EventCategory.INTEGRATION, **kwargs)
        self.integration_id = integration_id
        self.external_system = external_system
        self.operation = operation


# Event creation helpers
def create_system_event(
    event_type: EventType,
    source: str,
    payload: Dict[str, Any],
    **kwargs
) -> SystemEvent:
    """Create a system event"""
    return SystemEvent(
        event_type=event_type.value,
        source=source,
        payload=payload,
        **kwargs
    )


def create_data_event(
    event_type: EventType,
    source: str,
    dataset_id: str,
    payload: Dict[str, Any],
    **kwargs
) -> DataEvent:
    """Create a data event"""
    return DataEvent(
        event_type=event_type.value,
        source=source,
        dataset_id=dataset_id,
        payload=payload,
        **kwargs
    )


def create_model_event(
    event_type: EventType,
    source: str,
    model_id: str,
    model_name: str,
    payload: Dict[str, Any],
    **kwargs
) -> ModelEvent:
    """Create a model event"""
    return ModelEvent(
        event_type=event_type.value,
        source=source,
        model_id=model_id,
        model_name=model_name,
        payload=payload,
        **kwargs
    )


def create_processing_event(
    event_type: EventType,
    source: str,
    job_id: str,
    payload: Dict[str, Any],
    **kwargs
) -> ProcessingEvent:
    """Create a processing event"""
    return ProcessingEvent(
        event_type=event_type.value,
        source=source,
        job_id=job_id,
        payload=payload,
        **kwargs
    )


def create_audit_event(
    action: str,
    user_id: str,
    resource_type: str,
    resource_id: str,
    source: str,
    changes: Dict[str, Any] = None,
    **kwargs
) -> AuditEvent:
    """Create an audit event"""
    return AuditEvent(
        event_type=EventType.AUDIT_UPDATE.value,
        source=source,
        user_id=user_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        changes=changes or {},
        **kwargs
    )


# Re-export for backward compatibility
__all__ = [
    # From base
    'Event', 'EventPriority', 'EventCategory', 'EventStatus', 'EventType',
    'EventProcessingMode', 'EventDeliveryMode',
    # Specialized events
    'SystemEvent', 'DataEvent', 'ModelEvent', 'ProcessingEvent',
    'AuditEvent', 'WorkflowEvent', 'SecurityEvent', 'NotificationEvent',
    'IntegrationEvent',
    # Helpers
    'create_system_event', 'create_data_event', 'create_model_event',
    'create_processing_event', 'create_audit_event'
] 