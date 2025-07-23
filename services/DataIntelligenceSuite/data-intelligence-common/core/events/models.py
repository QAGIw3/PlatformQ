"""
Unified event models for DataIntelligenceSuite.

Combines event models and types from various modules.
"""

from typing import Dict, Any, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import uuid

from .base import Event, EventPriority


class EventCategory(str, Enum):
    """Event categories for classification"""
    SYSTEM = "system"
    DATA = "data"
    MODEL = "model"
    AUDIT = "audit"
    NOTIFICATION = "notification"
    PROCESSING = "processing"
    SECURITY = "security"
    WORKFLOW = "workflow"
    INTEGRATION = "integration"


class EventStatus(str, Enum):
    """Event processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"


class EventType(str, Enum):
    """Standard event types across the platform"""
    # System events
    SYSTEM_STARTUP = "system.startup"
    SYSTEM_SHUTDOWN = "system.shutdown"
    SYSTEM_ERROR = "system.error"
    SYSTEM_WARNING = "system.warning"
    SYSTEM_HEALTH_CHECK = "system.health_check"
    
    # Data events
    DATA_CREATED = "data.created"
    DATA_UPDATED = "data.updated"
    DATA_DELETED = "data.deleted"
    DATA_QUALITY_CHECK = "data.quality_check"
    DATA_TRANSFORMATION = "data.transformation"
    DATA_INGESTION = "data.ingestion"
    DATA_EXPORT = "data.export"
    
    # Model events
    MODEL_CREATED = "model.created"
    MODEL_UPDATED = "model.updated"
    MODEL_DEPLOYED = "model.deployed"
    MODEL_RETIRED = "model.retired"
    MODEL_TRAINING_STARTED = "model.training.started"
    MODEL_TRAINING_COMPLETED = "model.training.completed"
    MODEL_TRAINING_FAILED = "model.training.failed"
    MODEL_PREDICTION = "model.prediction"
    MODEL_EVALUATION = "model.evaluation"
    
    # Processing events
    PROCESSING_STARTED = "processing.started"
    PROCESSING_COMPLETED = "processing.completed"
    PROCESSING_FAILED = "processing.failed"
    PROCESSING_PROGRESS = "processing.progress"
    
    # Workflow events
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED = "workflow.failed"
    WORKFLOW_STEP_COMPLETED = "workflow.step.completed"
    
    # Security events
    SECURITY_LOGIN = "security.login"
    SECURITY_LOGOUT = "security.logout"
    SECURITY_ACCESS_DENIED = "security.access_denied"
    SECURITY_TOKEN_EXPIRED = "security.token_expired"
    
    # Audit events
    AUDIT_CREATE = "audit.create"
    AUDIT_UPDATE = "audit.update"
    AUDIT_DELETE = "audit.delete"
    AUDIT_ACCESS = "audit.access"
    
    # Notification events
    NOTIFICATION_SENT = "notification.sent"
    NOTIFICATION_FAILED = "notification.failed"
    NOTIFICATION_DELIVERED = "notification.delivered"


@dataclass
class SystemEvent(Event):
    """System-level event"""
    def __init__(self, 
                 event_type: str,
                 source: str,
                 message: str,
                 severity: str = "info",
                 **kwargs):
        super().__init__(
            event_type=event_type,
            source=source,
            data={
                "message": message,
                "severity": severity,
                **kwargs
            },
            metadata={
                "category": EventCategory.SYSTEM.value,
                "timestamp": datetime.utcnow().isoformat()
            }
        )


@dataclass
class DataEvent(Event):
    """Data-related event"""
    def __init__(self,
                 event_type: str,
                 source: str,
                 dataset_id: str,
                 dataset_name: str,
                 operation: str,
                 record_count: Optional[int] = None,
                 **kwargs):
        super().__init__(
            event_type=event_type,
            source=source,
            data={
                "dataset_id": dataset_id,
                "dataset_name": dataset_name,
                "operation": operation,
                "record_count": record_count,
                **kwargs
            },
            metadata={
                "category": EventCategory.DATA.value,
                "timestamp": datetime.utcnow().isoformat()
            }
        )


@dataclass
class ModelEvent(Event):
    """Machine learning model event"""
    def __init__(self,
                 event_type: str,
                 source: str,
                 model_id: str,
                 model_name: str,
                 model_version: Optional[str] = None,
                 metrics: Optional[Dict[str, float]] = None,
                 **kwargs):
        super().__init__(
            event_type=event_type,
            source=source,
            data={
                "model_id": model_id,
                "model_name": model_name,
                "model_version": model_version,
                "metrics": metrics or {},
                **kwargs
            },
            metadata={
                "category": EventCategory.MODEL.value,
                "timestamp": datetime.utcnow().isoformat()
            }
        )


@dataclass
class ProcessingEvent(Event):
    """Processing/job event"""
    def __init__(self,
                 event_type: str,
                 source: str,
                 job_id: str,
                 job_name: str,
                 status: str,
                 progress: Optional[float] = None,
                 error_message: Optional[str] = None,
                 **kwargs):
        super().__init__(
            event_type=event_type,
            source=source,
            data={
                "job_id": job_id,
                "job_name": job_name,
                "status": status,
                "progress": progress,
                "error_message": error_message,
                **kwargs
            },
            metadata={
                "category": EventCategory.PROCESSING.value,
                "timestamp": datetime.utcnow().isoformat()
            }
        )


@dataclass
class AuditEvent(Event):
    """Audit trail event"""
    def __init__(self,
                 event_type: str,
                 source: str,
                 user_id: str,
                 user_name: str,
                 action: str,
                 resource_type: str,
                 resource_id: str,
                 details: Optional[Dict[str, Any]] = None,
                 **kwargs):
        super().__init__(
            event_type=event_type,
            source=source,
            data={
                "user_id": user_id,
                "user_name": user_name,
                "action": action,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "details": details or {},
                **kwargs
            },
            metadata={
                "category": EventCategory.AUDIT.value,
                "timestamp": datetime.utcnow().isoformat()
            },
            priority=EventPriority.HIGH  # Audit events are high priority
        )


@dataclass
class NotificationEvent(Event):
    """Notification event"""
    def __init__(self,
                 event_type: str,
                 source: str,
                 recipient: str,
                 channel: str,
                 subject: str,
                 message: str,
                 status: Optional[str] = None,
                 **kwargs):
        super().__init__(
            event_type=event_type,
            source=source,
            data={
                "recipient": recipient,
                "channel": channel,
                "subject": subject,
                "message": message,
                "status": status,
                **kwargs
            },
            metadata={
                "category": EventCategory.NOTIFICATION.value,
                "timestamp": datetime.utcnow().isoformat()
            }
        )


@dataclass
class WorkflowEvent(Event):
    """Workflow execution event"""
    def __init__(self,
                 event_type: str,
                 source: str,
                 workflow_id: str,
                 workflow_name: str,
                 step_id: Optional[str] = None,
                 step_name: Optional[str] = None,
                 status: str = "running",
                 **kwargs):
        super().__init__(
            event_type=event_type,
            source=source,
            data={
                "workflow_id": workflow_id,
                "workflow_name": workflow_name,
                "step_id": step_id,
                "step_name": step_name,
                "status": status,
                **kwargs
            },
            metadata={
                "category": EventCategory.WORKFLOW.value,
                "timestamp": datetime.utcnow().isoformat()
            }
        )


@dataclass
class SecurityEvent(Event):
    """Security-related event"""
    def __init__(self,
                 event_type: str,
                 source: str,
                 user_id: Optional[str] = None,
                 ip_address: Optional[str] = None,
                 action: str = "",
                 result: str = "",
                 reason: Optional[str] = None,
                 **kwargs):
        super().__init__(
            event_type=event_type,
            source=source,
            data={
                "user_id": user_id,
                "ip_address": ip_address,
                "action": action,
                "result": result,
                "reason": reason,
                **kwargs
            },
            metadata={
                "category": EventCategory.SECURITY.value,
                "timestamp": datetime.utcnow().isoformat()
            },
            priority=EventPriority.HIGH  # Security events are high priority
        )


# Event factory functions
def create_system_event(
    source: str,
    message: str,
    severity: str = "info",
    event_type: Optional[str] = None
) -> SystemEvent:
    """Create a system event"""
    if not event_type:
        event_type = EventType.SYSTEM_WARNING.value if severity == "warning" else EventType.SYSTEM_ERROR.value
    
    return SystemEvent(
        event_type=event_type,
        source=source,
        message=message,
        severity=severity
    )


def create_data_event(
    source: str,
    dataset_id: str,
    dataset_name: str,
    operation: str,
    record_count: Optional[int] = None,
    event_type: Optional[str] = None
) -> DataEvent:
    """Create a data event"""
    if not event_type:
        operation_map = {
            "create": EventType.DATA_CREATED.value,
            "update": EventType.DATA_UPDATED.value,
            "delete": EventType.DATA_DELETED.value,
            "ingest": EventType.DATA_INGESTION.value,
            "export": EventType.DATA_EXPORT.value
        }
        event_type = operation_map.get(operation, EventType.DATA_UPDATED.value)
    
    return DataEvent(
        event_type=event_type,
        source=source,
        dataset_id=dataset_id,
        dataset_name=dataset_name,
        operation=operation,
        record_count=record_count
    )


def create_model_event(
    source: str,
    model_id: str,
    model_name: str,
    event_type: str,
    model_version: Optional[str] = None,
    metrics: Optional[Dict[str, float]] = None
) -> ModelEvent:
    """Create a model event"""
    return ModelEvent(
        event_type=event_type,
        source=source,
        model_id=model_id,
        model_name=model_name,
        model_version=model_version,
        metrics=metrics
    )


def create_processing_event(
    source: str,
    job_id: str,
    job_name: str,
    status: str,
    progress: Optional[float] = None,
    error_message: Optional[str] = None,
    event_type: Optional[str] = None
) -> ProcessingEvent:
    """Create a processing event"""
    if not event_type:
        status_map = {
            "started": EventType.PROCESSING_STARTED.value,
            "completed": EventType.PROCESSING_COMPLETED.value,
            "failed": EventType.PROCESSING_FAILED.value,
            "progress": EventType.PROCESSING_PROGRESS.value
        }
        event_type = status_map.get(status, EventType.PROCESSING_PROGRESS.value)
    
    return ProcessingEvent(
        event_type=event_type,
        source=source,
        job_id=job_id,
        job_name=job_name,
        status=status,
        progress=progress,
        error_message=error_message
    )


def create_audit_event(
    source: str,
    user_id: str,
    user_name: str,
    action: str,
    resource_type: str,
    resource_id: str,
    details: Optional[Dict[str, Any]] = None
) -> AuditEvent:
    """Create an audit event"""
    action_map = {
        "create": EventType.AUDIT_CREATE.value,
        "update": EventType.AUDIT_UPDATE.value,
        "delete": EventType.AUDIT_DELETE.value,
        "access": EventType.AUDIT_ACCESS.value
    }
    event_type = action_map.get(action.lower(), EventType.AUDIT_ACCESS.value)
    
    return AuditEvent(
        event_type=event_type,
        source=source,
        user_id=user_id,
        user_name=user_name,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        details=details
    ) 