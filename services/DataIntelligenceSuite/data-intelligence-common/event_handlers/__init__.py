"""
Event Handlers for DataIntelligenceSuite

Provides base classes and common patterns for event-driven processing.
"""

from .base import BaseEventProcessor, EventRouter, EventContext
from .common_handlers import (
    DataEventHandler,
    ModelEventHandler,
    SystemEventHandler,
    AuditEventHandler,
    NotificationHandler
)
from .event_models import (
    DataEvent,
    ModelEvent,
    SystemEvent,
    AuditEvent,
    NotificationEvent
)
from .event_types import (
    EventType,
    EventPriority,
    EventStatus,
    EventCategory
)
from .pulsar_bus import PulsarEventBus, PulsarConfig
from .event_store import EventStore, EventQuery, EventArchive
from .utils import (
    event_serializer,
    event_deserializer,
    generate_event_id,
    validate_event_schema
)

__all__ = [
    # Base
    "BaseEventProcessor",
    "EventRouter",
    "EventContext",
    
    # Handlers
    "DataEventHandler",
    "ModelEventHandler",
    "SystemEventHandler",
    "AuditEventHandler",
    "NotificationHandler",
    
    # Models
    "DataEvent",
    "ModelEvent",
    "SystemEvent",
    "AuditEvent",
    "NotificationEvent",
    
    # Types
    "EventType",
    "EventPriority",
    "EventStatus",
    "EventCategory",
    
    # Infrastructure
    "PulsarEventBus",
    "PulsarConfig",
    "EventStore",
    "EventQuery",
    "EventArchive",
    
    # Utils
    "event_serializer",
    "event_deserializer",
    "generate_event_id",
    "validate_event_schema"
] 