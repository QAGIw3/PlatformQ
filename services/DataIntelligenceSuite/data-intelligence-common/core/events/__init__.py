"""
Unified Event Framework for DataIntelligenceSuite

Provides comprehensive event processing capabilities with support for multiple patterns.
"""

# Base components
from .base import (
    Event,
    EventPriority,
    EventDeliveryMode,
    EventProcessingMode,
    EventHandler,
    EventProcessingConfig,
    EventRouter,
    BaseEventProcessor
)

# Event bus
from .bus import (
    UnifiedEventBus,
    EventSubscription,
    SubscriptionType,
    EventBackend,
    PulsarBackend
)

# Event models
from .models import (
    EventCategory,
    EventStatus,
    EventType,
    SystemEvent,
    DataEvent,
    ModelEvent,
    ProcessingEvent,
    AuditEvent,
    NotificationEvent,
    WorkflowEvent,
    SecurityEvent,
    create_system_event,
    create_data_event,
    create_model_event,
    create_processing_event,
    create_audit_event
)

# Event patterns
from .event_patterns import EventPattern, PatternMatcher

# Event store
from .event_store import EventStore, EventQuery

# Event sourcing
from .event_sourcing import EventSourcingMixin, AggregateRoot

# Saga pattern
from .saga import SagaOrchestrator, SagaStep, CompensationStrategy

# Backward compatibility aliases
EventBus = UnifiedEventBus
EventProcessor = BaseEventProcessor
EventConfig = EventProcessingConfig

__all__ = [
    # Base
    "Event",
    "EventPriority",
    "EventDeliveryMode",
    "EventProcessingMode",
    "EventHandler",
    "EventProcessingConfig",
    "EventRouter",
    "BaseEventProcessor",
    
    # Bus
    "UnifiedEventBus",
    "EventBus",  # Alias
    "EventSubscription",
    "SubscriptionType",
    "EventBackend",
    "PulsarBackend",
    
    # Models
    "EventCategory",
    "EventStatus",
    "EventType",
    "SystemEvent",
    "DataEvent",
    "ModelEvent",
    "ProcessingEvent",
    "AuditEvent",
    "NotificationEvent",
    "WorkflowEvent",
    "SecurityEvent",
    "create_system_event",
    "create_data_event",
    "create_model_event",
    "create_processing_event",
    "create_audit_event",
    
    # Patterns
    "EventPattern",
    "PatternMatcher",
    
    # Store
    "EventStore",
    "EventQuery",
    
    # Sourcing
    "EventSourcingMixin",
    "AggregateRoot",
    
    # Saga
    "SagaOrchestrator",
    "SagaStep",
    "CompensationStrategy",
    
    # Aliases for backward compatibility
    "EventProcessor",
    "EventConfig"
] 