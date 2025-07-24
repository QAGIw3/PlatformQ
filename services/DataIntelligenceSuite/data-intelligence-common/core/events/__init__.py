"""
Unified event system for DataIntelligenceSuite.

Provides comprehensive event handling, processing, and routing capabilities.
"""

# Base event models and types
from .base import (
    Event, EventPriority, EventCategory, EventStatus, EventType,
    EventProcessingMode, EventDeliveryMode, EventHandler,
    EventProcessingConfig, EventRouter, BaseEventProcessor,
    create_system_event, create_data_event, create_model_event,
    create_processing_event, create_audit_event
)

# Specialized event types
from .models import (
    SystemEvent, DataEvent, ModelEvent, ProcessingEvent,
    AuditEvent, WorkflowEvent, SecurityEvent, NotificationEvent,
    IntegrationEvent
)

# Event bus and subscriptions
from .bus import (
    UnifiedEventBus, EventSubscription, SubscriptionType,
    EventBackend
)

# Event processing
from .event_processor import (
    EventConfig, EventResult, EventProcessor
)

# Event patterns
from .event_patterns import (
    EventPattern, EventFilter, EventAggregator, EventEnricher,
    EventSplitter, EventRouter as PatternRouter, EventTransformer
)

# Event sourcing
from .event_sourcing import (
    EventSourcedAggregate, EventStore, Snapshot, Command,
    CommandHandler, EventProjection
)

# Event store
from .event_store import (
    InMemoryEventStore, PersistentEventStore, EventQuery,
    EventStream
)

# Saga pattern
from .saga import (
    Saga, SagaStep, SagaContext, SagaOrchestrator,
    CompensationHandler
)

# Backward compatibility aliases
EventBus = UnifiedEventBus

__all__ = [
    # Base
    'Event', 'EventPriority', 'EventCategory', 'EventStatus', 'EventType',
    'EventProcessingMode', 'EventDeliveryMode', 'EventHandler',
    'EventProcessingConfig', 'EventRouter', 'BaseEventProcessor',
    'create_system_event', 'create_data_event', 'create_model_event',
    'create_processing_event', 'create_audit_event',
    
    # Models
    'SystemEvent', 'DataEvent', 'ModelEvent', 'ProcessingEvent',
    'AuditEvent', 'WorkflowEvent', 'SecurityEvent', 'NotificationEvent',
    'IntegrationEvent',
    
    # Bus
    'EventBus', 'UnifiedEventBus', 'EventSubscription', 'SubscriptionType',
    'EventBackend',
    
    # Processing
    'EventConfig', 'EventResult', 'EventProcessor',
    
    # Patterns
    'EventPattern', 'EventFilter', 'EventAggregator', 'EventEnricher',
    'EventSplitter', 'PatternRouter', 'EventTransformer',
    
    # Event Sourcing
    'EventSourcedAggregate', 'EventStore', 'Snapshot', 'Command',
    'CommandHandler', 'EventProjection',
    
    # Event Store
    'InMemoryEventStore', 'PersistentEventStore', 'EventQuery',
    'EventStream',
    
    # Saga
    'Saga', 'SagaStep', 'SagaContext', 'SagaOrchestrator',
    'CompensationHandler'
] 