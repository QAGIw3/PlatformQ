"""
Enhanced Event Framework for DataIntelligenceSuite

Provides unified event processing capabilities with support for multiple patterns.
"""

from .event_bus import EventBus, EventSubscription
from .event_processor import EventProcessor, EventConfig, EventResult
from .event_patterns import EventPattern, PatternMatcher
from .event_store import EventStore, EventQuery
from .event_sourcing import EventSourcingMixin, AggregateRoot
from .saga import SagaOrchestrator, SagaStep, CompensationStrategy

__all__ = [
    "EventBus",
    "EventSubscription",
    "EventProcessor",
    "EventConfig",
    "EventResult",
    "EventPattern",
    "PatternMatcher",
    "EventStore",
    "EventQuery",
    "EventSourcingMixin",
    "AggregateRoot",
    "SagaOrchestrator",
    "SagaStep",
    "CompensationStrategy",
] 