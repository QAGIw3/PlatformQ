"""
PlatformQ Event Framework

Enhanced event processing framework with standardized patterns for:
- Event processing with retry and error handling
- Dead letter queue management
- Event correlation and tracing
- Batch processing
- Event sourcing patterns
"""

from .base_processor import (
    BaseEventProcessor,
    EventProcessingResult,
    EventProcessingStatus,
    EventContext
)
from .decorators import (
    event_handler,
    retry_on_failure,
    batch_processor,
    rate_limited,
    circuit_breaker
)
from .patterns import (
    EventSourcingMixin,
    SagaOrchestrator,
    EventAggregator,
    EventProjector
)
from .retry import (
    RetryPolicy,
    ExponentialBackoff,
    LinearBackoff,
    FixedDelay
)
from .monitoring import (
    EventMetrics,
    EventTracer,
    MetricsExporter
)
from .store import (
    EventStore,
    EventSnapshot,
    EventReplay
)

__all__ = [
    # Base classes
    "BaseEventProcessor",
    "EventProcessingResult",
    "EventProcessingStatus",
    "EventContext",
    
    # Decorators
    "event_handler",
    "retry_on_failure",
    "batch_processor",
    "rate_limited",
    "circuit_breaker",
    
    # Patterns
    "EventSourcingMixin",
    "SagaOrchestrator",
    "EventAggregator",
    "EventProjector",
    
    # Retry policies
    "RetryPolicy",
    "ExponentialBackoff",
    "LinearBackoff",
    "FixedDelay",
    
    # Monitoring
    "EventMetrics",
    "EventTracer",
    "MetricsExporter",
    
    # Event store
    "EventStore",
    "EventSnapshot",
    "EventReplay"
]

__version__ = "2.0.0" 