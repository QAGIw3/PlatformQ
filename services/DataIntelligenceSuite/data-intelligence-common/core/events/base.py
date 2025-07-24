"""
Unified base event processing for DataIntelligenceSuite.

This module consolidates all event-related base classes and models from:
- core/events/base.py (original)
- core/events/event_bus.py
- core/events/models.py
- Various event handlers
"""

from typing import Dict, Any, Callable, List, Optional, Set, Union, AsyncIterator, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from enum import Enum
import asyncio
import logging
import json
import uuid

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ...monitoring import MetricsCollector, StructuredLogger
from ..caching import CacheManager

logger = StructuredLogger.get_logger(__name__)


# Enums

class EventPriority(str, Enum):
    """Event priority levels"""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


class EventDeliveryMode(str, Enum):
    """Event delivery guarantees"""
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


class EventProcessingMode(str, Enum):
    """Event processing modes"""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    WINDOWED = "windowed"
    STATEFUL = "stateful"
    STREAM = "stream"


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
    BUSINESS = "business"


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
    DATA_VALIDATION = "data.validation"
    
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
    PROCESSING_CANCELLED = "processing.cancelled"
    
    # Workflow events
    WORKFLOW_STARTED = "workflow.started"
    WORKFLOW_COMPLETED = "workflow.completed"
    WORKFLOW_FAILED = "workflow.failed"
    WORKFLOW_STEP_COMPLETED = "workflow.step.completed"
    WORKFLOW_CANCELLED = "workflow.cancelled"
    
    # Security events
    SECURITY_LOGIN = "security.login"
    SECURITY_LOGOUT = "security.logout"
    SECURITY_ACCESS_DENIED = "security.access_denied"
    SECURITY_TOKEN_EXPIRED = "security.token_expired"
    SECURITY_PERMISSION_CHANGED = "security.permission_changed"
    
    # Audit events
    AUDIT_CREATE = "audit.create"
    AUDIT_UPDATE = "audit.update"
    AUDIT_DELETE = "audit.delete"
    AUDIT_ACCESS = "audit.access"
    AUDIT_EXPORT = "audit.export"
    
    # Notification events
    NOTIFICATION_SENT = "notification.sent"
    NOTIFICATION_FAILED = "notification.failed"
    NOTIFICATION_DELIVERED = "notification.delivered"
    NOTIFICATION_READ = "notification.read"
    
    # Custom event type
    CUSTOM = "custom"


# Data classes

@dataclass
class Event:
    """
    Unified event model.
    
    Consolidates event models from various modules with all features.
    """
    # Identity
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    source: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Classification
    category: Optional[EventCategory] = None
    priority: EventPriority = EventPriority.NORMAL
    
    # Event data
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    headers: Dict[str, str] = field(default_factory=dict)
    
    # Routing and correlation
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    partition_key: Optional[str] = None
    
    # Processing
    status: EventStatus = EventStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    
    # Security
    encrypted: bool = False
    user_context: Optional[Dict[str, Any]] = None
    
    # Versioning
    version: str = "1.0"
    schema_version: str = "1.0"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "category": self.category.value if self.category else None,
            "priority": self.priority.value,
            "data": self.data,
            "metadata": self.metadata,
            "headers": self.headers,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "partition_key": self.partition_key,
            "status": self.status.value,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "encrypted": self.encrypted,
            "user_context": self.user_context,
            "version": self.version,
            "schema_version": self.schema_version
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Create from dictionary"""
        data = data.copy()
        
        # Convert string timestamps
        if isinstance(data.get('timestamp'), str):
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
            
        # Convert enums
        if data.get('category'):
            data['category'] = EventCategory(data['category'])
        if data.get('priority'):
            data['priority'] = EventPriority(data['priority'])
        if data.get('status'):
            data['status'] = EventStatus(data['status'])
            
        return cls(**data)
    
    def with_correlation(self, correlation_id: str) -> 'Event':
        """Create new event with correlation ID"""
        self.correlation_id = correlation_id
        return self
    
    def with_causation(self, causation_id: str) -> 'Event':
        """Create new event with causation ID"""
        self.causation_id = causation_id
        return self
    
    def is_expired(self, ttl: timedelta) -> bool:
        """Check if event is expired"""
        return datetime.utcnow() - self.timestamp > ttl


@dataclass
class EventHandler:
    """Event handler configuration"""
    handler_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    event_types: List[str] = field(default_factory=list)
    handler_func: Optional[Callable] = None
    
    # Processing
    processing_mode: EventProcessingMode = EventProcessingMode.SEQUENTIAL
    max_concurrent: int = 1
    timeout: timedelta = field(default_factory=lambda: timedelta(seconds=30))
    
    # Error handling
    retry_on_error: bool = True
    max_retries: int = 3
    retry_delay: timedelta = field(default_factory=lambda: timedelta(seconds=1))
    dead_letter_handler: Optional[Callable] = None
    
    # Filtering
    filter_func: Optional[Callable[[Event], bool]] = None
    
    # Metrics
    events_processed: int = 0
    events_failed: int = 0
    last_error: Optional[str] = None
    last_processed: Optional[datetime] = None


@dataclass
class EventProcessingConfig:
    """Event processing configuration"""
    name: str
    description: str = ""
    
    # Processing
    processing_mode: EventProcessingMode = EventProcessingMode.SEQUENTIAL
    batch_size: int = 100
    batch_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    
    # Windowing
    window_size: Optional[timedelta] = None
    window_slide: Optional[timedelta] = None
    
    # State management
    enable_state: bool = False
    state_backend: str = "memory"  # memory, ignite, cassandra
    checkpoint_interval: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    
    # Deduplication
    enable_deduplication: bool = True
    deduplication_window: timedelta = field(default_factory=lambda: timedelta(hours=24))
    
    # Error handling
    max_retries: int = 3
    retry_delay: timedelta = field(default_factory=lambda: timedelta(seconds=1))
    dead_letter_topic: Optional[str] = None
    
    # Security
    enable_encryption: bool = False
    enable_rbac: bool = True
    allowed_roles: List[str] = field(default_factory=list)
    
    # Monitoring
    enable_metrics: bool = True
    enable_tracing: bool = True
    
    # Resource limits
    max_memory_mb: Optional[int] = None
    max_cpu_percent: Optional[float] = None
    
    # Advanced
    parallelism: int = 1
    buffer_size: int = 1000
    enable_watermarks: bool = False
    watermark_interval: timedelta = field(default_factory=lambda: timedelta(seconds=10))


class EventRouter:
    """
    Routes events based on patterns and rules.
    
    Consolidates routing logic from various modules.
    """
    
    def __init__(
        self,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ):
        self.vault_client = vault_client
        self.consul_client = consul_client
        
        # Routing rules: pattern -> list of handlers
        self._routes: Dict[str, List[EventHandler]] = {}
        self._regex_routes: List[Tuple[str, List[EventHandler]]] = []
        
        # Dynamic routing from Consul
        self._dynamic_routes_enabled = consul_client is not None
        self._dynamic_routes: Dict[str, List[EventHandler]] = {}
        
    def add_route(self, pattern: str, handler: EventHandler):
        """Add routing rule"""
        if '*' in pattern or '?' in pattern:
            # Convert wildcard to regex
            import fnmatch
            regex_pattern = fnmatch.translate(pattern)
            self._regex_routes.append((regex_pattern, [handler]))
        else:
            if pattern not in self._routes:
                self._routes[pattern] = []
            self._routes[pattern].append(handler)
            
    def get_handlers(self, event: Event) -> List[EventHandler]:
        """Get handlers for event"""
        handlers = []
        
        # Exact match
        if event.event_type in self._routes:
            handlers.extend(self._routes[event.event_type])
            
        # Pattern match
        import re
        for pattern, pattern_handlers in self._regex_routes:
            if re.match(pattern, event.event_type):
                handlers.extend(pattern_handlers)
                
        # Dynamic routes
        if self._dynamic_routes_enabled:
            if event.event_type in self._dynamic_routes:
                handlers.extend(self._dynamic_routes[event.event_type])
                
        # Filter handlers
        filtered_handlers = []
        for handler in handlers:
            if handler.filter_func is None or handler.filter_func(event):
                filtered_handlers.append(handler)
                
        return filtered_handlers
        
    async def refresh_dynamic_routes(self):
        """Refresh dynamic routes from Consul"""
        if not self.consul_client:
            return
            
        try:
            # Get routing rules from Consul
            routes_data = await self.consul_client.get_value("event_routes")
            if routes_data:
                self._dynamic_routes = json.loads(routes_data)
                logger.info(f"Refreshed {len(self._dynamic_routes)} dynamic routes")
        except Exception as e:
            logger.error(f"Failed to refresh dynamic routes: {e}")


class BaseEventProcessor(ABC):
    """
    Unified base class for event processing.
    
    Consolidates functionality from all event processor implementations.
    """
    
    def __init__(
        self,
        config: EventProcessingConfig,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None,
        cache_manager: Optional[CacheManager] = None,
        metrics_collector: Optional[MetricsCollector] = None
    ):
        self.config = config
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.cache = cache_manager
        self.metrics = metrics_collector or MetricsCollector(config.name)
        
        # Event routing
        self.router = EventRouter(vault_client, consul_client)
        
        # Processing state
        self._state: Dict[str, Any] = {}
        self._windows: Dict[str, List[Event]] = {}
        self._processed_events: Set[str] = set() if config.enable_deduplication else None
        
        # Handlers
        self._handlers: Dict[str, EventHandler] = {}
        self._global_handlers: List[EventHandler] = []
        
        # Processing metrics
        self._processing_metrics: Dict[str, int] = {
            "events_processed": 0,
            "events_failed": 0,
            "events_skipped": 0,
            "events_retried": 0
        }
        
        # User context for RBAC
        self._user_context: Optional[Dict[str, Any]] = None
        
        # Background tasks
        self._tasks: Set[asyncio.Task] = set()
        self._running = False
        
    @abstractmethod
    async def process_event(self, event: Event) -> Optional[Any]:
        """Process single event - must be implemented by subclasses"""
        pass
        
    async def start(self):
        """Start event processor"""
        self._running = True
        
        # Start background tasks
        if self.config.enable_state and self.config.checkpoint_interval:
            task = asyncio.create_task(self._checkpoint_loop())
            self._tasks.add(task)
            
        if self._dynamic_routes_enabled:
            task = asyncio.create_task(self._route_refresh_loop())
            self._tasks.add(task)
            
        logger.info(f"Started event processor: {self.config.name}")
        
    async def stop(self):
        """Stop event processor"""
        self._running = False
        
        # Cancel background tasks
        for task in self._tasks:
            task.cancel()
            
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        # Save final checkpoint
        if self.config.enable_state:
            await self._save_checkpoint()
            
        logger.info(f"Stopped event processor: {self.config.name}")
        
    def add_handler(self, event_type: str, handler: Union[Callable, EventHandler]):
        """Add event handler"""
        if isinstance(handler, EventHandler):
            self._handlers[event_type] = handler
            self.router.add_route(event_type, handler)
        else:
            # Create handler wrapper
            event_handler = EventHandler(
                name=f"{event_type}_handler",
                event_types=[event_type],
                handler_func=handler
            )
            self._handlers[event_type] = event_handler
            self.router.add_route(event_type, event_handler)
            
    def add_global_handler(self, handler: Union[Callable, EventHandler]):
        """Add global handler for all events"""
        if isinstance(handler, EventHandler):
            self._global_handlers.append(handler)
        else:
            event_handler = EventHandler(
                name="global_handler",
                event_types=["*"],
                handler_func=handler
            )
            self._global_handlers.append(event_handler)
            
    async def handle_event(self, event: Event) -> List[Any]:
        """Handle event using registered handlers"""
        results = []
        
        # Check deduplication
        if self._processed_events is not None:
            if event.event_id in self._processed_events:
                self._processing_metrics["events_skipped"] += 1
                logger.debug(f"Skipping duplicate event: {event.event_id}")
                return results
                
        # Get handlers
        handlers = self.router.get_handlers(event)
        handlers.extend(self._global_handlers)
        
        if not handlers:
            # Use default processing
            try:
                result = await self.process_event(event)
                if result is not None:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error processing event {event.event_id}: {e}")
                self._processing_metrics["events_failed"] += 1
                
                # Retry logic
                if event.retry_count < event.max_retries:
                    event.retry_count += 1
                    event.status = EventStatus.RETRYING
                    await self._retry_event(event)
                else:
                    event.status = EventStatus.FAILED
                    await self._handle_dead_letter(event, str(e))
        else:
            # Process with handlers
            for handler in handlers:
                try:
                    result = await self._execute_handler(handler, event)
                    if result is not None:
                        results.append(result)
                except Exception as e:
                    logger.error(f"Handler {handler.name} failed for event {event.event_id}: {e}")
                    handler.events_failed += 1
                    handler.last_error = str(e)
                    
        # Mark as processed
        if self._processed_events is not None:
            self._processed_events.add(event.event_id)
            
        # Update metrics
        self._processing_metrics["events_processed"] += 1
        event.status = EventStatus.PROCESSED
        
        return results
        
    async def _execute_handler(self, handler: EventHandler, event: Event) -> Any:
        """Execute handler with timeout and error handling"""
        if handler.handler_func is None:
            return None
            
        try:
            # Apply timeout
            result = await asyncio.wait_for(
                handler.handler_func(event),
                timeout=handler.timeout.total_seconds()
            )
            
            # Update handler metrics
            handler.events_processed += 1
            handler.last_processed = datetime.utcnow()
            
            return result
            
        except asyncio.TimeoutError:
            raise TimeoutError(f"Handler {handler.name} timed out")
            
    async def _retry_event(self, event: Event):
        """Retry event processing"""
        self._processing_metrics["events_retried"] += 1
        
        # Wait before retry
        await asyncio.sleep(self.config.retry_delay.total_seconds())
        
        # Re-process
        await self.handle_event(event)
        
    async def _handle_dead_letter(self, event: Event, error: str):
        """Handle dead letter event"""
        event.status = EventStatus.DEAD_LETTER
        event.metadata["error"] = error
        event.metadata["failed_at"] = datetime.utcnow().isoformat()
        
        # Log to dead letter topic if configured
        if self.config.dead_letter_topic:
            logger.warning(f"Sending event {event.event_id} to dead letter topic")
            # Implementation would publish to dead letter topic
            
    async def _checkpoint_loop(self):
        """Periodically save checkpoints"""
        while self._running:
            try:
                await asyncio.sleep(self.config.checkpoint_interval.total_seconds())
                await self._save_checkpoint()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Checkpoint error: {e}")
                
    async def _save_checkpoint(self):
        """Save processing state"""
        if not self.config.enable_state:
            return
            
        checkpoint = {
            "processor": self.config.name,
            "timestamp": datetime.utcnow().isoformat(),
            "state": self._state,
            "metrics": self._processing_metrics,
            "processed_count": len(self._processed_events) if self._processed_events else 0
        }
        
        # Save to state backend
        if self.cache:
            await self.cache.put(
                f"checkpoint:{self.config.name}",
                checkpoint,
                ttl=timedelta(days=7)
            )
            
    async def _route_refresh_loop(self):
        """Periodically refresh dynamic routes"""
        while self._running:
            try:
                await asyncio.sleep(60)  # Refresh every minute
                await self.router.refresh_dynamic_routes()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Route refresh error: {e}")
                
    def get_metrics(self) -> Dict[str, Any]:
        """Get processing metrics"""
        return {
            **self._processing_metrics,
            "handlers": len(self._handlers),
            "state_size": len(self._state),
            "dedup_cache_size": len(self._processed_events) if self._processed_events else 0
        }


# Factory functions for creating specific event types

def create_system_event(
    event_type: str,
    source: str,
    message: str,
    level: str = "info",
    **kwargs
) -> Event:
    """Create system event"""
    return Event(
        event_type=event_type,
        source=source,
        category=EventCategory.SYSTEM,
        data={
            "message": message,
            "level": level,
            **kwargs
        }
    )


def create_data_event(
    event_type: str,
    source: str,
    entity_type: str,
    entity_id: str,
    operation: str,
    **kwargs
) -> Event:
    """Create data event"""
    return Event(
        event_type=event_type,
        source=source,
        category=EventCategory.DATA,
        data={
            "entity_type": entity_type,
            "entity_id": entity_id,
            "operation": operation,
            **kwargs
        }
    )


def create_model_event(
    event_type: str,
    source: str,
    model_id: str,
    model_name: str,
    **kwargs
) -> Event:
    """Create model event"""
    return Event(
        event_type=event_type,
        source=source,
        category=EventCategory.MODEL,
        data={
            "model_id": model_id,
            "model_name": model_name,
            **kwargs
        }
    )


def create_processing_event(
    event_type: str,
    source: str,
    job_id: str,
    status: str,
    **kwargs
) -> Event:
    """Create processing event"""
    return Event(
        event_type=event_type,
        source=source,
        category=EventCategory.PROCESSING,
        data={
            "job_id": job_id,
            "status": status,
            **kwargs
        }
    )


def create_audit_event(
    event_type: str,
    source: str,
    user_id: str,
    action: str,
    resource: str,
    **kwargs
) -> Event:
    """Create audit event"""
    return Event(
        event_type=event_type,
        source=source,
        category=EventCategory.AUDIT,
        priority=EventPriority.HIGH,
        data={
            "user_id": user_id,
            "action": action,
            "resource": resource,
            "timestamp": datetime.utcnow().isoformat(),
            **kwargs
        }
    )


# Export all public classes and functions
__all__ = [
    # Enums
    'EventPriority',
    'EventDeliveryMode',
    'EventProcessingMode',
    'EventCategory',
    'EventStatus',
    'EventType',
    
    # Classes
    'Event',
    'EventHandler',
    'EventProcessingConfig',
    'EventRouter',
    'BaseEventProcessor',
    
    # Factory functions
    'create_system_event',
    'create_data_event',
    'create_model_event',
    'create_processing_event',
    'create_audit_event'
] 