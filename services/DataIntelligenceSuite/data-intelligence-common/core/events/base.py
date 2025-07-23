"""
Unified base event processing for DataIntelligenceSuite.

Combines functionality from event_handlers and core/events modules.
"""

from typing import Dict, Any, Callable, List, Optional, Set, Union, AsyncIterator
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


class EventProcessingMode(Enum):
    """Event processing modes"""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    WINDOWED = "windowed"
    STATEFUL = "stateful"


@dataclass
class Event:
    """Unified event model"""
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    source: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Event data
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Routing and processing
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    priority: EventPriority = EventPriority.NORMAL
    
    # Security
    encrypted: bool = False
    user_context: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
            "data": self.data,
            "metadata": self.metadata,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "priority": self.priority.value,
            "encrypted": self.encrypted,
            "user_context": self.user_context
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Event':
        """Create from dictionary"""
        event = cls(
            event_id=data.get("event_id", str(uuid.uuid4())),
            event_type=data.get("event_type", ""),
            source=data.get("source", ""),
            data=data.get("data", {}),
            metadata=data.get("metadata", {}),
            correlation_id=data.get("correlation_id"),
            causation_id=data.get("causation_id"),
            encrypted=data.get("encrypted", False),
            user_context=data.get("user_context")
        )
        
        # Parse timestamp
        if "timestamp" in data:
            if isinstance(data["timestamp"], str):
                event.timestamp = datetime.fromisoformat(data["timestamp"])
            else:
                event.timestamp = data["timestamp"]
                
        # Parse priority
        if "priority" in data:
            event.priority = EventPriority(data["priority"])
            
        return event


@dataclass
class EventHandler:
    """Event handler registration"""
    handler_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = ""
    handler_func: Callable = None
    filter_func: Optional[Callable] = None
    priority: int = 0  # Higher priority handlers run first
    required_role: Optional[str] = None
    encrypt_response: bool = False
    processing_mode: EventProcessingMode = EventProcessingMode.SEQUENTIAL
    
    def __post_init__(self):
        if self.handler_func and not asyncio.iscoroutinefunction(self.handler_func):
            raise ValueError(f"Handler function must be async: {self.handler_func.__name__}")


@dataclass
class EventProcessingConfig:
    """Configuration for event processing"""
    name: str
    processing_mode: EventProcessingMode = EventProcessingMode.SEQUENTIAL
    
    # Parallel processing
    max_concurrent_events: int = 10
    
    # Windowing
    window_size: Optional[timedelta] = None
    window_slide: Optional[timedelta] = None
    
    # State management
    enable_state: bool = False
    state_ttl: Optional[timedelta] = None
    
    # Event routing
    routing_rules: Dict[str, str] = field(default_factory=dict)
    
    # Error handling
    error_topic: Optional[str] = None
    dead_letter_topic: Optional[str] = None
    max_retries: int = 3
    retry_delay: timedelta = field(default_factory=lambda: timedelta(seconds=5))
    
    # Deduplication
    enable_deduplication: bool = False
    dedup_window: timedelta = field(default_factory=lambda: timedelta(minutes=5))
    
    # Security
    enable_encryption: bool = True
    required_roles: List[str] = field(default_factory=list)
    
    # Monitoring
    enable_metrics: bool = True
    enable_tracing: bool = True


class EventRouter:
    """Routes events to appropriate handlers with security integration"""
    
    def __init__(self, vault_client: Optional[VaultClient] = None,
                 consul_client: Optional[ConsulClient] = None):
        self.handlers: Dict[str, List[EventHandler]] = {}
        self._handler_metrics: Dict[str, Dict[str, int]] = {}
        self.vault_client = vault_client
        self.consul_client = consul_client
        self._handler_config: Dict[str, Dict[str, Any]] = {}
        self._config_watcher_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize router with configuration from Consul"""
        if self.consul_client:
            await self._load_handler_config()
            self._config_watcher_task = asyncio.create_task(self._watch_config_changes())
            
    async def shutdown(self):
        """Shutdown router"""
        if self._config_watcher_task:
            self._config_watcher_task.cancel()
            try:
                await self._config_watcher_task
            except asyncio.CancelledError:
                pass
                
    async def _load_handler_config(self):
        """Load handler configuration from Consul"""
        try:
            config_data = await self.consul_client.kv_get("data-intelligence/event-handlers/config")
            if config_data:
                self._handler_config = json.loads(config_data)
                logger.info("Loaded handler configuration from Consul")
        except Exception as e:
            logger.error(f"Failed to load handler config: {e}")
            
    async def _watch_config_changes(self):
        """Watch for configuration changes in Consul"""
        while True:
            try:
                await asyncio.sleep(30)
                await self._load_handler_config()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error watching config: {e}")
                
    def register_handler(self, handler: EventHandler):
        """Register event handler"""
        if handler.event_type not in self.handlers:
            self.handlers[handler.event_type] = []
            
        self.handlers[handler.event_type].append(handler)
        
        # Sort by priority
        self.handlers[handler.event_type].sort(key=lambda h: h.priority, reverse=True)
        
        # Initialize metrics
        if handler.event_type not in self._handler_metrics:
            self._handler_metrics[handler.event_type] = {
                "processed": 0,
                "failed": 0,
                "skipped": 0
            }
            
        logger.info(f"Registered handler for event type: {handler.event_type}")
        
    def unregister_handler(self, handler_id: str):
        """Unregister event handler"""
        for event_type, handlers in self.handlers.items():
            self.handlers[event_type] = [h for h in handlers if h.handler_id != handler_id]
            
    async def route_event(self, event: Event) -> List[Any]:
        """Route event to appropriate handlers with security checks"""
        if event.event_type not in self.handlers:
            logger.debug(f"No handlers registered for event type: {event.event_type}")
            return []
            
        results = []
        handlers = self.handlers[event.event_type]
        
        # Get handler config from Consul if available
        handler_config = self._handler_config.get(event.event_type, {})
        
        for handler in handlers:
            try:
                # Check role requirements
                if handler.required_role and event.user_context:
                    user_roles = event.user_context.get("roles", [])
                    if handler.required_role not in user_roles:
                        logger.warning(f"User lacks required role {handler.required_role} for handler {handler.handler_func.__name__}")
                        self._handler_metrics[event.event_type]["skipped"] += 1
                        continue
                
                # Apply filter if provided
                if handler.filter_func and not handler.filter_func(event):
                    self._handler_metrics[event.event_type]["skipped"] += 1
                    continue
                    
                # Call handler
                result = await handler.handler_func(event)
                
                # Encrypt response if required
                if handler.encrypt_response and self.vault_client and result:
                    encryption_key = handler_config.get("encryption_key", "event-responses")
                    encrypted_result = await self.vault_client.transit_encrypt(
                        encryption_key,
                        json.dumps(result)
                    )
                    result = {"encrypted": encrypted_result["ciphertext"]}
                
                results.append(result)
                self._handler_metrics[event.event_type]["processed"] += 1
                
            except Exception as e:
                logger.error(f"Error in handler {handler.handler_func.__name__}: {e}")
                self._handler_metrics[event.event_type]["failed"] += 1
                
        return results
        
    def get_metrics(self) -> Dict[str, Dict[str, int]]:
        """Get handler metrics"""
        return self._handler_metrics.copy()


class BaseEventProcessor(ABC):
    """
    Unified base class for event processing with Vault/Consul integration.
    
    Combines functionality from event_handlers and core/events modules.
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
        
    async def initialize(self):
        """Initialize event processor"""
        await self.router.initialize()
        
        # Start background tasks
        if self.config.enable_state and self.config.state_ttl:
            task = asyncio.create_task(self._cleanup_state_loop())
            self._tasks.add(task)
            
        if self.config.enable_deduplication:
            task = asyncio.create_task(self._cleanup_dedup_loop())
            self._tasks.add(task)
            
        logger.info(f"Initialized event processor: {self.config.name}")
        
    async def shutdown(self):
        """Shutdown event processor"""
        # Cancel background tasks
        for task in self._tasks:
            task.cancel()
            
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            
        await self.router.shutdown()
        logger.info(f"Shutdown event processor: {self.config.name}")
        
    def set_user_context(self, context: Dict[str, Any]):
        """Set user context for role-based access control"""
        self._user_context = context
        
    def add_handler(self, event_type: str, handler: Callable,
                   filter_func: Optional[Callable] = None,
                   priority: int = 0,
                   required_role: Optional[str] = None,
                   encrypt_response: bool = False):
        """Add event handler for specific event type"""
        event_handler = EventHandler(
            event_type=event_type,
            handler_func=handler,
            filter_func=filter_func,
            priority=priority,
            required_role=required_role,
            encrypt_response=encrypt_response,
            processing_mode=self.config.processing_mode
        )
        self.router.register_handler(event_handler)
        
    @abstractmethod
    async def process_event(self, event: Event) -> Optional[Dict[str, Any]]:
        """Process a single event. Must be implemented by derived classes."""
        pass
        
    async def handle_event(self, event: Union[Event, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Main event handling entry point with security and processing modes"""
        # Convert dict to Event if needed
        if isinstance(event, dict):
            event = Event.from_dict(event)
            
        # Set user context
        if self._user_context:
            event.user_context = self._user_context
            
        logger.info(f"Processing event {event.event_id} of type {event.event_type}")
        
        try:
            # Decrypt event data if encrypted
            if event.encrypted and self.vault_client:
                decrypted_data = await self.vault_client.transit_decrypt(
                    self.config.routing_rules.get("encryption_key", "event-data"),
                    event.data["encrypted"]
                )
                event.data = json.loads(decrypted_data)
                event.encrypted = False
            
            # Check deduplication
            if self.config.enable_deduplication:
                if not await self._check_duplicate(event):
                    self._processing_metrics["events_skipped"] += 1
                    return None
                    
            # Process based on mode
            result = None
            if self.config.processing_mode == EventProcessingMode.WINDOWED:
                await self._add_to_window(event)
            else:
                result = await self._process_with_retry(event)
                
            self._processing_metrics["events_processed"] += 1
            
            # Publish result events if configured
            if result and hasattr(self, 'event_bus') and self.event_bus:
                await self._publish_result(event, result)
                
            return result
            
        except Exception as e:
            logger.error(f"Error processing event {event.event_id}: {e}")
            self._processing_metrics["events_failed"] += 1
            
            # Publish error event
            if hasattr(self, 'event_bus') and self.event_bus:
                await self._publish_error(event, e)
                
            raise
            
    async def _process_with_retry(self, event: Event) -> Optional[Dict[str, Any]]:
        """Process event with retry logic"""
        last_error = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                # Route to handlers
                results = await self.router.route_event(event)
                
                # If no specific handler, use generic processor
                if not results:
                    result = await self.process_event(event)
                    results = [result] if result else []
                    
                return results[0] if results else None
                
            except Exception as e:
                last_error = e
                if attempt < self.config.max_retries:
                    self._processing_metrics["events_retried"] += 1
                    await asyncio.sleep(self.config.retry_delay.total_seconds() * (2 ** attempt))
                else:
                    raise
                    
        raise last_error
        
    async def _check_duplicate(self, event: Event) -> bool:
        """Check if event is duplicate"""
        if event.event_id in self._processed_events:
            return False
            
        self._processed_events.add(event.event_id)
        return True
        
    async def _add_to_window(self, event: Event):
        """Add event to processing window"""
        window_key = self._get_window_key(event)
        
        if window_key not in self._windows:
            self._windows[window_key] = []
            
        self._windows[window_key].append(event)
        
        # Check if window is ready for processing
        if self._is_window_ready(window_key):
            await self._process_window(window_key)
            
    def _get_window_key(self, event: Event) -> str:
        """Get window key for event"""
        # Simple time-based windowing
        window_start = event.timestamp.replace(second=0, microsecond=0)
        return f"{event.event_type}:{window_start.isoformat()}"
        
    def _is_window_ready(self, window_key: str) -> bool:
        """Check if window is ready for processing"""
        # Simple size-based trigger
        return len(self._windows.get(window_key, [])) >= 100
        
    async def _process_window(self, window_key: str):
        """Process events in window"""
        events = self._windows.pop(window_key, [])
        if not events:
            return
            
        # Process window of events
        for event in events:
            await self._process_with_retry(event)
            
    async def _cleanup_state_loop(self):
        """Cleanup expired state entries"""
        while True:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                if self.config.state_ttl:
                    cutoff = datetime.utcnow() - self.config.state_ttl
                    # Cleanup logic here
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in state cleanup: {e}")
                
    async def _cleanup_dedup_loop(self):
        """Cleanup old deduplication entries"""
        while True:
            try:
                await asyncio.sleep(60)  # Every minute
                
                # Keep only recent event IDs
                # This is simplified - real implementation would use timestamp tracking
                if len(self._processed_events) > 10000:
                    self._processed_events.clear()
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in dedup cleanup: {e}")
                
    async def _publish_result(self, event: Event, result: Dict[str, Any]):
        """Publish result event"""
        result_event = Event(
            event_type=f"{event.event_type}.result",
            source=self.config.name,
            data=result,
            correlation_id=event.correlation_id,
            causation_id=event.event_id
        )
        
        if hasattr(self, 'event_bus'):
            await self.event_bus.publish(result_event)
            
    async def _publish_error(self, event: Event, error: Exception):
        """Publish error event"""
        error_event = Event(
            event_type=f"{event.event_type}.error",
            source=self.config.name,
            data={
                "error_type": type(error).__name__,
                "error_message": str(error),
                "original_event_id": event.event_id
            },
            correlation_id=event.correlation_id,
            causation_id=event.event_id,
            priority=EventPriority.HIGH
        )
        
        if hasattr(self, 'event_bus'):
            await self.event_bus.publish(error_event)
            
    def get_metrics(self) -> Dict[str, Any]:
        """Get processing metrics"""
        return {
            "processing": self._processing_metrics.copy(),
            "routing": self.router.get_metrics()
        } 