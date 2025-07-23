"""
Change Data Capture (CDC) patterns for real-time data synchronization.

Provides abstractions for capturing and processing data changes.
"""

from typing import Dict, List, Any, Optional, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from abc import ABC, abstractmethod
import asyncio
import json

from ...monitoring import StructuredLogger
from ..events import EventBus, Event

logger = StructuredLogger.get_logger(__name__)


class CDCEventType(str, Enum):
    """Types of CDC events"""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    TRUNCATE = "truncate"
    SCHEMA_CHANGE = "schema_change"
    SNAPSHOT = "snapshot"


class CDCSourceType(str, Enum):
    """Types of CDC sources"""
    DATABASE_LOG = "database_log"      # WAL, binlog, etc.
    POLLING = "polling"                # Query-based CDC
    TRIGGERS = "triggers"              # Database triggers
    STREAM = "stream"                  # Message stream
    WEBHOOK = "webhook"                # HTTP callbacks


@dataclass
class CDCEvent:
    """Change Data Capture event"""
    event_id: str
    event_type: CDCEventType
    source: str
    table: str
    timestamp: datetime
    
    # Change data
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    
    # Metadata
    transaction_id: Optional[str] = None
    position: Optional[str] = None  # Log position, offset, etc.
    schema_version: Optional[int] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "source": self.source,
            "table": self.table,
            "timestamp": self.timestamp.isoformat(),
            "before": self.before,
            "after": self.after,
            "transaction_id": self.transaction_id,
            "position": self.position,
            "schema_version": self.schema_version,
            "metadata": self.metadata
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CDCEvent":
        """Create from dictionary"""
        return cls(
            event_id=data["event_id"],
            event_type=CDCEventType(data["event_type"]),
            source=data["source"],
            table=data["table"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            before=data.get("before"),
            after=data.get("after"),
            transaction_id=data.get("transaction_id"),
            position=data.get("position"),
            schema_version=data.get("schema_version"),
            metadata=data.get("metadata", {})
        )


@dataclass
class CDCPosition:
    """CDC position for resuming from last processed event"""
    source: str
    position: str  # Implementation-specific position
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "source": self.source,
            "position": self.position,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata
        }


@dataclass
class CDCMetrics:
    """CDC processing metrics"""
    events_processed: int = 0
    events_failed: int = 0
    lag_seconds: Optional[float] = None
    last_event_time: Optional[datetime] = None
    processing_rate: float = 0.0  # Events per second
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "events_processed": self.events_processed,
            "events_failed": self.events_failed,
            "lag_seconds": self.lag_seconds,
            "last_event_time": self.last_event_time.isoformat() if self.last_event_time else None,
            "processing_rate": self.processing_rate
        }


class CDCHandler(ABC):
    """Abstract handler for CDC events"""
    
    @abstractmethod
    async def handle(self, event: CDCEvent) -> bool:
        """
        Handle CDC event.
        
        Args:
            event: CDC event to handle
            
        Returns:
            True if handled successfully
        """
        pass
        
    @abstractmethod
    def can_handle(self, event: CDCEvent) -> bool:
        """
        Check if handler can process this event.
        
        Args:
            event: CDC event
            
        Returns:
            True if handler can process event
        """
        pass


class BaseCDCProcessor(ABC):
    """
    Abstract base class for CDC processors.
    
    Features:
    - Event capture and processing
    - Position tracking for resumption
    - Error handling and retries
    - Metrics collection
    - Event filtering
    """
    
    def __init__(
        self,
        source_name: str,
        event_bus: Optional[EventBus] = None,
        batch_size: int = 100,
        batch_timeout: float = 1.0
    ):
        self.source_name = source_name
        self.event_bus = event_bus
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        
        # Handlers
        self._handlers: List[CDCHandler] = []
        
        # State
        self._running = False
        self._position: Optional[CDCPosition] = None
        self._metrics = CDCMetrics()
        
        # Filters
        self._table_filters: Set[str] = set()
        self._event_type_filters: Set[CDCEventType] = set()
        
        # Tasks
        self._capture_task: Optional[asyncio.Task] = None
        self._process_task: Optional[asyncio.Task] = None
        
        # Queues
        self._event_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        
    async def start(self, resume_position: Optional[CDCPosition] = None):
        """
        Start CDC processing.
        
        Args:
            resume_position: Optional position to resume from
        """
        if self._running:
            logger.warning(f"CDC processor {self.source_name} already running")
            return
            
        logger.info(f"Starting CDC processor: {self.source_name}")
        
        self._running = True
        self._position = resume_position
        
        # Initialize implementation
        await self._initialize_impl()
        
        # Start tasks
        self._capture_task = asyncio.create_task(self._capture_loop())
        self._process_task = asyncio.create_task(self._process_loop())
        
        # Publish event
        if self.event_bus:
            await self.event_bus.publish(Event(
                type="cdc.processor.started",
                source="cdc_processor",
                data={
                    "source": self.source_name,
                    "resume_position": resume_position.to_dict() if resume_position else None
                }
            ))
            
        logger.info(f"CDC processor started: {self.source_name}")
        
    async def stop(self):
        """Stop CDC processing"""
        if not self._running:
            return
            
        logger.info(f"Stopping CDC processor: {self.source_name}")
        
        self._running = False
        
        # Cancel tasks
        if self._capture_task:
            self._capture_task.cancel()
        if self._process_task:
            self._process_task.cancel()
            
        # Wait for tasks
        await asyncio.gather(
            self._capture_task,
            self._process_task,
            return_exceptions=True
        )
        
        # Shutdown implementation
        await self._shutdown_impl()
        
        # Publish event
        if self.event_bus:
            await self.event_bus.publish(Event(
                type="cdc.processor.stopped",
                source="cdc_processor",
                data={
                    "source": self.source_name,
                    "metrics": self._metrics.to_dict()
                }
            ))
            
        logger.info(f"CDC processor stopped: {self.source_name}")
        
    @abstractmethod
    async def _initialize_impl(self):
        """Initialize implementation-specific components"""
        pass
        
    @abstractmethod
    async def _shutdown_impl(self):
        """Shutdown implementation-specific components"""
        pass
        
    def add_handler(self, handler: CDCHandler):
        """
        Add event handler.
        
        Args:
            handler: CDC event handler
        """
        self._handlers.append(handler)
        logger.info(f"Added CDC handler: {handler.__class__.__name__}")
        
    def add_table_filter(self, table: str):
        """
        Add table filter.
        
        Args:
            table: Table name to include
        """
        self._table_filters.add(table)
        
    def add_event_type_filter(self, event_type: CDCEventType):
        """
        Add event type filter.
        
        Args:
            event_type: Event type to include
        """
        self._event_type_filters.add(event_type)
        
    async def _capture_loop(self):
        """Main capture loop"""
        while self._running:
            try:
                # Capture events
                events = await self._capture_events()
                
                if events:
                    # Filter events
                    filtered_events = self._filter_events(events)
                    
                    # Queue events for processing
                    for event in filtered_events:
                        await self._event_queue.put(event)
                        
                    # Update position
                    if filtered_events:
                        last_event = filtered_events[-1]
                        self._position = CDCPosition(
                            source=self.source_name,
                            position=last_event.position or "",
                            timestamp=last_event.timestamp
                        )
                        
                else:
                    # No events, wait a bit
                    await asyncio.sleep(0.1)
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in CDC capture loop: {e}")
                await asyncio.sleep(1)  # Back off on error
                
    @abstractmethod
    async def _capture_events(self) -> List[CDCEvent]:
        """
        Capture events from source.
        
        Returns:
            List of captured events
        """
        pass
        
    def _filter_events(self, events: List[CDCEvent]) -> List[CDCEvent]:
        """Filter events based on configured filters"""
        filtered = []
        
        for event in events:
            # Table filter
            if self._table_filters and event.table not in self._table_filters:
                continue
                
            # Event type filter
            if self._event_type_filters and event.event_type not in self._event_type_filters:
                continue
                
            filtered.append(event)
            
        return filtered
        
    async def _process_loop(self):
        """Main processing loop"""
        batch: List[CDCEvent] = []
        last_batch_time = datetime.utcnow()
        
        while self._running:
            try:
                # Get event with timeout
                try:
                    event = await asyncio.wait_for(
                        self._event_queue.get(),
                        timeout=self.batch_timeout
                    )
                    batch.append(event)
                except asyncio.TimeoutError:
                    pass
                    
                # Process batch if ready
                should_process = (
                    len(batch) >= self.batch_size or
                    (datetime.utcnow() - last_batch_time).total_seconds() >= self.batch_timeout
                )
                
                if should_process and batch:
                    await self._process_batch(batch)
                    batch = []
                    last_batch_time = datetime.utcnow()
                    
            except asyncio.CancelledError:
                # Process remaining events
                if batch:
                    await self._process_batch(batch)
                break
            except Exception as e:
                logger.error(f"Error in CDC process loop: {e}")
                self._metrics.events_failed += len(batch)
                batch = []  # Clear failed batch
                
    async def _process_batch(self, events: List[CDCEvent]):
        """Process batch of events"""
        start_time = datetime.utcnow()
        success_count = 0
        
        for event in events:
            try:
                # Find handlers
                handlers = [h for h in self._handlers if h.can_handle(event)]
                
                if not handlers:
                    logger.warning(f"No handler for event: {event.event_type} on {event.table}")
                    continue
                    
                # Process with all handlers
                handled = True
                for handler in handlers:
                    if not await handler.handle(event):
                        handled = False
                        break
                        
                if handled:
                    success_count += 1
                else:
                    self._metrics.events_failed += 1
                    
            except Exception as e:
                logger.error(f"Error processing CDC event: {e}")
                self._metrics.events_failed += 1
                
        # Update metrics
        self._metrics.events_processed += success_count
        
        if events:
            self._metrics.last_event_time = events[-1].timestamp
            
            # Calculate lag
            current_time = datetime.utcnow()
            event_time = events[-1].timestamp
            self._metrics.lag_seconds = (current_time - event_time).total_seconds()
            
        # Calculate processing rate
        duration = (datetime.utcnow() - start_time).total_seconds()
        if duration > 0:
            self._metrics.processing_rate = len(events) / duration
            
        # Publish metrics event
        if self.event_bus and len(events) > 0:
            await self.event_bus.publish(Event(
                type="cdc.batch.processed",
                source="cdc_processor",
                data={
                    "source": self.source_name,
                    "batch_size": len(events),
                    "success_count": success_count,
                    "duration_ms": duration * 1000,
                    "lag_seconds": self._metrics.lag_seconds
                }
            ))
            
    async def get_position(self) -> Optional[CDCPosition]:
        """
        Get current CDC position.
        
        Returns:
            Current position or None
        """
        return self._position
        
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get CDC metrics.
        
        Returns:
            Metrics dictionary
        """
        return self._metrics.to_dict()
        
    async def save_position(self, storage_key: str):
        """
        Save current position to storage.
        
        Args:
            storage_key: Storage key for position
        """
        if self._position:
            await self._save_position_impl(storage_key, self._position)
            
    @abstractmethod
    async def _save_position_impl(self, key: str, position: CDCPosition):
        """Save position to storage"""
        pass
        
    async def load_position(self, storage_key: str) -> Optional[CDCPosition]:
        """
        Load position from storage.
        
        Args:
            storage_key: Storage key for position
            
        Returns:
            Saved position or None
        """
        return await self._load_position_impl(storage_key)
        
    @abstractmethod
    async def _load_position_impl(self, key: str) -> Optional[CDCPosition]:
        """Load position from storage"""
        pass 