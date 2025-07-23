"""
Base Event Backend Interface

Provides abstract interface for pluggable event backends.
"""

from typing import Any, Dict, List, Optional, Callable, AsyncIterator
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio

from ....monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class BackendType(str, Enum):
    """Supported event backend types"""
    PULSAR = "pulsar"
    KAFKA = "kafka"
    REDIS_STREAMS = "redis_streams"
    NATS = "nats"
    RABBITMQ = "rabbitmq"
    SQS = "sqs"


class DeliveryGuarantee(str, Enum):
    """Message delivery guarantees"""
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


@dataclass
class EventBackendConfig:
    """Base configuration for event backends"""
    backend_type: BackendType
    connection_url: str
    
    # Common settings
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    timeout_seconds: float = 30.0
    
    # Delivery semantics
    delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE
    
    # Performance
    batch_size: int = 100
    batch_timeout_ms: int = 100
    compression: Optional[str] = None
    
    # Security
    use_tls: bool = True
    auth_mechanism: Optional[str] = None
    credentials: Optional[Dict[str, Any]] = None
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Event:
    """Generic event structure"""
    id: str
    topic: str
    data: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    headers: Dict[str, str] = field(default_factory=dict)
    key: Optional[str] = None
    partition_key: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "topic": self.topic,
            "data": self.data,
            "timestamp": self.timestamp.isoformat(),
            "headers": self.headers,
            "key": self.key,
            "partition_key": self.partition_key
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Event":
        """Create from dictionary"""
        return cls(
            id=data["id"],
            topic=data["topic"],
            data=data["data"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            headers=data.get("headers", {}),
            key=data.get("key"),
            partition_key=data.get("partition_key")
        )


@dataclass
class PublishResult:
    """Result of publish operation"""
    success: bool
    message_id: Optional[str] = None
    partition: Optional[int] = None
    offset: Optional[int] = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ConsumerConfig:
    """Consumer configuration"""
    consumer_group: str
    topics: List[str]
    
    # Consumption settings
    auto_commit: bool = True
    commit_interval_ms: int = 5000
    max_poll_records: int = 500
    
    # Position
    start_from: str = "latest"  # latest, earliest, timestamp
    start_timestamp: Optional[datetime] = None
    
    # Processing
    enable_dead_letter: bool = True
    dead_letter_topic: Optional[str] = None
    max_redeliveries: int = 3


class EventBackend(ABC):
    """
    Abstract event backend interface.
    
    All event backends must implement this interface.
    """
    
    def __init__(self, config: EventBackendConfig):
        self.config = config
        self._connected = False
        self._consumers: Dict[str, asyncio.Task] = {}
        self._metrics = {
            "messages_published": 0,
            "messages_consumed": 0,
            "errors": 0,
            "last_error": None
        }
    
    @abstractmethod
    async def connect(self) -> None:
        """Connect to the event backend"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the event backend"""
        pass
    
    @abstractmethod
    async def publish(
        self,
        event: Event,
        timeout: Optional[float] = None
    ) -> PublishResult:
        """
        Publish a single event.
        
        Args:
            event: Event to publish
            timeout: Publish timeout
            
        Returns:
            Publish result
        """
        pass
    
    @abstractmethod
    async def publish_batch(
        self,
        events: List[Event],
        timeout: Optional[float] = None
    ) -> List[PublishResult]:
        """
        Publish multiple events.
        
        Args:
            events: Events to publish
            timeout: Publish timeout
            
        Returns:
            List of publish results
        """
        pass
    
    @abstractmethod
    async def subscribe(
        self,
        config: ConsumerConfig,
        handler: Callable[[Event], Any]
    ) -> str:
        """
        Subscribe to topics with a handler.
        
        Args:
            config: Consumer configuration
            handler: Event handler function
            
        Returns:
            Subscription ID
        """
        pass
    
    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> None:
        """
        Unsubscribe from topics.
        
        Args:
            subscription_id: Subscription to cancel
        """
        pass
    
    @abstractmethod
    async def consume_batch(
        self,
        config: ConsumerConfig,
        max_messages: int = 100,
        timeout: Optional[float] = None
    ) -> List[Event]:
        """
        Consume a batch of messages.
        
        Args:
            config: Consumer configuration
            max_messages: Maximum messages to consume
            timeout: Consume timeout
            
        Returns:
            List of events
        """
        pass
    
    @abstractmethod
    async def acknowledge(
        self,
        event: Event,
        success: bool = True
    ) -> None:
        """
        Acknowledge event processing.
        
        Args:
            event: Event to acknowledge
            success: Whether processing was successful
        """
        pass
    
    @abstractmethod
    async def create_topic(
        self,
        topic: str,
        partitions: int = 1,
        replication_factor: int = 1,
        config: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Create a topic.
        
        Args:
            topic: Topic name
            partitions: Number of partitions
            replication_factor: Replication factor
            config: Topic configuration
            
        Returns:
            Success status
        """
        pass
    
    @abstractmethod
    async def delete_topic(self, topic: str) -> bool:
        """
        Delete a topic.
        
        Args:
            topic: Topic name
            
        Returns:
            Success status
        """
        pass
    
    @abstractmethod
    async def list_topics(self) -> List[str]:
        """
        List all topics.
        
        Returns:
            List of topic names
        """
        pass
    
    @abstractmethod
    async def get_topic_info(self, topic: str) -> Dict[str, Any]:
        """
        Get topic information.
        
        Args:
            topic: Topic name
            
        Returns:
            Topic information
        """
        pass
    
    async def health_check(self) -> bool:
        """Check backend health"""
        try:
            # Default implementation - try to list topics
            await self.list_topics()
            return True
        except Exception:
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get backend metrics"""
        return self._metrics.copy()
    
    def _record_publish(self, success: bool):
        """Record publish metric"""
        if success:
            self._metrics["messages_published"] += 1
        else:
            self._metrics["errors"] += 1
    
    def _record_consume(self, count: int):
        """Record consume metric"""
        self._metrics["messages_consumed"] += count
    
    def _record_error(self, error: str):
        """Record error"""
        self._metrics["errors"] += 1
        self._metrics["last_error"] = error
    
    @abstractmethod
    async def stream(
        self,
        config: ConsumerConfig
    ) -> AsyncIterator[Event]:
        """
        Stream events as async iterator.
        
        Args:
            config: Consumer configuration
            
        Yields:
            Events
        """
        pass


class EventBackendFactory:
    """Factory for creating event backends"""
    
    _backends: Dict[BackendType, type] = {}
    
    @classmethod
    def register_backend(
        cls,
        backend_type: BackendType,
        backend_class: type
    ):
        """Register a backend implementation"""
        cls._backends[backend_type] = backend_class
    
    @classmethod
    def create_backend(
        cls,
        config: EventBackendConfig
    ) -> EventBackend:
        """Create a backend instance"""
        backend_class = cls._backends.get(config.backend_type)
        if not backend_class:
            raise ValueError(f"Unknown backend type: {config.backend_type}")
        
        return backend_class(config) 