"""PlatformQ Direct Communication Library."""

from .communicator import DirectCommunicator
from .message_types import MessageType, DirectMessage
from .exceptions import CommunicationError, TimeoutError, ConnectionError, MessageError
from .circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState
from .message_batcher import MessageBatcher, BatchConfig
from .message_replay import MessageReplayStore, ReplayableMessage

__version__ = "0.2.0"

__all__ = [
    # Core
    "DirectCommunicator",
    "MessageType",
    "DirectMessage",
    
    # Exceptions
    "CommunicationError",
    "TimeoutError",
    "ConnectionError",
    "MessageError",
    
    # Circuit Breaker
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    
    # Batching
    "MessageBatcher",
    "BatchConfig",
    
    # Replay
    "MessageReplayStore",
    "ReplayableMessage",
] 