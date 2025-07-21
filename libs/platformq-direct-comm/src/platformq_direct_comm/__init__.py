"""PlatformQ Direct Communication Library."""

from .communicator import DirectCommunicator
from .message_types import MessageType, DirectMessage
from .exceptions import CommunicationError, TimeoutError

__version__ = "0.1.0"

__all__ = [
    "DirectCommunicator",
    "MessageType",
    "DirectMessage",
    "CommunicationError",
    "TimeoutError",
] 