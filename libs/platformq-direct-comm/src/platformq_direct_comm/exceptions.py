"""Exceptions for direct communication."""


class CommunicationError(Exception):
    """Base exception for communication errors."""
    pass


class TimeoutError(CommunicationError):
    """Raised when a communication operation times out."""
    pass


class MessageError(CommunicationError):
    """Raised when there's an issue with message format or processing."""
    pass


class ConnectionError(CommunicationError):
    """Raised when there's a connection issue."""
    pass 