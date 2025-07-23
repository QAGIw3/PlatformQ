"""Messaging system configurations."""

from dataclasses import dataclass
from .base import MessagingConfig


@dataclass
class PulsarConfig(MessagingConfig):
    """Apache Pulsar configuration"""
    type: str = "pulsar"


@dataclass
class EventBusConfig(MessagingConfig):
    """Event bus configuration"""
    type: str = "eventbus"


@dataclass
class StreamingConfig(MessagingConfig):
    """Streaming configuration"""
    type: str = "streaming" 