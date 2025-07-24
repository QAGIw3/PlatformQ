"""
Event System Backends

Provides pluggable backends for event processing.
"""

from .base_backend import EventBackend, EventBackendConfig, BackendType
from .pulsar_backend import PulsarBackend
from .kafka_backend import KafkaBackend
from .ignite_backend import IgniteEventBackend
from .nats_backend import NATSBackend

__all__ = [
    "EventBackend",
    "EventBackendConfig",
    "BackendType",
    "PulsarBackend",
    "KafkaBackend", 
    "IgniteEventBackend",
    "NATSBackend"
] 