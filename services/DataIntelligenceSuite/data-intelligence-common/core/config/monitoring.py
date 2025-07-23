"""Monitoring configurations."""

from dataclasses import dataclass
from .base import ObservabilityConfig, BaseConfig


@dataclass
class MetricsConfig(BaseConfig):
    """Metrics configuration"""
    enabled: bool = True
    endpoint: str = "/metrics"


@dataclass
class TracingConfig(BaseConfig):
    """Tracing configuration"""
    enabled: bool = True
    endpoint: str = "http://localhost:4317"


@dataclass
class LoggingConfig(BaseConfig):
    """Logging configuration"""
    level: str = "INFO"
    format: str = "json"


@dataclass
class AlertingConfig(BaseConfig):
    """Alerting configuration"""
    enabled: bool = True
    webhook: str = "" 