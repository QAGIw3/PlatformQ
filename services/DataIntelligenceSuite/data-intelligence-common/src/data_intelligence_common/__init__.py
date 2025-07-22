"""
Data Intelligence Common Library

Provides shared components, patterns, and utilities for all DataIntelligenceSuite services.
"""

__version__ = "0.1.0"

# Import key components for easy access
from .base_service import DataIntelligenceBaseService, create_data_intelligence_app
from .vault_consul import VaultConsulIntegration, DataServiceConfig
from .monitoring import MetricsCollector, StructuredLogger
from .event_handlers import BaseEventProcessor, EventRouter

__all__ = [
    "DataIntelligenceBaseService",
    "create_data_intelligence_app",
    "VaultConsulIntegration",
    "DataServiceConfig",
    "MetricsCollector",
    "StructuredLogger",
    "BaseEventProcessor",
    "EventRouter",
] 