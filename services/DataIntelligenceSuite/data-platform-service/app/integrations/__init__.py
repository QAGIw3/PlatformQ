"""
Service integrations for Data Platform Service

This package provides integration with other platform services:
- ML Platform Service: Training data preparation and feature engineering
- Event Router Service: Real-time data ingestion via event streams
- Analytics Service: Federated analytics and cross-service queries
- Storage Service: Backup, archival, and document conversion
"""

from .ml_platform import MLPlatformIntegration
from .event_router import EventRouterIntegration
from .analytics import AnalyticsIntegration
from .storage import StorageIntegration
from .service_orchestrator import ServiceOrchestrator

__all__ = [
    "MLPlatformIntegration",
    "EventRouterIntegration",
    "AnalyticsIntegration",
    "StorageIntegration",
    "ServiceOrchestrator"
] 