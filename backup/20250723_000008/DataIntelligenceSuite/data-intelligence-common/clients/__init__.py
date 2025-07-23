"""
Data Intelligence Service Clients

Provides client implementations for internal service communication.
"""

from .base_client import BaseServiceClient, ClientConfig
from .auth_client import AuthServiceClient
from .catalog_client import CatalogServiceClient
from .analytics_client import AnalyticsServiceClient
from .ml_client import MLServiceClient
from .processing_client import ProcessingServiceClient

__all__ = [
    "BaseServiceClient",
    "ClientConfig",
    "AuthServiceClient",
    "CatalogServiceClient",
    "AnalyticsServiceClient",
    "MLServiceClient",
    "ProcessingServiceClient"
] 