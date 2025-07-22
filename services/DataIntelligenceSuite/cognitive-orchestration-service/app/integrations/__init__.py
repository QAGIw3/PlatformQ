"""Service integrations for Cognitive Orchestration Service"""

from .data_platform import DataPlatformClient
from .ml_platform import MLPlatformClient

__all__ = ["DataPlatformClient", "MLPlatformClient"] 