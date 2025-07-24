"""
Integration Client Plugins

This module contains plugin implementations for various external services,
organized by category for better maintainability.
"""

# Plugin categories
from . import data_stores
from . import messaging
from . import analytics
from . import orchestration
from . import storage
from . import governance
from . import quality
from . import realtime

__all__ = [
    "data_stores",
    "messaging", 
    "analytics",
    "orchestration",
    "storage",
    "governance",
    "quality",
    "realtime"
] 