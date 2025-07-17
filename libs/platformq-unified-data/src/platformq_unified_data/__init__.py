"""PlatformQ Unified Data Access Layer

A comprehensive data abstraction layer for unified access to all data stores.
"""

from .manager import UnifiedDataManager
from .repository import Repository, AsyncRepository
from .models import BaseModel
from .query import Query, QueryBuilder
from .cache import CacheManager
from .exceptions import (
    DataAccessError,
    NotFoundError,
    ValidationError,
    ConsistencyError,
    ConnectionError
)

__version__ = "0.1.0"

__all__ = [
    "UnifiedDataManager",
    "Repository",
    "AsyncRepository",
    "BaseModel",
    "Query",
    "QueryBuilder",
    "CacheManager",
    "DataAccessError",
    "NotFoundError",
    "ValidationError",
    "ConsistencyError",
    "ConnectionError"
] 