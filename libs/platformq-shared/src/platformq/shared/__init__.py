# This file makes 'shared_lib' a Python package
from .base_service import create_base_app, setup_structured_logging, setup_observability
from .event_publisher import EventPublisher
from .security import get_current_tenant_and_user
from .config import get_settings, Settings
from .db import CassandraSessionManager
from .repository import (
    AbstractRepository,
    BaseRepository,
    AsyncBaseRepository,
    CassandraRepository,
    QueryBuilder
)
from .event_framework import (
    EventProcessor, 
    BatchEventProcessor, 
    CompositeEventProcessor,
    event_handler,
    ProcessingStatus,
    ProcessingResult
)
from .service_client import (
    ServiceClient,
    ServiceClients,
    ServiceClientError,
    ServiceTimeoutError,
    get_service_client,
    close_all_clients,
    ServiceClientContext
)
from .error_handling import (
    ErrorCode,
    ErrorDetail,
    ErrorResponse,
    ServiceError,
    ValidationError,
    NotFoundError,
    AuthenticationError,
    AuthorizationError,
    ConflictError,
    RateLimitError,
    DependencyError,
    BusinessRuleError,
    add_error_handlers,
    ErrorMiddleware
)

__all__ = [
    'create_base_app',
    'setup_structured_logging', 
    'setup_observability',
    'EventPublisher',
    'get_current_tenant_and_user',
    'get_settings',
    'Settings',
    'CassandraSessionManager',
    'AbstractRepository',
    'BaseRepository',
    'AsyncBaseRepository',
    'CassandraRepository',
    'QueryBuilder',
    'EventProcessor',
    'BatchEventProcessor', 
    'CompositeEventProcessor',
    'event_handler',
    'ProcessingStatus',
    'ProcessingResult',
    'ServiceClient',
    'ServiceClients',
    'ServiceClientError',
    'ServiceTimeoutError',
    'get_service_client',
    'close_all_clients',
    'ServiceClientContext',
    'ErrorCode',
    'ErrorDetail',
    'ErrorResponse',
    'ServiceError',
    'ValidationError',
    'NotFoundError',
    'AuthenticationError',
    'AuthorizationError',
    'ConflictError',
    'RateLimitError',
    'DependencyError',
    'BusinessRuleError',
    'add_error_handlers',
    'ErrorMiddleware'
]
