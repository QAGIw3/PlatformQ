"""Custom exceptions for unified data access layer"""


class DataAccessError(Exception):
    """Base exception for data access errors"""
    pass


class NotFoundError(DataAccessError):
    """Raised when requested data is not found"""
    def __init__(self, model_type: str, identifier: str):
        self.model_type = model_type
        self.identifier = identifier
        super().__init__(f"{model_type} with id '{identifier}' not found")


class ValidationError(DataAccessError):
    """Raised when data validation fails"""
    def __init__(self, field: str, message: str):
        self.field = field
        self.message = message
        super().__init__(f"Validation error on field '{field}': {message}")


class ConsistencyError(DataAccessError):
    """Raised when data consistency checks fail"""
    pass


class ConnectionError(DataAccessError):
    """Raised when connection to data store fails"""
    def __init__(self, store_type: str, message: str):
        self.store_type = store_type
        super().__init__(f"Failed to connect to {store_type}: {message}")


class CacheError(DataAccessError):
    """Raised when cache operations fail"""
    pass


class QueryError(DataAccessError):
    """Raised when query execution fails"""
    pass


class TransactionError(DataAccessError):
    """Raised when transaction operations fail"""
    pass 