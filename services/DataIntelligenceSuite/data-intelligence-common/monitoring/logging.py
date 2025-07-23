"""Structured logging for DataIntelligenceSuite services."""

from typing import Dict, Any, Optional, List, Union
import logging
import logging.config
import json
import sys
from datetime import datetime
from contextlib import contextmanager
import structlog
from pythonjsonlogger import jsonlogger


class StructuredLogger:
    """
    Structured logger for DataIntelligenceSuite services.
    
    Features:
    - JSON formatted logs
    - Request context tracking
    - Performance logging
    - Error tracking with context
    - Log correlation across services
    """
    
    def __init__(self, name: str, service_name: str):
        self.name = name
        self.service_name = service_name
        self.logger = structlog.get_logger(name)
        self._context: Dict[str, Any] = {
            "service": service_name
        }
        
    def bind(self, **kwargs) -> "StructuredLogger":
        """Bind additional context to the logger."""
        new_logger = StructuredLogger(self.name, self.service_name)
        new_logger._context = {**self._context, **kwargs}
        new_logger.logger = self.logger.bind(**kwargs)
        return new_logger
        
    def unbind(self, *keys) -> "StructuredLogger":
        """Remove context keys from the logger."""
        new_logger = StructuredLogger(self.name, self.service_name)
        new_logger._context = {k: v for k, v in self._context.items() if k not in keys}
        new_logger.logger = self.logger.unbind(*keys)
        return new_logger
        
    def _add_context(self, kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Add standard context to log entries."""
        context = {
            **self._context,
            "timestamp": datetime.utcnow().isoformat(),
            **kwargs
        }
        return context
        
    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self.logger.debug(message, **self._add_context(kwargs))
        
    def info(self, message: str, **kwargs):
        """Log info message."""
        self.logger.info(message, **self._add_context(kwargs))
        
    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self.logger.warning(message, **self._add_context(kwargs))
        
    def error(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """Log error message with optional exception."""
        context = self._add_context(kwargs)
        if exception:
            context["exception"] = str(exception)
            context["exception_type"] = type(exception).__name__
        self.logger.error(message, **context)
        
    def critical(self, message: str, exception: Optional[Exception] = None, **kwargs):
        """Log critical message with optional exception."""
        context = self._add_context(kwargs)
        if exception:
            context["exception"] = str(exception)
            context["exception_type"] = type(exception).__name__
        self.logger.critical(message, **context)
        
    @contextmanager
    def operation(self, operation_name: str, **kwargs):
        """Context manager for logging operations with timing."""
        start_time = datetime.utcnow()
        operation_id = f"{operation_name}_{start_time.timestamp()}"
        
        # Log start
        self.info(
            f"Operation started: {operation_name}",
            operation_id=operation_id,
            operation_name=operation_name,
            **kwargs
        )
        
        try:
            yield operation_id
            
            # Log success
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.info(
                f"Operation completed: {operation_name}",
                operation_id=operation_id,
                operation_name=operation_name,
                duration_seconds=duration,
                status="success",
                **kwargs
            )
            
        except Exception as e:
            # Log failure
            duration = (datetime.utcnow() - start_time).total_seconds()
            self.error(
                f"Operation failed: {operation_name}",
                exception=e,
                operation_id=operation_id,
                operation_name=operation_name,
                duration_seconds=duration,
                status="failure",
                **kwargs
            )
            raise
            
    def log_request(self, method: str, path: str, status: int, duration: float, **kwargs):
        """Log HTTP request."""
        self.info(
            "HTTP request",
            method=method,
            path=path,
            status=status,
            duration_seconds=duration,
            **kwargs
        )
        
    def log_database_query(self, database: str, query: str, duration: float, success: bool, **kwargs):
        """Log database query."""
        level = "info" if success else "error"
        getattr(self, level)(
            "Database query",
            database=database,
            query=query[:100],  # Truncate long queries
            duration_seconds=duration,
            success=success,
            **kwargs
        )
        
    def log_event(self, event_type: str, event_data: Dict[str, Any], **kwargs):
        """Log event processing."""
        self.info(
            "Event processed",
            event_type=event_type,
            event_data=event_data,
            **kwargs
        )


def setup_logging(
    service_name: str,
    log_level: str = "INFO",
    log_format: str = "json",
    log_file: Optional[str] = None
):
    """
    Set up logging configuration for DataIntelligenceSuite services.
    
    Args:
        service_name: Name of the service
        log_level: Logging level
        log_format: Log format (json or text)
        log_file: Optional log file path
    """
    
    # Configure structlog
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    
    if log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())
        
    structlog.configure(
        processors=processors,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Configure standard logging
    handlers = []
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    if log_format == "json":
        formatter = jsonlogger.JsonFormatter(
            "%(timestamp)s %(level)s %(name)s %(message)s",
            timestamp=True
        )
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    console_handler.setFormatter(formatter)
    handlers.append(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)
        
    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        handlers=handlers,
        force=True
    )
    
    # Set service name in all logs
    logging.LoggerAdapter(
        logging.getLogger(),
        {"service": service_name}
    )
    
    # Suppress noisy loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured for {service_name}", extra={"service": service_name})


def get_logger(name: str, service_name: Optional[str] = None) -> StructuredLogger:
    """
    Get a structured logger instance.
    
    Args:
        name: Logger name (usually __name__)
        service_name: Service name (uses environment variable if not provided)
        
    Returns:
        StructuredLogger instance
    """
    import os
    
    if not service_name:
        service_name = os.environ.get("SERVICE_NAME", "unknown")
        
    return StructuredLogger(name, service_name) 