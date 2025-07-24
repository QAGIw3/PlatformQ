"""
Standardized Metric Naming Conventions

Provides consistent metric naming across all services.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum


class MetricCategory(str, Enum):
    """Standard metric categories"""
    # Service level
    REQUEST = "request"
    RESPONSE = "response"
    ERROR = "error"
    
    # Resource level
    CONNECTION = "connection"
    POOL = "pool"
    CACHE = "cache"
    QUEUE = "queue"
    
    # Processing level
    PROCESSING = "processing"
    ALGORITHM = "algorithm"
    PIPELINE = "pipeline"
    BATCH = "batch"
    STREAM = "stream"
    
    # Data level
    DATA = "data"
    RECORD = "record"
    EVENT = "event"
    MESSAGE = "message"
    
    # System level
    SYSTEM = "system"
    RESOURCE = "resource"
    HEALTH = "health"
    
    # Business level
    BUSINESS = "business"
    TRANSACTION = "transaction"
    WORKFLOW = "workflow"


class MetricUnit(str, Enum):
    """Standard metric units"""
    # Time
    NANOSECONDS = "ns"
    MICROSECONDS = "us"
    MILLISECONDS = "ms"
    SECONDS = "s"
    MINUTES = "min"
    HOURS = "h"
    
    # Size
    BYTES = "B"
    KILOBYTES = "KB"
    MEGABYTES = "MB"
    GIGABYTES = "GB"
    
    # Count
    COUNT = "1"  # Dimensionless
    PERCENT = "%"
    RATIO = "ratio"
    
    # Rate
    PER_SECOND = "/s"
    PER_MINUTE = "/min"
    PER_HOUR = "/h"
    
    # Resource
    CONNECTIONS = "connections"
    THREADS = "threads"
    PROCESSES = "processes"
    
    # Currency
    CREDITS = "credits"
    TOKENS = "tokens"


@dataclass
class MetricNamingConvention:
    """Standard metric naming convention"""
    # Format: {prefix}_{category}_{operation}_{subject}_{suffix}
    # Example: platformq_request_duration_api_histogram
    
    prefix: str = "platformq"
    category: MetricCategory = MetricCategory.REQUEST
    operation: str = ""  # e.g., "duration", "count", "size"
    subject: str = ""    # e.g., "api", "database", "cache"
    suffix: str = ""     # e.g., "total", "rate", "histogram"
    
    def build(self) -> str:
        """Build the metric name"""
        parts = [self.prefix]
        
        if self.category:
            parts.append(self.category.value)
            
        if self.operation:
            parts.append(self.operation)
            
        if self.subject:
            parts.append(self.subject)
            
        if self.suffix:
            parts.append(self.suffix)
            
        return "_".join(parts)


class StandardMetrics:
    """Standard metric definitions for common use cases"""
    
    # Request metrics
    REQUEST_TOTAL = MetricNamingConvention(
        category=MetricCategory.REQUEST,
        operation="count",
        suffix="total"
    ).build()
    
    REQUEST_DURATION = MetricNamingConvention(
        category=MetricCategory.REQUEST,
        operation="duration",
        suffix="histogram"
    ).build()
    
    REQUEST_SIZE = MetricNamingConvention(
        category=MetricCategory.REQUEST,
        operation="size",
        suffix="histogram"
    ).build()
    
    # Response metrics
    RESPONSE_SIZE = MetricNamingConvention(
        category=MetricCategory.RESPONSE,
        operation="size",
        suffix="histogram"
    ).build()
    
    RESPONSE_TIME = MetricNamingConvention(
        category=MetricCategory.RESPONSE,
        operation="time",
        suffix="histogram"
    ).build()
    
    # Error metrics
    ERROR_TOTAL = MetricNamingConvention(
        category=MetricCategory.ERROR,
        operation="count",
        suffix="total"
    ).build()
    
    ERROR_RATE = MetricNamingConvention(
        category=MetricCategory.ERROR,
        operation="rate",
        suffix="gauge"
    ).build()
    
    # Connection metrics
    CONNECTION_ACTIVE = MetricNamingConvention(
        category=MetricCategory.CONNECTION,
        operation="active",
        suffix="gauge"
    ).build()
    
    CONNECTION_IDLE = MetricNamingConvention(
        category=MetricCategory.CONNECTION,
        operation="idle",
        suffix="gauge"
    ).build()
    
    CONNECTION_ERRORS = MetricNamingConvention(
        category=MetricCategory.CONNECTION,
        operation="errors",
        suffix="total"
    ).build()
    
    # Cache metrics
    CACHE_HIT = MetricNamingConvention(
        category=MetricCategory.CACHE,
        operation="hit",
        suffix="total"
    ).build()
    
    CACHE_MISS = MetricNamingConvention(
        category=MetricCategory.CACHE,
        operation="miss",
        suffix="total"
    ).build()
    
    CACHE_SIZE = MetricNamingConvention(
        category=MetricCategory.CACHE,
        operation="size",
        suffix="gauge"
    ).build()
    
    # Processing metrics
    PROCESSING_TIME = MetricNamingConvention(
        category=MetricCategory.PROCESSING,
        operation="time",
        suffix="histogram"
    ).build()
    
    PROCESSING_RECORDS = MetricNamingConvention(
        category=MetricCategory.PROCESSING,
        operation="records",
        suffix="total"
    ).build()
    
    PROCESSING_ERRORS = MetricNamingConvention(
        category=MetricCategory.PROCESSING,
        operation="errors",
        suffix="total"
    ).build()
    
    # Queue metrics
    QUEUE_SIZE = MetricNamingConvention(
        category=MetricCategory.QUEUE,
        operation="size",
        suffix="gauge"
    ).build()
    
    QUEUE_LATENCY = MetricNamingConvention(
        category=MetricCategory.QUEUE,
        operation="latency",
        suffix="histogram"
    ).build()
    
    # System metrics
    SYSTEM_CPU = MetricNamingConvention(
        category=MetricCategory.SYSTEM,
        operation="cpu",
        suffix="gauge"
    ).build()
    
    SYSTEM_MEMORY = MetricNamingConvention(
        category=MetricCategory.SYSTEM,
        operation="memory",
        suffix="gauge"
    ).build()
    
    SYSTEM_DISK = MetricNamingConvention(
        category=MetricCategory.SYSTEM,
        operation="disk",
        suffix="gauge"
    ).build()


class MetricTags:
    """Standard metric tags/labels"""
    
    # Service tags
    SERVICE = "service"
    VERSION = "version"
    ENVIRONMENT = "environment"
    REGION = "region"
    CLUSTER = "cluster"
    NODE = "node"
    POD = "pod"
    
    # Request tags
    METHOD = "method"
    ENDPOINT = "endpoint"
    PATH = "path"
    STATUS = "status"
    STATUS_CODE = "status_code"
    CLIENT = "client"
    USER = "user"
    
    # Error tags
    ERROR_TYPE = "error_type"
    ERROR_CODE = "error_code"
    SEVERITY = "severity"
    
    # Resource tags
    RESOURCE_TYPE = "resource_type"
    RESOURCE_NAME = "resource_name"
    DATABASE = "database"
    TABLE = "table"
    CACHE_NAME = "cache_name"
    QUEUE_NAME = "queue_name"
    
    # Processing tags
    ALGORITHM = "algorithm"
    PIPELINE = "pipeline"
    STAGE = "stage"
    MODE = "mode"
    
    # Business tags
    TENANT = "tenant"
    ORGANIZATION = "organization"
    PROJECT = "project"
    WORKFLOW = "workflow"
    TRANSACTION_TYPE = "transaction_type"


class MetricBuilder:
    """Helper for building metric names following conventions"""
    
    def __init__(self, prefix: str = "platformq"):
        self.prefix = prefix
        
    def build_name(
        self,
        category: MetricCategory,
        operation: str,
        subject: Optional[str] = None,
        suffix: Optional[str] = None
    ) -> str:
        """Build a metric name following conventions"""
        convention = MetricNamingConvention(
            prefix=self.prefix,
            category=category,
            operation=operation,
            subject=subject or "",
            suffix=suffix or ""
        )
        return convention.build()
        
    def request_metric(self, operation: str, subject: str = "api") -> str:
        """Build a request metric name"""
        return self.build_name(
            MetricCategory.REQUEST,
            operation,
            subject,
            self._get_suffix_for_operation(operation)
        )
        
    def error_metric(self, operation: str = "count", subject: Optional[str] = None) -> str:
        """Build an error metric name"""
        return self.build_name(
            MetricCategory.ERROR,
            operation,
            subject,
            "total" if operation == "count" else "gauge"
        )
        
    def processing_metric(self, operation: str, subject: str) -> str:
        """Build a processing metric name"""
        return self.build_name(
            MetricCategory.PROCESSING,
            operation,
            subject,
            self._get_suffix_for_operation(operation)
        )
        
    def resource_metric(
        self,
        resource_type: str,
        operation: str,
        subject: Optional[str] = None
    ) -> str:
        """Build a resource metric name"""
        category_map = {
            "connection": MetricCategory.CONNECTION,
            "cache": MetricCategory.CACHE,
            "queue": MetricCategory.QUEUE,
            "pool": MetricCategory.POOL
        }
        
        category = category_map.get(resource_type, MetricCategory.RESOURCE)
        return self.build_name(
            category,
            operation,
            subject,
            self._get_suffix_for_operation(operation)
        )
        
    def _get_suffix_for_operation(self, operation: str) -> str:
        """Get appropriate suffix based on operation"""
        if operation in ["count", "errors", "hit", "miss"]:
            return "total"
        elif operation in ["duration", "time", "latency", "size"]:
            return "histogram"
        elif operation in ["active", "idle", "current", "cpu", "memory"]:
            return "gauge"
        elif operation == "rate":
            return "rate"
        else:
            return ""
            
    @staticmethod
    def build_tags(**kwargs) -> Dict[str, str]:
        """Build metric tags ensuring consistent naming"""
        tags = {}
        
        # Map common variations to standard names
        tag_mapping = {
            "svc": MetricTags.SERVICE,
            "srv": MetricTags.SERVICE,
            "env": MetricTags.ENVIRONMENT,
            "err": MetricTags.ERROR_TYPE,
            "err_type": MetricTags.ERROR_TYPE,
            "status_code": MetricTags.STATUS_CODE,
            "http_status": MetricTags.STATUS_CODE,
            "db": MetricTags.DATABASE,
            "tbl": MetricTags.TABLE
        }
        
        for key, value in kwargs.items():
            # Normalize key
            normalized_key = tag_mapping.get(key, key)
            
            # Convert value to string
            tags[normalized_key] = str(value)
            
        return tags


# Global metric builder instance
metric_builder = MetricBuilder()


# Convenience functions
def request_metric(operation: str, subject: str = "api") -> str:
    """Get a standard request metric name"""
    return metric_builder.request_metric(operation, subject)


def error_metric(operation: str = "count", subject: Optional[str] = None) -> str:
    """Get a standard error metric name"""
    return metric_builder.error_metric(operation, subject)


def processing_metric(operation: str, subject: str) -> str:
    """Get a standard processing metric name"""
    return metric_builder.processing_metric(operation, subject)


def resource_metric(
    resource_type: str,
    operation: str,
    subject: Optional[str] = None
) -> str:
    """Get a standard resource metric name"""
    return metric_builder.resource_metric(resource_type, operation, subject)


def build_tags(**kwargs) -> Dict[str, str]:
    """Build standardized metric tags"""
    return MetricBuilder.build_tags(**kwargs)


# Export main components
__all__ = [
    'MetricCategory',
    'MetricUnit',
    'MetricNamingConvention',
    'StandardMetrics',
    'MetricTags',
    'MetricBuilder',
    'metric_builder',
    'request_metric',
    'error_metric',
    'processing_metric',
    'resource_metric',
    'build_tags'
] 