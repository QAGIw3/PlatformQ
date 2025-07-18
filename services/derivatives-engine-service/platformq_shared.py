"""
Stub implementation of platformq_shared for testing
In production, this would be imported from libs/platformq-shared
"""

from enum import Enum
from typing import Optional, Dict, Any
from dataclasses import dataclass


class ProcessingStatus(Enum):
    """Processing status types"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class ProcessingResult:
    """Result of a processing operation"""
    status: ProcessingStatus
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class ServiceClient:
    """Mock service client for inter-service communication"""
    
    def __init__(
        self,
        service_name: str,
        circuit_breaker_threshold: int = 5,
        rate_limit: float = 100.0
    ):
        self.service_name = service_name
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.rate_limit = rate_limit
        
    async def call(self, endpoint: str, data: Dict) -> ProcessingResult:
        """Make a service call"""
        return ProcessingResult(
            status=ProcessingStatus.COMPLETED,
            data={"mock": "response"}
        ) 