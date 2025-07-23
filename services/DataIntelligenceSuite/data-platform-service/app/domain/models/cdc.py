"""
CDC Domain Models

Domain entities for Change Data Capture functionality
"""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class CDCStatus(str, Enum):
    """CDC source status"""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    FAILED = "failed"
    UNHEALTHY = "unhealthy"


class CDCSource(BaseModel):
    """CDC source entity"""
    id: str
    name: str
    source_type: str
    tables: List[str]
    mode: str
    status: CDCStatus
    created_at: datetime
    updated_at: Optional[datetime] = None
    stopped_at: Optional[datetime] = None
    metrics: Dict[str, Any] = Field(default_factory=dict)
    retry_count: int = 0
    error_message: Optional[str] = None
    
    class Config:
        use_enum_values = True


class CDCEvent(BaseModel):
    """CDC event entity"""
    id: str
    source_id: str
    event_type: str
    table_name: str
    operation: str  # INSERT, UPDATE, DELETE
    timestamp: datetime
    before: Optional[Dict[str, Any]] = None
    after: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CDCMetrics(BaseModel):
    """CDC metrics entity"""
    source_id: str
    timestamp: datetime
    events_processed: int
    bytes_processed: int
    latency_ms: float
    error_count: int
    lag_seconds: float
    throughput_eps: float  # events per second
    
    
class CDCConfiguration(BaseModel):
    """CDC configuration entity"""
    source_type: str
    connection_config: Dict[str, Any]
    tables: List[str]
    mode: str
    batch_size: int = 1000
    parallelism: int = 4
    schema_evolution: bool = True
    optimization: Optional[Dict[str, Any]] = None 