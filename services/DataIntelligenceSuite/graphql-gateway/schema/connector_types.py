"""
GraphQL types for connectors and processors
"""

import strawberry
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum


@strawberry.enum
class ConnectorType(Enum):
    """Available connector types"""
    SUITECRM = "suitecrm"
    METASFRESH = "metasfresh"
    OPENSTREETMAP = "openstreetmap"
    WEBHOOK = "webhook"


@strawberry.enum
class ProcessorType(Enum):
    """Available processor types"""
    BLENDER = "blender"
    FREECAD = "freecad"
    MULTIMEDIA = "multimedia"
    OPENFOAM = "openfoam"
    FLIGHTGEAR = "flightgear"


@strawberry.enum
class JobStatus(Enum):
    """Job status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@strawberry.type
class ConnectorConfig:
    """Connector configuration"""
    connector_id: str
    type: ConnectorType
    schedule: Optional[str]
    config: strawberry.scalars.JSON
    tenant_id: str
    created_at: datetime
    last_sync_time: Optional[datetime]


@strawberry.type
class ConnectorStatus:
    """Connector status"""
    connector_id: str
    type: ConnectorType
    scheduled: bool
    schedule: Optional[str]
    job_id: Optional[str]
    last_sync_time: Optional[datetime]
    status: str


@strawberry.type
class ProcessorInfo:
    """Processor information"""
    type: ProcessorType
    supported_formats: List[str]
    spark_config: strawberry.scalars.JSON


@strawberry.type
class ProcessingJob:
    """File processing job"""
    job_id: str
    processor_type: ProcessorType
    status: JobStatus
    input_path: str
    output_path: Optional[str]
    created_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    metadata: strawberry.scalars.JSON
    error: Optional[str]


@strawberry.type
class BatchProcessingResult:
    """Batch processing result"""
    batch_job_id: str
    processor_type: ProcessorType
    num_files: int
    spark_job_id: str
    status: str


# Input types

@strawberry.input
class ConnectorConfigInput:
    """Input for creating/updating connector configuration"""
    type: ConnectorType
    schedule: Optional[str] = None
    config: strawberry.scalars.JSON = strawberry.field(default_factory=dict)


@strawberry.input
class ProcessFileInput:
    """Input for processing a file"""
    file_path: str
    output_path: Optional[str] = None
    options: strawberry.scalars.JSON = strawberry.field(default_factory=dict)


@strawberry.input
class ProcessBatchInput:
    """Input for batch processing files"""
    file_paths: List[str]
    output_path: Optional[str] = None
    options: strawberry.scalars.JSON = strawberry.field(default_factory=dict)


@strawberry.input
class WebhookPayloadInput:
    """Input for webhook payload"""
    webhook_type: str
    payload: strawberry.scalars.JSON 