"""
Data processing models.

Provides models for batch and stream processing operations.
"""

import uuid
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field

from .base_models import TimestampedModel, VersionedModel, AuditedModel


class JobStatus(str, Enum):
    """Processing job status"""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"
    RETRYING = "retrying"


class ProcessingEngine(str, Enum):
    """Processing engines"""
    SPARK = "spark"
    FLINK = "flink"
    PYTHON = "python"
    SQL = "sql"
    CUSTOM = "custom"


class TriggerType(str, Enum):
    """Pipeline trigger types"""
    MANUAL = "manual"
    SCHEDULED = "scheduled"
    EVENT = "event"
    DATA_ARRIVAL = "data_arrival"
    CASCADE = "cascade"
    API = "api"


class DataFormat(str, Enum):
    """Data formats"""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    DELTA = "delta"
    ICEBERG = "iceberg"
    CUSTOM = "custom"


class StageType(str, Enum):
    """Pipeline stage types"""
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    JOIN = "join"
    SINK = "sink"
    VALIDATE = "validate"
    CUSTOM = "custom"


@dataclass
class DataSource:
    """Data source configuration"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    source_type: str = ""  # s3, database, kafka, api, etc.
    connection_config: Dict[str, Any] = field(default_factory=dict)
    format: DataFormat = DataFormat.JSON
    schema: Optional[Dict[str, Any]] = None
    partitions: Optional[List[str]] = None
    
    # Options
    incremental: bool = False
    watermark_column: Optional[str] = None
    batch_size: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "source_type": self.source_type,
            "connection_config": self.connection_config,
            "format": self.format.value,
            "schema": self.schema,
            "partitions": self.partitions,
            "incremental": self.incremental,
            "watermark_column": self.watermark_column,
            "batch_size": self.batch_size
        }


@dataclass
class DataSink:
    """Data sink configuration"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    sink_type: str = ""  # s3, database, kafka, api, etc.
    connection_config: Dict[str, Any] = field(default_factory=dict)
    format: DataFormat = DataFormat.PARQUET
    
    # Options
    mode: str = "append"  # append, overwrite, error_if_exists
    partitions: Optional[List[str]] = None
    compression: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "sink_type": self.sink_type,
            "connection_config": self.connection_config,
            "format": self.format.value,
            "mode": self.mode,
            "partitions": self.partitions,
            "compression": self.compression
        }


@dataclass
class Transform:
    """Data transformation"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    transform_type: str = ""  # sql, python, spark, custom
    config: Dict[str, Any] = field(default_factory=dict)
    
    # SQL transform
    sql: Optional[str] = None
    
    # Python transform
    function: Optional[str] = None
    code: Optional[str] = None
    
    # Dependencies
    dependencies: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "transform_type": self.transform_type,
            "config": self.config,
            "sql": self.sql,
            "function": self.function,
            "code": self.code,
            "dependencies": self.dependencies
        }


@dataclass
class PipelineStage(TimestampedModel):
    """Pipeline stage definition"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    stage_type: StageType = StageType.TRANSFORM
    
    # Configuration
    config: Dict[str, Any] = field(default_factory=dict)
    
    # Dependencies
    depends_on: List[str] = field(default_factory=list)  # Stage IDs
    
    # Processing
    engine: ProcessingEngine = ProcessingEngine.PYTHON
    transform: Optional[Transform] = None
    
    # Error handling
    retry_count: int = 3
    retry_delay: int = 60  # seconds
    error_handling: str = "fail"  # fail, skip, default
    
    # Monitoring
    timeout: Optional[int] = None  # seconds
    sla: Optional[int] = None  # seconds
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "stage_type": self.stage_type.value,
            "config": self.config,
            "depends_on": self.depends_on,
            "engine": self.engine.value,
            "transform": self.transform.to_dict() if self.transform else None,
            "retry_count": self.retry_count,
            "retry_delay": self.retry_delay,
            "error_handling": self.error_handling,
            "timeout": self.timeout,
            "sla": self.sla
        }


@dataclass
class Pipeline(VersionedModel):
    """Data processing pipeline"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: Optional[str] = None
    
    # Stages
    stages: List[PipelineStage] = field(default_factory=list)
    
    # Data flow
    sources: List[DataSource] = field(default_factory=list)
    sinks: List[DataSink] = field(default_factory=list)
    
    # Configuration
    engine: ProcessingEngine = ProcessingEngine.SPARK
    config: Dict[str, Any] = field(default_factory=dict)
    
    # Scheduling
    trigger_type: TriggerType = TriggerType.MANUAL
    schedule: Optional[str] = None  # Cron expression
    
    # Metadata
    owner: Optional[str] = None
    team: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    # State
    is_active: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    
    def add_stage(self, stage: PipelineStage) -> str:
        """Add stage to pipeline"""
        self.stages.append(stage)
        return stage.id
        
    def get_stage(self, stage_id: str) -> Optional[PipelineStage]:
        """Get stage by ID"""
        for stage in self.stages:
            if stage.id == stage_id:
                return stage
        return None
        
    def validate_dag(self) -> bool:
        """Validate pipeline DAG has no cycles"""
        # Build adjacency list
        graph = {stage.id: stage.depends_on for stage in self.stages}
        
        # Check for cycles using DFS
        visited = set()
        rec_stack = set()
        
        def has_cycle(node):
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True
                    
            rec_stack.remove(node)
            return False
            
        for stage in self.stages:
            if stage.id not in visited:
                if has_cycle(stage.id):
                    return False
                    
        return True


@dataclass
class ExecutionLog(TimestampedModel):
    """Execution log entry"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    job_id: str = ""
    stage_id: Optional[str] = None
    
    # Log details
    level: str = "info"  # debug, info, warning, error
    message: str = ""
    details: Optional[Dict[str, Any]] = None
    
    # Source
    source: Optional[str] = None
    line_number: Optional[int] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "job_id": self.job_id,
            "stage_id": self.stage_id,
            "timestamp": self.created_at.isoformat(),
            "level": self.level,
            "message": self.message,
            "details": self.details,
            "source": self.source,
            "line_number": self.line_number
        }


@dataclass
class JobMetrics:
    """Job execution metrics"""
    records_read: int = 0
    records_written: int = 0
    records_failed: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    
    # Performance
    cpu_seconds: float = 0.0
    memory_mb_seconds: float = 0.0
    shuffle_bytes: Optional[int] = None
    
    # Timing
    queue_time_seconds: Optional[float] = None
    execution_time_seconds: Optional[float] = None
    
    # Stage metrics
    stage_metrics: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "records_read": self.records_read,
            "records_written": self.records_written,
            "records_failed": self.records_failed,
            "bytes_read": self.bytes_read,
            "bytes_written": self.bytes_written,
            "cpu_seconds": self.cpu_seconds,
            "memory_mb_seconds": self.memory_mb_seconds,
            "shuffle_bytes": self.shuffle_bytes,
            "queue_time_seconds": self.queue_time_seconds,
            "execution_time_seconds": self.execution_time_seconds,
            "stage_metrics": self.stage_metrics
        }


@dataclass
class JobResult:
    """Job execution result"""
    status: JobStatus
    message: Optional[str] = None
    outputs: Dict[str, Any] = field(default_factory=dict)
    metrics: JobMetrics = field(default_factory=JobMetrics)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "status": self.status.value,
            "message": self.message,
            "outputs": self.outputs,
            "metrics": self.metrics.to_dict(),
            "errors": self.errors
        }


@dataclass
class ProcessingJob(AuditedModel):
    """Data processing job"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    
    # Pipeline reference
    pipeline_id: Optional[str] = None
    pipeline_version: Optional[str] = None
    
    # Or standalone config
    config: Dict[str, Any] = field(default_factory=dict)
    
    # Execution
    status: JobStatus = JobStatus.PENDING
    engine: ProcessingEngine = ProcessingEngine.SPARK
    
    # Timing
    scheduled_at: Optional[datetime] = None
    queued_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Resources
    requested_resources: Dict[str, Any] = field(default_factory=dict)
    allocated_resources: Dict[str, Any] = field(default_factory=dict)
    
    # Results
    result: Optional[JobResult] = None
    
    # Tracking
    trigger_type: TriggerType = TriggerType.MANUAL
    triggered_by: Optional[str] = None
    parent_job_id: Optional[str] = None
    
    # Error handling
    retry_count: int = 0
    max_retries: int = 3
    error_message: Optional[str] = None
    
    # Monitoring
    logs: List[ExecutionLog] = field(default_factory=list)
    checkpoints: Dict[str, Any] = field(default_factory=dict)
    
    def get_duration(self) -> Optional[timedelta]:
        """Get job duration"""
        if self.started_at and self.completed_at:
            return self.completed_at - self.started_at
        return None
        
    def add_log(
        self,
        message: str,
        level: str = "info",
        stage_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ):
        """Add log entry"""
        log = ExecutionLog(
            job_id=self.id,
            stage_id=stage_id,
            level=level,
            message=message,
            details=details
        )
        self.logs.append(log)
        
    def update_status(self, status: JobStatus, message: Optional[str] = None):
        """Update job status"""
        self.status = status
        
        if status == JobStatus.QUEUED and not self.queued_at:
            self.queued_at = datetime.utcnow()
        elif status == JobStatus.RUNNING and not self.started_at:
            self.started_at = datetime.utcnow()
        elif status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            self.completed_at = datetime.utcnow()
            
        if message:
            self.add_log(message, level="info")


@dataclass
class DataQualityCheck:
    """Data quality check configuration"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: Optional[str] = None
    
    # Check type
    check_type: str = ""  # completeness, accuracy, consistency, etc.
    
    # Configuration
    config: Dict[str, Any] = field(default_factory=dict)
    
    # Thresholds
    error_threshold: Optional[float] = None
    warning_threshold: Optional[float] = None
    
    # Actions
    on_failure: str = "warn"  # warn, fail, skip
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "check_type": self.check_type,
            "config": self.config,
            "error_threshold": self.error_threshold,
            "warning_threshold": self.warning_threshold,
            "on_failure": self.on_failure
        }


@dataclass
class StreamingConfig:
    """Streaming processing configuration"""
    # Window configuration
    window_type: str = "tumbling"  # tumbling, sliding, session
    window_duration: str = "1 minute"
    slide_duration: Optional[str] = None
    
    # Watermark
    watermark_column: Optional[str] = None
    watermark_delay: str = "10 seconds"
    
    # State management
    checkpoint_location: str = ""
    checkpoint_interval: str = "1 minute"
    
    # Output
    output_mode: str = "append"  # append, update, complete
    trigger_type: str = "processingTime"  # processingTime, once, continuous
    trigger_interval: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "window_type": self.window_type,
            "window_duration": self.window_duration,
            "slide_duration": self.slide_duration,
            "watermark_column": self.watermark_column,
            "watermark_delay": self.watermark_delay,
            "checkpoint_location": self.checkpoint_location,
            "checkpoint_interval": self.checkpoint_interval,
            "output_mode": self.output_mode,
            "trigger_type": self.trigger_type,
            "trigger_interval": self.trigger_interval
        }


@dataclass
class ProcessingResource:
    """Processing resource requirements"""
    # Compute
    cpu_cores: int = 1
    memory_gb: int = 4
    
    # Storage
    disk_gb: Optional[int] = None
    disk_type: str = "standard"  # standard, ssd
    
    # Executors (for distributed processing)
    num_executors: Optional[int] = None
    executor_cores: Optional[int] = None
    executor_memory: Optional[str] = None
    
    # GPU
    gpu_enabled: bool = False
    gpu_type: Optional[str] = None
    gpu_count: Optional[int] = None
    
    # Constraints
    node_selector: Optional[Dict[str, str]] = None
    tolerations: Optional[List[Dict[str, Any]]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "cpu_cores": self.cpu_cores,
            "memory_gb": self.memory_gb,
            "disk_gb": self.disk_gb,
            "disk_type": self.disk_type,
            "num_executors": self.num_executors,
            "executor_cores": self.executor_cores,
            "executor_memory": self.executor_memory,
            "gpu_enabled": self.gpu_enabled,
            "gpu_type": self.gpu_type,
            "gpu_count": self.gpu_count,
            "node_selector": self.node_selector,
            "tolerations": self.tolerations
        } 