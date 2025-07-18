"""
SeaTunnel Service Models

SQLAlchemy models for data integration pipelines and synchronization jobs.
"""

from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy import (
    Column, String, DateTime, Float, Integer, JSON, 
    Boolean, Text, ForeignKey, Index, Enum, UniqueConstraint
)
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
import uuid
import enum

from platformq_shared.database import Base


class PipelineStatus(str, enum.Enum):
    """Pipeline execution status"""
    DRAFT = "draft"
    READY = "ready"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SCHEDULED = "scheduled"


class JobStatus(str, enum.Enum):
    """Job execution status"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class SourceType(str, enum.Enum):
    """Data source types"""
    PULSAR = "pulsar"
    POSTGRESQL = "postgresql"
    CASSANDRA = "cassandra"
    ELASTICSEARCH = "elasticsearch"
    MINIO = "minio"
    JANUSGRAPH = "janusgraph"
    DRUID = "druid"
    HIVE = "hive"
    TRINO = "trino"
    HTTP_API = "http_api"
    FILE = "file"
    SPARK = "spark"
    FLINK = "flink"


class SinkType(str, enum.Enum):
    """Data sink types"""
    PULSAR = "pulsar"
    POSTGRESQL = "postgresql"
    CASSANDRA = "cassandra"
    ELASTICSEARCH = "elasticsearch"
    MINIO = "minio"
    JANUSGRAPH = "janusgraph"
    DRUID = "druid"
    HIVE = "hive"
    MILVUS = "milvus"
    HTTP_API = "http_api"
    FILE = "file"
    CONSOLE = "console"


class TransformType(str, enum.Enum):
    """Transform types"""
    SQL = "sql"
    FILTER = "filter"
    MAP = "map"
    AGGREGATE = "aggregate"
    JOIN = "join"
    WINDOW = "window"
    PYTHON_UDF = "python_udf"
    SCHEMA_TRANSFORM = "schema_transform"
    ENCRYPT = "encrypt"
    DECRYPT = "decrypt"
    MASK = "mask"


class ScheduleType(str, enum.Enum):
    """Schedule types"""
    ONCE = "once"
    CRON = "cron"
    INTERVAL = "interval"
    CONTINUOUS = "continuous"
    EVENT_TRIGGERED = "event_triggered"


class DataPipeline(Base):
    """Data integration pipeline definition"""
    __tablename__ = "data_pipelines"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    
    # Pipeline metadata
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    category = Column(String(100))  # etl, sync, migration, streaming, batch
    tags = Column(JSON, default=list)
    
    # Configuration
    config = Column(JSON, nullable=False)  # Full SeaTunnel config
    source_configs = Column(JSON, nullable=False)  # Source configurations
    sink_configs = Column(JSON, nullable=False)  # Sink configurations
    transform_configs = Column(JSON, default=list)  # Transform configurations
    
    # Schema information
    input_schema = Column(JSON)  # Expected input schema
    output_schema = Column(JSON)  # Output schema after transforms
    schema_evolution_enabled = Column(Boolean, default=True)
    
    # Performance settings
    parallelism = Column(Integer, default=1)
    checkpoint_interval = Column(Integer)  # Seconds
    batch_size = Column(Integer)
    memory_limit = Column(String(20))  # e.g., "2Gi"
    cpu_limit = Column(String(20))  # e.g., "1000m"
    
    # Scheduling
    schedule_type = Column(Enum(ScheduleType), default=ScheduleType.ONCE)
    schedule_config = Column(JSON)  # Cron expression or interval config
    
    # Status
    status = Column(Enum(PipelineStatus), default=PipelineStatus.DRAFT, index=True)
    is_active = Column(Boolean, default=True, index=True)
    
    # Version control
    version = Column(Integer, default=1)
    published_at = Column(DateTime)
    published_by = Column(String(255))
    
    # Audit
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    jobs = relationship("PipelineJob", back_populates="pipeline")
    templates = relationship("PipelineTemplateMapping", back_populates="pipeline")
    
    # Indexes
    __table_args__ = (
        Index('idx_pipeline_tenant_status', 'tenant_id', 'status'),
        Index('idx_pipeline_category', 'category'),
        UniqueConstraint('tenant_id', 'name', 'version', name='uq_pipeline_name_version'),
    )


class PipelineJob(Base):
    """Pipeline execution job"""
    __tablename__ = "pipeline_jobs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    pipeline_id = Column(String(255), ForeignKey("data_pipelines.pipeline_id"), nullable=False)
    
    # Execution details
    job_name = Column(String(255))
    execution_mode = Column(String(50))  # batch, streaming, micro-batch
    trigger_type = Column(String(50))  # manual, scheduled, event
    trigger_details = Column(JSON)
    
    # Runtime configuration
    runtime_config = Column(JSON)  # Override pipeline config
    env_vars = Column(JSON)
    
    # Execution context
    k8s_namespace = Column(String(255))
    k8s_job_name = Column(String(255))
    seatunnel_job_id = Column(String(255))
    
    # Status and progress
    status = Column(Enum(JobStatus), default=JobStatus.PENDING, index=True)
    progress = Column(Float, default=0)  # 0-100
    
    # Metrics
    records_read = Column(Integer, default=0)
    records_written = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)
    bytes_read = Column(Integer, default=0)
    bytes_written = Column(Integer, default=0)
    
    # Timing
    scheduled_at = Column(DateTime)
    started_at = Column(DateTime, index=True)
    completed_at = Column(DateTime)
    duration_seconds = Column(Integer)
    
    # Error handling
    error_message = Column(Text)
    error_details = Column(JSON)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=3)
    
    # Lineage
    source_datasets = Column(JSON)  # List of source dataset IDs
    target_datasets = Column(JSON)  # List of target dataset IDs
    
    # Cost tracking
    compute_cost = Column(Float)
    storage_cost = Column(Float)
    network_cost = Column(Float)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    pipeline = relationship("DataPipeline", back_populates="jobs")
    checkpoints = relationship("JobCheckpoint", back_populates="job")
    
    # Indexes
    __table_args__ = (
        Index('idx_job_tenant_status', 'tenant_id', 'status'),
        Index('idx_job_pipeline', 'pipeline_id'),
        Index('idx_job_started', 'started_at'),
    )


class JobCheckpoint(Base):
    """Job execution checkpoints for recovery"""
    __tablename__ = "job_checkpoints"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id = Column(String(255), ForeignKey("pipeline_jobs.job_id"), nullable=False)
    checkpoint_id = Column(String(255), unique=True, nullable=False, index=True)
    
    # Checkpoint data
    checkpoint_time = Column(DateTime, default=datetime.utcnow)
    offset_info = Column(JSON)  # Source offsets/positions
    state_data = Column(JSON)  # Stateful transform state
    
    # Metrics at checkpoint
    records_processed = Column(Integer)
    bytes_processed = Column(Integer)
    
    # Storage
    storage_path = Column(String(500))  # MinIO path for large state
    size_bytes = Column(Integer)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    job = relationship("PipelineJob", back_populates="checkpoints")


class DataSource(Base):
    """Registered data sources"""
    __tablename__ = "data_sources"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    
    # Source details
    name = Column(String(255), nullable=False, index=True)
    source_type = Column(Enum(SourceType), nullable=False, index=True)
    description = Column(Text)
    
    # Connection configuration
    connection_config = Column(JSON, nullable=False)  # Encrypted sensitive data
    test_query = Column(Text)  # Query to test connection
    
    # Schema information
    schemas = Column(JSON)  # Available schemas/databases
    default_schema = Column(String(255))
    
    # Access control
    allowed_users = Column(JSON)  # List of user IDs
    allowed_roles = Column(JSON)  # List of roles
    
    # Health monitoring
    is_active = Column(Boolean, default=True, index=True)
    last_health_check = Column(DateTime)
    health_status = Column(String(50))  # healthy, unhealthy, unknown
    health_details = Column(JSON)
    
    # Metadata
    tags = Column(JSON, default=list)
    custom_properties = Column(JSON, default=dict)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_source_tenant_type', 'tenant_id', 'source_type'),
        UniqueConstraint('tenant_id', 'name', name='uq_source_name'),
    )


class DataSink(Base):
    """Registered data sinks"""
    __tablename__ = "data_sinks"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sink_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    
    # Sink details
    name = Column(String(255), nullable=False, index=True)
    sink_type = Column(Enum(SinkType), nullable=False, index=True)
    description = Column(Text)
    
    # Connection configuration
    connection_config = Column(JSON, nullable=False)  # Encrypted sensitive data
    
    # Write settings
    write_mode = Column(String(50))  # append, overwrite, upsert
    batch_settings = Column(JSON)  # Batch size, interval, etc.
    
    # Schema handling
    schema_evolution = Column(Boolean, default=False)
    schema_mapping = Column(JSON)  # Field mappings
    
    # Access control
    allowed_users = Column(JSON)
    allowed_roles = Column(JSON)
    
    # Health monitoring
    is_active = Column(Boolean, default=True, index=True)
    last_health_check = Column(DateTime)
    health_status = Column(String(50))
    
    # Metadata
    tags = Column(JSON, default=list)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_sink_tenant_type', 'tenant_id', 'sink_type'),
        UniqueConstraint('tenant_id', 'name', name='uq_sink_name'),
    )


class PipelineTemplate(Base):
    """Reusable pipeline templates"""
    __tablename__ = "pipeline_templates"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    template_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    
    # Template metadata
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    category = Column(String(100))
    icon = Column(String(100))
    
    # Template definition
    template_config = Column(JSON, nullable=False)  # Parameterized config
    parameters = Column(JSON)  # Parameter definitions
    
    # Examples
    example_values = Column(JSON)  # Example parameter values
    preview_config = Column(JSON)  # Preview of generated config
    
    # Usage
    is_public = Column(Boolean, default=False)
    usage_count = Column(Integer, default=0)
    
    # Version
    version = Column(String(20))
    is_latest = Column(Boolean, default=True)
    
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    pipelines = relationship("PipelineTemplateMapping", back_populates="template")
    
    # Indexes
    __table_args__ = (
        Index('idx_template_tenant_category', 'tenant_id', 'category'),
        UniqueConstraint('tenant_id', 'name', 'version', name='uq_template_name_version'),
    )


class PipelineTemplateMapping(Base):
    """Mapping between pipelines and templates"""
    __tablename__ = "pipeline_template_mappings"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pipeline_id = Column(String(255), ForeignKey("data_pipelines.pipeline_id"), nullable=False)
    template_id = Column(String(255), ForeignKey("pipeline_templates.template_id"), nullable=False)
    
    # Parameter values used
    parameter_values = Column(JSON)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    pipeline = relationship("DataPipeline", back_populates="templates")
    template = relationship("PipelineTemplate", back_populates="pipelines")


class TransformFunction(Base):
    """Custom transform functions"""
    __tablename__ = "transform_functions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    function_id = Column(String(255), unique=True, nullable=False, index=True)
    tenant_id = Column(String(255), nullable=False, index=True)
    
    # Function details
    name = Column(String(255), nullable=False, index=True)
    description = Column(Text)
    transform_type = Column(Enum(TransformType), nullable=False)
    
    # Function definition
    function_code = Column(Text)  # Python/SQL code
    language = Column(String(50))  # python, sql, javascript
    
    # Parameters
    input_schema = Column(JSON)
    output_schema = Column(JSON)
    parameters = Column(JSON)
    
    # Testing
    test_input = Column(JSON)
    test_output = Column(JSON)
    
    # Usage
    is_public = Column(Boolean, default=False)
    usage_count = Column(Integer, default=0)
    
    created_by = Column(String(255))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Indexes
    __table_args__ = (
        Index('idx_function_tenant_type', 'tenant_id', 'transform_type'),
        UniqueConstraint('tenant_id', 'name', name='uq_function_name'),
    )


class PipelineMetrics(Base):
    """Pipeline execution metrics"""
    __tablename__ = "pipeline_metrics"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    tenant_id = Column(String(255), nullable=False, index=True)
    pipeline_id = Column(String(255), nullable=False, index=True)
    job_id = Column(String(255), index=True)
    
    # Metric details
    metric_name = Column(String(255), nullable=False, index=True)
    metric_value = Column(Float, nullable=False)
    metric_unit = Column(String(50))
    
    # Context
    tags = Column(JSON)  # Additional context tags
    
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    
    # Indexes
    __table_args__ = (
        Index('idx_metrics_pipeline_time', 'pipeline_id', 'timestamp'),
        Index('idx_metrics_job_time', 'job_id', 'timestamp'),
        Index('idx_metrics_name_time', 'metric_name', 'timestamp'),
    ) 