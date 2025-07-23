"""
Data Models for Data Intelligence

Provides models for datasets, schemas, and data quality.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass, field
from enum import Enum

from .base_models import TimestampedModel, VersionedModel


class DataType(Enum):
    """Supported data types"""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATETIME = "datetime"
    DATE = "date"
    TIME = "time"
    BINARY = "binary"
    JSON = "json"
    ARRAY = "array"
    STRUCT = "struct"
    MAP = "map"
    DECIMAL = "decimal"
    TIMESTAMP = "timestamp"
    UUID = "uuid"


class DataFormat(Enum):
    """Supported data formats"""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    ORC = "orc"
    XML = "xml"
    EXCEL = "excel"
    TEXT = "text"
    BINARY = "binary"
    DELTA = "delta"
    ICEBERG = "iceberg"


class StorageType(Enum):
    """Storage types"""
    S3 = "s3"
    MINIO = "minio"
    HDFS = "hdfs"
    LOCAL = "local"
    AZURE_BLOB = "azure_blob"
    GCS = "gcs"
    FTP = "ftp"
    HTTP = "http"
    JDBC = "jdbc"
    CASSANDRA = "cassandra"
    ELASTICSEARCH = "elasticsearch"
    PULSAR = "pulsar"


class CompressionType(Enum):
    """Compression types"""
    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"
    BZIP2 = "bzip2"
    DEFLATE = "deflate"


@dataclass
class DataField(TimestampedModel):
    """Represents a field in a data schema"""
    name: str
    data_type: DataType
    description: Optional[str] = None
    
    # Constraints
    nullable: bool = True
    unique: bool = False
    primary_key: bool = False
    foreign_key: Optional[str] = None
    
    # Validation
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    enum_values: Optional[List[Any]] = None
    
    # Default
    default_value: Optional[Any] = None
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    
    # For nested types
    element_type: Optional['DataType'] = None  # For arrays
    fields: Optional[List['DataField']] = None  # For structs
    key_type: Optional['DataType'] = None  # For maps
    value_type: Optional['DataType'] = None  # For maps
    
    # Statistics
    null_count: Optional[int] = None
    distinct_count: Optional[int] = None
    min_observed: Optional[Any] = None
    max_observed: Optional[Any] = None
    avg_length: Optional[float] = None


@dataclass
class DataSchema(VersionedModel):
    """Represents the schema of a dataset"""
    name: str
    fields: List[DataField]
    description: Optional[str] = None
    
    # Format info
    format: Optional[DataFormat] = None
    delimiter: Optional[str] = None
    quote_char: Optional[str] = None
    escape_char: Optional[str] = None
    header_row: bool = True
    encoding: str = "utf-8"
    
    # Partitioning
    partition_columns: List[str] = field(default_factory=list)
    clustering_columns: List[str] = field(default_factory=list)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    
    def get_field(self, name: str) -> Optional[DataField]:
        """Get field by name"""
        for field in self.fields:
            if field.name == name:
                return field
        return None
        
    def add_field(self, field: DataField):
        """Add a field to schema"""
        self.fields.append(field)
        self.increment_version()
        
    def remove_field(self, name: str) -> bool:
        """Remove field by name"""
        for i, field in enumerate(self.fields):
            if field.name == name:
                self.fields.pop(i)
                self.increment_version()
                return True
        return False


@dataclass
class DataSource(TimestampedModel):
    """Represents a data source"""
    name: str
    storage_type: StorageType
    connection_string: str
    description: Optional[str] = None
    
    # Authentication
    credentials: Optional[Dict[str, Any]] = None
    use_vault: bool = False
    vault_path: Optional[str] = None
    
    # Configuration
    config: Dict[str, Any] = field(default_factory=dict)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    
    # Status
    is_active: bool = True
    last_accessed: Optional[datetime] = None
    last_error: Optional[str] = None


@dataclass
class Dataset(VersionedModel):
    """Represents a dataset"""
    name: str
    source: DataSource
    schema: DataSchema
    description: Optional[str] = None
    
    # Location
    path: str
    format: DataFormat
    compression: CompressionType = CompressionType.NONE
    
    # Size info
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    
    # Time info
    created_time: Optional[datetime] = None
    modified_time: Optional[datetime] = None
    
    # Partitioning
    is_partitioned: bool = False
    partition_columns: List[str] = field(default_factory=list)
    partitions: List[Dict[str, Any]] = field(default_factory=list)
    
    # Quality
    quality_score: Optional[float] = None
    quality_checks: List[Dict[str, Any]] = field(default_factory=list)
    
    # Lineage
    upstream_datasets: List[str] = field(default_factory=list)
    downstream_datasets: List[str] = field(default_factory=list)
    
    # Access
    access_frequency: int = 0
    last_accessed: Optional[datetime] = None
    access_history: List[Dict[str, Any]] = field(default_factory=list)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    business_metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Lifecycle
    retention_days: Optional[int] = None
    archive_after_days: Optional[int] = None
    delete_after_days: Optional[int] = None
    is_archived: bool = False
    is_deleted: bool = False


@dataclass
class DataQualityDimension:
    """Represents a data quality dimension"""
    name: str
    description: str
    weight: float = 1.0
    
    
@dataclass
class DataQualityMetric:
    """Represents a data quality metric"""
    dimension: DataQualityDimension
    metric_name: str
    value: float
    threshold: float
    passed: bool
    details: Optional[Dict[str, Any]] = None


@dataclass
class DataQuality(TimestampedModel):
    """Represents data quality assessment"""
    dataset_id: str
    dataset_name: str
    
    # Overall score
    overall_score: float
    passed: bool
    
    # Dimension scores
    completeness_score: float = 0.0
    accuracy_score: float = 0.0
    consistency_score: float = 0.0
    validity_score: float = 0.0
    uniqueness_score: float = 0.0
    timeliness_score: float = 0.0
    
    # Detailed metrics
    metrics: List[DataQualityMetric] = field(default_factory=list)
    
    # Issues found
    issues: List[Dict[str, Any]] = field(default_factory=list)
    issue_count: int = 0
    critical_issues: int = 0
    
    # Profiling results
    profile: Dict[str, Any] = field(default_factory=dict)
    
    # Recommendations
    recommendations: List[str] = field(default_factory=list)
    
    # Metadata
    check_duration_seconds: Optional[float] = None
    checked_rows: Optional[int] = None
    total_rows: Optional[int] = None
    
    def add_metric(self, metric: DataQualityMetric):
        """Add a quality metric"""
        self.metrics.append(metric)
        
    def add_issue(
        self,
        issue_type: str,
        severity: str,
        description: str,
        affected_columns: Optional[List[str]] = None,
        sample_values: Optional[List[Any]] = None
    ):
        """Add a quality issue"""
        issue = {
            "type": issue_type,
            "severity": severity,
            "description": description,
            "affected_columns": affected_columns or [],
            "sample_values": sample_values or [],
            "timestamp": datetime.utcnow()
        }
        self.issues.append(issue)
        self.issue_count += 1
        if severity == "critical":
            self.critical_issues += 1 