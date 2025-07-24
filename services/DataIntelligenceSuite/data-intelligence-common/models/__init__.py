"""
Data Intelligence Common Models

Provides shared data models and schemas for the platform.
"""

from .base_models import (
    BaseModel,
    TimestampedModel,
    VersionedModel,
    AuditedModel
)

from .mixins import (
    MetadataMixin,
    OwnershipMixin,
    LifecycleMixin,
    QualityMixin,
    LineageMixin,
    SchemaEvolutionMixin,
    AccessControlMixin,
    MonitoringMixin
)

from .data_models import (
    Dataset,
    DataSource,
    DataSchema,
    DataField,
    DataType,
    DataQuality
)

from .ml_models import (
    MLModel,
    ModelVersion,
    ModelArtifact,
    TrainingJob,
    PredictionRequest,
    PredictionResult
)

from .processing_models import (
    ProcessingJob,
    JobStatus,
    JobResult,
    Pipeline,
    PipelineStage,
    ExecutionLog
)

from .catalog_models import (
    CatalogEntry,
    AssetType,
    AssetMetadata,
    Lineage,
    Tag,
    Classification
)

__all__ = [
    # Base
    "BaseModel",
    "TimestampedModel",
    "VersionedModel",
    "AuditedModel",
    
    # Mixins
    "MetadataMixin",
    "OwnershipMixin",
    "LifecycleMixin",
    "QualityMixin",
    "LineageMixin",
    "SchemaEvolutionMixin",
    "AccessControlMixin",
    "MonitoringMixin",
    
    # Data
    "Dataset",
    "DataSource",
    "DataSchema",
    "DataField",
    "DataType",
    "DataQuality",
    
    # ML
    "MLModel",
    "ModelVersion",
    "ModelArtifact",
    "TrainingJob",
    "PredictionRequest",
    "PredictionResult",
    
    # Processing
    "ProcessingJob",
    "JobStatus",
    "JobResult",
    "Pipeline",
    "PipelineStage",
    "ExecutionLog",
    
    # Catalog
    "CatalogEntry",
    "AssetType",
    "AssetMetadata",
    "Lineage",
    "Tag",
    "Classification"
] 