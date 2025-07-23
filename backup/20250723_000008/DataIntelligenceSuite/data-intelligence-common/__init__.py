"""
DataIntelligenceSuite Common Library

Provides shared components for all data intelligence services.
"""

__version__ = "1.0.0"

# Base Service Framework
from .base_service import (
    DataIntelligenceBaseService,
    ServiceMetadata,
    ServiceConfig,
    create_data_intelligence_app
)

# Processing Framework
from .core.processing import (
    # Base
    BaseProcessor,
    ProcessorConfig,
    ProcessingResult,
    ProcessingStatus,
    ProcessingMode,
    
    # Batch
    BatchProcessor,
    BatchConfig,
    BatchJob,
    BatchResult,
    PartitionStrategy,
    
    # Stream
    StreamProcessor,
    StreamConfig,
    StreamSource,
    StreamSink,
    WindowType,
    
    # Quality
    QualityProcessor,
    QualityConfig,
    QualityCheck,
    QualityResult,
    DataQualityDimension,
    
    # Pipeline
    PipelineBuilder,
    Pipeline,
    PipelineStage,
    StageResult,
    TransformFunction
)

# Event Framework
from .event_handlers import (
    BaseEventProcessor,
    EventRouter,
    PulsarEventBus,
    EventStore,
    EventType,
    EventPriority
)

# API Framework
from .core.api import (
    BaseRouter,
    RouterConfig,
    APIResponse,
    PaginatedResponse,
    ErrorResponse,
    HealthResponse
)

# Monitoring
from .monitoring import (
    MetricsCollector,
    StructuredLogger,
    HealthChecker,
    TracingManager,
    CircuitBreaker
)

# Caching
from .core.caching import (
    CacheManager,
    CacheConfig,
    CacheStrategy,
    cached,
    cache_aside,
    cache_invalidate
)

# Catalog - Updated to match actual implementations
from .core.catalog import (
    BaseCatalog,
    CatalogConfig,
    CatalogEntity,
    EntityType,
    EntityStatus,
    MetadataManager,
    LineageTracker,
    DiscoveryEngine,
    QualityIntegrator,
    CatalogSearch,
    GlossaryManager,
    AccessController
)

# ML Framework - Updated to match actual implementations
from .core.ml import (
    BaseMLModel,
    ModelConfig,
    ModelType,
    ProblemType,
    ModelStatus,
    ModelTrainer,
    TrainingConfig,
    TrainingResult,
    InferenceEngine,
    FeatureEngineering,
    ModelRegistry,
    ModelMonitor,
    AutoMLEngine,
    ModelExplainer
)

# Orchestration - Updated to match actual implementations
from .core.orchestration import (
    PipelineOrchestrator,
    PipelineRun,
    ExecutionMode,
    EventOrchestrator,
    EventRule,
    DistributedOrchestrator,
    ClusterNode,
    DistributedTask
)

# Events
from .core.events import (
    EventBus,
    Event,
    EventPattern,
    EventProcessor as CoreEventProcessor,
    EventStore as CoreEventStore,
    SagaOrchestrator
)

# Integration - New module for data integration patterns
from .core.integration import (
    DataSource,
    CacheStrategy,
    ConsistencyLevel,
    DataEntity,
    CacheRegion,
    DataSourceConfig,
    BaseDigitalIntegrationHub
)

# Clients
from .clients import (
    BaseServiceClient,
    ClientConfig,
    AuthServiceClient,
    CatalogServiceClient,
    AnalyticsServiceClient,
    MLServiceClient,
    ProcessingServiceClient
)

# Utils
from .utils import (
    # Converters
    DataFormat,
    TypeConverter,
    FormatConverter,
    convert_data,
    # DateTime
    DateTimeParser,
    TimeZoneUtils,
    parse_datetime,
    format_datetime,
    # Encryption
    SymmetricEncryption,
    HashUtils,
    PasswordUtils,
    encrypt,
    decrypt,
    hash_password,
    verify_password
)

# Vault/Consul Integration
from .vault_consul import (
    VaultConsulIntegration,
    DataServiceConfig
)

__all__ = [
    # Version
    "__version__",
    
    # Base Service
    "DataIntelligenceBaseService",
    "ServiceMetadata",
    "ServiceConfig",
    "create_data_intelligence_app",
    
    # Processing
    "BaseProcessor",
    "ProcessorConfig",
    "ProcessingResult",
    "ProcessingStatus",
    "ProcessingMode",
    "BatchProcessor",
    "BatchConfig",
    "StreamProcessor",
    "StreamConfig",
    "QualityProcessor",
    "QualityConfig",
    "PipelineBuilder",
    "Pipeline",
    
    # Events
    "BaseEventProcessor",
    "EventRouter",
    "PulsarEventBus",
    "EventStore",
    "EventType",
    "EventPriority",
    
    # API
    "BaseRouter",
    "RouterConfig",
    "APIResponse",
    "PaginatedResponse",
    "ErrorResponse",
    "HealthResponse",
    
    # Monitoring
    "MetricsCollector",
    "StructuredLogger",
    "HealthChecker",
    "TracingManager",
    "CircuitBreaker",
    
    # Caching
    "CacheManager",
    "CacheConfig",
    "CacheStrategy",
    "cached",
    
    # Catalog
    "BaseCatalog",
    "CatalogConfig",
    "CatalogEntity",
    "EntityType",
    "EntityStatus",
    "MetadataManager",
    "LineageTracker",
    "DiscoveryEngine",
    "QualityIntegrator",
    "CatalogSearch",
    "GlossaryManager",
    "AccessController",
    
    # ML
    "BaseMLModel",
    "ModelConfig",
    "ModelType",
    "ProblemType",
    "ModelStatus",
    "ModelTrainer",
    "TrainingConfig",
    "TrainingResult",
    "InferenceEngine",
    "FeatureEngineering",
    "ModelRegistry",
    "ModelMonitor",
    "AutoMLEngine",
    "ModelExplainer",
    
    # Orchestration
    "PipelineOrchestrator",
    "PipelineRun",
    "ExecutionMode",
    "EventOrchestrator",
    "EventRule",
    "DistributedOrchestrator",
    "ClusterNode",
    "DistributedTask",
    
    # Events
    "EventBus",
    "Event",
    "EventPattern",
    "SagaOrchestrator",
    
    # Integration
    "DataSource",
    "CacheStrategy",
    "ConsistencyLevel",
    "DataEntity",
    "CacheRegion",
    "DataSourceConfig",
    "BaseDigitalIntegrationHub",
    
    # Clients
    "BaseServiceClient",
    "ClientConfig",
    "AuthServiceClient",
    "CatalogServiceClient",
    "AnalyticsServiceClient",
    "MLServiceClient",
    "ProcessingServiceClient",
    
    # Utils
    "DataFormat",
    "TypeConverter",
    "FormatConverter",
    "convert_data",
    "DateTimeParser",
    "TimeZoneUtils",
    "parse_datetime",
    "format_datetime",
    "SymmetricEncryption",
    "HashUtils",
    "PasswordUtils",
    "encrypt",
    "decrypt",
    "hash_password",
    "verify_password",
    
    # Vault/Consul
    "VaultConsulIntegration",
    "DataServiceConfig"
] 