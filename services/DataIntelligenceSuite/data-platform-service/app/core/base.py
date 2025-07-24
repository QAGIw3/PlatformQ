"""
Data Platform Service Base Classes

Migrated to use the unified data-intelligence-common library.
"""

from typing import Dict, Any, List, Optional, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import uuid

from data_intelligence_common.base_service import DataIntelligenceBaseService
from data_intelligence_common.core.config.unified import UnifiedServiceConfig, DatabaseConnectionConfig
from data_intelligence_common.core.processing import (
    UnifiedProcessor, ProcessingConfig, ProcessingMode,
    DataSource, DataSink, ProcessingStage, ProcessingContext,
    FileSource, DatabaseSource, EventBusSource,
    FileSink, DatabaseSink, EventBusSink,
    QualityCheckStage, SchemaValidationStage, DataCleaningStage,
    DeduplicationStage, CommonQualityRules
)
from data_intelligence_common.core.events import Event, EventType, create_data_event
from data_intelligence_common.core.patterns.factory import Factory, FactoryRegistry
from data_intelligence_common.monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class DatasetType(str, Enum):
    """Types of datasets"""
    STRUCTURED = "structured"
    UNSTRUCTURED = "unstructured"
    SEMI_STRUCTURED = "semi_structured"
    STREAMING = "streaming"
    GRAPH = "graph"
    TIMESERIES = "timeseries"


class StorageFormat(str, Enum):
    """Storage formats"""
    PARQUET = "parquet"
    DELTA = "delta"
    ICEBERG = "iceberg"
    AVRO = "avro"
    ORC = "orc"
    JSON = "json"
    CSV = "csv"


@dataclass
class DataPlatformConfig(UnifiedServiceConfig):
    """Configuration for data platform service"""
    # Storage settings
    primary_storage: str = "minio"
    storage_path: str = "/data"
    default_format: StorageFormat = StorageFormat.PARQUET
    
    # Catalog settings
    enable_data_catalog: bool = True
    catalog_backend: str = "iceberg"  # iceberg, delta, hudi
    
    # Lakehouse settings
    enable_lakehouse: bool = True
    lakehouse_format: str = "delta"
    enable_time_travel: bool = True
    
    # Processing settings
    default_batch_size: int = 10000
    enable_auto_partitioning: bool = True
    enable_auto_compaction: bool = True
    
    # Lineage settings
    enable_lineage_tracking: bool = True
    lineage_backend: str = "atlas"
    
    # Quality settings
    enable_auto_profiling: bool = True
    enable_quality_gates: bool = True
    quality_threshold: float = 0.95
    
    # Governance
    enable_data_governance: bool = True
    enable_access_control: bool = True
    enable_data_classification: bool = True


class DataPlatformService(DataIntelligenceBaseService):
    """
    Data Platform service for data lifecycle management.
    
    Provides data ingestion, storage, cataloging, and governance.
    """
    
    def __init__(self, config: DataPlatformConfig):
        super().__init__(config)
        self.config = config
        
        # Platform components
        self._catalog = None
        self._lakehouse = None
        self._lineage_tracker = None
        self._storage_manager = None
        
        # Active pipelines
        self._ingestion_pipelines: Dict[str, UnifiedProcessor] = {}
        self._transformation_pipelines: Dict[str, UnifiedProcessor] = {}
        
        # Factories
        self._source_factory = None
        self._sink_factory = None
        
    async def _initialize_internal(self):
        """Initialize data platform components"""
        await super()._initialize_internal()
        
        # Initialize platform components
        await self._initialize_platform_components()
        
        # Initialize factories
        self._initialize_factories()
        
        # Register health checks
        self.register_health_check(
            "catalog",
            self._check_catalog_health,
            critical=True
        )
        
        self.register_health_check(
            "storage",
            self._check_storage_health,
            critical=True
        )
        
        # Start background tasks
        if self.config.enable_auto_compaction:
            self._start_background_task(self._compaction_loop())
            
        logger.info("Data platform service initialized")
        
    async def _initialize_platform_components(self):
        """Initialize platform infrastructure"""
        # Initialize catalog
        if self.config.enable_data_catalog:
            from ..catalog.data_catalog import DataCatalog
            self._catalog = DataCatalog(
                backend=self.config.catalog_backend,
                config=self.config
            )
            await self._catalog.initialize()
            
        # Initialize lakehouse
        if self.config.enable_lakehouse:
            from ..lakehouse.lakehouse_manager import LakehouseManager
            self._lakehouse = LakehouseManager(
                format=self.config.lakehouse_format,
                enable_time_travel=self.config.enable_time_travel
            )
            await self._lakehouse.initialize()
            
        # Initialize lineage tracker
        if self.config.enable_lineage_tracking:
            from ..lineage.lineage_tracker import LineageTracker
            self._lineage_tracker = LineageTracker(
                backend=self.config.lineage_backend
            )
            await self._lineage_tracker.initialize()
            
        # Initialize storage manager
        from ..storage.storage_manager import StorageManager
        self._storage_manager = StorageManager(
            backend=self.config.primary_storage,
            base_path=self.config.storage_path
        )
        await self._storage_manager.initialize()
        
    def _initialize_factories(self):
        """Initialize data source and sink factories"""
        # Source factory
        self._source_factory = FactoryRegistry()
        self._source_factory.register("file", FileSource)
        self._source_factory.register("database", DatabaseSource)
        self._source_factory.register("event", EventBusSource)
        
        # Sink factory
        self._sink_factory = FactoryRegistry()
        self._sink_factory.register("file", FileSink)
        self._sink_factory.register("database", DatabaseSink)
        self._sink_factory.register("event", EventBusSink)
        
    async def create_dataset(
        self,
        name: str,
        dataset_type: DatasetType,
        schema: Dict[str, Any],
        storage_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new dataset"""
        dataset_id = str(uuid.uuid4())
        
        try:
            # Create storage location
            storage_path = await self._storage_manager.create_dataset_storage(
                dataset_id=dataset_id,
                format=storage_config.get("format", self.config.default_format)
            )
            
            # Register in catalog
            if self._catalog:
                catalog_entry = await self._catalog.register_dataset(
                    dataset_id=dataset_id,
                    name=name,
                    dataset_type=dataset_type,
                    schema=schema,
                    storage_path=storage_path,
                    metadata=storage_config or {}
                )
                
            # Create lakehouse table if applicable
            if self._lakehouse and dataset_type == DatasetType.STRUCTURED:
                await self._lakehouse.create_table(
                    name=name,
                    schema=schema,
                    location=storage_path,
                    format=storage_config.get("format", self.config.default_format)
                )
                
            # Emit event
            await self.publish_event(
                event_type="dataset.created",
                data={
                    "dataset_id": dataset_id,
                    "name": name,
                    "type": dataset_type.value,
                    "storage_path": storage_path
                }
            )
            
            # Record metrics
            self.record_operation("dataset_created", {
                "type": dataset_type.value,
                "format": storage_config.get("format", self.config.default_format)
            })
            
            return {
                "dataset_id": dataset_id,
                "name": name,
                "type": dataset_type.value,
                "storage_path": storage_path,
                "created_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            self.record_error("dataset_creation_failed", e)
            raise
            
    async def ingest_data(
        self,
        dataset_id: str,
        source_config: Dict[str, Any],
        ingestion_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Ingest data into a dataset"""
        job_id = str(uuid.uuid4())
        
        try:
            # Get dataset info
            dataset_info = await self._catalog.get_dataset(dataset_id)
            
            # Create ingestion pipeline
            pipeline = await self._create_ingestion_pipeline(
                dataset_id=dataset_id,
                dataset_info=dataset_info,
                source_config=source_config,
                ingestion_config=ingestion_config or {}
            )
            
            # Store pipeline
            self._ingestion_pipelines[job_id] = pipeline
            
            # Start ingestion
            result = await pipeline.process(job_id=job_id)
            
            # Update lineage
            if self._lineage_tracker:
                await self._lineage_tracker.track_ingestion(
                    dataset_id=dataset_id,
                    source=source_config,
                    job_id=job_id,
                    result=result
                )
                
            # Emit event
            await self.publish_event(
                event_type="data.ingested",
                data={
                    "dataset_id": dataset_id,
                    "job_id": job_id,
                    "records_processed": result.get("records_processed", 0)
                }
            )
            
            # Record metrics
            self.record_operation("data_ingested", {
                "dataset_id": dataset_id,
                "records": result.get("records_processed", 0),
                "duration": result.get("duration", 0)
            })
            
            # Auto-profile if enabled
            if self.config.enable_auto_profiling:
                asyncio.create_task(
                    self._profile_dataset(dataset_id)
                )
                
            return result
            
        except Exception as e:
            self.record_error("data_ingestion_failed", e)
            raise
            
    async def _create_ingestion_pipeline(
        self,
        dataset_id: str,
        dataset_info: Dict[str, Any],
        source_config: Dict[str, Any],
        ingestion_config: Dict[str, Any]
    ) -> UnifiedProcessor:
        """Create data ingestion pipeline"""
        # Create processing config
        processing_config = ProcessingConfig(
            name=f"ingestion_{dataset_id}",
            mode=ProcessingMode.ADAPTIVE,
            batch_size=ingestion_config.get("batch_size", self.config.default_batch_size),
            enable_quality_checks=self.config.enable_quality_gates,
            enable_lineage_tracking=self.config.enable_lineage_tracking
        )
        
        # Create source
        source_type = source_config.pop("type")
        source = self._source_factory.create(source_type, **source_config)
        
        # Create sink
        sink = DatabaseSink(
            client=self._storage_manager,
            table=dataset_id,
            mode=ingestion_config.get("mode", "append")
        )
        
        # Build pipeline
        builder = UnifiedProcessor.pipeline(processing_config).from_source(source)
        
        # Add schema validation
        if dataset_info.get("schema"):
            builder = builder.transform(SchemaValidationStage(
                schema=dataset_info["schema"],
                strict=False,
                coerce_types=True
            ))
            
        # Add data cleaning
        builder = builder.transform(DataCleaningStage(
            trim_strings=True,
            remove_nulls=False,
            default_values=ingestion_config.get("defaults", {})
        ))
        
        # Add quality checks if enabled
        if self.config.enable_quality_gates:
            rules = await self._generate_quality_rules(dataset_info)
            builder = builder.transform(QualityCheckStage(
                rules=rules,
                fail_on_error=False,
                sample_rate=0.1
            ))
            
        # Add deduplication if specified
        if ingestion_config.get("deduplicate"):
            builder = builder.transform(DeduplicationStage(
                key_fields=ingestion_config.get("dedup_keys", ["id"]),
                window_size=10000
            ))
            
        # Add custom transformations
        if ingestion_config.get("transformations"):
            for transform in ingestion_config["transformations"]:
                builder = builder.transform(
                    self._create_transformation_stage(transform)
                )
                
        # Set sink
        builder = builder.to_sink(sink)
        
        # Build processor
        return builder.build(
            metrics_collector=self.metrics,
            event_bus=self.event_bus,
            cache_manager=self.cache
        )
        
    async def transform_data(
        self,
        source_dataset_id: str,
        target_dataset_id: str,
        transformation_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Transform data from one dataset to another"""
        job_id = str(uuid.uuid4())
        
        try:
            # Get dataset info
            source_info = await self._catalog.get_dataset(source_dataset_id)
            target_info = await self._catalog.get_dataset(target_dataset_id)
            
            # Create transformation pipeline
            pipeline = await self._create_transformation_pipeline(
                source_info=source_info,
                target_info=target_info,
                transformation_config=transformation_config
            )
            
            # Store pipeline
            self._transformation_pipelines[job_id] = pipeline
            
            # Execute transformation
            result = await pipeline.process(job_id=job_id)
            
            # Update lineage
            if self._lineage_tracker:
                await self._lineage_tracker.track_transformation(
                    source_dataset_id=source_dataset_id,
                    target_dataset_id=target_dataset_id,
                    job_id=job_id,
                    transformations=transformation_config.get("transformations", [])
                )
                
            # Emit event
            await self.publish_event(
                event_type="data.transformed",
                data={
                    "source_dataset_id": source_dataset_id,
                    "target_dataset_id": target_dataset_id,
                    "job_id": job_id,
                    "records_processed": result.get("records_processed", 0)
                }
            )
            
            return result
            
        except Exception as e:
            self.record_error("data_transformation_failed", e)
            raise
            
    async def _create_transformation_pipeline(
        self,
        source_info: Dict[str, Any],
        target_info: Dict[str, Any],
        transformation_config: Dict[str, Any]
    ) -> UnifiedProcessor:
        """Create data transformation pipeline"""
        # Create processing config
        processing_config = ProcessingConfig(
            name=f"transform_{source_info['name']}_to_{target_info['name']}",
            mode=ProcessingMode.BATCH,
            batch_size=transformation_config.get("batch_size", self.config.default_batch_size),
            enable_quality_checks=True,
            enable_lineage_tracking=True
        )
        
        # Create source
        source = DatabaseSource(
            client=self._storage_manager,
            query=f"SELECT * FROM {source_info['name']}",
            batch_size=processing_config.batch_size
        )
        
        # Create sink
        sink = DatabaseSink(
            client=self._storage_manager,
            table=target_info["name"],
            mode="overwrite"
        )
        
        # Build pipeline with transformations
        builder = UnifiedProcessor.pipeline(processing_config).from_source(source)
        
        # Add transformations
        for transform in transformation_config.get("transformations", []):
            builder = builder.transform(
                self._create_transformation_stage(transform)
            )
            
        # Add schema validation for target
        builder = builder.transform(SchemaValidationStage(
            schema=target_info["schema"],
            strict=True,
            coerce_types=True
        ))
        
        # Set sink
        builder = builder.to_sink(sink)
        
        return builder.build(
            metrics_collector=self.metrics,
            event_bus=self.event_bus,
            cache_manager=self.cache
        )
        
    def _create_transformation_stage(self, transform_config: Dict[str, Any]) -> ProcessingStage:
        """Create custom transformation stage"""
        transform_type = transform_config.get("type")
        
        class TransformationStage(ProcessingStage):
            async def process(self, data: Dict[str, Any], context: ProcessingContext) -> Optional[Dict[str, Any]]:
                if transform_type == "filter":
                    # Apply filter
                    condition = transform_config.get("condition")
                    if not eval(condition, {"data": data}):
                        return None
                        
                elif transform_type == "map":
                    # Apply mapping
                    mappings = transform_config.get("mappings", {})
                    for target, source in mappings.items():
                        if isinstance(source, str) and source.startswith("$"):
                            # Field reference
                            data[target] = data.get(source[1:])
                        else:
                            # Literal value
                            data[target] = source
                            
                elif transform_type == "aggregate":
                    # This would be handled differently for aggregations
                    pass
                    
                elif transform_type == "custom":
                    # Apply custom transformation
                    code = transform_config.get("code")
                    exec(code, {"data": data})
                    
                return data
                
        return TransformationStage()
        
    async def query_data(
        self,
        dataset_id: str,
        query: str,
        query_config: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Query data from a dataset"""
        # Check cache
        cache_key = f"query:{dataset_id}:{hash(query)}"
        cached_result = await self.get_cached(cache_key)
        if cached_result:
            return cached_result
            
        try:
            # Get dataset info
            dataset_info = await self._catalog.get_dataset(dataset_id)
            
            # Execute query
            if self._lakehouse and dataset_info["type"] == DatasetType.STRUCTURED:
                # Use lakehouse for structured data
                result = await self._lakehouse.query(
                    table=dataset_info["name"],
                    query=query,
                    **query_config or {}
                )
            else:
                # Use storage manager for other types
                result = await self._storage_manager.query(
                    dataset_id=dataset_id,
                    query=query
                )
                
            # Cache result
            await self.cache_result(
                cache_key,
                result,
                ttl=query_config.get("cache_ttl", 300)
            )
            
            # Record metrics
            self.record_operation("data_queried", {
                "dataset_id": dataset_id,
                "result_count": len(result)
            })
            
            return result
            
        except Exception as e:
            self.record_error("data_query_failed", e)
            raise
            
    async def _generate_quality_rules(self, dataset_info: Dict[str, Any]) -> List[Any]:
        """Generate quality rules based on dataset schema"""
        rules = []
        schema = dataset_info.get("schema", {})
        
        for field, field_info in schema.get("properties", {}).items():
            # Add not null rules for required fields
            if field in schema.get("required", []):
                rules.append(CommonQualityRules.not_null(field))
                
            # Add type-specific rules
            field_type = field_info.get("type")
            if field_type == "string" and field_info.get("pattern"):
                rules.append(CommonQualityRules.matches_pattern(
                    field,
                    field_info["pattern"]
                ))
            elif field_type in ["integer", "number"]:
                if "minimum" in field_info and "maximum" in field_info:
                    rules.append(CommonQualityRules.in_range(
                        field,
                        field_info["minimum"],
                        field_info["maximum"]
                    ))
                    
        return rules
        
    async def _profile_dataset(self, dataset_id: str):
        """Profile dataset for statistics and quality"""
        try:
            # Get sample data
            sample = await self.query_data(
                dataset_id,
                "SELECT * LIMIT 10000",
                {"cache_ttl": 0}
            )
            
            # Calculate statistics
            profile = {
                "dataset_id": dataset_id,
                "row_count": len(sample),
                "columns": {},
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Profile each column
            if sample:
                for column in sample[0].keys():
                    values = [row.get(column) for row in sample]
                    profile["columns"][column] = self._profile_column(values)
                    
            # Store profile
            if self._catalog:
                await self._catalog.update_dataset_metadata(
                    dataset_id,
                    {"profile": profile}
                )
                
            # Emit event
            await self.publish_event(
                event_type="dataset.profiled",
                data={
                    "dataset_id": dataset_id,
                    "profile": profile
                }
            )
            
        except Exception as e:
            logger.error(f"Failed to profile dataset {dataset_id}: {e}")
            
    def _profile_column(self, values: List[Any]) -> Dict[str, Any]:
        """Profile a single column"""
        profile = {
            "null_count": sum(1 for v in values if v is None),
            "null_percentage": sum(1 for v in values if v is None) / len(values) * 100
        }
        
        # Get non-null values
        non_null_values = [v for v in values if v is not None]
        
        if non_null_values:
            # Determine type
            sample_value = non_null_values[0]
            if isinstance(sample_value, (int, float)):
                profile["type"] = "numeric"
                profile["min"] = min(non_null_values)
                profile["max"] = max(non_null_values)
                profile["mean"] = sum(non_null_values) / len(non_null_values)
            elif isinstance(sample_value, str):
                profile["type"] = "string"
                profile["min_length"] = min(len(v) for v in non_null_values)
                profile["max_length"] = max(len(v) for v in non_null_values)
                profile["unique_count"] = len(set(non_null_values))
            elif isinstance(sample_value, bool):
                profile["type"] = "boolean"
                profile["true_count"] = sum(1 for v in non_null_values if v)
            else:
                profile["type"] = "unknown"
                
        return profile
        
    async def _compaction_loop(self):
        """Background task for data compaction"""
        while True:
            try:
                await asyncio.sleep(3600)  # Run hourly
                
                # Get datasets needing compaction
                datasets = await self._catalog.list_datasets(
                    filters={"needs_compaction": True}
                )
                
                for dataset in datasets:
                    try:
                        logger.info(f"Compacting dataset {dataset['id']}")
                        
                        # Compact dataset
                        await self._storage_manager.compact_dataset(
                            dataset["id"],
                            format=dataset.get("format", self.config.default_format)
                        )
                        
                        # Update catalog
                        await self._catalog.update_dataset_metadata(
                            dataset["id"],
                            {"last_compacted": datetime.utcnow().isoformat()}
                        )
                        
                    except Exception as e:
                        logger.error(f"Failed to compact dataset {dataset['id']}: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in compaction loop: {e}")
                
    async def _check_catalog_health(self) -> Dict[str, Any]:
        """Check catalog health"""
        if not self._catalog:
            return {"healthy": False, "reason": "Not initialized"}
            
        try:
            await self._catalog.list_datasets(limit=1)
            return {"healthy": True}
        except Exception as e:
            return {"healthy": False, "reason": str(e)}
            
    async def _check_storage_health(self) -> Dict[str, Any]:
        """Check storage health"""
        try:
            await self._storage_manager.check_health()
            return {"healthy": True}
        except Exception as e:
            return {"healthy": False, "reason": str(e)}
            
    async def _stop_internal(self):
        """Stop data platform components"""
        # Stop pipelines
        for pipeline in self._ingestion_pipelines.values():
            await pipeline.stop()
            
        for pipeline in self._transformation_pipelines.values():
            await pipeline.stop()
            
        # Cleanup components
        if self._catalog:
            await self._catalog.close()
            
        if self._lakehouse:
            await self._lakehouse.close()
            
        if self._storage_manager:
            await self._storage_manager.close()
            
        await super()._stop_internal()
        
        logger.info("Data platform service stopped")


# Export main components
__all__ = [
    'DatasetType',
    'StorageFormat',
    'DataPlatformConfig',
    'DataPlatformService'
] 