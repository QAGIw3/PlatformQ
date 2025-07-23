"""
Catalog Manager

Enhanced data catalog and metadata management for DataIntelligenceSuite v2.0
"""

import asyncio
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple, Set
import json
import uuid

from data_intelligence_common import (
    BaseProcessor,
    ProcessorConfig,
    MetricsCollector,
    StructuredLogger,
    cached,
    CacheStrategy,
    CacheManager as BaseCacheManager
)
from data_intelligence_common.core.events import EventBus, Event
from data_intelligence_common.core.catalog import (
    BaseCatalog,
    CatalogEntity,
    EntityType,
    EntityStatus,
    MetadataManager,
    LineageTracker,
    DiscoveryEngine
)

from ..infrastructure.iceberg import IcebergCatalog
from ..infrastructure.delta import DeltaLakeClient
from ..infrastructure.atlas import AtlasClient
from ..domain.models.catalog import (
    Dataset,
    Schema,
    Classification,
    Lineage,
    BusinessTerm,
    DataQualityRule
)

logger = StructuredLogger.get_logger(__name__)


class CatalogManager(BaseProcessor):
    """
    Enhanced Catalog Manager with v2.0 capabilities:
    - Unified metadata management
    - Schema evolution tracking
    - Data lineage and impact analysis
    - Business glossary integration
    - Automated classification
    - Quality rule management
    - Discovery and search
    """
    
    def __init__(
        self,
        iceberg_catalog: IcebergCatalog,
        delta_client: DeltaLakeClient,
        event_bus: EventBus,
        cache_manager: BaseCacheManager,
        config: ProcessorConfig
    ):
        super().__init__(config)
        self.iceberg = iceberg_catalog
        self.delta = delta_client
        self.event_bus = event_bus
        self.cache = cache_manager
        
        # Atlas client for metadata management (optional)
        self.atlas_client: Optional[AtlasClient] = None
        
        # Catalog components
        self.metadata_manager = MetadataManager()
        self.lineage_tracker = LineageTracker()
        self.discovery_engine = DiscoveryEngine()
        
        # Internal registries
        self.datasets: Dict[str, Dataset] = {}
        self.schemas: Dict[str, Schema] = {}
        self.classifications: Dict[str, Classification] = {}
        self.business_terms: Dict[str, BusinessTerm] = {}
        
        # Configuration
        self.auto_classify = config.get("auto_classify", True)
        self.track_lineage = config.get("track_lineage", True)
        self.enable_discovery = config.get("enable_discovery", True)
        
        # Background tasks
        self._discovery_task: Optional[asyncio.Task] = None
        self._classification_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize catalog manager"""
        logger.info("initializing_catalog_manager",
                   auto_classify=self.auto_classify,
                   track_lineage=self.track_lineage)
        
        # Initialize Atlas client if configured
        atlas_url = self.config.get("atlas_url")
        if atlas_url:
            self.atlas_client = AtlasClient(atlas_url)
            await self.atlas_client.initialize()
            
        # Start background tasks
        if self.enable_discovery:
            self._discovery_task = asyncio.create_task(self._run_discovery())
            
        if self.auto_classify:
            self._classification_task = asyncio.create_task(self._run_classification())
            
        # Subscribe to events
        await self.event_bus.subscribe("table.created", self._handle_table_created)
        await self.event_bus.subscribe("table.updated", self._handle_table_updated)
        await self.event_bus.subscribe("data.processed", self._handle_data_processed)
        
    async def shutdown(self):
        """Shutdown catalog manager"""
        logger.info("shutting_down_catalog_manager")
        
        # Cancel background tasks
        if self._discovery_task:
            self._discovery_task.cancel()
        if self._classification_task:
            self._classification_task.cancel()
            
        # Shutdown Atlas client
        if self.atlas_client:
            await self.atlas_client.shutdown()
            
    async def register_dataset(
        self,
        name: str,
        description: str,
        location: str,
        format: str,
        schema: Optional[Dict[str, Any]] = None,
        owner: Optional[str] = None,
        tags: Optional[List[str]] = None,
        properties: Optional[Dict[str, Any]] = None
    ) -> Dataset:
        """Register a new dataset in the catalog"""
        
        # Create dataset
        dataset = Dataset(
            id=str(uuid.uuid4()),
            name=name,
            description=description,
            location=location,
            format=format,
            owner=owner or "system",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            status=EntityStatus.ACTIVE,
            tags=tags or [],
            properties=properties or {}
        )
        
        # Register schema if provided
        if schema:
            schema_obj = await self.register_schema(
                dataset_id=dataset.id,
                schema_definition=schema,
                version=1
            )
            dataset.schema_id = schema_obj.id
            
        # Store dataset
        self.datasets[dataset.id] = dataset
        
        # Create catalog entity
        entity = CatalogEntity(
            id=dataset.id,
            name=name,
            type=EntityType.DATASET,
            status=EntityStatus.ACTIVE,
            metadata={
                "location": location,
                "format": format,
                "owner": owner
            }
        )
        
        # Register with metadata manager
        await self.metadata_manager.register_entity(entity)
        
        # Auto-classify if enabled
        if self.auto_classify:
            await self._queue_classification(dataset.id)
            
        # Emit event
        await self.event_bus.publish(Event(
            type="catalog.dataset.registered",
            data={
                "dataset_id": dataset.id,
                "name": name,
                "location": location,
                "format": format
            }
        ))
        
        # Track metrics
        self.metrics.increment("catalog.datasets.registered",
                             tags={"format": format})
        
        logger.info("dataset_registered",
                   dataset_id=dataset.id,
                   name=name,
                   format=format)
        
        return dataset
        
    async def register_schema(
        self,
        dataset_id: str,
        schema_definition: Dict[str, Any],
        version: int,
        compatibility_mode: str = "BACKWARD"
    ) -> Schema:
        """Register a schema for a dataset"""
        
        # Validate schema
        self._validate_schema(schema_definition)
        
        # Check compatibility if previous version exists
        existing_schemas = [s for s in self.schemas.values() 
                          if s.dataset_id == dataset_id]
        if existing_schemas:
            latest = max(existing_schemas, key=lambda s: s.version)
            if not self._check_compatibility(
                latest.definition,
                schema_definition,
                compatibility_mode
            ):
                raise ValueError(f"Schema not compatible with mode {compatibility_mode}")
                
        # Create schema
        schema = Schema(
            id=str(uuid.uuid4()),
            dataset_id=dataset_id,
            version=version,
            definition=schema_definition,
            compatibility_mode=compatibility_mode,
            created_at=datetime.utcnow()
        )
        
        # Store schema
        self.schemas[schema.id] = schema
        
        # Emit event
        await self.event_bus.publish(Event(
            type="catalog.schema.registered",
            data={
                "schema_id": schema.id,
                "dataset_id": dataset_id,
                "version": version
            }
        ))
        
        logger.info("schema_registered",
                   schema_id=schema.id,
                   dataset_id=dataset_id,
                   version=version)
        
        return schema
        
    async def create_lineage(
        self,
        source_datasets: List[str],
        target_dataset: str,
        process_name: str,
        process_type: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Lineage:
        """Create lineage relationship between datasets"""
        
        # Validate datasets exist
        for dataset_id in source_datasets + [target_dataset]:
            if dataset_id not in self.datasets:
                raise ValueError(f"Dataset {dataset_id} not found")
                
        # Create lineage
        lineage = Lineage(
            id=str(uuid.uuid4()),
            source_datasets=source_datasets,
            target_dataset=target_dataset,
            process_name=process_name,
            process_type=process_type,
            created_at=datetime.utcnow(),
            metadata=metadata or {}
        )
        
        # Track lineage
        if self.track_lineage:
            await self.lineage_tracker.add_lineage(
                sources=source_datasets,
                target=target_dataset,
                process=process_name,
                metadata=metadata
            )
            
        # Emit event
        await self.event_bus.publish(Event(
            type="catalog.lineage.created",
            data={
                "lineage_id": lineage.id,
                "sources": source_datasets,
                "target": target_dataset,
                "process": process_name
            }
        ))
        
        logger.info("lineage_created",
                   lineage_id=lineage.id,
                   sources=len(source_datasets),
                   target=target_dataset)
        
        return lineage
        
    async def add_classification(
        self,
        dataset_id: str,
        classification_name: str,
        confidence: float = 1.0,
        attributes: Optional[Dict[str, Any]] = None
    ) -> Classification:
        """Add classification to a dataset"""
        
        # Validate dataset exists
        if dataset_id not in self.datasets:
            raise ValueError(f"Dataset {dataset_id} not found")
            
        # Create classification
        classification = Classification(
            id=str(uuid.uuid4()),
            name=classification_name,
            dataset_id=dataset_id,
            confidence=confidence,
            attributes=attributes or {},
            created_at=datetime.utcnow()
        )
        
        # Store classification
        self.classifications[classification.id] = classification
        
        # Update dataset
        dataset = self.datasets[dataset_id]
        if "classifications" not in dataset.properties:
            dataset.properties["classifications"] = []
        dataset.properties["classifications"].append(classification_name)
        
        # Emit event
        await self.event_bus.publish(Event(
            type="catalog.classification.added",
            data={
                "dataset_id": dataset_id,
                "classification": classification_name,
                "confidence": confidence
            }
        ))
        
        logger.info("classification_added",
                   dataset_id=dataset_id,
                   classification=classification_name,
                   confidence=confidence)
        
        return classification
        
    async def create_business_term(
        self,
        name: str,
        definition: str,
        owner: str,
        synonyms: Optional[List[str]] = None,
        related_terms: Optional[List[str]] = None,
        mapped_datasets: Optional[List[str]] = None
    ) -> BusinessTerm:
        """Create a business glossary term"""
        
        # Create term
        term = BusinessTerm(
            id=str(uuid.uuid4()),
            name=name,
            definition=definition,
            owner=owner,
            synonyms=synonyms or [],
            related_terms=related_terms or [],
            mapped_datasets=mapped_datasets or [],
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        # Store term
        self.business_terms[term.id] = term
        
        # Map to datasets
        for dataset_id in mapped_datasets or []:
            if dataset_id in self.datasets:
                dataset = self.datasets[dataset_id]
                if "business_terms" not in dataset.properties:
                    dataset.properties["business_terms"] = []
                dataset.properties["business_terms"].append(name)
                
        # Emit event
        await self.event_bus.publish(Event(
            type="catalog.business_term.created",
            data={
                "term_id": term.id,
                "name": name,
                "mapped_datasets": len(mapped_datasets or [])
            }
        ))
        
        logger.info("business_term_created",
                   term_id=term.id,
                   name=name)
        
        return term
        
    @cached(ttl=300, strategy=CacheStrategy.CACHE_ASIDE)
    async def search_datasets(
        self,
        query: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 20,
        offset: int = 0
    ) -> Tuple[List[Dataset], int]:
        """Search datasets in the catalog"""
        
        # Use discovery engine if available
        if self.enable_discovery:
            results = await self.discovery_engine.search(
                query=query,
                entity_type=EntityType.DATASET,
                filters=filters,
                limit=limit,
                offset=offset
            )
            
            # Convert to datasets
            datasets = []
            for result in results.entities:
                if result.id in self.datasets:
                    datasets.append(self.datasets[result.id])
                    
            return datasets, results.total
            
        # Fallback to simple search
        matching_datasets = []
        query_lower = query.lower()
        
        for dataset in self.datasets.values():
            # Check name and description
            if (query_lower in dataset.name.lower() or 
                query_lower in dataset.description.lower()):
                
                # Apply filters
                if filters:
                    if not self._match_filters(dataset, filters):
                        continue
                        
                matching_datasets.append(dataset)
                
        # Apply pagination
        total = len(matching_datasets)
        paginated = matching_datasets[offset:offset + limit]
        
        return paginated, total
        
    async def get_dataset_lineage(
        self,
        dataset_id: str,
        direction: str = "both",
        depth: int = 3
    ) -> Dict[str, Any]:
        """Get lineage graph for a dataset"""
        
        if dataset_id not in self.datasets:
            raise ValueError(f"Dataset {dataset_id} not found")
            
        # Get lineage from tracker
        lineage_graph = await self.lineage_tracker.get_lineage(
            entity_id=dataset_id,
            direction=direction,
            max_depth=depth
        )
        
        # Enhance with dataset metadata
        enhanced_graph = {
            "root": dataset_id,
            "direction": direction,
            "depth": depth,
            "nodes": {},
            "edges": lineage_graph.get("edges", [])
        }
        
        # Add node details
        for node_id in lineage_graph.get("nodes", []):
            if node_id in self.datasets:
                dataset = self.datasets[node_id]
                enhanced_graph["nodes"][node_id] = {
                    "name": dataset.name,
                    "format": dataset.format,
                    "owner": dataset.owner,
                    "location": dataset.location
                }
                
        return enhanced_graph
        
    async def get_impact_analysis(
        self,
        dataset_id: str,
        change_type: str = "schema"
    ) -> Dict[str, Any]:
        """Analyze impact of changes to a dataset"""
        
        # Get downstream lineage
        lineage = await self.get_dataset_lineage(
            dataset_id,
            direction="downstream",
            depth=10
        )
        
        # Analyze impact
        impact = {
            "dataset_id": dataset_id,
            "change_type": change_type,
            "affected_datasets": [],
            "affected_processes": [],
            "risk_level": "low"
        }
        
        # Find affected datasets
        for edge in lineage.get("edges", []):
            if edge["source"] == dataset_id or edge["target"] in impact["affected_datasets"]:
                if edge["target"] not in impact["affected_datasets"]:
                    impact["affected_datasets"].append(edge["target"])
                if edge.get("process") and edge["process"] not in impact["affected_processes"]:
                    impact["affected_processes"].append(edge["process"])
                    
        # Calculate risk level
        affected_count = len(impact["affected_datasets"])
        if affected_count > 10:
            impact["risk_level"] = "high"
        elif affected_count > 5:
            impact["risk_level"] = "medium"
            
        return impact
        
    async def _handle_table_created(self, event: Event):
        """Handle table creation events"""
        
        # Extract table metadata
        table_name = event.data.get("table_name")
        location = event.data.get("location")
        format = event.data.get("format", "iceberg")
        schema = event.data.get("schema")
        
        # Register as dataset
        await self.register_dataset(
            name=table_name,
            description=f"Table created from {format}",
            location=location,
            format=format,
            schema=schema,
            properties={"source": "table_creation"}
        )
        
    async def _handle_table_updated(self, event: Event):
        """Handle table update events"""
        
        # Find dataset by location
        location = event.data.get("location")
        for dataset in self.datasets.values():
            if dataset.location == location:
                # Update metadata
                dataset.updated_at = datetime.utcnow()
                
                # Check for schema changes
                new_schema = event.data.get("schema")
                if new_schema and dataset.schema_id:
                    # Register new schema version
                    current_schema = self.schemas.get(dataset.schema_id)
                    if current_schema:
                        await self.register_schema(
                            dataset_id=dataset.id,
                            schema_definition=new_schema,
                            version=current_schema.version + 1
                        )
                        
    async def _handle_data_processed(self, event: Event):
        """Handle data processing events for lineage"""
        
        if not self.track_lineage:
            return
            
        # Extract lineage information
        source_datasets = event.data.get("sources", [])
        target_dataset = event.data.get("target")
        process_name = event.data.get("process_name", "unknown")
        process_type = event.data.get("process_type", "batch")
        
        if source_datasets and target_dataset:
            await self.create_lineage(
                source_datasets=source_datasets,
                target_dataset=target_dataset,
                process_name=process_name,
                process_type=process_type,
                metadata=event.data.get("metadata", {})
            )
            
    async def _run_discovery(self):
        """Background task for dataset discovery"""
        
        while True:
            try:
                # Discover datasets in lakehouse
                logger.info("running_dataset_discovery")
                
                # Check Iceberg tables
                # TODO: Implement Iceberg table discovery
                
                # Check Delta tables
                # TODO: Implement Delta table discovery
                
                # Sleep for discovery interval
                await asyncio.sleep(3600)  # Run hourly
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("discovery_error", error=str(e))
                await asyncio.sleep(300)  # Retry in 5 minutes
                
    async def _run_classification(self):
        """Background task for auto-classification"""
        
        while True:
            try:
                # Process classification queue
                # TODO: Implement classification queue processing
                
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("classification_error", error=str(e))
                await asyncio.sleep(60)
                
    async def _queue_classification(self, dataset_id: str):
        """Queue dataset for classification"""
        
        await self.event_bus.publish(Event(
            type="catalog.classification.requested",
            data={"dataset_id": dataset_id}
        ))
        
    def _validate_schema(self, schema: Dict[str, Any]):
        """Validate schema definition"""
        
        # Basic validation
        if not isinstance(schema, dict):
            raise ValueError("Schema must be a dictionary")
            
        if "fields" not in schema:
            raise ValueError("Schema must have 'fields'")
            
        # Validate fields
        for field in schema["fields"]:
            if "name" not in field or "type" not in field:
                raise ValueError("Each field must have 'name' and 'type'")
                
    def _check_compatibility(
        self,
        old_schema: Dict[str, Any],
        new_schema: Dict[str, Any],
        mode: str
    ) -> bool:
        """Check schema compatibility"""
        
        # Simple compatibility check
        # TODO: Implement proper compatibility checking based on mode
        
        if mode == "BACKWARD":
            # New schema can read old data
            old_fields = {f["name"] for f in old_schema.get("fields", [])}
            new_fields = {f["name"] for f in new_schema.get("fields", [])}
            
            # All old fields must exist in new schema
            return old_fields.issubset(new_fields)
            
        elif mode == "FORWARD":
            # Old schema can read new data
            old_fields = {f["name"] for f in old_schema.get("fields", [])}
            new_fields = {f["name"] for f in new_schema.get("fields", [])}
            
            # All new fields must exist in old schema
            return new_fields.issubset(old_fields)
            
        elif mode == "FULL":
            # Both backward and forward compatible
            return (self._check_compatibility(old_schema, new_schema, "BACKWARD") and
                   self._check_compatibility(old_schema, new_schema, "FORWARD"))
                   
        return True
        
    def _match_filters(self, dataset: Dataset, filters: Dict[str, Any]) -> bool:
        """Check if dataset matches filters"""
        
        for key, value in filters.items():
            if key == "format" and dataset.format != value:
                return False
            elif key == "owner" and dataset.owner != value:
                return False
            elif key == "tags":
                if isinstance(value, list):
                    if not any(tag in dataset.tags for tag in value):
                        return False
                elif value not in dataset.tags:
                    return False
                    
        return True 