"""
Data Lake Event Processors

Handles events for data ingestion, quality checks, and lineage tracking.
"""

import logging
from typing import Optional, Dict, Any
from datetime import datetime

from platformq_shared.event_framework import EventProcessor, event_handler
from platformq_shared.events import (
    DigitalAssetCreated,
    DigitalAssetUpdated,
    DataQualityCheckRequested,
    DataQualityCheckCompleted,
    DataIngestionRequested,
    DataIngestionCompleted,
    DataLineageUpdated,
    DataCatalogEntryCreated,
    ProcessingJobStarted,
    ProcessingJobCompleted
)
from platformq_shared.pulsar_client import get_pulsar_client
from .repository import (
    DataIngestionRepository,
    DataQualityRepository,
    DataLineageRepository,
    DataCatalogRepository,
    ProcessingJobRepository
)
from .models.medallion_architecture import (
    MedallionArchitecture,
    DataLayer,
    DataQualityLevel
)
from .quality.data_quality_manager import DataQualityManager
from .dependencies import get_spark_session, get_minio_client
from platformq_shared.database import get_db

logger = logging.getLogger(__name__)


class DataLakeEventProcessor(EventProcessor):
    """Event processor for data lake operations"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ingestion_repo = None
        self.quality_repo = None
        self.lineage_repo = None
        self.catalog_repo = None
        self.job_repo = None
        self.medallion = None
        self.quality_manager = None
    
    def initialize_resources(self):
        """Initialize required resources"""
        if not self.ingestion_repo:
            db = next(get_db())
            self.ingestion_repo = DataIngestionRepository(db)
            self.quality_repo = DataQualityRepository(db)
            self.lineage_repo = DataLineageRepository(db)
            self.catalog_repo = DataCatalogRepository(db)
            self.job_repo = ProcessingJobRepository(db)
            
            spark = get_spark_session()
            minio_client = get_minio_client()
            self.medallion = MedallionArchitecture(spark, minio_client)
            self.quality_manager = DataQualityManager(spark)
    
    @event_handler("persistent://public/default/asset-events")
    async def handle_asset_created(self, event: DigitalAssetCreated):
        """Handle new asset creation - trigger data ingestion if applicable"""
        self.initialize_resources()
        
        try:
            # Check if asset is data-related
            if event.asset_type in ["dataset", "data-stream", "data-model"]:
                # Create data ingestion request
                ingestion = self.ingestion_repo.create({
                    "asset_id": event.asset_id,
                    "source_name": event.metadata.get("source", "unknown"),
                    "source_type": event.asset_type,
                    "tenant_id": event.tenant_id,
                    "status": "pending",
                    "metadata": event.metadata
                })
                
                # Publish ingestion request event
                await self.publish_event(
                    DataIngestionRequested(
                        ingestion_id=ingestion.id,
                        asset_id=event.asset_id,
                        source_type=event.asset_type,
                        tenant_id=event.tenant_id
                    ),
                    "persistent://public/default/data-ingestion-events"
                )
                
                logger.info(f"Created ingestion request {ingestion.id} for asset {event.asset_id}")
        
        except Exception as e:
            logger.error(f"Error handling asset created event: {e}")
    
    @event_handler("persistent://public/default/data-ingestion-events")
    async def handle_ingestion_request(self, event: DataIngestionRequested):
        """Handle data ingestion request"""
        self.initialize_resources()
        
        try:
            # Update ingestion status
            ingestion = self.ingestion_repo.get(event.ingestion_id, event.tenant_id)
            if not ingestion:
                logger.error(f"Ingestion {event.ingestion_id} not found")
                return
            
            ingestion.status = "processing"
            self.ingestion_repo.update(ingestion.id, {"status": "processing"})
            
            # Create processing job
            job = self.job_repo.create({
                "job_type": "ingestion",
                "dataset_id": event.asset_id,
                "tenant_id": event.tenant_id,
                "status": "running",
                "started_at": datetime.utcnow()
            })
            
            # Perform ingestion based on source type
            if event.source_type == "dataset":
                # Ingest to bronze layer
                dataset_path = await self.medallion.ingest_to_bronze(
                    source_path=event.metadata.get("source_path"),
                    dataset_name=event.metadata.get("name"),
                    tenant_id=event.tenant_id,
                    metadata=event.metadata
                )
                
                # Create catalog entry
                catalog_entry = self.catalog_repo.create({
                    "dataset_id": event.asset_id,
                    "name": event.metadata.get("name"),
                    "description": event.metadata.get("description"),
                    "layer": DataLayer.BRONZE.value,
                    "path": dataset_path,
                    "schema": event.metadata.get("schema"),
                    "tags": event.metadata.get("tags", []),
                    "tenant_id": event.tenant_id
                })
                
                # Update ingestion status
                self.ingestion_repo.update(ingestion.id, {
                    "status": "completed",
                    "completed_at": datetime.utcnow(),
                    "output_path": dataset_path
                })
                
                # Update job status
                self.job_repo.update(job.id, {
                    "status": "completed",
                    "completed_at": datetime.utcnow()
                })
                
                # Publish completion event
                await self.publish_event(
                    DataIngestionCompleted(
                        ingestion_id=ingestion.id,
                        asset_id=event.asset_id,
                        layer=DataLayer.BRONZE.value,
                        path=dataset_path,
                        tenant_id=event.tenant_id
                    ),
                    "persistent://public/default/data-ingestion-events"
                )
                
                # Trigger quality check
                await self.publish_event(
                    DataQualityCheckRequested(
                        dataset_id=event.asset_id,
                        layer=DataLayer.BRONZE.value,
                        tenant_id=event.tenant_id
                    ),
                    "persistent://public/default/data-quality-events"
                )
            
        except Exception as e:
            logger.error(f"Error processing ingestion request: {e}")
            # Update statuses on failure
            self.ingestion_repo.update(ingestion.id, {
                "status": "failed",
                "error_message": str(e)
            })
            self.job_repo.update(job.id, {
                "status": "failed",
                "completed_at": datetime.utcnow(),
                "error_message": str(e)
            })
    
    @event_handler("persistent://public/default/data-quality-events")
    async def handle_quality_check_request(self, event: DataQualityCheckRequested):
        """Handle data quality check request"""
        self.initialize_resources()
        
        try:
            # Get dataset info
            catalog_entry = self.catalog_repo.query().filter_by(
                dataset_id=event.dataset_id,
                tenant_id=event.tenant_id
            ).first()
            
            if not catalog_entry:
                logger.error(f"Dataset {event.dataset_id} not found in catalog")
                return
            
            # Create quality check record
            quality_check = self.quality_repo.create({
                "dataset_id": event.dataset_id,
                "layer": event.layer,
                "tenant_id": event.tenant_id,
                "status": "running",
                "check_date": datetime.utcnow()
            })
            
            # Run quality checks
            quality_results = await self.quality_manager.run_quality_checks(
                dataset_path=catalog_entry.path,
                layer=DataLayer(event.layer),
                rules=event.rules or []
            )
            
            # Update quality check with results
            self.quality_repo.update(quality_check.id, {
                "status": "passed" if quality_results["passed"] else "failed",
                "total_checks": quality_results["total_checks"],
                "passed_checks": quality_results["passed_checks"],
                "failed_checks": quality_results["failed_checks"],
                "quality_score": quality_results["quality_score"],
                "results": quality_results["details"]
            })
            
            # Publish completion event
            await self.publish_event(
                DataQualityCheckCompleted(
                    check_id=quality_check.id,
                    dataset_id=event.dataset_id,
                    layer=event.layer,
                    passed=quality_results["passed"],
                    quality_score=quality_results["quality_score"],
                    tenant_id=event.tenant_id
                ),
                "persistent://public/default/data-quality-events"
            )
            
            # If quality checks pass and in bronze layer, promote to silver
            if quality_results["passed"] and event.layer == DataLayer.BRONZE.value:
                await self._promote_to_silver(catalog_entry, event.tenant_id)
            
        except Exception as e:
            logger.error(f"Error running quality checks: {e}")
            self.quality_repo.update(quality_check.id, {
                "status": "error",
                "error_message": str(e)
            })
    
    async def _promote_to_silver(self, bronze_entry: Any, tenant_id: str):
        """Promote dataset from bronze to silver layer"""
        try:
            # Create processing job
            job = self.job_repo.create({
                "job_type": "promotion",
                "dataset_id": bronze_entry.dataset_id,
                "tenant_id": tenant_id,
                "status": "running",
                "started_at": datetime.utcnow(),
                "metadata": {"from_layer": "bronze", "to_layer": "silver"}
            })
            
            # Promote to silver layer
            silver_path = await self.medallion.promote_to_silver(
                bronze_path=bronze_entry.path,
                dataset_name=bronze_entry.name,
                tenant_id=tenant_id
            )
            
            # Create new catalog entry for silver layer
            silver_entry = self.catalog_repo.create({
                "dataset_id": bronze_entry.dataset_id,
                "name": bronze_entry.name,
                "description": bronze_entry.description,
                "layer": DataLayer.SILVER.value,
                "path": silver_path,
                "schema": bronze_entry.schema,
                "tags": bronze_entry.tags,
                "tenant_id": tenant_id
            })
            
            # Update job
            self.job_repo.update(job.id, {
                "status": "completed",
                "completed_at": datetime.utcnow()
            })
            
            # Create lineage
            self.lineage_repo.create({
                "source_dataset_id": bronze_entry.dataset_id,
                "target_dataset_id": silver_entry.dataset_id,
                "transformation_type": "promotion",
                "transformation_details": {"from_layer": "bronze", "to_layer": "silver"},
                "tenant_id": tenant_id
            })
            
            # Publish lineage update event
            await self.publish_event(
                DataLineageUpdated(
                    source_dataset_id=bronze_entry.dataset_id,
                    target_dataset_id=silver_entry.dataset_id,
                    transformation_type="promotion",
                    tenant_id=tenant_id
                ),
                "persistent://public/default/data-lineage-events"
            )
            
        except Exception as e:
            logger.error(f"Error promoting to silver layer: {e}")
            self.job_repo.update(job.id, {
                "status": "failed",
                "completed_at": datetime.utcnow(),
                "error_message": str(e)
            })
    
    @event_handler("persistent://public/default/data-transformation-events")
    async def handle_transformation_request(self, event: Dict[str, Any]):
        """Handle data transformation requests"""
        self.initialize_resources()
        
        try:
            # This would handle various transformation requests
            # such as aggregations, joins, feature engineering etc.
            transformation_type = event.get("transformation_type")
            source_datasets = event.get("source_datasets", [])
            target_dataset_name = event.get("target_dataset_name")
            tenant_id = event.get("tenant_id")
            
            # Create processing job
            job = self.job_repo.create({
                "job_type": f"transformation_{transformation_type}",
                "dataset_id": target_dataset_name,
                "tenant_id": tenant_id,
                "status": "running",
                "started_at": datetime.utcnow(),
                "metadata": event
            })
            
            # Perform transformation based on type
            if transformation_type == "aggregation":
                result_path = await self.medallion.perform_aggregation(
                    source_datasets=source_datasets,
                    aggregation_spec=event.get("aggregation_spec"),
                    target_name=target_dataset_name,
                    tenant_id=tenant_id
                )
            elif transformation_type == "feature_engineering":
                result_path = await self.medallion.perform_feature_engineering(
                    source_datasets=source_datasets,
                    feature_spec=event.get("feature_spec"),
                    target_name=target_dataset_name,
                    tenant_id=tenant_id
                )
            else:
                raise ValueError(f"Unknown transformation type: {transformation_type}")
            
            # Create catalog entry for result
            catalog_entry = self.catalog_repo.create({
                "dataset_id": f"{target_dataset_name}_{datetime.utcnow().timestamp()}",
                "name": target_dataset_name,
                "description": f"Result of {transformation_type} transformation",
                "layer": DataLayer.GOLD.value,
                "path": result_path,
                "tags": ["transformed", transformation_type],
                "tenant_id": tenant_id
            })
            
            # Create lineage records
            for source in source_datasets:
                self.lineage_repo.create({
                    "source_dataset_id": source,
                    "target_dataset_id": catalog_entry.dataset_id,
                    "transformation_type": transformation_type,
                    "transformation_details": event,
                    "tenant_id": tenant_id
                })
            
            # Update job
            self.job_repo.update(job.id, {
                "status": "completed",
                "completed_at": datetime.utcnow(),
                "output_path": result_path
            })
            
        except Exception as e:
            logger.error(f"Error handling transformation: {e}")
            self.job_repo.update(job.id, {
                "status": "failed",
                "completed_at": datetime.utcnow(),
                "error_message": str(e)
            }) 