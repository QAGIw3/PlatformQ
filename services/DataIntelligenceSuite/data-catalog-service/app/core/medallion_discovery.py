"""
Medallion Layer Auto-Discovery

Automatically discovers and catalogs data across Bronze, Silver, and Gold layers
in the data lake, maintaining metadata and quality information.
"""

import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
import asyncio
from dataclasses import dataclass
from enum import Enum
import json

from minio import Minio
from minio.error import S3Error
import pandas as pd
import pyarrow.parquet as pq
from delta import DeltaTable
import httpx

from app.core.atlas_client import AtlasClient
from app.core.config import settings

logger = logging.getLogger(__name__)


class DataLayer(str, Enum):
    """Data layer types in medallion architecture"""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class DiscoveredAsset:
    """Represents a discovered data asset"""
    name: str
    qualified_name: str
    layer: DataLayer
    path: str
    format: str
    size_bytes: int
    row_count: Optional[int]
    column_count: Optional[int]
    schema: Optional[Dict[str, Any]]
    partitions: Optional[List[str]]
    created_time: datetime
    modified_time: datetime
    quality_score: Optional[float]
    data_profile: Optional[Dict[str, Any]]
    tags: List[str]
    metadata: Dict[str, Any]


class MedallionDiscoveryEngine:
    """
    Discovers and catalogs data assets across medallion architecture layers
    """
    
    def __init__(
        self,
        atlas_client: AtlasClient,
        minio_client: Minio,
        quality_service_url: Optional[str] = None
    ):
        self.atlas_client = atlas_client
        self.minio_client = minio_client
        self.quality_service_url = quality_service_url or settings.quality_service_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Layer bucket mapping
        self.layer_buckets = {
            DataLayer.BRONZE: settings.minio_bucket_bronze or "lake-bronze",
            DataLayer.SILVER: settings.minio_bucket_silver or "lake-silver",
            DataLayer.GOLD: settings.minio_bucket_gold or "lake-gold"
        }
        
        # Format detection patterns
        self.format_patterns = {
            ".parquet": "parquet",
            ".csv": "csv",
            ".json": "json",
            ".avro": "avro",
            ".orc": "orc",
            "_delta_log": "delta"
        }
        
        # Discovery state
        self.last_discovery_time = {}
        self.discovered_assets = {}
        
    async def discover_all_layers(self, force_full_scan: bool = False) -> Dict[str, List[DiscoveredAsset]]:
        """
        Discover assets across all medallion layers
        """
        discoveries = {}
        
        for layer in DataLayer:
            logger.info(f"Starting discovery for {layer.value} layer")
            assets = await self.discover_layer(layer, force_full_scan)
            discoveries[layer.value] = assets
            logger.info(f"Discovered {len(assets)} assets in {layer.value} layer")
        
        return discoveries
    
    async def discover_layer(
        self,
        layer: DataLayer,
        force_full_scan: bool = False
    ) -> List[DiscoveredAsset]:
        """
        Discover assets in a specific layer
        """
        bucket = self.layer_buckets.get(layer)
        if not bucket:
            logger.warning(f"No bucket configured for {layer.value} layer")
            return []
        
        # Check if bucket exists
        try:
            if not self.minio_client.bucket_exists(bucket):
                logger.warning(f"Bucket {bucket} does not exist")
                return []
        except S3Error as e:
            logger.error(f"Error checking bucket {bucket}: {e}")
            return []
        
        # Get last discovery time
        last_scan = self.last_discovery_time.get(layer, datetime.min)
        if force_full_scan:
            last_scan = datetime.min
        
        discovered = []
        datasets = self._group_objects_by_dataset(bucket)
        
        for dataset_name, objects in datasets.items():
            try:
                # Check if dataset was modified since last scan
                latest_modified = max(obj.last_modified for obj in objects)
                if latest_modified.replace(tzinfo=None) <= last_scan:
                    continue
                
                # Discover dataset details
                asset = await self._discover_dataset(
                    dataset_name,
                    objects,
                    bucket,
                    layer
                )
                
                if asset:
                    discovered.append(asset)
                    
                    # Get quality score if available
                    if self.quality_service_url:
                        asset.quality_score = await self._get_quality_score(asset.qualified_name)
                    
                    # Auto-tag based on layer and content
                    asset.tags = self._generate_auto_tags(asset)
                    
            except Exception as e:
                logger.error(f"Error discovering dataset {dataset_name}: {e}")
        
        # Update last discovery time
        self.last_discovery_time[layer] = datetime.utcnow()
        
        return discovered
    
    def _group_objects_by_dataset(self, bucket: str) -> Dict[str, List]:
        """
        Group objects by dataset name
        """
        datasets = {}
        
        try:
            objects = self.minio_client.list_objects(bucket, recursive=True)
            
            for obj in objects:
                # Extract dataset name from path
                parts = obj.object_name.split('/')
                
                # Skip system files
                if any(part.startswith('.') or part.startswith('_') for part in parts[:2]):
                    continue
                
                # Dataset name is typically the first directory
                dataset_name = parts[0]
                
                if dataset_name not in datasets:
                    datasets[dataset_name] = []
                datasets[dataset_name].append(obj)
                
        except Exception as e:
            logger.error(f"Error listing objects in {bucket}: {e}")
            
        return datasets
    
    async def _discover_dataset(
        self,
        dataset_name: str,
        objects: List[Any],
        bucket: str,
        layer: DataLayer
    ) -> Optional[DiscoveredAsset]:
        """
        Discover details about a dataset
        """
        try:
            # Determine format
            format_type = self._detect_format(objects)
            
            # Calculate size
            total_size = sum(obj.size for obj in objects)
            
            # Get timestamps
            created_time = min(obj.last_modified for obj in objects)
            modified_time = max(obj.last_modified for obj in objects)
            
            # Build asset
            asset = DiscoveredAsset(
                name=dataset_name,
                qualified_name=f"{layer.value}.{bucket}.{dataset_name}",
                layer=layer,
                path=f"s3a://{bucket}/{dataset_name}",
                format=format_type,
                size_bytes=total_size,
                row_count=None,
                column_count=None,
                schema=None,
                partitions=None,
                created_time=created_time.replace(tzinfo=None),
                modified_time=modified_time.replace(tzinfo=None),
                quality_score=None,
                data_profile=None,
                tags=[],
                metadata={
                    "object_count": len(objects),
                    "layer": layer.value
                }
            )
            
            # Try to get schema and stats
            if format_type in ["parquet", "delta"]:
                schema_info = await self._extract_schema_info(asset, objects[0])
                if schema_info:
                    asset.schema = schema_info.get("schema")
                    asset.row_count = schema_info.get("row_count")
                    asset.column_count = schema_info.get("column_count")
                    asset.partitions = schema_info.get("partitions")
            
            # Extract data profile for sample
            if settings.enable_data_profiling:
                asset.data_profile = await self._profile_dataset(asset)
            
            return asset
            
        except Exception as e:
            logger.error(f"Error discovering dataset {dataset_name}: {e}")
            return None
    
    def _detect_format(self, objects: List[Any]) -> str:
        """
        Detect the format of the dataset
        """
        # Check for Delta Lake
        if any("_delta_log" in obj.object_name for obj in objects):
            return "delta"
        
        # Check file extensions
        for obj in objects:
            for pattern, format_type in self.format_patterns.items():
                if pattern in obj.object_name.lower():
                    return format_type
        
        return "unknown"
    
    async def _extract_schema_info(
        self,
        asset: DiscoveredAsset,
        sample_object: Any
    ) -> Optional[Dict[str, Any]]:
        """
        Extract schema information from the dataset
        """
        try:
            if asset.format == "parquet":
                # Download sample file
                response = self.minio_client.get_object(
                    asset.path.replace("s3a://", "").split('/')[0],
                    sample_object.object_name
                )
                
                # Read parquet metadata
                import io
                data = response.read()
                response.close()
                response.release_conn()
                
                parquet_file = pq.ParquetFile(io.BytesIO(data))
                schema = parquet_file.schema_arrow
                
                # Extract schema info
                schema_dict = {
                    "fields": [
                        {
                            "name": field.name,
                            "type": str(field.type),
                            "nullable": field.nullable
                        }
                        for field in schema
                    ]
                }
                
                # Get statistics
                metadata = parquet_file.metadata
                
                return {
                    "schema": schema_dict,
                    "row_count": metadata.num_rows,
                    "column_count": len(schema),
                    "partitions": self._extract_partitions(sample_object.object_name)
                }
                
            elif asset.format == "delta":
                # For Delta Lake, we'd need to use Delta Lake libraries
                # This is a simplified version
                return {
                    "schema": {"type": "delta_lake"},
                    "row_count": None,
                    "column_count": None,
                    "partitions": self._extract_partitions(sample_object.object_name)
                }
                
        except Exception as e:
            logger.error(f"Error extracting schema: {e}")
            
        return None
    
    def _extract_partitions(self, object_path: str) -> List[str]:
        """
        Extract partition columns from object path
        """
        partitions = []
        parts = object_path.split('/')
        
        for part in parts:
            if '=' in part:
                partition_col = part.split('=')[0]
                if partition_col not in partitions:
                    partitions.append(partition_col)
        
        return partitions
    
    async def _get_quality_score(self, qualified_name: str) -> Optional[float]:
        """
        Get quality score from quality service
        """
        try:
            response = await self.http_client.get(
                f"{self.quality_service_url}/api/v1/quality/score",
                params={"dataset": qualified_name}
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get("overall_score")
                
        except Exception as e:
            logger.debug(f"Could not get quality score: {e}")
            
        return None
    
    async def _profile_dataset(self, asset: DiscoveredAsset) -> Optional[Dict[str, Any]]:
        """
        Profile dataset to extract statistics
        """
        try:
            # This would connect to the data processing service
            # to run profiling jobs
            response = await self.http_client.post(
                f"{settings.batch_service_url}/api/v1/profile",
                json={
                    "dataset_path": asset.path,
                    "format": asset.format,
                    "sample_size": settings.profiling_sample_size
                }
            )
            
            if response.status_code == 200:
                return response.json()
                
        except Exception as e:
            logger.debug(f"Could not profile dataset: {e}")
            
        return None
    
    def _generate_auto_tags(self, asset: DiscoveredAsset) -> List[str]:
        """
        Generate automatic tags based on asset properties
        """
        tags = []
        
        # Layer tag
        tags.append(f"layer:{asset.layer.value}")
        
        # Format tag
        tags.append(f"format:{asset.format}")
        
        # Size-based tags
        size_gb = asset.size_bytes / (1024 ** 3)
        if size_gb < 1:
            tags.append("size:small")
        elif size_gb < 100:
            tags.append("size:medium")
        else:
            tags.append("size:large")
        
        # Quality tags
        if asset.quality_score:
            if asset.quality_score >= 0.9:
                tags.append("quality:high")
            elif asset.quality_score >= 0.7:
                tags.append("quality:medium")
            else:
                tags.append("quality:low")
        
        # Freshness tags
        age_days = (datetime.utcnow() - asset.modified_time).days
        if age_days <= 1:
            tags.append("freshness:real-time")
        elif age_days <= 7:
            tags.append("freshness:recent")
        elif age_days <= 30:
            tags.append("freshness:current")
        else:
            tags.append("freshness:historical")
        
        # Partitioned tag
        if asset.partitions:
            tags.append("partitioned")
        
        return tags
    
    async def register_discovered_assets(
        self,
        assets: List[DiscoveredAsset],
        update_existing: bool = True
    ) -> Dict[str, Any]:
        """
        Register discovered assets in the catalog
        """
        registered = 0
        updated = 0
        failed = 0
        
        for asset in assets:
            try:
                # Check if asset exists
                existing = await self.atlas_client.get_entity_by_attribute(
                    type_name="dataset",
                    attr_name="qualifiedName",
                    attr_value=asset.qualified_name
                )
                
                if existing and update_existing:
                    # Update existing entity
                    await self._update_catalog_entity(existing["guid"], asset)
                    updated += 1
                elif not existing:
                    # Create new entity
                    await self._create_catalog_entity(asset)
                    registered += 1
                    
            except Exception as e:
                logger.error(f"Error registering asset {asset.qualified_name}: {e}")
                failed += 1
        
        return {
            "registered": registered,
            "updated": updated,
            "failed": failed,
            "total": len(assets)
        }
    
    async def _create_catalog_entity(self, asset: DiscoveredAsset):
        """
        Create a new catalog entity for discovered asset
        """
        entity = {
            "typeName": "dataset",
            "attributes": {
                "name": asset.name,
                "qualifiedName": asset.qualified_name,
                "description": f"Auto-discovered {asset.layer.value} layer dataset",
                "owner": "auto-discovery",
                "location": asset.path,
                "format": asset.format,
                "layer": asset.layer.value,
                "sizeBytes": asset.size_bytes,
                "rowCount": asset.row_count,
                "columnCount": asset.column_count,
                "createdTime": asset.created_time.isoformat(),
                "modifiedTime": asset.modified_time.isoformat(),
                "dataQualityScore": asset.quality_score,
                "partitionKeys": asset.partitions,
                "customAttributes": asset.metadata
            },
            "classifications": self._get_classifications(asset),
            "labels": asset.tags
        }
        
        # Add schema if available
        if asset.schema:
            entity["attributes"]["schema"] = json.dumps(asset.schema)
        
        await self.atlas_client.create_entity(entity)
    
    async def _update_catalog_entity(self, guid: str, asset: DiscoveredAsset):
        """
        Update existing catalog entity
        """
        updates = {
            "modifiedTime": asset.modified_time.isoformat(),
            "sizeBytes": asset.size_bytes,
            "rowCount": asset.row_count,
            "dataQualityScore": asset.quality_score,
            "labels": asset.tags
        }
        
        if asset.schema:
            updates["schema"] = json.dumps(asset.schema)
        
        await self.atlas_client.partial_update_entity(guid, updates)
    
    def _get_classifications(self, asset: DiscoveredAsset) -> List[str]:
        """
        Determine classifications for the asset
        """
        classifications = []
        
        # Layer-based classifications
        if asset.layer == DataLayer.BRONZE:
            classifications.append("RawData")
        elif asset.layer == DataLayer.SILVER:
            classifications.append("CleansedData")
        elif asset.layer == DataLayer.GOLD:
            classifications.append("BusinessReady")
        
        # Quality-based classifications
        if asset.quality_score and asset.quality_score >= 0.9:
            classifications.append("HighQuality")
        
        return classifications
    
    async def schedule_continuous_discovery(
        self,
        interval_minutes: int = 60,
        full_scan_interval_hours: int = 24
    ):
        """
        Schedule continuous discovery of medallion layers
        """
        last_full_scan = datetime.min
        
        while True:
            try:
                # Determine if full scan is needed
                force_full_scan = (
                    datetime.utcnow() - last_full_scan
                ).total_seconds() > full_scan_interval_hours * 3600
                
                # Run discovery
                discoveries = await self.discover_all_layers(force_full_scan)
                
                # Register discovered assets
                for layer, assets in discoveries.items():
                    if assets:
                        result = await self.register_discovered_assets(assets)
                        logger.info(
                            f"Registered {result['registered']} new and "
                            f"updated {result['updated']} existing assets in {layer} layer"
                        )
                
                if force_full_scan:
                    last_full_scan = datetime.utcnow()
                
                # Wait for next iteration
                await asyncio.sleep(interval_minutes * 60)
                
            except Exception as e:
                logger.error(f"Error in continuous discovery: {e}")
                await asyncio.sleep(60)  # Wait 1 minute on error
    
    async def cleanup(self):
        """
        Cleanup resources
        """
        await self.http_client.aclose() 