"""
Feature Store Manager

Manages feature lifecycle including:
- Feature registration and versioning
- Feature computation and storage
- Time-travel queries
- Feature quality monitoring
"""

import logging
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
import asyncio
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import json

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from ..lake.medallion_architecture import MedallionLakeManager
from ..lineage.lineage_tracker import DataLineageTracker
from ..quality.profiler import DataQualityProfiler
from ..core.cache_manager import DataCacheManager
from .models import (
    FeatureGroup,
    Feature,
    FeatureVersion,
    FeatureSet,
    FeatureView,
    FeatureType,
    FeatureTransformType
)
from .feature_registry import FeatureRegistry
from .feature_server import FeatureServer
from .ignite_feature_cache import IgniteFeatureCache

logger = logging.getLogger(__name__)


class FeatureStoreManager:
    """
    Manages feature lifecycle including:
    - Feature registration and versioning
    - Feature computation and storage
    - Time-travel queries
    - Feature quality monitoring
    - Online/offline serving coordination
    """
    
    def __init__(self,
                 lake_manager: MedallionLakeManager,
                 lineage_tracker: DataLineageTracker,
                 quality_profiler: DataQualityProfiler,
                 cache_manager: DataCacheManager,
                 feature_zone: str = "gold"):  # Features typically in gold zone
        self.lake_manager = lake_manager
        self.lineage_tracker = lineage_tracker
        self.quality_profiler = quality_profiler
        self.cache_manager = cache_manager
        self.feature_zone = feature_zone
        
        # Feature registry
        self.registry = FeatureRegistry()
        
        # Feature server for online serving
        self.feature_server = FeatureServer(
            registry=self.registry,
            cache_ttl=300,
            max_batch_size=1000
        )
        
        # Ignite cache for direct access
        self.ignite_cache = IgniteFeatureCache(
            cache_name="feature_store",
            ttl_seconds=300,
            enable_sql=True
        )
        
        # Feature paths
        self.feature_path = f"{self.lake_manager.zone_paths[self.feature_zone]}/features"
        
        # Statistics
        self.stats = {
            "features_computed": 0,
            "features_served": 0,
            "cache_hits": 0,
            "cache_misses": 0
        }
    
    async def initialize(self) -> None:
        """Initialize feature store components"""
        await self.registry.initialize()
        await self.feature_server.initialize()
        await self.ignite_cache.connect()
        logger.info("Feature store manager initialized")
    
    async def create_feature_group(self, feature_group: FeatureGroup) -> Dict[str, Any]:
        """
        Create a new feature group
        
        Args:
            feature_group: Feature group definition
            
        Returns:
            Feature group metadata
        """
        try:
            # Validate feature group
            await self._validate_feature_group(feature_group)
            
            # Register in catalog
            group_id = await self.registry.register_feature_group(feature_group)
            
            # Create storage paths
            offline_path = f"{self.offline_store_path}/{feature_group.name}"
            self.lake_manager.spark._jvm.java.nio.file.Files.createDirectories(
                self.lake_manager.spark._jvm.java.nio.file.Paths.get(
                    f"{self.lake_manager.base_path}/{offline_path}"
                )
            )
            
            # Initialize version tracking
            await self._initialize_version_tracking(feature_group.name)
            
            # Track lineage
            await self.lineage_tracker.track_entity_creation(
                entity_type="feature_group",
                entity_id=group_id,
                metadata={
                    "name": feature_group.name,
                    "features": [f.name for f in feature_group.features],
                    "source_type": feature_group.source_type.value
                }
            )
            
            logger.info(f"Created feature group: {feature_group.name}")
            
            return {
                "feature_group_id": group_id,
                "name": feature_group.name,
                "offline_path": offline_path,
                "status": "created"
            }
            
        except Exception as e:
            logger.error(f"Failed to create feature group: {e}")
            raise
            
    async def compute_features(self,
                             feature_group_name: str,
                             start_date: Optional[datetime] = None,
                             end_date: Optional[datetime] = None,
                             incremental: bool = True) -> Dict[str, Any]:
        """
        Compute features for a feature group
        
        Args:
            feature_group_name: Name of feature group
            start_date: Start date for computation
            end_date: End date for computation
            incremental: Whether to compute incrementally
            
        Returns:
            Computation results
        """
        try:
            # Get feature group
            feature_group = await self.registry.get_feature_group(feature_group_name)
            if not feature_group:
                raise ValueError(f"Feature group not found: {feature_group_name}")
            
            # Determine computation range
            if incremental:
                last_computed = await self._get_last_computed_timestamp(feature_group_name)
                if last_computed:
                    start_date = last_computed
                    
            # Execute source query
            df = await self._execute_feature_query(feature_group, start_date, end_date)
            
            # Apply transformations
            for feature in feature_group.features:
                df = await self._apply_feature_transformation(df, feature)
                
            # Profile features
            quality_stats = await self._profile_features(df, feature_group)
            
            # Version the features
            version = await self._create_feature_version(
                feature_group_name=feature_group_name,
                df=df,
                quality_stats=quality_stats
            )
            
            # Store offline
            offline_path = await self._store_offline_features(
                feature_group_name=feature_group_name,
                version=version,
                df=df
            )
            
            # Update online store if enabled
            if feature_group.online_store:
                await self._update_online_features(feature_group_name, df)
                
            # Track lineage
            await self._track_feature_lineage(feature_group, version, df)
            
            return {
                "feature_group": feature_group_name,
                "version": version,
                "row_count": df.count(),
                "offline_path": offline_path,
                "quality_stats": quality_stats,
                "computation_time": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to compute features: {e}")
            raise
            
    async def get_features(self,
                          feature_set: FeatureSet,
                          entities: Dict[str, Any],
                          timestamp: Optional[datetime] = None) -> pd.DataFrame:
        """
        Get features for given entities
        
        Args:
            feature_set: Feature set definition
            entities: Entity values
            timestamp: Point-in-time for features
            
        Returns:
            Features as DataFrame
        """
        try:
            # Get feature groups
            feature_groups = []
            for group_name in feature_set.feature_groups:
                group = await self.registry.get_feature_group(group_name)
                if group:
                    feature_groups.append(group)
                    
            if not feature_groups:
                raise ValueError("No valid feature groups found")
                
            # Check online store first
            if all(g.online_store for g in feature_groups) and not timestamp:
                features = await self._get_online_features(
                    feature_groups=feature_groups,
                    entities=entities,
                    features=feature_set.features
                )
                if features is not None:
                    return features
                    
            # Fall back to offline store
            return await self._get_offline_features(
                feature_groups=feature_groups,
                entities=entities,
                features=feature_set.features,
                timestamp=timestamp
            )
            
        except Exception as e:
            logger.error(f"Failed to get features: {e}")
            raise
            
    async def get_historical_features(self,
                                    entity_df: pd.DataFrame,
                                    feature_set: FeatureSet,
                                    timestamp_column: str = "event_timestamp") -> pd.DataFrame:
        """
        Get historical features with point-in-time correctness
        
        Args:
            entity_df: DataFrame with entities and timestamps
            feature_set: Feature set definition
            timestamp_column: Name of timestamp column
            
        Returns:
            Features joined with entities
        """
        try:
            # Convert to Spark DataFrame
            spark_entity_df = self.lake_manager.spark.createDataFrame(entity_df)
            
            # Get feature groups
            result_df = spark_entity_df
            
            for group_name in feature_set.feature_groups:
                feature_group = await self.registry.get_feature_group(group_name)
                if not feature_group:
                    continue
                    
                # Get versioned features with time travel
                features_df = await self._get_versioned_features(
                    feature_group_name=group_name,
                    timestamp_column=timestamp_column
                )
                
                # Point-in-time join
                join_keys = list(set(feature_set.entity_keys) & set(feature_group.primary_keys))
                
                if feature_group.event_time_column:
                    # Time-based join
                    result_df = self._point_in_time_join(
                        entity_df=result_df,
                        feature_df=features_df,
                        entity_keys=join_keys,
                        entity_timestamp=timestamp_column,
                        feature_timestamp=feature_group.event_time_column
                    )
                else:
                    # Regular join
                    result_df = result_df.join(
                        features_df.select(join_keys + feature_set.features),
                        on=join_keys,
                        how=feature_set.join_type
                    )
                    
            # Convert back to pandas
            return result_df.toPandas()
            
        except Exception as e:
            logger.error(f"Failed to get historical features: {e}")
            raise
            
    async def create_feature_view(self, feature_view: FeatureView) -> Dict[str, Any]:
        """
        Create materialized feature view
        
        Args:
            feature_view: Feature view definition
            
        Returns:
            View creation results
        """
        try:
            # Register view
            view_id = await self.registry.register_feature_view(feature_view)
            
            # Materialize initial data
            if feature_view.materialization_enabled:
                await self._materialize_feature_view(feature_view)
                
            # Set up refresh schedule
            if feature_view.refresh_schedule:
                await self._schedule_view_refresh(feature_view)
                
            # Enable online serving
            if feature_view.online_enabled:
                await self._enable_online_serving(feature_view)
                
            return {
                "view_id": view_id,
                "name": feature_view.name,
                "status": "created",
                "materialized": feature_view.materialization_enabled,
                "online_enabled": feature_view.online_enabled
            }
            
        except Exception as e:
            logger.error(f"Failed to create feature view: {e}")
            raise
            
    async def materialize_features(self,
                                 feature_group: FeatureGroup,
                                 start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None,
                                 write_to_online: bool = True) -> Dict[str, Any]:
        """
        Materialize features to offline and optionally online store
        
        Args:
            feature_group: Feature group to materialize
            start_date: Start date for materialization
            end_date: End date for materialization
            write_to_online: Whether to write to online store (Ignite)
            
        Returns:
            Materialization result
        """
        try:
            # Compute features
            df = await self.compute_feature_group(
                feature_group=feature_group,
                start_date=start_date,
                end_date=end_date
            )
            
            # Write to offline store (Iceberg/Delta)
            offline_path = await self._write_offline_features(
                df=df,
                feature_group=feature_group,
                timestamp=datetime.utcnow()
            )
            
            # Write to online store if requested
            online_count = 0
            if write_to_online:
                online_count = await self._write_online_features(
                    df=df,
                    feature_group=feature_group
                )
            
            return {
                "status": "success",
                "feature_group": feature_group.name,
                "offline_path": offline_path,
                "online_features_written": online_count,
                "row_count": df.count()
            }
            
        except Exception as e:
            logger.error(f"Failed to materialize features: {e}")
            raise
    
    async def _write_online_features(self,
                                   df: SparkDataFrame,
                                   feature_group: FeatureGroup) -> int:
        """Write features to online store (Ignite)"""
        try:
            count = 0
            batch_size = 1000
            feature_batch = {}
            
            # Collect features in batches
            for row in df.toLocalIterator():
                # Build entity ID
                entity_values = []
                for key in feature_group.entity_keys:
                    if key in row:
                        entity_values.append(str(row[key]))
                entity_id = "_".join(entity_values) if entity_values else str(row.get("id", "unknown"))
                
                # Add each feature to batch
                for feature in feature_group.features:
                    if feature.name in row:
                        feature_key = f"{feature_group.name}.{feature.name}"
                        feature_batch[(feature_key, entity_id)] = row[feature.name]
                
                # Write batch when full
                if len(feature_batch) >= batch_size:
                    written = await self.ignite_cache.put_features_batch(
                        features=feature_batch,
                        ttl=300  # 5 minutes default TTL
                    )
                    count += written
                    feature_batch = {}
            
            # Write remaining features
            if feature_batch:
                written = await self.ignite_cache.put_features_batch(
                    features=feature_batch,
                    ttl=300
                )
                count += written
            
            logger.info(f"Wrote {count} features to online store")
            return count
            
        except Exception as e:
            logger.error(f"Failed to write online features: {e}")
            raise
    
    async def get_online_features(self,
                                entities: Dict[str, Any],
                                features: List[str]) -> Dict[str, Any]:
        """
        Get features from online store for serving
        
        Args:
            entities: Entity values
            features: List of features to retrieve
            
        Returns:
            Feature values
        """
        from .models import FeatureRequest
        
        request = FeatureRequest(
            entities=entities,
            features=features
        )
        
        response = await self.feature_server.get_online_features(request)
        self.stats["features_served"] += len(response.features)
        
        return response.features
            
    # Helper methods
    
    async def _validate_feature_group(self, feature_group: FeatureGroup):
        """Validate feature group definition"""
        # Check for duplicate features
        feature_names = [f.name for f in feature_group.features]
        if len(feature_names) != len(set(feature_names)):
            raise ValueError("Duplicate feature names found")
            
        # Validate source configuration
        if feature_group.source_type == "batch" and not feature_group.source_query:
            raise ValueError("Batch feature group requires source_query")
            
        if feature_group.source_type == "stream" and not feature_group.source_stream:
            raise ValueError("Stream feature group requires source_stream")
            
    async def _initialize_version_tracking(self, feature_group_name: str):
        """Initialize version tracking for feature group"""
        version_path = f"{self.offline_store_path}/{feature_group_name}/_versions"
        self.lake_manager.spark._jvm.java.nio.file.Files.createDirectories(
            self.lake_manager.spark._jvm.java.nio.file.Paths.get(
                f"{self.lake_manager.base_path}/{version_path}"
            )
        )
        
        # Create version metadata file
        metadata = {
            "feature_group": feature_group_name,
            "created_at": datetime.utcnow().isoformat(),
            "current_version": 0,
            "versions": []
        }
        
        metadata_path = f"{version_path}/metadata.json"
        with open(f"{self.lake_manager.base_path}/{metadata_path}", "w") as f:
            json.dump(metadata, f)
            
    async def _execute_feature_query(self,
                                   feature_group: FeatureGroup,
                                   start_date: Optional[datetime],
                                   end_date: Optional[datetime]) -> SparkDataFrame:
        """Execute query to get source data"""
        query = feature_group.source_query
        
        # Add time filters if provided
        if start_date or end_date:
            time_column = feature_group.event_time_column
            if time_column:
                conditions = []
                if start_date:
                    conditions.append(f"{time_column} >= '{start_date}'")
                if end_date:
                    conditions.append(f"{time_column} <= '{end_date}'")
                    
                where_clause = " AND ".join(conditions)
                query = f"SELECT * FROM ({query}) WHERE {where_clause}"
                
        # Execute query
        return self.lake_manager.spark.sql(query)
        
    async def _apply_feature_transformation(self,
                                          df: SparkDataFrame,
                                          feature: Feature) -> SparkDataFrame:
        """Apply transformation to feature"""
        if feature.transform == FeatureTransformType.NONE:
            return df
            
        column = feature.name
        
        if feature.transform == FeatureTransformType.NORMALIZE:
            # Min-max normalization
            min_val = df.agg(F.min(column)).collect()[0][0]
            max_val = df.agg(F.max(column)).collect()[0][0]
            df = df.withColumn(
                column,
                (F.col(column) - min_val) / (max_val - min_val)
            )
            
        elif feature.transform == FeatureTransformType.STANDARDIZE:
            # Z-score standardization
            mean = df.agg(F.mean(column)).collect()[0][0]
            stddev = df.agg(F.stddev(column)).collect()[0][0]
            df = df.withColumn(
                column,
                (F.col(column) - mean) / stddev
            )
            
        elif feature.transform == FeatureTransformType.ONE_HOT:
            # One-hot encoding
            distinct_values = df.select(column).distinct().collect()
            for row in distinct_values:
                value = row[0]
                df = df.withColumn(
                    f"{column}_{value}",
                    F.when(F.col(column) == value, 1).otherwise(0)
                )
            df = df.drop(column)
            
        elif feature.transform == FeatureTransformType.CUSTOM:
            # Custom transformation
            if feature.derivation:
                df = df.withColumn(column, F.expr(feature.derivation))
                
        return df
        
    async def _profile_features(self,
                              df: SparkDataFrame,
                              feature_group: FeatureGroup) -> Dict[str, Any]:
        """Profile feature quality"""
        stats = {}
        
        for feature in feature_group.features:
            if feature.name not in df.columns:
                continue
                
            # Basic statistics
            feature_stats = df.select(feature.name).summary().toPandas()
            
            # Null count
            null_count = df.filter(F.col(feature.name).isNull()).count()
            total_count = df.count()
            
            stats[feature.name] = {
                "count": total_count,
                "null_count": null_count,
                "completeness": 1.0 - (null_count / total_count if total_count > 0 else 0),
                "summary": feature_stats.to_dict()
            }
            
        return stats
        
    async def _create_feature_version(self,
                                    feature_group_name: str,
                                    df: SparkDataFrame,
                                    quality_stats: Dict[str, Any]) -> int:
        """Create new feature version"""
        # Get current version
        version_metadata_path = f"{self.offline_store_path}/{feature_group_name}/_versions/metadata.json"
        with open(f"{self.lake_manager.base_path}/{version_metadata_path}", "r") as f:
            metadata = json.load(f)
            
        # Increment version
        new_version = metadata["current_version"] + 1
        
        # Create version record
        version_record = {
            "version": new_version,
            "created_at": datetime.utcnow().isoformat(),
            "row_count": df.count(),
            "quality_stats": quality_stats,
            "schema": df.schema.jsonValue()
        }
        
        # Update metadata
        metadata["current_version"] = new_version
        metadata["versions"].append(version_record)
        
        with open(f"{self.lake_manager.base_path}/{version_metadata_path}", "w") as f:
            json.dump(metadata, f)
            
        return new_version
        
    async def _store_offline_features(self,
                                    feature_group_name: str,
                                    version: int,
                                    df: SparkDataFrame) -> str:
        """Store features in offline store"""
        # Version path
        version_path = f"{self.offline_store_path}/{feature_group_name}/v{version}"
        full_path = f"{self.lake_manager.base_path}/{version_path}"
        
        # Write as partitioned parquet
        df.write.mode("overwrite").parquet(full_path)
        
        # Create success marker
        success_path = f"{full_path}/_SUCCESS"
        Path(success_path).touch()
        
        return version_path
        
    async def _update_online_features(self,
                                    feature_group_name: str,
                                    df: SparkDataFrame):
        """Update online feature store"""
        # Convert to pandas for online store
        features_pd = df.toPandas()
        
        # Update cache
        for _, row in features_pd.iterrows():
            # Create cache key from primary keys
            cache_key = f"{self.online_store_config['cache_prefix']}:{feature_group_name}:{row['entity_id']}"
            
            # Store features
            await self.cache_manager.set(
                key=cache_key,
                value=row.to_dict(),
                ttl=86400  # 24 hours
            )
            
    async def _track_feature_lineage(self,
                                   feature_group: FeatureGroup,
                                   version: int,
                                   df: SparkDataFrame):
        """Track feature lineage"""
        # Track transformation lineage
        for feature in feature_group.features:
            if feature.source_columns:
                for source_col in feature.source_columns:
                    await self.lineage_tracker.track_transformation(
                        source_entity=f"column:{source_col}",
                        target_entity=f"feature:{feature_group.name}.{feature.name}",
                        transformation_type="feature_engineering",
                        metadata={
                            "version": version,
                            "transform": feature.transform.value
                        }
                    )
                    
    def _point_in_time_join(self,
                           entity_df: SparkDataFrame,
                           feature_df: SparkDataFrame,
                           entity_keys: List[str],
                           entity_timestamp: str,
                           feature_timestamp: str) -> SparkDataFrame:
        """Perform point-in-time join"""
        # Add time window for join
        window_duration = "1 day"  # Configurable
        
        # Create join condition
        join_conditions = [
            entity_df[key] == feature_df[key] for key in entity_keys
        ]
        
        # Add time condition
        join_conditions.append(
            (feature_df[feature_timestamp] <= entity_df[entity_timestamp]) &
            (feature_df[feature_timestamp] >= entity_df[entity_timestamp] - F.expr(f"INTERVAL {window_duration}"))
        )
        
        # Perform join
        joined = entity_df.join(
            feature_df,
            on=join_conditions,
            how="left"
        )
        
        # Keep only most recent feature values
        window_spec = F.window().partitionBy(entity_keys + [entity_timestamp]).orderBy(F.desc(feature_timestamp))
        
        return joined.withColumn(
            "row_num",
            F.row_number().over(window_spec)
        ).filter(F.col("row_num") == 1).drop("row_num")
        
    async def _get_last_computed_timestamp(self, feature_group_name: str) -> Optional[datetime]:
        """Get timestamp of last computed features"""
        try:
            version_metadata_path = f"{self.offline_store_path}/{feature_group_name}/_versions/metadata.json"
            with open(f"{self.lake_manager.base_path}/{version_metadata_path}", "r") as f:
                metadata = json.load(f)
                
            if metadata["versions"]:
                last_version = metadata["versions"][-1]
                return datetime.fromisoformat(last_version["created_at"])
                
        except Exception:
            pass
            
        return None 