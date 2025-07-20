"""
ML Platform Service Integration

Provides integration with the Unified ML Platform Service for:
- Training data preparation from data lake
- Feature engineering pipelines
- Dataset registration for ML experiments
- Model training data access patterns
"""

import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
import asyncio
import httpx
import pandas as pd
import pyarrow.parquet as pq

from ..lake.medallion_architecture import MedallionLakeManager
from ..lake.transformation_engine import TransformationEngine
from ..core.cache_manager import DataCacheManager
from ..federation.federated_query_engine import FederatedQueryEngine

logger = logging.getLogger(__name__)


class MLPlatformIntegration:
    """Integration with Unified ML Platform Service"""
    
    def __init__(self,
                 lake_manager: MedallionLakeManager,
                 transformation_engine: TransformationEngine,
                 cache_manager: DataCacheManager,
                 federated_engine: FederatedQueryEngine,
                 ml_service_url: str = "http://unified-ml-platform-service:8000"):
        self.lake_manager = lake_manager
        self.transformation_engine = transformation_engine
        self.cache_manager = cache_manager
        self.federated_engine = federated_engine
        self.ml_service_url = ml_service_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
    async def prepare_training_data(self,
                                   source_path: str,
                                   feature_config: Dict[str, Any],
                                   experiment_id: Optional[str] = None,
                                   target_zone: str = "gold") -> Dict[str, Any]:
        """
        Prepare training data from data lake for ML model training
        
        Args:
            source_path: Path to source data in lake
            feature_config: Feature engineering configuration
            experiment_id: Optional MLflow experiment ID
            target_zone: Target zone for processed data (default: gold)
            
        Returns:
            Training data metadata including path and statistics
        """
        try:
            # Apply feature engineering transformations
            transformations = self._build_feature_transformations(feature_config)
            
            # Generate target path
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            target_path = f"ml_training/{experiment_id or 'default'}/features_{timestamp}"
            
            # Apply transformations
            result = await self.transformation_engine.transform(
                source_path=source_path,
                target_path=target_path,
                transformations=transformations
            )
            
            # Profile the dataset
            stats = await self._profile_training_data(target_path)
            
            # Register dataset with ML platform
            dataset_info = await self._register_ml_dataset(
                path=target_path,
                experiment_id=experiment_id,
                feature_config=feature_config,
                statistics=stats
            )
            
            return {
                "dataset_id": dataset_info["dataset_id"],
                "path": target_path,
                "format": "parquet",
                "statistics": stats,
                "features": list(feature_config.keys()),
                "row_count": stats.get("row_count", 0),
                "size_bytes": stats.get("size_bytes", 0)
            }
            
        except Exception as e:
            logger.error(f"Failed to prepare training data: {e}")
            raise
            
    async def create_feature_store_dataset(self,
                                          query: str,
                                          feature_group: str,
                                          feature_definitions: List[Dict[str, Any]],
                                          refresh_schedule: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a feature store dataset using federated queries
        
        Args:
            query: SQL query to extract features
            feature_group: Feature group name
            feature_definitions: List of feature definitions
            refresh_schedule: Optional cron schedule for updates
            
        Returns:
            Feature store dataset metadata
        """
        try:
            # Use the new feature store implementation
            from ..feature_store.models import FeatureGroup, Feature, FeatureType, FeatureTransformType, FeatureSourceType
            
            # Convert feature definitions to Feature objects
            features = []
            for feat_def in feature_definitions:
                feature = Feature(
                    name=feat_def["name"],
                    description=feat_def.get("description", ""),
                    type=FeatureType(feat_def.get("type", "float")),
                    transform=FeatureTransformType(feat_def.get("transform", "none")),
                    transform_params=feat_def.get("transform_params"),
                    tags=feat_def.get("tags", []),
                    owner=feat_def.get("owner", "ml-platform"),
                    source_columns=feat_def.get("source_columns", []),
                    derivation=feat_def.get("expression")
                )
                features.append(feature)
            
            # Create feature group
            feature_group_obj = FeatureGroup(
                name=feature_group,
                description=f"Features created from query: {query[:100]}...",
                features=features,
                primary_keys=feature_definitions[0].get("entity_keys", ["entity_id"]),
                event_time_column=feature_definitions[0].get("timestamp_column"),
                source_type=FeatureSourceType.FEDERATED,
                source_query=query,
                owner="ml-platform",
                versioning_enabled=True,
                online_store={"enabled": True} if refresh_schedule else None
            )
            
            # Register with feature store
            feature_store = self.cache_manager.get("feature_store_manager")  # Get from app state
            if feature_store:
                result = await feature_store.create_feature_group(feature_group_obj)
                
                # Compute initial features
                compute_result = await feature_store.compute_features(
                    feature_group_name=feature_group,
                    incremental=False
                )
                
                # Schedule refresh if requested
                if refresh_schedule:
                    # Register with pipeline coordinator
                    await self._schedule_feature_refresh(feature_group, refresh_schedule)
                
                return {
                    **result,
                    **compute_result,
                    "refresh_schedule": refresh_schedule
                }
            else:
                # Fallback to original implementation
                return await self._legacy_create_feature_store_dataset(
                    query, feature_group, feature_definitions, refresh_schedule
                )
            
        except Exception as e:
            logger.error(f"Failed to create feature store dataset: {e}")
            raise
            
    async def get_training_features(self,
                                   entity_df: pd.DataFrame,
                                   feature_groups: List[str],
                                   timestamp_column: str = "event_timestamp") -> pd.DataFrame:
        """
        Get training features with point-in-time correctness
        
        Args:
            entity_df: DataFrame with entities and timestamps
            feature_groups: List of feature groups to retrieve
            timestamp_column: Name of timestamp column
            
        Returns:
            Training data with features
        """
        try:
            from ..feature_store.models import FeatureSet
            
            # Get feature store manager
            feature_store = self.cache_manager.get("feature_store_manager")
            if not feature_store:
                raise ValueError("Feature store not available")
            
            # Get all features from specified groups
            all_features = []
            for group_name in feature_groups:
                group = await feature_store.registry.get_feature_group(group_name)
                if group:
                    all_features.extend([f"{group_name}.{f.name}" for f in group.features])
            
            # Create feature set
            feature_set = FeatureSet(
                name="ml_training_features",
                feature_groups=feature_groups,
                features=all_features,
                entity_keys=list(entity_df.columns.difference([timestamp_column])),
                timestamp_column=timestamp_column,
                created_by="ml-platform"
            )
            
            # Get historical features
            return await feature_store.get_historical_features(
                entity_df=entity_df,
                feature_set=feature_set,
                timestamp_column=timestamp_column
            )
            
        except Exception as e:
            logger.error(f"Failed to get training features: {e}")
            raise
            
    async def register_model_features(self,
                                    model_id: str,
                                    feature_groups: List[str],
                                    feature_importance: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        """
        Register features used by a model for tracking
        
        Args:
            model_id: ML model ID
            feature_groups: Feature groups used
            feature_importance: Optional feature importance scores
            
        Returns:
            Registration results
        """
        try:
            # Track model-feature lineage
            for group_name in feature_groups:
                await self.lineage_tracker.track_dependency(
                    source_entity=f"feature_group:{group_name}",
                    target_entity=f"model:{model_id}",
                    dependency_type="model_training",
                    metadata={
                        "feature_importance": feature_importance,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
            
            # Register with ML platform
            response = await self.http_client.post(
                f"{self.ml_service_url}/api/v1/models/{model_id}/features",
                json={
                    "feature_groups": feature_groups,
                    "feature_importance": feature_importance
                }
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to register model features: {e}")
            raise
            
    async def export_for_automl(self,
                               dataset_path: str,
                               target_column: str,
                               problem_type: str,
                               validation_split: float = 0.2) -> Dict[str, Any]:
        """
        Export dataset for AutoML training
        
        Args:
            dataset_path: Path to dataset in lake
            target_column: Target column for prediction
            problem_type: Type of ML problem (classification, regression, etc.)
            validation_split: Validation data split ratio
            
        Returns:
            AutoML dataset metadata
        """
        try:
            # Read dataset from lake
            df = self.lake_manager.spark.read.parquet(f"{self.lake_manager.base_path}/{dataset_path}")
            
            # Perform train/validation split
            train_df, val_df = df.randomSplit([1 - validation_split, validation_split], seed=42)
            
            # Save splits
            train_path = f"automl/{problem_type}/train_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            val_path = f"automl/{problem_type}/val_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            
            train_df.write.mode("overwrite").parquet(f"{self.lake_manager.base_path}/{train_path}")
            val_df.write.mode("overwrite").parquet(f"{self.lake_manager.base_path}/{val_path}")
            
            # Submit to AutoML
            response = await self.http_client.post(
                f"{self.ml_service_url}/api/v1/training/automl",
                params={
                    "problem_type": problem_type,
                    "target_column": target_column,
                    "tenant_id": "data-platform"
                },
                json={
                    "data_source": {
                        "type": "data_lake",
                        "train_path": train_path,
                        "validation_path": val_path
                    },
                    "time_budget": 3600,
                    "optimization_metric": self._get_default_metric(problem_type)
                }
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to export for AutoML: {e}")
            raise
            
    async def prepare_federated_learning_data(self,
                                            query: str,
                                            participant_column: str,
                                            min_samples_per_participant: int = 100) -> Dict[str, Any]:
        """
        Prepare data for federated learning by partitioning by participant
        
        Args:
            query: Query to extract training data
            participant_column: Column identifying participants
            min_samples_per_participant: Minimum samples required per participant
            
        Returns:
            Federated learning dataset metadata
        """
        try:
            # Execute query
            result = await self.federated_engine.execute_query(query)
            df = pd.DataFrame(result["rows"], columns=[col["name"] for col in result["columns"]])
            
            # Partition by participant
            participant_data = {}
            for participant, group in df.groupby(participant_column):
                if len(group) >= min_samples_per_participant:
                    participant_data[participant] = group
                    
            # Save partitioned data
            fl_path = f"federated_learning/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            metadata = {
                "participants": list(participant_data.keys()),
                "partitions": {}
            }
            
            for participant, data in participant_data.items():
                partition_path = f"{fl_path}/{participant}"
                await self._save_features_to_lake(data, partition_path)
                metadata["partitions"][participant] = {
                    "path": partition_path,
                    "samples": len(data),
                    "features": list(data.columns)
                }
                
            # Register with federated learning service
            response = await self.http_client.post(
                f"{self.ml_service_url}/api/v1/federated/sessions",
                params={"tenant_id": "data-platform"},
                json={
                    "name": f"FL Dataset {datetime.utcnow().isoformat()}",
                    "model_type": "tabular",
                    "dataset_requirements": {
                        "base_path": fl_path,
                        "partitions": metadata["partitions"]
                    },
                    "min_participants": len(participant_data) // 2,
                    "num_rounds": 10
                }
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Failed to prepare federated learning data: {e}")
            raise
            
    async def stream_training_data(self,
                                  dataset_path: str,
                                  batch_size: int = 1000,
                                  shuffle: bool = True) -> AsyncIterator[pd.DataFrame]:
        """
        Stream training data in batches for online learning
        
        Args:
            dataset_path: Path to dataset in lake
            batch_size: Batch size for streaming
            shuffle: Whether to shuffle data
            
        Yields:
            Batches of training data as DataFrames
        """
        try:
            # Get dataset info
            df = self.lake_manager.spark.read.parquet(f"{self.lake_manager.base_path}/{dataset_path}")
            
            if shuffle:
                df = df.orderBy(self.lake_manager.spark.sql.functions.rand())
                
            # Convert to pandas iterator
            for batch_df in df.toPandas().itertuples(index=False, chunksize=batch_size):
                yield pd.DataFrame(batch_df)
                
        except Exception as e:
            logger.error(f"Failed to stream training data: {e}")
            raise
            
    # Helper methods
    
    def _build_feature_transformations(self, feature_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Build feature engineering transformations"""
        transformations = []
        
        for feature_name, config in feature_config.items():
            if config["type"] == "numeric":
                if config.get("normalize"):
                    transformations.append({
                        "type": "normalize",
                        "column": feature_name,
                        "method": config.get("normalization_method", "min_max")
                    })
                    
            elif config["type"] == "categorical":
                transformations.append({
                    "type": "encode",
                    "column": feature_name,
                    "method": config.get("encoding_method", "one_hot")
                })
                
            elif config["type"] == "text":
                transformations.append({
                    "type": "vectorize",
                    "column": feature_name,
                    "method": config.get("vectorization_method", "tfidf"),
                    "max_features": config.get("max_features", 1000)
                })
                
        return transformations
        
    async def _profile_training_data(self, data_path: str) -> Dict[str, Any]:
        """Profile training dataset"""
        df = self.lake_manager.spark.read.parquet(f"{self.lake_manager.base_path}/{data_path}")
        
        return {
            "row_count": df.count(),
            "column_count": len(df.columns),
            "columns": df.columns,
            "size_bytes": self.lake_manager.spark._jvm.org.apache.spark.util.SizeEstimator.estimate(df._jdf),
            "null_counts": {col: df.filter(df[col].isNull()).count() for col in df.columns[:5]},  # Sample
            "numeric_stats": df.describe().toPandas().to_dict()
        }
        
    async def _register_ml_dataset(self,
                                  path: str,
                                  experiment_id: Optional[str],
                                  feature_config: Dict[str, Any],
                                  statistics: Dict[str, Any]) -> Dict[str, Any]:
        """Register dataset with ML platform"""
        response = await self.http_client.post(
            f"{self.ml_service_url}/api/v1/datasets",
            params={"tenant_id": "data-platform"},
            json={
                "name": f"Training Data {datetime.utcnow().isoformat()}",
                "path": path,
                "experiment_id": experiment_id,
                "feature_config": feature_config,
                "statistics": statistics,
                "created_by": "data-platform-service"
            }
        )
        response.raise_for_status()
        return response.json()
        
    async def _save_features_to_lake(self, df: pd.DataFrame, path: str):
        """Save features DataFrame to data lake"""
        # Convert to Spark DataFrame
        spark_df = self.lake_manager.spark.createDataFrame(df)
        
        # Save to lake
        spark_df.write.mode("overwrite").parquet(f"{self.lake_manager.base_path}/{path}")
        
    def _get_default_metric(self, problem_type: str) -> str:
        """Get default optimization metric for problem type"""
        metrics = {
            "classification": "f1_score",
            "regression": "rmse",
            "time_series": "mape",
            "clustering": "silhouette_score"
        }
        return metrics.get(problem_type, "accuracy")
        
    async def close(self):
        """Clean up resources"""
        await self.http_client.aclose() 