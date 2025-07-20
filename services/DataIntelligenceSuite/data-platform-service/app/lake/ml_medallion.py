"""
ML Medallion Architecture

Implements specialized data lake patterns for ML workloads including:
- Training data versioning and lineage
- Feature store integration
- Model artifact management
- Experiment tracking data
- Performance metrics aggregation
"""

import logging
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import hashlib
import pandas as pd
import numpy as np
from pathlib import Path

from minio import Minio
from minio.error import S3Error
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb

logger = logging.getLogger(__name__)


class MLDataLayer(Enum):
    """ML-specific data layers"""
    RAW_DATA = "raw_data"  # Original training data
    PROCESSED_DATA = "processed_data"  # Cleaned and transformed
    FEATURE_DATA = "feature_data"  # Engineered features
    TRAINING_DATA = "training_data"  # Train/val/test splits
    MODEL_ARTIFACTS = "model_artifacts"  # Trained models
    PREDICTIONS = "predictions"  # Model outputs
    METRICS = "metrics"  # Performance metrics


@dataclass
class TrainingDataset:
    """Represents a versioned training dataset"""
    dataset_id: str
    name: str
    version: str
    source_paths: List[str]
    split_ratios: Dict[str, float]  # train/val/test
    feature_columns: List[str]
    target_column: str
    created_at: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    statistics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FeatureSet:
    """Represents a feature set for ML"""
    feature_set_id: str
    name: str
    version: str
    features: List[Dict[str, Any]]  # Feature definitions
    entity_type: str
    created_at: datetime
    update_frequency: str  # batch/streaming
    statistics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ModelArtifact:
    """Represents a trained model artifact"""
    model_id: str
    name: str
    version: str
    algorithm: str
    framework: str
    artifact_path: str
    training_dataset_id: str
    feature_set_id: str
    metrics: Dict[str, float]
    parameters: Dict[str, Any]
    created_at: datetime
    size_bytes: int


class MLMedallionArchitecture:
    """Manages ML data in medallion architecture"""
    
    def __init__(self, minio_client: Minio, bucket_prefix: str = "ml"):
        self.minio = minio_client
        self.bucket_prefix = bucket_prefix
        self.duckdb_conn = duckdb.connect(':memory:')
        self._initialize_buckets()
        
    def _initialize_buckets(self):
        """Initialize ML-specific buckets"""
        buckets = [
            f"{self.bucket_prefix}-raw-data",
            f"{self.bucket_prefix}-processed-data",
            f"{self.bucket_prefix}-features",
            f"{self.bucket_prefix}-training-data",
            f"{self.bucket_prefix}-models",
            f"{self.bucket_prefix}-predictions",
            f"{self.bucket_prefix}-metrics"
        ]
        
        for bucket in buckets:
            try:
                if not self.minio.bucket_exists(bucket):
                    self.minio.make_bucket(bucket)
                    logger.info(f"Created bucket: {bucket}")
            except S3Error as e:
                logger.error(f"Error creating bucket {bucket}: {e}")
                
    async def ingest_training_data(self, data: pd.DataFrame, dataset_name: str,
                                  source: str = "upload") -> TrainingDataset:
        """Ingest raw training data into Bronze layer"""
        try:
            # Generate dataset ID and version
            dataset_id = f"ds-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            version = "v1.0"
            
            # Calculate data statistics
            statistics = self._calculate_data_statistics(data)
            
            # Convert to Parquet
            table = pa.Table.from_pandas(data)
            
            # Save to Bronze layer
            bronze_path = f"raw/{dataset_name}/{version}/data.parquet"
            bronze_bucket = f"{self.bucket_prefix}-raw-data"
            
            # Write to MinIO
            parquet_buffer = pa.BufferOutputStream()
            pq.write_table(table, parquet_buffer)
            
            self.minio.put_object(
                bronze_bucket,
                bronze_path,
                parquet_buffer.getvalue(),
                length=parquet_buffer.size
            )
            
            # Create dataset metadata
            dataset = TrainingDataset(
                dataset_id=dataset_id,
                name=dataset_name,
                version=version,
                source_paths=[f"s3://{bronze_bucket}/{bronze_path}"],
                split_ratios={"train": 0.7, "val": 0.15, "test": 0.15},
                feature_columns=list(data.columns),
                target_column="",  # To be set later
                created_at=datetime.utcnow(),
                metadata={
                    "source": source,
                    "row_count": len(data),
                    "column_count": len(data.columns),
                    "size_bytes": parquet_buffer.size
                },
                statistics=statistics
            )
            
            # Save metadata
            await self._save_dataset_metadata(dataset)
            
            logger.info(f"Ingested training data: {dataset_id}")
            return dataset
            
        except Exception as e:
            logger.error(f"Error ingesting training data: {e}")
            raise
            
    async def process_training_data(self, dataset_id: str,
                                   processing_config: Dict[str, Any]) -> TrainingDataset:
        """Process raw data to Silver layer"""
        try:
            # Load dataset metadata
            dataset = await self._load_dataset_metadata(dataset_id)
            
            # Load raw data
            raw_data = await self._load_parquet_data(dataset.source_paths[0])
            
            # Apply processing steps
            processed_data = raw_data.copy()
            
            # Handle missing values
            if processing_config.get("handle_missing", True):
                processed_data = self._handle_missing_values(
                    processed_data,
                    processing_config.get("missing_strategy", "mean")
                )
                
            # Normalize numerical features
            if processing_config.get("normalize", True):
                numerical_cols = processed_data.select_dtypes(include=[np.number]).columns
                processed_data[numerical_cols] = self._normalize_features(
                    processed_data[numerical_cols]
                )
                
            # Encode categorical features
            if processing_config.get("encode_categorical", True):
                categorical_cols = processed_data.select_dtypes(include=['object']).columns
                processed_data = self._encode_categorical_features(
                    processed_data,
                    categorical_cols
                )
                
            # Remove outliers
            if processing_config.get("remove_outliers", False):
                processed_data = self._remove_outliers(
                    processed_data,
                    threshold=processing_config.get("outlier_threshold", 3)
                )
                
            # Save to Silver layer
            silver_path = f"processed/{dataset.name}/{dataset.version}/data.parquet"
            silver_bucket = f"{self.bucket_prefix}-processed-data"
            
            table = pa.Table.from_pandas(processed_data)
            parquet_buffer = pa.BufferOutputStream()
            pq.write_table(table, parquet_buffer)
            
            self.minio.put_object(
                silver_bucket,
                silver_path,
                parquet_buffer.getvalue(),
                length=parquet_buffer.size
            )
            
            # Update dataset metadata
            dataset.source_paths.append(f"s3://{silver_bucket}/{silver_path}")
            dataset.metadata["processing_config"] = processing_config
            dataset.metadata["processed_at"] = datetime.utcnow().isoformat()
            dataset.statistics = self._calculate_data_statistics(processed_data)
            
            await self._save_dataset_metadata(dataset)
            
            logger.info(f"Processed training data: {dataset_id}")
            return dataset
            
        except Exception as e:
            logger.error(f"Error processing training data: {e}")
            raise
            
    async def create_feature_set(self, dataset_id: str,
                               feature_config: Dict[str, Any]) -> FeatureSet:
        """Create engineered features in Gold layer"""
        try:
            # Load processed data
            dataset = await self._load_dataset_metadata(dataset_id)
            data = await self._load_parquet_data(dataset.source_paths[-1])
            
            # Feature engineering
            features_df = data.copy()
            engineered_features = []
            
            # Polynomial features
            if feature_config.get("polynomial_features", False):
                numerical_cols = data.select_dtypes(include=[np.number]).columns
                for col in numerical_cols[:5]:  # Limit to prevent explosion
                    features_df[f"{col}_squared"] = data[col] ** 2
                    features_df[f"{col}_cubed"] = data[col] ** 3
                    engineered_features.extend([f"{col}_squared", f"{col}_cubed"])
                    
            # Interaction features
            if feature_config.get("interaction_features", False):
                numerical_cols = data.select_dtypes(include=[np.number]).columns
                for i, col1 in enumerate(numerical_cols[:5]):
                    for col2 in numerical_cols[i+1:6]:
                        features_df[f"{col1}_x_{col2}"] = data[col1] * data[col2]
                        engineered_features.append(f"{col1}_x_{col2}")
                        
            # Time-based features (if datetime column exists)
            datetime_cols = data.select_dtypes(include=['datetime']).columns
            if len(datetime_cols) > 0 and feature_config.get("time_features", False):
                for col in datetime_cols:
                    features_df[f"{col}_hour"] = pd.to_datetime(data[col]).dt.hour
                    features_df[f"{col}_dayofweek"] = pd.to_datetime(data[col]).dt.dayofweek
                    features_df[f"{col}_month"] = pd.to_datetime(data[col]).dt.month
                    engineered_features.extend([f"{col}_hour", f"{col}_dayofweek", f"{col}_month"])
                    
            # Statistical aggregations
            if feature_config.get("statistical_features", False):
                numerical_cols = data.select_dtypes(include=[np.number]).columns
                for col in numerical_cols[:10]:
                    # Rolling statistics would require time series data
                    features_df[f"{col}_zscore"] = (data[col] - data[col].mean()) / data[col].std()
                    engineered_features.append(f"{col}_zscore")
                    
            # Save feature set
            feature_set_id = f"fs-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            feature_path = f"features/{dataset.name}/{feature_set_id}/features.parquet"
            feature_bucket = f"{self.bucket_prefix}-features"
            
            table = pa.Table.from_pandas(features_df)
            parquet_buffer = pa.BufferOutputStream()
            pq.write_table(table, parquet_buffer)
            
            self.minio.put_object(
                feature_bucket,
                feature_path,
                parquet_buffer.getvalue(),
                length=parquet_buffer.size
            )
            
            # Create feature set metadata
            feature_definitions = []
            for col in features_df.columns:
                feature_def = {
                    "name": col,
                    "type": str(features_df[col].dtype),
                    "engineered": col in engineered_features,
                    "statistics": {
                        "mean": float(features_df[col].mean()) if features_df[col].dtype in [np.float64, np.int64] else None,
                        "std": float(features_df[col].std()) if features_df[col].dtype in [np.float64, np.int64] else None,
                        "min": float(features_df[col].min()) if features_df[col].dtype in [np.float64, np.int64] else None,
                        "max": float(features_df[col].max()) if features_df[col].dtype in [np.float64, np.int64] else None,
                    }
                }
                feature_definitions.append(feature_def)
                
            feature_set = FeatureSet(
                feature_set_id=feature_set_id,
                name=f"{dataset.name}_features",
                version="v1.0",
                features=feature_definitions,
                entity_type="training_dataset",
                created_at=datetime.utcnow(),
                update_frequency="batch",
                statistics={
                    "total_features": len(features_df.columns),
                    "engineered_features": len(engineered_features),
                    "feature_path": f"s3://{feature_bucket}/{feature_path}"
                }
            )
            
            # Save feature set metadata
            await self._save_feature_metadata(feature_set)
            
            logger.info(f"Created feature set: {feature_set_id}")
            return feature_set
            
        except Exception as e:
            logger.error(f"Error creating feature set: {e}")
            raise
            
    async def create_training_splits(self, feature_set_id: str,
                                   target_column: str,
                                   split_config: Dict[str, float]) -> Dict[str, str]:
        """Create train/validation/test splits"""
        try:
            # Load feature data
            feature_set = await self._load_feature_metadata(feature_set_id)
            feature_data = await self._load_parquet_data(
                feature_set.statistics["feature_path"]
            )
            
            # Shuffle data
            feature_data = feature_data.sample(frac=1, random_state=42).reset_index(drop=True)
            
            # Calculate split indices
            n_samples = len(feature_data)
            train_size = int(n_samples * split_config.get("train", 0.7))
            val_size = int(n_samples * split_config.get("val", 0.15))
            
            # Create splits
            train_data = feature_data.iloc[:train_size]
            val_data = feature_data.iloc[train_size:train_size + val_size]
            test_data = feature_data.iloc[train_size + val_size:]
            
            # Save splits
            training_bucket = f"{self.bucket_prefix}-training-data"
            split_paths = {}
            
            for split_name, split_data in [("train", train_data), ("val", val_data), ("test", test_data)]:
                split_path = f"splits/{feature_set.name}/{feature_set_id}/{split_name}.parquet"
                
                table = pa.Table.from_pandas(split_data)
                parquet_buffer = pa.BufferOutputStream()
                pq.write_table(table, parquet_buffer)
                
                self.minio.put_object(
                    training_bucket,
                    split_path,
                    parquet_buffer.getvalue(),
                    length=parquet_buffer.size
                )
                
                split_paths[split_name] = f"s3://{training_bucket}/{split_path}"
                
                logger.info(f"Created {split_name} split with {len(split_data)} samples")
                
            # Save split metadata
            split_metadata = {
                "feature_set_id": feature_set_id,
                "target_column": target_column,
                "split_config": split_config,
                "split_paths": split_paths,
                "split_sizes": {
                    "train": len(train_data),
                    "val": len(val_data),
                    "test": len(test_data)
                },
                "created_at": datetime.utcnow().isoformat()
            }
            
            metadata_path = f"splits/{feature_set.name}/{feature_set_id}/metadata.json"
            self.minio.put_object(
                training_bucket,
                metadata_path,
                json.dumps(split_metadata).encode(),
                length=len(json.dumps(split_metadata))
            )
            
            return split_paths
            
        except Exception as e:
            logger.error(f"Error creating training splits: {e}")
            raise
            
    async def save_model_artifact(self, model_path: str, model_metadata: Dict[str, Any]) -> ModelArtifact:
        """Save trained model artifact"""
        try:
            # Generate model ID
            model_id = f"model-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            
            # Read model file
            with open(model_path, 'rb') as f:
                model_data = f.read()
                
            # Calculate model hash
            model_hash = hashlib.sha256(model_data).hexdigest()
            
            # Save to model artifacts bucket
            artifact_bucket = f"{self.bucket_prefix}-models"
            artifact_path = f"models/{model_metadata['name']}/{model_id}/model.pkl"
            
            self.minio.put_object(
                artifact_bucket,
                artifact_path,
                model_data,
                length=len(model_data)
            )
            
            # Create model artifact metadata
            artifact = ModelArtifact(
                model_id=model_id,
                name=model_metadata["name"],
                version=model_metadata.get("version", "v1.0"),
                algorithm=model_metadata["algorithm"],
                framework=model_metadata["framework"],
                artifact_path=f"s3://{artifact_bucket}/{artifact_path}",
                training_dataset_id=model_metadata["training_dataset_id"],
                feature_set_id=model_metadata["feature_set_id"],
                metrics=model_metadata["metrics"],
                parameters=model_metadata["parameters"],
                created_at=datetime.utcnow(),
                size_bytes=len(model_data)
            )
            
            # Save artifact metadata
            metadata_path = f"models/{model_metadata['name']}/{model_id}/metadata.json"
            self.minio.put_object(
                artifact_bucket,
                metadata_path,
                json.dumps(artifact.__dict__, default=str).encode(),
                length=len(json.dumps(artifact.__dict__, default=str))
            )
            
            logger.info(f"Saved model artifact: {model_id}")
            return artifact
            
        except Exception as e:
            logger.error(f"Error saving model artifact: {e}")
            raise
            
    async def track_predictions(self, model_id: str, predictions: pd.DataFrame,
                              request_metadata: Dict[str, Any]) -> str:
        """Track model predictions for monitoring"""
        try:
            # Generate prediction batch ID
            batch_id = f"pred-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
            
            # Add metadata columns
            predictions["model_id"] = model_id
            predictions["batch_id"] = batch_id
            predictions["timestamp"] = datetime.utcnow()
            predictions["request_metadata"] = json.dumps(request_metadata)
            
            # Save predictions
            predictions_bucket = f"{self.bucket_prefix}-predictions"
            predictions_path = f"predictions/{model_id}/{batch_id}/predictions.parquet"
            
            table = pa.Table.from_pandas(predictions)
            parquet_buffer = pa.BufferOutputStream()
            pq.write_table(table, parquet_buffer)
            
            self.minio.put_object(
                predictions_bucket,
                predictions_path,
                parquet_buffer.getvalue(),
                length=parquet_buffer.size
            )
            
            logger.info(f"Tracked predictions batch: {batch_id}")
            return batch_id
            
        except Exception as e:
            logger.error(f"Error tracking predictions: {e}")
            raise
            
    async def aggregate_model_metrics(self, model_id: str,
                                    time_window: timedelta = timedelta(days=7)) -> Dict[str, Any]:
        """Aggregate model performance metrics"""
        try:
            # Query predictions within time window
            end_time = datetime.utcnow()
            start_time = end_time - time_window
            
            # Load predictions (simplified - in production use proper query)
            predictions_bucket = f"{self.bucket_prefix}-predictions"
            
            # Aggregate metrics
            aggregated_metrics = {
                "model_id": model_id,
                "time_window": {
                    "start": start_time.isoformat(),
                    "end": end_time.isoformat()
                },
                "prediction_count": 0,
                "average_confidence": 0.0,
                "latency_p50": 0.0,
                "latency_p90": 0.0,
                "latency_p99": 0.0,
                "error_rate": 0.0
            }
            
            # Save aggregated metrics
            metrics_bucket = f"{self.bucket_prefix}-metrics"
            metrics_path = f"metrics/{model_id}/{end_time.strftime('%Y%m%d')}/daily_metrics.json"
            
            self.minio.put_object(
                metrics_bucket,
                metrics_path,
                json.dumps(aggregated_metrics).encode(),
                length=len(json.dumps(aggregated_metrics))
            )
            
            return aggregated_metrics
            
        except Exception as e:
            logger.error(f"Error aggregating model metrics: {e}")
            raise
            
    # Helper methods
    def _calculate_data_statistics(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Calculate comprehensive data statistics"""
        stats = {
            "row_count": len(data),
            "column_count": len(data.columns),
            "missing_values": data.isnull().sum().to_dict(),
            "dtypes": data.dtypes.astype(str).to_dict(),
            "numerical_stats": {},
            "categorical_stats": {}
        }
        
        # Numerical statistics
        numerical_cols = data.select_dtypes(include=[np.number]).columns
        for col in numerical_cols:
            stats["numerical_stats"][col] = {
                "mean": float(data[col].mean()),
                "std": float(data[col].std()),
                "min": float(data[col].min()),
                "max": float(data[col].max()),
                "q25": float(data[col].quantile(0.25)),
                "q50": float(data[col].quantile(0.50)),
                "q75": float(data[col].quantile(0.75))
            }
            
        # Categorical statistics
        categorical_cols = data.select_dtypes(include=['object']).columns
        for col in categorical_cols:
            stats["categorical_stats"][col] = {
                "unique_count": data[col].nunique(),
                "top_values": data[col].value_counts().head(10).to_dict()
            }
            
        return stats
        
    def _handle_missing_values(self, data: pd.DataFrame, strategy: str) -> pd.DataFrame:
        """Handle missing values based on strategy"""
        if strategy == "drop":
            return data.dropna()
        elif strategy == "mean":
            numerical_cols = data.select_dtypes(include=[np.number]).columns
            data[numerical_cols] = data[numerical_cols].fillna(data[numerical_cols].mean())
            return data
        elif strategy == "median":
            numerical_cols = data.select_dtypes(include=[np.number]).columns
            data[numerical_cols] = data[numerical_cols].fillna(data[numerical_cols].median())
            return data
        elif strategy == "forward_fill":
            return data.fillna(method='ffill')
        else:
            return data
            
    def _normalize_features(self, data: pd.DataFrame) -> pd.DataFrame:
        """Normalize numerical features"""
        return (data - data.mean()) / data.std()
        
    def _encode_categorical_features(self, data: pd.DataFrame, 
                                   categorical_cols: List[str]) -> pd.DataFrame:
        """Encode categorical features"""
        # Simple one-hot encoding (in production use more sophisticated methods)
        return pd.get_dummies(data, columns=categorical_cols, drop_first=True)
        
    def _remove_outliers(self, data: pd.DataFrame, threshold: float) -> pd.DataFrame:
        """Remove outliers using z-score method"""
        numerical_cols = data.select_dtypes(include=[np.number]).columns
        z_scores = np.abs((data[numerical_cols] - data[numerical_cols].mean()) / data[numerical_cols].std())
        return data[(z_scores < threshold).all(axis=1)]
        
    async def _save_dataset_metadata(self, dataset: TrainingDataset):
        """Save dataset metadata"""
        metadata_bucket = f"{self.bucket_prefix}-raw-data"
        metadata_path = f"metadata/datasets/{dataset.dataset_id}.json"
        
        self.minio.put_object(
            metadata_bucket,
            metadata_path,
            json.dumps(dataset.__dict__, default=str).encode(),
            length=len(json.dumps(dataset.__dict__, default=str))
        )
        
    async def _load_dataset_metadata(self, dataset_id: str) -> TrainingDataset:
        """Load dataset metadata"""
        metadata_bucket = f"{self.bucket_prefix}-raw-data"
        metadata_path = f"metadata/datasets/{dataset_id}.json"
        
        response = self.minio.get_object(metadata_bucket, metadata_path)
        metadata = json.loads(response.read())
        
        return TrainingDataset(
            dataset_id=metadata["dataset_id"],
            name=metadata["name"],
            version=metadata["version"],
            source_paths=metadata["source_paths"],
            split_ratios=metadata["split_ratios"],
            feature_columns=metadata["feature_columns"],
            target_column=metadata["target_column"],
            created_at=datetime.fromisoformat(metadata["created_at"]),
            metadata=metadata["metadata"],
            statistics=metadata["statistics"]
        )
        
    async def _save_feature_metadata(self, feature_set: FeatureSet):
        """Save feature set metadata"""
        metadata_bucket = f"{self.bucket_prefix}-features"
        metadata_path = f"metadata/feature_sets/{feature_set.feature_set_id}.json"
        
        self.minio.put_object(
            metadata_bucket,
            metadata_path,
            json.dumps(feature_set.__dict__, default=str).encode(),
            length=len(json.dumps(feature_set.__dict__, default=str))
        )
        
    async def _load_feature_metadata(self, feature_set_id: str) -> FeatureSet:
        """Load feature set metadata"""
        metadata_bucket = f"{self.bucket_prefix}-features"
        metadata_path = f"metadata/feature_sets/{feature_set_id}.json"
        
        response = self.minio.get_object(metadata_bucket, metadata_path)
        metadata = json.loads(response.read())
        
        return FeatureSet(
            feature_set_id=metadata["feature_set_id"],
            name=metadata["name"],
            version=metadata["version"],
            features=metadata["features"],
            entity_type=metadata["entity_type"],
            created_at=datetime.fromisoformat(metadata["created_at"]),
            update_frequency=metadata["update_frequency"],
            statistics=metadata["statistics"]
        )
        
    async def _load_parquet_data(self, s3_path: str) -> pd.DataFrame:
        """Load parquet data from S3 path"""
        # Parse S3 path
        parts = s3_path.replace("s3://", "").split("/", 1)
        bucket = parts[0]
        key = parts[1]
        
        # Load from MinIO
        response = self.minio.get_object(bucket, key)
        return pd.read_parquet(response)
        
    async def query_training_data(self, query: str) -> pd.DataFrame:
        """Query training data using DuckDB"""
        try:
            # Register MinIO data as DuckDB tables
            # (Simplified - in production, implement proper S3 integration)
            result = self.duckdb_conn.execute(query).fetchdf()
            return result
            
        except Exception as e:
            logger.error(f"Error querying training data: {e}")
            raise 