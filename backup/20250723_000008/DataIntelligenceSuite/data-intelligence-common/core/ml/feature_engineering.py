"""
Feature engineering for ML.

Provides feature transformation, engineering, and feature store capabilities.
"""

import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass, field
import numpy as np
import pandas as pd
from collections import defaultdict
import hashlib

from ..caching import CacheManager
from ..events import EventBus, Event
from ...monitoring import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class FeatureType(str, Enum):
    """Feature data types"""
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    TEXT = "text"
    TIMESTAMP = "timestamp"
    BINARY = "binary"
    VECTOR = "vector"
    IMAGE = "image"
    EMBEDDING = "embedding"


class TransformationType(str, Enum):
    """Feature transformation types"""
    SCALING = "scaling"
    ENCODING = "encoding"
    BINNING = "binning"
    POLYNOMIAL = "polynomial"
    INTERACTION = "interaction"
    AGGREGATION = "aggregation"
    WINDOW = "window"
    EMBEDDING = "embedding"
    CUSTOM = "custom"


class AggregationType(str, Enum):
    """Aggregation types for features"""
    SUM = "sum"
    MEAN = "mean"
    MEDIAN = "median"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    STD = "std"
    VAR = "var"
    FIRST = "first"
    LAST = "last"


@dataclass
class Feature:
    """Feature definition"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: Optional[str] = None
    feature_type: FeatureType = FeatureType.NUMERIC
    
    # Source information
    source_table: Optional[str] = None
    source_column: Optional[str] = None
    source_query: Optional[str] = None
    
    # Transformation
    transformations: List[Dict[str, Any]] = field(default_factory=list)
    
    # Metadata
    tags: List[str] = field(default_factory=list)
    owner: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Statistics
    statistics: Dict[str, Any] = field(default_factory=dict)
    
    # Versioning
    version: str = "1.0"
    is_deprecated: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "feature_type": self.feature_type.value,
            "source_table": self.source_table,
            "source_column": self.source_column,
            "source_query": self.source_query,
            "transformations": self.transformations,
            "tags": self.tags,
            "owner": self.owner,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "statistics": self.statistics,
            "version": self.version,
            "is_deprecated": self.is_deprecated
        }


@dataclass
class FeatureGroup:
    """Group of related features"""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    description: Optional[str] = None
    
    # Features in group
    features: List[Feature] = field(default_factory=list)
    feature_ids: List[str] = field(default_factory=list)
    
    # Configuration
    entity_key: str = ""  # Primary key for joining
    timestamp_key: Optional[str] = None  # For time-based features
    
    # Storage
    storage_config: Dict[str, Any] = field(default_factory=dict)
    
    # Metadata
    tags: List[str] = field(default_factory=list)
    owner: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def add_feature(self, feature: Feature):
        """Add feature to group"""
        if feature.id not in self.feature_ids:
            self.features.append(feature)
            self.feature_ids.append(feature.id)
            
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "features": [f.to_dict() for f in self.features],
            "feature_ids": self.feature_ids,
            "entity_key": self.entity_key,
            "timestamp_key": self.timestamp_key,
            "storage_config": self.storage_config,
            "tags": self.tags,
            "owner": self.owner,
            "created_at": self.created_at.isoformat()
        }


@dataclass
class FeatureVector:
    """Computed feature vector"""
    entity_id: str
    features: Dict[str, Any]
    timestamp: datetime = field(default_factory=datetime.utcnow)
    feature_group_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_array(self, feature_names: List[str]) -> np.ndarray:
        """Convert to numpy array"""
        return np.array([self.features.get(name) for name in feature_names])
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "entity_id": self.entity_id,
            "features": self.features,
            "timestamp": self.timestamp.isoformat(),
            "feature_group_id": self.feature_group_id,
            "metadata": self.metadata
        }


class BaseTransformer(ABC):
    """Base feature transformer"""
    
    @abstractmethod
    def fit(self, data: pd.DataFrame, **kwargs):
        """Fit transformer on data"""
        pass
        
    @abstractmethod
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform data"""
        pass
        
    @abstractmethod
    def get_params(self) -> Dict[str, Any]:
        """Get transformer parameters"""
        pass


class StandardScaler(BaseTransformer):
    """Standard scaling transformer"""
    
    def __init__(self):
        self.mean_ = None
        self.std_ = None
        self.columns_ = None
        
    def fit(self, data: pd.DataFrame, columns: Optional[List[str]] = None):
        """Fit scaler on data"""
        self.columns_ = columns or data.select_dtypes(include=[np.number]).columns.tolist()
        self.mean_ = data[self.columns_].mean()
        self.std_ = data[self.columns_].std()
        
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform data"""
        if self.mean_ is None or self.std_ is None:
            raise ValueError("Scaler not fitted")
            
        result = data.copy()
        result[self.columns_] = (data[self.columns_] - self.mean_) / self.std_
        return result
        
    def get_params(self) -> Dict[str, Any]:
        """Get scaler parameters"""
        return {
            "mean": self.mean_.to_dict() if self.mean_ is not None else None,
            "std": self.std_.to_dict() if self.std_ is not None else None,
            "columns": self.columns_
        }


class OneHotEncoder(BaseTransformer):
    """One-hot encoding transformer"""
    
    def __init__(self):
        self.categories_ = None
        self.columns_ = None
        
    def fit(self, data: pd.DataFrame, columns: Optional[List[str]] = None):
        """Fit encoder on data"""
        self.columns_ = columns or data.select_dtypes(include=['object', 'category']).columns.tolist()
        self.categories_ = {}
        
        for col in self.columns_:
            self.categories_[col] = data[col].unique().tolist()
            
    def transform(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform data"""
        if self.categories_ is None:
            raise ValueError("Encoder not fitted")
            
        result = data.copy()
        
        for col in self.columns_:
            # Create dummy variables
            dummies = pd.get_dummies(data[col], prefix=col)
            
            # Ensure all categories are present
            for cat in self.categories_[col]:
                col_name = f"{col}_{cat}"
                if col_name not in dummies.columns:
                    dummies[col_name] = 0
                    
            # Drop original column and concat dummies
            result = result.drop(columns=[col])
            result = pd.concat([result, dummies], axis=1)
            
        return result
        
    def get_params(self) -> Dict[str, Any]:
        """Get encoder parameters"""
        return {
            "categories": self.categories_,
            "columns": self.columns_
        }


class FeatureEngineering:
    """
    Feature engineering and management system.
    
    Features:
    - Feature definition and versioning
    - Transformation pipelines
    - Feature computation
    - Feature store integration
    - Feature statistics
    """
    
    def __init__(
        self,
        cache_manager: Optional[CacheManager] = None,
        event_bus: Optional[EventBus] = None
    ):
        self.cache = cache_manager
        self.event_bus = event_bus
        
        # Storage
        self._features: Dict[str, Feature] = {}
        self._feature_groups: Dict[str, FeatureGroup] = {}
        self._transformers: Dict[str, BaseTransformer] = {}
        
        # Feature store (simple in-memory for now)
        self._feature_store: Dict[str, Dict[str, FeatureVector]] = defaultdict(dict)
        
        # Statistics tracking
        self._feature_stats: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
        # Register built-in transformers
        self._register_builtin_transformers()
        
    def _register_builtin_transformers(self):
        """Register built-in transformers"""
        self._transformers["standard_scaler"] = StandardScaler()
        self._transformers["onehot_encoder"] = OneHotEncoder()
        
    def create_feature(
        self,
        name: str,
        feature_type: FeatureType,
        source_table: Optional[str] = None,
        source_column: Optional[str] = None,
        source_query: Optional[str] = None,
        transformations: Optional[List[Dict[str, Any]]] = None,
        **kwargs
    ) -> Feature:
        """Create feature definition"""
        feature = Feature(
            name=name,
            feature_type=feature_type,
            source_table=source_table,
            source_column=source_column,
            source_query=source_query,
            transformations=transformations or [],
            **kwargs
        )
        
        self._features[feature.id] = feature
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="feature.created",
                source="feature_engineering",
                data={
                    "feature_id": feature.id,
                    "feature_name": name,
                    "feature_type": feature_type.value
                }
            ))
            
        logger.info(f"Created feature: {name}")
        return feature
        
    def create_feature_group(
        self,
        name: str,
        entity_key: str,
        features: Optional[List[Feature]] = None,
        **kwargs
    ) -> FeatureGroup:
        """Create feature group"""
        group = FeatureGroup(
            name=name,
            entity_key=entity_key,
            features=features or [],
            **kwargs
        )
        
        # Add feature IDs
        for feature in group.features:
            if feature.id not in group.feature_ids:
                group.feature_ids.append(feature.id)
                
        self._feature_groups[group.id] = group
        
        # Publish event
        if self.event_bus:
            self.event_bus.publish(Event(
                type="feature_group.created",
                source="feature_engineering",
                data={
                    "group_id": group.id,
                    "group_name": name,
                    "feature_count": len(group.features)
                }
            ))
            
        logger.info(f"Created feature group: {name}")
        return group
        
    def compute_features(
        self,
        data: pd.DataFrame,
        feature_group_id: str,
        timestamp: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Compute features for data"""
        group = self._feature_groups.get(feature_group_id)
        if not group:
            raise ValueError(f"Feature group not found: {feature_group_id}")
            
        timestamp = timestamp or datetime.utcnow()
        result = data.copy()
        
        # Compute each feature
        for feature in group.features:
            try:
                # Apply transformations
                feature_data = self._compute_single_feature(data, feature)
                result[feature.name] = feature_data
                
                # Update statistics
                self._update_feature_statistics(feature.id, feature_data)
                
            except Exception as e:
                logger.error(f"Failed to compute feature {feature.name}: {e}")
                
        # Store computed features
        if group.entity_key in data.columns:
            for _, row in result.iterrows():
                entity_id = str(row[group.entity_key])
                feature_values = {
                    col: row[col] for col in result.columns
                    if col != group.entity_key
                }
                
                vector = FeatureVector(
                    entity_id=entity_id,
                    features=feature_values,
                    timestamp=timestamp,
                    feature_group_id=feature_group_id
                )
                
                self._store_feature_vector(feature_group_id, entity_id, vector)
                
        return result
        
    def _compute_single_feature(
        self,
        data: pd.DataFrame,
        feature: Feature
    ) -> pd.Series:
        """Compute single feature"""
        # Get source data
        if feature.source_column and feature.source_column in data.columns:
            result = data[feature.source_column].copy()
        elif feature.source_query:
            # Execute query (would integrate with data platform)
            result = pd.Series([None] * len(data))
        else:
            result = pd.Series([None] * len(data))
            
        # Apply transformations
        for transform in feature.transformations:
            result = self._apply_transformation(result, transform)
            
        return result
        
    def _apply_transformation(
        self,
        data: pd.Series,
        transform_config: Dict[str, Any]
    ) -> pd.Series:
        """Apply transformation to data"""
        transform_type = transform_config.get("type")
        params = transform_config.get("params", {})
        
        if transform_type == TransformationType.SCALING.value:
            # Apply scaling
            if params.get("method") == "standard":
                return (data - data.mean()) / data.std()
            elif params.get("method") == "minmax":
                return (data - data.min()) / (data.max() - data.min())
                
        elif transform_type == TransformationType.BINNING.value:
            # Apply binning
            bins = params.get("bins", 10)
            return pd.cut(data, bins=bins, labels=False)
            
        elif transform_type == TransformationType.AGGREGATION.value:
            # Apply aggregation
            window = params.get("window", 1)
            agg_type = params.get("agg_type", "mean")
            
            if agg_type == "mean":
                return data.rolling(window).mean()
            elif agg_type == "sum":
                return data.rolling(window).sum()
            elif agg_type == "max":
                return data.rolling(window).max()
            elif agg_type == "min":
                return data.rolling(window).min()
                
        return data
        
    def _update_feature_statistics(
        self,
        feature_id: str,
        data: pd.Series
    ):
        """Update feature statistics"""
        stats = {
            "count": len(data),
            "null_count": data.isnull().sum(),
            "unique_count": data.nunique()
        }
        
        # Numeric statistics
        if pd.api.types.is_numeric_dtype(data):
            stats.update({
                "mean": float(data.mean()),
                "std": float(data.std()),
                "min": float(data.min()),
                "max": float(data.max()),
                "median": float(data.median()),
                "q25": float(data.quantile(0.25)),
                "q75": float(data.quantile(0.75))
            })
            
        # Categorical statistics
        elif pd.api.types.is_categorical_dtype(data) or pd.api.types.is_object_dtype(data):
            value_counts = data.value_counts()
            stats.update({
                "top_values": value_counts.head(10).to_dict(),
                "cardinality": len(value_counts)
            })
            
        self._feature_stats[feature_id] = stats
        
        # Update feature object
        if feature_id in self._features:
            self._features[feature_id].statistics = stats
            
    def _store_feature_vector(
        self,
        feature_group_id: str,
        entity_id: str,
        vector: FeatureVector
    ):
        """Store feature vector"""
        # Store in feature store
        self._feature_store[feature_group_id][entity_id] = vector
        
        # Cache if available
        if self.cache:
            cache_key = f"features:{feature_group_id}:{entity_id}"
            self.cache.set(cache_key, vector.to_dict(), ttl=3600)
            
    def get_features(
        self,
        entity_ids: List[str],
        feature_group_id: str,
        features: Optional[List[str]] = None,
        timestamp: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Get features from store"""
        group = self._feature_groups.get(feature_group_id)
        if not group:
            raise ValueError(f"Feature group not found: {feature_group_id}")
            
        # Get feature vectors
        vectors = []
        for entity_id in entity_ids:
            # Check cache first
            if self.cache:
                cache_key = f"features:{feature_group_id}:{entity_id}"
                cached = self.cache.get(cache_key)
                if cached:
                    vector = FeatureVector(**cached)
                    vectors.append(vector)
                    continue
                    
            # Get from store
            if entity_id in self._feature_store[feature_group_id]:
                vector = self._feature_store[feature_group_id][entity_id]
                
                # Filter by timestamp if provided
                if timestamp and vector.timestamp > timestamp:
                    continue
                    
                vectors.append(vector)
                
        # Convert to DataFrame
        if not vectors:
            return pd.DataFrame()
            
        data = []
        for vector in vectors:
            row = {"entity_id": vector.entity_id}
            
            # Filter features if specified
            if features:
                row.update({
                    k: v for k, v in vector.features.items()
                    if k in features
                })
            else:
                row.update(vector.features)
                
            data.append(row)
            
        return pd.DataFrame(data)
        
    def create_training_dataset(
        self,
        feature_group_ids: List[str],
        entity_ids: List[str],
        label_column: Optional[str] = None,
        timestamp: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Create training dataset from multiple feature groups"""
        datasets = []
        
        for group_id in feature_group_ids:
            df = self.get_features(
                entity_ids=entity_ids,
                feature_group_id=group_id,
                timestamp=timestamp
            )
            
            if not df.empty:
                # Set entity_id as index for joining
                df.set_index("entity_id", inplace=True)
                datasets.append(df)
                
        if not datasets:
            return pd.DataFrame()
            
        # Join all datasets
        result = datasets[0]
        for df in datasets[1:]:
            result = result.join(df, how="outer")
            
        # Reset index
        result.reset_index(inplace=True)
        
        # Move label column to end if specified
        if label_column and label_column in result.columns:
            cols = [col for col in result.columns if col != label_column]
            cols.append(label_column)
            result = result[cols]
            
        return result
        
    def get_feature_importance(
        self,
        feature_group_id: str,
        model: Optional[Any] = None
    ) -> Dict[str, float]:
        """Get feature importance scores"""
        group = self._feature_groups.get(feature_group_id)
        if not group:
            return {}
            
        importance = {}
        
        # If model provided, get importance from model
        if model and hasattr(model, "feature_importances_"):
            feature_names = [f.name for f in group.features]
            importances = model.feature_importances_
            
            for name, imp in zip(feature_names, importances):
                importance[name] = float(imp)
                
        else:
            # Use variance as proxy for importance
            for feature in group.features:
                stats = self._feature_stats.get(feature.id, {})
                if "std" in stats:
                    importance[feature.name] = stats["std"]
                else:
                    importance[feature.name] = 0.0
                    
        return importance
        
    def validate_features(
        self,
        data: pd.DataFrame,
        feature_group_id: str
    ) -> Dict[str, List[str]]:
        """Validate features in data"""
        group = self._feature_groups.get(feature_group_id)
        if not group:
            raise ValueError(f"Feature group not found: {feature_group_id}")
            
        issues = defaultdict(list)
        
        for feature in group.features:
            if feature.name not in data.columns:
                issues[feature.name].append("Missing feature")
                continue
                
            feature_data = data[feature.name]
            
            # Check data type
            if feature.feature_type == FeatureType.NUMERIC:
                if not pd.api.types.is_numeric_dtype(feature_data):
                    issues[feature.name].append("Expected numeric type")
                    
            elif feature.feature_type == FeatureType.CATEGORICAL:
                if pd.api.types.is_numeric_dtype(feature_data):
                    issues[feature.name].append("Expected categorical type")
                    
            # Check statistics
            stats = self._feature_stats.get(feature.id, {})
            if stats:
                # Check range for numeric features
                if "min" in stats and "max" in stats:
                    data_min = feature_data.min()
                    data_max = feature_data.max()
                    
                    if data_min < stats["min"] * 0.9:  # 10% tolerance
                        issues[feature.name].append(f"Values below expected minimum: {data_min} < {stats['min']}")
                    if data_max > stats["max"] * 1.1:  # 10% tolerance
                        issues[feature.name].append(f"Values above expected maximum: {data_max} > {stats['max']}")
                        
                # Check cardinality for categorical features
                if "cardinality" in stats:
                    data_cardinality = feature_data.nunique()
                    if data_cardinality > stats["cardinality"] * 1.5:  # 50% tolerance
                        issues[feature.name].append(f"Higher cardinality than expected: {data_cardinality} > {stats['cardinality']}")
                        
        return dict(issues)
        
    def export_feature_definitions(
        self,
        feature_group_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Export feature definitions"""
        if feature_group_id:
            group = self._feature_groups.get(feature_group_id)
            if not group:
                return {}
            return group.to_dict()
        else:
            # Export all
            return {
                "features": {
                    fid: f.to_dict() for fid, f in self._features.items()
                },
                "feature_groups": {
                    gid: g.to_dict() for gid, g in self._feature_groups.items()
                },
                "exported_at": datetime.utcnow().isoformat()
            } 