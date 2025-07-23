"""
Feature Store for ML pipelines.
"""

import asyncio
from typing import Dict, List, Optional, Any, Union, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import pandas as pd
import numpy as np
from collections import defaultdict

from pyignite import Client as IgniteClient
from pyignite.datatypes import String, DoubleArray, LongArray, BoolArray

from data_intelligence_common.core.events import EventBus
from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.integrations import IgniteClient as BaseIgniteClient

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class FeatureType(str, Enum):
    """Types of features."""
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    EMBEDDING = "embedding"
    BINARY = "binary"
    TEXT = "text"
    IMAGE = "image"
    TIME_SERIES = "time_series"
    COMPOSITE = "composite"


class FeatureStatus(str, Enum):
    """Feature lifecycle status."""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


@dataclass
class FeatureDefinition:
    """Definition of a feature."""
    name: str
    description: str
    feature_type: FeatureType
    data_type: str  # numpy/pandas dtype
    shape: Optional[Tuple[int, ...]] = None  # For embeddings/arrays
    default_value: Any = None
    tags: List[str] = field(default_factory=list)
    owner: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    version: int = 1
    status: FeatureStatus = FeatureStatus.ACTIVE
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Lineage
    source_features: List[str] = field(default_factory=list)  # Features this depends on
    transformation: Optional[str] = None  # Transformation applied
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "feature_type": self.feature_type.value,
            "data_type": self.data_type,
            "shape": self.shape,
            "default_value": self.default_value,
            "tags": self.tags,
            "owner": self.owner,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "version": self.version,
            "status": self.status.value,
            "metadata": self.metadata,
            "source_features": self.source_features,
            "transformation": self.transformation
        }


@dataclass
class FeatureValue:
    """Value of a feature for an entity."""
    entity_id: str
    feature_name: str
    value: Any
    timestamp: datetime = field(default_factory=datetime.utcnow)
    event_timestamp: Optional[datetime] = None  # When the feature was generated
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "entity_id": self.entity_id,
            "feature_name": self.feature_name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "event_timestamp": self.event_timestamp.isoformat() if self.event_timestamp else None,
            "metadata": self.metadata
        }


@dataclass
class FeatureSet:
    """A collection of related features."""
    name: str
    description: str
    features: List[str]  # Feature names
    entity_type: str  # Type of entity (user, item, etc.)
    tags: List[str] = field(default_factory=list)
    owner: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class FeatureView:
    """A view combining multiple feature sets."""
    name: str
    description: str
    feature_sets: List[str]  # Feature set names
    features: List[str]  # Specific features to include
    entity_types: List[str]
    join_keys: Dict[str, str] = field(default_factory=dict)  # Entity type -> join key
    filters: Optional[str] = None  # SQL-like filter expression
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


class FeatureStore:
    """
    Centralized feature store for ML pipelines.
    """
    
    def __init__(
        self,
        event_bus: EventBus,
        cache_manager: CacheManager,
        ignite_client: Optional[BaseIgniteClient] = None
    ):
        self.event_bus = event_bus
        self.cache_manager = cache_manager
        self.ignite_client = ignite_client
        
        # Feature definitions
        self.features: Dict[str, FeatureDefinition] = {}
        self.feature_sets: Dict[str, FeatureSet] = {}
        self.feature_views: Dict[str, FeatureView] = {}
        
        # Online store (Ignite caches)
        self.online_caches: Dict[str, Any] = {}
        
        # Feature statistics
        self.feature_stats: Dict[str, Dict[str, Any]] = defaultdict(dict)
        
        # Background tasks
        self._monitor_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        
        logger.info("Feature Store initialized")
        
    async def initialize(self):
        """Initialize feature store."""
        # Create default caches
        await self._create_default_caches()
        
        # Subscribe to events
        await self.event_bus.subscribe("feature.compute.complete", self._handle_feature_compute)
        await self.event_bus.subscribe("feature.request", self._handle_feature_request)
        
        # Start background tasks
        self._monitor_task = asyncio.create_task(self._monitor_features())
        self._cleanup_task = asyncio.create_task(self._cleanup_old_features())
        
        logger.info("Feature Store ready")
        
    async def cleanup(self):
        """Cleanup feature store resources."""
        # Cancel background tasks
        if self._monitor_task:
            self._monitor_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
        
        logger.info("Feature Store cleaned up")
        
    async def register_feature(self, feature_def: FeatureDefinition):
        """Register a new feature definition."""
        # Validate feature
        if feature_def.name in self.features:
            existing = self.features[feature_def.name]
            if existing.version >= feature_def.version:
                raise ValueError(f"Feature {feature_def.name} already exists with version {existing.version}")
        
        # Store feature definition
        self.features[feature_def.name] = feature_def
        
        # Create online cache if needed
        if feature_def.status == FeatureStatus.ACTIVE:
            await self._create_feature_cache(feature_def)
        
        # Cache feature definition
        await self.cache_manager.set(
            f"feature:definition:{feature_def.name}",
            feature_def.to_dict()
        )
        
        # Publish event
        await self.event_bus.publish("feature.registered", {
            "name": feature_def.name,
            "type": feature_def.feature_type.value,
            "version": feature_def.version
        })
        
        logger.info(f"Registered feature: {feature_def.name} v{feature_def.version}")
        
    async def create_feature_set(self, feature_set: FeatureSet):
        """Create a feature set."""
        # Validate features exist
        missing_features = [f for f in feature_set.features if f not in self.features]
        if missing_features:
            raise ValueError(f"Features not found: {missing_features}")
        
        # Store feature set
        self.feature_sets[feature_set.name] = feature_set
        
        # Cache feature set
        await self.cache_manager.set(
            f"feature:set:{feature_set.name}",
            {
                "name": feature_set.name,
                "description": feature_set.description,
                "features": feature_set.features,
                "entity_type": feature_set.entity_type,
                "tags": feature_set.tags,
                "owner": feature_set.owner,
                "created_at": feature_set.created_at.isoformat()
            }
        )
        
        # Publish event
        await self.event_bus.publish("feature.set.created", {
            "name": feature_set.name,
            "features": feature_set.features,
            "entity_type": feature_set.entity_type
        })
        
        logger.info(f"Created feature set: {feature_set.name}")
        
    async def create_feature_view(self, feature_view: FeatureView):
        """Create a feature view."""
        # Validate feature sets exist
        missing_sets = [fs for fs in feature_view.feature_sets if fs not in self.feature_sets]
        if missing_sets:
            raise ValueError(f"Feature sets not found: {missing_sets}")
        
        # Store feature view
        self.feature_views[feature_view.name] = feature_view
        
        # Publish event
        await self.event_bus.publish("feature.view.created", {
            "name": feature_view.name,
            "feature_sets": feature_view.feature_sets,
            "entity_types": feature_view.entity_types
        })
        
        logger.info(f"Created feature view: {feature_view.name}")
        
    async def write_feature_values(
        self,
        feature_values: List[FeatureValue],
        validate: bool = True
    ) -> int:
        """Write feature values to online store."""
        success_count = 0
        
        # Group by feature name for batch writing
        by_feature = defaultdict(list)
        for fv in feature_values:
            by_feature[fv.feature_name].append(fv)
        
        for feature_name, values in by_feature.items():
            # Validate feature exists
            if feature_name not in self.features:
                logger.warning(f"Feature {feature_name} not found")
                continue
            
            feature_def = self.features[feature_name]
            
            # Validate if requested
            if validate:
                values = await self._validate_feature_values(feature_def, values)
            
            # Get feature cache
            cache = self.online_caches.get(feature_name)
            if not cache:
                logger.warning(f"No online cache for feature {feature_name}")
                continue
            
            # Write values
            for value in values:
                key = f"{value.entity_id}:{value.feature_name}"
                cache_value = {
                    "value": value.value,
                    "timestamp": value.timestamp.isoformat(),
                    "event_timestamp": value.event_timestamp.isoformat() if value.event_timestamp else None,
                    "metadata": value.metadata
                }
                
                try:
                    cache.put(key, cache_value)
                    success_count += 1
                    
                    # Update statistics
                    self._update_feature_stats(feature_name, value)
                    
                except Exception as e:
                    logger.error(f"Error writing feature value: {e}")
        
        # Publish event
        await self.event_bus.publish("feature.values.written", {
            "count": success_count,
            "features": list(by_feature.keys())
        })
        
        logger.info(f"Wrote {success_count} feature values")
        return success_count
        
    async def get_online_features(
        self,
        entity_ids: List[str],
        feature_names: List[str],
        include_metadata: bool = False
    ) -> pd.DataFrame:
        """Get feature values from online store."""
        results = []
        
        for entity_id in entity_ids:
            row = {"entity_id": entity_id}
            
            for feature_name in feature_names:
                # Get feature definition
                feature_def = self.features.get(feature_name)
                if not feature_def:
                    row[feature_name] = None
                    continue
                
                # Get from cache
                cache = self.online_caches.get(feature_name)
                if not cache:
                    row[feature_name] = feature_def.default_value
                    continue
                
                key = f"{entity_id}:{feature_name}"
                try:
                    cache_value = cache.get(key)
                    if cache_value:
                        row[feature_name] = cache_value["value"]
                        
                        if include_metadata:
                            row[f"{feature_name}_timestamp"] = cache_value["timestamp"]
                            row[f"{feature_name}_metadata"] = cache_value.get("metadata", {})
                    else:
                        row[feature_name] = feature_def.default_value
                        
                except Exception as e:
                    logger.error(f"Error getting feature {feature_name} for entity {entity_id}: {e}")
                    row[feature_name] = feature_def.default_value
            
            results.append(row)
        
        # Convert to DataFrame
        df = pd.DataFrame(results)
        
        # Update access statistics
        for feature_name in feature_names:
            self.feature_stats[feature_name]["access_count"] = \
                self.feature_stats[feature_name].get("access_count", 0) + len(entity_ids)
        
        return df
        
    async def get_feature_vector(
        self,
        entity_id: str,
        feature_set_name: str
    ) -> Optional[np.ndarray]:
        """Get feature vector for an entity from a feature set."""
        feature_set = self.feature_sets.get(feature_set_name)
        if not feature_set:
            return None
        
        # Get features
        df = await self.get_online_features([entity_id], feature_set.features)
        
        if df.empty:
            return None
        
        # Convert to numpy array
        row = df.iloc[0]
        values = []
        
        for feature_name in feature_set.features:
            value = row.get(feature_name)
            if value is None:
                feature_def = self.features.get(feature_name)
                value = feature_def.default_value if feature_def else 0
            
            # Handle different types
            if isinstance(value, (list, np.ndarray)):
                values.extend(value)
            else:
                values.append(value)
        
        return np.array(values)
        
    async def get_batch_features(
        self,
        entity_ids: List[str],
        feature_view_name: str
    ) -> pd.DataFrame:
        """Get batch features using a feature view."""
        feature_view = self.feature_views.get(feature_view_name)
        if not feature_view:
            raise ValueError(f"Feature view {feature_view_name} not found")
        
        # Collect all features from feature sets
        all_features = []
        for fs_name in feature_view.feature_sets:
            fs = self.feature_sets.get(fs_name)
            if fs:
                all_features.extend(fs.features)
        
        # Filter to requested features
        if feature_view.features:
            features = [f for f in feature_view.features if f in all_features]
        else:
            features = all_features
        
        # Get features
        df = await self.get_online_features(entity_ids, features)
        
        # Apply filters if specified
        if feature_view.filters:
            df = df.query(feature_view.filters)
        
        return df
        
    async def _create_default_caches(self):
        """Create default feature caches."""
        # Feature metadata cache
        metadata_cache_config = {
            "name": "feature_metadata",
            "cache_mode": "REPLICATED",
            "atomicity_mode": "ATOMIC"
        }
        
        try:
            metadata_cache = self.ignite_client.create_cache(metadata_cache_config)
            self.online_caches["_metadata"] = metadata_cache
        except Exception as e:
            logger.warning(f"Could not create metadata cache: {e}")
            
    async def _create_feature_cache(self, feature_def: FeatureDefinition):
        """Create cache for a feature."""
        cache_name = f"feature_{feature_def.name}"
        
        # Determine cache configuration based on feature type
        if feature_def.feature_type == FeatureType.EMBEDDING:
            # Larger cache for embeddings
            cache_config = {
                "name": cache_name,
                "cache_mode": "PARTITIONED",
                "backups": 1,
                "eviction_policy": {
                    "policy": "LRU",
                    "max_size": 1000000  # 1M entries
                }
            }
        else:
            # Standard cache
            cache_config = {
                "name": cache_name,
                "cache_mode": "PARTITIONED",
                "backups": 1,
                "eviction_policy": {
                    "policy": "LRU",
                    "max_size": 10000000  # 10M entries
                }
            }
        
        try:
            cache = self.ignite_client.create_cache(cache_config)
            self.online_caches[feature_def.name] = cache
            logger.info(f"Created cache for feature {feature_def.name}")
        except Exception as e:
            logger.error(f"Error creating cache for feature {feature_def.name}: {e}")
            
    async def _validate_feature_values(
        self,
        feature_def: FeatureDefinition,
        values: List[FeatureValue]
    ) -> List[FeatureValue]:
        """Validate feature values against definition."""
        validated = []
        
        for value in values:
            try:
                # Type validation
                if feature_def.feature_type == FeatureType.NUMERIC:
                    if not isinstance(value.value, (int, float, np.number)):
                        logger.warning(f"Invalid numeric value for {feature_def.name}: {value.value}")
                        continue
                        
                elif feature_def.feature_type == FeatureType.CATEGORICAL:
                    if not isinstance(value.value, str):
                        value.value = str(value.value)
                        
                elif feature_def.feature_type == FeatureType.EMBEDDING:
                    if not isinstance(value.value, (list, np.ndarray)):
                        logger.warning(f"Invalid embedding value for {feature_def.name}")
                        continue
                    
                    # Check shape
                    if feature_def.shape:
                        value_shape = np.array(value.value).shape
                        if value_shape != feature_def.shape:
                            logger.warning(f"Shape mismatch for {feature_def.name}: expected {feature_def.shape}, got {value_shape}")
                            continue
                
                validated.append(value)
                
            except Exception as e:
                logger.error(f"Error validating feature value: {e}")
        
        return validated
        
    def _update_feature_stats(self, feature_name: str, value: FeatureValue):
        """Update feature statistics."""
        stats = self.feature_stats[feature_name]
        
        # Update counts
        stats["write_count"] = stats.get("write_count", 0) + 1
        stats["last_write"] = datetime.utcnow().isoformat()
        
        # Update value statistics for numeric features
        feature_def = self.features.get(feature_name)
        if feature_def and feature_def.feature_type == FeatureType.NUMERIC:
            if isinstance(value.value, (int, float)):
                # Running statistics
                if "min" not in stats:
                    stats["min"] = value.value
                    stats["max"] = value.value
                    stats["sum"] = value.value
                    stats["count"] = 1
                else:
                    stats["min"] = min(stats["min"], value.value)
                    stats["max"] = max(stats["max"], value.value)
                    stats["sum"] += value.value
                    stats["count"] += 1
                    stats["mean"] = stats["sum"] / stats["count"]
                    
    async def _monitor_features(self):
        """Monitor feature health and statistics."""
        while True:
            try:
                # Collect statistics
                for feature_name, stats in self.feature_stats.items():
                    # Report metrics
                    await self.event_bus.publish("feature.metrics", {
                        "feature": feature_name,
                        "stats": stats
                    })
                
                # Check feature health
                for feature_name, feature_def in self.features.items():
                    if feature_def.status == FeatureStatus.ACTIVE:
                        cache = self.online_caches.get(feature_name)
                        if cache:
                            try:
                                size = cache.get_size()
                                logger.debug(f"Feature {feature_name} cache size: {size}")
                            except Exception as e:
                                logger.error(f"Error checking cache for {feature_name}: {e}")
                
                # Sleep for 5 minutes
                await asyncio.sleep(300)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in feature monitoring: {e}")
                await asyncio.sleep(300)
                
    async def _cleanup_old_features(self):
        """Cleanup old feature values."""
        while True:
            try:
                # Clean up deprecated features
                for feature_name, feature_def in list(self.features.items()):
                    if feature_def.status == FeatureStatus.ARCHIVED:
                        # Remove cache
                        if feature_name in self.online_caches:
                            cache = self.online_caches[feature_name]
                            cache.destroy()
                            del self.online_caches[feature_name]
                        
                        # Remove definition
                        del self.features[feature_name]
                        
                        logger.info(f"Cleaned up archived feature: {feature_name}")
                
                # Sleep for 1 hour
                await asyncio.sleep(3600)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in feature cleanup: {e}")
                await asyncio.sleep(3600)
                
    async def _handle_feature_compute(self, event_data: Dict[str, Any]):
        """Handle computed feature values."""
        try:
            feature_values = []
            
            for item in event_data.get("values", []):
                fv = FeatureValue(
                    entity_id=item["entity_id"],
                    feature_name=item["feature_name"],
                    value=item["value"],
                    event_timestamp=datetime.fromisoformat(item.get("event_timestamp", datetime.utcnow().isoformat())),
                    metadata=item.get("metadata", {})
                )
                feature_values.append(fv)
            
            if feature_values:
                await self.write_feature_values(feature_values)
                
        except Exception as e:
            logger.error(f"Error handling feature compute: {e}")
            
    async def _handle_feature_request(self, event_data: Dict[str, Any]):
        """Handle feature request event."""
        try:
            entity_ids = event_data.get("entity_ids", [])
            features = event_data.get("features", [])
            
            if entity_ids and features:
                df = await self.get_online_features(entity_ids, features)
                
                # Publish response
                await self.event_bus.publish("feature.response", {
                    "request_id": event_data.get("request_id"),
                    "data": df.to_dict("records")
                })
                
        except Exception as e:
            logger.error(f"Error handling feature request: {e}")
            
    def get_feature_statistics(self) -> Dict[str, Any]:
        """Get feature store statistics."""
        return {
            "total_features": len(self.features),
            "active_features": len([f for f in self.features.values() if f.status == FeatureStatus.ACTIVE]),
            "feature_sets": len(self.feature_sets),
            "feature_views": len(self.feature_views),
            "feature_stats": dict(self.feature_stats)
        } 