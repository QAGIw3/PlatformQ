"""
Unified Feature Store for PlatformQ ML Platform

Provides centralized feature management with:
- Online serving via Apache Ignite (<5ms latency)
- Offline serving via Apache Iceberg
- Real-time updates via Apache Pulsar
- Feature versioning and lineage tracking
"""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import pandas as pd
import numpy as np
from enum import Enum

from pyignite import Client as IgniteClient
from pyignite.datatypes import String, DoubleArray, LongArray, BoolArray
from pulsar import Client as PulsarClient, Producer, Consumer
import pyarrow as pa
import pyarrow.parquet as pq

from platformq_shared.iceberg_client import IcebergClient
from platformq_shared.config import ConfigLoader

logger = logging.getLogger(__name__)


class FeatureType(str, Enum):
    """Types of features"""
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    EMBEDDING = "embedding"
    BINARY = "binary"
    TEXT = "text"
    IMAGE = "image"
    TIME_SERIES = "time_series"


class FeatureStatus(str, Enum):
    """Feature lifecycle status"""
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


@dataclass
class FeatureDefinition:
    """Definition of a feature"""
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


@dataclass
class FeatureValue:
    """Value of a feature for an entity"""
    entity_id: str
    feature_name: str
    value: Any
    timestamp: datetime = field(default_factory=datetime.utcnow)
    event_timestamp: Optional[datetime] = None  # When the feature was generated
    metadata: Dict[str, Any] = field(default_factory=dict)


class FeatureStore:
    """
    Unified Feature Store implementation
    """
    
    def __init__(self,
                 ignite_host: str = "localhost",
                 ignite_port: int = 10800,
                 pulsar_url: str = "pulsar://localhost:6650",
                 iceberg_catalog: str = "platformq",
                 config_loader: Optional[ConfigLoader] = None):
        """
        Initialize Feature Store
        
        Args:
            ignite_host: Apache Ignite host
            ignite_port: Apache Ignite port
            pulsar_url: Apache Pulsar broker URL
            iceberg_catalog: Iceberg catalog name
            config_loader: Configuration loader
        """
        self.config_loader = config_loader or ConfigLoader()
        
        # Initialize Ignite client for online serving
        self.ignite_client = IgniteClient()
        self.ignite_client.connect(ignite_host, ignite_port)
        
        # Initialize Pulsar client for real-time updates
        self.pulsar_client = PulsarClient(pulsar_url)
        self._init_pulsar_topics()
        
        # Initialize Iceberg client for offline serving
        self.iceberg_client = IcebergClient(catalog_name=iceberg_catalog)
        
        # Feature registry cache
        self._feature_registry: Dict[str, FeatureDefinition] = {}
        self._load_feature_registry()
        
        # Initialize caches
        self._init_ignite_caches()
        
        # Start background tasks
        self._running = True
        self._background_tasks = []
        self._start_background_tasks()
        
    def _init_ignite_caches(self):
        """Initialize Ignite caches for features"""
        # Feature values cache (online serving)
        self.feature_cache = self.ignite_client.get_or_create_cache(
            "feature_store_values"
        )
        
        # Feature metadata cache
        self.metadata_cache = self.ignite_client.get_or_create_cache(
            "feature_store_metadata"
        )
        
        # Feature statistics cache
        self.stats_cache = self.ignite_client.get_or_create_cache(
            "feature_store_stats"
        )
        
        logger.info("Initialized Ignite caches for feature store")
        
    def _init_pulsar_topics(self):
        """Initialize Pulsar topics for feature updates"""
        self.feature_update_topic = "persistent://platformq/features/updates"
        self.feature_request_topic = "persistent://platformq/features/requests"
        self.feature_log_topic = "persistent://platformq/features/logs"
        
        # Create producers
        self.update_producer = self.pulsar_client.create_producer(
            self.feature_update_topic,
            producer_name="feature-store-producer",
            batching_enabled=True,
            batching_max_publish_delay_ms=10
        )
        
        # Create consumers for background processing
        self.update_consumer = self.pulsar_client.subscribe(
            self.feature_update_topic,
            "feature-store-consumer",
            consumer_type=ConsumerType.Shared
        )
        
    def _load_feature_registry(self):
        """Load feature definitions from storage"""
        try:
            # Load from Iceberg table
            registry_df = self.iceberg_client.read_table(
                "feature_registry",
                namespace="ml"
            )
            
            for _, row in registry_df.iterrows():
                feature = FeatureDefinition(
                    name=row['name'],
                    description=row['description'],
                    feature_type=FeatureType(row['feature_type']),
                    data_type=row['data_type'],
                    shape=tuple(row['shape']) if row['shape'] else None,
                    default_value=row['default_value'],
                    tags=row['tags'].split(',') if row['tags'] else [],
                    owner=row['owner'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at'],
                    version=row['version'],
                    status=FeatureStatus(row['status']),
                    metadata=json.loads(row['metadata']) if row['metadata'] else {}
                )
                self._feature_registry[feature.name] = feature
                
            logger.info(f"Loaded {len(self._feature_registry)} features from registry")
            
        except Exception as e:
            logger.warning(f"Could not load feature registry: {e}")
            # Initialize empty registry
            self._create_feature_registry_table()
            
    def _create_feature_registry_table(self):
        """Create feature registry table in Iceberg"""
        schema = pa.schema([
            ('name', pa.string()),
            ('description', pa.string()),
            ('feature_type', pa.string()),
            ('data_type', pa.string()),
            ('shape', pa.list_(pa.int64())),
            ('default_value', pa.string()),
            ('tags', pa.string()),
            ('owner', pa.string()),
            ('created_at', pa.timestamp('us')),
            ('updated_at', pa.timestamp('us')),
            ('version', pa.int64()),
            ('status', pa.string()),
            ('metadata', pa.string())
        ])
        
        # Create empty table
        empty_df = pd.DataFrame(columns=schema.names)
        self.iceberg_client.write_table(
            empty_df,
            "feature_registry",
            namespace="ml",
            mode="create"
        )
        
    async def register_feature(self, feature: FeatureDefinition) -> bool:
        """
        Register a new feature or update existing
        
        Args:
            feature: Feature definition
            
        Returns:
            Success status
        """
        try:
            # Check if feature exists
            if feature.name in self._feature_registry:
                # Update version
                existing = self._feature_registry[feature.name]
                feature.version = existing.version + 1
                feature.created_at = existing.created_at
                
            feature.updated_at = datetime.utcnow()
            
            # Update registry
            self._feature_registry[feature.name] = feature
            
            # Persist to Iceberg
            feature_data = {
                'name': feature.name,
                'description': feature.description,
                'feature_type': feature.feature_type.value,
                'data_type': feature.data_type,
                'shape': list(feature.shape) if feature.shape else None,
                'default_value': str(feature.default_value),
                'tags': ','.join(feature.tags),
                'owner': feature.owner,
                'created_at': feature.created_at,
                'updated_at': feature.updated_at,
                'version': feature.version,
                'status': feature.status.value,
                'metadata': json.dumps(feature.metadata)
            }
            
            # Upsert to registry table
            filter_expr = self.iceberg_client.eq('name', feature.name)
            
            try:
                # Try to update existing
                self.iceberg_client.update_table(
                    'feature_registry',
                    namespace='ml',
                    updates=feature_data,
                    filter_expr=filter_expr
                )
            except:
                # Insert new
                df = pd.DataFrame([feature_data])
                self.iceberg_client.write_table(
                    df,
                    'feature_registry',
                    namespace='ml',
                    mode='append'
                )
                
            # Publish update event
            await self._publish_feature_event('feature_registered', feature)
            
            logger.info(f"Registered feature: {feature.name} (v{feature.version})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register feature: {e}")
            return False
            
    async def set_feature(self,
                         entity_id: str,
                         feature_name: str,
                         value: Any,
                         event_timestamp: Optional[datetime] = None) -> bool:
        """
        Set feature value for an entity (online)
        
        Args:
            entity_id: Entity identifier
            feature_name: Name of the feature
            value: Feature value
            event_timestamp: When the feature was generated
            
        Returns:
            Success status
        """
        try:
            # Validate feature exists
            if feature_name not in self._feature_registry:
                raise ValueError(f"Feature {feature_name} not registered")
                
            feature_def = self._feature_registry[feature_name]
            
            # Create feature value
            feature_value = FeatureValue(
                entity_id=entity_id,
                feature_name=feature_name,
                value=value,
                event_timestamp=event_timestamp or datetime.utcnow()
            )
            
            # Store in Ignite (online serving)
            cache_key = f"{entity_id}:{feature_name}"
            
            # Convert value based on type
            if feature_def.feature_type == FeatureType.EMBEDDING:
                # Store embeddings as double array
                cache_value = {
                    'value': value.tolist() if hasattr(value, 'tolist') else value,
                    'timestamp': feature_value.timestamp.isoformat(),
                    'event_timestamp': feature_value.event_timestamp.isoformat()
                }
            else:
                cache_value = {
                    'value': value,
                    'timestamp': feature_value.timestamp.isoformat(),
                    'event_timestamp': feature_value.event_timestamp.isoformat()
                }
                
            self.feature_cache.put(cache_key, cache_value)
            
            # Publish update event for downstream processing
            await self._publish_feature_update(feature_value)
            
            # Update statistics
            await self._update_feature_stats(feature_name, value)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to set feature: {e}")
            return False
            
    async def set_features(self,
                          entity_id: str,
                          features: Dict[str, Any],
                          event_timestamp: Optional[datetime] = None) -> bool:
        """
        Set multiple features for an entity
        
        Args:
            entity_id: Entity identifier
            features: Dictionary of feature_name -> value
            event_timestamp: When the features were generated
            
        Returns:
            Success status
        """
        results = []
        for feature_name, value in features.items():
            result = await self.set_feature(
                entity_id, 
                feature_name, 
                value, 
                event_timestamp
            )
            results.append(result)
            
        return all(results)
        
    def get_feature(self,
                    entity_id: str,
                    feature_name: str,
                    use_default: bool = True) -> Optional[Any]:
        """
        Get feature value for an entity (online)
        
        Args:
            entity_id: Entity identifier
            feature_name: Name of the feature
            use_default: Whether to use default value if not found
            
        Returns:
            Feature value or None
        """
        try:
            # Check cache
            cache_key = f"{entity_id}:{feature_name}"
            cached = self.feature_cache.get(cache_key)
            
            if cached:
                return cached['value']
                
            # Use default if enabled
            if use_default and feature_name in self._feature_registry:
                return self._feature_registry[feature_name].default_value
                
            return None
            
        except Exception as e:
            logger.error(f"Failed to get feature: {e}")
            return None
            
    def get_features(self,
                     entity_id: str,
                     feature_names: List[str],
                     use_default: bool = True) -> Dict[str, Any]:
        """
        Get multiple features for an entity (online)
        
        Args:
            entity_id: Entity identifier
            feature_names: List of feature names
            use_default: Whether to use default values
            
        Returns:
            Dictionary of feature_name -> value
        """
        features = {}
        for feature_name in feature_names:
            value = self.get_feature(entity_id, feature_name, use_default)
            if value is not None:
                features[feature_name] = value
                
        return features
        
    def get_feature_batch(self,
                          entity_ids: List[str],
                          feature_names: List[str],
                          use_default: bool = True) -> pd.DataFrame:
        """
        Get features for multiple entities (batch online serving)
        
        Args:
            entity_ids: List of entity identifiers
            feature_names: List of feature names
            use_default: Whether to use default values
            
        Returns:
            DataFrame with entities as rows and features as columns
        """
        data = []
        
        for entity_id in entity_ids:
            row = {'entity_id': entity_id}
            features = self.get_features(entity_id, feature_names, use_default)
            row.update(features)
            data.append(row)
            
        return pd.DataFrame(data)
        
    def get_historical_features(self,
                               entity_ids: List[str],
                               feature_names: List[str],
                               start_time: datetime,
                               end_time: Optional[datetime] = None) -> pd.DataFrame:
        """
        Get historical features (offline serving)
        
        Args:
            entity_ids: List of entity identifiers
            feature_names: List of feature names
            start_time: Start of time range
            end_time: End of time range (default: now)
            
        Returns:
            DataFrame with historical feature values
        """
        try:
            # Build filter expression
            filters = [
                self.iceberg_client.in_('entity_id', entity_ids),
                self.iceberg_client.in_('feature_name', feature_names),
                self.iceberg_client.gte('event_timestamp', start_time)
            ]
            
            if end_time:
                filters.append(self.iceberg_client.lte('event_timestamp', end_time))
                
            filter_expr = self.iceberg_client.and_(*filters)
            
            # Read from offline store
            df = self.iceberg_client.read_table(
                'feature_values',
                namespace='ml',
                filter_expr=filter_expr
            )
            
            # Pivot to wide format
            if not df.empty:
                df_pivot = df.pivot_table(
                    index=['entity_id', 'event_timestamp'],
                    columns='feature_name',
                    values='value',
                    aggfunc='last'
                ).reset_index()
                
                return df_pivot
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Failed to get historical features: {e}")
            return pd.DataFrame()
            
    def get_feature_statistics(self,
                              feature_name: str,
                              window: Optional[timedelta] = None) -> Dict[str, Any]:
        """
        Get feature statistics
        
        Args:
            feature_name: Name of the feature
            window: Time window for statistics (default: all time)
            
        Returns:
            Dictionary of statistics
        """
        try:
            stats_key = f"stats:{feature_name}"
            
            if window:
                # Calculate time-windowed stats
                stats_key = f"{stats_key}:{int(window.total_seconds())}"
                
            cached_stats = self.stats_cache.get(stats_key)
            
            if cached_stats:
                return cached_stats
            else:
                # Calculate from offline store
                return self._calculate_feature_statistics(feature_name, window)
                
        except Exception as e:
            logger.error(f"Failed to get feature statistics: {e}")
            return {}
            
    async def _publish_feature_update(self, feature_value: FeatureValue):
        """Publish feature update to Pulsar"""
        message = {
            'entity_id': feature_value.entity_id,
            'feature_name': feature_value.feature_name,
            'value': feature_value.value,
            'timestamp': feature_value.timestamp.isoformat(),
            'event_timestamp': feature_value.event_timestamp.isoformat(),
            'metadata': feature_value.metadata
        }
        
        self.update_producer.send_async(
            json.dumps(message).encode('utf-8'),
            callback=lambda res, msg: logger.debug(f"Feature update published: {res}")
        )
        
    async def _publish_feature_event(self, event_type: str, feature: FeatureDefinition):
        """Publish feature registry event"""
        message = {
            'event_type': event_type,
            'feature_name': feature.name,
            'version': feature.version,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        self.update_producer.send_async(
            json.dumps(message).encode('utf-8'),
            properties={'event_type': event_type}
        )
        
    async def _update_feature_stats(self, feature_name: str, value: Any):
        """Update feature statistics incrementally"""
        stats_key = f"stats:{feature_name}"
        
        # Get current stats
        current_stats = self.stats_cache.get(stats_key) or {
            'count': 0,
            'sum': 0,
            'sum_squares': 0,
            'min': float('inf'),
            'max': float('-inf'),
            'last_updated': datetime.utcnow().isoformat()
        }
        
        # Update stats (for numeric features)
        try:
            if isinstance(value, (int, float)):
                current_stats['count'] += 1
                current_stats['sum'] += value
                current_stats['sum_squares'] += value ** 2
                current_stats['min'] = min(current_stats['min'], value)
                current_stats['max'] = max(current_stats['max'], value)
                current_stats['mean'] = current_stats['sum'] / current_stats['count']
                current_stats['variance'] = (
                    current_stats['sum_squares'] / current_stats['count'] - 
                    current_stats['mean'] ** 2
                )
                current_stats['std'] = current_stats['variance'] ** 0.5
                
            current_stats['last_updated'] = datetime.utcnow().isoformat()
            
            # Update cache
            self.stats_cache.put(stats_key, current_stats)
            
        except Exception as e:
            logger.warning(f"Failed to update stats for {feature_name}: {e}")
            
    def _calculate_feature_statistics(self,
                                     feature_name: str,
                                     window: Optional[timedelta] = None) -> Dict[str, Any]:
        """Calculate feature statistics from offline store"""
        try:
            # Build filter
            filters = [self.iceberg_client.eq('feature_name', feature_name)]
            
            if window:
                start_time = datetime.utcnow() - window
                filters.append(self.iceberg_client.gte('event_timestamp', start_time))
                
            filter_expr = self.iceberg_client.and_(*filters)
            
            # Read data
            df = self.iceberg_client.read_table(
                'feature_values',
                namespace='ml',
                columns=['value'],
                filter_expr=filter_expr
            )
            
            if df.empty:
                return {}
                
            # Calculate statistics
            feature_def = self._feature_registry.get(feature_name)
            
            if feature_def and feature_def.feature_type == FeatureType.NUMERIC:
                stats = {
                    'count': len(df),
                    'mean': df['value'].mean(),
                    'std': df['value'].std(),
                    'min': df['value'].min(),
                    'max': df['value'].max(),
                    'q25': df['value'].quantile(0.25),
                    'q50': df['value'].quantile(0.50),
                    'q75': df['value'].quantile(0.75),
                    'unique': df['value'].nunique(),
                    'null_count': df['value'].isnull().sum()
                }
            else:
                # Categorical or other types
                stats = {
                    'count': len(df),
                    'unique': df['value'].nunique(),
                    'top_values': df['value'].value_counts().head(10).to_dict(),
                    'null_count': df['value'].isnull().sum()
                }
                
            # Cache stats
            stats_key = f"stats:{feature_name}"
            if window:
                stats_key = f"{stats_key}:{int(window.total_seconds())}"
                
            self.stats_cache.put(stats_key, stats, ttl=3600)  # 1 hour TTL
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to calculate statistics: {e}")
            return {}
            
    def _start_background_tasks(self):
        """Start background processing tasks"""
        # Feature update processor
        task1 = asyncio.create_task(self._process_feature_updates())
        self._background_tasks.append(task1)
        
        # Offline store sync
        task2 = asyncio.create_task(self._sync_to_offline_store())
        self._background_tasks.append(task2)
        
        # Stats refresh
        task3 = asyncio.create_task(self._refresh_statistics())
        self._background_tasks.append(task3)
        
        logger.info("Started feature store background tasks")
        
    async def _process_feature_updates(self):
        """Process feature updates from Pulsar"""
        while self._running:
            try:
                msg = self.update_consumer.receive(timeout_millis=1000)
                
                try:
                    data = json.loads(msg.data().decode('utf-8'))
                    
                    # Process based on message type
                    if msg.properties().get('event_type') == 'feature_registered':
                        # Reload registry
                        self._load_feature_registry()
                    else:
                        # Feature value update - already in cache
                        pass
                        
                    self.update_consumer.acknowledge(msg)
                    
                except Exception as e:
                    logger.error(f"Error processing update: {e}")
                    self.update_consumer.negative_acknowledge(msg)
                    
            except Exception:
                # Timeout - continue
                await asyncio.sleep(0.1)
                
    async def _sync_to_offline_store(self):
        """Periodically sync online features to offline store"""
        while self._running:
            try:
                await asyncio.sleep(60)  # Sync every minute
                
                # Get all cached features
                batch = []
                
                for key in self.feature_cache.get_all().keys():
                    entity_id, feature_name = key.split(':', 1)
                    value_data = self.feature_cache.get(key)
                    
                    if value_data:
                        batch.append({
                            'entity_id': entity_id,
                            'feature_name': feature_name,
                            'value': str(value_data['value']),  # Convert to string
                            'timestamp': datetime.fromisoformat(value_data['timestamp']),
                            'event_timestamp': datetime.fromisoformat(value_data['event_timestamp'])
                        })
                        
                    # Write batch
                    if len(batch) >= 1000:
                        df = pd.DataFrame(batch)
                        self.iceberg_client.write_table(
                            df,
                            'feature_values',
                            namespace='ml',
                            mode='append'
                        )
                        batch = []
                        
                # Write remaining
                if batch:
                    df = pd.DataFrame(batch)
                    self.iceberg_client.write_table(
                        df,
                        'feature_values',
                        namespace='ml',
                        mode='append'
                    )
                    
                logger.info(f"Synced {len(batch)} features to offline store")
                
            except Exception as e:
                logger.error(f"Error syncing to offline store: {e}")
                
    async def _refresh_statistics(self):
        """Periodically refresh feature statistics"""
        while self._running:
            try:
                await asyncio.sleep(300)  # Every 5 minutes
                
                # Refresh stats for active features
                for feature_name, feature_def in self._feature_registry.items():
                    if feature_def.status == FeatureStatus.ACTIVE:
                        # Refresh 1-hour window stats
                        self._calculate_feature_statistics(
                            feature_name,
                            window=timedelta(hours=1)
                        )
                        
            except Exception as e:
                logger.error(f"Error refreshing statistics: {e}")
                
    def close(self):
        """Clean up resources"""
        self._running = False
        
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
            
        # Close connections
        self.ignite_client.close()
        self.pulsar_client.close()
        
        logger.info("Feature store closed") 