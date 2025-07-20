"""
Apache Ignite Feature Cache

Provides high-performance, distributed caching for online feature serving.
Uses Apache Ignite's in-memory data grid for sub-millisecond latency.
"""

import logging
from typing import Dict, List, Optional, Any, Tuple, Set
from datetime import datetime, timedelta
import asyncio
import json
import pickle

from pyignite import Client, AsyncClient
from pyignite.datatypes import String, IntObject, FloatObject, TimestampObject
from pyignite.cache import Cache

from platformq_shared.utils.logger import get_logger
from ..core.config import settings

logger = get_logger(__name__)


class IgniteFeatureCache:
    """
    Apache Ignite-based feature cache for online serving.
    
    Features:
    - Distributed in-memory storage
    - Sub-millisecond latency
    - Automatic cache eviction
    - Batch operations
    - Binary protocol for efficiency
    - SQL queries for complex lookups
    """
    
    def __init__(self,
                 nodes: Optional[List[str]] = None,
                 cache_name: str = "feature_store",
                 ttl_seconds: int = 300,
                 enable_sql: bool = True,
                 enable_persistence: bool = False):
        """
        Initialize Ignite feature cache
        
        Args:
            nodes: List of Ignite nodes (host:port)
            cache_name: Name of the cache
            ttl_seconds: Default TTL for cached features
            enable_sql: Enable SQL queries
            enable_persistence: Enable persistence to disk
        """
        self.nodes = nodes or [f"{settings.ignite_host}:{settings.ignite_port}"]
        self.cache_name = cache_name
        self.ttl_seconds = ttl_seconds
        self.enable_sql = enable_sql
        self.enable_persistence = enable_persistence
        
        # Ignite clients
        self.sync_client: Optional[Client] = None
        self.async_client: Optional[AsyncClient] = None
        self.cache: Optional[Cache] = None
        
        # Cache statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "writes": 0,
            "errors": 0,
            "avg_latency_ms": 0
        }
        
    async def connect(self) -> None:
        """Connect to Ignite cluster"""
        try:
            # Create sync client for initialization
            self.sync_client = Client()
            self.sync_client.connect(self.nodes)
            
            # Create async client for operations
            self.async_client = AsyncClient()
            await self.async_client.connect(self.nodes)
            
            # Create or get cache
            cache_config = {
                'name': self.cache_name,
                'backup_count': 1,
                'atomicity_mode': 'TRANSACTIONAL',
                'cache_mode': 'PARTITIONED'
            }
            
            if self.enable_sql:
                cache_config['query_entities'] = [{
                    'table_name': 'features',
                    'key_type': 'java.lang.String',
                    'value_type': 'java.lang.String',
                    'key_fields': [
                        {'name': 'feature_key', 'type': 'java.lang.String'}
                    ],
                    'value_fields': [
                        {'name': 'feature_value', 'type': 'java.lang.String'},
                        {'name': 'feature_group', 'type': 'java.lang.String'},
                        {'name': 'entity_id', 'type': 'java.lang.String'},
                        {'name': 'timestamp', 'type': 'java.sql.Timestamp'},
                        {'name': 'version', 'type': 'java.lang.Integer'}
                    ],
                    'indexes': [
                        {
                            'name': 'idx_group_entity',
                            'type': 'SORTED',
                            'fields': ['feature_group', 'entity_id']
                        },
                        {
                            'name': 'idx_timestamp',
                            'type': 'SORTED',
                            'fields': ['timestamp']
                        }
                    ]
                }]
            
            if self.enable_persistence:
                cache_config['data_region_name'] = 'persistent_region'
            
            self.cache = await self.async_client.get_or_create_cache(cache_config)
            
            logger.info(f"Connected to Ignite cluster: {self.nodes}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
    
    async def get_feature(self,
                         feature_key: str,
                         entity_id: str,
                         version: Optional[int] = None) -> Optional[Any]:
        """
        Get a single feature value
        
        Args:
            feature_key: Feature identifier (group.name)
            entity_id: Entity identifier
            version: Optional version number
            
        Returns:
            Feature value or None if not found
        """
        start_time = datetime.utcnow()
        
        try:
            # Build cache key
            cache_key = self._build_cache_key(feature_key, entity_id, version)
            
            # Get from cache
            value = await self.cache.get(cache_key)
            
            if value is not None:
                self.stats["hits"] += 1
                # Deserialize if needed
                if isinstance(value, str) and value.startswith("pickle:"):
                    value = pickle.loads(value[7:].encode('latin-1'))
            else:
                self.stats["misses"] += 1
                
            # Update latency stats
            latency_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_latency_stats(latency_ms)
            
            return value
            
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to get feature: {e}")
            return None
    
    async def get_features_batch(self,
                               feature_keys: List[str],
                               entity_ids: List[str],
                               version: Optional[int] = None) -> Dict[Tuple[str, str], Any]:
        """
        Get multiple features in batch
        
        Args:
            feature_keys: List of feature identifiers
            entity_ids: List of entity identifiers
            version: Optional version number
            
        Returns:
            Dictionary mapping (feature_key, entity_id) to values
        """
        start_time = datetime.utcnow()
        results = {}
        
        try:
            # Build all cache keys
            cache_keys = []
            key_mapping = {}
            
            for feature_key in feature_keys:
                for entity_id in entity_ids:
                    cache_key = self._build_cache_key(feature_key, entity_id, version)
                    cache_keys.append(cache_key)
                    key_mapping[cache_key] = (feature_key, entity_id)
            
            # Batch get from cache
            if cache_keys:
                values = await self.cache.get_all(cache_keys)
                
                for cache_key, value in values.items():
                    if value is not None:
                        feature_key, entity_id = key_mapping[cache_key]
                        # Deserialize if needed
                        if isinstance(value, str) and value.startswith("pickle:"):
                            value = pickle.loads(value[7:].encode('latin-1'))
                        results[(feature_key, entity_id)] = value
                        self.stats["hits"] += 1
                    else:
                        self.stats["misses"] += 1
            
            # Update latency stats
            latency_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_latency_stats(latency_ms)
            
            return results
            
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to get features batch: {e}")
            return results
    
    async def put_feature(self,
                         feature_key: str,
                         entity_id: str,
                         value: Any,
                         version: Optional[int] = None,
                         ttl: Optional[int] = None) -> bool:
        """
        Store a single feature value
        
        Args:
            feature_key: Feature identifier (group.name)
            entity_id: Entity identifier
            value: Feature value
            version: Optional version number
            ttl: Optional TTL in seconds
            
        Returns:
            Success status
        """
        try:
            # Build cache key
            cache_key = self._build_cache_key(feature_key, entity_id, version)
            
            # Serialize complex objects
            if not isinstance(value, (str, int, float, bool)):
                value = "pickle:" + pickle.dumps(value).decode('latin-1')
            
            # Put to cache with TTL
            ttl = ttl or self.ttl_seconds
            await self.cache.put(cache_key, value, ttl_ms=ttl * 1000)
            
            # If SQL enabled, also insert into SQL table
            if self.enable_sql:
                await self._insert_sql_record(
                    feature_key=feature_key,
                    entity_id=entity_id,
                    value=value,
                    version=version
                )
            
            self.stats["writes"] += 1
            return True
            
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to put feature: {e}")
            return False
    
    async def put_features_batch(self,
                               features: Dict[Tuple[str, str], Any],
                               version: Optional[int] = None,
                               ttl: Optional[int] = None) -> int:
        """
        Store multiple features in batch
        
        Args:
            features: Dict mapping (feature_key, entity_id) to values
            version: Optional version number
            ttl: Optional TTL in seconds
            
        Returns:
            Number of features stored
        """
        try:
            # Build cache entries
            cache_entries = {}
            
            for (feature_key, entity_id), value in features.items():
                cache_key = self._build_cache_key(feature_key, entity_id, version)
                
                # Serialize complex objects
                if not isinstance(value, (str, int, float, bool)):
                    value = "pickle:" + pickle.dumps(value).decode('latin-1')
                    
                cache_entries[cache_key] = value
            
            # Batch put to cache
            ttl = ttl or self.ttl_seconds
            await self.cache.put_all(cache_entries, ttl_ms=ttl * 1000)
            
            self.stats["writes"] += len(cache_entries)
            return len(cache_entries)
            
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Failed to put features batch: {e}")
            return 0
    
    async def query_features_sql(self, sql: str) -> List[Dict[str, Any]]:
        """
        Query features using SQL
        
        Args:
            sql: SQL query string
            
        Returns:
            List of result rows
        """
        if not self.enable_sql:
            raise ValueError("SQL queries not enabled")
            
        try:
            query = self.cache.query(sql)
            results = []
            
            async for row in query:
                results.append(dict(row))
                
            return results
            
        except Exception as e:
            logger.error(f"SQL query failed: {e}")
            raise
    
    async def clear_entity_features(self, entity_id: str) -> int:
        """Clear all features for an entity"""
        try:
            if self.enable_sql:
                sql = f"DELETE FROM features WHERE entity_id = '{entity_id}'"
                await self.cache.query(sql)
                return 1
            else:
                # Without SQL, would need to scan all keys
                logger.warning("Clear entity features requires SQL to be enabled")
                return 0
                
        except Exception as e:
            logger.error(f"Failed to clear entity features: {e}")
            return 0
    
    def _build_cache_key(self,
                        feature_key: str,
                        entity_id: str,
                        version: Optional[int] = None) -> str:
        """Build cache key from components"""
        if version is not None:
            return f"{feature_key}:{entity_id}:v{version}"
        return f"{feature_key}:{entity_id}"
    
    async def _insert_sql_record(self,
                               feature_key: str,
                               entity_id: str,
                               value: Any,
                               version: Optional[int] = None) -> None:
        """Insert record into SQL table for querying"""
        try:
            group, name = feature_key.split(".", 1)
            sql = """
            INSERT INTO features (
                feature_key, feature_value, feature_group, 
                entity_id, timestamp, version
            ) VALUES (?, ?, ?, ?, ?, ?)
            """
            
            await self.cache.query(
                sql,
                feature_key,
                str(value),
                group,
                entity_id,
                datetime.utcnow(),
                version or 0
            )
            
        except Exception as e:
            logger.warning(f"Failed to insert SQL record: {e}")
    
    def _update_latency_stats(self, latency_ms: float) -> None:
        """Update average latency statistics"""
        current_avg = self.stats["avg_latency_ms"]
        total_ops = self.stats["hits"] + self.stats["misses"]
        
        if total_ops > 0:
            self.stats["avg_latency_ms"] = (
                (current_avg * (total_ops - 1) + latency_ms) / total_ops
            )
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get cache statistics"""
        cache_hit_rate = 0
        if self.stats["hits"] + self.stats["misses"] > 0:
            cache_hit_rate = self.stats["hits"] / (self.stats["hits"] + self.stats["misses"])
            
        return {
            **self.stats,
            "cache_hit_rate": cache_hit_rate,
            "nodes": self.nodes,
            "cache_name": self.cache_name
        }
    
    async def close(self) -> None:
        """Close Ignite connections"""
        try:
            if self.async_client:
                await self.async_client.close()
            if self.sync_client:
                self.sync_client.close()
                
            logger.info("Closed Ignite connections")
            
        except Exception as e:
            logger.error(f"Error closing Ignite connections: {e}") 