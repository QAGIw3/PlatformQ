"""Cache manager for unified graph service using Apache Ignite"""

import logging
from typing import Dict, Any, Optional, List
import json
import asyncio
from datetime import datetime, timedelta

import pyignite
from pyignite import Client
from pyignite.datatypes import String, IntObject

from app.core.config import Settings


logger = logging.getLogger(__name__)


class CacheManager:
    """Manages distributed caching using Apache Ignite"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client: Optional[Client] = None
        self.cache: Optional[Any] = None
        self.connected = False
        
    async def connect(self):
        """Connect to Ignite cluster"""
        logger.info(f"Connecting to Ignite at {self.settings.ignite_host}:{self.settings.ignite_port}")
        
        try:
            self.client = Client()
            self.client.connect(self.settings.ignite_host, self.settings.ignite_port)
            
            # Get or create cache
            self.cache = self.client.get_or_create_cache(self.settings.ignite_cache_name)
            
            self.connected = True
            logger.info("Connected to Ignite cache")
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
            
    async def disconnect(self):
        """Disconnect from Ignite"""
        if self.client:
            self.client.close()
            self.connected = False
            logger.info("Disconnected from Ignite cache")
            
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache"""
        if not self.connected:
            return None
            
        try:
            value = self.cache.get(key)
            if value:
                # Check if expired
                data = json.loads(value)
                if 'expiry' in data:
                    expiry = datetime.fromisoformat(data['expiry'])
                    if expiry < datetime.utcnow():
                        # Expired, remove it
                        await self.delete(key)
                        return None
                        
                return data.get('value')
            return None
            
        except Exception as e:
            logger.error(f"Cache get error for key {key}: {e}")
            return None
            
    async def set(self, key: str, value: Any, ttl: Optional[int] = None):
        """Set value in cache with optional TTL"""
        if not self.connected:
            return
            
        try:
            ttl_seconds = ttl or self.settings.cache_ttl
            
            # Prepare cache entry
            cache_data = {
                'value': value,
                'created_at': datetime.utcnow().isoformat()
            }
            
            if ttl_seconds > 0:
                cache_data['expiry'] = (datetime.utcnow() + timedelta(seconds=ttl_seconds)).isoformat()
                
            # Store in cache
            self.cache.put(key, json.dumps(cache_data))
            
        except Exception as e:
            logger.error(f"Cache set error for key {key}: {e}")
            
    async def delete(self, key: str):
        """Delete value from cache"""
        if not self.connected:
            return
            
        try:
            self.cache.remove(key)
        except Exception as e:
            logger.error(f"Cache delete error for key {key}: {e}")
            
    async def clear_pattern(self, pattern: str):
        """Clear all keys matching pattern"""
        if not self.connected:
            return
            
        try:
            # Get all keys and filter by pattern
            keys_to_delete = []
            
            # Note: In production, use Ignite SQL queries for better performance
            scan_query = self.cache.scan()
            
            for key, _ in scan_query:
                if pattern in str(key):
                    keys_to_delete.append(key)
                    
            # Delete matching keys
            for key in keys_to_delete:
                await self.delete(key)
                
            logger.info(f"Cleared {len(keys_to_delete)} cache entries matching pattern: {pattern}")
            
        except Exception as e:
            logger.error(f"Cache clear pattern error: {e}")
            
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        if not self.connected:
            return {}
            
        try:
            metrics = self.cache.get_metrics()
            
            return {
                'size': self.cache.get_size(),
                'hits': metrics.get_cache_hits(),
                'misses': metrics.get_cache_misses(),
                'hit_rate': metrics.get_cache_hit_percentage(),
                'average_get_time': metrics.get_average_get_time(),
                'average_put_time': metrics.get_average_put_time()
            }
            
        except Exception as e:
            logger.error(f"Failed to get cache stats: {e}")
            return {}
            
    # Graph-specific cache methods
    
    async def cache_graph_query(self, query: str, params: Dict[str, Any], 
                               result: Any, ttl: Optional[int] = None):
        """Cache graph query result"""
        # Create cache key from query and params
        key = self._create_query_key(query, params)
        await self.set(key, result, ttl)
        
    async def get_cached_query(self, query: str, params: Dict[str, Any]) -> Optional[Any]:
        """Get cached graph query result"""
        key = self._create_query_key(query, params)
        return await self.get(key)
        
    async def cache_node(self, node_id: str, node_data: Dict[str, Any],
                        ttl: Optional[int] = None):
        """Cache node data"""
        key = f"node:{node_id}"
        await self.set(key, node_data, ttl)
        
    async def get_cached_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get cached node data"""
        key = f"node:{node_id}"
        return await self.get(key)
        
    async def cache_edge(self, edge_id: str, edge_data: Dict[str, Any],
                        ttl: Optional[int] = None):
        """Cache edge data"""
        key = f"edge:{edge_id}"
        await self.set(key, edge_data, ttl)
        
    async def get_cached_edge(self, edge_id: str) -> Optional[Dict[str, Any]]:
        """Get cached edge data"""
        key = f"edge:{edge_id}"
        return await self.get(key)
        
    async def cache_analytics_result(self, job_id: str, result: Dict[str, Any],
                                   ttl: Optional[int] = None):
        """Cache analytics job result"""
        key = f"analytics:{job_id}"
        # Use longer TTL for analytics results
        ttl = ttl or 86400  # 24 hours default
        await self.set(key, result, ttl)
        
    async def get_cached_analytics(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get cached analytics result"""
        key = f"analytics:{job_id}"
        return await self.get(key)
        
    async def cache_trust_score(self, entity_id: str, context: str,
                               score: Dict[str, Any], ttl: Optional[int] = None):
        """Cache trust score"""
        key = f"trust:{entity_id}:{context}"
        ttl = ttl or 300  # 5 minutes default for trust scores
        await self.set(key, score, ttl)
        
    async def get_cached_trust_score(self, entity_id: str, 
                                   context: str) -> Optional[Dict[str, Any]]:
        """Get cached trust score"""
        key = f"trust:{entity_id}:{context}"
        return await self.get(key)
        
    async def cache_lineage(self, entity_id: str, direction: str,
                           lineage: Dict[str, Any], ttl: Optional[int] = None):
        """Cache lineage data"""
        key = f"lineage:{entity_id}:{direction}"
        ttl = ttl or 600  # 10 minutes default
        await self.set(key, lineage, ttl)
        
    async def get_cached_lineage(self, entity_id: str,
                                direction: str) -> Optional[Dict[str, Any]]:
        """Get cached lineage data"""
        key = f"lineage:{entity_id}:{direction}"
        return await self.get(key)
        
    async def invalidate_node_cache(self, node_id: str):
        """Invalidate all cache entries related to a node"""
        await self.delete(f"node:{node_id}")
        await self.clear_pattern(f"trust:{node_id}")
        await self.clear_pattern(f"lineage:{node_id}")
        # Also clear query cache that might include this node
        await self.clear_pattern(node_id)
        
    async def warm_cache(self, popular_nodes: List[str]):
        """Warm cache with popular nodes"""
        # This would be called periodically to pre-load frequently accessed data
        logger.info(f"Warming cache with {len(popular_nodes)} popular nodes")
        
        # Implementation would fetch and cache node data
        # This is a placeholder for the concept
        
    def _create_query_key(self, query: str, params: Dict[str, Any]) -> str:
        """Create cache key from query and parameters"""
        # Sort params for consistent key generation
        sorted_params = json.dumps(params, sort_keys=True)
        # Use first 100 chars of query to keep key manageable
        query_prefix = query[:100].replace(' ', '_')
        # Create hash of full query + params
        import hashlib
        query_hash = hashlib.md5(f"{query}{sorted_params}".encode()).hexdigest()
        
        return f"query:{query_prefix}:{query_hash}" 