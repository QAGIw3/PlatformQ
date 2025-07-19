"""
Data Cache Manager using Apache Ignite
"""
import asyncio
import json
import hashlib
from typing import Any, Dict, Optional, List, Union
from datetime import datetime, timedelta
import logging
from contextlib import asynccontextmanager

from pyignite import AsyncClient
from pyignite.datatypes.cache_config import CacheMode
from pyignite.datatypes.prop_codes import PropCode
import orjson

from platformq_shared.utils.logger import get_logger

logger = get_logger(__name__)


class DataCacheManager:
    """
    Distributed cache manager using Apache Ignite.
    
    Features:
    - Query result caching
    - Metadata caching
    - Distributed cache invalidation
    - TTL-based expiration
    - LRU eviction policy
    - Cache statistics and monitoring
    """
    
    def __init__(self,
                 ignite_host: str = "ignite",
                 ignite_port: int = 10800,
                 cache_ttl: int = 3600,
                 max_cache_size: int = 10000):
        self.ignite_host = ignite_host
        self.ignite_port = ignite_port
        self.default_ttl = cache_ttl
        self.max_cache_size = max_cache_size
        self.client: Optional[AsyncClient] = None
        self.caches: Dict[str, Any] = {}
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "errors": 0
        }
        
    async def connect(self) -> None:
        """Connect to Ignite cluster"""
        try:
            self.client = AsyncClient()
            await self.client.connect(self.ignite_host, self.ignite_port)
            
            # Create caches with configurations
            await self._create_caches()
            
            logger.info(f"Connected to Ignite at {self.ignite_host}:{self.ignite_port}")
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
    
    async def _create_caches(self) -> None:
        """Create different cache configurations"""
        # Query result cache
        self.caches["query_results"] = await self.client.get_or_create_cache({
            PropCode.NAME: "platformq_query_results",
            PropCode.CACHE_MODE: CacheMode.PARTITIONED,
            PropCode.BACKUPS: 1,
            PropCode.CACHE_ATOMICITY_MODE: 0,  # ATOMIC
            PropCode.EXPIRY_POLICY: {
                "expiry_policy_type": "created",
                "duration": self.default_ttl * 1000  # milliseconds
            }
        })
        
        # Metadata cache
        self.caches["metadata"] = await self.client.get_or_create_cache({
            PropCode.NAME: "platformq_metadata",
            PropCode.CACHE_MODE: CacheMode.REPLICATED,
            PropCode.CACHE_ATOMICITY_MODE: 0,  # ATOMIC
            PropCode.EXPIRY_POLICY: {
                "expiry_policy_type": "created",
                "duration": 86400000  # 24 hours
            }
        })
        
        # Session cache
        self.caches["sessions"] = await self.client.get_or_create_cache({
            PropCode.NAME: "platformq_sessions",
            PropCode.CACHE_MODE: CacheMode.PARTITIONED,
            PropCode.BACKUPS: 1,
            PropCode.CACHE_ATOMICITY_MODE: 0,  # ATOMIC
            PropCode.EXPIRY_POLICY: {
                "expiry_policy_type": "created",
                "duration": 3600000  # 1 hour
            }
        })
        
        # Lineage cache
        self.caches["lineage"] = await self.client.get_or_create_cache({
            PropCode.NAME: "platformq_lineage",
            PropCode.CACHE_MODE: CacheMode.REPLICATED,
            PropCode.CACHE_ATOMICITY_MODE: 0  # ATOMIC
        })
    
    async def close(self) -> None:
        """Close Ignite connection"""
        if self.client:
            await self.client.close()
            logger.info("Disconnected from Ignite")
    
    def _generate_key(self, 
                     key_type: str,
                     key_data: Union[str, Dict[str, Any]]) -> str:
        """Generate a cache key"""
        if isinstance(key_data, dict):
            # Sort dict for consistent hashing
            key_str = json.dumps(key_data, sort_keys=True)
        else:
            key_str = str(key_data)
            
        # Create hash for long keys
        if len(key_str) > 250:
            key_hash = hashlib.sha256(key_str.encode()).hexdigest()
            return f"{key_type}:{key_hash}"
        
        return f"{key_type}:{key_str}"
    
    async def get_query_result(self, 
                              query: str,
                              params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Get cached query result"""
        try:
            key = self._generate_key("query", {
                "query": query,
                "params": params or {}
            })
            
            cache = self.caches.get("query_results")
            if not cache:
                return None
                
            result = await cache.get(key)
            
            if result:
                self._stats["hits"] += 1
                logger.debug(f"Cache hit for query: {query[:50]}...")
                
                # Deserialize if needed
                if isinstance(result, str):
                    result = orjson.loads(result)
                    
                return result
            else:
                self._stats["misses"] += 1
                return None
                
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Cache get error: {e}")
            return None
    
    async def set_query_result(self,
                              query: str,
                              result: Dict[str, Any],
                              params: Optional[Dict[str, Any]] = None,
                              ttl: Optional[int] = None) -> bool:
        """Cache query result"""
        try:
            key = self._generate_key("query", {
                "query": query,
                "params": params or {}
            })
            
            cache = self.caches.get("query_results")
            if not cache:
                return False
            
            # Serialize result
            serialized = orjson.dumps(result).decode()
            
            # Set with TTL if specified
            if ttl:
                await cache.put(key, serialized, ttl_sec=ttl)
            else:
                await cache.put(key, serialized)
                
            logger.debug(f"Cached query result: {query[:50]}...")
            return True
            
        except Exception as e:
            self._stats["errors"] += 1
            logger.error(f"Cache set error: {e}")
            return False
    
    async def get_metadata(self, 
                          metadata_type: str,
                          object_id: str) -> Optional[Dict[str, Any]]:
        """Get cached metadata"""
        try:
            key = self._generate_key(f"meta:{metadata_type}", object_id)
            cache = self.caches.get("metadata")
            
            if not cache:
                return None
                
            result = await cache.get(key)
            
            if result and isinstance(result, str):
                result = orjson.loads(result)
                
            return result
            
        except Exception as e:
            logger.error(f"Metadata cache get error: {e}")
            return None
    
    async def set_metadata(self,
                          metadata_type: str,
                          object_id: str,
                          metadata: Dict[str, Any]) -> bool:
        """Cache metadata"""
        try:
            key = self._generate_key(f"meta:{metadata_type}", object_id)
            cache = self.caches.get("metadata")
            
            if not cache:
                return False
                
            serialized = orjson.dumps(metadata).decode()
            await cache.put(key, serialized)
            
            return True
            
        except Exception as e:
            logger.error(f"Metadata cache set error: {e}")
            return False
    
    async def invalidate_query_cache(self, 
                                   pattern: Optional[str] = None) -> int:
        """Invalidate query cache entries"""
        try:
            cache = self.caches.get("query_results")
            if not cache:
                return 0
                
            if pattern:
                # Pattern-based invalidation
                count = 0
                async for key in cache.scan():
                    if pattern in str(key):
                        await cache.remove(key)
                        count += 1
                        
                logger.info(f"Invalidated {count} cache entries matching pattern: {pattern}")
                return count
            else:
                # Clear entire cache
                await cache.clear()
                logger.info("Cleared entire query cache")
                return -1
                
        except Exception as e:
            logger.error(f"Cache invalidation error: {e}")
            return 0
    
    async def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data from cache"""
        try:
            cache = self.caches.get("sessions")
            if not cache:
                return None
                
            result = await cache.get(session_id)
            
            if result and isinstance(result, str):
                result = orjson.loads(result)
                
            return result
            
        except Exception as e:
            logger.error(f"Session cache get error: {e}")
            return None
    
    async def set_session(self,
                         session_id: str,
                         session_data: Dict[str, Any],
                         ttl: int = 3600) -> bool:
        """Cache session data"""
        try:
            cache = self.caches.get("sessions")
            if not cache:
                return False
                
            serialized = orjson.dumps(session_data).decode()
            await cache.put(session_id, serialized, ttl_sec=ttl)
            
            return True
            
        except Exception as e:
            logger.error(f"Session cache set error: {e}")
            return False
    
    async def get_lineage(self, asset_id: str) -> Optional[Dict[str, Any]]:
        """Get cached lineage data"""
        try:
            cache = self.caches.get("lineage")
            if not cache:
                return None
                
            result = await cache.get(asset_id)
            
            if result and isinstance(result, str):
                result = orjson.loads(result)
                
            return result
            
        except Exception as e:
            logger.error(f"Lineage cache get error: {e}")
            return None
    
    async def set_lineage(self,
                         asset_id: str,
                         lineage_data: Dict[str, Any]) -> bool:
        """Cache lineage data"""
        try:
            cache = self.caches.get("lineage")
            if not cache:
                return False
                
            serialized = orjson.dumps(lineage_data).decode()
            await cache.put(asset_id, serialized)
            
            return True
            
        except Exception as e:
            logger.error(f"Lineage cache set error: {e}")
            return False
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_requests = self._stats["hits"] + self._stats["misses"]
        hit_rate = (self._stats["hits"] / total_requests * 100) if total_requests > 0 else 0
        
        stats = {
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "hit_rate": round(hit_rate, 2),
            "evictions": self._stats["evictions"],
            "errors": self._stats["errors"],
            "total_requests": total_requests
        }
        
        # Get cache sizes
        for cache_name, cache in self.caches.items():
            try:
                size = await cache.get_size()
                stats[f"{cache_name}_size"] = size
            except:
                stats[f"{cache_name}_size"] = -1
                
        return stats
    
    async def cleanup_expired_entries(self) -> None:
        """Background task to cleanup expired entries"""
        while True:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes
                
                # Ignite handles TTL automatically, but we can add custom cleanup logic
                logger.debug("Running cache cleanup task")
                
                # Update stats
                stats = await self.get_stats()
                logger.info(f"Cache stats: {stats}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Cleanup task error: {e}")
    
    async def health_check(self) -> bool:
        """Check cache health"""
        try:
            # Try to write and read a test value
            test_key = "_health_check"
            test_value = {"timestamp": datetime.utcnow().isoformat()}
            
            cache = self.caches.get("metadata")
            if cache:
                await cache.put(test_key, orjson.dumps(test_value).decode())
                result = await cache.get(test_key)
                await cache.remove(test_key)
                
                return result is not None
                
            return False
            
        except Exception as e:
            logger.error(f"Cache health check failed: {e}")
            return False
    
    @asynccontextmanager
    async def cache_transaction(self, cache_name: str = "query_results"):
        """Context manager for cache transactions"""
        cache = self.caches.get(cache_name)
        if not cache:
            yield None
            return
            
        try:
            # Start transaction if supported
            yield cache
        except Exception as e:
            logger.error(f"Cache transaction error: {e}")
            raise
        finally:
            # Cleanup if needed
            pass 