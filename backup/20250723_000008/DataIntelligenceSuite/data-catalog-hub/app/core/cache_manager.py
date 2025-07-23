"""
Cache Manager using Apache Ignite
"""

import json
import pickle
from typing import Any, Optional, Dict, List
from datetime import datetime, timedelta

from pyignite import AsyncClient
from pyignite.datatypes import String

from platformq_shared.logging import get_logger
from ..core.config import Settings

logger = get_logger(__name__)


class CacheManager:
    """Manages distributed caching using Apache Ignite"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client: Optional[AsyncClient] = None
        self.caches: Dict[str, Any] = {}
        
    async def connect(self):
        """Connect to Apache Ignite"""
        logger.info("Connecting to Apache Ignite")
        
        self.client = AsyncClient()
        await self.client.connect(self.settings.ignite_host, self.settings.ignite_port)
        
        # Create default caches
        await self._create_default_caches()
        
        logger.info("Connected to Apache Ignite")
        
    async def disconnect(self):
        """Disconnect from Apache Ignite"""
        if self.client:
            await self.client.close()
            
    async def _create_default_caches(self):
        """Create default cache configurations"""
        cache_configs = {
            "catalog_entities": {
                "cache_mode": "PARTITIONED",
                "backups": 1,
                "atomicity_mode": "ATOMIC"
            },
            "catalog_schemas": {
                "cache_mode": "REPLICATED",
                "atomicity_mode": "ATOMIC"
            },
            "catalog_search": {
                "cache_mode": "PARTITIONED",
                "backups": 1,
                "atomicity_mode": "ATOMIC",
                "expiry_policy": {
                    "create": self.settings.cache_ttl * 1000,
                    "update": self.settings.cache_ttl * 1000,
                    "access": self.settings.cache_ttl * 1000
                }
            },
            "catalog_lineage": {
                "cache_mode": "PARTITIONED",
                "backups": 1,
                "atomicity_mode": "ATOMIC"
            },
            "catalog_glossary": {
                "cache_mode": "REPLICATED",
                "atomicity_mode": "ATOMIC"
            }
        }
        
        for cache_name, config in cache_configs.items():
            try:
                cache = await self.client.get_or_create_cache({
                    "name": cache_name,
                    **config
                })
                self.caches[cache_name] = cache
            except Exception as e:
                logger.error(f"Failed to create cache {cache_name}: {e}")
                
    async def get(self, key: str, cache_name: str = "catalog_entities") -> Optional[Any]:
        """Get value from cache"""
        try:
            cache = self.caches.get(cache_name)
            if not cache:
                cache = await self.client.get_cache(cache_name)
                self.caches[cache_name] = cache
                
            value = await cache.get(key)
            
            if value:
                # Check if it's a serialized object
                if isinstance(value, bytes):
                    try:
                        return pickle.loads(value)
                    except:
                        return value
                elif isinstance(value, str) and value.startswith('{'):
                    try:
                        return json.loads(value)
                    except:
                        return value
                        
            return value
            
        except Exception as e:
            logger.error(f"Cache get error: {e}")
            return None
            
    async def set(self, 
                  key: str, 
                  value: Any, 
                  cache_name: str = "catalog_entities",
                  ttl: Optional[int] = None) -> bool:
        """Set value in cache"""
        try:
            cache = self.caches.get(cache_name)
            if not cache:
                cache = await self.client.get_cache(cache_name)
                self.caches[cache_name] = cache
                
            # Serialize complex objects
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            elif not isinstance(value, (str, int, float, bool)):
                value = pickle.dumps(value)
                
            # Set with TTL if specified
            if ttl:
                # Ignite expects TTL in milliseconds
                ttl_ms = ttl * 1000
                await cache.put(key, value, ttl=ttl_ms)
            else:
                await cache.put(key, value)
                
            return True
            
        except Exception as e:
            logger.error(f"Cache set error: {e}")
            return False
            
    async def delete(self, key: str, cache_name: str = "catalog_entities") -> bool:
        """Delete value from cache"""
        try:
            cache = self.caches.get(cache_name)
            if not cache:
                cache = await self.client.get_cache(cache_name)
                self.caches[cache_name] = cache
                
            await cache.remove(key)
            return True
            
        except Exception as e:
            logger.error(f"Cache delete error: {e}")
            return False
            
    async def delete_pattern(self, pattern: str, cache_name: str = "catalog_entities"):
        """Delete all keys matching pattern"""
        try:
            cache = self.caches.get(cache_name)
            if not cache:
                cache = await self.client.get_cache(cache_name)
                self.caches[cache_name] = cache
                
            # Scan cache for matching keys
            # Note: This is inefficient for large caches
            # In production, consider using Ignite SQL queries
            async with cache.scan() as cursor:
                async for key, _ in cursor:
                    if isinstance(key, str) and pattern.replace('*', '') in key:
                        await cache.remove(key)
                        
        except Exception as e:
            logger.error(f"Cache delete pattern error: {e}")
            
    async def clear(self, cache_name: str = "catalog_entities"):
        """Clear entire cache"""
        try:
            cache = self.caches.get(cache_name)
            if not cache:
                cache = await self.client.get_cache(cache_name)
                self.caches[cache_name] = cache
                
            await cache.clear()
            logger.info(f"Cleared cache: {cache_name}")
            
        except Exception as e:
            logger.error(f"Cache clear error: {e}")
            
    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        stats = {}
        
        for cache_name, cache in self.caches.items():
            try:
                size = await cache.get_size()
                stats[cache_name] = {
                    "size": size,
                    "name": cache_name
                }
            except Exception as e:
                logger.error(f"Failed to get stats for {cache_name}: {e}")
                
        return stats
        
    async def exists(self, key: str, cache_name: str = "catalog_entities") -> bool:
        """Check if key exists in cache"""
        try:
            cache = self.caches.get(cache_name)
            if not cache:
                cache = await self.client.get_cache(cache_name)
                self.caches[cache_name] = cache
                
            return await cache.contains_key(key)
            
        except Exception as e:
            logger.error(f"Cache exists error: {e}")
            return False
            
    async def get_many(self, 
                      keys: List[str], 
                      cache_name: str = "catalog_entities") -> Dict[str, Any]:
        """Get multiple values from cache"""
        try:
            cache = self.caches.get(cache_name)
            if not cache:
                cache = await self.client.get_cache(cache_name)
                self.caches[cache_name] = cache
                
            # Ignite batch get
            values = await cache.get_all(keys)
            
            # Deserialize values
            result = {}
            for key, value in values.items():
                if value:
                    if isinstance(value, bytes):
                        try:
                            result[key] = pickle.loads(value)
                        except:
                            result[key] = value
                    elif isinstance(value, str) and value.startswith('{'):
                        try:
                            result[key] = json.loads(value)
                        except:
                            result[key] = value
                    else:
                        result[key] = value
                        
            return result
            
        except Exception as e:
            logger.error(f"Cache get_many error: {e}")
            return {}
            
    async def set_many(self, 
                      items: Dict[str, Any], 
                      cache_name: str = "catalog_entities",
                      ttl: Optional[int] = None) -> bool:
        """Set multiple values in cache"""
        try:
            cache = self.caches.get(cache_name)
            if not cache:
                cache = await self.client.get_cache(cache_name)
                self.caches[cache_name] = cache
                
            # Serialize values
            serialized = {}
            for key, value in items.items():
                if isinstance(value, (dict, list)):
                    serialized[key] = json.dumps(value)
                elif not isinstance(value, (str, int, float, bool)):
                    serialized[key] = pickle.dumps(value)
                else:
                    serialized[key] = value
                    
            # Batch put
            await cache.put_all(serialized)
            
            return True
            
        except Exception as e:
            logger.error(f"Cache set_many error: {e}")
            return False 