"""
Apache Ignite Manager for State Management Service

Handles all interactions with Apache Ignite for distributed state management.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import asyncio
import json

from pyignite import Client, GenericObjectMeta
from pyignite.datatypes import String, IntObject, BoolObject, MapObject, CollectionObject
from pyignite.exceptions import CacheError

logger = logging.getLogger(__name__)


class CacheConfig:
    """Configuration for a cache region"""
    
    def __init__(self, 
                 name: str,
                 cache_mode: str = "PARTITIONED",
                 backups: int = 1,
                 atomicity_mode: str = "TRANSACTIONAL",
                 eviction_policy: str = "LRU",
                 eviction_max_size: int = 1000000,
                 sql_schema: Optional[str] = None,
                 indexes: Optional[List[Tuple[str, str]]] = None):
        self.name = name
        self.cache_mode = cache_mode
        self.backups = backups
        self.atomicity_mode = atomicity_mode
        self.eviction_policy = eviction_policy
        self.eviction_max_size = eviction_max_size
        self.sql_schema = sql_schema
        self.indexes = indexes or []
    
    def to_ignite_config(self) -> Dict[str, Any]:
        """Convert to Ignite cache configuration"""
        config = {
            "name": self.name,
            "cacheMode": self.cache_mode,
            "backups": self.backups,
            "atomicityMode": self.atomicity_mode,
            "evictionPolicy": {
                "name": self.eviction_policy,
                "maxSize": self.eviction_max_size
            },
            "queryEntities": []
        }
        
        if self.sql_schema:
            # Add SQL query entity
            query_entity = {
                "keyType": "java.lang.String",
                "valueType": "java.lang.String",
                "tableName": self.name.upper(),
                "keyFieldName": "key",
                "valueFieldName": "value",
                "fields": [
                    {"name": "key", "type": "java.lang.String"},
                    {"name": "value", "type": "java.lang.String"}
                ]
            }
            
            # Add indexes
            if self.indexes:
                query_entity["indexes"] = [
                    {
                        "name": f"idx_{self.name}_{field}",
                        "indexType": idx_type,
                        "fields": {field: True}
                    }
                    for field, idx_type in self.indexes
                ]
            
            config["queryEntities"].append(query_entity)
        
        return config


class IgniteStateManager:
    """Manages state storage using Apache Ignite"""
    
    def __init__(self, nodes: List[Tuple[str, int]]):
        self.nodes = nodes
        self.client = None
        self.caches: Dict[str, Any] = {}
        self._connected = False
        self._reconnect_task = None
    
    async def connect(self):
        """Connect to Ignite cluster"""
        try:
            self.client = Client()
            self.client.connect(self.nodes)
            self._connected = True
            logger.info(f"Connected to Ignite cluster at {self.nodes}")
            
            # Start reconnection task
            self._reconnect_task = asyncio.create_task(self._reconnect_loop())
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
    
    async def disconnect(self):
        """Disconnect from Ignite cluster"""
        if self._reconnect_task:
            self._reconnect_task.cancel()
            try:
                await self._reconnect_task
            except asyncio.CancelledError:
                pass
        
        if self.client:
            self.client.close()
            self._connected = False
            logger.info("Disconnected from Ignite cluster")
    
    async def _reconnect_loop(self):
        """Background task to handle reconnection"""
        while True:
            try:
                await asyncio.sleep(5)  # Check every 5 seconds
                
                if not self._connected:
                    try:
                        self.client.connect(self.nodes)
                        self._connected = True
                        logger.info("Reconnected to Ignite cluster")
                    except Exception as e:
                        logger.warning(f"Reconnection attempt failed: {e}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in reconnect loop: {e}")
    
    def create_cache(self, config: CacheConfig) -> bool:
        """Create a new cache region"""
        try:
            cache_config = config.to_ignite_config()
            cache = self.client.create_cache(cache_config)
            self.caches[config.name] = cache
            logger.info(f"Created cache: {config.name}")
            return True
            
        except CacheError as e:
            if "already exists" in str(e):
                # Cache already exists, get reference
                cache = self.client.get_cache(config.name)
                self.caches[config.name] = cache
                logger.info(f"Cache already exists: {config.name}")
                return True
            else:
                logger.error(f"Failed to create cache {config.name}: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to create cache {config.name}: {e}")
            raise
    
    def get_cache(self, name: str):
        """Get cache by name"""
        if name not in self.caches:
            try:
                cache = self.client.get_cache(name)
                self.caches[name] = cache
            except Exception as e:
                logger.error(f"Failed to get cache {name}: {e}")
                raise
        
        return self.caches[name]
    
    async def get(self, cache_name: str, key: str) -> Optional[Any]:
        """Get value from cache"""
        try:
            cache = self.get_cache(cache_name)
            value = cache.get(key)
            
            # Deserialize if it's JSON
            if value and isinstance(value, str):
                try:
                    value = json.loads(value)
                except:
                    pass
            
            return value
            
        except Exception as e:
            logger.error(f"Failed to get key {key} from cache {cache_name}: {e}")
            return None
    
    async def put(self, cache_name: str, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Put value into cache"""
        try:
            cache = self.get_cache(cache_name)
            
            # Serialize complex objects to JSON
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            
            if ttl:
                # Ignite expects TTL in milliseconds
                cache.put(key, value, ttl_sec=ttl)
            else:
                cache.put(key, value)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to put key {key} into cache {cache_name}: {e}")
            return False
    
    async def put_all(self, cache_name: str, items: Dict[str, Any], ttl: Optional[int] = None) -> int:
        """Put multiple items into cache"""
        try:
            cache = self.get_cache(cache_name)
            
            # Serialize values
            serialized_items = {}
            for k, v in items.items():
                if isinstance(v, (dict, list)):
                    serialized_items[k] = json.dumps(v)
                else:
                    serialized_items[k] = v
            
            if ttl:
                # Put with TTL one by one (putAll doesn't support TTL)
                count = 0
                for k, v in serialized_items.items():
                    cache.put(k, v, ttl_sec=ttl)
                    count += 1
                return count
            else:
                cache.put_all(serialized_items)
                return len(serialized_items)
                
        except Exception as e:
            logger.error(f"Failed to put items into cache {cache_name}: {e}")
            return 0
    
    async def get_all(self, cache_name: str, keys: List[str]) -> Dict[str, Any]:
        """Get multiple items from cache"""
        try:
            cache = self.get_cache(cache_name)
            result = cache.get_all(keys)
            
            # Deserialize values
            deserialized = {}
            for k, v in result.items():
                if v and isinstance(v, str):
                    try:
                        deserialized[k] = json.loads(v)
                    except:
                        deserialized[k] = v
                else:
                    deserialized[k] = v
            
            return deserialized
            
        except Exception as e:
            logger.error(f"Failed to get items from cache {cache_name}: {e}")
            return {}
    
    async def delete(self, cache_name: str, key: str) -> bool:
        """Delete key from cache"""
        try:
            cache = self.get_cache(cache_name)
            cache.remove_key(key)
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete key {key} from cache {cache_name}: {e}")
            return False
    
    async def delete_all(self, cache_name: str, keys: List[str]) -> int:
        """Delete multiple keys from cache"""
        try:
            cache = self.get_cache(cache_name)
            cache.remove_keys(keys)
            return len(keys)
            
        except Exception as e:
            logger.error(f"Failed to delete keys from cache {cache_name}: {e}")
            return 0
    
    async def clear_cache(self, cache_name: str) -> bool:
        """Clear all entries from cache"""
        try:
            cache = self.get_cache(cache_name)
            cache.clear()
            return True
            
        except Exception as e:
            logger.error(f"Failed to clear cache {cache_name}: {e}")
            return False
    
    async def query(self, cache_name: str, sql: str, params: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """Execute SQL query on cache"""
        try:
            cache = self.get_cache(cache_name)
            
            # Execute query
            if params:
                result = cache.sql(sql, params)
            else:
                result = cache.sql(sql)
            
            # Convert to list of dicts
            rows = []
            with result:
                for row in result:
                    # Row is a list of values
                    rows.append(dict(zip(result.field_names, row)))
            
            return rows
            
        except Exception as e:
            logger.error(f"Failed to execute query on cache {cache_name}: {e}")
            return []
    
    async def get_cache_size(self, cache_name: str) -> int:
        """Get number of entries in cache"""
        try:
            cache = self.get_cache(cache_name)
            return cache.get_size()
            
        except Exception as e:
            logger.error(f"Failed to get size of cache {cache_name}: {e}")
            return 0
    
    async def get_cache_metrics(self, cache_name: str) -> Dict[str, Any]:
        """Get cache metrics"""
        try:
            cache = self.get_cache(cache_name)
            
            # Get cache metrics
            metrics = {
                "name": cache_name,
                "size": cache.get_size(),
                "is_empty": cache.is_empty()
            }
            
            # Try to get additional metrics via SQL
            try:
                result = self.client.sql(
                    f"SELECT COUNT(*) as count FROM \"{cache_name}\".CACHE"
                )
                with result:
                    for row in result:
                        metrics["sql_count"] = row[0]
            except:
                pass
            
            return metrics
            
        except Exception as e:
            logger.error(f"Failed to get metrics for cache {cache_name}: {e}")
            return {"name": cache_name, "error": str(e)}
    
    def list_caches(self) -> List[str]:
        """List all cache names"""
        try:
            return self.client.get_cache_names()
        except Exception as e:
            logger.error(f"Failed to list caches: {e}")
            return []
    
    # Transaction support
    
    def begin_transaction(self, 
                         concurrency: str = "PESSIMISTIC",
                         isolation: str = "REPEATABLE_READ",
                         timeout: int = 5000) -> Any:
        """Begin a new transaction"""
        try:
            from pyignite import TransactionConcurrency, TransactionIsolation
            
            # Map string values to enums
            concurrency_map = {
                "OPTIMISTIC": TransactionConcurrency.OPTIMISTIC,
                "PESSIMISTIC": TransactionConcurrency.PESSIMISTIC
            }
            
            isolation_map = {
                "READ_COMMITTED": TransactionIsolation.READ_COMMITTED,
                "REPEATABLE_READ": TransactionIsolation.REPEATABLE_READ,
                "SERIALIZABLE": TransactionIsolation.SERIALIZABLE
            }
            
            tx = self.client.tx_start(
                concurrency=concurrency_map.get(concurrency, TransactionConcurrency.PESSIMISTIC),
                isolation=isolation_map.get(isolation, TransactionIsolation.REPEATABLE_READ),
                timeout=timeout
            )
            
            return tx
            
        except Exception as e:
            logger.error(f"Failed to begin transaction: {e}")
            raise
    
    def commit_transaction(self, tx) -> bool:
        """Commit a transaction"""
        try:
            tx.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to commit transaction: {e}")
            return False
    
    def rollback_transaction(self, tx) -> bool:
        """Rollback a transaction"""
        try:
            tx.rollback()
            return True
        except Exception as e:
            logger.error(f"Failed to rollback transaction: {e}")
            return False
    
    # Continuous queries
    
    async def register_continuous_query(self, 
                                      cache_name: str,
                                      filter_sql: str,
                                      callback: callable) -> str:
        """Register a continuous query for real-time updates"""
        # Note: PyIgnite doesn't have direct support for continuous queries
        # This would need to be implemented using Ignite's binary protocol
        # or by polling with change detection
        
        logger.warning("Continuous queries not yet implemented in PyIgnite")
        return f"cq_{cache_name}_{id(callback)}"
    
    # Health checks
    
    async def health_check(self) -> Dict[str, Any]:
        """Check Ignite cluster health"""
        try:
            if not self._connected:
                return {
                    "status": "unhealthy",
                    "connected": False,
                    "error": "Not connected to cluster"
                }
            
            # Get cluster state
            cluster_state = self.client.is_binary_client_protocol_supported()
            
            # Get node info
            nodes = []
            for node in self.client.get_nodes():
                nodes.append({
                    "id": str(node.uuid),
                    "addresses": node.addresses,
                    "alive": node.alive
                })
            
            # Get cache info
            cache_names = self.list_caches()
            
            return {
                "status": "healthy",
                "connected": True,
                "binary_protocol": cluster_state,
                "nodes": nodes,
                "cache_count": len(cache_names),
                "caches": cache_names[:10]  # First 10 caches
            }
            
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "connected": self._connected,
                "error": str(e)
            } 