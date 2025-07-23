"""State Manager for Stream Processing Service

Manages distributed state using Apache Ignite for caching and state storage.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List, Set
from datetime import datetime, timedelta
import json
import pickle

from pyignite import Client
from pyignite.datatypes import String, LongObject, BinaryObject

from app.core.config import Settings


logger = logging.getLogger(__name__)


class StateType:
    """State type constants"""
    JOB_STATE = "job_state"
    CHECKPOINT = "checkpoint"
    PATTERN_STATE = "pattern_state"
    WINDOW_STATE = "window_state"
    KEY_VALUE = "key_value"
    COUNTER = "counter"
    LIST = "list"
    SET = "set"


class StateEntry:
    """Represents a state entry"""
    
    def __init__(self, key: str, value: Any, state_type: str, 
                 ttl: Optional[int] = None, metadata: Optional[Dict[str, Any]] = None):
        self.key = key
        self.value = value
        self.type = state_type
        self.ttl = ttl
        self.metadata = metadata or {}
        self.created_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()
        self.access_count = 0
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "key": self.key,
            "value": self.value,
            "type": self.type,
            "ttl": self.ttl,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "access_count": self.access_count
        }


class StateManager:
    """Manages distributed state using Apache Ignite"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client: Optional[Client] = None
        self.cache_name = settings.ignite_cache_name
        self.connected = False
        self._cleanup_task: Optional[asyncio.Task] = None
        self._metrics: Dict[str, int] = {
            "reads": 0,
            "writes": 0,
            "deletes": 0,
            "hits": 0,
            "misses": 0
        }
        
    async def initialize(self):
        """Initialize the state manager"""
        logger.info("Initializing StateManager")
        
        try:
            # Connect to Ignite
            self.client = Client()
            self.client.connect(self.settings.ignite_host, self.settings.ignite_port)
            self.connected = True
            
            # Create cache if it doesn't exist
            cache_config = {
                'name': self.cache_name,
                'backup_count': 1,
                'read_from_backup': True,
                'copy_on_read': True,
                'data_region': 'default',
                'query_entities': [
                    {
                        'table_name': 'STATE_ENTRIES',
                        'key_type': 'java.lang.String',
                        'value_type': 'org.apache.ignite.binary.BinaryObject',
                        'fields': [
                            {'name': 'key', 'type': 'java.lang.String'},
                            {'name': 'type', 'type': 'java.lang.String'},
                            {'name': 'created_at', 'type': 'java.lang.Long'},
                            {'name': 'ttl', 'type': 'java.lang.Integer'}
                        ]
                    }
                ]
            }
            
            cache = self.client.get_or_create_cache(cache_config)
            
            # Start cleanup task
            self._cleanup_task = asyncio.create_task(self._cleanup_expired_states())
            
            logger.info("StateManager initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize StateManager: {e}")
            self.connected = False
            raise
            
    async def cleanup(self):
        """Cleanup the state manager"""
        logger.info("Cleaning up StateManager")
        
        # Cancel cleanup task
        if self._cleanup_task:
            self._cleanup_task.cancel()
            
        # Disconnect from Ignite
        if self.client and self.connected:
            self.client.close()
            self.connected = False
            
        logger.info("StateManager cleaned up")
        
    async def get(self, key: str, default: Any = None) -> Any:
        """Get state value by key"""
        if not self.connected:
            logger.warning("StateManager not connected")
            return default
            
        try:
            cache = self.client.get_cache(self.cache_name)
            value = cache.get(key)
            
            self._metrics["reads"] += 1
            
            if value is None:
                self._metrics["misses"] += 1
                return default
            else:
                self._metrics["hits"] += 1
                
                # Deserialize if binary
                if isinstance(value, bytes):
                    value = pickle.loads(value)
                    
                return value
                
        except Exception as e:
            logger.error(f"Failed to get state for key {key}: {e}")
            return default
            
    async def put(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Put state value"""
        if not self.connected:
            logger.warning("StateManager not connected")
            return False
            
        try:
            cache = self.client.get_cache(self.cache_name)
            
            # Serialize complex objects
            if not isinstance(value, (str, int, float, bool)):
                value = pickle.dumps(value)
                
            # Put with TTL if specified
            if ttl:
                cache.put(key, value, ttl_seconds=ttl)
            else:
                cache.put(key, value)
                
            self._metrics["writes"] += 1
            return True
            
        except Exception as e:
            logger.error(f"Failed to put state for key {key}: {e}")
            return False
            
    async def delete(self, key: str) -> bool:
        """Delete state by key"""
        if not self.connected:
            logger.warning("StateManager not connected")
            return False
            
        try:
            cache = self.client.get_cache(self.cache_name)
            cache.remove(key)
            self._metrics["deletes"] += 1
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete state for key {key}: {e}")
            return False
            
    async def exists(self, key: str) -> bool:
        """Check if key exists"""
        if not self.connected:
            return False
            
        try:
            cache = self.client.get_cache(self.cache_name)
            return cache.contains_key(key)
            
        except Exception as e:
            logger.error(f"Failed to check existence for key {key}: {e}")
            return False
            
    async def get_all(self, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values by keys"""
        if not self.connected:
            return {}
            
        try:
            cache = self.client.get_cache(self.cache_name)
            results = cache.get_all(keys)
            
            # Deserialize values
            for key, value in results.items():
                if isinstance(value, bytes):
                    results[key] = pickle.loads(value)
                    
            self._metrics["reads"] += len(keys)
            self._metrics["hits"] += len(results)
            self._metrics["misses"] += len(keys) - len(results)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to get multiple states: {e}")
            return {}
            
    async def put_all(self, entries: Dict[str, Any]) -> bool:
        """Put multiple values"""
        if not self.connected:
            return False
            
        try:
            cache = self.client.get_cache(self.cache_name)
            
            # Serialize complex objects
            serialized = {}
            for key, value in entries.items():
                if not isinstance(value, (str, int, float, bool)):
                    serialized[key] = pickle.dumps(value)
                else:
                    serialized[key] = value
                    
            cache.put_all(serialized)
            self._metrics["writes"] += len(entries)
            return True
            
        except Exception as e:
            logger.error(f"Failed to put multiple states: {e}")
            return False
            
    # Atomic operations
    async def increment(self, key: str, delta: int = 1) -> int:
        """Atomically increment a counter"""
        if not self.connected:
            return 0
            
        try:
            cache = self.client.get_cache(self.cache_name)
            
            # Get current value
            current = cache.get(key) or 0
            new_value = current + delta
            
            # Put new value (should use compare-and-swap in production)
            cache.put(key, new_value)
            
            return new_value
            
        except Exception as e:
            logger.error(f"Failed to increment counter {key}: {e}")
            return 0
            
    async def add_to_set(self, key: str, value: Any) -> bool:
        """Add value to a set"""
        if not self.connected:
            return False
            
        try:
            cache = self.client.get_cache(self.cache_name)
            
            # Get current set
            current_set = cache.get(key)
            if current_set is None:
                current_set = set()
            elif isinstance(current_set, bytes):
                current_set = pickle.loads(current_set)
                
            # Add value
            current_set.add(value)
            
            # Put back
            cache.put(key, pickle.dumps(current_set))
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to add to set {key}: {e}")
            return False
            
    async def remove_from_set(self, key: str, value: Any) -> bool:
        """Remove value from a set"""
        if not self.connected:
            return False
            
        try:
            cache = self.client.get_cache(self.cache_name)
            
            # Get current set
            current_set = cache.get(key)
            if current_set is None:
                return True
            elif isinstance(current_set, bytes):
                current_set = pickle.loads(current_set)
                
            # Remove value
            current_set.discard(value)
            
            # Put back or delete if empty
            if current_set:
                cache.put(key, pickle.dumps(current_set))
            else:
                cache.remove(key)
                
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove from set {key}: {e}")
            return False
            
    async def get_set(self, key: str) -> Set[Any]:
        """Get a set"""
        if not self.connected:
            return set()
            
        try:
            cache = self.client.get_cache(self.cache_name)
            
            current_set = cache.get(key)
            if current_set is None:
                return set()
            elif isinstance(current_set, bytes):
                return pickle.loads(current_set)
            else:
                return current_set
                
        except Exception as e:
            logger.error(f"Failed to get set {key}: {e}")
            return set()
            
    # Checkpoint management
    async def save_checkpoint(self, job_id: str, checkpoint_data: Dict[str, Any]) -> bool:
        """Save job checkpoint"""
        checkpoint_key = f"checkpoint:{job_id}:{datetime.utcnow().timestamp()}"
        return await self.put(checkpoint_key, checkpoint_data, ttl=86400)  # 24 hour TTL
        
    async def get_latest_checkpoint(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get latest checkpoint for a job"""
        # In production, this would query by pattern
        # For now, returning None
        return None
        
    # Metrics
    def get_metrics(self) -> Dict[str, int]:
        """Get state manager metrics"""
        return self._metrics.copy()
        
    async def _cleanup_expired_states(self):
        """Periodically cleanup expired states"""
        while True:
            try:
                await asyncio.sleep(3600)  # Run every hour
                
                # In production, this would query and remove expired entries
                # based on TTL metadata
                
                logger.info("Cleaned up expired states")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error during state cleanup: {e}") 