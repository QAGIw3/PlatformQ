"""
State Management for Stream Processing.
"""

import asyncio
from typing import Dict, Any, Optional, List, Set, Tuple, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import json
import pickle
from collections import defaultdict

from data_intelligence_common.core.caching import CacheManager
from data_intelligence_common.integrations import IgniteClient

from platformq_shared.logging_config import get_logger

logger = get_logger(__name__)


class StateBackend(str, Enum):
    """State backend types."""
    MEMORY = "memory"
    ROCKSDB = "rocksdb"
    IGNITE = "ignite"
    REDIS = "redis"


class StateType(str, Enum):
    """Types of state."""
    VALUE = "value"
    LIST = "list"
    MAP = "map"
    REDUCING = "reducing"
    AGGREGATING = "aggregating"


@dataclass
class CheckpointConfig:
    """Configuration for state checkpointing."""
    enabled: bool = True
    interval_ms: int = 60000  # 1 minute
    min_pause_between_checkpoints_ms: int = 5000
    timeout_ms: int = 600000  # 10 minutes
    max_concurrent_checkpoints: int = 1
    externalized_checkpoint_cleanup: bool = True
    state_backend: StateBackend = StateBackend.MEMORY
    state_ttl_ms: Optional[int] = None  # State TTL in milliseconds


@dataclass
class StateMetadata:
    """Metadata for state entries."""
    key: str
    state_type: StateType
    created_at: datetime
    updated_at: datetime
    access_count: int = 0
    size_bytes: int = 0
    ttl_ms: Optional[int] = None


class StateStore:
    """
    Abstract state store interface.
    """
    
    async def get(self, key: str) -> Optional[Any]:
        """Get state value."""
        raise NotImplementedError
        
    async def put(self, key: str, value: Any):
        """Put state value."""
        raise NotImplementedError
        
    async def delete(self, key: str):
        """Delete state value."""
        raise NotImplementedError
        
    async def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        """List all keys."""
        raise NotImplementedError
        
    async def clear(self):
        """Clear all state."""
        raise NotImplementedError
        
    async def checkpoint(self, checkpoint_id: str) -> Dict[str, Any]:
        """Create checkpoint."""
        raise NotImplementedError
        
    async def restore(self, checkpoint_id: str):
        """Restore from checkpoint."""
        raise NotImplementedError


class MemoryStateStore(StateStore):
    """
    In-memory state store implementation.
    """
    
    def __init__(self):
        self.state: Dict[str, Any] = {}
        self.metadata: Dict[str, StateMetadata] = {}
        self.checkpoints: Dict[str, Dict[str, Any]] = {}
        
    async def get(self, key: str) -> Optional[Any]:
        """Get state value."""
        if key in self.state:
            # Update metadata
            if key in self.metadata:
                self.metadata[key].access_count += 1
                self.metadata[key].updated_at = datetime.utcnow()
            
            return self.state[key]
        return None
        
    async def put(self, key: str, value: Any):
        """Put state value."""
        self.state[key] = value
        
        # Update metadata
        if key not in self.metadata:
            self.metadata[key] = StateMetadata(
                key=key,
                state_type=StateType.VALUE,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
        else:
            self.metadata[key].updated_at = datetime.utcnow()
            
        # Estimate size
        try:
            self.metadata[key].size_bytes = len(pickle.dumps(value))
        except:
            pass
            
    async def delete(self, key: str):
        """Delete state value."""
        if key in self.state:
            del self.state[key]
        if key in self.metadata:
            del self.metadata[key]
            
    async def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        """List all keys."""
        keys = list(self.state.keys())
        if prefix:
            keys = [k for k in keys if k.startswith(prefix)]
        return keys
        
    async def clear(self):
        """Clear all state."""
        self.state.clear()
        self.metadata.clear()
        
    async def checkpoint(self, checkpoint_id: str) -> Dict[str, Any]:
        """Create checkpoint."""
        checkpoint = {
            "state": self.state.copy(),
            "metadata": {k: v.__dict__ for k, v in self.metadata.items()},
            "timestamp": datetime.utcnow().isoformat()
        }
        self.checkpoints[checkpoint_id] = checkpoint
        return {"checkpoint_id": checkpoint_id, "size": len(self.state)}
        
    async def restore(self, checkpoint_id: str):
        """Restore from checkpoint."""
        if checkpoint_id in self.checkpoints:
            checkpoint = self.checkpoints[checkpoint_id]
            self.state = checkpoint["state"].copy()
            # Restore metadata
            self.metadata.clear()
            for k, v in checkpoint["metadata"].items():
                self.metadata[k] = StateMetadata(**v)


class IgniteStateStore(StateStore):
    """
    Apache Ignite-based state store implementation.
    """
    
    def __init__(self, ignite_client: IgniteClient, cache_name: str = "stream_state"):
        self.ignite_client = ignite_client
        self.cache_name = cache_name
        self.cache = None
        
    async def initialize(self):
        """Initialize Ignite cache."""
        self.cache = await self.ignite_client.get_or_create_cache(self.cache_name)
        
    async def get(self, key: str) -> Optional[Any]:
        """Get state value."""
        if self.cache:
            return await self.cache.get(key)
        return None
        
    async def put(self, key: str, value: Any):
        """Put state value."""
        if self.cache:
            await self.cache.put(key, value)
            
    async def delete(self, key: str):
        """Delete state value."""
        if self.cache:
            await self.cache.remove(key)
            
    async def list_keys(self, prefix: Optional[str] = None) -> List[str]:
        """List all keys."""
        # Ignite doesn't have native prefix scan, would need to implement
        return []
        
    async def clear(self):
        """Clear all state."""
        if self.cache:
            await self.cache.clear()
            
    async def checkpoint(self, checkpoint_id: str) -> Dict[str, Any]:
        """Create checkpoint."""
        # Would implement using Ignite snapshots
        return {"checkpoint_id": checkpoint_id}
        
    async def restore(self, checkpoint_id: str):
        """Restore from checkpoint."""
        # Would implement using Ignite snapshots
        pass


class StateManager:
    """
    Manages stateful stream processing with different backends.
    """
    
    def __init__(
        self,
        checkpoint_config: Optional[CheckpointConfig] = None,
        cache_manager: Optional[CacheManager] = None,
        ignite_client: Optional[IgniteClient] = None
    ):
        self.checkpoint_config = checkpoint_config or CheckpointConfig()
        self.cache_manager = cache_manager
        self.ignite_client = ignite_client
        
        # State stores by operator
        self.state_stores: Dict[str, StateStore] = {}
        
        # Checkpoint metadata
        self.checkpoints: List[Dict[str, Any]] = []
        self.last_checkpoint_time = datetime.utcnow()
        
        # Background tasks
        self._running = True
        self._checkpoint_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        
    async def initialize(self):
        """Initialize state manager."""
        # Start checkpoint task if enabled
        if self.checkpoint_config.enabled:
            self._checkpoint_task = asyncio.create_task(self._checkpoint_loop())
            
        # Start cleanup task for TTL
        if self.checkpoint_config.state_ttl_ms:
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            
        logger.info(f"State Manager initialized with backend: {self.checkpoint_config.state_backend}")
        
    def get_or_create_state_store(self, operator_id: str) -> StateStore:
        """Get or create state store for operator."""
        if operator_id not in self.state_stores:
            # Create state store based on backend
            if self.checkpoint_config.state_backend == StateBackend.MEMORY:
                store = MemoryStateStore()
            elif self.checkpoint_config.state_backend == StateBackend.IGNITE and self.ignite_client:
                store = IgniteStateStore(self.ignite_client, f"state_{operator_id}")
                asyncio.create_task(store.initialize())
            else:
                # Default to memory
                store = MemoryStateStore()
                
            self.state_stores[operator_id] = store
            
        return self.state_stores[operator_id]
        
    async def get_value_state(
        self,
        operator_id: str,
        key: str,
        default: Any = None
    ) -> Any:
        """Get value state."""
        store = self.get_or_create_state_store(operator_id)
        value = await store.get(key)
        return value if value is not None else default
        
    async def update_value_state(
        self,
        operator_id: str,
        key: str,
        value: Any
    ):
        """Update value state."""
        store = self.get_or_create_state_store(operator_id)
        await store.put(key, value)
        
    async def get_list_state(
        self,
        operator_id: str,
        key: str
    ) -> List[Any]:
        """Get list state."""
        store = self.get_or_create_state_store(operator_id)
        value = await store.get(key)
        return value if isinstance(value, list) else []
        
    async def append_to_list_state(
        self,
        operator_id: str,
        key: str,
        item: Any
    ):
        """Append to list state."""
        current = await self.get_list_state(operator_id, key)
        current.append(item)
        await self.update_value_state(operator_id, key, current)
        
    async def get_map_state(
        self,
        operator_id: str,
        key: str
    ) -> Dict[str, Any]:
        """Get map state."""
        store = self.get_or_create_state_store(operator_id)
        value = await store.get(key)
        return value if isinstance(value, dict) else {}
        
    async def put_in_map_state(
        self,
        operator_id: str,
        key: str,
        map_key: str,
        map_value: Any
    ):
        """Put value in map state."""
        current = await self.get_map_state(operator_id, key)
        current[map_key] = map_value
        await self.update_value_state(operator_id, key, current)
        
    async def get_reducing_state(
        self,
        operator_id: str,
        key: str,
        reduce_function: Callable[[Any, Any], Any],
        initial_value: Any = None
    ) -> Any:
        """Get reducing state."""
        store = self.get_or_create_state_store(operator_id)
        return await store.get(key) or initial_value
        
    async def add_to_reducing_state(
        self,
        operator_id: str,
        key: str,
        value: Any,
        reduce_function: Callable[[Any, Any], Any],
        initial_value: Any = None
    ):
        """Add to reducing state."""
        current = await self.get_reducing_state(operator_id, key, reduce_function, initial_value)
        if current is None:
            new_value = value
        else:
            new_value = reduce_function(current, value)
        await self.update_value_state(operator_id, key, new_value)
        
    async def clear_state(self, operator_id: str, key: Optional[str] = None):
        """Clear state for operator."""
        store = self.get_or_create_state_store(operator_id)
        if key:
            await store.delete(key)
        else:
            await store.clear()
            
    async def trigger_checkpoint(self) -> Dict[str, Any]:
        """Manually trigger checkpoint."""
        checkpoint_id = f"checkpoint_{datetime.utcnow().timestamp()}"
        checkpoint_metadata = {
            "checkpoint_id": checkpoint_id,
            "timestamp": datetime.utcnow().isoformat(),
            "operators": {}
        }
        
        # Checkpoint each operator's state
        for operator_id, store in self.state_stores.items():
            try:
                result = await store.checkpoint(checkpoint_id)
                checkpoint_metadata["operators"][operator_id] = result
            except Exception as e:
                logger.error(f"Error checkpointing operator {operator_id}: {e}")
                
        # Save checkpoint metadata
        self.checkpoints.append(checkpoint_metadata)
        
        # Cache checkpoint metadata
        if self.cache_manager:
            await self.cache_manager.set(
                f"stream:checkpoint:{checkpoint_id}",
                checkpoint_metadata,
                ttl=86400  # 24 hours
            )
            
        self.last_checkpoint_time = datetime.utcnow()
        
        logger.info(f"Checkpoint completed: {checkpoint_id}")
        return checkpoint_metadata
        
    async def restore_from_checkpoint(self, checkpoint_id: str):
        """Restore state from checkpoint."""
        # Get checkpoint metadata
        checkpoint_metadata = None
        
        if self.cache_manager:
            checkpoint_metadata = await self.cache_manager.get(
                f"stream:checkpoint:{checkpoint_id}"
            )
            
        if not checkpoint_metadata:
            # Try to find in local list
            for cp in self.checkpoints:
                if cp["checkpoint_id"] == checkpoint_id:
                    checkpoint_metadata = cp
                    break
                    
        if not checkpoint_metadata:
            raise ValueError(f"Checkpoint {checkpoint_id} not found")
            
        # Restore each operator's state
        for operator_id in checkpoint_metadata["operators"]:
            if operator_id in self.state_stores:
                try:
                    await self.state_stores[operator_id].restore(checkpoint_id)
                    logger.info(f"Restored state for operator {operator_id}")
                except Exception as e:
                    logger.error(f"Error restoring operator {operator_id}: {e}")
                    
        logger.info(f"State restored from checkpoint: {checkpoint_id}")
        
    async def _checkpoint_loop(self):
        """Background task for periodic checkpointing."""
        while self._running:
            try:
                await asyncio.sleep(self.checkpoint_config.interval_ms / 1000)
                
                # Check if enough time passed since last checkpoint
                time_since_last = (datetime.utcnow() - self.last_checkpoint_time).total_seconds() * 1000
                if time_since_last >= self.checkpoint_config.min_pause_between_checkpoints_ms:
                    await self.trigger_checkpoint()
                    
            except Exception as e:
                logger.error(f"Error in checkpoint loop: {e}")
                
    async def _cleanup_loop(self):
        """Background task for TTL cleanup."""
        while self._running:
            try:
                await asyncio.sleep(60)  # Check every minute
                
                # Clean up expired state
                for operator_id, store in self.state_stores.items():
                    if isinstance(store, MemoryStateStore):
                        # Check TTL for each key
                        expired_keys = []
                        
                        for key, metadata in store.metadata.items():
                            if metadata.ttl_ms:
                                age_ms = (datetime.utcnow() - metadata.updated_at).total_seconds() * 1000
                                if age_ms > metadata.ttl_ms:
                                    expired_keys.append(key)
                                    
                        # Delete expired keys
                        for key in expired_keys:
                            await store.delete(key)
                            
                        if expired_keys:
                            logger.debug(f"Cleaned up {len(expired_keys)} expired keys for operator {operator_id}")
                            
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                
    def get_state_metrics(self) -> Dict[str, Any]:
        """Get state management metrics."""
        total_keys = 0
        total_size = 0
        operator_stats = {}
        
        for operator_id, store in self.state_stores.items():
            if isinstance(store, MemoryStateStore):
                num_keys = len(store.state)
                size = sum(m.size_bytes for m in store.metadata.values())
                
                operator_stats[operator_id] = {
                    "num_keys": num_keys,
                    "size_bytes": size,
                    "num_checkpoints": len(store.checkpoints)
                }
                
                total_keys += num_keys
                total_size += size
                
        return {
            "total_operators": len(self.state_stores),
            "total_keys": total_keys,
            "total_size_bytes": total_size,
            "num_checkpoints": len(self.checkpoints),
            "last_checkpoint": self.last_checkpoint_time.isoformat(),
            "operators": operator_stats
        }
        
    async def close(self):
        """Clean up resources."""
        self._running = False
        
        # Cancel background tasks
        if self._checkpoint_task:
            self._checkpoint_task.cancel()
        if self._cleanup_task:
            self._cleanup_task.cancel()
            
        # Clear state stores
        for store in self.state_stores.values():
            await store.clear()
            
        logger.info("State manager closed") 