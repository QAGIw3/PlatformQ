"""
Base classes for Vault and Consul integrations.

Provides common functionality and patterns for secure service integration.
"""

from typing import Dict, Any, Optional, List, Set
from abc import ABC
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BaseIntegration(ABC):
    """Base class for service integrations."""
    
    def __init__(self, service_name: str, config: Optional[Dict[str, Any]] = None):
        self.service_name = service_name
        self.config = config or {}
        self._initialized = False
        self._active_tasks: Set[asyncio.Task] = set()
        
    async def initialize(self):
        """Initialize the integration."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement initialize method"
        )
        
    async def shutdown(self):
        """Shutdown the integration."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement shutdown method"
        )
        
    async def _cancel_tasks(self):
        """Cancel all active tasks."""
        for task in self._active_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._active_tasks.clear()
        
    def _create_task(self, coro) -> asyncio.Task:
        """Create and track an async task."""
        task = asyncio.create_task(coro)
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
        return task
        
    @property
    def is_initialized(self) -> bool:
        """Check if integration is initialized."""
        return self._initialized


class CacheableMixin:
    """
    Mixin for adding caching capabilities.
    
    DEPRECATED: Use the @cached decorator from core.caching instead.
    This is maintained for backward compatibility only.
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
        
    def _get_from_cache(self, key: str, ttl_seconds: int = 300) -> Optional[Any]:
        """Get value from cache if not expired."""
        import warnings
        warnings.warn(
            "CacheableMixin is deprecated. Use @cached decorator from core.caching instead.",
            DeprecationWarning,
            stacklevel=2
        )
        if key in self._cache:
            timestamp = self._cache_timestamps.get(key)
            if timestamp and (datetime.utcnow() - timestamp).seconds < ttl_seconds:
                return self._cache[key]
        return None
        
    def _set_cache(self, key: str, value: Any):
        """Set value in cache."""
        self._cache[key] = value
        self._cache_timestamps[key] = datetime.utcnow()
        
    def _clear_cache(self, key: Optional[str] = None):
        """Clear cache."""
        if key:
            self._cache.pop(key, None)
            self._cache_timestamps.pop(key, None)
        else:
            self._cache.clear()
            self._cache_timestamps.clear()


class LeaseManagerMixin:
    """Mixin for managing leases and renewals."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._active_leases: Dict[str, str] = {}
        self._renewal_tasks: Dict[str, asyncio.Task] = {}
        
    async def _track_lease(self, key: str, lease_id: str, ttl: int):
        """Track a lease for renewal."""
        self._active_leases[key] = lease_id
        
        # Cancel existing renewal task if any
        if key in self._renewal_tasks:
            self._renewal_tasks[key].cancel()
            
        # Start renewal task
        self._renewal_tasks[key] = self._create_task(
            self._renew_lease_loop(key, lease_id, ttl)
        )
        
    async def _renew_lease_loop(self, key: str, lease_id: str, ttl: int):
        """Renew lease periodically."""
        renewal_interval = max(ttl // 3, 10)  # Renew at 1/3 of TTL
        
        while True:
            try:
                await asyncio.sleep(renewal_interval)
                await self._renew_lease(lease_id)
                logger.debug(f"Renewed lease for {key}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Failed to renew lease for {key}: {e}")
                break
                
    async def _renew_lease(self, lease_id: str):
        """Renew a specific lease."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _renew_lease method"
        )
        
    async def _revoke_all_leases(self):
        """Revoke all active leases."""
        for key, lease_id in list(self._active_leases.items()):
            try:
                await self._revoke_lease(lease_id)
                del self._active_leases[key]
            except Exception as e:
                logger.error(f"Failed to revoke lease {lease_id}: {e}")
                
    async def _revoke_lease(self, lease_id: str):
        """Revoke a specific lease."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _revoke_lease method"
        )


class ConfigWatcherMixin:
    """Mixin for watching configuration changes."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._config_watchers: Dict[str, asyncio.Task] = {}
        self._config_callbacks: Dict[str, List[callable]] = {}
        
    async def watch_config(self, key: str, callback: callable):
        """Watch configuration key for changes."""
        # Add callback
        if key not in self._config_callbacks:
            self._config_callbacks[key] = []
        self._config_callbacks[key].append(callback)
        
        # Start watcher if not already running
        if key not in self._config_watchers:
            self._config_watchers[key] = self._create_task(
                self._watch_key_loop(key)
            )
            
    async def _watch_key_loop(self, key: str):
        """Watch a key for changes."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _watch_key_loop method"
        )
        
    async def _notify_callbacks(self, key: str, value: Any):
        """Notify all callbacks for a key."""
        for callback in self._config_callbacks.get(key, []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(key, value)
                else:
                    callback(key, value)
            except Exception as e:
                logger.error(f"Error in config callback for {key}: {e}")
                
    async def _stop_watchers(self):
        """Stop all config watchers."""
        for task in self._config_watchers.values():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._config_watchers.clear() 