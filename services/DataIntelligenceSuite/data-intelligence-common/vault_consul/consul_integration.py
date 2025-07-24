"""Consul integration for DataIntelligenceSuite services."""

from typing import Dict, Any, Optional, List, Callable, Set
from dataclasses import dataclass, field
from datetime import datetime
import asyncio
import logging
import json
import uuid

from platformq_shared.consul.consul_client import ConsulClient
import consul.aio
from .base import BaseIntegration, CacheableMixin, ConfigWatcherMixin

logger = logging.getLogger(__name__)


@dataclass 
class ConsulConfig:
    """Consul configuration for DataIntelligenceSuite services."""
    
    # Service registration
    enable_service_registration: bool = True
    deregister_on_shutdown: bool = True
    
    # Health checks
    enable_health_checks: bool = True
    health_check_interval: str = "10s"
    health_check_timeout: str = "5s"
    
    # Service discovery
    enable_service_discovery: bool = True
    discovery_cache_ttl: int = 60  # seconds
    
    # Configuration management
    enable_config_watch: bool = True
    config_prefix: str = "data-intelligence"
    
    # Distributed coordination
    enable_distributed_locks: bool = True
    default_lock_ttl: int = 60  # seconds
    
    # KV store paths
    kv_paths: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        if not self.kv_paths:
            self.kv_paths = [
                "config",
                "features",
                "policies",
                "schemas"
            ]


class ConsulIntegration(BaseIntegration, CacheableMixin, ConfigWatcherMixin):
    """
    Consul integration for DataIntelligenceSuite services.
    
    Provides:
    - Service registration and discovery
    - Health checking
    - Configuration management
    - Distributed coordination
    - KV store operations
    """
    
    def __init__(
        self,
        consul_client: ConsulClient,
        service_name: str,
        config: Optional[ConsulConfig] = None
    ):
        super().__init__(service_name, config)
        self.client = consul_client
        self.config = config or ConsulConfig()
        
        # Distributed locks
        self._active_locks: Dict[str, consul.aio.Semaphore] = {}
        
        # Service registration
        self._service_id: Optional[str] = None
        self._session_id: Optional[str] = None
        
    async def initialize(self):
        """Initialize Consul integration."""
        try:
            # Create session for distributed operations
            self._session_id = await self._create_session()
            
            # Initialize KV paths
            await self._initialize_kv_paths()
            
            # Start service discovery cache refresh
            if self.config.enable_service_discovery:
                self._create_task(self._refresh_service_cache())
            
            self._initialized = True
            logger.info(f"Consul integration initialized for {self.service_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Consul integration: {e}")
            raise
            
    async def _create_session(self) -> str:
        """Create Consul session for distributed operations."""
        session_id = str(uuid.uuid4())
        
        # Create session with TTL
        response = await self.client.agent.session.create(
            name=f"{self.service_name}-session",
            ttl=120,  # 2 minutes
            behavior="delete",
            lock_delay=15
        )
        
        session_id = response["ID"]
        
        # Start session renewal
        self._create_task(self._renew_session(session_id))
        
        return session_id
        
    async def _renew_session(self, session_id: str):
        """Renew Consul session periodically."""
        while True:
            try:
                await asyncio.sleep(60)  # Renew every minute
                await self.client.agent.session.renew(session_id)
                logger.debug(f"Renewed session {session_id}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Failed to renew session: {e}")
                await asyncio.sleep(10)
                
    async def _initialize_kv_paths(self):
        """Initialize KV store paths."""
        for path in self.config.kv_paths:
            full_path = f"{self.config.config_prefix}/{self.service_name}/{path}"
            
            # Check if path exists
            try:
                await self.client.kv.get(full_path)
            except:
                # Create path with initial data
                await self.client.kv.put(
                    full_path,
                    json.dumps({
                        "initialized": datetime.utcnow().isoformat(),
                        "service": self.service_name,
                        "version": "1.0.0"
                    })
                )
                logger.info(f"Initialized KV path: {full_path}")
                
    # Service registration
    async def register_service(
        self,
        service_id: str,
        tags: List[str] = None,
        meta: Dict[str, str] = None,
        check: Dict[str, Any] = None
    ) -> str:
        """Register service with Consul."""
        if not self.config.enable_service_registration:
            logger.info("Service registration disabled")
            return service_id
            
        self._service_id = service_id
        
        # Delegate to client
        await self.client.register_service({
            "ID": service_id,
            "Name": self.service_name,
            "Tags": tags or [],
            "Meta": meta or {},
            "Check": check or {
                "HTTP": f"http://localhost:8000/health",
                "Interval": self.config.health_check_interval,
                "Timeout": self.config.health_check_timeout
            }
        })
        
        logger.info(f"Registered service {self.service_name} with ID {service_id}")
        return service_id
        
    async def deregister_service(self, service_id: str):
        """Deregister service from Consul."""
        if not self.config.deregister_on_shutdown:
            return
            
        await self.client.agent.service.deregister(service_id)
        logger.info(f"Deregistered service {service_id}")
        
    # Service discovery
    async def discover_service(
        self,
        service_name: str,
        passing_only: bool = True
    ) -> List[Dict[str, Any]]:
        """Discover instances of a service."""
        if not self.config.enable_service_discovery:
            raise ValueError("Service discovery is disabled")
            
        # Check cache
        cached = self._get_from_cache(service_name, self.config.discovery_cache_ttl)
        if cached is not None:
            return cached
                
        # Query Consul
        _, services = await self.client.health.service(
            service_name,
            passing=passing_only
        )
        
        # Parse service instances
        instances = []
        for service in services:
            instances.append({
                "id": service["Service"]["ID"],
                "address": service["Service"]["Address"] or service["Node"]["Address"],
                "port": service["Service"]["Port"],
                "tags": service["Service"]["Tags"],
                "meta": service["Service"]["Meta"],
                "status": service["Checks"][0]["Status"] if service["Checks"] else "unknown"
            })
            
        # Update cache
        self._set_cache(service_name, instances)
        
        return instances
        
    async def _refresh_service_cache(self):
        """Periodically refresh service discovery cache."""
        while True:
            try:
                await asyncio.sleep(self.config.discovery_cache_ttl)
                
                # Refresh all cached services
                for service_name in list(self._cache.keys()):
                    try:
                        await self.discover_service(service_name)
                    except Exception as e:
                        logger.error(f"Failed to refresh service {service_name}: {e}")
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in service cache refresh: {e}")
                
    # Configuration management
    async def get_config(self, key: str) -> Optional[Dict[str, Any]]:
        """Get configuration from Consul KV store."""
        full_key = f"{self.config.config_prefix}/{key}"
        
        try:
            _, data = await self.client.kv.get(full_key)
            if data:
                return json.loads(data["Value"].decode())
            return None
        except Exception as e:
            logger.error(f"Failed to get config {key}: {e}")
            return None
            
    async def put_config(self, key: str, value: Dict[str, Any]):
        """Put configuration to Consul KV store."""
        full_key = f"{self.config.config_prefix}/{key}"
        
        await self.client.kv.put(
            full_key,
            json.dumps(value)
        )
        
    async def watch_config(
        self,
        key: str,
        callback: Callable[[str, Any], None]
    ):
        """Watch configuration key for changes."""
        if not self.config.enable_config_watch:
            raise ValueError("Configuration watching is disabled")
            
        full_key = f"{self.config.config_prefix}/{key}"
        await super().watch_config(full_key, callback)
            
    async def _watch_key_loop(self, key: str):
        """Watch a configuration key for changes."""
        index = None
        
        while True:
            try:
                # Watch for changes
                index, data = await self.client.kv.get(key, index=index)
                
                if data:
                    value = json.loads(data["Value"].decode())
                    
                    # Notify callbacks
                    await self._notify_callbacks(key, value)
                            
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error watching key {key}: {e}")
                await asyncio.sleep(5)
                
    # Distributed coordination
    async def acquire_lock(
        self,
        key: str,
        ttl: int = None
    ) -> bool:
        """Acquire a distributed lock."""
        if not self.config.enable_distributed_locks:
            raise ValueError("Distributed locks are disabled")
            
        ttl = ttl or self.config.default_lock_ttl
        full_key = f"{self.config.config_prefix}/locks/{key}"
        
        # Create lock
        lock = consul.aio.Semaphore(
            self.client,
            full_key,
            session_id=self._session_id,
            limit=1
        )
        
        # Try to acquire
        acquired = await lock.acquire(blocking=False)
        
        if acquired:
            self._active_locks[key] = lock
            
            # Auto-release after TTL
            self._create_task(self._auto_release_lock(key, ttl))
            
        return acquired
        
    async def release_lock(self, key: str):
        """Release a distributed lock."""
        if key in self._active_locks:
            lock = self._active_locks[key]
            await lock.release()
            del self._active_locks[key]
            
    async def _auto_release_lock(self, key: str, ttl: int):
        """Auto-release lock after TTL."""
        await asyncio.sleep(ttl)
        
        if key in self._active_locks:
            await self.release_lock(key)
            logger.info(f"Auto-released lock: {key}")
            
    # KV store operations
    async def get_kv(self, key: str) -> Optional[Any]:
        """Get value from KV store."""
        return await self.get_config(key)
        
    async def put_kv(self, key: str, value: Any):
        """Put value to KV store."""
        await self.put_config(key, value)
        
    async def delete_kv(self, key: str):
        """Delete key from KV store."""
        full_key = f"{self.config.config_prefix}/{key}"
        await self.client.kv.delete(full_key)
        
    async def list_kv(self, prefix: str) -> List[str]:
        """List keys with prefix."""
        full_prefix = f"{self.config.config_prefix}/{prefix}"
        _, keys = await self.client.kv.get(full_prefix, keys=True)
        return keys or []
        
    # Cleanup
    async def shutdown(self):
        """Shutdown Consul integration."""
        # Stop all watchers
        await self._stop_watchers()
        
        # Cancel all tasks
        await self._cancel_tasks()
        
        # Release locks
        for key in list(self._active_locks.keys()):
            await self.release_lock(key)
            
        # Destroy session
        if self._session_id:
            await self.client.agent.session.destroy(self._session_id)
            
        # Deregister service
        if self._service_id and self.config.deregister_on_shutdown:
            await self.deregister_service(self._service_id)
        
        self._initialized = False
        logger.info(f"Consul integration shutdown for {self.service_name}") 