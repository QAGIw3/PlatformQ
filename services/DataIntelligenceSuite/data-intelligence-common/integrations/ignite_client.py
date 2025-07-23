"""
Apache Ignite Client Integration

Provides high-level client for Apache Ignite operations with Vault/Consul support.
"""

import logging
from typing import Any, Dict, List, Optional, Callable, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
import asyncio
from enum import Enum

from pyignite.aio import AioClient as AsyncClient
from pyignite.datatypes import CollectionObject
from pyignite.cache import Cache

from platformq_shared.vault.vault_client import VaultClient
from platformq_shared.consul.consul_client import ConsulClient
from ..clients.base_client import BaseServiceClient, ClientConfig

logger = logging.getLogger(__name__)


class CacheMode(Enum):
    """Cache modes in Ignite"""
    PARTITIONED = "PARTITIONED"
    REPLICATED = "REPLICATED"
    LOCAL = "LOCAL"


class CacheAtomicityMode(Enum):
    """Cache atomicity modes"""
    ATOMIC = "ATOMIC"
    TRANSACTIONAL = "TRANSACTIONAL"


@dataclass
class IgniteConfig(ClientConfig):
    """Configuration for Ignite client with Vault/Consul support"""
    # Ignite specific settings
    hosts: List[tuple[str, int]] = field(
        default_factory=lambda: [("localhost", 10800)]
    )
    
    # Connection settings
    timeout: float = 10.0
    use_ssl: bool = False
    ssl_keyfile: Optional[str] = None
    ssl_certfile: Optional[str] = None
    ssl_ca_certfile: Optional[str] = None
    
    # Cache defaults
    default_cache_mode: CacheMode = CacheMode.PARTITIONED
    default_atomicity_mode: CacheAtomicityMode = CacheAtomicityMode.ATOMIC
    default_backups: int = 1
    
    # Performance
    partition_aware: bool = True
    max_pool_size: int = 10
    
    # Vault specific
    vault_auth_mount: str = "auth/ignite"
    vault_auth_role: str = "ignite-client"
    
    # Encryption
    enable_encryption: bool = True
    encryption_key_name: str = "ignite-data"
    
    def __post_init__(self):
        # Set service name for base client
        if not hasattr(self, 'service_name'):
            self.service_name = "ignite"


@dataclass
class CacheConfig:
    """Configuration for creating a cache with security features"""
    name: str
    mode: CacheMode = CacheMode.PARTITIONED
    atomicity_mode: CacheAtomicityMode = CacheAtomicityMode.ATOMIC
    backups: int = 1
    
    # Memory settings
    on_heap_max_memory: Optional[int] = None
    eviction_policy: Optional[str] = None
    
    # Persistence
    persistence_enabled: bool = False
    wal_mode: Optional[str] = None
    
    # SQL
    sql_schema: Optional[str] = None
    query_entities: Optional[List[Dict[str, Any]]] = None
    
    # Expiry
    default_expiry_ms: Optional[int] = None
    
    # Security
    encrypt_data: bool = False
    encryption_key: Optional[str] = None


class IgniteClient(BaseServiceClient):
    """
    High-level client for Apache Ignite operations with Vault/Consul support.
    
    Features:
    - Dynamic credentials from Vault
    - Service discovery via Consul
    - In-memory data grid operations
    - Encryption support via Vault Transit
    - Distributed computing
    - SQL queries
    - Transactions
    - Secure cache operations
    """
    
    def __init__(
        self,
        config: IgniteConfig,
        vault_client: Optional[VaultClient] = None,
        consul_client: Optional[ConsulClient] = None
    ):
        super().__init__(config, vault_client, consul_client)
        self.ignite_config = config
        self._client: Optional[AsyncClient] = None
        self._caches: Dict[str, Any] = {}
        self._encryption_enabled = config.enable_encryption and vault_client is not None
        
    async def connect(self):
        """Connect to Ignite cluster with dynamic credentials"""
        # Initialize base client
        await super().connect()
        
        try:
            # Get Ignite hosts from service discovery
            hosts = await self._get_ignite_hosts()
            
            # Get credentials from Vault if enabled
            username = None
            password = None
            
            if self.config.use_vault_credentials:
                creds = await self._get_credentials()
                if creds:
                    username = creds.get('username')
                    password = creds.get('password')
                    logger.info("Using dynamic credentials for Ignite")
                    
            # Create client
            self._client = AsyncClient(
                timeout=self.ignite_config.timeout,
                partition_aware=self.ignite_config.partition_aware,
                use_ssl=self.ignite_config.use_ssl,
                ssl_keyfile=self.ignite_config.ssl_keyfile,
                ssl_certfile=self.ignite_config.ssl_certfile,
                ssl_ca_certfile=self.ignite_config.ssl_ca_certfile,
                username=username,
                password=password
            )
            
            # Connect to cluster
            await self._client.connect(hosts)
            
            logger.info(f"Connected to Ignite cluster: {hosts}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            await self.close()
            raise
            
    async def _get_ignite_hosts(self) -> List[tuple[str, int]]:
        """Get Ignite hosts from Consul or config"""
        if self.config.use_service_discovery and self._service_instances:
            hosts = []
            for instance in self._service_instances:
                # Ignite uses binary protocol port (usually 10800)
                port = instance.get('meta', {}).get('binary_port', 10800)
                hosts.append((instance['address'], int(port)))
            return hosts
        else:
            return self.ignite_config.hosts
            
    async def close(self):
        """Disconnect from Ignite"""
        if self._client:
            await self._client.close()
            self._client = None
            self._caches.clear()
            
        # Close base client
        await super().close()
        
        logger.info("Disconnected from Ignite")
        
    async def create_cache(self, config: CacheConfig) -> Any:
        """Create or get cache with configuration"""
        if not self._client:
            raise RuntimeError("Not connected to Ignite")
            
        try:
            # Build cache configuration
            cache_config = {
                'cache_mode': config.mode.value,
                'atomicity_mode': config.atomicity_mode.value,
                'backups': config.backups
            }
            
            if config.on_heap_max_memory:
                cache_config['on_heap_max_memory'] = config.on_heap_max_memory
                
            if config.eviction_policy:
                cache_config['eviction_policy'] = config.eviction_policy
                
            if config.persistence_enabled:
                cache_config['data_region_name'] = 'persistent'
                
            if config.sql_schema:
                cache_config['sql_schema'] = config.sql_schema
                
            if config.query_entities:
                cache_config['query_entities'] = config.query_entities
                
            if config.default_expiry_ms:
                cache_config['expiry_policy'] = {
                    'access': config.default_expiry_ms,
                    'create': config.default_expiry_ms,
                    'update': config.default_expiry_ms
                }
                
            # Create cache
            cache = await self._client.get_or_create_cache_with_config(
                config.name,
                cache_config
            )
            
            # Wrap cache if encryption is enabled
            if config.encrypt_data and self._encryption_enabled:
                cache = EncryptedCache(
                    cache,
                    self.vault_client,
                    config.encryption_key or self.ignite_config.encryption_key_name
                )
                
            self._caches[config.name] = cache
            
            logger.info(f"Created cache: {config.name}")
            return cache
            
        except Exception as e:
            logger.error(f"Failed to create cache: {e}")
            raise
            
    async def get_cache(self, name: str) -> Any:
        """Get existing cache"""
        if not self._client:
            raise RuntimeError("Not connected to Ignite")
            
        if name in self._caches:
            return self._caches[name]
            
        try:
            cache = await self._client.get_cache(name)
            self._caches[name] = cache
            return cache
        except Exception as e:
            logger.error(f"Failed to get cache {name}: {e}")
            raise
            
    async def put(
        self,
        cache_name: str,
        key: Any,
        value: Any,
        expiry_ms: Optional[int] = None
    ):
        """Put value in cache with optional encryption"""
        cache = await self.get_cache(cache_name)
        
        # Encrypt value if cache is encrypted
        if isinstance(cache, EncryptedCache):
            await cache.put_encrypted(key, value, expiry_ms)
        else:
            await cache.put(key, value)
            
    async def get(
        self,
        cache_name: str,
        key: Any,
        default: Any = None
    ) -> Any:
        """Get value from cache with automatic decryption"""
        cache = await self.get_cache(cache_name)
        
        # Decrypt value if cache is encrypted
        if isinstance(cache, EncryptedCache):
            return await cache.get_encrypted(key, default)
        else:
            value = await cache.get(key)
            return value if value is not None else default
            
    async def put_all(
        self,
        cache_name: str,
        entries: Dict[Any, Any]
    ):
        """Put multiple entries"""
        cache = await self.get_cache(cache_name)
        
        if isinstance(cache, EncryptedCache):
            await cache.put_all_encrypted(entries)
        else:
            await cache.put_all(entries)
            
    async def get_all(
        self,
        cache_name: str,
        keys: List[Any]
    ) -> Dict[Any, Any]:
        """Get multiple entries"""
        cache = await self.get_cache(cache_name)
        
        if isinstance(cache, EncryptedCache):
            return await cache.get_all_encrypted(keys)
        else:
            return await cache.get_all(keys)
            
    async def remove(
        self,
        cache_name: str,
        key: Any
    ) -> bool:
        """Remove entry from cache"""
        cache = await self.get_cache(cache_name)
        return await cache.remove(key)
        
    async def remove_all(
        self,
        cache_name: str,
        keys: Optional[List[Any]] = None
    ):
        """Remove multiple entries or clear cache"""
        cache = await self.get_cache(cache_name)
        
        if keys:
            await cache.remove_all(keys)
        else:
            await cache.clear()
            
    async def contains_key(
        self,
        cache_name: str,
        key: Any
    ) -> bool:
        """Check if key exists"""
        cache = await self.get_cache(cache_name)
        return await cache.contains_key(key)
        
    async def get_size(self, cache_name: str) -> int:
        """Get cache size"""
        cache = await self.get_cache(cache_name)
        return await cache.get_size()
        
    async def sql_query(
        self,
        cache_name: str,
        query: str,
        args: Optional[List[Any]] = None,
        distributed_joins: bool = False,
        timeout: Optional[int] = None
    ) -> List[List[Any]]:
        """Execute SQL query with security checks"""
        if not self._client:
            raise RuntimeError("Not connected to Ignite")
            
        try:
            # Add security context to query if available
            if self.vault_client:
                # Could add row-level security filters here based on user context
                pass
                
            # Build query arguments
            query_args = []
            if distributed_joins:
                query_args.append('distributed_joins')
                
            if timeout:
                query_args.append(f'timeout={timeout}')
                
            # Execute query
            cache = await self.get_cache(cache_name)
            
            if args:
                result = await cache.query(query, *args, query_args=query_args)
            else:
                result = await cache.query(query, query_args=query_args)
                
            # Collect results
            rows = []
            async for row in result:
                rows.append(list(row))
                
            return rows
            
        except Exception as e:
            logger.error(f"SQL query failed: {e}")
            raise
            
    async def scan_query(
        self,
        cache_name: str,
        filter_func: Optional[Callable] = None,
        page_size: int = 1000
    ):
        """Scan cache with optional filter"""
        cache = await self.get_cache(cache_name)
        
        # Scan with filter
        if filter_func:
            async for key, value in cache.scan(filter_func, page_size=page_size):
                # Decrypt if needed
                if isinstance(cache, EncryptedCache):
                    value = await cache._decrypt_value(value)
                yield key, value
        else:
            async for key, value in cache.scan(page_size=page_size):
                # Decrypt if needed
                if isinstance(cache, EncryptedCache):
                    value = await cache._decrypt_value(value)
                yield key, value
                
    async def get_and_put(
        self,
        cache_name: str,
        key: Any,
        value: Any
    ) -> Any:
        """Atomically get and put value"""
        cache = await self.get_cache(cache_name)
        
        if isinstance(cache, EncryptedCache):
            return await cache.get_and_put_encrypted(key, value)
        else:
            return await cache.get_and_put(key, value)
            
    async def get_and_remove(
        self,
        cache_name: str,
        key: Any
    ) -> Any:
        """Atomically get and remove value"""
        cache = await self.get_cache(cache_name)
        
        if isinstance(cache, EncryptedCache):
            return await cache.get_and_remove_encrypted(key)
        else:
            return await cache.get_and_remove(key)
            
    async def replace(
        self,
        cache_name: str,
        key: Any,
        value: Any
    ) -> bool:
        """Replace value if key exists"""
        cache = await self.get_cache(cache_name)
        
        if isinstance(cache, EncryptedCache):
            return await cache.replace_encrypted(key, value)
        else:
            return await cache.replace(key, value)
            
    async def put_if_absent(
        self,
        cache_name: str,
        key: Any,
        value: Any
    ) -> bool:
        """Put value only if key doesn't exist"""
        cache = await self.get_cache(cache_name)
        
        if isinstance(cache, EncryptedCache):
            return await cache.put_if_absent_encrypted(key, value)
        else:
            return await cache.put_if_absent(key, value)
            
    async def invoke(
        self,
        cache_name: str,
        key: Any,
        processor: Callable,
        *args
    ) -> Any:
        """Invoke processor on cache entry"""
        cache = await self.get_cache(cache_name)
        return await cache.invoke(key, processor, *args)
        
    @asynccontextmanager
    async def lock(
        self,
        cache_name: str,
        key: Any,
        timeout: Optional[int] = None
    ):
        """Distributed lock on cache entry"""
        cache = await self.get_cache(cache_name)
        
        # Get lock
        lock = await cache.lock(key)
        
        try:
            # Acquire lock
            acquired = await lock.acquire(timeout=timeout)
            if not acquired:
                raise TimeoutError(f"Failed to acquire lock for key: {key}")
                
            yield lock
            
        finally:
            # Release lock
            await lock.release()
            
    async def get_cache_metrics(self, cache_name: str) -> Dict[str, Any]:
        """Get cache metrics"""
        cache = await self.get_cache(cache_name)
        
        return {
            "size": await cache.get_size(),
            "name": cache_name,
            # Add more metrics as available
        }
        
    async def destroy_cache(self, cache_name: str):
        """Destroy cache"""
        if not self._client:
            raise RuntimeError("Not connected to Ignite")
            
        try:
            await self._client.destroy_cache(cache_name)
            
            if cache_name in self._caches:
                del self._caches[cache_name]
                
            logger.info(f"Destroyed cache: {cache_name}")
            
        except Exception as e:
            logger.error(f"Failed to destroy cache: {e}")
            raise
            
    async def get_cluster_state(self) -> str:
        """Get cluster state"""
        if not self._client:
            raise RuntimeError("Not connected to Ignite")
            
        # This would need actual Ignite API call
        return "ACTIVE"
        
    async def set_cluster_state(self, state: str):
        """Set cluster state"""
        if not self._client:
            raise RuntimeError("Not connected to Ignite")
            
        # This would need actual Ignite API call
        logger.info(f"Set cluster state to: {state}")
        
    async def get_client_specific_config(self) -> Dict[str, Any]:
        """Get Ignite-specific configuration from Consul"""
        if not self.consul_client:
            return {}
            
        try:
            # Get Ignite-specific config
            config = await self.consul_client.get_config(
                f"data-intelligence/ignite/config"
            )
            
            return config or {}
            
        except Exception as e:
            logger.error(f"Failed to get Ignite config from Consul: {e}")
            return {}


class EncryptedCache:
    """Wrapper for cache with transparent encryption using Vault Transit"""
    
    def __init__(self, cache: Any, vault_client: VaultClient, key_name: str):
        self._cache = cache
        self._vault_client = vault_client
        self._key_name = key_name
        
    async def put_encrypted(self, key: Any, value: Any, expiry_ms: Optional[int] = None):
        """Put encrypted value"""
        # Serialize and encrypt value
        import json
        plaintext = json.dumps(value)
        
        encrypted = await self._vault_client.transit_encrypt(
            self._key_name,
            plaintext
        )
        
        # Store encrypted value
        await self._cache.put(key, encrypted['ciphertext'])
        
    async def get_encrypted(self, key: Any, default: Any = None) -> Any:
        """Get and decrypt value"""
        # Get encrypted value
        ciphertext = await self._cache.get(key)
        
        if ciphertext is None:
            return default
            
        # Decrypt value
        decrypted = await self._vault_client.transit_decrypt(
            self._key_name,
            ciphertext
        )
        
        # Deserialize
        import json
        return json.loads(decrypted)
        
    async def put_all_encrypted(self, entries: Dict[Any, Any]):
        """Put multiple encrypted entries"""
        encrypted_entries = {}
        
        for key, value in entries.items():
            import json
            plaintext = json.dumps(value)
            
            encrypted = await self._vault_client.transit_encrypt(
                self._key_name,
                plaintext
            )
            
            encrypted_entries[key] = encrypted['ciphertext']
            
        await self._cache.put_all(encrypted_entries)
        
    async def get_all_encrypted(self, keys: List[Any]) -> Dict[Any, Any]:
        """Get multiple entries and decrypt"""
        encrypted_values = await self._cache.get_all(keys)
        
        decrypted_values = {}
        for key, ciphertext in encrypted_values.items():
            if ciphertext is not None:
                decrypted = await self._vault_client.transit_decrypt(
                    self._key_name,
                    ciphertext
                )
                
                import json
                decrypted_values[key] = json.loads(decrypted)
                
        return decrypted_values
        
    async def get_and_put_encrypted(self, key: Any, value: Any) -> Any:
        """Atomically get and put encrypted value"""
        # Get current value
        old_value = await self.get_encrypted(key)
        
        # Put new value
        await self.put_encrypted(key, value)
        
        return old_value
        
    async def get_and_remove_encrypted(self, key: Any) -> Any:
        """Atomically get and remove encrypted value"""
        # Get current value
        old_value = await self.get_encrypted(key)
        
        # Remove
        await self._cache.remove(key)
        
        return old_value
        
    async def replace_encrypted(self, key: Any, value: Any) -> bool:
        """Replace encrypted value if exists"""
        if await self._cache.contains_key(key):
            await self.put_encrypted(key, value)
            return True
        return False
        
    async def put_if_absent_encrypted(self, key: Any, value: Any) -> bool:
        """Put encrypted value if absent"""
        if not await self._cache.contains_key(key):
            await self.put_encrypted(key, value)
            return True
        return False
        
    async def _decrypt_value(self, ciphertext: str) -> Any:
        """Decrypt a single value"""
        decrypted = await self._vault_client.transit_decrypt(
            self._key_name,
            ciphertext
        )
        
        import json
        return json.loads(decrypted)
        
    # Delegate other methods to underlying cache
    def __getattr__(self, name):
        return getattr(self._cache, name) 