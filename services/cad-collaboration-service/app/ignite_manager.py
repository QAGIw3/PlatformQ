import asyncio
from typing import Dict, Optional, Any, List
from pyignite import Client, Cache
from pyignite.datatypes import String, LongObject, BinaryObject
import json
import zlib
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class IgniteManager:
    """Manager for Apache Ignite in-memory caching of CAD sessions and geometry data"""
    
    def __init__(self, 
                 ignite_hosts: List[str] = None,
                 cache_config: Dict[str, Any] = None):
        self.hosts = ignite_hosts or ['ignite:10800']
        self.client = Client()
        self.connected = False
        
        # Cache names
        self.ACTIVE_GEOMETRY_CACHE = "active-geometry"
        self.SESSION_STATE_CACHE = "cad-sessions"
        self.OPERATION_LOG_CACHE = "operation-log"
        self.USER_PRESENCE_CACHE = "user-presence"
        self.MESH_SNAPSHOT_CACHE = "geometry-snapshots"
        
        # Cache configurations
        self.cache_config = cache_config or {
            self.ACTIVE_GEOMETRY_CACHE: {
                'cache_mode': 'REPLICATED',
                'atomicity_mode': 'TRANSACTIONAL',
                'backups': 2,
                'eviction_policy': 'LRU',
                'max_memory': 1024 * 1024 * 1024  # 1GB
            },
            self.SESSION_STATE_CACHE: {
                'cache_mode': 'PARTITIONED',
                'atomicity_mode': 'TRANSACTIONAL',
                'backups': 2,
                'expiry_policy': {
                    'access': 24 * 60 * 60  # 24 hours
                }
            },
            self.OPERATION_LOG_CACHE: {
                'cache_mode': 'PARTITIONED',
                'atomicity_mode': 'ATOMIC',
                'backups': 1,
                'expiry_policy': {
                    'ttl': 24 * 60 * 60  # 24 hours
                }
            }
        }
        
    async def connect(self):
        """Connect to Ignite cluster"""
        try:
            self.client.connect(self.hosts)
            self.connected = True
            logger.info(f"Connected to Ignite cluster at {self.hosts}")
            
            # Create caches if they don't exist
            await self._initialize_caches()
            
        except Exception as e:
            logger.error(f"Failed to connect to Ignite: {e}")
            raise
            
    async def disconnect(self):
        """Disconnect from Ignite cluster"""
        if self.connected:
            self.client.close()
            self.connected = False
            logger.info("Disconnected from Ignite cluster")
            
    async def _initialize_caches(self):
        """Initialize required caches with configurations"""
        for cache_name, config in self.cache_config.items():
            try:
                cache = self.client.get_or_create_cache(cache_name)
                logger.info(f"Initialized cache: {cache_name}")
            except Exception as e:
                logger.error(f"Failed to create cache {cache_name}: {e}")
                
    # CAD Session Management
    
    async def get_session_state(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get CAD session state from cache"""
        cache = self.client.get_cache(self.SESSION_STATE_CACHE)
        state_data = cache.get(session_id)
        
        if state_data:
            return json.loads(state_data)
        return None
        
    async def update_session_state(self, session_id: str, state: Dict[str, Any]):
        """Update CAD session state in cache"""
        cache = self.client.get_cache(self.SESSION_STATE_CACHE)
        state_json = json.dumps(state)
        cache.put(session_id, state_json)
        
    async def get_active_geometry(self, session_id: str) -> Optional[bytes]:
        """Get compressed CRDT state for active geometry"""
        cache = self.client.get_cache(self.ACTIVE_GEOMETRY_CACHE)
        return cache.get(session_id)
        
    async def update_active_geometry(self, session_id: str, crdt_state: bytes):
        """Update compressed CRDT state for active geometry"""
        cache = self.client.get_cache(self.ACTIVE_GEOMETRY_CACHE)
        
        # Compress if not already compressed
        if not self._is_compressed(crdt_state):
            crdt_state = zlib.compress(crdt_state)
            
        cache.put(session_id, crdt_state)
        
    # Operation Log Management
    
    async def append_operation(self, session_id: str, operation: Dict[str, Any]):
        """Append operation to the distributed log"""
        cache = self.client.get_cache(self.OPERATION_LOG_CACHE)
        
        # Create composite key: session_id:timestamp:operation_id
        timestamp = int(datetime.utcnow().timestamp() * 1000000)  # Microseconds
        key = f"{session_id}:{timestamp}:{operation['id']}"
        
        cache.put(key, json.dumps(operation))
        
    async def get_operations_range(self, session_id: str, 
                                  start_time: Optional[datetime] = None,
                                  end_time: Optional[datetime] = None) -> List[Dict[str, Any]]:
        """Get operations within a time range"""
        cache = self.client.get_cache(self.OPERATION_LOG_CACHE)
        
        # Convert times to microseconds
        start_key = f"{session_id}:0" if not start_time else \
                   f"{session_id}:{int(start_time.timestamp() * 1000000)}"
        end_key = f"{session_id}:9999999999999999" if not end_time else \
                 f"{session_id}:{int(end_time.timestamp() * 1000000)}"
        
        # Scan cache for keys in range
        operations = []
        scan = cache.scan()
        
        for key, value in scan:
            if isinstance(key, str) and key.startswith(session_id):
                if start_key <= key <= end_key:
                    operations.append(json.loads(value))
                    
        # Sort by timestamp (embedded in key)
        operations.sort(key=lambda op: op.get('timestamp', 0))
        
        return operations
        
    # User Presence Management
    
    async def update_user_presence(self, session_id: str, user_id: str, 
                                  presence_data: Dict[str, Any]):
        """Update user presence information (cursor position, viewport, etc.)"""
        cache = self.client.get_cache(self.USER_PRESENCE_CACHE)
        
        key = f"{session_id}:{user_id}"
        presence_data['last_update'] = datetime.utcnow().isoformat()
        
        cache.put(key, json.dumps(presence_data), ttl=30)  # 30 second TTL
        
    async def get_session_presence(self, session_id: str) -> Dict[str, Dict[str, Any]]:
        """Get all user presence data for a session"""
        cache = self.client.get_cache(self.USER_PRESENCE_CACHE)
        
        presence_map = {}
        scan = cache.scan()
        
        for key, value in scan:
            if isinstance(key, str) and key.startswith(f"{session_id}:"):
                user_id = key.split(":", 1)[1]
                presence_map[user_id] = json.loads(value)
                
        return presence_map
        
    # Mesh Snapshot Management
    
    async def store_mesh_snapshot(self, snapshot_id: str, mesh_data: bytes, 
                                 metadata: Optional[Dict[str, Any]] = None):
        """Store a mesh snapshot with metadata"""
        cache = self.client.get_cache(self.MESH_SNAPSHOT_CACHE)
        
        # Compress mesh data
        compressed_data = zlib.compress(mesh_data) if not self._is_compressed(mesh_data) else mesh_data
        
        # Store with metadata
        snapshot_data = {
            'data': compressed_data,
            'metadata': metadata or {},
            'created_at': datetime.utcnow().isoformat(),
            'size_compressed': len(compressed_data),
            'size_original': len(mesh_data)
        }
        
        cache.put(snapshot_id, snapshot_data)
        
    async def get_mesh_snapshot(self, snapshot_id: str) -> Optional[Dict[str, Any]]:
        """Get a mesh snapshot with metadata"""
        cache = self.client.get_cache(self.MESH_SNAPSHOT_CACHE)
        snapshot_data = cache.get(snapshot_id)
        
        if snapshot_data:
            # Decompress data
            snapshot_data['data'] = zlib.decompress(snapshot_data['data'])
            return snapshot_data
            
        return None
        
    # Utility Methods
    
    def _is_compressed(self, data: bytes) -> bool:
        """Check if data is zlib compressed"""
        return data[:2] == b'\x78\x9c' or data[:2] == b'\x78\x01' or data[:2] == b'\x78\xda'
        
    async def clear_session_data(self, session_id: str):
        """Clear all cached data for a session"""
        # Clear from all caches
        for cache_name in [self.ACTIVE_GEOMETRY_CACHE, self.SESSION_STATE_CACHE]:
            cache = self.client.get_cache(cache_name)
            cache.remove(session_id)
            
        # Clear operation logs
        cache = self.client.get_cache(self.OPERATION_LOG_CACHE)
        scan = cache.scan()
        
        keys_to_remove = []
        for key, _ in scan:
            if isinstance(key, str) and key.startswith(f"{session_id}:"):
                keys_to_remove.append(key)
                
        for key in keys_to_remove:
            cache.remove(key)
            
        # Clear user presence
        cache = self.client.get_cache(self.USER_PRESENCE_CACHE)
        scan = cache.scan()
        
        keys_to_remove = []
        for key, _ in scan:
            if isinstance(key, str) and key.startswith(f"{session_id}:"):
                keys_to_remove.append(key)
                
        for key in keys_to_remove:
            cache.remove(key)
            
        logger.info(f"Cleared all cached data for session {session_id}")
        
    async def get_cache_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all caches"""
        stats = {}
        
        for cache_name in self.cache_config.keys():
            try:
                cache = self.client.get_cache(cache_name)
                stats[cache_name] = {
                    'size': cache.get_size(),
                    # Additional metrics would be available through Ignite metrics API
                }
            except Exception as e:
                logger.error(f"Failed to get stats for cache {cache_name}: {e}")
                stats[cache_name] = {'error': str(e)}
                
        return stats 