"""
Apache Ignite manager for simulation state caching

Provides high-performance in-memory caching for active simulation states,
collaboration sessions, and real-time metrics.
"""

import asyncio
from typing import Dict, Optional, Any, List
from pyignite import Client, Cache
from pyignite.datatypes import String, LongObject, BinaryObject
import json
import zlib
import logging
from datetime import datetime, timedelta
import numpy as np

logger = logging.getLogger(__name__)


class SimulationIgniteManager:
    """Manager for Apache Ignite caching of simulation data"""
    
    def __init__(self, ignite_hosts: List[str] = None):
        self.hosts = ignite_hosts or ['ignite:10800']
        self.client = Client()
        self.connected = False
        
        # Cache names
        self.SIMULATION_STATE_CACHE = "simulation-states"
        self.SESSION_METADATA_CACHE = "simulation-sessions"
        self.AGENT_STATE_CACHE = "agent-states"
        self.METRICS_CACHE = "simulation-metrics"
        self.CHECKPOINT_CACHE = "simulation-checkpoints"
        self.FEDERATED_SHARED_STATE_CACHE = "federated-shared-states"
        
        # Cache configurations
        self.cache_config = {
            self.SIMULATION_STATE_CACHE: {
                'cache_mode': 'PARTITIONED',
                'atomicity_mode': 'TRANSACTIONAL',
                'backups': 2,
                'eviction_policy': 'LRU',
                'max_memory': 4 * 1024 * 1024 * 1024  # 4GB
            },
            self.AGENT_STATE_CACHE: {
                'cache_mode': 'PARTITIONED',
                'atomicity_mode': 'ATOMIC',
                'backups': 1,
                'expiry_policy': {
                    'ttl': 3600  # 1 hour
                }
            },
            self.SESSION_METADATA_CACHE: {
                'cache_mode': 'REPLICATED',
                'atomicity_mode': 'TRANSACTIONAL',
                'backups': 2
            },
            self.METRICS_CACHE: {
                'cache_mode': 'PARTITIONED',
                'atomicity_mode': 'ATOMIC',
                'backups': 1,
                'expiry_policy': {
                    'ttl': 300  # 5 minutes
                }
            },
            self.FEDERATED_SHARED_STATE_CACHE: {
                'cache_mode': 'PARTITIONED',
                'atomicity_mode': 'ATOMIC',
                'backups': 1
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
    
    # Simulation State Management
    
    async def save_simulation_state(self, session_id: str, state_data: bytes):
        """Save compressed simulation state"""
        cache = self.client.get_cache(self.SIMULATION_STATE_CACHE)
        
        # Store with metadata
        state_entry = {
            'data': state_data,
            'compressed_size': len(state_data),
            'last_updated': datetime.utcnow().isoformat(),
            'version': 1
        }
        
        cache.put(session_id, state_entry)
    
    async def get_simulation_state(self, session_id: str) -> Optional[bytes]:
        """Get compressed simulation state"""
        cache = self.client.get_cache(self.SIMULATION_STATE_CACHE)
        state_entry = cache.get(session_id)
        
        if state_entry:
            return state_entry['data']
        return None

    # Federated Simulation State Management
    
    async def create_federated_session_cache(self, federation_id: str):
        """Creates a dedicated cache for a federated simulation session."""
        cache_name = f"federated_{federation_id}"
        # This is a simplified approach. In a real-world scenario, you might use
        # cache templates or dynamic cache configuration.
        self.client.get_or_create_cache(cache_name)
        logger.info(f"Created dedicated cache for federation {federation_id}: {cache_name}")
        return cache_name

    async def update_shared_state(self, federation_id: str, key: str, value: Any):
        """Updates a value in the shared state for a federated simulation."""
        cache_name = f"federated_{federation_id}"
        cache = self.client.get_cache(cache_name)
        cache.put(key, value)
        
    async def get_shared_state(self, federation_id: str, key: str) -> Optional[Any]:
        """Gets a value from the shared state for a federated simulation."""
        cache_name = f"federated_{federation_id}"
        cache = self.client.get_cache(cache_name)
        return cache.get(key)
        
    # Agent State Management (for distributed processing)
    
    async def update_agent_batch(self, session_id: str, agents: List[Dict[str, Any]]):
        """Update a batch of agent states"""
        cache = self.client.get_cache(self.AGENT_STATE_CACHE)
        
        # Batch put for performance
        batch = {}
        for agent in agents:
            key = f"{session_id}:{agent['id']}"
            batch[key] = {
                'position': agent['position'],
                'velocity': agent['velocity'],
                'properties': agent.get('properties', {}),
                'last_updated': datetime.utcnow().timestamp()
            }
        
        cache.put_all(batch)
    
    async def get_agents_in_region(self, session_id: str, min_pos: List[float], 
                                  max_pos: List[float]) -> List[Dict[str, Any]]:
        """Get agents within a spatial region (for spatial partitioning)"""
        cache = self.client.get_cache(self.AGENT_STATE_CACHE)
        
        # In production, this would use Ignite's SQL capabilities
        # For now, we'll scan and filter
        agents = []
        scan = cache.scan()
        
        for key, value in scan:
            if isinstance(key, str) and key.startswith(f"{session_id}:"):
                pos = value.get('position', [0, 0, 0])
                if (min_pos[0] <= pos[0] <= max_pos[0] and
                    min_pos[1] <= pos[1] <= max_pos[1] and
                    min_pos[2] <= pos[2] <= max_pos[2]):
                    
                    agent_id = key.split(":", 1)[1]
                    agents.append({
                        'id': agent_id,
                        'position': pos,
                        'velocity': value.get('velocity', [0, 0, 0]),
                        'properties': value.get('properties', {})
                    })
        
        return agents
    
    # Session Management
    
    async def create_session_metadata(self, session_id: str, simulation_id: str, 
                                    creator_id: str):
        """Create session metadata"""
        cache = self.client.get_cache(self.SESSION_METADATA_CACHE)
        
        metadata = {
            'session_id': session_id,
            'simulation_id': simulation_id,
            'creator_id': creator_id,
            'created_at': datetime.utcnow().isoformat(),
            'active_users': [creator_id],
            'state': 'active',
            'agent_count': 0,
            'current_tick': 0
        }
        
        cache.put(session_id, metadata)
    
    async def update_session_metadata(self, session_id: str, updates: Dict[str, Any]):
        """Update session metadata"""
        cache = self.client.get_cache(self.SESSION_METADATA_CACHE)
        
        metadata = cache.get(session_id)
        if metadata:
            metadata.update(updates)
            metadata['last_updated'] = datetime.utcnow().isoformat()
            cache.put(session_id, metadata)
    
    async def get_active_sessions(self) -> List[Dict[str, Any]]:
        """Get all active simulation sessions"""
        cache = self.client.get_cache(self.SESSION_METADATA_CACHE)
        
        sessions = []
        scan = cache.scan()
        
        for key, value in scan:
            if value.get('state') == 'active':
                sessions.append(value)
        
        return sessions
    
    # Metrics Management
    
    async def update_session_metrics(self, session_id: str, metrics: Dict[str, Any]):
        """Update real-time session metrics"""
        cache = self.client.get_cache(self.METRICS_CACHE)
        
        metrics_entry = {
            'session_id': session_id,
            'timestamp': datetime.utcnow().timestamp(),
            'metrics': metrics
        }
        
        # Use time-based key for time series data
        key = f"{session_id}:{int(datetime.utcnow().timestamp())}"
        cache.put(key, metrics_entry)
    
    async def get_session_metrics_history(self, session_id: str, 
                                        duration_seconds: int = 300) -> List[Dict[str, Any]]:
        """Get metrics history for a session"""
        cache = self.client.get_cache(self.METRICS_CACHE)
        
        cutoff_time = datetime.utcnow().timestamp() - duration_seconds
        metrics_history = []
        
        scan = cache.scan()
        for key, value in scan:
            if isinstance(key, str) and key.startswith(f"{session_id}:"):
                if value['timestamp'] >= cutoff_time:
                    metrics_history.append(value)
        
        # Sort by timestamp
        metrics_history.sort(key=lambda x: x['timestamp'])
        return metrics_history
    
    # Checkpoint Management
    
    async def create_checkpoint(self, session_id: str, checkpoint_id: str, 
                              state_data: bytes, metadata: Dict[str, Any]):
        """Create a simulation checkpoint"""
        cache = self.client.get_cache(self.CHECKPOINT_CACHE)
        
        checkpoint = {
            'checkpoint_id': checkpoint_id,
            'session_id': session_id,
            'state_data': state_data,
            'metadata': metadata,
            'created_at': datetime.utcnow().isoformat(),
            'size_bytes': len(state_data)
        }
        
        cache.put(checkpoint_id, checkpoint)
    
    async def get_checkpoint(self, checkpoint_id: str) -> Optional[Dict[str, Any]]:
        """Get a checkpoint by ID"""
        cache = self.client.get_cache(self.CHECKPOINT_CACHE)
        return cache.get(checkpoint_id)
    
    async def list_checkpoints(self, session_id: str) -> List[Dict[str, Any]]:
        """List all checkpoints for a session"""
        cache = self.client.get_cache(self.CHECKPOINT_CACHE)
        
        checkpoints = []
        scan = cache.scan()
        
        for key, value in scan:
            if value.get('session_id') == session_id:
                checkpoints.append({
                    'checkpoint_id': value['checkpoint_id'],
                    'created_at': value['created_at'],
                    'size_bytes': value['size_bytes'],
                    'metadata': value['metadata']
                })
        
        # Sort by creation time
        checkpoints.sort(key=lambda x: x['created_at'], reverse=True)
        return checkpoints
    
    # Performance Optimization
    
    async def bulk_get_agent_states(self, session_id: str, agent_ids: List[str]) -> Dict[str, Any]:
        """Bulk get agent states for performance"""
        cache = self.client.get_cache(self.AGENT_STATE_CACHE)
        
        keys = [f"{session_id}:{agent_id}" for agent_id in agent_ids]
        results = cache.get_all(keys)
        
        # Transform results
        agent_states = {}
        for key, value in results.items():
            if value:
                agent_id = key.split(":", 1)[1]
                agent_states[agent_id] = value
        
        return agent_states
    
    async def clear_session_data(self, session_id: str):
        """Clear all data for a session"""
        # Clear from all caches
        for cache_name in [
            self.SIMULATION_STATE_CACHE,
            self.SESSION_METADATA_CACHE,
            self.AGENT_STATE_CACHE,
            self.METRICS_CACHE
        ]:
            cache = self.client.get_cache(cache_name)
            
            # Scan and remove matching keys
            to_remove = []
            scan = cache.scan()
            
            for key, _ in scan:
                if isinstance(key, str) and (key == session_id or key.startswith(f"{session_id}:")):
                    to_remove.append(key)
            
            if to_remove:
                cache.remove_all(to_remove) 