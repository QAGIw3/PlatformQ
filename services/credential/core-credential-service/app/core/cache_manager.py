"""
Cache Manager for credential operations using Apache Ignite
"""

import json
import logging
from typing import Any, Dict, Optional, List
from datetime import datetime, timedelta
import hashlib

from pyignite import Client as IgniteClient
from pyignite.cache import Cache

logger = logging.getLogger(__name__)


class CacheManager:
    """Manages distributed caching using Apache Ignite"""
    
    def __init__(self, ignite_client: IgniteClient, ttl_seconds: int = 3600):
        self.ignite_client = ignite_client
        self.ttl_seconds = ttl_seconds
        self._caches: Dict[str, Cache] = {}
        
    async def initialize(self):
        """Initialize cache instances"""
        try:
            # Create caches with appropriate configurations
            self._caches['credentials'] = self.ignite_client.get_or_create_cache({
                'name': 'credentials',
                'cache_mode': 'PARTITIONED',
                'backups': 1,
                'write_synchronization_mode': 'PRIMARY_SYNC',
                'expiry_policy': {
                    'access': self.ttl_seconds * 1000,  # milliseconds
                    'create': self.ttl_seconds * 1000,
                    'update': self.ttl_seconds * 1000
                }
            })
            
            self._caches['did_documents'] = self.ignite_client.get_or_create_cache({
                'name': 'did_documents',
                'cache_mode': 'REPLICATED',
                'write_synchronization_mode': 'FULL_SYNC'
            })
            
            self._caches['issuer_keys'] = self.ignite_client.get_or_create_cache({
                'name': 'issuer_keys',
                'cache_mode': 'REPLICATED',
                'write_synchronization_mode': 'FULL_SYNC'
            })
            
            self._caches['revocation_lists'] = self.ignite_client.get_or_create_cache({
                'name': 'revocation_lists',
                'cache_mode': 'PARTITIONED',
                'backups': 2,
                'write_synchronization_mode': 'FULL_SYNC'
            })
            
            logger.info("Initialized Ignite caches")
            
        except Exception as e:
            logger.error(f"Failed to initialize caches: {e}")
            raise
    
    async def get_credential(self, credential_id: str) -> Optional[Dict[str, Any]]:
        """Get credential from cache"""
        try:
            cache = self._caches.get('credentials')
            if not cache:
                return None
                
            cached_data = cache.get(credential_id)
            if cached_data:
                return json.loads(cached_data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting credential from cache: {e}")
            return None
    
    async def set_credential(self, credential_id: str, credential: Dict[str, Any], 
                           ttl_override: Optional[int] = None):
        """Store credential in cache"""
        try:
            cache = self._caches.get('credentials')
            if not cache:
                return
                
            ttl = ttl_override or self.ttl_seconds
            cache.put(credential_id, json.dumps(credential), ttl_ms=ttl * 1000)
            
        except Exception as e:
            logger.error(f"Error setting credential in cache: {e}")
    
    async def invalidate_credential(self, credential_id: str):
        """Remove credential from cache"""
        try:
            cache = self._caches.get('credentials')
            if cache:
                cache.remove(credential_id)
                
        except Exception as e:
            logger.error(f"Error invalidating credential: {e}")
    
    async def get_did_document(self, did: str) -> Optional[Dict[str, Any]]:
        """Get DID document from cache"""
        try:
            cache = self._caches.get('did_documents')
            if not cache:
                return None
                
            cached_data = cache.get(did)
            if cached_data:
                return json.loads(cached_data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting DID document from cache: {e}")
            return None
    
    async def set_did_document(self, did: str, document: Dict[str, Any]):
        """Store DID document in cache"""
        try:
            cache = self._caches.get('did_documents')
            if cache:
                cache.put(did, json.dumps(document))
                
        except Exception as e:
            logger.error(f"Error setting DID document in cache: {e}")
    
    async def get_issuer_key(self, issuer_did: str) -> Optional[Dict[str, Any]]:
        """Get cached issuer public key info"""
        try:
            cache = self._caches.get('issuer_keys')
            if not cache:
                return None
                
            cached_data = cache.get(issuer_did)
            if cached_data:
                return json.loads(cached_data)
            return None
            
        except Exception as e:
            logger.error(f"Error getting issuer key from cache: {e}")
            return None
    
    async def set_issuer_key(self, issuer_did: str, key_info: Dict[str, Any]):
        """Cache issuer public key info"""
        try:
            cache = self._caches.get('issuer_keys')
            if cache:
                cache.put(issuer_did, json.dumps(key_info))
                
        except Exception as e:
            logger.error(f"Error setting issuer key in cache: {e}")
    
    async def check_revocation_status(self, credential_id: str) -> Optional[bool]:
        """Check if credential is revoked (cached)"""
        try:
            cache = self._caches.get('revocation_lists')
            if not cache:
                return None
                
            # Use a hash of credential_id for privacy
            key = hashlib.sha256(credential_id.encode()).hexdigest()
            return cache.get(key)
            
        except Exception as e:
            logger.error(f"Error checking revocation status: {e}")
            return None
    
    async def set_revocation_status(self, credential_id: str, is_revoked: bool):
        """Cache revocation status"""
        try:
            cache = self._caches.get('revocation_lists')
            if cache:
                key = hashlib.sha256(credential_id.encode()).hexdigest()
                cache.put(key, is_revoked)
                
        except Exception as e:
            logger.error(f"Error setting revocation status: {e}")
    
    async def batch_get_credentials(self, credential_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Get multiple credentials from cache"""
        try:
            cache = self._caches.get('credentials')
            if not cache:
                return {}
                
            # Use Ignite's getAll for efficiency
            results = cache.get_all(credential_ids)
            
            return {
                cred_id: json.loads(data) 
                for cred_id, data in results.items() 
                if data
            }
            
        except Exception as e:
            logger.error(f"Error batch getting credentials: {e}")
            return {}
    
    async def clear_tenant_cache(self, tenant_id: str):
        """Clear all cached data for a tenant"""
        try:
            # This would use Ignite SQL to find and remove tenant data
            # For now, log the intent
            logger.info(f"Would clear cache for tenant: {tenant_id}")
            
        except Exception as e:
            logger.error(f"Error clearing tenant cache: {e}")
    
    async def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            stats = {}
            for name, cache in self._caches.items():
                metrics = cache.get_metrics()
                stats[name] = {
                    'size': metrics.get('CacheSize', 0),
                    'hits': metrics.get('CacheHits', 0),
                    'misses': metrics.get('CacheMisses', 0),
                    'hit_rate': metrics.get('CacheHitPercentage', 0)
                }
            return stats
            
        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {}
    
    async def health_check(self) -> bool:
        """Check if cache is healthy"""
        try:
            # Try to access a cache
            cache = self._caches.get('credentials')
            if cache:
                # Perform a simple operation
                test_key = "__health_check__"
                cache.put(test_key, "OK", ttl_ms=1000)
                result = cache.get(test_key)
                cache.remove(test_key)
                return result == "OK"
            return False
            
        except Exception as e:
            logger.error(f"Cache health check failed: {e}")
            return False 