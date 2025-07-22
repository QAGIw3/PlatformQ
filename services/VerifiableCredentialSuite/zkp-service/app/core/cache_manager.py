"""
Apache Ignite Cache Manager for ZKP Proofs
"""

import json
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

from pyignite import AsyncClient
from pyignite.datatypes import String

from app.config import settings


class ProofCacheManager:
    """
    Manages caching of zero-knowledge proofs using Apache Ignite
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 10800,
        ttl_seconds: int = 3600,
        max_entries: int = 10000
    ):
        self.host = host
        self.port = port
        self.ttl_seconds = ttl_seconds
        self.max_entries = max_entries
        self.client: Optional[AsyncClient] = None
        self.connected = False
        
        # Cache names
        self.PROOF_CACHE = "zkp_proofs"
        self.VERIFICATION_CACHE = "zkp_verifications"
        self.PUBLIC_KEY_CACHE = "zkp_public_keys"
        self.TEMPLATE_CACHE = "zkp_templates"
        
        # Statistics
        self.cache_hits = 0
        self.cache_misses = 0
    
    async def connect(self):
        """Connect to Apache Ignite"""
        if self.connected:
            return
            
        try:
            self.client = AsyncClient()
            await self.client.connect(self.host, self.port)
            
            # Create caches with configuration
            for cache_name in [
                self.PROOF_CACHE,
                self.VERIFICATION_CACHE,
                self.PUBLIC_KEY_CACHE,
                self.TEMPLATE_CACHE
            ]:
                cache = await self.client.get_or_create_cache(cache_name)
                # Configure cache with max entries
                # This would need Ignite-specific cache configuration
                
            self.connected = True
            
        except Exception as e:
            print(f"Failed to connect to Apache Ignite: {str(e)}")
            self.connected = False
            raise
    
    async def disconnect(self):
        """Disconnect from Apache Ignite"""
        if self.client and self.connected:
            await self.client.close()
            self.connected = False
    
    async def health_check(self) -> bool:
        """Check if cache is healthy"""
        if not self.connected:
            return False
            
        try:
            # Try to access a cache
            cache = await self.client.get_cache(self.PROOF_CACHE)
            await cache.get("health_check_key")
            return True
        except Exception:
            return False
    
    # Proof caching
    async def get_proof(self, key: str) -> Optional[Dict[str, Any]]:
        """Get proof from cache"""
        if not self.connected:
            return None
            
        try:
            cache = await self.client.get_cache(self.PROOF_CACHE)
            cached_data = await cache.get(key)
            
            if cached_data:
                self.cache_hits += 1
                return json.loads(cached_data)
            else:
                self.cache_misses += 1
                return None
                
        except Exception as e:
            print(f"Cache get error for proof {key}: {str(e)}")
            self.cache_misses += 1
            return None
    
    async def set_proof(
        self,
        key: str,
        proof: Dict[str, Any],
        ttl: Optional[int] = None
    ):
        """Set proof in cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.PROOF_CACHE)
            
            # Serialize the proof
            serialized = json.dumps(proof)
            
            # Set with TTL
            ttl_seconds = ttl or self.ttl_seconds
            await cache.put(key, serialized, ttl=ttl_seconds * 1000)  # Convert to milliseconds
            
        except Exception as e:
            print(f"Cache set error for proof {key}: {str(e)}")
    
    async def delete_proof(self, key: str):
        """Delete proof from cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.PROOF_CACHE)
            await cache.remove(key)
            
        except Exception as e:
            print(f"Cache delete error for proof {key}: {str(e)}")
    
    # Verification result caching
    async def get_verification(self, key: str) -> Optional[Dict[str, Any]]:
        """Get verification result from cache"""
        if not self.connected:
            return None
            
        try:
            cache = await self.client.get_cache(self.VERIFICATION_CACHE)
            cached_data = await cache.get(key)
            
            if cached_data:
                return json.loads(cached_data)
                
            return None
            
        except Exception as e:
            print(f"Cache get error for verification {key}: {str(e)}")
            return None
    
    async def set_verification(
        self,
        key: str,
        result: Dict[str, Any],
        ttl: Optional[int] = None
    ):
        """Set verification result in cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.VERIFICATION_CACHE)
            serialized = json.dumps(result)
            
            # Use shorter TTL for verification results
            ttl_seconds = ttl or 300  # 5 minutes default
            await cache.put(key, serialized, ttl=ttl_seconds * 1000)
            
        except Exception as e:
            print(f"Cache set error for verification {key}: {str(e)}")
    
    # Public key caching
    async def get_public_key(self, issuer: str) -> Optional[Dict[str, Any]]:
        """Get public key from cache"""
        if not self.connected:
            return None
            
        try:
            cache = await self.client.get_cache(self.PUBLIC_KEY_CACHE)
            cached_data = await cache.get(issuer)
            
            if cached_data:
                return json.loads(cached_data)
                
            return None
            
        except Exception as e:
            print(f"Cache get error for public key {issuer}: {str(e)}")
            return None
    
    async def set_public_key(
        self,
        issuer: str,
        public_key: Dict[str, Any],
        ttl: Optional[int] = None
    ):
        """Set public key in cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.PUBLIC_KEY_CACHE)
            serialized = json.dumps(public_key)
            
            # Use longer TTL for public keys
            ttl_seconds = ttl or 86400  # 24 hours default
            await cache.put(issuer, serialized, ttl=ttl_seconds * 1000)
            
        except Exception as e:
            print(f"Cache set error for public key {issuer}: {str(e)}")
    
    # Template caching
    async def get_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        """Get proof template from cache"""
        if not self.connected:
            return None
            
        try:
            cache = await self.client.get_cache(self.TEMPLATE_CACHE)
            cached_data = await cache.get(template_id)
            
            if cached_data:
                return json.loads(cached_data)
                
            return None
            
        except Exception as e:
            print(f"Cache get error for template {template_id}: {str(e)}")
            return None
    
    async def set_template(
        self,
        template_id: str,
        template: Dict[str, Any],
        ttl: Optional[int] = None
    ):
        """Set proof template in cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.TEMPLATE_CACHE)
            serialized = json.dumps(template)
            
            # Templates don't change often, use long TTL
            ttl_seconds = ttl or 604800  # 7 days default
            await cache.put(template_id, serialized, ttl=ttl_seconds * 1000)
            
        except Exception as e:
            print(f"Cache set error for template {template_id}: {str(e)}")
    
    # Batch operations
    async def get_proofs_batch(self, keys: List[str]) -> Dict[str, Optional[Dict[str, Any]]]:
        """Get multiple proofs from cache"""
        if not self.connected:
            return {key: None for key in keys}
            
        try:
            cache = await self.client.get_cache(self.PROOF_CACHE)
            results = await cache.get_all(keys)
            
            # Deserialize results
            deserialized = {}
            for key, data in results.items():
                if data:
                    deserialized[key] = json.loads(data)
                    self.cache_hits += 1
                else:
                    deserialized[key] = None
                    self.cache_misses += 1
                    
            # Add missing keys
            for key in keys:
                if key not in deserialized:
                    deserialized[key] = None
                    self.cache_misses += 1
                    
            return deserialized
            
        except Exception as e:
            print(f"Cache batch get error: {str(e)}")
            self.cache_misses += len(keys)
            return {key: None for key in keys}
    
    async def set_proofs_batch(
        self,
        proofs: Dict[str, Dict[str, Any]],
        ttl: Optional[int] = None
    ):
        """Set multiple proofs in cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.PROOF_CACHE)
            
            # Serialize all proofs
            serialized = {
                key: json.dumps(proof)
                for key, proof in proofs.items()
            }
            
            # Set with TTL
            ttl_seconds = ttl or self.ttl_seconds
            
            # Batch put operations
            for key, data in serialized.items():
                await cache.put(key, data, ttl=ttl_seconds * 1000)
                
        except Exception as e:
            print(f"Cache batch set error: {str(e)}")
    
    # Cache management
    async def clear_expired(self):
        """Clear expired entries (handled by Ignite TTL)"""
        # Apache Ignite handles TTL expiration automatically
        pass
    
    async def get_cache_size(self) -> int:
        """Get number of cached proofs"""
        if not self.connected:
            return 0
            
        try:
            cache = await self.client.get_cache(self.PROOF_CACHE)
            # This would need proper Ignite cache size API
            return self.max_entries  # Placeholder
            
        except Exception:
            return 0
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get cache statistics"""
        total_requests = self.cache_hits + self.cache_misses
        hit_rate = self.cache_hits / total_requests if total_requests > 0 else 0
        
        stats = {
            "connected": self.connected,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "hit_rate": hit_rate,
            "total_requests": total_requests,
            "cache_size": await self.get_cache_size(),
            "max_entries": self.max_entries,
            "ttl_seconds": self.ttl_seconds
        }
        
        return stats
    
    async def reset_statistics(self):
        """Reset cache statistics"""
        self.cache_hits = 0
        self.cache_misses = 0
    
    # Pre-warming
    async def warm_cache(self, common_proofs: List[Dict[str, Any]]):
        """Pre-warm cache with common proofs"""
        if not self.connected:
            return
            
        for proof_spec in common_proofs:
            key = proof_spec.get("key")
            proof = proof_spec.get("proof")
            
            if key and proof:
                await self.set_proof(key, proof)
        
        print(f"Pre-warmed cache with {len(common_proofs)} proofs") 