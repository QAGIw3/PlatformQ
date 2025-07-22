"""
Apache Ignite Cache Manager for DID Service
"""

import json
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

from pyignite import AsyncClient
from pyignite.datatypes import String

from app.config import settings


class DIDCacheManager:
    """
    Manages caching of DID documents and related data using Apache Ignite
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 10800,
        ttl_seconds: int = 3600
    ):
        self.host = host
        self.port = port
        self.ttl_seconds = ttl_seconds
        self.client: Optional[AsyncClient] = None
        self.connected = False
        
        # Cache names
        self.DID_DOCUMENT_CACHE = "did_documents"
        self.DID_METHOD_CACHE = "did_methods"
        self.DID_KEY_CACHE = "did_keys"
        self.DID_METADATA_CACHE = "did_metadata"
    
    async def connect(self):
        """Connect to Apache Ignite"""
        if self.connected:
            return
            
        try:
            self.client = AsyncClient()
            await self.client.connect(self.host, self.port)
            
            # Create caches if they don't exist
            for cache_name in [
                self.DID_DOCUMENT_CACHE,
                self.DID_METHOD_CACHE,
                self.DID_KEY_CACHE,
                self.DID_METADATA_CACHE
            ]:
                cache = await self.client.get_or_create_cache(cache_name)
                
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
            cache = await self.client.get_cache(self.DID_DOCUMENT_CACHE)
            # Perform a simple operation
            await cache.get("health_check_key")
            return True
        except Exception:
            return False
    
    # DID Document caching
    async def get_did_document(self, did: str) -> Optional[Dict[str, Any]]:
        """Get DID document from cache"""
        if not self.connected:
            return None
            
        try:
            cache = await self.client.get_cache(self.DID_DOCUMENT_CACHE)
            cached_data = await cache.get(did)
            
            if cached_data:
                return json.loads(cached_data)
                
            return None
            
        except Exception as e:
            print(f"Cache get error for DID {did}: {str(e)}")
            return None
    
    async def set_did_document(
        self,
        did: str,
        did_document: Dict[str, Any],
        ttl: Optional[int] = None
    ):
        """Set DID document in cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.DID_DOCUMENT_CACHE)
            
            # Serialize the document
            serialized = json.dumps(did_document)
            
            # Set with TTL
            ttl_seconds = ttl or self.ttl_seconds
            await cache.put(did, serialized, ttl=ttl_seconds * 1000)  # Convert to milliseconds
            
        except Exception as e:
            print(f"Cache set error for DID {did}: {str(e)}")
    
    async def delete_did_document(self, did: str):
        """Delete DID document from cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.DID_DOCUMENT_CACHE)
            await cache.remove(did)
            
        except Exception as e:
            print(f"Cache delete error for DID {did}: {str(e)}")
    
    # DID method resolver caching
    async def get_method_resolver(self, method: str) -> Optional[Dict[str, Any]]:
        """Get method resolver configuration from cache"""
        if not self.connected:
            return None
            
        try:
            cache = await self.client.get_cache(self.DID_METHOD_CACHE)
            cached_data = await cache.get(method)
            
            if cached_data:
                return json.loads(cached_data)
                
            return None
            
        except Exception as e:
            print(f"Cache get error for method {method}: {str(e)}")
            return None
    
    async def set_method_resolver(
        self,
        method: str,
        resolver_config: Dict[str, Any]
    ):
        """Set method resolver configuration in cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.DID_METHOD_CACHE)
            serialized = json.dumps(resolver_config)
            
            # Method configs don't change often, use longer TTL
            await cache.put(method, serialized, ttl=86400 * 1000)  # 24 hours
            
        except Exception as e:
            print(f"Cache set error for method {method}: {str(e)}")
    
    # Key caching for performance
    async def get_public_key(self, key_id: str) -> Optional[Dict[str, Any]]:
        """Get public key from cache"""
        if not self.connected:
            return None
            
        try:
            cache = await self.client.get_cache(self.DID_KEY_CACHE)
            cached_data = await cache.get(key_id)
            
            if cached_data:
                return json.loads(cached_data)
                
            return None
            
        except Exception as e:
            print(f"Cache get error for key {key_id}: {str(e)}")
            return None
    
    async def set_public_key(
        self,
        key_id: str,
        key_data: Dict[str, Any],
        ttl: Optional[int] = None
    ):
        """Set public key in cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.DID_KEY_CACHE)
            serialized = json.dumps(key_data)
            
            ttl_seconds = ttl or self.ttl_seconds
            await cache.put(key_id, serialized, ttl=ttl_seconds * 1000)
            
        except Exception as e:
            print(f"Cache set error for key {key_id}: {str(e)}")
    
    async def delete_public_key(self, key_id: str):
        """Delete public key from cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.DID_KEY_CACHE)
            await cache.remove(key_id)
            
        except Exception as e:
            print(f"Cache delete error for key {key_id}: {str(e)}")
    
    # Batch operations
    async def get_did_documents_batch(
        self,
        dids: List[str]
    ) -> Dict[str, Optional[Dict[str, Any]]]:
        """Get multiple DID documents from cache"""
        if not self.connected:
            return {did: None for did in dids}
            
        try:
            cache = await self.client.get_cache(self.DID_DOCUMENT_CACHE)
            results = await cache.get_all(dids)
            
            # Deserialize results
            deserialized = {}
            for did, data in results.items():
                if data:
                    deserialized[did] = json.loads(data)
                else:
                    deserialized[did] = None
                    
            # Add missing DIDs
            for did in dids:
                if did not in deserialized:
                    deserialized[did] = None
                    
            return deserialized
            
        except Exception as e:
            print(f"Cache batch get error: {str(e)}")
            return {did: None for did in dids}
    
    async def set_did_documents_batch(
        self,
        documents: Dict[str, Dict[str, Any]],
        ttl: Optional[int] = None
    ):
        """Set multiple DID documents in cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.DID_DOCUMENT_CACHE)
            
            # Serialize all documents
            serialized = {
                did: json.dumps(doc)
                for did, doc in documents.items()
            }
            
            # Set with TTL
            ttl_seconds = ttl or self.ttl_seconds
            
            # Ignite doesn't support batch TTL directly, so we'll use individual puts
            # This could be optimized with a custom cache configuration
            for did, data in serialized.items():
                await cache.put(did, data, ttl=ttl_seconds * 1000)
                
        except Exception as e:
            print(f"Cache batch set error: {str(e)}")
    
    # Metadata caching
    async def get_did_metadata(self, did: str) -> Optional[Dict[str, Any]]:
        """Get DID metadata from cache"""
        if not self.connected:
            return None
            
        try:
            cache = await self.client.get_cache(self.DID_METADATA_CACHE)
            cached_data = await cache.get(did)
            
            if cached_data:
                return json.loads(cached_data)
                
            return None
            
        except Exception as e:
            print(f"Cache get error for metadata {did}: {str(e)}")
            return None
    
    async def set_did_metadata(
        self,
        did: str,
        metadata: Dict[str, Any],
        ttl: Optional[int] = None
    ):
        """Set DID metadata in cache"""
        if not self.connected:
            return
            
        try:
            cache = await self.client.get_cache(self.DID_METADATA_CACHE)
            serialized = json.dumps(metadata)
            
            ttl_seconds = ttl or self.ttl_seconds
            await cache.put(did, serialized, ttl=ttl_seconds * 1000)
            
        except Exception as e:
            print(f"Cache set error for metadata {did}: {str(e)}")
    
    # Cache invalidation
    async def invalidate_did_cache(self, did: str):
        """Invalidate all caches for a DID"""
        if not self.connected:
            return
            
        try:
            # Remove from all caches
            await self.delete_did_document(did)
            await self.delete_did_metadata(did)
            
            # Also invalidate any keys associated with this DID
            # This would require tracking key IDs per DID
            
        except Exception as e:
            print(f"Cache invalidation error for DID {did}: {str(e)}")
    
    async def clear_all_caches(self):
        """Clear all caches (use with caution)"""
        if not self.connected:
            return
            
        try:
            for cache_name in [
                self.DID_DOCUMENT_CACHE,
                self.DID_METHOD_CACHE,
                self.DID_KEY_CACHE,
                self.DID_METADATA_CACHE
            ]:
                cache = await self.client.get_cache(cache_name)
                await cache.clear()
                
        except Exception as e:
            print(f"Cache clear error: {str(e)}") 