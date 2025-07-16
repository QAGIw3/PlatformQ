from typing import Dict, Any, Optional
import httpx
import asyncio
import logging
from cachetools import TTLCache

from .did_manager import DIDDocument
from .did_methods import DIDMethodWeb, DIDMethodKey, DIDMethodEthr

logger = logging.getLogger(__name__)

class DIDResolver:
    """Universal DID Resolver supporting multiple DID methods"""
    
    def __init__(self, cache_ttl: int = 3600):
        self.methods = {}
        self.cache = TTLCache(maxsize=1000, ttl=cache_ttl)
        self._register_default_methods()
        
    def _register_default_methods(self):
        """Register default DID method resolvers"""
        self.register_method("web", DIDMethodWeb())
        self.register_method("key", DIDMethodKey())
        self.register_method("ethr", DIDMethodEthr())
    
    def register_method(self, method_name: str, resolver):
        """Register a DID method resolver"""
        self.methods[method_name] = resolver
        logger.info(f"Registered DID resolver for method: {method_name}")
    
    async def resolve(self, did: str, options: Optional[Dict[str, Any]] = None) -> Optional[DIDDocument]:
        """Resolve a DID to its document"""
        # Check cache first
        if did in self.cache:
            logger.debug(f"Returning cached DID document for: {did}")
            return self.cache[did]
        
        # Parse DID
        parts = did.split(":")
        if len(parts) < 3 or parts[0] != "did":
            logger.error(f"Invalid DID format: {did}")
            return None
        
        method = parts[1]
        
        # Check if we have a resolver for this method
        if method not in self.methods:
            logger.error(f"No resolver for DID method: {method}")
            return None
        
        # Resolve using the appropriate method
        try:
            resolver = self.methods[method]
            did_doc = await self._resolve_with_method(resolver, did, options)
            
            if did_doc:
                # Cache the result
                self.cache[did] = did_doc
                logger.info(f"Successfully resolved DID: {did}")
            else:
                logger.warning(f"Failed to resolve DID: {did}")
                
            return did_doc
            
        except Exception as e:
            logger.error(f"Error resolving DID {did}: {e}")
            return None
    
    async def _resolve_with_method(self, resolver, did: str, 
                                 options: Optional[Dict[str, Any]]) -> Optional[DIDDocument]:
        """Resolve using a specific method resolver"""
        # Handle async vs sync resolvers
        if asyncio.iscoroutinefunction(resolver.resolve_did):
            return await resolver.resolve_did(did)
        else:
            return resolver.resolve_did(did)
    
    async def resolve_from_universal_resolver(self, did: str, 
                                            resolver_url: str = "https://dev.uniresolver.io/1.0/identifiers/") -> Optional[Dict[str, Any]]:
        """Resolve a DID using the Universal Resolver service"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{resolver_url}{did}")
                
                if response.status_code == 200:
                    data = response.json()
                    if "didDocument" in data:
                        # Convert to our DIDDocument format
                        did_doc = DIDDocument(did)
                        did_doc.document = data["didDocument"]
                        return did_doc
                        
            return None
            
        except Exception as e:
            logger.error(f"Error using universal resolver: {e}")
            return None
    
    def clear_cache(self, did: Optional[str] = None):
        """Clear the cache for a specific DID or all DIDs"""
        if did:
            self.cache.pop(did, None)
            logger.info(f"Cleared cache for DID: {did}")
        else:
            self.cache.clear()
            logger.info("Cleared all DID cache")
    
    async def batch_resolve(self, dids: list[str]) -> Dict[str, Optional[DIDDocument]]:
        """Resolve multiple DIDs in parallel"""
        tasks = [self.resolve(did) for did in dids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        resolved = {}
        for did, result in zip(dids, results):
            if isinstance(result, Exception):
                logger.error(f"Error resolving {did}: {result}")
                resolved[did] = None
            else:
                resolved[did] = result
                
        return resolved 