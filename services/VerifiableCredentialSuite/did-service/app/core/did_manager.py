"""
DID Manager - Core business logic for DID operations
"""

import json
from typing import Optional, Dict, Any, List, Type
from datetime import datetime, timezone
import httpx

from app.config import settings
from app.storage.did_store import DIDStore
from app.core.cache_manager import DIDCacheManager
from app.resolvers.base import DIDResolver
from app.resolvers.did_key import DIDKeyResolver
from platformq_consul import ConsulClient


class DIDManager:
    """
    Manages DID operations including creation, resolution, updates, and deletion
    """
    
    def __init__(
        self,
        did_store: DIDStore,
        cache_manager: Optional[DIDCacheManager],
        http_client: httpx.AsyncClient,
        consul_client: Optional[ConsulClient],
        vault_client: Optional[Any]
    ):
        self.did_store = did_store
        self.cache_manager = cache_manager
        self.http_client = http_client
        self.consul_client = consul_client
        self.vault_client = vault_client
        
        # DID method resolvers
        self.resolvers: Dict[str, DIDResolver] = {}
        
    async def initialize_resolvers(self):
        """Initialize DID method resolvers"""
        
        # Initialize did:key resolver
        if "key" in settings.supported_did_methods:
            self.resolvers["key"] = DIDKeyResolver(
                http_client=self.http_client,
                key_management_url=settings.key_management_url
            )
            print("✓ Initialized did:key resolver")
        
        # Initialize did:web resolver
        if "web" in settings.supported_did_methods:
            try:
                from app.resolvers.did_web import DIDWebResolver
                self.resolvers["web"] = DIDWebResolver(
                    http_client=self.http_client,
                    default_domain=settings.did_web_domain,
                    path_prefix=settings.did_web_path_prefix
                )
                print("✓ Initialized did:web resolver")
            except ImportError:
                print("⚠ did:web resolver not implemented yet")
        
        # Initialize did:platformq resolver
        if "platformq" in settings.supported_did_methods:
            try:
                from app.resolvers.did_platformq import DIDPlatformQResolver
                self.resolvers["platformq"] = DIDPlatformQResolver(
                    http_client=self.http_client,
                    prefix=settings.did_platformq_prefix,
                    network=settings.did_platformq_network,
                    blockchain_url=settings.blockchain_connector_url
                )
                print("✓ Initialized did:platformq resolver")
            except ImportError:
                print("⚠ did:platformq resolver not implemented yet")
        
        # Initialize did:ethr resolver
        if "ethr" in settings.supported_did_methods and settings.enable_did_ethr:
            try:
                from app.resolvers.did_ethr import DIDEthrResolver
                self.resolvers["ethr"] = DIDEthrResolver(
                    http_client=self.http_client,
                    blockchain_url=settings.blockchain_connector_url
                )
                print("✓ Initialized did:ethr resolver")
            except ImportError:
                print("⚠ did:ethr resolver not implemented yet")
    
    def _extract_method(self, did: str) -> str:
        """Extract DID method from DID string"""
        if not did.startswith("did:"):
            raise ValueError(f"Invalid DID format: {did}")
        
        parts = did.split(":")
        if len(parts) < 3:
            raise ValueError(f"Invalid DID format: {did}")
            
        return parts[1]
    
    async def create_did(
        self,
        method: str,
        options: Optional[Dict[str, Any]] = None,
        key_type: Optional[str] = None,
        services: Optional[List[Dict[str, Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a new DID"""
        
        # Validate method
        if method not in self.resolvers:
            raise ValueError(f"Unsupported DID method: {method}")
        
        # Get resolver
        resolver = self.resolvers[method]
        
        # Create DID
        result = await resolver.create(
            options=options or {},
            key_type=key_type,
            services=services
        )
        
        # Extract DID and document
        did = result["did"]
        did_document = result["did_document"]
        
        # Store in database
        await self.did_store.store_did_document(
            did=did,
            did_document=did_document,
            metadata=metadata
        )
        
        # Cache the document
        if self.cache_manager:
            await self.cache_manager.set_did_document(did, did_document)
            if metadata:
                await self.cache_manager.set_did_metadata(did, metadata)
        
        # Return complete result
        return {
            "did": did,
            "did_document": did_document,
            "metadata": metadata,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
    
    async def resolve_did(self, did: str) -> Optional[Dict[str, Any]]:
        """Resolve a DID to its DID document"""
        
        # Check cache first
        if self.cache_manager:
            cached_doc = await self.cache_manager.get_did_document(did)
            if cached_doc:
                # Get metadata from cache
                metadata = await self.cache_manager.get_did_metadata(did)
                return {
                    "did_document": cached_doc,
                    "metadata": metadata
                }
        
        # Check local storage
        stored = await self.did_store.get_did_document(did)
        if stored:
            # Update cache
            if self.cache_manager:
                await self.cache_manager.set_did_document(
                    did,
                    stored["did_document"]
                )
                if stored.get("metadata"):
                    await self.cache_manager.set_did_metadata(
                        did,
                        stored["metadata"]
                    )
            
            return {
                "did_document": stored["did_document"],
                "metadata": stored.get("metadata"),
                "created_at": stored.get("created_at"),
                "updated_at": stored.get("updated_at")
            }
        
        # If not found locally, try to resolve using the appropriate resolver
        method = self._extract_method(did)
        
        if method not in self.resolvers:
            return None
        
        resolver = self.resolvers[method]
        
        # Resolve using the method-specific resolver
        resolved = await resolver.resolve(did)
        
        if resolved:
            # Store locally for future reference
            await self.did_store.store_did_document(
                did=did,
                did_document=resolved["did_document"],
                metadata={"resolved_from": method}
            )
            
            # Cache it
            if self.cache_manager:
                await self.cache_manager.set_did_document(
                    did,
                    resolved["did_document"]
                )
            
            return {
                "did_document": resolved["did_document"],
                "metadata": {"resolved_from": method},
                "created_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc)
            }
        
        return None
    
    async def update_did(
        self,
        did: str,
        add_keys: Optional[List[Dict[str, Any]]] = None,
        remove_keys: Optional[List[str]] = None,
        add_services: Optional[List[Dict[str, Any]]] = None,
        remove_services: Optional[List[str]] = None,
        update_metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Update a DID document"""
        
        # Get current document
        current = await self.resolve_did(did)
        if not current:
            raise ValueError(f"DID not found: {did}")
        
        did_document = current["did_document"].copy()
        
        # Update verification methods (keys)
        if add_keys or remove_keys:
            verification_methods = did_document.get("verificationMethod", [])
            
            # Remove keys
            if remove_keys:
                verification_methods = [
                    vm for vm in verification_methods
                    if vm.get("id") not in remove_keys
                ]
            
            # Add keys
            if add_keys:
                # Get method resolver
                method = self._extract_method(did)
                if method in self.resolvers:
                    resolver = self.resolvers[method]
                    
                    # Add each key
                    for key_spec in add_keys:
                        new_key = await resolver.add_verification_method(
                            did=did,
                            key_type=key_spec.get("type", "Ed25519VerificationKey2020"),
                            purpose=key_spec.get("purpose", ["authentication"])
                        )
                        verification_methods.append(new_key)
            
            did_document["verificationMethod"] = verification_methods
            
            # Update authentication, assertionMethod, etc.
            for rel_type in ["authentication", "assertionMethod", "keyAgreement", "capabilityInvocation", "capabilityDelegation"]:
                if rel_type in did_document:
                    # Remove references to deleted keys
                    if remove_keys:
                        did_document[rel_type] = [
                            ref for ref in did_document[rel_type]
                            if (isinstance(ref, str) and ref not in remove_keys) or
                               (isinstance(ref, dict) and ref.get("id") not in remove_keys)
                        ]
        
        # Update services
        if add_services or remove_services:
            services = did_document.get("service", [])
            
            # Remove services
            if remove_services:
                services = [
                    svc for svc in services
                    if svc.get("id") not in remove_services
                ]
            
            # Add services
            if add_services:
                services.extend(add_services)
            
            did_document["service"] = services
        
        # Update document
        did_document["updated"] = datetime.now(timezone.utc).isoformat()
        
        # Store updated document
        await self.did_store.update_did_document(
            did=did,
            did_document=did_document,
            metadata=update_metadata
        )
        
        # Invalidate cache
        if self.cache_manager:
            await self.cache_manager.invalidate_did_cache(did)
        
        return {
            "did": did,
            "did_document": did_document,
            "metadata": current.get("metadata", {}),
            "updated_at": datetime.now(timezone.utc)
        }
    
    async def deactivate_did(self, did: str):
        """Deactivate a DID"""
        
        # Get current document
        current = await self.resolve_did(did)
        if not current:
            raise ValueError(f"DID not found: {did}")
        
        # Mark as deactivated
        await self.did_store.deactivate_did(did)
        
        # Invalidate cache
        if self.cache_manager:
            await self.cache_manager.invalidate_did_cache(did)
    
    async def list_dids(
        self,
        method: Optional[str] = None,
        controller: Optional[str] = None,
        active_only: bool = True,
        page: int = 1,
        page_size: int = 20
    ) -> Dict[str, Any]:
        """List DIDs with filtering and pagination"""
        
        # Get from storage
        result = await self.did_store.list_dids(
            method=method,
            controller=controller,
            active_only=active_only,
            page=page,
            page_size=page_size
        )
        
        # Enhance with cache if available
        if self.cache_manager and result["dids"]:
            # Try to get documents from cache
            dids = [item["did"] for item in result["dids"]]
            cached_docs = await self.cache_manager.get_did_documents_batch(dids)
            
            # Update results with cached documents
            for item in result["dids"]:
                if item["did"] in cached_docs and cached_docs[item["did"]]:
                    item["did_document"] = cached_docs[item["did"]]
        
        return result
    
    async def get_supported_methods(self) -> List[Dict[str, Any]]:
        """Get information about supported DID methods"""
        
        methods = []
        
        for method_name, resolver in self.resolvers.items():
            method_info = {
                "method": method_name,
                "prefix": f"did:{method_name}",
                "description": resolver.__class__.__doc__ or f"DID {method_name} resolver",
                "features": {
                    "create": hasattr(resolver, "create"),
                    "resolve": hasattr(resolver, "resolve"),
                    "update": hasattr(resolver, "update"),
                    "deactivate": hasattr(resolver, "deactivate")
                }
            }
            
            # Add method-specific configuration
            if method_name == "key":
                method_info["supported_key_types"] = settings.allowed_key_types
            elif method_name == "web":
                method_info["domain"] = settings.did_web_domain
            elif method_name == "platformq":
                method_info["network"] = settings.did_platformq_network
            elif method_name == "ethr":
                method_info["enabled"] = settings.enable_did_ethr
            
            methods.append(method_info)
        
        return methods
    
    async def verify_signature(
        self,
        did: str,
        message: str,
        signature: str,
        key_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Verify a signature using a DID"""
        
        # Resolve DID document
        resolved = await self.resolve_did(did)
        if not resolved:
            raise ValueError(f"DID not found: {did}")
        
        did_document = resolved["did_document"]
        
        # Get verification methods
        verification_methods = did_document.get("verificationMethod", [])
        
        if not verification_methods:
            raise ValueError(f"No verification methods found for DID: {did}")
        
        # If key_id specified, find that specific key
        if key_id:
            key = next(
                (vm for vm in verification_methods if vm.get("id") == key_id),
                None
            )
            if not key:
                raise ValueError(f"Key not found: {key_id}")
            verification_methods = [key]
        
        # Try to verify with each key
        for vm in verification_methods:
            try:
                # Get the public key
                public_key = vm.get("publicKeyJwk") or vm.get("publicKeyBase58")
                if not public_key:
                    continue
                
                # Call key management service to verify
                response = await self.http_client.post(
                    f"{settings.key_management_url}/verify",
                    json={
                        "message": message,
                        "signature": signature,
                        "public_key": public_key,
                        "key_type": vm.get("type"),
                        "algorithm": vm.get("algorithm")
                    }
                )
                
                if response.status_code == 200:
                    result = response.json()
                    if result.get("valid"):
                        return {
                            "valid": True,
                            "key_id": vm.get("id"),
                            "algorithm": vm.get("type")
                        }
                        
            except Exception as e:
                print(f"Verification failed with key {vm.get('id')}: {str(e)}")
                continue
        
        return {
            "valid": False,
            "error": "Signature verification failed with all available keys"
        }
    
    async def rotate_keys(
        self,
        did: str,
        key_ids: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Rotate cryptographic keys for a DID"""
        
        # Get current document
        current = await self.resolve_did(did)
        if not current:
            raise ValueError(f"DID not found: {did}")
        
        did_document = current["did_document"]
        verification_methods = did_document.get("verificationMethod", [])
        
        # Determine which keys to rotate
        if key_ids:
            keys_to_rotate = [
                vm for vm in verification_methods
                if vm.get("id") in key_ids
            ]
        else:
            # Rotate all keys
            keys_to_rotate = verification_methods
        
        if not keys_to_rotate:
            raise ValueError("No keys found to rotate")
        
        # Get method resolver
        method = self._extract_method(did)
        if method not in self.resolvers:
            raise ValueError(f"Cannot rotate keys for method: {method}")
        
        resolver = self.resolvers[method]
        
        rotated_keys = []
        new_keys = []
        
        # Rotate each key
        for old_key in keys_to_rotate:
            # Create new key
            new_key = await resolver.add_verification_method(
                did=did,
                key_type=old_key.get("type", "Ed25519VerificationKey2020"),
                purpose=["authentication", "assertionMethod"]
            )
            
            new_keys.append(new_key)
            rotated_keys.append(old_key["id"])
        
        # Update the document
        # Remove old keys and add new ones
        updated_vms = [
            vm for vm in verification_methods
            if vm.get("id") not in rotated_keys
        ]
        updated_vms.extend(new_keys)
        
        # Update document
        await self.update_did(
            did=did,
            remove_keys=rotated_keys,
            add_keys=new_keys,
            update_metadata={
                "keys_rotated_at": datetime.now(timezone.utc).isoformat(),
                "rotated_keys": rotated_keys
            }
        )
        
        return {
            "rotated_keys": rotated_keys,
            "new_keys": [key["id"] for key in new_keys]
        } 