"""
did:web Method Resolver

The did:web method uses HTTPS web infrastructure for DID document hosting.
DID documents are hosted at well-known URLs derived from the DID.
"""

import json
from typing import Dict, Any, Optional, List
from urllib.parse import quote, unquote
import httpx

from app.resolvers.base import DIDResolver


class DIDWebResolver(DIDResolver):
    """
    Resolver for did:web method
    
    Example DIDs:
    - did:web:example.com
    - did:web:example.com:user:alice
    - did:web:example.com%3A8443
    """
    
    def __init__(
        self,
        http_client: httpx.AsyncClient,
        default_domain: str = "example.com",
        path_prefix: str = ".well-known",
        use_https: bool = True
    ):
        self.http_client = http_client
        self.default_domain = default_domain
        self.path_prefix = path_prefix
        self.use_https = use_https
    
    def _did_to_url(self, did: str) -> str:
        """Convert did:web identifier to URL"""
        # Remove did:web: prefix
        if not did.startswith("did:web:"):
            raise ValueError(f"Invalid did:web: {did}")
        
        identifier = did[8:]  # Remove "did:web:"
        
        # Decode any URL encoding
        identifier = unquote(identifier)
        
        # Split by colons for path components
        parts = identifier.split(":")
        domain = parts[0]
        path_parts = parts[1:] if len(parts) > 1 else []
        
        # Build URL
        protocol = "https" if self.use_https else "http"
        
        if path_parts:
            # Custom path specified
            path = "/".join(path_parts) + "/did.json"
        else:
            # Default to .well-known
            path = f"{self.path_prefix}/did.json"
        
        return f"{protocol}://{domain}/{path}"
    
    def _url_to_did(self, url: str) -> str:
        """Convert URL to did:web identifier"""
        # Remove protocol
        if url.startswith("https://"):
            url = url[8:]
        elif url.startswith("http://"):
            url = url[7:]
        
        # Remove trailing did.json
        if url.endswith("/did.json"):
            url = url[:-9]
        elif url.endswith("did.json"):
            url = url[:-8]
        
        # Remove .well-known prefix if present
        if f"/{self.path_prefix}" in url:
            url = url.replace(f"/{self.path_prefix}", "")
        
        # Replace slashes with colons
        parts = url.split("/")
        domain = parts[0]
        path_parts = parts[1:] if len(parts) > 1 else []
        
        # URL encode the domain if it contains a port
        if ":" in domain:
            domain = quote(domain, safe="")
        
        # Build DID
        did = f"did:web:{domain}"
        if path_parts:
            did += ":" + ":".join(path_parts)
        
        return did
    
    async def create(
        self,
        options: Optional[Dict[str, Any]] = None,
        key_type: Optional[str] = None,
        services: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create a new did:web DID
        
        Note: This creates the DID document but doesn't host it.
        The document needs to be hosted at the appropriate URL.
        """
        options = options or {}
        
        # Get domain and path from options
        domain = options.get("domain", self.default_domain)
        path = options.get("path", "")
        
        # Generate DID
        if path:
            did = f"did:web:{domain}:{path.replace('/', ':')}"
        else:
            did = f"did:web:{domain}"
        
        # Get URL where document should be hosted
        doc_url = self._did_to_url(did)
        
        # Create DID document
        did_document = {
            "@context": [
                "https://www.w3.org/ns/did/v1",
                "https://w3id.org/security/suites/jws-2020/v1"
            ],
            "id": did,
            "verificationMethod": [],
            "authentication": [],
            "assertionMethod": []
        }
        
        # Note: did:web typically doesn't generate keys itself
        # Keys are usually managed externally
        if options.get("verification_method"):
            # Add provided verification method
            vm = options["verification_method"]
            vm["controller"] = did
            did_document["verificationMethod"].append(vm)
            
            # Add to authentication and assertion
            did_document["authentication"].append(vm["id"])
            did_document["assertionMethod"].append(vm["id"])
        
        # Add services if provided
        if services:
            did_document["service"] = services
        
        return {
            "did": did,
            "did_document": did_document,
            "hosting_url": doc_url,
            "instructions": f"Host the DID document at: {doc_url}"
        }
    
    async def resolve(self, did: str) -> Optional[Dict[str, Any]]:
        """Resolve a did:web DID by fetching from the web"""
        try:
            # Convert DID to URL
            url = self._did_to_url(did)
            
            # Fetch document
            response = await self.http_client.get(
                url,
                follow_redirects=True,
                timeout=10.0
            )
            
            if response.status_code != 200:
                return None
            
            # Parse document
            did_document = response.json()
            
            # Validate document ID matches requested DID
            if did_document.get("id") != did:
                print(f"DID mismatch: requested {did}, got {did_document.get('id')}")
                return None
            
            return {
                "did_document": did_document,
                "resolved_from": url
            }
            
        except Exception as e:
            print(f"Failed to resolve {did}: {str(e)}")
            return None
    
    async def update(self, did: str, did_document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update a did:web DID
        
        Note: This only returns the updated document.
        The actual hosting/updating needs to be done externally.
        """
        # Ensure document ID matches
        did_document["id"] = did
        
        # Get hosting URL
        doc_url = self._did_to_url(did)
        
        return {
            "did": did,
            "did_document": did_document,
            "hosting_url": doc_url,
            "instructions": f"Update the DID document at: {doc_url}"
        }
    
    async def deactivate(self, did: str) -> Dict[str, Any]:
        """
        Deactivate a did:web DID
        
        Note: For did:web, deactivation means removing the document from the web.
        """
        doc_url = self._did_to_url(did)
        
        return {
            "did": did,
            "status": "deactivated",
            "instructions": f"Remove the DID document from: {doc_url}"
        }
    
    async def add_verification_method(
        self,
        did: str,
        key_type: str,
        purpose: List[str]
    ) -> Dict[str, Any]:
        """
        Add a verification method to a did:web DID
        
        Note: This doesn't actually update the hosted document.
        """
        # For did:web, we don't generate keys
        # Return a template that needs to be filled
        return {
            "id": f"{did}#key-{id(self)}",  # Simple unique ID
            "type": key_type,
            "controller": did,
            "publicKeyJwk": {
                "note": "Add actual public key here"
            }
        } 