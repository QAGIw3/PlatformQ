"""
Credential Manager - Core business logic for credential operations
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json

import httpx
from platformq_shared.consul import ConsulClient

from platformq_vc_common import (
    VerifiableCredentialModel,
    create_credential_id,
    sign_credential,
    verify_credential_signature,
    is_credential_expired,
    calculate_credential_expiry,
    hash_credential,
    get_full_context
)

from ..config import settings
from .cache_manager import CacheManager
from .event_publisher import CredentialEventPublisher
from ..storage.credential_store import CredentialStore

logger = logging.getLogger(__name__)


class CredentialManager:
    """Manages credential operations with integration to external services"""
    
    def __init__(
        self,
        credential_store: CredentialStore,
        cache_manager: CacheManager,
        event_publisher: CredentialEventPublisher,
        http_client: httpx.AsyncClient,
        key_management_url: str,
        blockchain_connector_url: str,
        did_service_url: str,
        consul_client: Optional[ConsulClient] = None
    ):
        self.credential_store = credential_store
        self.cache_manager = cache_manager
        self.event_publisher = event_publisher
        self.http_client = http_client
        self.key_management_url = key_management_url
        self.blockchain_connector_url = blockchain_connector_url
        self.did_service_url = did_service_url
        self.consul_client = consul_client
        
        # Default values - will be updated from Consul if available
        self.enable_blockchain_anchoring = settings.enable_blockchain_anchoring
        self.default_validity_days = settings.credential_default_validity_days
    
    async def load_consul_config(self):
        """Load configuration from Consul - call this after initialization"""
        if self.consul_client:
            try:
                # Get feature flags
                anchoring_config = await self.consul_client.get_kv(
                    "credential-service/features/blockchain-anchoring"
                )
                if anchoring_config:
                    self.enable_blockchain_anchoring = anchoring_config.lower() == "true"
                
                # Get credential validity defaults
                validity_config = await self.consul_client.get_kv(
                    "credential-service/defaults/validity-days"
                )
                if validity_config:
                    self.default_validity_days = int(validity_config)
                
                logger.info("Loaded configuration from Consul")
            except Exception as e:
                logger.warning(f"Failed to load Consul config, using defaults: {e}")
    
    async def issue_credential(
        self,
        credential_type: str,
        subject: Dict[str, Any],
        issuer_did: Optional[str] = None,
        validity_days: Optional[int] = None,
        description: Optional[str] = None,
        name: Optional[str] = None,
        tenant_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        store_on_ipfs: bool = True,
        encrypt_storage: bool = True,
        anchor_on_blockchain: bool = True,
        blockchain_networks: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Issue a new verifiable credential"""
        
        try:
            # Generate credential ID
            credential_id = create_credential_id()
            
            # Get or create issuer DID
            if not issuer_did:
                issuer_did = await self._get_default_issuer_did(tenant_id)
            
            # Extract subject DID if present
            subject_did = subject.get("id")
            
            # Calculate expiration
            issuance_date = datetime.utcnow()
            validity = validity_days or self.default_validity_days
            expiration_date = calculate_credential_expiry(issuance_date, validity)
            
            # Build contexts
            contexts = get_full_context([credential_type])
            
            # Create credential structure
            credential_data = {
                "@context": contexts,
                "id": credential_id,
                "type": ["VerifiableCredential", credential_type],
                "issuer": issuer_did,
                "issuanceDate": issuance_date.isoformat() + "Z",
                "expirationDate": expiration_date.isoformat() + "Z",
                "credentialSubject": subject
            }
            
            # Add optional fields
            if description:
                credential_data["description"] = description
            if name:
                credential_data["name"] = name
            if tenant_id:
                credential_data["tenantId"] = tenant_id
            if metadata:
                credential_data["metadata"] = metadata
            
            # Validate with model
            credential_model = VerifiableCredentialModel(**credential_data)
            
            # Sign the credential
            signed_credential = await self._sign_credential(
                credential_model.dict(by_alias=True),
                issuer_did
            )
            
            # Store credential
            storage_result = await self.credential_store.store_credential(
                credential_id=credential_id,
                credential=signed_credential,
                issuer_did=issuer_did,
                subject_did=subject_did,
                credential_type=credential_type,
                tenant_id=tenant_id,
                store_on_ipfs=store_on_ipfs,
                encrypt=encrypt_storage
            )
            
            # Cache credential
            await self.cache_manager.set_credential(
                credential_id,
                signed_credential
            )
            
            # Blockchain anchoring
            blockchain_info = None
            if anchor_on_blockchain and self.enable_blockchain_anchoring:
                blockchain_info = await self._anchor_on_blockchain(
                    credential_id,
                    signed_credential,
                    blockchain_networks or settings.anchor_chains
                )
            
            # Publish event
            await self.event_publisher.publish_credential_issued(
                credential_id=credential_id,
                credential=signed_credential,
                issuer_did=issuer_did,
                subject_did=subject_did,
                credential_type=credential_type,
                tenant_id=tenant_id,
                blockchain_info=blockchain_info,
                storage_info=storage_result.get("storage_info")
            )
            
            return {
                "credential": signed_credential,
                "credential_id": credential_id,
                "storage_info": storage_result.get("storage_info"),
                "blockchain_info": blockchain_info
            }
            
        except Exception as e:
            logger.error(f"Failed to issue credential: {e}")
            raise
    
    async def verify_credential(
        self,
        credential: Dict[str, Any],
        check_revocation: bool = True,
        check_expiration: bool = True,
        verify_signature: bool = True,
        verify_issuer: bool = True,
        expected_issuer: Optional[str] = None,
        expected_subject: Optional[str] = None
    ) -> Dict[str, Any]:
        """Verify a credential"""
        
        credential_id = credential.get("id", "unknown")
        errors = []
        warnings = []
        checks = {}
        
        try:
            # Check expiration
            if check_expiration:
                is_expired = is_credential_expired(credential)
                checks["expiration"] = not is_expired
                if is_expired:
                    errors.append("Credential has expired")
            
            # Check revocation status
            if check_revocation:
                is_revoked = await self._check_revocation_status(credential_id)
                checks["revocation"] = not is_revoked
                if is_revoked:
                    errors.append("Credential has been revoked")
            
            # Verify signature
            if verify_signature:
                issuer_did = credential.get("issuer")
                if isinstance(issuer_did, dict):
                    issuer_did = issuer_did.get("id")
                    
                sig_valid = await self._verify_signature(credential, issuer_did)
                checks["signature"] = sig_valid
                if not sig_valid:
                    errors.append("Invalid signature")
            
            # Verify issuer
            if verify_issuer and expected_issuer:
                actual_issuer = credential.get("issuer")
                if isinstance(actual_issuer, dict):
                    actual_issuer = actual_issuer.get("id")
                    
                checks["issuer"] = actual_issuer == expected_issuer
                if not checks["issuer"]:
                    errors.append(f"Unexpected issuer: {actual_issuer}")
            
            # Verify subject
            if expected_subject:
                subject = credential.get("credentialSubject", {})
                actual_subject = subject.get("id") if isinstance(subject, dict) else None
                
                checks["subject"] = actual_subject == expected_subject
                if not checks["subject"]:
                    errors.append(f"Unexpected subject: {actual_subject}")
            
            # Overall validity
            valid = len(errors) == 0
            
            # Publish verification event
            verification_result = {
                "valid": valid,
                "checks": checks,
                "errors": errors,
                "warnings": warnings
            }
            
            await self.event_publisher.publish_credential_verified(
                credential_id=credential_id,
                verification_result=verification_result
            )
            
            return {
                "valid": valid,
                "credential_id": credential_id,
                "checks": checks,
                "errors": errors,
                "warnings": warnings
            }
            
        except Exception as e:
            logger.error(f"Error during verification: {e}")
            return {
                "valid": False,
                "credential_id": credential_id,
                "checks": checks,
                "errors": ["Verification error: " + str(e)],
                "warnings": warnings
            }
    
    async def get_credential(self, credential_id: str) -> Optional[Dict[str, Any]]:
        """Get a credential by ID"""
        
        # Check cache first
        cached = await self.cache_manager.get_credential(credential_id)
        if cached:
            return cached
        
        # Get from storage
        result = await self.credential_store.get_credential(credential_id)
        if result and result.get("credential"):
            credential = result["credential"]
            
            # Cache for future requests
            await self.cache_manager.set_credential(credential_id, credential)
            
            return credential
        
        return None
    
    async def revoke_credential(
        self,
        credential_id: str,
        reason: str,
        issuer_did: Optional[str] = None
    ) -> bool:
        """Revoke a credential"""
        
        # Update status in database
        success = await self.credential_store.update_credential_status(
            credential_id,
            status="revoked",
            reason=reason
        )
        
        if success:
            # Invalidate cache
            await self.cache_manager.invalidate_credential(credential_id)
            
            # Cache revocation status
            await self.cache_manager.set_revocation_status(credential_id, True)
            
            # Publish event
            await self.event_publisher.publish_credential_revoked(
                credential_id=credential_id,
                issuer_did=issuer_did or "unknown",
                reason=reason
            )
        
        return success
    
    async def get_credential_status(self, credential_id: str) -> Optional[Dict[str, Any]]:
        """Get credential status information"""
        
        result = await self.credential_store.get_credential(credential_id)
        if not result:
            return None
        
        metadata = result.get("metadata", {})
        
        # Determine current status
        status = metadata.get("status", "unknown")
        if metadata.get("revoked"):
            status = "revoked"
        elif metadata.get("expires_at"):
            expiry = datetime.fromisoformat(metadata["expires_at"])
            if expiry < datetime.utcnow():
                status = "expired"
        
        return {
            "credential_id": credential_id,
            "status": status,
            "issued_at": datetime.fromisoformat(metadata.get("issued_at")),
            "expires_at": datetime.fromisoformat(metadata["expires_at"]) if metadata.get("expires_at") else None,
            "revoked_at": metadata.get("revoked_at"),
            "revocation_reason": metadata.get("revocation_reason"),
            "blockchain_anchors": metadata.get("blockchain_anchors")
        }
    
    async def batch_issue_credentials(
        self,
        requests: List[Dict[str, Any]],
        fail_on_error: bool = False,
        parallel_processing: bool = True
    ) -> List[Dict[str, Any]]:
        """Issue multiple credentials in batch"""
        
        if parallel_processing:
            # Process in parallel
            tasks = [
                self._issue_with_error_handling(req)
                for req in requests
            ]
            results = await asyncio.gather(*tasks, return_exceptions=not fail_on_error)
        else:
            # Process sequentially
            results = []
            for req in requests:
                try:
                    result = await self._issue_with_error_handling(req)
                    results.append(result)
                except Exception as e:
                    if fail_on_error:
                        raise
                    results.append({
                        "success": False,
                        "error": str(e),
                        "request": req
                    })
        
        return results
    
    async def batch_verify_credentials(
        self,
        credentials: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Verify multiple credentials in batch"""
        
        # Use Ignite to parallelize if available
        tasks = [
            self.verify_credential(credential)
            for credential in credentials
        ]
        
        return await asyncio.gather(*tasks, return_exceptions=False)
    
    async def search_credentials(
        self,
        issuer: Optional[str] = None,
        subject: Optional[str] = None,
        credential_type: Optional[str] = None,
        issued_after: Optional[datetime] = None,
        issued_before: Optional[datetime] = None,
        include_revoked: bool = False,
        only_valid: bool = True,
        offset: int = 0,
        limit: int = 20
    ) -> Dict[str, Any]:
        """Search credentials with filters"""
        
        # Note: This is delegated to the store for now
        # In future, could use Elasticsearch for advanced search
        
        return await self.credential_store.search_credentials(
            issuer=issuer,
            subject=subject,
            credential_type=credential_type,
            include_revoked=include_revoked,
            offset=offset,
            limit=limit
        )
    
    async def _get_default_issuer_did(self, tenant_id: Optional[str]) -> str:
        """Get default issuer DID for tenant"""
        
        if tenant_id:
            # Get tenant-specific issuer from DID service
            try:
                response = await self.http_client.get(
                    f"{self.did_service_url}/api/v1/dids/tenant/{tenant_id}/issuer"
                )
                if response.status_code == 200:
                    return response.json()["did"]
            except Exception as e:
                logger.warning(f"Failed to get tenant issuer DID: {e}")
        
        # Return platform default
        return f"did:platformq:default:issuer"
    
    async def _sign_credential(
        self,
        credential: Dict[str, Any],
        issuer_did: str
    ) -> Dict[str, Any]:
        """Sign credential using key management service"""
        
        try:
            # Request signature from key management service
            response = await self.http_client.post(
                f"{self.key_management_url}/api/v1/sign/credential",
                json={
                    "credential": credential,
                    "issuer_did": issuer_did,
                    "proof_type": "Ed25519Signature2020"
                }
            )
            response.raise_for_status()
            
            return response.json()["signed_credential"]
            
        except Exception as e:
            logger.error(f"Failed to sign credential: {e}")
            raise
    
    async def _verify_signature(
        self,
        credential: Dict[str, Any],
        issuer_did: str
    ) -> bool:
        """Verify credential signature"""
        
        # Check cache for issuer key
        key_info = await self.cache_manager.get_issuer_key(issuer_did)
        
        if not key_info:
            # Resolve DID to get public key
            try:
                response = await self.http_client.get(
                    f"{self.did_service_url}/api/v1/dids/{issuer_did}"
                )
                if response.status_code == 200:
                    did_doc = response.json()
                    # Extract public key from verification method
                    key_info = self._extract_public_key(did_doc)
                    
                    # Cache for future use
                    await self.cache_manager.set_issuer_key(issuer_did, key_info)
            except Exception as e:
                logger.error(f"Failed to resolve issuer DID: {e}")
                return False
        
        if key_info:
            # Verify using public key
            try:
                public_key = base64.b64decode(key_info["public_key"])
                return verify_credential_signature(
                    credential,
                    public_key,
                    proof_type=credential.get("proof", {}).get("type", "Ed25519Signature2020")
                )
            except Exception as e:
                logger.error(f"Signature verification failed: {e}")
                
        return False
    
    async def _check_revocation_status(self, credential_id: str) -> bool:
        """Check if credential is revoked"""
        
        # Check cache first
        cached_status = await self.cache_manager.check_revocation_status(credential_id)
        if cached_status is not None:
            return cached_status
        
        # Check database
        status = await self.get_credential_status(credential_id)
        if status:
            is_revoked = status.get("status") == "revoked"
            
            # Cache result
            await self.cache_manager.set_revocation_status(credential_id, is_revoked)
            
            return is_revoked
        
        return False
    
    async def _anchor_on_blockchain(
        self,
        credential_id: str,
        credential: Dict[str, Any],
        blockchain_networks: List[str]
    ) -> Dict[str, Any]:
        """Anchor credential on blockchain(s)"""
        
        # Create credential hash
        credential_hash = hash_credential(credential)
        
        anchors = {}
        for network in blockchain_networks:
            try:
                # Call blockchain connector service
                response = await self.http_client.post(
                    f"{self.blockchain_connector_url}/api/v1/anchor",
                    json={
                        "chain": network,
                        "data": {
                            "type": "credential",
                            "id": credential_id,
                            "hash": credential_hash
                        }
                    }
                )
                
                if response.status_code == 200:
                    result = response.json()
                    anchors[network] = {
                        "transaction_hash": result["transaction_hash"],
                        "status": "pending"
                    }
                    
                    # Store anchor info
                    await self.credential_store.add_blockchain_anchor(
                        credential_id,
                        network,
                        result["transaction_hash"]
                    )
                    
                    # Publish event
                    await self.event_publisher.publish_credential_anchored(
                        credential_id=credential_id,
                        blockchain=network,
                        transaction_hash=result["transaction_hash"]
                    )
                    
            except Exception as e:
                logger.error(f"Failed to anchor on {network}: {e}")
                anchors[network] = {"error": str(e)}
        
        return anchors
    
    async def _issue_with_error_handling(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Issue credential with error handling for batch operations"""
        try:
            result = await self.issue_credential(**request)
            return {
                "success": True,
                "credential_id": result["credential_id"],
                "result": result
            }
        except Exception as e:
            logger.error(f"Failed to issue credential in batch: {e}")
            return {
                "success": False,
                "error": str(e),
                "request": request
            }
    
    def _extract_public_key(self, did_document: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract public key from DID document"""
        
        verification_methods = did_document.get("verificationMethod", [])
        for method in verification_methods:
            if method.get("type") in ["Ed25519VerificationKey2020", "Ed25519VerificationKey2018"]:
                return {
                    "key_id": method["id"],
                    "type": method["type"],
                    "public_key": method.get("publicKeyBase58") or method.get("publicKeyBase64")
                }
        
        return None


# Import for base64 operations
import base64 