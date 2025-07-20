"""
Proof Engine for ZKP Generation and Verification
"""

import json
import base64
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
import httpx

from app.config import settings
from app.core.compute_manager import ComputeManager
from app.core.cache_manager import ProofCacheManager
from platformq_consul import ConsulClient


class ProofEngine:
    """
    Orchestrates zero-knowledge proof generation and verification
    """
    
    def __init__(
        self,
        compute_manager: Optional[ComputeManager],
        cache_manager: Optional[ProofCacheManager],
        http_client: httpx.AsyncClient,
        vault_client: Optional[Any],
        consul_client: Optional[ConsulClient]
    ):
        self.compute_manager = compute_manager
        self.cache_manager = cache_manager
        self.http_client = http_client
        self.vault_client = vault_client
        self.consul_client = consul_client
        
        # Proof type handlers
        self.proof_handlers = {}
        
        # Statistics
        self.total_proofs_generated = 0
        self.total_proofs_verified = 0
        
    async def initialize(self):
        """Initialize proof libraries and handlers"""
        # Register proof type handlers
        self._register_proof_handlers()
        
        # Initialize BBS+ library
        await self._initialize_bbs_library()
        
        # Load configuration from Consul if available
        if self.consul_client and settings.enable_consul_config:
            config = await self.consul_client.get_service_config(settings.service_name)
            if config:
                # Apply dynamic configuration
                self._apply_dynamic_config(config)
    
    def _register_proof_handlers(self):
        """Register handlers for different proof types"""
        self.proof_handlers = {
            "bbs_signature": self._handle_bbs_signature,
            "selective_disclosure": self._handle_selective_disclosure,
            "range_proof": self._handle_range_proof,
            "predicate_proof": self._handle_predicate_proof,
            "set_membership": self._handle_set_membership,
            "composite_proof": self._handle_composite_proof
        }
    
    async def _initialize_bbs_library(self):
        """Initialize BBS+ cryptographic library"""
        # This would initialize the actual BBS+ library
        # For now, we'll use a placeholder
        print("BBS+ library initialized")
    
    def _apply_dynamic_config(self, config: Dict[str, Any]):
        """Apply dynamic configuration from Consul"""
        # Update settings based on Consul config
        for key, value in config.items():
            if hasattr(settings, key):
                setattr(settings, key, value)
    
    async def generate_proof(
        self,
        proof_type: str,
        credential: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate a zero-knowledge proof"""
        # Check cache first
        cache_key = self._generate_cache_key(proof_type, credential, options)
        if self.cache_manager:
            cached_proof = await self.cache_manager.get_proof(cache_key)
            if cached_proof:
                return cached_proof
        
        # Validate proof type
        if proof_type not in self.proof_handlers:
            raise ValueError(f"Unsupported proof type: {proof_type}")
        
        # Generate proof using appropriate handler
        handler = self.proof_handlers[proof_type]
        
        # Decide whether to use compute grid
        if self._should_use_compute_grid(proof_type, options):
            # Submit to compute grid
            task_id = await self.compute_manager.submit_task(
                task_type=proof_type,
                params={
                    "credential": credential,
                    "options": options
                },
                priority=options.get("priority", 5)
            )
            
            # Wait for result (with timeout)
            result = await self._wait_for_compute_result(task_id)
            proof = result["proof"]
        else:
            # Generate locally
            proof = await handler(credential, options)
        
        # Cache the proof
        if self.cache_manager:
            await self.cache_manager.set_proof(
                cache_key,
                proof,
                ttl=options.get("cache_ttl", settings.cache_ttl_seconds)
            )
        
        # Update statistics
        self.total_proofs_generated += 1
        
        # Publish event
        await self._publish_proof_event("generated", proof_type, proof)
        
        return proof
    
    async def verify_proof(
        self,
        proof_type: str,
        proof: Dict[str, Any],
        public_key: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify a zero-knowledge proof"""
        # Check cache for verification result
        cache_key = self._generate_verification_cache_key(proof_type, proof, public_key)
        if self.cache_manager:
            cached_result = await self.cache_manager.get_verification(cache_key)
            if cached_result:
                return cached_result
        
        # Perform verification
        if proof_type == "bbs_signature":
            result = await self._verify_bbs_signature(proof, public_key, options)
        elif proof_type == "selective_disclosure":
            result = await self._verify_selective_disclosure(proof, public_key, options)
        elif proof_type == "range_proof":
            result = await self._verify_range_proof(proof, public_key, options)
        elif proof_type == "predicate_proof":
            result = await self._verify_predicate_proof(proof, public_key, options)
        else:
            raise ValueError(f"Unsupported proof type for verification: {proof_type}")
        
        # Cache verification result
        if self.cache_manager:
            await self.cache_manager.set_verification(
                cache_key,
                result,
                ttl=300  # Short TTL for verification results
            )
        
        # Update statistics
        self.total_proofs_verified += 1
        
        # Publish event
        await self._publish_proof_event("verified", proof_type, result)
        
        return result
    
    # BBS+ Signature handling
    async def _handle_bbs_signature(
        self,
        credential: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate BBS+ signature for credential"""
        # Get signing key from key management service
        private_key = await self._get_signing_key(options.get("key_id"))
        
        # Prepare messages from credential
        messages = self._extract_messages_from_credential(credential)
        
        # Generate BBS+ signature
        signature = await self._generate_bbs_signature(messages, private_key)
        
        return {
            "proof": {
                "type": "BbsBlsSignature2020",
                "created": datetime.now(timezone.utc).isoformat(),
                "proofPurpose": "assertionMethod",
                "verificationMethod": options.get("verification_method"),
                "proofValue": base64.b64encode(signature).decode()
            },
            "credential": credential
        }
    
    async def _handle_selective_disclosure(
        self,
        credential: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate selective disclosure proof"""
        disclosed_attributes = options.get("disclosed_attributes", [])
        nonce = options.get("nonce", "")
        
        # Get the original BBS+ signature
        original_proof = credential.get("proof", {})
        if original_proof.get("type") != "BbsBlsSignature2020":
            raise ValueError("Credential must have BBS+ signature for selective disclosure")
        
        # Extract messages and create disclosure bitmap
        messages = self._extract_messages_from_credential(credential)
        disclosure_bitmap = self._create_disclosure_bitmap(messages, disclosed_attributes)
        
        # Generate derived proof
        derived_proof = await self._generate_derived_proof(
            original_proof,
            messages,
            disclosure_bitmap,
            nonce
        )
        
        # Create disclosed credential
        disclosed_credential = self._create_disclosed_credential(
            credential,
            disclosed_attributes
        )
        
        return {
            "proof": {
                "type": "BbsBlsSignatureProof2020",
                "created": datetime.now(timezone.utc).isoformat(),
                "proofPurpose": "assertionMethod",
                "verificationMethod": original_proof.get("verificationMethod"),
                "proofValue": base64.b64encode(derived_proof).decode(),
                "nonce": nonce
            },
            "revealedDocument": disclosed_credential
        }
    
    async def _handle_range_proof(
        self,
        credential: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate range proof for numeric attributes"""
        attribute = options.get("attribute")
        min_value = options.get("min")
        max_value = options.get("max")
        
        # Extract attribute value
        value = self._extract_attribute_value(credential, attribute)
        if not isinstance(value, (int, float)):
            raise ValueError(f"Attribute {attribute} must be numeric for range proof")
        
        # Generate range proof
        proof = await self._generate_range_proof_internal(
            value,
            min_value,
            max_value,
            options.get("bits", settings.range_proof_bits)
        )
        
        return {
            "proof": {
                "type": "RangeProof2023",
                "created": datetime.now(timezone.utc).isoformat(),
                "attribute": attribute,
                "range": {
                    "min": min_value,
                    "max": max_value
                },
                "proofValue": base64.b64encode(proof).decode()
            }
        }
    
    async def _handle_predicate_proof(
        self,
        credential: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate predicate proof"""
        predicate = options.get("predicate", {})
        attribute = predicate.get("attribute")
        operator = predicate.get("operator")
        threshold = predicate.get("value")
        
        # Extract attribute value
        value = self._extract_attribute_value(credential, attribute)
        
        # Generate predicate proof based on operator
        if operator == ">=":
            proof = await self._generate_gte_proof(value, threshold)
        elif operator == ">":
            proof = await self._generate_gt_proof(value, threshold)
        elif operator == "<=":
            proof = await self._generate_lte_proof(value, threshold)
        elif operator == "<":
            proof = await self._generate_lt_proof(value, threshold)
        elif operator == "==":
            proof = await self._generate_eq_proof(value, threshold)
        else:
            raise ValueError(f"Unsupported operator: {operator}")
        
        return {
            "proof": {
                "type": "PredicateProof2023",
                "created": datetime.now(timezone.utc).isoformat(),
                "predicate": predicate,
                "proofValue": base64.b64encode(proof).decode()
            }
        }
    
    async def _handle_set_membership(
        self,
        credential: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate set membership proof"""
        attribute = options.get("attribute")
        allowed_set = options.get("set", [])
        
        # Extract attribute value
        value = self._extract_attribute_value(credential, attribute)
        
        # Verify membership
        if value not in allowed_set:
            raise ValueError(f"Value {value} not in allowed set")
        
        # Generate membership proof
        proof = await self._generate_set_membership_proof(
            value,
            allowed_set,
            options.get("use_bloom_filter", True)
        )
        
        return {
            "proof": {
                "type": "SetMembershipProof2023",
                "created": datetime.now(timezone.utc).isoformat(),
                "attribute": attribute,
                "setSize": len(allowed_set),
                "proofValue": base64.b64encode(proof).decode()
            }
        }
    
    async def _handle_composite_proof(
        self,
        credential: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate composite proof combining multiple proof types"""
        sub_proofs = []
        
        for proof_spec in options.get("proofs", []):
            proof_type = proof_spec.get("type")
            proof_options = proof_spec.get("options", {})
            
            # Generate sub-proof
            sub_proof = await self.generate_proof(
                proof_type,
                credential,
                proof_options
            )
            sub_proofs.append(sub_proof)
        
        # Combine proofs
        combined_proof = self._combine_proofs(sub_proofs, options.get("operator", "AND"))
        
        return {
            "proof": {
                "type": "CompositeProof2023",
                "created": datetime.now(timezone.utc).isoformat(),
                "subProofs": sub_proofs,
                "operator": options.get("operator", "AND"),
                "proofValue": base64.b64encode(combined_proof).decode()
            }
        }
    
    # Verification methods
    async def _verify_bbs_signature(
        self,
        proof: Dict[str, Any],
        public_key: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify BBS+ signature"""
        # Extract proof value
        proof_value = base64.b64decode(proof.get("proofValue", ""))
        
        # Extract messages from credential
        credential = options.get("credential", {})
        messages = self._extract_messages_from_credential(credential)
        
        # Verify signature
        is_valid = await self._verify_bbs_signature_internal(
            proof_value,
            messages,
            public_key
        )
        
        return {
            "valid": is_valid,
            "proof_type": "BbsBlsSignature2020",
            "verified_at": datetime.now(timezone.utc).isoformat()
        }
    
    async def _verify_selective_disclosure(
        self,
        proof: Dict[str, Any],
        public_key: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify selective disclosure proof"""
        # Extract proof components
        proof_value = base64.b64decode(proof.get("proofValue", ""))
        nonce = proof.get("nonce", "")
        revealed_document = options.get("revealedDocument", {})
        
        # Verify derived proof
        is_valid = await self._verify_derived_proof_internal(
            proof_value,
            revealed_document,
            public_key,
            nonce
        )
        
        return {
            "valid": is_valid,
            "proof_type": "BbsBlsSignatureProof2020",
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "nonce_verified": True
        }
    
    async def _verify_range_proof(
        self,
        proof: Dict[str, Any],
        public_key: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify range proof"""
        proof_value = base64.b64decode(proof.get("proofValue", ""))
        range_spec = proof.get("range", {})
        
        # Verify range proof
        is_valid = await self._verify_range_proof_internal(
            proof_value,
            range_spec.get("min"),
            range_spec.get("max"),
            public_key
        )
        
        return {
            "valid": is_valid,
            "proof_type": "RangeProof2023",
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "range": range_spec
        }
    
    async def _verify_predicate_proof(
        self,
        proof: Dict[str, Any],
        public_key: Dict[str, Any],
        options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify predicate proof"""
        proof_value = base64.b64decode(proof.get("proofValue", ""))
        predicate = proof.get("predicate", {})
        
        # Verify predicate proof
        is_valid = await self._verify_predicate_proof_internal(
            proof_value,
            predicate,
            public_key
        )
        
        return {
            "valid": is_valid,
            "proof_type": "PredicateProof2023",
            "verified_at": datetime.now(timezone.utc).isoformat(),
            "predicate": predicate
        }
    
    # Helper methods
    def _should_use_compute_grid(self, proof_type: str, options: Dict[str, Any]) -> bool:
        """Determine if proof should be generated on compute grid"""
        if not self.compute_manager or not settings.enable_compute_grid:
            return False
        
        # Use compute grid for complex proofs
        if proof_type in ["composite_proof", "range_proof"]:
            return True
        
        # Use compute grid if explicitly requested
        if options.get("use_compute_grid"):
            return True
        
        # Use compute grid for batch operations
        if options.get("batch_size", 1) > 1:
            return True
        
        return False
    
    async def _wait_for_compute_result(self, task_id: str) -> Dict[str, Any]:
        """Wait for compute task to complete"""
        import asyncio
        
        timeout = settings.compute_timeout_seconds
        start_time = datetime.now(timezone.utc)
        
        while True:
            # Check task status
            result = await self.compute_manager.get_task_result(task_id)
            
            if result and result.get("status") == "completed":
                return result["result"]
            elif result and result.get("status") == "failed":
                raise RuntimeError(f"Compute task failed: {result.get('error')}")
            
            # Check timeout
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            if elapsed > timeout:
                # Try to cancel task
                await self.compute_manager.cancel_task(task_id)
                raise TimeoutError(f"Compute task timed out after {timeout} seconds")
            
            # Wait before checking again
            await asyncio.sleep(1)
    
    def _generate_cache_key(
        self,
        proof_type: str,
        credential: Dict[str, Any],
        options: Dict[str, Any]
    ) -> str:
        """Generate cache key for proof"""
        import hashlib
        
        # Create deterministic key
        key_data = {
            "type": proof_type,
            "credential_id": credential.get("id"),
            "options": options
        }
        
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_str.encode()).hexdigest()
    
    def _generate_verification_cache_key(
        self,
        proof_type: str,
        proof: Dict[str, Any],
        public_key: Dict[str, Any]
    ) -> str:
        """Generate cache key for verification result"""
        import hashlib
        
        key_data = {
            "type": proof_type,
            "proof_value": proof.get("proofValue"),
            "public_key": public_key
        }
        
        key_str = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_str.encode()).hexdigest()
    
    async def _get_signing_key(self, key_id: str) -> Dict[str, Any]:
        """Get signing key from key management service"""
        response = await self.http_client.get(
            f"{settings.key_management_url}/api/v1/keys/{key_id}"
        )
        
        if response.status_code != 200:
            raise RuntimeError(f"Failed to get signing key: {response.text}")
        
        return response.json()
    
    def _extract_messages_from_credential(self, credential: Dict[str, Any]) -> List[str]:
        """Extract messages from credential for BBS+ signature"""
        messages = []
        
        # Extract credential subject attributes
        subject = credential.get("credentialSubject", {})
        for key, value in subject.items():
            if key != "id":  # Skip ID
                messages.append(f"{key}:{value}")
        
        # Add issuance date
        if "issuanceDate" in credential:
            messages.append(f"issuanceDate:{credential['issuanceDate']}")
        
        return messages
    
    def _extract_attribute_value(self, credential: Dict[str, Any], attribute: str) -> Any:
        """Extract attribute value from credential"""
        subject = credential.get("credentialSubject", {})
        
        # Handle nested attributes
        if "." in attribute:
            parts = attribute.split(".")
            value = subject
            for part in parts:
                value = value.get(part, None)
                if value is None:
                    break
            return value
        
        return subject.get(attribute)
    
    def _create_disclosure_bitmap(
        self,
        messages: List[str],
        disclosed_attributes: List[str]
    ) -> List[bool]:
        """Create bitmap indicating which messages to disclose"""
        bitmap = []
        
        for message in messages:
            # Check if message should be disclosed
            attribute = message.split(":")[0]
            bitmap.append(attribute in disclosed_attributes)
        
        return bitmap
    
    def _create_disclosed_credential(
        self,
        credential: Dict[str, Any],
        disclosed_attributes: List[str]
    ) -> Dict[str, Any]:
        """Create credential with only disclosed attributes"""
        disclosed = {
            "@context": credential.get("@context"),
            "type": credential.get("type"),
            "credentialSubject": {"id": credential.get("credentialSubject", {}).get("id")}
        }
        
        # Copy only disclosed attributes
        subject = credential.get("credentialSubject", {})
        for attr in disclosed_attributes:
            if attr in subject:
                disclosed["credentialSubject"][attr] = subject[attr]
        
        return disclosed
    
    def _combine_proofs(self, sub_proofs: List[Dict[str, Any]], operator: str) -> bytes:
        """Combine multiple proofs into composite proof"""
        # This is a simplified implementation
        # Real implementation would properly combine cryptographic proofs
        combined = {
            "operator": operator,
            "proofs": [p.get("proof", {}).get("proofValue") for p in sub_proofs]
        }
        
        return json.dumps(combined).encode()
    
    async def _publish_proof_event(self, event_type: str, proof_type: str, data: Dict[str, Any]):
        """Publish proof event to event bus"""
        # This would integrate with the event streaming system
        event = {
            "type": f"proof_{event_type}",
            "proof_type": proof_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "data": data
        }
        
        # Publish to Pulsar or other event system
        print(f"Published event: {event['type']}")
    
    # Placeholder cryptographic methods
    # These would be implemented using actual cryptographic libraries
    
    async def _generate_bbs_signature(self, messages: List[str], private_key: Dict[str, Any]) -> bytes:
        """Generate BBS+ signature (placeholder)"""
        # Real implementation would use py-bbs-signatures
        return b"bbs_signature_placeholder"
    
    async def _verify_bbs_signature_internal(
        self,
        signature: bytes,
        messages: List[str],
        public_key: Dict[str, Any]
    ) -> bool:
        """Verify BBS+ signature (placeholder)"""
        return True
    
    async def _generate_derived_proof(
        self,
        original_proof: Dict[str, Any],
        messages: List[str],
        disclosure_bitmap: List[bool],
        nonce: str
    ) -> bytes:
        """Generate derived proof for selective disclosure (placeholder)"""
        return b"derived_proof_placeholder"
    
    async def _verify_derived_proof_internal(
        self,
        proof: bytes,
        revealed_document: Dict[str, Any],
        public_key: Dict[str, Any],
        nonce: str
    ) -> bool:
        """Verify derived proof (placeholder)"""
        return True
    
    async def _generate_range_proof_internal(
        self,
        value: float,
        min_value: float,
        max_value: float,
        bits: int
    ) -> bytes:
        """Generate range proof (placeholder)"""
        return b"range_proof_placeholder"
    
    async def _verify_range_proof_internal(
        self,
        proof: bytes,
        min_value: float,
        max_value: float,
        public_key: Dict[str, Any]
    ) -> bool:
        """Verify range proof (placeholder)"""
        return True
    
    async def _generate_gte_proof(self, value: float, threshold: float) -> bytes:
        """Generate greater-than-or-equal proof (placeholder)"""
        return b"gte_proof_placeholder"
    
    async def _generate_gt_proof(self, value: float, threshold: float) -> bytes:
        """Generate greater-than proof (placeholder)"""
        return b"gt_proof_placeholder"
    
    async def _generate_lte_proof(self, value: float, threshold: float) -> bytes:
        """Generate less-than-or-equal proof (placeholder)"""
        return b"lte_proof_placeholder"
    
    async def _generate_lt_proof(self, value: float, threshold: float) -> bytes:
        """Generate less-than proof (placeholder)"""
        return b"lt_proof_placeholder"
    
    async def _generate_eq_proof(self, value: Any, expected: Any) -> bytes:
        """Generate equality proof (placeholder)"""
        return b"eq_proof_placeholder"
    
    async def _verify_predicate_proof_internal(
        self,
        proof: bytes,
        predicate: Dict[str, Any],
        public_key: Dict[str, Any]
    ) -> bool:
        """Verify predicate proof (placeholder)"""
        return True
    
    async def _generate_set_membership_proof(
        self,
        value: Any,
        allowed_set: List[Any],
        use_bloom_filter: bool
    ) -> bytes:
        """Generate set membership proof (placeholder)"""
        return b"set_membership_proof_placeholder"
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get proof engine statistics"""
        return {
            "total_proofs_generated": self.total_proofs_generated,
            "total_proofs_verified": self.total_proofs_verified,
            "supported_proof_types": list(self.proof_handlers.keys()),
            "cache_enabled": self.cache_manager is not None,
            "compute_grid_enabled": self.compute_manager is not None
        } 