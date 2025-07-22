"""
Verifiable Presentation Manager
"""

import json
import base64
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
from enum import Enum
import httpx
import uuid

from app.config import settings
from app.storage.presentation_store import PresentationStore
from app.core.session_manager import SessionManager
from platformq_consul import ConsulClient


class PresentationStatus(str, Enum):
    """Presentation status enum"""
    DRAFT = "draft"
    SUBMITTED = "submitted"
    VERIFIED = "verified"
    REJECTED = "rejected"
    EXPIRED = "expired"


class VerificationResult(str, Enum):
    """Verification result enum"""
    VALID = "valid"
    INVALID = "invalid"
    EXPIRED = "expired"
    REVOKED = "revoked"


class PresentationManager:
    """
    Manages Verifiable Presentation operations including creation,
    submission, and verification
    """
    
    def __init__(
        self,
        credential_service_url: str,
        zkp_service_url: str,
        did_service_url: str,
        http_client: httpx.AsyncClient,
        vault_client: Optional[Any],
        consul_client: Optional[ConsulClient],
        presentation_store: Optional[PresentationStore],
        session_manager: Optional[SessionManager]
    ):
        self.credential_service_url = credential_service_url
        self.zkp_service_url = zkp_service_url
        self.did_service_url = did_service_url
        self.http_client = http_client
        self.vault_client = vault_client
        self.consul_client = consul_client
        self.presentation_store = presentation_store
        self.session_manager = session_manager
        self.initialized = False
        
        # Caches
        self.verification_policies = {}
        self.trusted_issuers = set()
        
        # Statistics
        self.total_created = 0
        self.total_verified = 0
        self.total_rejected = 0
    
    async def initialize(self):
        """Initialize the presentation manager"""
        # Load verification policies from Consul if available
        if self.consul_client and settings.enable_consul_config:
            config = await self.consul_client.get_service_config(settings.service_name)
            if config:
                self.verification_policies = config.get("verification_policies", {})
                self.trusted_issuers = set(config.get("trusted_issuers", []))
        
        self.initialized = True
    
    async def create_presentation(
        self,
        holder_did: str,
        credential_ids: List[str],
        verifier_did: Optional[str] = None,
        challenge: Optional[str] = None,
        domain: Optional[str] = None,
        selective_disclosure: Optional[Dict[str, List[str]]] = None,
        proof_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create a Verifiable Presentation
        
        Args:
            holder_did: DID of the holder creating the presentation
            credential_ids: List of credential IDs to include
            verifier_did: Optional DID of intended verifier
            challenge: Optional challenge nonce from verifier
            domain: Optional domain for domain binding
            selective_disclosure: Optional fields to disclose per credential
            proof_options: Optional proof generation options
            
        Returns:
            Created presentation
        """
        # Create presentation ID
        presentation_id = f"vp-{uuid.uuid4()}"
        
        # Get credentials
        credentials = []
        for cred_id in credential_ids:
            credential = await self._get_credential(cred_id)
            if not credential:
                raise ValueError(f"Credential {cred_id} not found")
            credentials.append(credential)
        
        # Build presentation
        presentation = {
            "@context": [
                "https://www.w3.org/2018/credentials/v1",
                "https://www.w3.org/2018/credentials/examples/v1"
            ],
            "type": ["VerifiablePresentation"],
            "id": presentation_id,
            "holder": holder_did,
            "verifiableCredential": []
        }
        
        # Process each credential
        for i, credential in enumerate(credentials):
            cred_id = credential_ids[i]
            
            # Apply selective disclosure if requested
            if selective_disclosure and cred_id in selective_disclosure:
                disclosed_fields = selective_disclosure[cred_id]
                
                # Generate ZKP for selective disclosure
                zkp_proof = await self._generate_selective_disclosure_proof(
                    credential,
                    disclosed_fields,
                    challenge or ""
                )
                
                presentation["verifiableCredential"].append({
                    "credential": zkp_proof["revealedDocument"],
                    "proof": zkp_proof["proof"]
                })
            else:
                # Include full credential
                presentation["verifiableCredential"].append(credential)
        
        # Generate presentation proof
        presentation_proof = await self._generate_presentation_proof(
            presentation,
            holder_did,
            challenge,
            domain,
            proof_options
        )
        
        presentation["proof"] = presentation_proof
        
        # Store presentation
        stored = await self.presentation_store.create(
            presentation_id=presentation_id,
            holder_did=holder_did,
            verifier_did=verifier_did,
            presentation=presentation,
            credential_ids=credential_ids,
            challenge=challenge,
            domain=domain,
            status=PresentationStatus.DRAFT
        )
        
        # Update statistics
        self.total_created += 1
        
        return {
            "id": presentation_id,
            "presentation": presentation,
            "status": PresentationStatus.DRAFT,
            "created_at": stored.created_at.isoformat()
        }
    
    async def submit_presentation(
        self,
        presentation_id: str,
        verifier_did: str,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Submit a presentation to a verifier
        
        Args:
            presentation_id: ID of presentation to submit
            verifier_did: DID of the verifier
            session_id: Optional session ID for the submission
            
        Returns:
            Submission details
        """
        # Get presentation
        presentation_record = await self.presentation_store.get(presentation_id)
        if not presentation_record:
            raise ValueError(f"Presentation {presentation_id} not found")
        
        if presentation_record.status != PresentationStatus.DRAFT:
            raise ValueError(f"Presentation already submitted with status: {presentation_record.status}")
        
        # Update verifier if not set
        if not presentation_record.verifier_did:
            presentation_record.verifier_did = verifier_did
        elif presentation_record.verifier_did != verifier_did:
            raise ValueError("Presentation was created for a different verifier")
        
        # Create or update session
        if session_id:
            session = await self.session_manager.get_session(session_id)
            if not session:
                raise ValueError(f"Session {session_id} not found")
            
            # Update session with presentation
            await self.session_manager.update_session(
                session_id,
                {"presentation_id": presentation_id, "status": "submitted"}
            )
        else:
            # Create new session
            session = await self.session_manager.create_session(
                holder_did=presentation_record.holder_did,
                verifier_did=verifier_did,
                presentation_id=presentation_id,
                metadata={"status": "submitted"}
            )
            session_id = session["id"]
        
        # Update presentation status
        await self.presentation_store.update(
            presentation_id=presentation_id,
            status=PresentationStatus.SUBMITTED,
            submitted_at=datetime.now(timezone.utc),
            session_id=session_id
        )
        
        return {
            "presentation_id": presentation_id,
            "session_id": session_id,
            "status": PresentationStatus.SUBMITTED,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "verifier": verifier_did
        }
    
    async def verify_presentation(
        self,
        presentation_id: Optional[str] = None,
        presentation: Optional[Dict[str, Any]] = None,
        verification_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Verify a Verifiable Presentation
        
        Args:
            presentation_id: ID of stored presentation to verify
            presentation: Raw presentation to verify (if not stored)
            verification_options: Additional verification options
            
        Returns:
            Verification result
        """
        # Get presentation
        if presentation_id:
            presentation_record = await self.presentation_store.get(presentation_id)
            if not presentation_record:
                raise ValueError(f"Presentation {presentation_id} not found")
            presentation = presentation_record.presentation
        elif not presentation:
            raise ValueError("Either presentation_id or presentation must be provided")
        
        verification_results = {
            "valid": True,
            "checks": {},
            "errors": []
        }
        
        # 1. Verify presentation proof
        try:
            proof_result = await self._verify_presentation_proof(
                presentation,
                verification_options
            )
            verification_results["checks"]["presentation_proof"] = proof_result["valid"]
            if not proof_result["valid"]:
                verification_results["valid"] = False
                verification_results["errors"].append("Invalid presentation proof")
        except Exception as e:
            verification_results["valid"] = False
            verification_results["checks"]["presentation_proof"] = False
            verification_results["errors"].append(f"Proof verification failed: {str(e)}")
        
        # 2. Verify each credential
        credentials = presentation.get("verifiableCredential", [])
        credential_results = []
        
        for i, cred_data in enumerate(credentials):
            cred_result = {
                "index": i,
                "valid": True,
                "checks": {}
            }
            
            # Extract credential
            if isinstance(cred_data, dict) and "credential" in cred_data:
                # Selective disclosure case
                credential = cred_data["credential"]
                proof = cred_data.get("proof")
            else:
                credential = cred_data
                proof = None
            
            # Verify credential
            try:
                cred_verification = await self._verify_credential(
                    credential,
                    proof,
                    verification_options
                )
                cred_result["checks"] = cred_verification
                cred_result["valid"] = all(cred_verification.values())
                
                if not cred_result["valid"]:
                    verification_results["valid"] = False
                    
            except Exception as e:
                cred_result["valid"] = False
                cred_result["error"] = str(e)
                verification_results["valid"] = False
                verification_results["errors"].append(f"Credential {i} verification failed: {str(e)}")
            
            credential_results.append(cred_result)
        
        verification_results["credentials"] = credential_results
        
        # 3. Apply verification policies
        if verification_options and "policy" in verification_options:
            policy_result = await self._apply_verification_policy(
                presentation,
                verification_options["policy"]
            )
            verification_results["checks"]["policy"] = policy_result["valid"]
            if not policy_result["valid"]:
                verification_results["valid"] = False
                verification_results["errors"].extend(policy_result.get("errors", []))
        
        # Update statistics
        if verification_results["valid"]:
            self.total_verified += 1
        else:
            self.total_rejected += 1
        
        # Store verification result if presentation was stored
        if presentation_id:
            await self.presentation_store.record_verification(
                presentation_id=presentation_id,
                verifier=verification_options.get("verifier_did", "unknown"),
                result=VerificationResult.VALID if verification_results["valid"] else VerificationResult.INVALID,
                details=verification_results
            )
            
            # Update presentation status
            await self.presentation_store.update(
                presentation_id=presentation_id,
                status=PresentationStatus.VERIFIED if verification_results["valid"] else PresentationStatus.REJECTED,
                verified_at=datetime.now(timezone.utc)
            )
        
        return verification_results
    
    async def get_presentation(self, presentation_id: str) -> Optional[Dict[str, Any]]:
        """Get presentation by ID"""
        record = await self.presentation_store.get(presentation_id)
        if not record:
            return None
        
        return self._format_presentation(record)
    
    async def list_presentations(
        self,
        holder_did: Optional[str] = None,
        verifier_did: Optional[str] = None,
        status: Optional[PresentationStatus] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List presentations with filters"""
        presentations = await self.presentation_store.list_presentations(
            holder_did=holder_did,
            verifier_did=verifier_did,
            status=status,
            limit=limit,
            offset=offset
        )
        
        return [self._format_presentation(p) for p in presentations]
    
    async def revoke_presentation(
        self,
        presentation_id: str,
        reason: str,
        revoker_did: str
    ) -> Dict[str, Any]:
        """
        Revoke a presentation
        
        Args:
            presentation_id: ID of presentation to revoke
            reason: Reason for revocation
            revoker_did: DID of the revoker
            
        Returns:
            Revocation details
        """
        # Get presentation
        presentation = await self.presentation_store.get(presentation_id)
        if not presentation:
            raise ValueError(f"Presentation {presentation_id} not found")
        
        # Check permissions
        if presentation.holder_did != revoker_did:
            raise PermissionError("Only the holder can revoke their presentation")
        
        # Update status
        await self.presentation_store.update(
            presentation_id=presentation_id,
            status=PresentationStatus.EXPIRED,
            revoked_at=datetime.now(timezone.utc),
            revocation_reason=reason
        )
        
        # If there's an active session, update it
        if presentation.session_id:
            await self.session_manager.update_session(
                presentation.session_id,
                {"status": "revoked", "revocation_reason": reason}
            )
        
        return {
            "presentation_id": presentation_id,
            "status": PresentationStatus.EXPIRED,
            "revoked_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason
        }
    
    # Helper methods
    
    async def _get_credential(self, credential_id: str) -> Optional[Dict[str, Any]]:
        """Get credential from credential service"""
        try:
            response = await self.http_client.get(
                f"{self.credential_service_url}/api/v1/credentials/{credential_id}"
            )
            
            if response.status_code == 200:
                return response.json()
            
            return None
            
        except Exception as e:
            print(f"Failed to get credential: {str(e)}")
            return None
    
    async def _generate_selective_disclosure_proof(
        self,
        credential: Dict[str, Any],
        disclosed_fields: List[str],
        nonce: str
    ) -> Dict[str, Any]:
        """Generate selective disclosure proof"""
        try:
            response = await self.http_client.post(
                f"{self.zkp_service_url}/api/v1/proofs/selective-disclosure",
                json={
                    "credential": credential,
                    "disclosed_attributes": disclosed_fields,
                    "nonce": nonce
                }
            )
            
            if response.status_code == 200:
                return response.json()
            
            raise RuntimeError(f"Failed to generate selective disclosure: {response.text}")
            
        except Exception as e:
            print(f"Failed to generate selective disclosure: {str(e)}")
            raise
    
    async def _generate_presentation_proof(
        self,
        presentation: Dict[str, Any],
        holder_did: str,
        challenge: Optional[str],
        domain: Optional[str],
        options: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Generate proof for presentation"""
        # Get holder's key
        key_response = await self.http_client.get(
            f"{self.did_service_url}/api/v1/dids/{holder_did}"
        )
        
        if key_response.status_code != 200:
            raise RuntimeError(f"Failed to get holder DID: {key_response.text}")
        
        did_doc = key_response.json()
        verification_method = did_doc.get("verificationMethod", [{}])[0]
        
        # Create proof
        proof = {
            "type": "Ed25519Signature2020",
            "created": datetime.now(timezone.utc).isoformat(),
            "verificationMethod": verification_method.get("id"),
            "proofPurpose": "authentication"
        }
        
        if challenge:
            proof["challenge"] = challenge
        
        if domain:
            proof["domain"] = domain
        
        # Sign presentation
        # This would use actual cryptographic signing
        proof["proofValue"] = base64.b64encode(
            f"{presentation['id']}:{holder_did}:{challenge or ''}".encode()
        ).decode()
        
        return proof
    
    async def _verify_presentation_proof(
        self,
        presentation: Dict[str, Any],
        options: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Verify presentation proof"""
        proof = presentation.get("proof", {})
        
        # Verify challenge if provided
        if options and "challenge" in options:
            if proof.get("challenge") != options["challenge"]:
                return {"valid": False, "error": "Challenge mismatch"}
        
        # Verify domain if provided
        if options and "domain" in options:
            if proof.get("domain") != options["domain"]:
                return {"valid": False, "error": "Domain mismatch"}
        
        # Verify signature
        # This would use actual cryptographic verification
        # For now, basic validation
        if not proof.get("proofValue"):
            return {"valid": False, "error": "Missing proof value"}
        
        return {"valid": True}
    
    async def _verify_credential(
        self,
        credential: Dict[str, Any],
        proof: Optional[Dict[str, Any]],
        options: Optional[Dict[str, Any]]
    ) -> Dict[str, bool]:
        """Verify individual credential"""
        checks = {
            "signature": False,
            "not_expired": False,
            "not_revoked": False,
            "trusted_issuer": False
        }
        
        # Check signature
        try:
            if proof:
                # Verify ZKP proof
                verify_response = await self.http_client.post(
                    f"{self.zkp_service_url}/api/v1/proofs/selective-disclosure/verify",
                    json={
                        "proof": proof,
                        "public_key": {},  # Would get from issuer
                        "credential": credential,
                        "nonce": proof.get("nonce")
                    }
                )
                checks["signature"] = verify_response.status_code == 200 and verify_response.json().get("valid", False)
            else:
                # Verify regular credential
                verify_response = await self.http_client.post(
                    f"{self.credential_service_url}/api/v1/credentials/verify",
                    json={"credential": credential}
                )
                checks["signature"] = verify_response.status_code == 200 and verify_response.json().get("valid", False)
        except Exception:
            checks["signature"] = False
        
        # Check expiration
        expiration_date = credential.get("expirationDate")
        if expiration_date:
            exp_dt = datetime.fromisoformat(expiration_date.replace('Z', '+00:00'))
            checks["not_expired"] = exp_dt > datetime.now(timezone.utc)
        else:
            checks["not_expired"] = True
        
        # Check revocation
        cred_id = credential.get("id")
        if cred_id:
            try:
                status_response = await self.http_client.get(
                    f"{self.credential_service_url}/api/v1/credentials/{cred_id}/status"
                )
                if status_response.status_code == 200:
                    status = status_response.json()
                    checks["not_revoked"] = status.get("status") != "revoked"
                else:
                    checks["not_revoked"] = True
            except Exception:
                checks["not_revoked"] = True
        else:
            checks["not_revoked"] = True
        
        # Check trusted issuer
        issuer = credential.get("issuer")
        if isinstance(issuer, dict):
            issuer = issuer.get("id")
        
        if issuer:
            checks["trusted_issuer"] = issuer in self.trusted_issuers or not self.trusted_issuers
        else:
            checks["trusted_issuer"] = False
        
        return checks
    
    async def _apply_verification_policy(
        self,
        presentation: Dict[str, Any],
        policy_name: str
    ) -> Dict[str, Any]:
        """Apply verification policy"""
        if policy_name not in self.verification_policies:
            return {"valid": True}  # No policy to apply
        
        policy = self.verification_policies[policy_name]
        errors = []
        
        # Check minimum credentials
        min_creds = policy.get("minimum_credentials", 0)
        if len(presentation.get("verifiableCredential", [])) < min_creds:
            errors.append(f"Requires at least {min_creds} credentials")
        
        # Check required credential types
        required_types = policy.get("required_credential_types", [])
        presented_types = set()
        for cred in presentation.get("verifiableCredential", []):
            if isinstance(cred, dict) and "type" in cred:
                presented_types.update(cred["type"])
        
        for req_type in required_types:
            if req_type not in presented_types:
                errors.append(f"Missing required credential type: {req_type}")
        
        # Check required issuers
        required_issuers = policy.get("required_issuers", [])
        if required_issuers:
            presented_issuers = set()
            for cred in presentation.get("verifiableCredential", []):
                issuer = cred.get("issuer")
                if isinstance(issuer, dict):
                    issuer = issuer.get("id")
                if issuer:
                    presented_issuers.add(issuer)
            
            if not any(issuer in required_issuers for issuer in presented_issuers):
                errors.append("No credentials from required issuers")
        
        return {
            "valid": len(errors) == 0,
            "errors": errors
        }
    
    def _format_presentation(self, record: Any) -> Dict[str, Any]:
        """Format presentation record for API response"""
        return {
            "id": record.presentation_id,
            "holder": record.holder_did,
            "verifier": record.verifier_did,
            "presentation": record.presentation,
            "status": record.status,
            "created_at": record.created_at.isoformat(),
            "submitted_at": record.submitted_at.isoformat() if record.submitted_at else None,
            "verified_at": record.verified_at.isoformat() if record.verified_at else None,
            "session_id": record.session_id
        }
    
    async def check_credential_service(self) -> bool:
        """Check if credential service is accessible"""
        try:
            response = await self.http_client.get(
                f"{self.credential_service_url}/health"
            )
            return response.status_code == 200
        except Exception:
            return False
    
    async def check_zkp_service(self) -> bool:
        """Check if ZKP service is accessible"""
        try:
            response = await self.http_client.get(
                f"{self.zkp_service_url}/health"
            )
            return response.status_code == 200
        except Exception:
            return False
    
    async def check_did_service(self) -> bool:
        """Check if DID service is accessible"""
        try:
            response = await self.http_client.get(
                f"{self.did_service_url}/health"
            )
            return response.status_code == 200
        except Exception:
            return False
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get presentation service statistics"""
        stats = await self.presentation_store.get_statistics()
        
        return {
            "total_created": self.total_created,
            "total_verified": self.total_verified,
            "total_rejected": self.total_rejected,
            "database_stats": stats,
            "verification_acceptance_rate": (
                self.total_verified / (self.total_verified + self.total_rejected)
            ) if (self.total_verified + self.total_rejected) > 0 else 0
        } 