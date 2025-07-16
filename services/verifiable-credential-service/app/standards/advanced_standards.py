"""
Advanced Standards Compliance Module

Implements support for emerging W3C and blockchain standards including:
- Verifiable Presentations (W3C VP 2.0)
- SoulBound Tokens (SBTs) for non-transferable credentials
- Decentralized Web Nodes (DWN) for credential storage
- Credential Manifest for discovery
"""

import json
import hashlib
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import uuid
import jwt
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives import serialization
import base64

from w3c_vc import VerifiableCredential, VerifiablePresentation


class PresentationPurpose(Enum):
    AUTHENTICATION = "authentication"
    ASSERTION = "assertionMethod"
    KEY_AGREEMENT = "keyAgreement"
    CAPABILITY_INVOCATION = "capabilityInvocation"
    CAPABILITY_DELEGATION = "capabilityDelegation"


@dataclass
class PresentationRequest:
    """Represents a request for credential presentation"""
    id: str
    input_descriptors: List[Dict[str, Any]]
    purpose: PresentationPurpose
    challenge: str
    domain: str
    submission_requirements: Optional[List[Dict[str, Any]]] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class SoulBoundToken:
    """Represents a non-transferable credential token"""
    token_id: str
    credential_hash: str
    owner_address: str
    issuer_address: str
    soul_signature: str
    metadata_uri: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    revocable: bool = True
    burn_auth: str = "OWNER_OR_ISSUER"  # Who can burn the token


class VerifiablePresentationBuilder:
    """
    Builds W3C Verifiable Presentations with advanced features
    """
    
    def __init__(self, holder_did: str, holder_private_key: Optional[str] = None):
        self.holder_did = holder_did
        self.holder_private_key = holder_private_key
        
    def create_presentation(
        self,
        credentials: List[Dict[str, Any]],
        presentation_request: PresentationRequest,
        disclosed_claims: Optional[Dict[str, List[str]]] = None
    ) -> Dict[str, Any]:
        """
        Create a Verifiable Presentation from credentials
        
        Args:
            credentials: List of verifiable credentials
            presentation_request: The presentation request to fulfill
            disclosed_claims: Optional selective disclosure per credential
            
        Returns:
            A Verifiable Presentation
        """
        # Create base presentation
        vp = VerifiablePresentation({
            "@context": [
                "https://www.w3.org/2018/credentials/v1",
                "https://w3id.org/security/v2"
            ],
            "id": f"urn:uuid:{uuid.uuid4()}",
            "type": ["VerifiablePresentation"],
            "holder": self.holder_did,
            "verifiableCredential": []
        })
        
        # Process each credential
        for i, credential in enumerate(credentials):
            # Apply selective disclosure if specified
            if disclosed_claims and str(i) in disclosed_claims:
                disclosed_credential = self._apply_selective_disclosure(
                    credential,
                    disclosed_claims[str(i)]
                )
                vp.data["verifiableCredential"].append(disclosed_credential)
            else:
                vp.data["verifiableCredential"].append(credential)
                
        # Add presentation submission
        vp.data["presentation_submission"] = self._create_submission(
            presentation_request,
            credentials
        )
        
        # Add proof
        proof = self._create_presentation_proof(
            vp.data,
            presentation_request
        )
        vp.data["proof"] = proof
        
        return vp.data
        
    def _apply_selective_disclosure(
        self,
        credential: Dict[str, Any],
        disclosed_claims: List[str]
    ) -> Dict[str, Any]:
        """Apply selective disclosure to a credential"""
        # Create a derived credential with only disclosed claims
        derived = {
            "@context": credential.get("@context", []),
            "id": f"{credential['id']}#derived-{uuid.uuid4().hex[:8]}",
            "type": credential.get("type", []),
            "issuer": credential.get("issuer"),
            "issuanceDate": credential.get("issuanceDate"),
            "credentialSubject": {}
        }
        
        # Copy only disclosed claims
        original_subject = credential.get("credentialSubject", {})
        for claim in disclosed_claims:
            if claim in original_subject:
                derived["credentialSubject"][claim] = original_subject[claim]
                
        # Add selective disclosure proof
        derived["proof"] = {
            **credential.get("proof", {}),
            "type": "BbsBlsSignatureProof2020",
            "created": datetime.utcnow().isoformat() + "Z",
            "verificationMethod": self.holder_did + "#key-1",
            "proofPurpose": "assertionMethod",
            "nonce": base64.b64encode(uuid.uuid4().bytes).decode(),
            "revealedAttributes": disclosed_claims
        }
        
        return derived
        
    def _create_submission(
        self,
        request: PresentationRequest,
        credentials: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Create presentation submission descriptor"""
        descriptor_map = []
        
        for i, input_desc in enumerate(request.input_descriptors):
            # Find matching credential
            for j, credential in enumerate(credentials):
                if self._matches_descriptor(credential, input_desc):
                    descriptor_map.append({
                        "id": input_desc["id"],
                        "format": "ldp_vc",
                        "path": f"$.verifiableCredential[{j}]"
                    })
                    break
                    
        return {
            "id": f"submission-{uuid.uuid4()}",
            "definition_id": request.id,
            "descriptor_map": descriptor_map
        }
        
    def _matches_descriptor(
        self,
        credential: Dict[str, Any],
        descriptor: Dict[str, Any]
    ) -> bool:
        """Check if credential matches input descriptor"""
        # Simplified matching logic
        constraints = descriptor.get("constraints", {})
        fields = constraints.get("fields", [])
        
        for field in fields:
            path = field.get("path", [])
            # Check if required fields exist in credential
            # This is a simplified implementation
            if not self._check_json_path(credential, path):
                return False
                
        return True
        
    def _check_json_path(
        self,
        data: Dict[str, Any],
        paths: List[str]
    ) -> bool:
        """Check if any JSON path exists in data"""
        # Simplified JSON path checking
        for path in paths:
            if path.startswith("$.credentialSubject."):
                field = path.replace("$.credentialSubject.", "")
                if field in data.get("credentialSubject", {}):
                    return True
        return False
        
    def _create_presentation_proof(
        self,
        presentation: Dict[str, Any],
        request: PresentationRequest
    ) -> Dict[str, Any]:
        """Create proof for the presentation"""
        return {
            "type": "Ed25519Signature2020",
            "created": datetime.utcnow().isoformat() + "Z",
            "verificationMethod": self.holder_did + "#key-1",
            "proofPurpose": request.purpose.value,
            "challenge": request.challenge,
            "domain": request.domain,
            "proofValue": self._sign_presentation(presentation, request)
        }
        
    def _sign_presentation(
        self,
        presentation: Dict[str, Any],
        request: PresentationRequest
    ) -> str:
        """Sign the presentation (simplified)"""
        # In production, use proper LD-Proofs or JWT
        to_sign = {
            "presentation": presentation,
            "challenge": request.challenge,
            "domain": request.domain
        }
        
        if self.holder_private_key:
            # Create JWT signature
            return jwt.encode(
                to_sign,
                self.holder_private_key,
                algorithm="ES256K"
            )
        else:
            # Return mock signature for demo
            return base64.b64encode(
                hashlib.sha256(
                    json.dumps(to_sign, sort_keys=True).encode()
                ).digest()
            ).decode()


class SoulBoundTokenManager:
    """
    Manages SoulBound Tokens (SBTs) for non-transferable credentials
    """
    
    def __init__(self, blockchain_client):
        self.blockchain_client = blockchain_client
        self.sbt_registry: Dict[str, SoulBoundToken] = {}
        
    async def mint_sbt(
        self,
        credential: Dict[str, Any],
        owner_address: str,
        issuer_address: str,
        metadata_uri: Optional[str] = None
    ) -> SoulBoundToken:
        """
        Mint a new SoulBound Token for a credential
        
        Args:
            credential: The verifiable credential
            owner_address: Blockchain address of the credential holder
            issuer_address: Blockchain address of the issuer
            metadata_uri: Optional IPFS URI for credential metadata
            
        Returns:
            The minted SoulBound Token
        """
        # Generate token ID and credential hash
        token_id = f"sbt_{uuid.uuid4().hex}"
        credential_hash = hashlib.sha256(
            json.dumps(credential, sort_keys=True).encode()
        ).hexdigest()
        
        # Create soul signature (binding token to owner)
        soul_data = {
            "token_id": token_id,
            "credential_hash": credential_hash,
            "owner": owner_address,
            "issuer": issuer_address,
            "timestamp": datetime.utcnow().isoformat()
        }
        soul_signature = hashlib.sha256(
            json.dumps(soul_data, sort_keys=True).encode()
        ).hexdigest()
        
        # Create SBT
        sbt = SoulBoundToken(
            token_id=token_id,
            credential_hash=credential_hash,
            owner_address=owner_address,
            issuer_address=issuer_address,
            soul_signature=soul_signature,
            metadata_uri=metadata_uri
        )
        
        # Deploy to blockchain
        await self._deploy_sbt_contract(sbt)
        
        # Store in registry
        self.sbt_registry[token_id] = sbt
        
        return sbt
        
    async def _deploy_sbt_contract(self, sbt: SoulBoundToken):
        """Deploy SBT to blockchain (simplified)"""
        # In production, this would deploy an actual smart contract
        contract_data = {
            "tokenId": sbt.token_id,
            "owner": sbt.owner_address,
            "issuer": sbt.issuer_address,
            "credentialHash": sbt.credential_hash,
            "soulSignature": sbt.soul_signature,
            "metadataURI": sbt.metadata_uri,
            "transferable": False,  # Key SBT property
            "revocable": sbt.revocable,
            "burnAuth": sbt.burn_auth
        }
        
        if self.blockchain_client:
            await self.blockchain_client.deploy_contract(
                "SoulBoundToken",
                contract_data
            )
            
    async def verify_sbt(
        self,
        token_id: str,
        expected_owner: str
    ) -> Dict[str, Any]:
        """Verify a SoulBound Token"""
        sbt = self.sbt_registry.get(token_id)
        if not sbt:
            return {"valid": False, "error": "Token not found"}
            
        # Verify owner
        if sbt.owner_address != expected_owner:
            return {"valid": False, "error": "Owner mismatch"}
            
        # Verify soul signature
        soul_data = {
            "token_id": sbt.token_id,
            "credential_hash": sbt.credential_hash,
            "owner": sbt.owner_address,
            "issuer": sbt.issuer_address,
            "timestamp": sbt.created_at.isoformat()
        }
        expected_signature = hashlib.sha256(
            json.dumps(soul_data, sort_keys=True).encode()
        ).hexdigest()
        
        if sbt.soul_signature != expected_signature:
            return {"valid": False, "error": "Invalid soul signature"}
            
        return {
            "valid": True,
            "token": sbt,
            "metadata": {
                "created_at": sbt.created_at.isoformat(),
                "issuer": sbt.issuer_address,
                "revocable": sbt.revocable
            }
        }
        
    async def burn_sbt(
        self,
        token_id: str,
        requester_address: str
    ) -> bool:
        """Burn (revoke) a SoulBound Token"""
        sbt = self.sbt_registry.get(token_id)
        if not sbt:
            return False
            
        # Check burn authorization
        authorized = False
        if sbt.burn_auth == "OWNER_ONLY" and requester_address == sbt.owner_address:
            authorized = True
        elif sbt.burn_auth == "ISSUER_ONLY" and requester_address == sbt.issuer_address:
            authorized = True
        elif sbt.burn_auth == "OWNER_OR_ISSUER" and requester_address in [sbt.owner_address, sbt.issuer_address]:
            authorized = True
            
        if not authorized:
            return False
            
        # Remove from registry
        del self.sbt_registry[token_id]
        
        # Update blockchain
        if self.blockchain_client:
            await self.blockchain_client.burn_token(token_id)
            
        return True


class CredentialManifest:
    """
    Implements Credential Manifest for credential discovery and issuance
    """
    
    def __init__(self):
        self.manifests: Dict[str, Dict[str, Any]] = {}
        
    def create_manifest(
        self,
        issuer_did: str,
        credential_type: str,
        name: str,
        description: str,
        input_descriptors: List[Dict[str, Any]],
        output_descriptors: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Create a credential manifest for discovery"""
        manifest = {
            "id": f"manifest_{uuid.uuid4().hex}",
            "version": "0.3.0",
            "issuer": {
                "id": issuer_did,
                "name": name
            },
            "output_descriptors": output_descriptors,
            "presentation_definition": {
                "id": f"pd_{uuid.uuid4().hex}",
                "input_descriptors": input_descriptors,
                "format": {
                    "ldp_vc": {
                        "proof_type": ["Ed25519Signature2020", "BbsBlsSignature2020"]
                    },
                    "jwt_vc": {
                        "alg": ["ES256K", "ES384"]
                    }
                }
            },
            "credential_type": credential_type,
            "description": description,
            "created": datetime.utcnow().isoformat() + "Z"
        }
        
        self.manifests[manifest["id"]] = manifest
        return manifest
        
    def create_application(
        self,
        manifest_id: str,
        applicant_did: str,
        presentation: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a credential application"""
        manifest = self.manifests.get(manifest_id)
        if not manifest:
            raise ValueError("Manifest not found")
            
        return {
            "id": f"app_{uuid.uuid4().hex}",
            "spec_version": "https://identity.foundation/credential-manifest/spec/v0.3.0/",
            "manifest_id": manifest_id,
            "applicant": applicant_did,
            "presentation_submission": presentation.get("presentation_submission"),
            "verifiablePresentation": presentation,
            "created": datetime.utcnow().isoformat() + "Z"
        }
        
    def create_fulfillment(
        self,
        application_id: str,
        issued_credentials: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Create a credential fulfillment response"""
        return {
            "id": f"fulfillment_{uuid.uuid4().hex}",
            "spec_version": "https://identity.foundation/credential-manifest/spec/v0.3.0/",
            "manifest_id": application_id,
            "application_id": application_id,
            "verifiableCredential": issued_credentials,
            "created": datetime.utcnow().isoformat() + "Z"
        } 