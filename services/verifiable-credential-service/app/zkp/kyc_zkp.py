"""
KYC Zero-Knowledge Proof Module
Implements privacy-preserving KYC verification with multiple tiers
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
import hashlib
import json
import logging
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.backends import default_backend
import base64

from app.zkp.zkp_manager import ZKPManager
from app.zkp.selective_disclosure import SelectiveDisclosure
from app.did.did_manager import DIDManager
from app.schemas.credential_schemas import CredentialSchema

logger = logging.getLogger(__name__)


class KYCLevel(Enum):
    """KYC verification levels"""
    TIER_1 = 1  # Basic - $10k limit
    TIER_2 = 2  # Enhanced - $100k limit
    TIER_3 = 3  # Institutional - Unlimited


class KYCAttribute(Enum):
    """KYC attributes that can be proven"""
    AGE_OVER_18 = "age_over_18"
    AGE_OVER_21 = "age_over_21"
    COUNTRY_NOT_SANCTIONED = "country_not_sanctioned"
    IDENTITY_VERIFIED = "identity_verified"
    ADDRESS_VERIFIED = "address_verified"
    SOURCE_OF_FUNDS_VERIFIED = "source_of_funds_verified"
    ACCREDITED_INVESTOR = "accredited_investor"
    INSTITUTIONAL_VERIFIED = "institutional_verified"
    KYC_LEVEL = "kyc_level"
    VERIFIED_DATE = "verified_date"
    EXPIRY_DATE = "expiry_date"


@dataclass
class KYCProof:
    """Zero-knowledge proof of KYC status"""
    proof_id: str
    holder_did: str
    attributes_proven: List[KYCAttribute]
    kyc_level: KYCLevel
    proof_data: Dict[str, Any]
    created_at: datetime
    expires_at: datetime
    issuer_signature: str


@dataclass
class KYCCredential:
    """KYC Verifiable Credential"""
    credential_id: str
    holder_did: str
    issuer_did: str
    kyc_level: KYCLevel
    attributes: Dict[str, Any]
    verified_at: datetime
    expires_at: datetime
    credential_hash: str
    revocation_registry: Optional[str] = None


class KYCZeroKnowledgeProof:
    """
    Handles zero-knowledge proofs for KYC verification
    Allows proving KYC status without revealing personal information
    """
    
    def __init__(
        self,
        zkp_manager: ZKPManager,
        selective_disclosure: SelectiveDisclosure,
        did_manager: DIDManager
    ):
        self.zkp_manager = zkp_manager
        self.selective_disclosure = selective_disclosure
        self.did_manager = did_manager
        
        # ZK circuits for different proofs
        self.circuits = {
            KYCAttribute.AGE_OVER_18: "age_comparison_circuit",
            KYCAttribute.AGE_OVER_21: "age_comparison_circuit",
            KYCAttribute.COUNTRY_NOT_SANCTIONED: "sanctions_check_circuit",
            KYCAttribute.KYC_LEVEL: "level_verification_circuit",
            KYCAttribute.ACCREDITED_INVESTOR: "accreditation_circuit"
        }
        
        # Proof templates
        self.proof_templates = self._initialize_proof_templates()
        
    def _initialize_proof_templates(self) -> Dict[KYCLevel, Dict]:
        """Initialize proof templates for each KYC level"""
        return {
            KYCLevel.TIER_1: {
                "required_attributes": [
                    KYCAttribute.AGE_OVER_18,
                    KYCAttribute.IDENTITY_VERIFIED,
                    KYCAttribute.COUNTRY_NOT_SANCTIONED
                ],
                "optional_attributes": [],
                "validity_period": timedelta(days=365)
            },
            KYCLevel.TIER_2: {
                "required_attributes": [
                    KYCAttribute.AGE_OVER_18,
                    KYCAttribute.IDENTITY_VERIFIED,
                    KYCAttribute.ADDRESS_VERIFIED,
                    KYCAttribute.COUNTRY_NOT_SANCTIONED,
                    KYCAttribute.SOURCE_OF_FUNDS_VERIFIED
                ],
                "optional_attributes": [KYCAttribute.ACCREDITED_INVESTOR],
                "validity_period": timedelta(days=365)
            },
            KYCLevel.TIER_3: {
                "required_attributes": [
                    KYCAttribute.IDENTITY_VERIFIED,
                    KYCAttribute.ADDRESS_VERIFIED,
                    KYCAttribute.COUNTRY_NOT_SANCTIONED,
                    KYCAttribute.SOURCE_OF_FUNDS_VERIFIED,
                    KYCAttribute.INSTITUTIONAL_VERIFIED
                ],
                "optional_attributes": [],
                "validity_period": timedelta(days=730)
            }
        }
        
    async def create_kyc_credential(
        self,
        holder_did: str,
        issuer_did: str,
        kyc_data: Dict[str, Any],
        kyc_level: KYCLevel
    ) -> KYCCredential:
        """Create a KYC verifiable credential"""
        
        # Validate required data
        template = self.proof_templates[kyc_level]
        self._validate_kyc_data(kyc_data, template["required_attributes"])
        
        # Create credential
        credential_id = f"kyc_{holder_did}_{datetime.utcnow().timestamp()}"
        
        # Hash sensitive data
        hashed_attributes = self._hash_sensitive_attributes(kyc_data)
        
        # Create credential structure
        credential_data = {
            "id": credential_id,
            "type": ["VerifiableCredential", "KYCCredential"],
            "issuer": issuer_did,
            "issuanceDate": datetime.utcnow().isoformat(),
            "expirationDate": (
                datetime.utcnow() + template["validity_period"]
            ).isoformat(),
            "credentialSubject": {
                "id": holder_did,
                "kycLevel": kyc_level.value,
                "attributes": hashed_attributes
            }
        }
        
        # Generate credential hash
        credential_hash = self._generate_credential_hash(credential_data)
        
        # Create KYC credential object
        kyc_credential = KYCCredential(
            credential_id=credential_id,
            holder_did=holder_did,
            issuer_did=issuer_did,
            kyc_level=kyc_level,
            attributes=hashed_attributes,
            verified_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + template["validity_period"],
            credential_hash=credential_hash
        )
        
        logger.info(f"Created KYC credential {credential_id} for {holder_did}")
        return kyc_credential
        
    async def generate_kyc_proof(
        self,
        holder_did: str,
        credential: KYCCredential,
        attributes_to_prove: List[KYCAttribute],
        verifier_challenge: Optional[str] = None
    ) -> KYCProof:
        """Generate zero-knowledge proof of KYC attributes"""
        
        # Validate credential is not expired
        if datetime.utcnow() > credential.expires_at:
            raise ValueError("KYC credential has expired")
            
        # Generate proof for each requested attribute
        proofs = {}
        for attribute in attributes_to_prove:
            if attribute in credential.attributes:
                proof = await self._generate_attribute_proof(
                    attribute,
                    credential.attributes[attribute],
                    verifier_challenge
                )
                proofs[attribute.value] = proof
            else:
                raise ValueError(f"Attribute {attribute.value} not in credential")
                
        # Create composite proof
        proof_id = f"kyc_proof_{datetime.utcnow().timestamp()}"
        
        proof_data = {
            "credential_id": credential.credential_id,
            "attribute_proofs": proofs,
            "kyc_level": credential.kyc_level.value,
            "proof_timestamp": datetime.utcnow().isoformat(),
            "challenge": verifier_challenge
        }
        
        # Sign the proof
        issuer_signature = await self._sign_proof(proof_data, holder_did)
        
        kyc_proof = KYCProof(
            proof_id=proof_id,
            holder_did=holder_did,
            attributes_proven=attributes_to_prove,
            kyc_level=credential.kyc_level,
            proof_data=proof_data,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(hours=24),  # Proof valid for 24h
            issuer_signature=issuer_signature
        )
        
        logger.info(f"Generated KYC proof {proof_id} for {holder_did}")
        return kyc_proof
        
    async def verify_kyc_proof(
        self,
        proof: KYCProof,
        required_attributes: List[KYCAttribute],
        min_kyc_level: KYCLevel,
        verifier_challenge: Optional[str] = None
    ) -> Dict[str, Any]:
        """Verify a zero-knowledge proof of KYC status"""
        
        # Check proof expiry
        if datetime.utcnow() > proof.expires_at:
            return {
                "valid": False,
                "reason": "Proof has expired"
            }
            
        # Verify KYC level
        if proof.kyc_level.value < min_kyc_level.value:
            return {
                "valid": False,
                "reason": f"Insufficient KYC level. Required: {min_kyc_level.name}"
            }
            
        # Verify all required attributes are proven
        proven_attributes = set(proof.attributes_proven)
        required_set = set(required_attributes)
        
        if not required_set.issubset(proven_attributes):
            missing = required_set - proven_attributes
            return {
                "valid": False,
                "reason": f"Missing required attributes: {[a.value for a in missing]}"
            }
            
        # Verify challenge if provided
        if verifier_challenge and proof.proof_data.get("challenge") != verifier_challenge:
            return {
                "valid": False,
                "reason": "Challenge mismatch"
            }
            
        # Verify signature
        signature_valid = await self._verify_signature(
            proof.proof_data,
            proof.issuer_signature,
            proof.holder_did
        )
        
        if not signature_valid:
            return {
                "valid": False,
                "reason": "Invalid signature"
            }
            
        # Verify individual attribute proofs
        for attribute in required_attributes:
            attribute_proof = proof.proof_data["attribute_proofs"].get(attribute.value)
            if not attribute_proof:
                return {
                    "valid": False,
                    "reason": f"Missing proof for {attribute.value}"
                }
                
            proof_valid = await self._verify_attribute_proof(
                attribute,
                attribute_proof
            )
            
            if not proof_valid:
                return {
                    "valid": False,
                    "reason": f"Invalid proof for {attribute.value}"
                }
                
        return {
            "valid": True,
            "kyc_level": proof.kyc_level.name,
            "attributes_verified": [a.value for a in proof.attributes_proven],
            "verified_at": datetime.utcnow().isoformat()
        }
        
    async def create_selective_disclosure_proof(
        self,
        credential: KYCCredential,
        attributes_to_disclose: List[str],
        attributes_to_prove: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a selective disclosure proof revealing only specific attributes"""
        
        # Use BBS+ for selective disclosure
        disclosed_data = {
            attr: credential.attributes.get(attr)
            for attr in attributes_to_disclose
            if attr in credential.attributes
        }
        
        # Create proofs for attributes without revealing them
        proven_attributes = {}
        for attr, condition in attributes_to_prove.items():
            if attr in credential.attributes:
                proof = await self._create_predicate_proof(
                    attr,
                    credential.attributes[attr],
                    condition
                )
                proven_attributes[attr] = proof
                
        return {
            "disclosed_attributes": disclosed_data,
            "proven_attributes": proven_attributes,
            "credential_id": credential.credential_id,
            "kyc_level": credential.kyc_level.value
        }
        
    async def revoke_kyc_credential(
        self,
        credential_id: str,
        reason: str
    ) -> Dict[str, Any]:
        """Revoke a KYC credential"""
        
        # Add to revocation registry
        revocation_entry = {
            "credential_id": credential_id,
            "revoked_at": datetime.utcnow().isoformat(),
            "reason": reason
        }
        
        # This would update an on-chain revocation registry
        # For now, we'll simulate it
        logger.info(f"Revoked KYC credential {credential_id}: {reason}")
        
        return revocation_entry
        
    async def check_credential_revocation(
        self,
        credential_id: str
    ) -> bool:
        """Check if a credential has been revoked"""
        
        # This would check an on-chain revocation registry
        # For now, return False (not revoked)
        return False
        
    def _validate_kyc_data(
        self,
        kyc_data: Dict[str, Any],
        required_attributes: List[KYCAttribute]
    ):
        """Validate that all required KYC data is present"""
        
        attribute_mapping = {
            KYCAttribute.AGE_OVER_18: "date_of_birth",
            KYCAttribute.AGE_OVER_21: "date_of_birth",
            KYCAttribute.IDENTITY_VERIFIED: "identity_document",
            KYCAttribute.ADDRESS_VERIFIED: "address_proof",
            KYCAttribute.COUNTRY_NOT_SANCTIONED: "country",
            KYCAttribute.SOURCE_OF_FUNDS_VERIFIED: "source_of_funds",
            KYCAttribute.ACCREDITED_INVESTOR: "accreditation_proof",
            KYCAttribute.INSTITUTIONAL_VERIFIED: "institution_docs"
        }
        
        for attribute in required_attributes:
            required_field = attribute_mapping.get(attribute)
            if required_field and required_field not in kyc_data:
                raise ValueError(f"Missing required field: {required_field}")
                
    def _hash_sensitive_attributes(
        self,
        kyc_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Hash sensitive attributes while keeping proof capability"""
        
        hashed_data = {}
        
        # Hash PII fields
        pii_fields = [
            "full_name", "date_of_birth", "address", 
            "identity_document", "tax_id"
        ]
        
        for field, value in kyc_data.items():
            if field in pii_fields:
                # Create salted hash
                salt = hashlib.sha256(f"{field}_{value}".encode()).hexdigest()[:16]
                hashed_value = self._hash_with_salt(str(value), salt)
                hashed_data[field] = {
                    "hash": hashed_value,
                    "salt": salt,
                    "proof_commitment": self._create_commitment(value, salt)
                }
            else:
                hashed_data[field] = value
                
        return hashed_data
        
    def _hash_with_salt(self, value: str, salt: str) -> str:
        """Create salted hash of a value"""
        
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt.encode(),
            iterations=100000,
            backend=default_backend()
        )
        
        key = kdf.derive(value.encode())
        return base64.b64encode(key).decode()
        
    def _create_commitment(self, value: Any, salt: str) -> str:
        """Create a cryptographic commitment to a value"""
        
        # Pedersen commitment or similar
        # Simplified for illustration
        commitment_data = f"{value}_{salt}_{datetime.utcnow().timestamp()}"
        return hashlib.sha256(commitment_data.encode()).hexdigest()
        
    def _generate_credential_hash(self, credential_data: Dict) -> str:
        """Generate hash of credential data"""
        
        # Canonical JSON serialization
        canonical = json.dumps(credential_data, sort_keys=True, separators=(',', ':'))
        return hashlib.sha256(canonical.encode()).hexdigest()
        
    async def _generate_attribute_proof(
        self,
        attribute: KYCAttribute,
        attribute_value: Any,
        challenge: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate proof for a specific attribute"""
        
        circuit = self.circuits.get(attribute)
        if not circuit:
            # Simple hash proof for attributes without special circuits
            proof_data = {
                "attribute": attribute.value,
                "commitment": attribute_value.get("proof_commitment") if isinstance(attribute_value, dict) else self._create_commitment(attribute_value, ""),
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if challenge:
                proof_data["challenge_response"] = hashlib.sha256(
                    f"{proof_data['commitment']}_{challenge}".encode()
                ).hexdigest()
                
            return proof_data
            
        # Use ZK circuit for complex proofs
        return await self.zkp_manager.generate_proof(
            circuit,
            {"value": attribute_value, "challenge": challenge}
        )
        
    async def _verify_attribute_proof(
        self,
        attribute: KYCAttribute,
        proof: Dict[str, Any]
    ) -> bool:
        """Verify proof for a specific attribute"""
        
        circuit = self.circuits.get(attribute)
        if not circuit:
            # Verify simple hash proof
            return "commitment" in proof and "timestamp" in proof
            
        # Verify using ZK circuit
        return await self.zkp_manager.verify_proof(circuit, proof)
        
    async def _sign_proof(
        self,
        proof_data: Dict[str, Any],
        holder_did: str
    ) -> str:
        """Sign proof data"""
        
        # This would use the DID's signing key
        # Simplified for illustration
        data_str = json.dumps(proof_data, sort_keys=True)
        signature = hashlib.sha256(
            f"{data_str}_{holder_did}".encode()
        ).hexdigest()
        
        return signature
        
    async def _verify_signature(
        self,
        proof_data: Dict[str, Any],
        signature: str,
        holder_did: str
    ) -> bool:
        """Verify proof signature"""
        
        # Recreate expected signature
        data_str = json.dumps(proof_data, sort_keys=True)
        expected_signature = hashlib.sha256(
            f"{data_str}_{holder_did}".encode()
        ).hexdigest()
        
        return signature == expected_signature
        
    async def _create_predicate_proof(
        self,
        attribute: str,
        value: Any,
        condition: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a predicate proof (e.g., age > 18 without revealing actual age)"""
        
        # This would use a ZK range proof or comparison circuit
        # Simplified for illustration
        operator = condition.get("operator", ">=")
        threshold = condition.get("value")
        
        # Create proof without revealing actual value
        proof = {
            "attribute": attribute,
            "predicate": f"{operator} {threshold}",
            "proof_type": "range_proof",
            "commitment": self._create_commitment(value, attribute),
            "verified": True  # In reality, this would be a ZK proof
        }
        
        return proof 