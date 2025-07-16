from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass
import json
import hashlib
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

@dataclass
class ZKPProof:
    """Represents a zero-knowledge proof"""
    proof_type: str
    created: datetime
    verification_method: str
    proof_purpose: str
    proof_value: str
    revealed_attributes: List[str]
    challenge: Optional[str] = None
    domain: Optional[str] = None

class ZKPManager:
    """Manager for creating and verifying zero-knowledge proofs"""
    
    def __init__(self):
        self.proof_types = {
            "BbsBlsSignature2020": self._create_bbs_proof,
            "CLSignature2019": self._create_cl_proof,
            "GrothProof2021": self._create_groth_proof
        }
        self.verifiers = {
            "BbsBlsSignature2020": self._verify_bbs_proof,
            "CLSignature2019": self._verify_cl_proof,
            "GrothProof2021": self._verify_groth_proof
        }
    
    def create_proof(self, credential: Dict[str, Any], 
                    revealed_attributes: List[str],
                    proof_type: str = "BbsBlsSignature2020",
                    options: Optional[Dict[str, Any]] = None) -> ZKPProof:
        """Create a ZKP for selective disclosure"""
        options = options or {}
        
        if proof_type not in self.proof_types:
            raise ValueError(f"Unsupported proof type: {proof_type}")
        
        logger.info(f"Creating {proof_type} proof with {len(revealed_attributes)} revealed attributes")
        
        # Create the proof using the appropriate method
        proof_creator = self.proof_types[proof_type]
        proof = proof_creator(credential, revealed_attributes, options)
        
        return proof
    
    def verify_proof(self, proof: ZKPProof, 
                    public_key: bytes,
                    options: Optional[Dict[str, Any]] = None) -> bool:
        """Verify a zero-knowledge proof"""
        options = options or {}
        
        if proof.proof_type not in self.verifiers:
            logger.error(f"Unsupported proof type: {proof.proof_type}")
            return False
        
        logger.info(f"Verifying {proof.proof_type} proof")
        
        # Verify using the appropriate method
        verifier = self.verifiers[proof.proof_type]
        result = verifier(proof, public_key, options)
        
        return result
    
    def _create_bbs_proof(self, credential: Dict[str, Any],
                         revealed_attributes: List[str],
                         options: Dict[str, Any]) -> ZKPProof:
        """Create a BBS+ signature proof"""
        from .bbs_plus import BBSPlusSignature
        
        # Initialize BBS+ signature scheme
        bbs = BBSPlusSignature()
        
        # Get all attributes from credential
        all_attributes = self._extract_attributes(credential)
        
        # Create commitment for hidden attributes
        hidden_attributes = [attr for attr in all_attributes 
                           if attr not in revealed_attributes]
        
        # Generate proof
        proof_value = bbs.create_proof(
            credential=credential,
            revealed_attributes=revealed_attributes,
            hidden_attributes=hidden_attributes,
            nonce=options.get("nonce", self._generate_nonce())
        )
        
        # Create proof object
        proof = ZKPProof(
            proof_type="BbsBlsSignature2020",
            created=datetime.utcnow(),
            verification_method=options.get("verification_method", ""),
            proof_purpose=options.get("proof_purpose", "assertionMethod"),
            proof_value=proof_value,
            revealed_attributes=revealed_attributes,
            challenge=options.get("challenge"),
            domain=options.get("domain")
        )
        
        return proof
    
    def _verify_bbs_proof(self, proof: ZKPProof,
                         public_key: bytes,
                         options: Dict[str, Any]) -> bool:
        """Verify a BBS+ signature proof"""
        from .bbs_plus import BBSPlusSignature
        
        bbs = BBSPlusSignature()
        
        # Verify the proof
        result = bbs.verify_proof(
            proof_value=proof.proof_value,
            revealed_attributes=proof.revealed_attributes,
            public_key=public_key,
            challenge=proof.challenge
        )
        
        return result
    
    def _create_cl_proof(self, credential: Dict[str, Any],
                        revealed_attributes: List[str],
                        options: Dict[str, Any]) -> ZKPProof:
        """Create a CL (Camenisch-Lysyanskaya) signature proof"""
        # Placeholder for CL signature implementation
        # This would use the Hyperledger Indy crypto library
        
        logger.info("Creating CL signature proof (placeholder)")
        
        proof_value = hashlib.sha256(
            json.dumps(credential).encode()
        ).hexdigest()
        
        proof = ZKPProof(
            proof_type="CLSignature2019",
            created=datetime.utcnow(),
            verification_method=options.get("verification_method", ""),
            proof_purpose="assertionMethod",
            proof_value=proof_value,
            revealed_attributes=revealed_attributes
        )
        
        return proof
    
    def _verify_cl_proof(self, proof: ZKPProof,
                        public_key: bytes,
                        options: Dict[str, Any]) -> bool:
        """Verify a CL signature proof"""
        # Placeholder verification
        logger.info("Verifying CL signature proof (placeholder)")
        return True
    
    def _create_groth_proof(self, credential: Dict[str, Any],
                           revealed_attributes: List[str],
                           options: Dict[str, Any]) -> ZKPProof:
        """Create a Groth16 zkSNARK proof"""
        # Placeholder for Groth16 implementation
        # This would use a zkSNARK library like py-ecc
        
        logger.info("Creating Groth16 proof (placeholder)")
        
        proof_value = hashlib.sha256(
            json.dumps(credential).encode()
        ).hexdigest()
        
        proof = ZKPProof(
            proof_type="GrothProof2021",
            created=datetime.utcnow(),
            verification_method=options.get("verification_method", ""),
            proof_purpose="assertionMethod",
            proof_value=proof_value,
            revealed_attributes=revealed_attributes
        )
        
        return proof
    
    def _verify_groth_proof(self, proof: ZKPProof,
                           public_key: bytes,
                           options: Dict[str, Any]) -> bool:
        """Verify a Groth16 proof"""
        # Placeholder verification
        logger.info("Verifying Groth16 proof (placeholder)")
        return True
    
    def _extract_attributes(self, credential: Dict[str, Any]) -> List[str]:
        """Extract all attribute names from a credential"""
        attributes = []
        
        # Get attributes from credentialSubject
        if "credentialSubject" in credential:
            subject = credential["credentialSubject"]
            if isinstance(subject, dict):
                attributes.extend(subject.keys())
        
        return attributes
    
    def _generate_nonce(self) -> str:
        """Generate a random nonce"""
        import secrets
        return secrets.token_hex(16)
    
    def create_presentation(self, credentials: List[Dict[str, Any]],
                          revealed_attributes_map: Dict[str, List[str]],
                          holder_did: str,
                          options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create a verifiable presentation with selective disclosure"""
        options = options or {}
        
        presentation = {
            "@context": [
                "https://www.w3.org/2018/credentials/v1",
                "https://w3id.org/security/bbs/v1"
            ],
            "type": ["VerifiablePresentation"],
            "holder": holder_did,
            "verifiableCredential": []
        }
        
        # Process each credential
        for i, credential in enumerate(credentials):
            credential_id = credential.get("id", f"credential-{i}")
            revealed_attrs = revealed_attributes_map.get(credential_id, [])
            
            # Create proof for this credential
            proof = self.create_proof(
                credential=credential,
                revealed_attributes=revealed_attrs,
                options=options
            )
            
            # Create derived credential with only revealed attributes
            derived_credential = self._create_derived_credential(
                original_credential=credential,
                revealed_attributes=revealed_attrs,
                proof=proof
            )
            
            presentation["verifiableCredential"].append(derived_credential)
        
        # Add presentation proof
        presentation["proof"] = {
            "type": "BbsBlsSignatureProof2020",
            "created": datetime.utcnow().isoformat() + "Z",
            "proofPurpose": "authentication",
            "verificationMethod": f"{holder_did}#key-1",
            "challenge": options.get("challenge"),
            "domain": options.get("domain")
        }
        
        return presentation
    
    def _create_derived_credential(self, original_credential: Dict[str, Any],
                                 revealed_attributes: List[str],
                                 proof: ZKPProof) -> Dict[str, Any]:
        """Create a derived credential with only revealed attributes"""
        derived = {
            "@context": original_credential.get("@context", []),
            "id": original_credential.get("id"),
            "type": original_credential.get("type", []),
            "issuer": original_credential.get("issuer"),
            "issuanceDate": original_credential.get("issuanceDate"),
            "credentialSubject": {}
        }
        
        # Copy only revealed attributes
        original_subject = original_credential.get("credentialSubject", {})
        for attr in revealed_attributes:
            if attr in original_subject:
                derived["credentialSubject"][attr] = original_subject[attr]
        
        # Add the ZKP proof
        derived["proof"] = {
            "type": proof.proof_type,
            "created": proof.created.isoformat() + "Z",
            "verificationMethod": proof.verification_method,
            "proofPurpose": proof.proof_purpose,
            "proofValue": proof.proof_value
        }
        
        return derived 