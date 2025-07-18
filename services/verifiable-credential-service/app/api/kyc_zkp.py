"""
KYC Zero-Knowledge Proof API Endpoints
Handles KYC credential creation and ZK proof generation/verification
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel
import logging

from app.zkp.kyc_zkp import (
    KYCZeroKnowledgeProof,
    KYCLevel,
    KYCAttribute,
    KYCCredential,
    KYCProof
)
from app.zkp.zkp_manager import ZKPManager
from app.zkp.selective_disclosure import SelectiveDisclosure
from app.did.did_manager import DIDManager
from app.storage.ipfs_storage import IPFSStorage
from app.api.deps import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()


# Request/Response Models
class CreateKYCCredentialRequest(BaseModel):
    holder_did: str
    issuer_did: str
    kyc_data: Dict
    kyc_level: int


class CreateKYCCredentialResponse(BaseModel):
    credential_id: str
    holder_did: str
    kyc_level: int
    expires_at: str
    status: str


class GenerateKYCProofRequest(BaseModel):
    credential_id: str
    holder_did: str
    attributes_to_prove: List[str]
    min_kyc_level: Optional[int] = 1
    verifier_challenge: Optional[str] = None


class GenerateKYCProofResponse(BaseModel):
    proof_id: str
    attributes_proven: List[str]
    kyc_level: str
    expires_at: str
    status: str


class VerifyKYCProofRequest(BaseModel):
    proof_id: str
    required_attributes: List[str]
    min_kyc_level: int
    verifier_challenge: Optional[str] = None


class VerifyKYCProofResponse(BaseModel):
    valid: bool
    reason: Optional[str] = None
    kyc_level: Optional[str] = None
    attributes_verified: Optional[List[str]] = None
    verified_at: Optional[str] = None


# Initialize KYC ZKP handler
kyc_zkp_handler: Optional[KYCZeroKnowledgeProof] = None


def get_kyc_zkp_handler() -> KYCZeroKnowledgeProof:
    """Get or initialize KYC ZKP handler"""
    global kyc_zkp_handler
    if not kyc_zkp_handler:
        # These would be injected properly in production
        zkp_manager = ZKPManager()
        selective_disclosure = SelectiveDisclosure()
        did_manager = DIDManager()
        kyc_zkp_handler = KYCZeroKnowledgeProof(
            zkp_manager=zkp_manager,
            selective_disclosure=selective_disclosure,
            did_manager=did_manager
        )
    return kyc_zkp_handler


@router.post("/credentials/kyc/create", response_model=CreateKYCCredentialResponse)
async def create_kyc_credential(
    request: CreateKYCCredentialRequest,
    kyc_handler: KYCZeroKnowledgeProof = Depends(get_kyc_zkp_handler)
):
    """
    Create a KYC verifiable credential
    """
    try:
        # Convert level to enum
        kyc_level = KYCLevel(request.kyc_level)
        
        # Create credential
        credential = await kyc_handler.create_kyc_credential(
            holder_did=request.holder_did,
            issuer_did=request.issuer_did,
            kyc_data=request.kyc_data,
            kyc_level=kyc_level
        )
        
        # Store credential (in production, this would be stored securely)
        # For now, we'll use in-memory storage via the handler
        
        logger.info(f"Created KYC credential {credential.credential_id}")
        
        return CreateKYCCredentialResponse(
            credential_id=credential.credential_id,
            holder_did=credential.holder_did,
            kyc_level=credential.kyc_level.value,
            expires_at=credential.expires_at.isoformat(),
            status="created"
        )
        
    except ValueError as e:
        logger.error(f"Invalid request: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating KYC credential: {e}")
        raise HTTPException(status_code=500, detail="Failed to create credential")


@router.post("/zkp/kyc/generate-proof", response_model=GenerateKYCProofResponse)
async def generate_kyc_proof(
    request: GenerateKYCProofRequest,
    kyc_handler: KYCZeroKnowledgeProof = Depends(get_kyc_zkp_handler)
):
    """
    Generate a zero-knowledge proof of KYC status
    """
    try:
        # Convert string attributes to enum
        attributes_to_prove = [
            KYCAttribute(attr) for attr in request.attributes_to_prove
        ]
        
        # Load credential (in production, from secure storage)
        # For now, mock credential
        credential = KYCCredential(
            credential_id=request.credential_id,
            holder_did=request.holder_did,
            issuer_did="did:platform:compliance-service",
            kyc_level=KYCLevel(request.min_kyc_level),
            attributes={
                "age_over_18": {"hash": "...", "salt": "...", "proof_commitment": "..."},
                "identity_verified": True,
                "country_not_sanctioned": True,
                "kyc_level": request.min_kyc_level
            },
            verified_at=datetime.utcnow(),
            expires_at=datetime.utcnow().replace(year=datetime.utcnow().year + 1),
            credential_hash="mock_hash"
        )
        
        # Generate proof
        proof = await kyc_handler.generate_kyc_proof(
            holder_did=request.holder_did,
            credential=credential,
            attributes_to_prove=attributes_to_prove,
            verifier_challenge=request.verifier_challenge
        )
        
        # Store proof (in production, this would be stored securely)
        
        logger.info(f"Generated KYC proof {proof.proof_id}")
        
        return GenerateKYCProofResponse(
            proof_id=proof.proof_id,
            attributes_proven=[attr.value for attr in proof.attributes_proven],
            kyc_level=proof.kyc_level.name,
            expires_at=proof.expires_at.isoformat(),
            status="generated"
        )
        
    except ValueError as e:
        logger.error(f"Invalid request: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error generating KYC proof: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate proof")


@router.post("/zkp/kyc/verify-proof", response_model=VerifyKYCProofResponse)
async def verify_kyc_proof(
    request: VerifyKYCProofRequest,
    kyc_handler: KYCZeroKnowledgeProof = Depends(get_kyc_zkp_handler)
):
    """
    Verify a zero-knowledge proof of KYC status
    """
    try:
        # Convert attributes and level
        required_attributes = [
            KYCAttribute(attr) for attr in request.required_attributes
        ]
        min_kyc_level = KYCLevel(request.min_kyc_level)
        
        # Load proof (in production, from storage)
        # For now, mock proof
        proof = KYCProof(
            proof_id=request.proof_id,
            holder_did="did:platform:user123",
            attributes_proven=required_attributes,
            kyc_level=min_kyc_level,
            proof_data={
                "credential_id": "kyc_credential_123",
                "attribute_proofs": {
                    attr.value: {"commitment": "...", "timestamp": "..."}
                    for attr in required_attributes
                },
                "kyc_level": min_kyc_level.value,
                "proof_timestamp": datetime.utcnow().isoformat(),
                "challenge": request.verifier_challenge
            },
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow().replace(hour=datetime.utcnow().hour + 24),
            issuer_signature="mock_signature"
        )
        
        # Verify proof
        result = await kyc_handler.verify_kyc_proof(
            proof=proof,
            required_attributes=required_attributes,
            min_kyc_level=min_kyc_level,
            verifier_challenge=request.verifier_challenge
        )
        
        if result["valid"]:
            return VerifyKYCProofResponse(
                valid=True,
                kyc_level=result.get("kyc_level"),
                attributes_verified=result.get("attributes_verified"),
                verified_at=result.get("verified_at")
            )
        else:
            return VerifyKYCProofResponse(
                valid=False,
                reason=result.get("reason")
            )
            
    except ValueError as e:
        logger.error(f"Invalid request: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error verifying KYC proof: {e}")
        raise HTTPException(status_code=500, detail="Failed to verify proof")


@router.post("/zkp/kyc/selective-disclosure")
async def create_selective_disclosure_proof(
    credential_id: str,
    attributes_to_disclose: List[str],
    attributes_to_prove: Dict[str, Dict],
    holder_did: str,
    kyc_handler: KYCZeroKnowledgeProof = Depends(get_kyc_zkp_handler)
):
    """
    Create a selective disclosure proof revealing only specific attributes
    """
    try:
        # Load credential (mock for now)
        credential = KYCCredential(
            credential_id=credential_id,
            holder_did=holder_did,
            issuer_did="did:platform:compliance-service",
            kyc_level=KYCLevel.TIER_2,
            attributes={
                "country": "US",
                "age_over_18": True,
                "identity_verified": True,
                "accredited_investor": True
            },
            verified_at=datetime.utcnow(),
            expires_at=datetime.utcnow().replace(year=datetime.utcnow().year + 1),
            credential_hash="mock_hash"
        )
        
        # Create selective disclosure proof
        proof = await kyc_handler.create_selective_disclosure_proof(
            credential=credential,
            attributes_to_disclose=attributes_to_disclose,
            attributes_to_prove=attributes_to_prove
        )
        
        return {
            "proof": proof,
            "status": "created"
        }
        
    except Exception as e:
        logger.error(f"Error creating selective disclosure proof: {e}")
        raise HTTPException(status_code=500, detail="Failed to create proof")


@router.post("/zkp/kyc/revoke/{credential_id}")
async def revoke_kyc_credential(
    credential_id: str,
    reason: str,
    kyc_handler: KYCZeroKnowledgeProof = Depends(get_kyc_zkp_handler)
):
    """
    Revoke a KYC credential
    """
    try:
        result = await kyc_handler.revoke_kyc_credential(
            credential_id=credential_id,
            reason=reason
        )
        
        return {
            "credential_id": credential_id,
            "status": "revoked",
            "revocation": result
        }
        
    except Exception as e:
        logger.error(f"Error revoking credential: {e}")
        raise HTTPException(status_code=500, detail="Failed to revoke credential")


@router.get("/zkp/kyc/check-revocation/{credential_id}")
async def check_credential_revocation(
    credential_id: str,
    kyc_handler: KYCZeroKnowledgeProof = Depends(get_kyc_zkp_handler)
):
    """
    Check if a credential has been revoked
    """
    try:
        is_revoked = await kyc_handler.check_credential_revocation(credential_id)
        
        return {
            "credential_id": credential_id,
            "revoked": is_revoked
        }
        
    except Exception as e:
        logger.error(f"Error checking revocation: {e}")
        raise HTTPException(status_code=500, detail="Failed to check revocation") 