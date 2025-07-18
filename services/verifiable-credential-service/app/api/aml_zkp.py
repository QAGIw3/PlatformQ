"""
AML Zero-Knowledge Proof API Endpoints
Handles AML compliance credential creation and ZK proof generation/verification
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel
import logging

from app.zkp.aml_zkp import (
    AMLZeroKnowledgeProof,
    AMLAttribute,
    AMLCredential,
    AMLProof
)
from app.zkp.zkp_manager import ZKPManager
from app.zkp.selective_disclosure import SelectiveDisclosure
from app.did.did_manager import DIDManager
from app.api.deps import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter()


# Request/Response Models
class CreateAMLCredentialRequest(BaseModel):
    holder_did: str
    issuer_did: str
    credential_type: str  # risk_assessment, sanctions_check, monitoring_summary
    aml_data: Dict


class CreateAMLCredentialResponse(BaseModel):
    credential_id: str
    holder_did: str
    credential_type: str
    expires_at: str
    status: str


class GenerateAMLProofRequest(BaseModel):
    credential_id: str
    holder_did: str
    proof_template: str  # standard_aml, enhanced_aml, transaction_compliance
    custom_constraints: Optional[Dict] = None
    verifier_challenge: Optional[str] = None


class GenerateAMLProofResponse(BaseModel):
    proof_id: str
    attributes_proven: List[str]
    constraints_satisfied: Dict[str, bool]
    expires_at: str
    status: str


class VerifyAMLProofRequest(BaseModel):
    proof_id: str
    required_attributes: List[str]
    constraints: Dict
    verifier_challenge: Optional[str] = None


class VerifyAMLProofResponse(BaseModel):
    valid: bool
    reason: Optional[str] = None
    attributes_verified: Optional[List[str]] = None
    constraints_satisfied: Optional[Dict[str, bool]] = None
    verified_at: Optional[str] = None


class AMLComplianceCheckRequest(BaseModel):
    holder_did: str
    compliance_type: str  # basic, enhanced, transaction
    max_risk_score: Optional[float] = 0.7
    check_recency_hours: Optional[int] = 24
    additional_requirements: Optional[Dict] = None


class AMLComplianceCheckResponse(BaseModel):
    compliant: bool
    proof_id: Optional[str] = None
    missing_credentials: Optional[List[str]] = None
    recommendations: List[str]


# Initialize AML ZKP handler
aml_zkp_handler: Optional[AMLZeroKnowledgeProof] = None


def get_aml_zkp_handler() -> AMLZeroKnowledgeProof:
    """Get or initialize AML ZKP handler"""
    global aml_zkp_handler
    if not aml_zkp_handler:
        # These would be injected properly in production
        zkp_manager = ZKPManager()
        selective_disclosure = SelectiveDisclosure()
        did_manager = DIDManager()
        aml_zkp_handler = AMLZeroKnowledgeProof(
            zkp_manager=zkp_manager,
            selective_disclosure=selective_disclosure,
            did_manager=did_manager
        )
    return aml_zkp_handler


@router.post("/credentials/aml/create", response_model=CreateAMLCredentialResponse)
async def create_aml_credential(
    request: CreateAMLCredentialRequest,
    aml_handler: AMLZeroKnowledgeProof = Depends(get_aml_zkp_handler)
):
    """
    Create an AML compliance verifiable credential
    """
    try:
        # Create credential
        credential = AMLCredential(
            credential_id=f"aml_{request.credential_type}_{datetime.utcnow().timestamp()}",
            holder_did=request.holder_did,
            issuer_did=request.issuer_did,
            credential_type=request.credential_type,
            attributes=request.aml_data,
            issued_at=datetime.utcnow(),
            expires_at=datetime.utcnow().replace(hour=datetime.utcnow().hour + 24),
            credential_hash=f"hash_{datetime.utcnow().timestamp()}"
        )
        
        # Store credential (in production, this would be stored securely)
        # For now, we'll use in-memory storage via the handler
        
        logger.info(f"Created AML credential {credential.credential_id}")
        
        return CreateAMLCredentialResponse(
            credential_id=credential.credential_id,
            holder_did=credential.holder_did,
            credential_type=credential.credential_type,
            expires_at=credential.expires_at.isoformat(),
            status="created"
        )
        
    except ValueError as e:
        logger.error(f"Invalid request: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating AML credential: {e}")
        raise HTTPException(status_code=500, detail="Failed to create credential")


@router.post("/zkp/aml/generate-proof", response_model=GenerateAMLProofResponse)
async def generate_aml_proof(
    request: GenerateAMLProofRequest,
    aml_handler: AMLZeroKnowledgeProof = Depends(get_aml_zkp_handler)
):
    """
    Generate a zero-knowledge proof of AML compliance
    """
    try:
        # Get proof template
        template = aml_handler.proof_templates.get(request.proof_template)
        if not template:
            raise ValueError(f"Unknown proof template: {request.proof_template}")
            
        # Merge custom constraints if provided
        constraints = template["constraints"].copy()
        if request.custom_constraints:
            constraints.update(request.custom_constraints)
            
        # Convert attributes to enum
        attributes_to_prove = template["required_attributes"]
        
        # Load credential (in production, from secure storage)
        # For now, mock credential based on type
        credential = AMLCredential(
            credential_id=request.credential_id,
            holder_did=request.holder_did,
            issuer_did="did:platform:compliance-service",
            credential_type="risk_assessment",
            attributes={
                "riskScore": 0.4,
                "riskLevel": "MEDIUM",
                "isSanctioned": False,
                "checkDate": datetime.utcnow().isoformat(),
                "complianceStatus": "compliant",
                "totalVolume": "50000",
                "analyticsScore": 0.85
            },
            issued_at=datetime.utcnow(),
            expires_at=datetime.utcnow().replace(hour=datetime.utcnow().hour + 24),
            credential_hash="mock_hash"
        )
        
        # Generate proof
        proof = await aml_handler.generate_aml_proof(
            holder_did=request.holder_did,
            credential=credential,
            attributes_to_prove=attributes_to_prove,
            constraints=constraints,
            verifier_challenge=request.verifier_challenge
        )
        
        # Store proof (in production, this would be stored securely)
        
        logger.info(f"Generated AML proof {proof.proof_id}")
        
        return GenerateAMLProofResponse(
            proof_id=proof.proof_id,
            attributes_proven=[attr.value for attr in proof.attributes_proven],
            constraints_satisfied=proof.constraints_satisfied,
            expires_at=proof.expires_at.isoformat(),
            status="generated"
        )
        
    except ValueError as e:
        logger.error(f"Invalid request: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error generating AML proof: {e}")
        raise HTTPException(status_code=500, detail="Failed to generate proof")


@router.post("/zkp/aml/verify-proof", response_model=VerifyAMLProofResponse)
async def verify_aml_proof(
    request: VerifyAMLProofRequest,
    aml_handler: AMLZeroKnowledgeProof = Depends(get_aml_zkp_handler)
):
    """
    Verify a zero-knowledge proof of AML compliance
    """
    try:
        # Convert attributes
        required_attributes = [
            AMLAttribute(attr) for attr in request.required_attributes
        ]
        
        # Load proof (in production, from storage)
        # For now, mock proof
        proof = AMLProof(
            proof_id=request.proof_id,
            holder_did="did:platform:user123",
            attributes_proven=required_attributes,
            proof_data={
                "credential_id": "aml_credential_123",
                "attribute_proofs": {
                    attr.value: {"proof": "...", "satisfied": True}
                    for attr in required_attributes
                },
                "timestamp": datetime.utcnow().isoformat(),
                "challenge": request.verifier_challenge
            },
            constraints_satisfied={attr.value: True for attr in required_attributes},
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow().replace(hour=datetime.utcnow().hour + 1),
            issuer_signature="mock_signature"
        )
        
        # Verify proof
        result = await aml_handler.verify_aml_proof(
            proof=proof,
            required_attributes=required_attributes,
            constraints=request.constraints,
            verifier_challenge=request.verifier_challenge
        )
        
        if result["valid"]:
            return VerifyAMLProofResponse(
                valid=True,
                attributes_verified=result.get("attributes_verified"),
                constraints_satisfied=result.get("constraints_satisfied"),
                verified_at=result.get("verified_at")
            )
        else:
            return VerifyAMLProofResponse(
                valid=False,
                reason=result.get("reason")
            )
            
    except ValueError as e:
        logger.error(f"Invalid request: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error verifying AML proof: {e}")
        raise HTTPException(status_code=500, detail="Failed to verify proof")


@router.post("/compliance/aml/check", response_model=AMLComplianceCheckResponse)
async def check_aml_compliance(
    request: AMLComplianceCheckRequest,
    aml_handler: AMLZeroKnowledgeProof = Depends(get_aml_zkp_handler)
):
    """
    Check if a user is AML compliant based on their credentials
    """
    try:
        # Determine required proof template
        template_map = {
            "basic": "standard_aml",
            "enhanced": "enhanced_aml",
            "transaction": "transaction_compliance"
        }
        
        template_name = template_map.get(request.compliance_type, "standard_aml")
        template = aml_handler.proof_templates[template_name]
        
        # Check if user has necessary credentials
        # In production, query credential store
        has_risk_assessment = True  # Mock
        has_sanctions_check = True  # Mock
        has_monitoring = request.compliance_type == "transaction"  # Mock
        
        missing_credentials = []
        if not has_risk_assessment:
            missing_credentials.append("risk_assessment")
        if not has_sanctions_check:
            missing_credentials.append("sanctions_check")
        if request.compliance_type == "transaction" and not has_monitoring:
            missing_credentials.append("transaction_monitoring")
            
        if missing_credentials:
            return AMLComplianceCheckResponse(
                compliant=False,
                missing_credentials=missing_credentials,
                recommendations=[
                    f"Obtain {cred} credential" for cred in missing_credentials
                ]
            )
            
        # Generate compliance proof
        constraints = template["constraints"].copy()
        if request.max_risk_score is not None:
            constraints["max_risk_score"] = request.max_risk_score
        if request.check_recency_hours is not None:
            constraints["check_recency_hours"] = request.check_recency_hours
            
        # Mock proof generation
        proof_id = f"compliance_proof_{datetime.utcnow().timestamp()}"
        
        return AMLComplianceCheckResponse(
            compliant=True,
            proof_id=proof_id,
            recommendations=[]
        )
        
    except Exception as e:
        logger.error(f"Error checking AML compliance: {e}")
        raise HTTPException(status_code=500, detail="Failed to check compliance")


@router.get("/zkp/aml/templates")
async def get_aml_proof_templates(
    aml_handler: AMLZeroKnowledgeProof = Depends(get_aml_zkp_handler)
):
    """
    Get available AML proof templates
    """
    templates = []
    for name, template in aml_handler.proof_templates.items():
        templates.append({
            "name": name,
            "required_attributes": [attr.value for attr in template["required_attributes"]],
            "constraints": template["constraints"],
            "description": {
                "standard_aml": "Basic AML compliance check",
                "enhanced_aml": "Enhanced due diligence compliance",
                "transaction_compliance": "Transaction monitoring compliance"
            }.get(name, "Custom AML template")
        })
        
    return {"templates": templates}


@router.post("/zkp/aml/batch-verify")
async def batch_verify_aml_proofs(
    proof_ids: List[str],
    constraints: Dict,
    aml_handler: AMLZeroKnowledgeProof = Depends(get_aml_zkp_handler)
):
    """
    Batch verify multiple AML proofs
    """
    results = []
    
    for proof_id in proof_ids:
        try:
            # Mock verification
            results.append({
                "proof_id": proof_id,
                "valid": True,
                "verified_at": datetime.utcnow().isoformat()
            })
        except Exception as e:
            results.append({
                "proof_id": proof_id,
                "valid": False,
                "error": str(e)
            })
            
    return {
        "total": len(proof_ids),
        "valid": sum(1 for r in results if r["valid"]),
        "invalid": sum(1 for r in results if not r["valid"]),
        "results": results
    } 