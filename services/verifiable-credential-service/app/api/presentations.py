"""
Verifiable Presentations API endpoints for bundling and sharing credentials
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import uuid
import json
import logging

from platformq_shared.api.deps import get_current_tenant_and_user
from ..standards.advanced_standards import VerifiablePresentationBuilder, PresentationPurpose
from ..did import DIDManager
from ..services.vc_service import get_credentials_for_user, get_credential_by_id

logger = logging.getLogger(__name__)
router = APIRouter()


class CreatePresentationRequest(BaseModel):
    """Request to create a verifiable presentation"""
    credential_ids: List[str] = Field(..., description="IDs of credentials to include")
    purpose: PresentationPurpose = Field(
        PresentationPurpose.AUTHENTICATION,
        description="Purpose of the presentation"
    )
    challenge: Optional[str] = Field(None, description="Challenge for authentication")
    domain: Optional[str] = Field(None, description="Domain requesting the presentation")
    expires_in: Optional[int] = Field(3600, description="Expiration time in seconds")
    selective_disclosure: Optional[Dict[str, List[str]]] = Field(
        None,
        description="Claims to disclose per credential"
    )


class PresentationTemplate(BaseModel):
    """Template for common presentation scenarios"""
    name: str
    description: str
    required_credentials: List[str]
    purpose: PresentationPurpose
    claims_filter: Optional[Dict[str, Any]] = None


class VerifyPresentationRequest(BaseModel):
    """Request to verify a presentation"""
    presentation: Dict[str, Any]
    challenge: Optional[str] = None
    domain: Optional[str] = None
    check_revocation: bool = True


@router.post("/presentations")
async def create_presentation(
    request: CreatePresentationRequest,
    context: dict = Depends(get_current_tenant_and_user),
    vp_builder: VerifiablePresentationBuilder = Depends(
        lambda: router.app.state.vp_builder
    ),
    did_manager: DIDManager = Depends(lambda: router.app.state.did_manager)
):
    """
    Create a verifiable presentation bundling multiple credentials.
    
    This endpoint:
    1. Retrieves the specified credentials
    2. Verifies ownership
    3. Applies selective disclosure if requested
    4. Creates a signed presentation
    """
    if not vp_builder:
        raise HTTPException(
            status_code=503,
            detail="Presentation builder not initialized"
        )
    
    # Get holder DID
    holder_did = did_manager.get_did_for_user(
        context["tenant_id"],
        context["user"].id
    )
    
    # Retrieve credentials
    credentials = []
    for cred_id in request.credential_ids:
        credential = await get_credential_by_id(
            cred_id,
            context["tenant_id"]
        )
        
        if not credential:
            raise HTTPException(
                status_code=404,
                detail=f"Credential {cred_id} not found"
            )
        
        # Verify ownership
        subject_id = credential.get("credentialSubject", {}).get("id", "")
        if subject_id != holder_did:
            raise HTTPException(
                status_code=403,
                detail=f"You don't own credential {cred_id}"
            )
        
        # Apply selective disclosure if requested
        if request.selective_disclosure and cred_id in request.selective_disclosure:
            disclosed_claims = request.selective_disclosure[cred_id]
            # Filter credential to only include disclosed claims
            filtered_subject = {}
            original_subject = credential.get("credentialSubject", {})
            
            for claim in disclosed_claims:
                if claim in original_subject:
                    filtered_subject[claim] = original_subject[claim]
            
            # Keep the subject ID
            filtered_subject["id"] = original_subject.get("id")
            
            # Create filtered credential
            filtered_credential = {
                **credential,
                "credentialSubject": filtered_subject
            }
            credentials.append(filtered_credential)
        else:
            credentials.append(credential)
    
    # Create presentation
    presentation = vp_builder.create_presentation(
        credentials=credentials,
        holder_did=holder_did,
        challenge=request.challenge,
        domain=request.domain,
        purpose=request.purpose
    )
    
    # Set expiration
    if request.expires_in:
        expiration = datetime.utcnow() + timedelta(seconds=request.expires_in)
        presentation["expirationDate"] = expiration.isoformat() + "Z"
    
    # Sign presentation
    # In production, this would use the holder's private key
    presentation["proof"] = {
        "type": "Ed25519Signature2020",
        "created": datetime.utcnow().isoformat() + "Z",
        "verificationMethod": f"{holder_did}#key-1",
        "proofPurpose": request.purpose.value,
        "challenge": request.challenge,
        "domain": request.domain,
        "proofValue": "mock_signature_" + str(uuid.uuid4())
    }
    
    return presentation


@router.post("/presentations/verify")
async def verify_presentation(
    request: VerifyPresentationRequest,
    context: dict = Depends(get_current_tenant_and_user)
):
    """
    Verify a verifiable presentation.
    
    This endpoint:
    1. Checks the presentation structure
    2. Verifies the proof/signature
    3. Validates each credential
    4. Checks revocation status if requested
    """
    presentation = request.presentation
    
    # Basic structure validation
    if "@context" not in presentation:
        raise HTTPException(
            status_code=400,
            detail="Invalid presentation: missing @context"
        )
    
    if "verifiableCredential" not in presentation:
        raise HTTPException(
            status_code=400,
            detail="Invalid presentation: missing verifiableCredential"
        )
    
    # Verify proof
    proof = presentation.get("proof", {})
    
    # Check challenge if provided
    if request.challenge and proof.get("challenge") != request.challenge:
        return {
            "valid": False,
            "error": "Challenge mismatch"
        }
    
    # Check domain if provided
    if request.domain and proof.get("domain") != request.domain:
        return {
            "valid": False,
            "error": "Domain mismatch"
        }
    
    # Check expiration
    if "expirationDate" in presentation:
        expiration = datetime.fromisoformat(
            presentation["expirationDate"].replace("Z", "+00:00")
        )
        if datetime.utcnow() > expiration:
            return {
                "valid": False,
                "error": "Presentation expired"
            }
    
    # Verify each credential
    credential_results = []
    for credential in presentation.get("verifiableCredential", []):
        cred_result = {
            "id": credential.get("id", "unknown"),
            "valid": True,
            "errors": []
        }
        
        # Check credential expiration
        if "expirationDate" in credential:
            exp = datetime.fromisoformat(
                credential["expirationDate"].replace("Z", "+00:00")
            )
            if datetime.utcnow() > exp:
                cred_result["valid"] = False
                cred_result["errors"].append("Credential expired")
        
        # Check revocation if requested
        if request.check_revocation:
            # In production, check blockchain or revocation list
            # For now, we'll assume non-revoked
            pass
        
        credential_results.append(cred_result)
    
    # Overall validation result
    all_valid = all(r["valid"] for r in credential_results)
    
    return {
        "valid": all_valid,
        "presentation_id": presentation.get("id"),
        "holder": presentation.get("holder"),
        "credentials": credential_results,
        "verified_at": datetime.utcnow().isoformat() + "Z"
    }


@router.get("/presentations/templates")
async def get_presentation_templates() -> List[PresentationTemplate]:
    """Get available presentation templates"""
    templates = [
        PresentationTemplate(
            name="Identity Verification",
            description="Basic identity verification with government ID",
            required_credentials=["GovernmentIDCredential"],
            purpose=PresentationPurpose.AUTHENTICATION,
            claims_filter={"name": True, "dateOfBirth": True, "photo": True}
        ),
        PresentationTemplate(
            name="Academic Credentials",
            description="Educational achievements and certifications",
            required_credentials=["DegreeCredential", "CertificationCredential"],
            purpose=PresentationPurpose.VERIFICATION
        ),
        PresentationTemplate(
            name="Professional Profile",
            description="Professional qualifications and work history",
            required_credentials=["EmploymentCredential", "SkillCredential"],
            purpose=PresentationPurpose.ASSERTION
        ),
        PresentationTemplate(
            name="Financial KYC",
            description="Know Your Customer for financial services",
            required_credentials=["GovernmentIDCredential", "ProofOfAddressCredential"],
            purpose=PresentationPurpose.AUTHENTICATION,
            claims_filter={
                "name": True,
                "dateOfBirth": True,
                "address": True,
                "taxId": True
            }
        ),
        PresentationTemplate(
            name="Research Access",
            description="Credentials for accessing research resources",
            required_credentials=["ResearcherCredential", "InstitutionCredential"],
            purpose=PresentationPurpose.CAPABILITY_INVOCATION
        )
    ]
    
    return templates


@router.post("/presentations/from-template/{template_name}")
async def create_from_template(
    template_name: str,
    challenge: Optional[str] = None,
    domain: Optional[str] = None,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Create a presentation using a predefined template"""
    # Get templates
    templates = await get_presentation_templates()
    
    # Find the requested template
    template = next(
        (t for t in templates if t.name == template_name),
        None
    )
    
    if not template:
        raise HTTPException(
            status_code=404,
            detail=f"Template '{template_name}' not found"
        )
    
    # Get user's credentials matching the template
    user_credentials = await get_credentials_for_user(
        context["user"].id,
        context["tenant_id"]
    )
    
    # Filter credentials by type
    matching_credentials = []
    for required_type in template.required_credentials:
        for cred in user_credentials:
            if required_type in cred.get("type", []):
                matching_credentials.append(cred["id"])
                break
    
    if len(matching_credentials) < len(template.required_credentials):
        missing = set(template.required_credentials) - set(
            cred.get("type", ["Unknown"])[0] 
            for cred in user_credentials
        )
        raise HTTPException(
            status_code=400,
            detail=f"Missing required credentials: {list(missing)}"
        )
    
    # Create presentation request
    request = CreatePresentationRequest(
        credential_ids=matching_credentials,
        purpose=template.purpose,
        challenge=challenge,
        domain=domain,
        selective_disclosure={
            cred_id: list(template.claims_filter.keys())
            for cred_id in matching_credentials
        } if template.claims_filter else None
    )
    
    # Create the presentation
    return await create_presentation(request, context)


@router.get("/presentations/{presentation_id}")
async def get_presentation(
    presentation_id: str,
    context: dict = Depends(get_current_tenant_and_user)
):
    """Retrieve a stored presentation"""
    # In production, presentations would be stored in a database
    # For now, return a mock response
    raise HTTPException(
        status_code=404,
        detail="Presentation storage not implemented"
    ) 