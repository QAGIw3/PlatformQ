"""
Verification API Endpoints
"""

from typing import Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.core.presentation_manager import PresentationManager, VerificationResult
from app import main

router = APIRouter()


# Request/Response models
class VerifyPresentationRequest(BaseModel):
    """Request to verify a presentation"""
    presentation: Optional[Dict[str, Any]] = Field(
        description="Presentation to verify (if not stored)",
        default=None
    )
    verification_options: Optional[Dict[str, Any]] = Field(
        description="Verification options",
        default=None,
        example={
            "challenge": "nonce-123",
            "domain": "example.com",
            "verifier_did": "did:example:verifier",
            "policy": "default"
        }
    )


class VerifyStoredPresentationRequest(BaseModel):
    """Request to verify a stored presentation"""
    verification_options: Optional[Dict[str, Any]] = Field(
        description="Verification options",
        default=None
    )


class VerificationResultResponse(BaseModel):
    """Verification result response"""
    valid: bool
    checks: Dict[str, Any]
    errors: list
    credentials: list
    timestamp: str


class VerificationPolicyResponse(BaseModel):
    """Verification policy response"""
    name: str
    description: str
    rules: Dict[str, Any]
    active: bool


# Dependency to get presentation manager
def get_presentation_manager() -> PresentationManager:
    """Get presentation manager instance"""
    if not main.presentation_manager:
        raise HTTPException(
            status_code=503,
            detail="Presentation manager not initialized"
        )
    return main.presentation_manager


# API Endpoints

@router.post("/verify", response_model=VerificationResultResponse)
async def verify_presentation(
    request: VerifyPresentationRequest,
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Verify a Verifiable Presentation
    
    Verifies a presentation that is provided directly (not stored).
    Performs comprehensive checks including signature, expiration,
    revocation, and policy compliance.
    """
    try:
        if not request.presentation:
            raise ValueError("Presentation must be provided")
        
        result = await presentation_manager.verify_presentation(
            presentation=request.presentation,
            verification_options=request.verification_options
        )
        
        return VerificationResultResponse(
            valid=result["valid"],
            checks=result["checks"],
            errors=result.get("errors", []),
            credentials=result.get("credentials", []),
            timestamp=datetime.now().isoformat()
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify presentation: {str(e)}"
        )


@router.post("/verify/{presentation_id}", response_model=VerificationResultResponse)
async def verify_stored_presentation(
    presentation_id: str,
    request: VerifyStoredPresentationRequest,
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Verify a stored presentation
    
    Verifies a presentation that has been previously stored.
    """
    try:
        result = await presentation_manager.verify_presentation(
            presentation_id=presentation_id,
            verification_options=request.verification_options
        )
        
        return VerificationResultResponse(
            valid=result["valid"],
            checks=result["checks"],
            errors=result.get("errors", []),
            credentials=result.get("credentials", []),
            timestamp=datetime.now().isoformat()
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify presentation: {str(e)}"
        )


@router.post("/verify/credential")
async def verify_single_credential(
    credential: Dict[str, Any] = Body(..., description="Credential to verify"),
    proof: Optional[Dict[str, Any]] = Body(None, description="Optional ZKP proof"),
    options: Optional[Dict[str, Any]] = Body(None, description="Verification options"),
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Verify a single credential
    
    Verifies an individual credential outside of a presentation context.
    Useful for testing or direct credential verification.
    """
    try:
        # Use internal verification method
        checks = await presentation_manager._verify_credential(
            credential=credential,
            proof=proof,
            options=options
        )
        
        valid = all(checks.values())
        
        return JSONResponse(
            content={
                "valid": valid,
                "checks": checks,
                "credential_id": credential.get("id"),
                "issuer": credential.get("issuer"),
                "timestamp": datetime.now().isoformat()
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify credential: {str(e)}"
        )


@router.get("/verify/policies")
async def get_verification_policies(
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Get available verification policies
    
    Returns the list of configured verification policies that can
    be applied during presentation verification.
    """
    policies = []
    
    for name, policy in presentation_manager.verification_policies.items():
        policies.append({
            "name": name,
            "description": policy.get("description", ""),
            "rules": {
                "minimum_credentials": policy.get("minimum_credentials", 0),
                "required_credential_types": policy.get("required_credential_types", []),
                "required_issuers": policy.get("required_issuers", []),
                "custom_rules": policy.get("custom_rules", {})
            },
            "active": True
        })
    
    # Add default policy if none exist
    if not policies:
        policies.append({
            "name": "default",
            "description": "Default verification policy",
            "rules": {
                "minimum_credentials": 0,
                "required_credential_types": [],
                "required_issuers": [],
                "custom_rules": {}
            },
            "active": True
        })
    
    return JSONResponse(
        content={
            "policies": policies,
            "total": len(policies)
        }
    )


@router.get("/verify/trusted-issuers")
async def get_trusted_issuers(
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Get list of trusted issuers
    
    Returns the configured list of trusted credential issuers.
    """
    return JSONResponse(
        content={
            "trusted_issuers": list(presentation_manager.trusted_issuers),
            "total": len(presentation_manager.trusted_issuers)
        }
    )


@router.post("/verify/trusted-issuers")
async def add_trusted_issuer(
    issuer_did: str = Body(..., description="Issuer DID to trust"),
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Add a trusted issuer
    
    Adds an issuer to the trusted issuers list.
    """
    presentation_manager.trusted_issuers.add(issuer_did)
    
    # Persist to Consul if available
    if presentation_manager.consul_client and settings.enable_consul_config:
        try:
            await presentation_manager.consul_client.put_key(
                f"config/{settings.service_name}/trusted_issuers",
                list(presentation_manager.trusted_issuers)
            )
        except Exception as e:
            print(f"Failed to persist trusted issuers: {str(e)}")
    
    return JSONResponse(
        content={
            "message": f"Added {issuer_did} to trusted issuers",
            "trusted_issuers": list(presentation_manager.trusted_issuers)
        }
    )


@router.delete("/verify/trusted-issuers/{issuer_did}")
async def remove_trusted_issuer(
    issuer_did: str,
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Remove a trusted issuer
    
    Removes an issuer from the trusted issuers list.
    """
    if issuer_did in presentation_manager.trusted_issuers:
        presentation_manager.trusted_issuers.remove(issuer_did)
        
        # Persist to Consul if available
        if presentation_manager.consul_client and settings.enable_consul_config:
            try:
                await presentation_manager.consul_client.put_key(
                    f"config/{settings.service_name}/trusted_issuers",
                    list(presentation_manager.trusted_issuers)
                )
            except Exception as e:
                print(f"Failed to persist trusted issuers: {str(e)}")
        
        return JSONResponse(
            content={
                "message": f"Removed {issuer_did} from trusted issuers",
                "trusted_issuers": list(presentation_manager.trusted_issuers)
            }
        )
    else:
        raise HTTPException(
            status_code=404,
            detail=f"Issuer {issuer_did} not found in trusted list"
        )


@router.get("/verify/history")
async def get_verification_history(
    verifier: Optional[str] = Query(None, description="Filter by verifier DID"),
    result: Optional[VerificationResult] = Query(None, description="Filter by result"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum records"),
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """
    Get verification history
    
    Retrieves the history of all verification attempts.
    """
    history = await presentation_manager.presentation_store.get_verification_history(
        verifier=verifier,
        limit=limit
    )
    
    # Filter by result if specified
    if result:
        history = [h for h in history if h.result == result]
    
    # Format response
    history_data = []
    for record in history:
        history_data.append({
            "id": record.id,
            "presentation_id": record.presentation_id,
            "verifier": record.verifier,
            "result": record.result,
            "timestamp": record.timestamp.isoformat(),
            "valid": record.result == VerificationResult.VALID,
            "summary": {
                "total_checks": len(record.details.get("checks", {})),
                "passed_checks": sum(1 for v in record.details.get("checks", {}).values() if v),
                "errors": len(record.details.get("errors", []))
            }
        })
    
    return JSONResponse(
        content={
            "history": history_data,
            "total": len(history_data),
            "filters": {
                "verifier": verifier,
                "result": result
            }
        }
    )


@router.get("/verify/stats")
async def get_verification_statistics(
    presentation_manager: PresentationManager = Depends(get_presentation_manager)
):
    """Get verification statistics"""
    stats = await presentation_manager.get_statistics()
    
    return JSONResponse(
        content={
            "total_verified": stats.get("total_verified", 0),
            "total_rejected": stats.get("total_rejected", 0),
            "acceptance_rate": stats.get("verification_acceptance_rate", 0),
            "trusted_issuers_count": len(presentation_manager.trusted_issuers),
            "policies_count": len(presentation_manager.verification_policies)
        }
    ) 