"""
ZKP Service API Endpoints
"""

from typing import Optional, List, Dict, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends, Query, Body
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.core.proof_engine import ProofEngine
from app import main

router = APIRouter()


# Request/Response models
class BBSSignatureRequest(BaseModel):
    """Request to generate BBS+ signature"""
    credential: Dict[str, Any] = Field(
        description="Credential to sign"
    )
    private_key_id: str = Field(
        description="ID of private key to use for signing"
    )
    verification_method: Optional[str] = Field(
        description="Verification method URL",
        default=None
    )


class SelectiveDisclosureRequest(BaseModel):
    """Request for selective disclosure proof"""
    credential: Dict[str, Any] = Field(
        description="Credential with BBS+ signature"
    )
    disclosed_attributes: List[str] = Field(
        description="Attributes to disclose",
        example=["name", "dateOfBirth"]
    )
    nonce: str = Field(
        description="Challenge nonce from verifier"
    )


class RangeProofRequest(BaseModel):
    """Request for range proof"""
    attribute: str = Field(
        description="Attribute to prove",
        example="age"
    )
    min: float = Field(
        description="Minimum value (inclusive)"
    )
    max: float = Field(
        description="Maximum value (inclusive)"
    )
    credential: Dict[str, Any] = Field(
        description="Credential containing the attribute"
    )
    bits: Optional[int] = Field(
        description="Number of bits for range proof",
        default=32
    )


class PredicateProofRequest(BaseModel):
    """Request for predicate proof"""
    predicate: Dict[str, Any] = Field(
        description="Predicate specification",
        example={
            "attribute": "age",
            "operator": ">=",
            "value": 18
        }
    )
    credential: Dict[str, Any] = Field(
        description="Credential containing the attribute"
    )


class SetMembershipRequest(BaseModel):
    """Request for set membership proof"""
    attribute: str = Field(
        description="Attribute to prove membership"
    )
    set: List[Any] = Field(
        description="Allowed set of values"
    )
    credential: Dict[str, Any] = Field(
        description="Credential containing the attribute"
    )
    use_bloom_filter: Optional[bool] = Field(
        description="Use bloom filter (true) or Merkle tree (false)",
        default=True
    )


class BatchProofRequest(BaseModel):
    """Request for batch proof generation"""
    proofs: List[Dict[str, Any]] = Field(
        description="List of proof specifications"
    )
    priority: Optional[str] = Field(
        description="Priority level",
        default="normal",
        enum=["low", "normal", "high"]
    )


class VerifyProofRequest(BaseModel):
    """Request to verify a proof"""
    proof: Dict[str, Any] = Field(
        description="Proof to verify"
    )
    public_key: Dict[str, Any] = Field(
        description="Public key for verification"
    )
    nonce: Optional[str] = Field(
        description="Expected nonce for selective disclosure",
        default=None
    )
    credential: Optional[Dict[str, Any]] = Field(
        description="Original credential (for some proof types)",
        default=None
    )


class ProofResponse(BaseModel):
    """Proof generation response"""
    proof: Dict[str, Any] = Field(
        description="Generated proof"
    )
    metadata: Optional[Dict[str, Any]] = Field(
        description="Additional metadata",
        default=None
    )


class BatchProofResponse(BaseModel):
    """Batch proof response"""
    batch_id: str = Field(
        description="Batch processing ID"
    )
    status: str = Field(
        description="Batch status"
    )
    task_ids: List[str] = Field(
        description="Individual task IDs"
    )


class VerificationResponse(BaseModel):
    """Proof verification response"""
    valid: bool = Field(
        description="Whether proof is valid"
    )
    details: Optional[Dict[str, Any]] = Field(
        description="Verification details",
        default=None
    )


# Dependency to get proof engine
def get_proof_engine() -> ProofEngine:
    """Get proof engine instance"""
    if not main.proof_engine:
        raise HTTPException(
            status_code=503,
            detail="Proof engine not initialized"
        )
    return main.proof_engine


# API Endpoints

# BBS+ Signatures
@router.post("/proofs/bbs/sign", response_model=ProofResponse)
async def generate_bbs_signature(
    request: BBSSignatureRequest,
    proof_engine: ProofEngine = Depends(get_proof_engine)
):
    """
    Generate BBS+ signature for a credential
    
    Creates a BBS+ signature that enables selective disclosure
    without requiring re-issuance of the credential.
    """
    try:
        options = {
            "key_id": request.private_key_id,
            "verification_method": request.verification_method
        }
        
        result = await proof_engine.generate_proof(
            proof_type="bbs_signature",
            credential=request.credential,
            options=options
        )
        
        return ProofResponse(
            proof=result,
            metadata={
                "signature_type": "BbsBlsSignature2020",
                "enables_selective_disclosure": True
            }
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate BBS+ signature: {str(e)}"
        )


@router.post("/proofs/bbs/verify", response_model=VerificationResponse)
async def verify_bbs_signature(
    request: VerifyProofRequest,
    proof_engine: ProofEngine = Depends(get_proof_engine)
):
    """
    Verify a BBS+ signature
    
    Verifies that a BBS+ signature is valid for the given
    credential and public key.
    """
    try:
        options = {
            "credential": request.credential
        }
        
        result = await proof_engine.verify_proof(
            proof_type="bbs_signature",
            proof=request.proof,
            public_key=request.public_key,
            options=options
        )
        
        return VerificationResponse(
            valid=result["valid"],
            details=result
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify BBS+ signature: {str(e)}"
        )


# Selective Disclosure
@router.post("/proofs/selective-disclosure", response_model=ProofResponse)
async def generate_selective_disclosure(
    request: SelectiveDisclosureRequest,
    proof_engine: ProofEngine = Depends(get_proof_engine)
):
    """
    Generate selective disclosure proof
    
    Creates a derived proof that reveals only specified attributes
    while maintaining cryptographic validity.
    """
    try:
        options = {
            "disclosed_attributes": request.disclosed_attributes,
            "nonce": request.nonce
        }
        
        result = await proof_engine.generate_proof(
            proof_type="selective_disclosure",
            credential=request.credential,
            options=options
        )
        
        return ProofResponse(
            proof=result["proof"],
            metadata={
                "revealed_attributes": request.disclosed_attributes,
                "revealed_document": result.get("revealedDocument")
            }
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate selective disclosure: {str(e)}"
        )


@router.post("/proofs/selective-disclosure/verify", response_model=VerificationResponse)
async def verify_selective_disclosure(
    request: VerifyProofRequest,
    proof_engine: ProofEngine = Depends(get_proof_engine)
):
    """
    Verify a selective disclosure proof
    
    Verifies that a selective disclosure proof is valid and
    matches the expected nonce.
    """
    try:
        options = {
            "revealedDocument": request.credential,
            "nonce": request.nonce
        }
        
        result = await proof_engine.verify_proof(
            proof_type="selective_disclosure",
            proof=request.proof,
            public_key=request.public_key,
            options=options
        )
        
        return VerificationResponse(
            valid=result["valid"],
            details=result
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to verify selective disclosure: {str(e)}"
        )


# Range Proofs
@router.post("/proofs/range", response_model=ProofResponse)
async def generate_range_proof(
    request: RangeProofRequest,
    proof_engine: ProofEngine = Depends(get_proof_engine)
):
    """
    Generate range proof
    
    Proves that an attribute value falls within a specified range
    without revealing the actual value.
    """
    try:
        options = {
            "attribute": request.attribute,
            "min": request.min,
            "max": request.max,
            "bits": request.bits
        }
        
        result = await proof_engine.generate_proof(
            proof_type="range_proof",
            credential=request.credential,
            options=options
        )
        
        return ProofResponse(
            proof=result["proof"],
            metadata={
                "proof_type": "range",
                "attribute": request.attribute,
                "range": {"min": request.min, "max": request.max}
            }
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate range proof: {str(e)}"
        )


# Predicate Proofs
@router.post("/proofs/predicate", response_model=ProofResponse)
async def generate_predicate_proof(
    request: PredicateProofRequest,
    proof_engine: ProofEngine = Depends(get_proof_engine)
):
    """
    Generate predicate proof
    
    Proves that an attribute satisfies a predicate (e.g., age >= 18)
    without revealing the attribute value.
    """
    try:
        options = {
            "predicate": request.predicate
        }
        
        result = await proof_engine.generate_proof(
            proof_type="predicate_proof",
            credential=request.credential,
            options=options
        )
        
        return ProofResponse(
            proof=result["proof"],
            metadata={
                "proof_type": "predicate",
                "predicate": request.predicate
            }
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate predicate proof: {str(e)}"
        )


# Set Membership
@router.post("/proofs/set-membership", response_model=ProofResponse)
async def generate_set_membership_proof(
    request: SetMembershipRequest,
    proof_engine: ProofEngine = Depends(get_proof_engine)
):
    """
    Generate set membership proof
    
    Proves that an attribute value is a member of a specified set
    without revealing which specific value.
    """
    try:
        options = {
            "attribute": request.attribute,
            "set": request.set,
            "use_bloom_filter": request.use_bloom_filter
        }
        
        result = await proof_engine.generate_proof(
            proof_type="set_membership",
            credential=request.credential,
            options=options
        )
        
        return ProofResponse(
            proof=result["proof"],
            metadata={
                "proof_type": "set_membership",
                "attribute": request.attribute,
                "set_size": len(request.set),
                "method": "bloom_filter" if request.use_bloom_filter else "merkle_tree"
            }
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to generate set membership proof: {str(e)}"
        )


# Batch Operations
@router.post("/proofs/batch", response_model=BatchProofResponse)
async def submit_batch_proofs(
    request: BatchProofRequest,
    proof_engine: ProofEngine = Depends(get_proof_engine)
):
    """
    Submit batch proof generation
    
    Submits multiple proofs for generation on the compute grid.
    Returns immediately with batch ID for status tracking.
    """
    try:
        if not main.compute_manager:
            raise HTTPException(
                status_code=503,
                detail="Compute grid not available"
            )
        
        # Map priority to numeric value
        priority_map = {"low": 1, "normal": 5, "high": 10}
        priority = priority_map.get(request.priority, 5)
        
        # Submit batch to compute manager
        task_ids = await main.compute_manager.submit_batch(
            tasks=request.proofs,
            priority=priority
        )
        
        # Create batch ID
        batch_id = f"batch-{datetime.now().timestamp()}"
        
        return BatchProofResponse(
            batch_id=batch_id,
            status="processing",
            task_ids=task_ids
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit batch: {str(e)}"
        )


@router.get("/proofs/batch/{batch_id}/status")
async def get_batch_status(batch_id: str):
    """
    Get batch processing status
    
    Returns the current status of a batch proof generation job.
    """
    # This is a simplified implementation
    # Real implementation would track batch status
    return JSONResponse(
        content={
            "batch_id": batch_id,
            "status": "processing",
            "completed": 0,
            "total": 0,
            "results": []
        }
    )


# Statistics
@router.get("/stats/proof-engine")
async def get_proof_engine_stats(
    proof_engine: ProofEngine = Depends(get_proof_engine)
):
    """Get proof engine statistics"""
    stats = await proof_engine.get_statistics()
    return JSONResponse(content=stats)


@router.get("/stats/cache")
async def get_cache_stats():
    """Get cache statistics"""
    if not main.cache_manager:
        raise HTTPException(
            status_code=503,
            detail="Cache not enabled"
        )
    
    stats = await main.cache_manager.get_statistics()
    return JSONResponse(content=stats)


# Health check for proofs
@router.get("/proofs/health")
async def proof_health_check():
    """Check if proof generation is healthy"""
    checks = {
        "proof_engine": main.proof_engine is not None,
        "compute_grid": main.compute_manager is not None and main.compute_manager.connected,
        "cache": main.cache_manager is not None and main.cache_manager.connected
    }
    
    all_healthy = all(checks.values())
    
    return JSONResponse(
        content={
            "status": "healthy" if all_healthy else "degraded",
            "checks": checks
        },
        status_code=200 if all_healthy else 503
    ) 