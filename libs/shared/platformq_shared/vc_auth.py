"""
Verifiable Credential Authorization Framework

Provides decorators and middleware for protecting API endpoints with VC requirements.
Enhanced with Zero-Knowledge Proof support for privacy-preserving authentication.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from functools import wraps
from typing import List, Dict, Optional, Any, Callable, Union
from enum import Enum
import secrets
import base64

from fastapi import HTTPException, Request, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import httpx
import jwt
from pydantic import BaseModel, Field

from .cache import get_cache_client
from .zkp import ZKProof, ZKPRequest, ZKPGenerator, ZKPVerifier

logger = logging.getLogger(__name__)

# Configuration
VC_SERVICE_URL = "http://verifiable-credential-service:8000"
CACHE_TTL = 300  # 5 minutes
VERIFY_ON_CHAIN_DEFAULT = False


class VerificationStatus(str, Enum):
    VALID = "valid"
    INVALID = "invalid"
    EXPIRED = "expired"
    REVOKED = "revoked"
    NOT_FOUND = "not_found"


class ClaimOperator(str, Enum):
    EQUALS = "$eq"
    NOT_EQUALS = "$ne"
    GREATER_THAN = "$gt"
    GREATER_THAN_OR_EQUAL = "$gte"
    LESS_THAN = "$lt"
    LESS_THAN_OR_EQUAL = "$lte"
    IN = "$in"
    NOT_IN = "$nin"
    EXISTS = "$exists"


class VCRequirement(BaseModel):
    """Defines requirements for a verifiable credential"""
    credential_types: List[str] = Field(..., description="Accepted credential types")
    claims: Optional[Dict[str, Any]] = Field(None, description="Required claims with operators")
    verify_on_chain: bool = Field(VERIFY_ON_CHAIN_DEFAULT, description="Verify blockchain anchor")
    allow_expired: bool = Field(False, description="Accept expired credentials")
    max_age_days: Optional[int] = Field(None, description="Maximum credential age in days")
    issuers: Optional[List[str]] = Field(None, description="Accepted issuers")
    allow_zkp: bool = Field(True, description="Accept Zero-Knowledge Proofs")
    zkp_claims: Optional[List[str]] = Field(None, description="Claims that can be proven via ZKP")


class VerifiedCredential(BaseModel):
    """Represents a verified credential with its claims"""
    id: str
    type: List[str]
    issuer: str
    subject: Dict[str, Any]
    issuance_date: datetime
    expiration_date: Optional[datetime]
    verification_status: VerificationStatus
    blockchain_anchor: Optional[Dict[str, Any]]


security = HTTPBearer()

# ZKP Generator and Verifier instances
zkp_generator = ZKPGenerator()
zkp_verifier = ZKPVerifier()


async def extract_vcs_or_zkp_from_request(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> Union[List[Dict[str, Any]], ZKProof]:
    """Extract VCs or ZKP from Authorization header"""
    token = credentials.credentials
    
    try:
        # Try to decode as JWT VC
        decoded = jwt.decode(token, options={"verify_signature": False})
        
        # Check if it's a ZKP
        if "proof_type" in decoded and "public_inputs" in decoded:
            return ZKProof(
                proof_type=decoded["proof_type"],
                public_inputs=decoded["public_inputs"],
                proof_data=decoded["proof_data"],
                timestamp=datetime.fromisoformat(decoded["timestamp"])
            )
        elif "vc" in decoded:
            return [decoded["vc"]]
        elif "vp" in decoded and "verifiableCredential" in decoded["vp"]:
            # Verifiable Presentation
            return decoded["vp"]["verifiableCredential"]
        else:
            # Assume it's a plain VC
            return [decoded]
    except jwt.DecodeError:
        # Try to parse as JSON
        try:
            data = json.loads(token)
            
            # Check if it's a ZKP
            if isinstance(data, dict) and "proof_type" in data:
                return ZKProof(
                    proof_type=data["proof_type"],
                    public_inputs=data["public_inputs"],
                    proof_data=data["proof_data"],
                    timestamp=datetime.fromisoformat(data["timestamp"])
                )
            elif isinstance(data, list):
                return data
            else:
                return [data]
        except json.JSONDecodeError:
            # Try base64 decode
            try:
                decoded_bytes = base64.b64decode(token)
                data = json.loads(decoded_bytes)
                
                if isinstance(data, dict) and "proof_type" in data:
                    return ZKProof(
                        proof_type=data["proof_type"],
                        public_inputs=data["public_inputs"],
                        proof_data=data["proof_data"],
                        timestamp=datetime.fromisoformat(data["timestamp"])
                    )
                else:
                    raise HTTPException(status_code=401, detail="Invalid credential format")
            except Exception:
                raise HTTPException(status_code=401, detail="Invalid credential format")


async def extract_vcs_from_request(
    credentials: HTTPAuthorizationCredentials = Depends(security)
) -> List[Dict[str, Any]]:
    """Extract VCs from Authorization header (backward compatibility)"""
    result = await extract_vcs_or_zkp_from_request(credentials)
    if isinstance(result, ZKProof):
        raise HTTPException(status_code=401, detail="Expected VC but received ZKP")
    return result


async def verify_credential(
    vc: Dict[str, Any],
    requirement: VCRequirement,
    cache_client: Optional[Any] = None
) -> VerifiedCredential:
    """Verify a single credential against requirements"""
    
    vc_id = vc.get("id", "unknown")
    
    # Check cache first
    if cache_client:
        cached = await cache_client.get(f"vc:verified:{vc_id}")
        if cached:
            return VerifiedCredential(**json.loads(cached))
    
    # Extract basic info
    vc_types = vc.get("type", [])
    if isinstance(vc_types, str):
        vc_types = [vc_types]
    
    issuer = vc.get("issuer", {})
    if isinstance(issuer, str):
        issuer_id = issuer
    else:
        issuer_id = issuer.get("id", "unknown")
    
    subject = vc.get("credentialSubject", {})
    issuance_date = datetime.fromisoformat(
        vc.get("issuanceDate", "").replace("Z", "+00:00")
    )
    
    expiration_str = vc.get("expirationDate")
    expiration_date = None
    if expiration_str:
        expiration_date = datetime.fromisoformat(expiration_str.replace("Z", "+00:00"))
    
    # Initial verification result
    result = VerifiedCredential(
        id=vc_id,
        type=vc_types,
        issuer=issuer_id,
        subject=subject,
        issuance_date=issuance_date,
        expiration_date=expiration_date,
        verification_status=VerificationStatus.VALID,
        blockchain_anchor=None
    )
    
    # Check credential type
    type_match = any(
        required_type in vc_types 
        for required_type in requirement.credential_types
    )
    if not type_match:
        result.verification_status = VerificationStatus.INVALID
        return result
    
    # Check expiration
    if expiration_date and datetime.utcnow() > expiration_date:
        if not requirement.allow_expired:
            result.verification_status = VerificationStatus.EXPIRED
            return result
    
    # Check age
    if requirement.max_age_days:
        max_age = timedelta(days=requirement.max_age_days)
        if datetime.utcnow() - issuance_date > max_age:
            result.verification_status = VerificationStatus.EXPIRED
            return result
    
    # Check issuer
    if requirement.issuers and issuer_id not in requirement.issuers:
        result.verification_status = VerificationStatus.INVALID
        return result
    
    # Check claims
    if requirement.claims:
        if not verify_claims(subject, requirement.claims):
            result.verification_status = VerificationStatus.INVALID
            return result
    
    # Verify on blockchain if required
    if requirement.verify_on_chain:
        blockchain_status = await verify_on_chain(vc_id)
        if not blockchain_status["valid"]:
            result.verification_status = VerificationStatus.INVALID
            result.blockchain_anchor = blockchain_status
            return result
        result.blockchain_anchor = blockchain_status
    
    # Cache the result
    if cache_client:
        await cache_client.setex(
            f"vc:verified:{vc_id}",
            CACHE_TTL,
            json.dumps(result.dict())
        )
    
    return result


def verify_claims(subject: Dict[str, Any], requirements: Dict[str, Any]) -> bool:
    """Verify credential subject claims against requirements"""
    
    for claim_path, requirement in requirements.items():
        # Navigate nested claims using dot notation
        value = subject
        for part in claim_path.split("."):
            if isinstance(value, dict) and part in value:
                value = value[part]
            else:
                # Claim doesn't exist
                if isinstance(requirement, dict) and "$exists" in requirement:
                    if not requirement["$exists"]:
                        continue  # Claim should not exist
                return False
        
        # Check the claim value
        if not check_claim_value(value, requirement):
            return False
    
    return True


def check_claim_value(value: Any, requirement: Any) -> bool:
    """Check if a claim value meets the requirement"""
    
    if isinstance(requirement, dict):
        # Operator-based requirement
        for operator, expected in requirement.items():
            if operator == ClaimOperator.EQUALS:
                if value != expected:
                    return False
            elif operator == ClaimOperator.NOT_EQUALS:
                if value == expected:
                    return False
            elif operator == ClaimOperator.GREATER_THAN:
                if not (value > expected):
                    return False
            elif operator == ClaimOperator.GREATER_THAN_OR_EQUAL:
                if not (value >= expected):
                    return False
            elif operator == ClaimOperator.LESS_THAN:
                if not (value < expected):
                    return False
            elif operator == ClaimOperator.LESS_THAN_OR_EQUAL:
                if not (value <= expected):
                    return False
            elif operator == ClaimOperator.IN:
                if value not in expected:
                    return False
            elif operator == ClaimOperator.NOT_IN:
                if value in expected:
                    return False
            elif operator == ClaimOperator.EXISTS:
                # Already handled above
                pass
    else:
        # Direct equality check
        if value != requirement:
            return False
    
    return True


async def verify_on_chain(vc_id: str) -> Dict[str, Any]:
    """Verify credential on blockchain"""
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{VC_SERVICE_URL}/api/v1/credentials/{vc_id}/verify"
            )
            if response.status_code == 200:
                return response.json()
            else:
                return {"valid": False, "error": "Verification failed"}
    except Exception as e:
        logger.error(f"Blockchain verification error: {e}")
        return {"valid": False, "error": str(e)}


def require_vc(*requirements: VCRequirement):
    """
    Decorator to protect endpoints with VC requirements
    
    Usage:
        @require_vc(
            VCRequirement(
                credential_types=["ResearcherCredential"],
                claims={"level": {"$gte": 2}}
            )
        )
        async def protected_endpoint():
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Extract request from kwargs
            request = None
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            for key, value in kwargs.items():
                if isinstance(value, Request):
                    request = value
                    break
            
            if not request:
                raise HTTPException(
                    status_code=500,
                    detail="Request object not found"
                )
            
            # Get VCs from request
            auth_header = request.headers.get("Authorization", "")
            if not auth_header:
                raise HTTPException(
                    status_code=401,
                    detail="Authorization header required"
                )
            
            # Create mock credentials object for the extractor
            from fastapi.security import HTTPAuthorizationCredentials
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]
                credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
            else:
                credentials = HTTPAuthorizationCredentials(scheme="Bearer", credentials=auth_header)
            
            vcs = await extract_vcs_from_request(credentials)
            
            if not vcs:
                raise HTTPException(
                    status_code=401,
                    detail="No credentials provided"
                )
            
            # Get cache client
            cache_client = await get_cache_client()
            
            # Verify each requirement
            verified_credentials = []
            for requirement in requirements:
                requirement_met = False
                
                for vc in vcs:
                    result = await verify_credential(vc, requirement, cache_client)
                    
                    if result.verification_status == VerificationStatus.VALID:
                        verified_credentials.append(result)
                        requirement_met = True
                        break
                
                if not requirement_met:
                    raise HTTPException(
                        status_code=403,
                        detail=f"Missing required credential: {requirement.credential_types}"
                    )
            
            # Attach verified credentials to request
            request.state.verified_credentials = verified_credentials
            
            # Call original function
            return await func(*args, **kwargs)
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # For sync functions, run in event loop
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(async_wrapper(*args, **kwargs))
        
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


class VCVerificationMiddleware:
    """
    Middleware to verify VCs on all requests
    
    Usage:
        app.add_middleware(VCVerificationMiddleware)
    """
    
    def __init__(self, app, verify_all: bool = False):
        self.app = app
        self.verify_all = verify_all
    
    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            # Only process HTTP requests
            request = Request(scope, receive)
            
            # Check if endpoint requires VC verification
            if self.verify_all or request.url.path.startswith("/api/v1/protected"):
                auth_header = request.headers.get("Authorization", "")
                
                if auth_header:
                    try:
                        vcs = await extract_vcs_from_request(auth_header)
                        
                        # Basic verification for all VCs
                        cache_client = await get_cache_client()
                        verified = []
                        
                        for vc in vcs:
                            result = await verify_credential(
                                vc,
                                VCRequirement(credential_types=["VerifiableCredential"]),
                                cache_client
                            )
                            verified.append(result)
                        
                        # Store in request state
                        request.state.verified_credentials = verified
                    except Exception as e:
                        logger.error(f"VC verification error: {e}")
        
        await self.app(scope, receive, send)


# Helper functions for common requirements
def researcher_required(level: int = 1):
    """Require ResearcherCredential with minimum level"""
    return VCRequirement(
        credential_types=["ResearcherCredential"],
        claims={"level": {"$gte": level}}
    )


def compute_allowance_required(hours: int = 1):
    """Require ComputeAllowanceCredential with minimum hours"""
    return VCRequirement(
        credential_types=["ComputeAllowanceCredential"],
        claims={"compute_hours": {"$gt": hours}}
    )


def trust_score_required(min_score: float = 0.5):
    """Require TrustScoreCredential with minimum score"""
    return VCRequirement(
        credential_types=["TrustScoreCredential"],
        claims={"trustScore": {"$gte": min_score}},
        max_age_days=30  # Trust scores expire after 30 days
    ) 