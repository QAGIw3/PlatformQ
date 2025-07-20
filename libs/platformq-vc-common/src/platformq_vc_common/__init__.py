"""
PlatformQ Verifiable Credential Common Library

Provides shared models, utilities, and standards for W3C Verifiable Credentials
"""

from .models import (
    VerifiableCredentialModel,
    VerifiablePresentationModel,
    CredentialSubject,
    CredentialStatus,
    CredentialProof,
    PresentationProof,
    CredentialType
)

from .utils import (
    create_credential_id,
    create_presentation_id,
    verify_credential_signature,
    verify_presentation_signature,
    canonicalize_credential,
    hash_credential
)

from .standards import (
    W3C_VC_CONTEXT,
    W3C_VP_CONTEXT,
    PLATFORMQ_CONTEXT,
    JSON_LD_PROCESSOR
)

__version__ = "0.1.0"

__all__ = [
    # Models
    "VerifiableCredentialModel",
    "VerifiablePresentationModel", 
    "CredentialSubject",
    "CredentialStatus",
    "CredentialProof",
    "PresentationProof",
    "CredentialType",
    
    # Utils
    "create_credential_id",
    "create_presentation_id",
    "verify_credential_signature",
    "verify_presentation_signature",
    "canonicalize_credential",
    "hash_credential",
    
    # Standards
    "W3C_VC_CONTEXT",
    "W3C_VP_CONTEXT",
    "PLATFORMQ_CONTEXT",
    "JSON_LD_PROCESSOR"
] 