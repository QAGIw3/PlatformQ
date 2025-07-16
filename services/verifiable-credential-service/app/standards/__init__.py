"""
Advanced Standards module for Verifiable Credential Service

Implements emerging W3C and blockchain standards including:
- Verifiable Presentations (VP 2.0)
- SoulBound Tokens (SBTs)
- Credential Manifest
- Presentation Exchange
"""

from .advanced_standards import (
    VerifiablePresentationBuilder,
    SoulBoundTokenManager,
    CredentialManifest,
    PresentationRequest,
    PresentationPurpose,
    SoulBoundToken
)

__all__ = [
    'VerifiablePresentationBuilder',
    'SoulBoundTokenManager',
    'CredentialManifest',
    'PresentationRequest',
    'PresentationPurpose',
    'SoulBoundToken'
] 