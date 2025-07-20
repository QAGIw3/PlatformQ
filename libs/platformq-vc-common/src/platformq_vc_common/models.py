"""
W3C Verifiable Credential Data Models

Based on W3C VC Data Model 1.1: https://www.w3.org/TR/vc-data-model/
"""

from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from pydantic import BaseModel, Field, validator
from enum import Enum
import uuid


class CredentialType(str, Enum):
    """Standard credential types in PlatformQ"""
    VERIFIABLE_CREDENTIAL = "VerifiableCredential"
    ACHIEVEMENT_CREDENTIAL = "AchievementCredential"
    REPUTATION_CREDENTIAL = "ReputationCredential"
    KYC_CREDENTIAL = "KYCCredential"
    AML_COMPLIANCE_CREDENTIAL = "AMLComplianceCredential"
    DAO_MEMBERSHIP_CREDENTIAL = "DAOMembershipCredential"
    VOTING_POWER_CREDENTIAL = "VotingPowerCredential"
    REPUTATION_SCORE_CREDENTIAL = "ReputationScoreCredential"
    PROPOSAL_APPROVAL_CREDENTIAL = "ProposalApprovalCredential"
    SKILL_CREDENTIAL = "SkillCredential"
    CERTIFICATION_CREDENTIAL = "CertificationCredential"
    

class CredentialStatus(BaseModel):
    """Credential status for revocation checking"""
    id: str = Field(..., description="URL to check credential status")
    type: str = Field(..., description="Type of status method (e.g., RevocationList2020)")
    revocationListIndex: Optional[int] = Field(None, description="Index in revocation list")
    revocationListCredential: Optional[str] = Field(None, description="URL to revocation list credential")


class CredentialSubject(BaseModel):
    """Base credential subject - can be extended for specific types"""
    id: Optional[str] = Field(None, description="DID or identifier of the subject")
    
    class Config:
        extra = "allow"  # Allow additional fields for specific credential types


class CredentialProof(BaseModel):
    """Proof attached to a credential"""
    type: str = Field(..., description="Proof type (e.g., Ed25519Signature2020)")
    created: datetime = Field(..., description="When the proof was created")
    verificationMethod: str = Field(..., description="Verification method URL")
    proofPurpose: str = Field(default="assertionMethod", description="Purpose of the proof")
    proofValue: str = Field(..., description="The proof value (signature)")
    
    # Optional fields
    challenge: Optional[str] = Field(None, description="Challenge for proof")
    domain: Optional[str] = Field(None, description="Domain for proof")
    nonce: Optional[str] = Field(None, description="Nonce for uniqueness")
    
    # Additional fields for blockchain anchoring
    blockchainAnchor: Optional[Dict[str, Any]] = Field(None, description="Blockchain anchor details")
    ipfsCID: Optional[str] = Field(None, description="IPFS content ID")


class VerifiableCredentialModel(BaseModel):
    """W3C Verifiable Credential data model"""
    # Required fields
    context: List[Union[str, Dict[str, Any]]] = Field(
        ..., 
        alias="@context",
        description="JSON-LD context"
    )
    id: str = Field(..., description="Unique credential identifier")
    type: List[str] = Field(..., description="Credential types")
    issuer: Union[str, Dict[str, Any]] = Field(..., description="Issuer DID or object")
    issuanceDate: datetime = Field(..., description="When credential was issued")
    credentialSubject: Union[CredentialSubject, List[CredentialSubject], Dict[str, Any]] = Field(
        ..., 
        description="Subject(s) of the credential"
    )
    
    # Optional fields
    proof: Optional[Union[CredentialProof, List[CredentialProof]]] = Field(
        None, 
        description="Cryptographic proof(s)"
    )
    expirationDate: Optional[datetime] = Field(None, description="When credential expires")
    credentialStatus: Optional[CredentialStatus] = Field(None, description="Status method")
    description: Optional[str] = Field(None, description="Human-readable description")
    name: Optional[str] = Field(None, description="Human-readable name")
    
    # PlatformQ extensions
    tenantId: Optional[str] = Field(None, description="Tenant identifier")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")
    
    class Config:
        allow_population_by_field_name = True
        json_encoders = {
            datetime: lambda v: v.isoformat() + "Z" if v else None
        }
        
    @validator('context', pre=True)
    def ensure_vc_context(cls, v):
        """Ensure W3C VC context is included"""
        if isinstance(v, str):
            v = [v]
        if "https://www.w3.org/2018/credentials/v1" not in v:
            v.insert(0, "https://www.w3.org/2018/credentials/v1")
        return v
    
    @validator('type')
    def ensure_vc_type(cls, v):
        """Ensure VerifiableCredential type is included"""
        if CredentialType.VERIFIABLE_CREDENTIAL.value not in v:
            v.insert(0, CredentialType.VERIFIABLE_CREDENTIAL.value)
        return v
    
    @validator('id', pre=True)
    def generate_id_if_missing(cls, v):
        """Generate credential ID if not provided"""
        if not v:
            return f"urn:uuid:{uuid.uuid4()}"
        return v


class PresentationProof(BaseModel):
    """Proof attached to a presentation"""
    type: str = Field(..., description="Proof type")
    created: datetime = Field(..., description="When proof was created")
    verificationMethod: str = Field(..., description="Verification method URL")
    proofPurpose: str = Field(default="authentication", description="Purpose of proof")
    challenge: str = Field(..., description="Challenge that was signed")
    proofValue: str = Field(..., description="The proof value (signature)")
    
    # Optional
    domain: Optional[str] = Field(None, description="Domain for proof")
    

class VerifiablePresentationModel(BaseModel):
    """W3C Verifiable Presentation data model"""
    # Required fields
    context: List[Union[str, Dict[str, Any]]] = Field(
        ..., 
        alias="@context",
        description="JSON-LD context"
    )
    type: List[str] = Field(..., description="Presentation types")
    verifiableCredential: List[Union[VerifiableCredentialModel, Dict[str, Any]]] = Field(
        ..., 
        description="Credentials being presented"
    )
    
    # Optional fields
    id: Optional[str] = Field(None, description="Unique presentation identifier")
    holder: Optional[str] = Field(None, description="Holder DID")
    proof: Optional[Union[PresentationProof, List[PresentationProof]]] = Field(
        None, 
        description="Cryptographic proof(s)"
    )
    
    class Config:
        allow_population_by_field_name = True
        json_encoders = {
            datetime: lambda v: v.isoformat() + "Z" if v else None
        }
        
    @validator('context', pre=True)
    def ensure_vp_context(cls, v):
        """Ensure W3C VP context is included"""
        if isinstance(v, str):
            v = [v]
        if "https://www.w3.org/2018/credentials/v1" not in v:
            v.insert(0, "https://www.w3.org/2018/credentials/v1")
        return v
    
    @validator('type')
    def ensure_vp_type(cls, v):
        """Ensure VerifiablePresentation type is included"""
        if "VerifiablePresentation" not in v:
            v.insert(0, "VerifiablePresentation")
        return v


# Specific credential subject models

class AchievementCredentialSubject(CredentialSubject):
    """Subject for achievement credentials"""
    achievement: str = Field(..., description="Achievement identifier or name")
    achievementType: Optional[str] = Field(None, description="Type of achievement")
    level: Optional[str] = Field(None, description="Achievement level")
    points: Optional[int] = Field(None, description="Points earned")
    awardedDate: Optional[datetime] = Field(None, description="When achievement was earned")
    

class ReputationCredentialSubject(CredentialSubject):
    """Subject for reputation credentials"""
    dimensions: Dict[str, float] = Field(..., description="Reputation dimensions and scores")
    overallScore: Optional[float] = Field(None, description="Overall reputation score")
    totalInteractions: Optional[int] = Field(None, description="Total platform interactions")
    

class KYCCredentialSubject(CredentialSubject):
    """Subject for KYC credentials"""
    kycLevel: int = Field(..., description="KYC verification level (1-3)")
    verifiedAttributes: List[str] = Field(..., description="List of verified attributes")
    jurisdiction: str = Field(..., description="Jurisdiction of verification")
    verificationDate: datetime = Field(..., description="When KYC was performed")
    

class AMLComplianceCredentialSubject(CredentialSubject):
    """Subject for AML compliance credentials"""
    riskScore: float = Field(..., description="Risk score (0.0-1.0)")
    riskLevel: str = Field(..., description="Risk level (LOW, MEDIUM, HIGH)")
    sanctionsCheck: str = Field(..., description="Sanctions check result")
    lastAssessment: datetime = Field(..., description="Last assessment date")
    assessmentDetails: Optional[Dict[str, Any]] = Field(None, description="Detailed assessment data") 