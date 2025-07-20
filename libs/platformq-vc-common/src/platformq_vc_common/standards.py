"""
W3C Standards and JSON-LD Contexts for Verifiable Credentials
"""

from typing import Dict, Any, List, Optional
import json


# W3C Standard Contexts
W3C_VC_CONTEXT = "https://www.w3.org/2018/credentials/v1"
W3C_VP_CONTEXT = "https://www.w3.org/2018/credentials/v1"
W3C_SECURITY_V2_CONTEXT = "https://w3id.org/security/v2"
W3C_DID_CONTEXT = "https://www.w3.org/ns/did/v1"

# PlatformQ Custom Context
PLATFORMQ_CONTEXT = {
    "@context": {
        "@version": 1.1,
        "@protected": True,
        
        "platformq": "https://platformq.com/vocab#",
        
        # Credential types
        "AchievementCredential": "platformq:AchievementCredential",
        "ReputationCredential": "platformq:ReputationCredential",
        "KYCCredential": "platformq:KYCCredential",
        "AMLComplianceCredential": "platformq:AMLComplianceCredential",
        "DAOMembershipCredential": "platformq:DAOMembershipCredential",
        "VotingPowerCredential": "platformq:VotingPowerCredential",
        "ReputationScoreCredential": "platformq:ReputationScoreCredential",
        "ProposalApprovalCredential": "platformq:ProposalApprovalCredential",
        
        # Achievement properties
        "achievement": "platformq:achievement",
        "achievementType": "platformq:achievementType",
        "level": "platformq:level",
        "points": {
            "@id": "platformq:points",
            "@type": "xsd:integer"
        },
        "awardedDate": {
            "@id": "platformq:awardedDate",
            "@type": "xsd:dateTime"
        },
        
        # Reputation properties
        "dimensions": "platformq:dimensions",
        "overallScore": {
            "@id": "platformq:overallScore",
            "@type": "xsd:float"
        },
        "totalInteractions": {
            "@id": "platformq:totalInteractions",
            "@type": "xsd:integer"
        },
        
        # KYC properties
        "kycLevel": {
            "@id": "platformq:kycLevel",
            "@type": "xsd:integer"
        },
        "verifiedAttributes": "platformq:verifiedAttributes",
        "jurisdiction": "platformq:jurisdiction",
        "verificationDate": {
            "@id": "platformq:verificationDate",
            "@type": "xsd:dateTime"
        },
        
        # AML properties
        "riskScore": {
            "@id": "platformq:riskScore",
            "@type": "xsd:float"
        },
        "riskLevel": "platformq:riskLevel",
        "sanctionsCheck": "platformq:sanctionsCheck",
        "lastAssessment": {
            "@id": "platformq:lastAssessment",
            "@type": "xsd:dateTime"
        },
        "assessmentDetails": "platformq:assessmentDetails",
        
        # DAO properties
        "daoId": "platformq:daoId",
        "memberSince": {
            "@id": "platformq:memberSince",
            "@type": "xsd:dateTime"
        },
        "role": "platformq:role",
        "votingPower": {
            "@id": "platformq:votingPower",
            "@type": "xsd:integer"
        },
        "proposalsCreated": {
            "@id": "platformq:proposalsCreated",
            "@type": "xsd:integer"
        },
        "votesParticipated": {
            "@id": "platformq:votesParticipated",
            "@type": "xsd:integer"
        },
        
        # Blockchain anchoring
        "blockchainAnchor": "platformq:blockchainAnchor",
        "ipfsCID": "platformq:ipfsCID",
        "transactionHash": "platformq:transactionHash",
        "blockNumber": {
            "@id": "platformq:blockNumber",
            "@type": "xsd:integer"
        },
        
        # Metadata
        "tenantId": "platformq:tenantId",
        "metadata": "platformq:metadata"
    }
}

# Standard proof types
PROOF_TYPES = {
    "Ed25519Signature2020": {
        "@context": {
            "Ed25519Signature2020": {
                "@id": "https://w3id.org/security#Ed25519Signature2020",
                "@type": "sec:SignatureSuite",
                "canonicalizationAlgorithm": "https://w3id.org/security#URDNA2015",
                "signatureAlgorithm": "http://w3id.org/digests#ed25519"
            }
        }
    },
    "BbsBlsSignature2020": {
        "@context": {
            "BbsBlsSignature2020": {
                "@id": "https://w3id.org/security#BbsBlsSignature2020",
                "@type": "sec:SignatureSuite",
                "canonicalizationAlgorithm": "https://w3id.org/security#URDNA2015",
                "signatureAlgorithm": "https://w3id.org/security#BbsBlsSignature2020"
            }
        }
    }
}

# Standard credential status types
CREDENTIAL_STATUS_TYPES = {
    "RevocationList2020": {
        "@context": {
            "RevocationList2020": "https://w3id.org/vc-revocation-list-2020#RevocationList2020",
            "revocationListIndex": "https://w3id.org/vc-revocation-list-2020#revocationListIndex",
            "revocationListCredential": "https://w3id.org/vc-revocation-list-2020#revocationListCredential"
        }
    },
    "StatusList2021": {
        "@context": {
            "StatusList2021": "https://w3id.org/vc-status-list-2021#StatusList2021",
            "statusListIndex": "https://w3id.org/vc-status-list-2021#statusListIndex",
            "statusListCredential": "https://w3id.org/vc-status-list-2021#statusListCredential"
        }
    }
}


class JSON_LD_PROCESSOR:
    """Utilities for JSON-LD processing"""
    
    @staticmethod
    def compact(document: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """
        Compact a JSON-LD document
        Note: This is a simplified implementation. 
        For production, use pyld library.
        """
        # For now, just ensure context is set
        if isinstance(context, str):
            document["@context"] = context
        elif isinstance(context, list):
            document["@context"] = context
        elif isinstance(context, dict):
            document["@context"] = context
        
        return document
    
    @staticmethod
    def expand(document: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Expand a JSON-LD document
        Note: This is a simplified implementation.
        For production, use pyld library.
        """
        # For now, return as-is in a list
        return [document]
    
    @staticmethod
    def frame(document: Dict[str, Any], frame: Dict[str, Any]) -> Dict[str, Any]:
        """
        Frame a JSON-LD document
        Note: This is a simplified implementation.
        For production, use pyld library.
        """
        # For now, return document as-is
        return document


def get_full_context(credential_types: List[str]) -> List[Any]:
    """
    Build a complete JSON-LD context for given credential types
    
    Args:
        credential_types: List of credential type names
        
    Returns:
        List of context URLs and objects
    """
    contexts = [W3C_VC_CONTEXT]
    
    # Add security context if using advanced proofs
    if any(t in ["BbsBlsSignature2020", "JsonWebSignature2020"] for t in credential_types):
        contexts.append(W3C_SECURITY_V2_CONTEXT)
    
    # Add PlatformQ context if using custom types
    platformq_types = {
        "AchievementCredential", "ReputationCredential", "KYCCredential",
        "AMLComplianceCredential", "DAOMembershipCredential", "VotingPowerCredential",
        "ReputationScoreCredential", "ProposalApprovalCredential"
    }
    
    if any(t in platformq_types for t in credential_types):
        contexts.append(PLATFORMQ_CONTEXT)
    
    return contexts


def validate_context(context: Any) -> bool:
    """
    Validate that a context is properly formed
    
    Args:
        context: The @context value to validate
        
    Returns:
        True if valid, False otherwise
    """
    if isinstance(context, str):
        # Should be a valid URL
        return context.startswith("http://") or context.startswith("https://")
    elif isinstance(context, list):
        # All items should be strings or objects
        return all(
            isinstance(item, (str, dict)) for item in context
        ) and len(context) > 0
    elif isinstance(context, dict):
        # Should have @context key or be a valid context object
        return "@context" in context or "@version" in context
    else:
        return False


def normalize_credential_type(type_name: str) -> str:
    """
    Normalize a credential type name to standard format
    
    Args:
        type_name: The type name to normalize
        
    Returns:
        Normalized type name
    """
    # Remove spaces and ensure PascalCase
    parts = type_name.replace("_", " ").replace("-", " ").split()
    return "".join(word.capitalize() for word in parts)


def is_standard_type(type_name: str) -> bool:
    """
    Check if a type name is a W3C standard type
    
    Args:
        type_name: The type name to check
        
    Returns:
        True if standard type, False otherwise
    """
    standard_types = {
        "VerifiableCredential",
        "VerifiablePresentation",
        "RevocationList2020",
        "StatusList2021"
    }
    
    return type_name in standard_types 