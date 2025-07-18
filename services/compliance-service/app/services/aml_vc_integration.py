"""
AML Verifiable Credential Integration Service
Provides privacy-preserving AML compliance using verifiable credentials
"""

import logging
import httpx
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal

from ..aml.aml_engine import AMLEngine, RiskLevel, TransactionRiskAssessment
from ..aml.risk_assessment import SanctionsCheckResult, BlockchainAnalytics

logger = logging.getLogger(__name__)


class AMLVCIntegrationService:
    """
    Service for integrating AML processes with Verifiable Credentials
    """
    
    def __init__(
        self,
        vc_service_url: str,
        aml_engine: AMLEngine,
        issuer_did: str = "did:platform:compliance-service"
    ):
        self.vc_service_url = vc_service_url
        self.aml_engine = aml_engine
        self.issuer_did = issuer_did
        self.client = httpx.AsyncClient(timeout=30.0)
        
    async def issue_aml_risk_credential(
        self,
        user_did: str,
        risk_assessment: TransactionRiskAssessment
    ) -> Dict[str, Any]:
        """Issue an AML risk assessment credential"""
        
        credential_data = {
            "@context": [
                "https://www.w3.org/2018/credentials/v1",
                "https://w3id.org/citizenship/v1"
            ],
            "type": ["VerifiableCredential", "AMLRiskAssessmentCredential"],
            "issuer": self.issuer_did,
            "issuanceDate": datetime.utcnow().isoformat(),
            "expirationDate": (datetime.utcnow() + timedelta(hours=24)).isoformat(),
            "credentialSubject": {
                "id": user_did,
                "riskScore": float(risk_assessment.risk_score),
                "riskLevel": risk_assessment.risk_level.value,
                "assessmentDate": risk_assessment.timestamp.isoformat(),
                "transactionId": risk_assessment.transaction_id,
                "requiresEnhancedVerification": risk_assessment.requires_enhanced_verification,
                "requiresManualReview": risk_assessment.requires_manual_review,
                "riskFactors": risk_assessment.risk_factors
            }
        }
        
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/issue",
            json=credential_data
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to issue AML risk credential: {response.text}")
            
        return response.json()
        
    async def issue_sanctions_check_credential(
        self,
        entity_did: str,
        sanctions_result: SanctionsCheckResult,
        validity_hours: int = 24
    ) -> Dict[str, Any]:
        """Issue a sanctions check credential"""
        
        credential_data = {
            "@context": [
                "https://www.w3.org/2018/credentials/v1",
                "https://w3id.org/security/v1"
            ],
            "type": ["VerifiableCredential", "SanctionsCheckCredential"],
            "issuer": self.issuer_did,
            "issuanceDate": datetime.utcnow().isoformat(),
            "expirationDate": (datetime.utcnow() + timedelta(hours=validity_hours)).isoformat(),
            "credentialSubject": {
                "id": entity_did,
                "entityName": sanctions_result.entity_name,
                "isSanctioned": sanctions_result.is_sanctioned,
                "matchScore": sanctions_result.match_score,
                "listsChecked": list(set(sanctions_result.lists_matched)) if sanctions_result.lists_matched else [],
                "checkDate": datetime.utcnow().isoformat(),
                "nextCheckRequired": (datetime.utcnow() + timedelta(hours=validity_hours)).isoformat()
            }
        }
        
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/issue",
            json=credential_data
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to issue sanctions check credential: {response.text}")
            
        return response.json()
        
    async def issue_blockchain_analytics_credential(
        self,
        address_did: str,
        analytics: BlockchainAnalytics,
        chain: str
    ) -> Dict[str, Any]:
        """Issue a blockchain analytics credential for an address"""
        
        credential_data = {
            "@context": [
                "https://www.w3.org/2018/credentials/v1",
                "https://w3id.org/blockchain/v1"
            ],
            "type": ["VerifiableCredential", "BlockchainAnalyticsCredential"],
            "issuer": self.issuer_did,
            "issuanceDate": datetime.utcnow().isoformat(),
            "expirationDate": (datetime.utcnow() + timedelta(days=7)).isoformat(),
            "credentialSubject": {
                "id": address_did,
                "address": analytics.address,
                "chain": chain,
                "riskScore": analytics.risk_score,
                "riskCategory": analytics.risk_category,
                "exposureTypes": analytics.exposure_types,
                "lastActivity": analytics.last_activity.isoformat(),
                "provider": analytics.provider.value,
                "analysisDate": datetime.utcnow().isoformat()
            }
        }
        
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/issue",
            json=credential_data
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to issue blockchain analytics credential: {response.text}")
            
        return response.json()
        
    async def create_aml_zkp_proof(
        self,
        credential_id: str,
        holder_did: str,
        attributes_to_prove: List[str],
        verifier_challenge: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a zero-knowledge proof for AML compliance"""
        
        # Define ZKP circuit for AML proofs
        zkp_request = {
            "credential_id": credential_id,
            "holder_did": holder_did,
            "proof_type": "aml_compliance",
            "attributes_to_prove": attributes_to_prove,
            "constraints": {
                "risk_score": {"max": 0.7},  # Prove risk score is below threshold
                "is_sanctioned": {"equals": False},  # Prove not sanctioned
                "risk_level": {"in": ["LOW", "MEDIUM"]}  # Prove acceptable risk level
            },
            "verifier_challenge": verifier_challenge
        }
        
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/zkp/generate",
            json=zkp_request
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to generate AML ZKP: {response.text}")
            
        return response.json()
        
    async def verify_aml_compliance_proof(
        self,
        proof_id: str,
        required_checks: List[str],
        max_risk_score: float = 0.7,
        verifier_challenge: Optional[str] = None
    ) -> Dict[str, Any]:
        """Verify an AML compliance zero-knowledge proof"""
        
        verification_request = {
            "proof_id": proof_id,
            "proof_type": "aml_compliance",
            "required_attributes": required_checks,
            "constraints": {
                "risk_score": {"max": max_risk_score},
                "is_sanctioned": {"equals": False}
            },
            "verifier_challenge": verifier_challenge
        }
        
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/zkp/verify",
            json=verification_request
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to verify AML proof: {response.text}")
            
        return response.json()
        
    async def create_aml_compliance_presentation(
        self,
        holder_did: str,
        credential_ids: List[str],
        purpose: str = "aml_compliance_check",
        verifier_did: Optional[str] = None,
        validity_minutes: int = 30
    ) -> Dict[str, Any]:
        """Create a verifiable presentation for AML compliance"""
        
        presentation_request = {
            "holder_did": holder_did,
            "credential_ids": credential_ids,
            "purpose": purpose,
            "verifier_did": verifier_did,
            "validity_minutes": validity_minutes,
            "selective_disclosure": {
                "risk_level": True,
                "is_sanctioned": False,  # Don't disclose actual sanctions data
                "assessment_date": True,
                "next_check_required": True
            },
            "derived_predicates": [
                {
                    "attribute": "risk_score",
                    "predicate": "less_than",
                    "threshold": 0.7
                },
                {
                    "attribute": "assessment_date",
                    "predicate": "after",
                    "threshold": (datetime.utcnow() - timedelta(days=1)).isoformat()
                }
            ]
        }
        
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/presentations",
            json=presentation_request
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to create AML presentation: {response.text}")
            
        return response.json()
        
    async def issue_transaction_monitoring_credential(
        self,
        user_did: str,
        monitoring_period: Dict[str, Any],
        summary: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Issue a credential summarizing transaction monitoring results"""
        
        credential_data = {
            "@context": [
                "https://www.w3.org/2018/credentials/v1",
                "https://w3id.org/security/v1"
            ],
            "type": ["VerifiableCredential", "TransactionMonitoringCredential"],
            "issuer": self.issuer_did,
            "issuanceDate": datetime.utcnow().isoformat(),
            "expirationDate": (datetime.utcnow() + timedelta(days=30)).isoformat(),
            "credentialSubject": {
                "id": user_did,
                "monitoringPeriod": {
                    "start": monitoring_period["start"],
                    "end": monitoring_period["end"]
                },
                "transactionCount": summary.get("total_transactions", 0),
                "totalVolume": str(summary.get("total_volume", 0)),
                "flaggedTransactions": summary.get("flagged_count", 0),
                "riskIndicators": summary.get("risk_indicators", []),
                "complianceStatus": summary.get("status", "compliant"),
                "nextReviewDate": (datetime.utcnow() + timedelta(days=30)).isoformat()
            }
        }
        
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/issue",
            json=credential_data
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to issue monitoring credential: {response.text}")
            
        return response.json()
        
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose() 