"""
KYC Verifiable Credential Integration Service
Provides seamless integration between Compliance Service and VC Service
"""

import logging
import httpx
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum

from ..core.kyc_manager import KYCManager, KYCStatus, KYCLevel
from ..models.kyc import KYCVerification

logger = logging.getLogger(__name__)


class VCIntegrationService:
    """
    Service for integrating KYC processes with Verifiable Credentials
    """
    
    def __init__(
        self,
        vc_service_url: str,
        kyc_manager: KYCManager,
        issuer_did: str = "did:platform:compliance-service"
    ):
        self.vc_service_url = vc_service_url
        self.kyc_manager = kyc_manager
        self.issuer_did = issuer_did
        self.client = httpx.AsyncClient(timeout=30.0)
        
    async def issue_kyc_credential(
        self,
        verification: KYCVerification,
        user_did: str
    ) -> Dict[str, Any]:
        """Issue a KYC verifiable credential after successful verification"""
        
        # Prepare KYC data for credential
        kyc_attributes = await self._prepare_kyc_attributes(verification)
        
        # Create KYC credential via VC service
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/credentials/kyc/create",
            json={
                "holder_did": user_did,
                "issuer_did": self.issuer_did,
                "kyc_data": kyc_attributes,
                "kyc_level": verification.level
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to create KYC credential: {response.text}")
            
        credential_data = response.json()
        logger.info(f"Issued KYC credential {credential_data['credential_id']} for {user_did}")
        
        return credential_data
        
    async def generate_kyc_proof(
        self,
        credential_id: str,
        holder_did: str,
        attributes_to_prove: List[str],
        min_kyc_level: int,
        verifier_challenge: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate a zero-knowledge proof for KYC attributes"""
        
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/zkp/kyc/generate-proof",
            json={
                "credential_id": credential_id,
                "holder_did": holder_did,
                "attributes_to_prove": attributes_to_prove,
                "min_kyc_level": min_kyc_level,
                "verifier_challenge": verifier_challenge
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to generate KYC proof: {response.text}")
            
        return response.json()
        
    async def verify_kyc_proof(
        self,
        proof_id: str,
        required_attributes: List[str],
        min_kyc_level: int,
        verifier_challenge: Optional[str] = None
    ) -> Dict[str, Any]:
        """Verify a zero-knowledge proof of KYC status"""
        
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/zkp/kyc/verify-proof",
            json={
                "proof_id": proof_id,
                "required_attributes": required_attributes,
                "min_kyc_level": min_kyc_level,
                "verifier_challenge": verifier_challenge
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to verify KYC proof: {response.text}")
            
        return response.json()
        
    async def issue_document_verification_credential(
        self,
        user_did: str,
        document_type: str,
        verification_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Issue a credential for verified documents"""
        
        credential_data = {
            "@context": ["https://www.w3.org/2018/credentials/v1"],
            "type": ["VerifiableCredential", "DocumentVerificationCredential"],
            "issuer": self.issuer_did,
            "issuanceDate": datetime.utcnow().isoformat(),
            "expirationDate": (datetime.utcnow() + timedelta(days=365)).isoformat(),
            "credentialSubject": {
                "id": user_did,
                "documentType": document_type,
                "verificationStatus": verification_result.get("status"),
                "verifiedAt": datetime.utcnow().isoformat(),
                "verificationMethod": verification_result.get("method", "automated"),
                "confidence": verification_result.get("confidence", 0.95)
            }
        }
        
        # Issue via direct issuance endpoint
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/issue",
            json=credential_data
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to issue document credential: {response.text}")
            
        return response.json()
        
    async def create_kyc_presentation(
        self,
        user_did: str,
        credential_ids: List[str],
        purpose: str,
        verifier_did: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a verifiable presentation bundling KYC credentials"""
        
        response = await self.client.post(
            f"{self.vc_service_url}/api/v1/presentations",
            json={
                "holder_did": user_did,
                "credential_ids": credential_ids,
                "purpose": purpose,
                "verifier_did": verifier_did,
                "selective_disclosure": {
                    "kyc_level": True,
                    "verified_date": True,
                    "expiry_date": True
                }
            }
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to create presentation: {response.text}")
            
        return response.json()
        
    async def _prepare_kyc_attributes(
        self,
        verification: KYCVerification
    ) -> Dict[str, Any]:
        """Prepare KYC attributes for credential issuance"""
        
        attributes = {
            "kyc_level": verification.level,
            "verified_at": verification.completed_at.isoformat() if verification.completed_at else None,
            "expires_at": verification.expires_at.isoformat() if verification.expires_at else None,
            "verification_id": verification.id,
            "completed_checks": verification.completed_checks or [],
        }
        
        # Add level-specific attributes
        if verification.level >= 1:
            attributes.update({
                "identity_verified": True,
                "email_verified": True,
                "phone_verified": True
            })
            
        if verification.level >= 2:
            attributes.update({
                "address_verified": True,
                "document_verified": True,
                "enhanced_due_diligence": True
            })
            
        if verification.level >= 3:
            attributes.update({
                "institutional_verified": True,
                "source_of_funds_verified": True,
                "business_verification": True
            })
            
        # Add result-specific attributes
        if verification.result:
            # Country verification
            if "country" in verification.result:
                attributes["country_not_sanctioned"] = verification.result.get("country_not_sanctioned", True)
                
            # Age verification
            if "age_verified" in verification.result:
                attributes["age_over_18"] = verification.result.get("age_over_18", False)
                attributes["age_over_21"] = verification.result.get("age_over_21", False)
                
            # Risk assessment
            if "risk_score" in verification.result:
                attributes["risk_level"] = self._calculate_risk_level(verification.result["risk_score"])
                
        return attributes
        
    def _calculate_risk_level(self, risk_score: float) -> str:
        """Calculate risk level from score"""
        if risk_score < 0.3:
            return "low"
        elif risk_score < 0.6:
            return "medium"
        elif risk_score < 0.8:
            return "high"
        else:
            return "critical"
            
    async def close(self):
        """Close HTTP client"""
        await self.client.aclose() 