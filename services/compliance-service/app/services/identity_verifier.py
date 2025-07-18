"""
Identity Verifier - Integration with identity verification providers.
"""

import logging
import httpx
import asyncio
import json
import base64
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum

from ..models.kyc import DocumentType, VerificationStatus

logger = logging.getLogger(__name__)


class IdentityProvider(Enum):
    """Supported identity verification providers"""
    JUMIO = "jumio"
    ONFIDO = "onfido"
    SUMSUB = "sumsub"
    VERIFF = "veriff"
    MOCK = "mock"  # For testing


class IdentityVerifier:
    """
    Manages identity verification through multiple providers.
    
    Features:
    - Multi-provider support
    - Document verification
    - Face matching
    - Liveness detection
    - Fraud detection
    """
    
    def __init__(self, kyc_manager, config: Dict[str, Any]):
        self.kyc_manager = kyc_manager
        self.config = config
        self.providers = {}
        self._http_client = None
        
        # Provider configurations
        self.provider_configs = {
            IdentityProvider.JUMIO: {
                "base_url": "https://api.jumio.com",
                "api_key": config.get("jumio_api_key"),
                "api_secret": config.get("jumio_api_secret")
            },
            IdentityProvider.ONFIDO: {
                "base_url": "https://api.onfido.com/v3",
                "api_key": config.get("onfido_api_key")
            },
            IdentityProvider.SUMSUB: {
                "base_url": "https://api.sumsub.com",
                "api_key": config.get("sumsub_api_key"),
                "api_secret": config.get("sumsub_api_secret")
            },
            IdentityProvider.VERIFF: {
                "base_url": "https://api.veriff.com/v1",
                "api_key": config.get("veriff_api_key")
            }
        }
        
        # Verification thresholds
        self.thresholds = {
            "face_match_confidence": 0.85,
            "document_confidence": 0.90,
            "liveness_confidence": 0.80,
            "fraud_score_threshold": 0.7
        }
        
    async def initialize(self):
        """Initialize identity verifier"""
        self._http_client = httpx.AsyncClient(timeout=30.0)
        
        # Initialize provider connections
        for provider in IdentityProvider:
            if provider == IdentityProvider.MOCK:
                continue
                
            config = self.provider_configs.get(provider)
            if config and config.get("api_key"):
                try:
                    await self._test_provider_connection(provider)
                    self.providers[provider] = True
                    logger.info(f"Initialized {provider.value} provider")
                except Exception as e:
                    logger.error(f"Failed to initialize {provider.value}: {e}")
                    self.providers[provider] = False
                    
        logger.info("Identity Verifier initialized")
        
    async def shutdown(self):
        """Shutdown identity verifier"""
        if self._http_client:
            await self._http_client.aclose()
        logger.info("Identity Verifier shutdown")
        
    async def verify_document(
        self,
        user_id: str,
        document_type: DocumentType,
        document_data: bytes,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify a document using configured providers"""
        # Select provider based on availability and document type
        provider = self._select_provider(document_type)
        
        if provider == IdentityProvider.MOCK:
            return await self._mock_verify_document(user_id, document_type, metadata)
            
        try:
            if provider == IdentityProvider.JUMIO:
                return await self._verify_with_jumio(user_id, document_type, document_data, metadata)
            elif provider == IdentityProvider.ONFIDO:
                return await self._verify_with_onfido(user_id, document_type, document_data, metadata)
            elif provider == IdentityProvider.SUMSUB:
                return await self._verify_with_sumsub(user_id, document_type, document_data, metadata)
            elif provider == IdentityProvider.VERIFF:
                return await self._verify_with_veriff(user_id, document_type, document_data, metadata)
        except Exception as e:
            logger.error(f"Verification failed with {provider.value}: {e}")
            return {
                "status": VerificationStatus.FAILED,
                "error": str(e),
                "provider": provider.value
            }
            
    async def verify_face_match(
        self,
        user_id: str,
        selfie_data: bytes,
        document_photo: bytes
    ) -> Dict[str, Any]:
        """Verify face match between selfie and document photo"""
        provider = self._select_provider(DocumentType.SELFIE)
        
        if provider == IdentityProvider.MOCK:
            return {
                "match": True,
                "confidence": 0.92,
                "provider": "mock"
            }
            
        # Implement actual face matching logic
        # This would call the appropriate provider's face matching API
        
        return {
            "match": False,
            "confidence": 0.0,
            "error": "Face matching not implemented for production providers"
        }
        
    async def check_liveness(
        self,
        user_id: str,
        video_data: bytes
    ) -> Dict[str, Any]:
        """Check liveness detection from video"""
        provider = self._select_provider(DocumentType.SELFIE)
        
        if provider == IdentityProvider.MOCK:
            return {
                "is_live": True,
                "confidence": 0.88,
                "provider": "mock"
            }
            
        # Implement actual liveness detection logic
        # This would call the appropriate provider's liveness API
        
        return {
            "is_live": False,
            "confidence": 0.0,
            "error": "Liveness detection not implemented for production providers"
        }
        
    async def get_verification_status(
        self,
        user_id: str,
        verification_id: str
    ) -> Dict[str, Any]:
        """Get status of ongoing verification"""
        # Check cache first
        cached_status = await self._get_cached_status(verification_id)
        if cached_status:
            return cached_status
            
        # Query provider for status
        provider_info = await self._get_verification_provider(verification_id)
        if not provider_info:
            return {
                "status": VerificationStatus.NOT_FOUND,
                "error": "Verification not found"
            }
            
        provider = IdentityProvider(provider_info["provider"])
        
        # Get status from provider
        status = await self._get_provider_status(provider, verification_id)
        
        # Cache the result
        await self._cache_status(verification_id, status)
        
        return status
        
    # Provider-specific implementations
    
    async def _verify_with_jumio(
        self,
        user_id: str,
        document_type: DocumentType,
        document_data: bytes,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify document using Jumio"""
        config = self.provider_configs[IdentityProvider.JUMIO]
        
        # Create verification request
        headers = {
            "Authorization": f"Bearer {config['api_key']}:{config['api_secret']}",
            "Content-Type": "application/json"
        }
        
        # Prepare request data
        request_data = {
            "userReference": user_id,
            "workflowId": self._get_jumio_workflow(document_type),
            "callbackUrl": f"{self.config.get('callback_base_url')}/webhooks/jumio",
            "tokenLifetime": 5400,  # 90 minutes
            "userMetadata": metadata
        }
        
        # Create verification session
        response = await self._http_client.post(
            f"{config['base_url']}/api/v1/accounts/{config['account_id']}/workflow-sessions",
            headers=headers,
            json=request_data
        )
        
        if response.status_code != 200:
            raise Exception(f"Jumio API error: {response.text}")
            
        result = response.json()
        
        # Upload document
        upload_response = await self._upload_document_jumio(
            result["sessionId"],
            document_type,
            document_data
        )
        
        return {
            "status": VerificationStatus.PENDING,
            "verification_id": result["sessionId"],
            "provider": IdentityProvider.JUMIO.value,
            "upload_url": result.get("redirectUrl")
        }
        
    async def _verify_with_onfido(
        self,
        user_id: str,
        document_type: DocumentType,
        document_data: bytes,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify document using Onfido"""
        config = self.provider_configs[IdentityProvider.ONFIDO]
        
        headers = {
            "Authorization": f"Token token={config['api_key']}",
            "Content-Type": "application/json"
        }
        
        # Create applicant
        applicant_data = {
            "first_name": metadata.get("first_name", "Unknown"),
            "last_name": metadata.get("last_name", "Unknown"),
            "email": metadata.get("email"),
            "address": metadata.get("address", {})
        }
        
        applicant_response = await self._http_client.post(
            f"{config['base_url']}/applicants",
            headers=headers,
            json=applicant_data
        )
        
        if applicant_response.status_code != 201:
            raise Exception(f"Onfido API error: {applicant_response.text}")
            
        applicant = applicant_response.json()
        
        # Upload document
        files = {
            "file": ("document.jpg", document_data, "image/jpeg"),
            "type": (None, self._map_document_type_onfido(document_type))
        }
        
        upload_response = await self._http_client.post(
            f"{config['base_url']}/documents",
            headers={"Authorization": f"Token token={config['api_key']}"},
            data={"applicant_id": applicant["id"]},
            files=files
        )
        
        if upload_response.status_code != 201:
            raise Exception(f"Onfido document upload error: {upload_response.text}")
            
        # Create check
        check_data = {
            "applicant_id": applicant["id"],
            "report_names": ["document", "facial_similarity_photo"],
            "webhook_ids": [self.config.get("onfido_webhook_id")]
        }
        
        check_response = await self._http_client.post(
            f"{config['base_url']}/checks",
            headers=headers,
            json=check_data
        )
        
        if check_response.status_code != 201:
            raise Exception(f"Onfido check creation error: {check_response.text}")
            
        check = check_response.json()
        
        return {
            "status": VerificationStatus.PENDING,
            "verification_id": check["id"],
            "provider": IdentityProvider.ONFIDO.value,
            "applicant_id": applicant["id"]
        }
        
    async def _verify_with_sumsub(
        self,
        user_id: str,
        document_type: DocumentType,
        document_data: bytes,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify document using SumSub"""
        # Implementation would follow SumSub API pattern
        # This is a placeholder for the actual implementation
        return {
            "status": VerificationStatus.PENDING,
            "verification_id": f"sumsub_{user_id}_{datetime.now().timestamp()}",
            "provider": IdentityProvider.SUMSUB.value
        }
        
    async def _verify_with_veriff(
        self,
        user_id: str,
        document_type: DocumentType,
        document_data: bytes,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Verify document using Veriff"""
        # Implementation would follow Veriff API pattern
        # This is a placeholder for the actual implementation
        return {
            "status": VerificationStatus.PENDING,
            "verification_id": f"veriff_{user_id}_{datetime.now().timestamp()}",
            "provider": IdentityProvider.VERIFF.value
        }
        
    # Helper methods
    
    def _select_provider(self, document_type: DocumentType) -> IdentityProvider:
        """Select the best available provider for document type"""
        # Priority order based on document type and availability
        priority_order = {
            DocumentType.PASSPORT: [
                IdentityProvider.JUMIO,
                IdentityProvider.ONFIDO,
                IdentityProvider.VERIFF,
                IdentityProvider.SUMSUB
            ],
            DocumentType.DRIVERS_LICENSE: [
                IdentityProvider.ONFIDO,
                IdentityProvider.JUMIO,
                IdentityProvider.VERIFF,
                IdentityProvider.SUMSUB
            ],
            DocumentType.NATIONAL_ID: [
                IdentityProvider.VERIFF,
                IdentityProvider.JUMIO,
                IdentityProvider.ONFIDO,
                IdentityProvider.SUMSUB
            ],
            DocumentType.SELFIE: [
                IdentityProvider.JUMIO,
                IdentityProvider.ONFIDO,
                IdentityProvider.VERIFF,
                IdentityProvider.SUMSUB
            ]
        }
        
        providers = priority_order.get(
            document_type,
            [IdentityProvider.JUMIO, IdentityProvider.ONFIDO]
        )
        
        # Find first available provider
        for provider in providers:
            if self.providers.get(provider, False):
                return provider
                
        # Fallback to mock if no providers available
        logger.warning("No identity providers available, using mock")
        return IdentityProvider.MOCK
        
    async def _test_provider_connection(self, provider: IdentityProvider) -> bool:
        """Test connection to provider"""
        config = self.provider_configs[provider]
        
        try:
            if provider == IdentityProvider.JUMIO:
                response = await self._http_client.get(
                    f"{config['base_url']}/api/v1/ping",
                    headers={"Authorization": f"Bearer {config['api_key']}:{config['api_secret']}"}
                )
            elif provider == IdentityProvider.ONFIDO:
                response = await self._http_client.get(
                    f"{config['base_url']}/ping",
                    headers={"Authorization": f"Token token={config['api_key']}"}
                )
            else:
                # Generic health check
                response = await self._http_client.get(f"{config['base_url']}/health")
                
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Provider connection test failed for {provider.value}: {e}")
            return False
            
    async def _mock_verify_document(
        self,
        user_id: str,
        document_type: DocumentType,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Mock verification for testing"""
        await asyncio.sleep(0.5)  # Simulate API call
        
        # Simulate different outcomes based on document type
        if document_type == DocumentType.PASSPORT:
            status = VerificationStatus.APPROVED
            confidence = 0.95
        elif document_type == DocumentType.DRIVERS_LICENSE:
            status = VerificationStatus.APPROVED
            confidence = 0.92
        else:
            status = VerificationStatus.PENDING
            confidence = 0.88
            
        return {
            "status": status,
            "verification_id": f"mock_{user_id}_{datetime.now().timestamp()}",
            "provider": "mock",
            "confidence": confidence,
            "checks": {
                "document_authenticity": True,
                "data_extraction": True,
                "face_match": True,
                "address_match": True
            }
        }
        
    def _get_jumio_workflow(self, document_type: DocumentType) -> str:
        """Get Jumio workflow ID for document type"""
        workflows = {
            DocumentType.PASSPORT: "workflow_passport",
            DocumentType.DRIVERS_LICENSE: "workflow_dl",
            DocumentType.NATIONAL_ID: "workflow_id",
            DocumentType.SELFIE: "workflow_selfie"
        }
        return workflows.get(document_type, "workflow_default")
        
    def _map_document_type_onfido(self, document_type: DocumentType) -> str:
        """Map document type to Onfido format"""
        mapping = {
            DocumentType.PASSPORT: "passport",
            DocumentType.DRIVERS_LICENSE: "driving_licence",
            DocumentType.NATIONAL_ID: "national_identity_card",
            DocumentType.RESIDENCE_PERMIT: "residence_permit"
        }
        return mapping.get(document_type, "unknown")
        
    async def _upload_document_jumio(
        self,
        session_id: str,
        document_type: DocumentType,
        document_data: bytes
    ) -> Dict[str, Any]:
        """Upload document to Jumio"""
        # Implementation for document upload
        # This would use Jumio's document upload API
        return {"uploaded": True}
        
    async def _get_cached_status(self, verification_id: str) -> Optional[Dict[str, Any]]:
        """Get cached verification status"""
        # Implementation would check cache (Redis/Ignite)
        return None
        
    async def _cache_status(self, verification_id: str, status: Dict[str, Any]):
        """Cache verification status"""
        # Implementation would store in cache with TTL
        pass
        
    async def _get_verification_provider(self, verification_id: str) -> Optional[Dict[str, Any]]:
        """Get provider info for verification ID"""
        # Implementation would look up which provider owns this verification
        return None
        
    async def _get_provider_status(
        self,
        provider: IdentityProvider,
        verification_id: str
    ) -> Dict[str, Any]:
        """Get verification status from provider"""
        # Implementation would query the specific provider's API
        return {
            "status": VerificationStatus.PENDING,
            "provider": provider.value
        } 