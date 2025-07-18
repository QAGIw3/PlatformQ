"""
Unit tests for KYC Verifiable Credential Integration Service
"""

import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime, timedelta

from app.services.kyc_vc_integration import VCIntegrationService
from app.core.kyc_manager import KYCStatus, KYCLevel
from app.models.kyc import KYCVerification


class TestVCIntegrationService:
    """Test cases for VC Integration Service"""
    
    @pytest.fixture
    def mock_kyc_manager(self):
        """Mock KYC manager"""
        mock = Mock()
        return mock
        
    @pytest.fixture
    def mock_http_client(self):
        """Mock HTTP client"""
        mock = AsyncMock()
        mock.post = AsyncMock()
        mock.aclose = AsyncMock()
        return mock
        
    @pytest.fixture
    def vc_integration(self, mock_kyc_manager, mock_http_client):
        """Create VC integration service with mocks"""
        service = VCIntegrationService(
            vc_service_url="http://test-vc-service:8000",
            kyc_manager=mock_kyc_manager,
            issuer_did="did:platform:test-compliance"
        )
        # Replace HTTP client with mock
        service.client = mock_http_client
        return service
        
    @pytest.fixture
    def sample_verification(self):
        """Sample KYC verification"""
        return KYCVerification(
            id="test_verification_123",
            user_id="user_123",
            level=2,
            status="approved",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=365),
            required_documents=["passport", "proof_of_address"],
            completed_checks=["identity", "address", "sanctions"],
            pending_checks=[],
            result={
                "identity_verified": True,
                "address_verified": True,
                "age_over_18": True,
                "country": "US",
                "risk_score": 0.2
            }
        )
        
    @pytest.mark.asyncio
    async def test_issue_kyc_credential_success(
        self,
        vc_integration,
        mock_http_client,
        sample_verification
    ):
        """Test successful KYC credential issuance"""
        # Mock VC service response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "credential_id": "vc_kyc_123",
            "holder_did": "did:platform:user_123",
            "kyc_level": 2,
            "expires_at": (datetime.utcnow() + timedelta(days=365)).isoformat(),
            "status": "created"
        }
        
        # Issue credential
        result = await vc_integration.issue_kyc_credential(
            verification=sample_verification,
            user_did="did:platform:user_123"
        )
        
        # Verify request
        mock_http_client.post.assert_called_once()
        call_args = mock_http_client.post.call_args
        
        assert call_args[0][0] == "http://test-vc-service:8000/api/v1/credentials/kyc/create"
        assert call_args[1]["json"]["holder_did"] == "did:platform:user_123"
        assert call_args[1]["json"]["issuer_did"] == "did:platform:test-compliance"
        assert call_args[1]["json"]["kyc_level"] == 2
        
        # Verify response
        assert result["credential_id"] == "vc_kyc_123"
        assert result["kyc_level"] == 2
        
    @pytest.mark.asyncio
    async def test_issue_kyc_credential_failure(
        self,
        vc_integration,
        mock_http_client,
        sample_verification
    ):
        """Test KYC credential issuance failure"""
        # Mock VC service error
        mock_http_client.post.return_value.status_code = 500
        mock_http_client.post.return_value.text = "Internal server error"
        
        # Attempt to issue credential
        with pytest.raises(Exception) as exc_info:
            await vc_integration.issue_kyc_credential(
                verification=sample_verification,
                user_did="did:platform:user_123"
            )
            
        assert "Failed to create KYC credential" in str(exc_info.value)
        
    @pytest.mark.asyncio
    async def test_generate_kyc_proof(
        self,
        vc_integration,
        mock_http_client
    ):
        """Test KYC proof generation"""
        # Mock response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "proof_id": "proof_123",
            "attributes_proven": ["age_over_18", "identity_verified"],
            "kyc_level": "TIER_2",
            "expires_at": (datetime.utcnow() + timedelta(hours=1)).isoformat(),
            "status": "generated"
        }
        
        # Generate proof
        result = await vc_integration.generate_kyc_proof(
            credential_id="vc_kyc_123",
            holder_did="did:platform:user_123",
            attributes_to_prove=["age_over_18", "identity_verified"],
            min_kyc_level=2,
            verifier_challenge="challenge_123"
        )
        
        # Verify request
        call_args = mock_http_client.post.call_args
        request_data = call_args[1]["json"]
        
        assert request_data["credential_id"] == "vc_kyc_123"
        assert request_data["attributes_to_prove"] == ["age_over_18", "identity_verified"]
        assert request_data["min_kyc_level"] == 2
        assert request_data["verifier_challenge"] == "challenge_123"
        
        # Verify response
        assert result["proof_id"] == "proof_123"
        assert result["status"] == "generated"
        
    @pytest.mark.asyncio
    async def test_verify_kyc_proof(
        self,
        vc_integration,
        mock_http_client
    ):
        """Test KYC proof verification"""
        # Mock response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "valid": True,
            "attributes_verified": ["age_over_18", "identity_verified"],
            "kyc_level": "TIER_2",
            "verified_at": datetime.utcnow().isoformat()
        }
        
        # Verify proof
        result = await vc_integration.verify_kyc_proof(
            proof_id="proof_123",
            required_attributes=["age_over_18", "identity_verified"],
            min_kyc_level=2,
            verifier_challenge="challenge_123"
        )
        
        # Check result
        assert result["valid"] is True
        assert "age_over_18" in result["attributes_verified"]
        
    @pytest.mark.asyncio
    async def test_prepare_kyc_attributes_tier1(
        self,
        vc_integration
    ):
        """Test KYC attribute preparation for Tier 1"""
        verification = KYCVerification(
            id="test_123",
            user_id="user_123",
            level=1,
            status="approved",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=365),
            required_documents=[],
            completed_checks=["email", "phone"],
            pending_checks=[],
            result={}
        )
        
        attributes = await vc_integration._prepare_kyc_attributes(verification)
        
        assert attributes["kyc_level"] == 1
        assert attributes["identity_verified"] is True
        assert attributes["email_verified"] is True
        assert attributes["phone_verified"] is True
        assert attributes.get("address_verified") is None
        
    @pytest.mark.asyncio
    async def test_prepare_kyc_attributes_tier2(
        self,
        vc_integration
    ):
        """Test KYC attribute preparation for Tier 2"""
        verification = KYCVerification(
            id="test_123",
            user_id="user_123",
            level=2,
            status="approved",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(days=365),
            required_documents=["passport", "proof_of_address"],
            completed_checks=["identity", "address", "sanctions"],
            pending_checks=[],
            result={
                "country": "US",
                "country_not_sanctioned": True,
                "age_verified": True,
                "age_over_18": True,
                "age_over_21": True,
                "risk_score": 0.3
            }
        )
        
        attributes = await vc_integration._prepare_kyc_attributes(verification)
        
        assert attributes["kyc_level"] == 2
        assert attributes["identity_verified"] is True
        assert attributes["address_verified"] is True
        assert attributes["enhanced_due_diligence"] is True
        assert attributes["country_not_sanctioned"] is True
        assert attributes["age_over_18"] is True
        assert attributes["risk_level"] == "medium"
        
    @pytest.mark.asyncio
    async def test_issue_document_verification_credential(
        self,
        vc_integration,
        mock_http_client
    ):
        """Test document verification credential issuance"""
        # Mock response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "credential_id": "vc_doc_123",
            "status": "issued"
        }
        
        # Issue document credential
        result = await vc_integration.issue_document_verification_credential(
            user_did="did:platform:user_123",
            document_type="passport",
            verification_result={
                "status": "verified",
                "method": "automated",
                "confidence": 0.98
            }
        )
        
        # Verify request
        call_args = mock_http_client.post.call_args
        request_data = call_args[1]["json"]
        
        assert request_data["type"] == ["VerifiableCredential", "DocumentVerificationCredential"]
        assert request_data["credentialSubject"]["documentType"] == "passport"
        assert request_data["credentialSubject"]["verificationStatus"] == "verified"
        assert request_data["credentialSubject"]["confidence"] == 0.98
        
        # Check result
        assert result["credential_id"] == "vc_doc_123"
        
    @pytest.mark.asyncio
    async def test_create_kyc_presentation(
        self,
        vc_integration,
        mock_http_client
    ):
        """Test KYC presentation creation"""
        # Mock response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "presentation_id": "pres_123",
            "status": "created"
        }
        
        # Create presentation
        result = await vc_integration.create_kyc_presentation(
            user_did="did:platform:user_123",
            credential_ids=["vc_kyc_123", "vc_doc_123"],
            purpose="defi_trading",
            verifier_did="did:platform:defi_protocol"
        )
        
        # Verify request
        call_args = mock_http_client.post.call_args
        request_data = call_args[1]["json"]
        
        assert request_data["holder_did"] == "did:platform:user_123"
        assert len(request_data["credential_ids"]) == 2
        assert request_data["purpose"] == "defi_trading"
        assert request_data["verifier_did"] == "did:platform:defi_protocol"
        assert request_data["selective_disclosure"]["kyc_level"] is True
        
        # Check result
        assert result["presentation_id"] == "pres_123"
        
    @pytest.mark.asyncio
    async def test_calculate_risk_level(
        self,
        vc_integration
    ):
        """Test risk level calculation"""
        assert vc_integration._calculate_risk_level(0.2) == "low"
        assert vc_integration._calculate_risk_level(0.5) == "medium"
        assert vc_integration._calculate_risk_level(0.7) == "high"
        assert vc_integration._calculate_risk_level(0.9) == "critical"
        
    @pytest.mark.asyncio
    async def test_close_client(
        self,
        vc_integration,
        mock_http_client
    ):
        """Test client cleanup"""
        await vc_integration.close()
        mock_http_client.aclose.assert_called_once() 