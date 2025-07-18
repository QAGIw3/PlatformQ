"""
Unit tests for AML Verifiable Credential Integration Service
"""

import pytest
from unittest.mock import Mock, AsyncMock
from datetime import datetime, timedelta
from decimal import Decimal

from app.services.aml_vc_integration import AMLVCIntegrationService
from app.aml.aml_engine import RiskLevel, TransactionRiskAssessment
from app.aml.risk_assessment import SanctionsCheckResult, BlockchainAnalytics, ComplianceProvider


class TestAMLVCIntegrationService:
    """Test cases for AML VC Integration Service"""
    
    @pytest.fixture
    def mock_aml_engine(self):
        """Mock AML engine"""
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
    def aml_vc_integration(self, mock_aml_engine, mock_http_client):
        """Create AML VC integration service with mocks"""
        service = AMLVCIntegrationService(
            vc_service_url="http://test-vc-service:8000",
            aml_engine=mock_aml_engine,
            issuer_did="did:platform:test-compliance"
        )
        # Replace HTTP client with mock
        service.client = mock_http_client
        return service
        
    @pytest.fixture
    def sample_risk_assessment(self):
        """Sample risk assessment"""
        return TransactionRiskAssessment(
            transaction_id="txn_123",
            user_id="user_123",
            from_address="0x123",
            to_address="0x456",
            amount_usd=Decimal("5000"),
            asset_type="ETH",
            risk_score=0.4,
            risk_level=RiskLevel.MEDIUM,
            risk_factors=["high_value", "new_account"],
            requires_enhanced_verification=False,
            requires_manual_review=False,
            timestamp=datetime.utcnow(),
            chain="ethereum",
            metadata={"country": "US"}
        )
        
    @pytest.fixture
    def sample_sanctions_result(self):
        """Sample sanctions check result"""
        return SanctionsCheckResult(
            entity_name="John Doe",
            is_sanctioned=False,
            match_score=0.0,
            lists_matched=[],
            aliases=[],
            metadata={"checked_at": datetime.utcnow().isoformat()}
        )
        
    @pytest.fixture
    def sample_blockchain_analytics(self):
        """Sample blockchain analytics"""
        return BlockchainAnalytics(
            address="0x123",
            risk_score=0.2,
            risk_category="low",
            exposure_types=["exchange", "defi"],
            last_activity=datetime.utcnow(),
            total_received=Decimal("10000"),
            total_sent=Decimal("8000"),
            counterparties=15,
            provider=ComplianceProvider.CHAINALYSIS,
            raw_data={}
        )
        
    @pytest.mark.asyncio
    async def test_issue_aml_risk_credential(
        self,
        aml_vc_integration,
        mock_http_client,
        sample_risk_assessment
    ):
        """Test AML risk assessment credential issuance"""
        # Mock response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "credential_id": "vc_aml_risk_123",
            "status": "issued"
        }
        
        # Issue credential
        result = await aml_vc_integration.issue_aml_risk_credential(
            user_did="did:platform:user_123",
            risk_assessment=sample_risk_assessment
        )
        
        # Verify request
        call_args = mock_http_client.post.call_args
        request_data = call_args[1]["json"]
        
        assert request_data["type"] == ["VerifiableCredential", "AMLRiskAssessmentCredential"]
        assert request_data["issuer"] == "did:platform:test-compliance"
        assert request_data["credentialSubject"]["riskScore"] == 0.4
        assert request_data["credentialSubject"]["riskLevel"] == "MEDIUM"
        assert "high_value" in request_data["credentialSubject"]["riskFactors"]
        
        # Check result
        assert result["credential_id"] == "vc_aml_risk_123"
        
    @pytest.mark.asyncio
    async def test_issue_sanctions_check_credential(
        self,
        aml_vc_integration,
        mock_http_client,
        sample_sanctions_result
    ):
        """Test sanctions check credential issuance"""
        # Mock response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "credential_id": "vc_sanctions_123",
            "status": "issued"
        }
        
        # Issue credential
        result = await aml_vc_integration.issue_sanctions_check_credential(
            entity_did="did:platform:user_123",
            sanctions_result=sample_sanctions_result,
            validity_hours=24
        )
        
        # Verify request
        call_args = mock_http_client.post.call_args
        request_data = call_args[1]["json"]
        
        assert request_data["type"] == ["VerifiableCredential", "SanctionsCheckCredential"]
        assert request_data["credentialSubject"]["entityName"] == "John Doe"
        assert request_data["credentialSubject"]["isSanctioned"] is False
        assert request_data["credentialSubject"]["matchScore"] == 0.0
        
        # Check result
        assert result["credential_id"] == "vc_sanctions_123"
        
    @pytest.mark.asyncio
    async def test_issue_blockchain_analytics_credential(
        self,
        aml_vc_integration,
        mock_http_client,
        sample_blockchain_analytics
    ):
        """Test blockchain analytics credential issuance"""
        # Mock response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "credential_id": "vc_analytics_123",
            "status": "issued"
        }
        
        # Issue credential
        result = await aml_vc_integration.issue_blockchain_analytics_credential(
            address_did="did:blockchain:ethereum:0x123",
            analytics=sample_blockchain_analytics,
            chain="ethereum"
        )
        
        # Verify request
        call_args = mock_http_client.post.call_args
        request_data = call_args[1]["json"]
        
        assert request_data["type"] == ["VerifiableCredential", "BlockchainAnalyticsCredential"]
        assert request_data["credentialSubject"]["address"] == "0x123"
        assert request_data["credentialSubject"]["riskScore"] == 0.2
        assert request_data["credentialSubject"]["riskCategory"] == "low"
        assert request_data["credentialSubject"]["provider"] == "chainalysis"
        
        # Check result
        assert result["credential_id"] == "vc_analytics_123"
        
    @pytest.mark.asyncio
    async def test_create_aml_zkp_proof(
        self,
        aml_vc_integration,
        mock_http_client
    ):
        """Test AML ZKP proof creation"""
        # Mock response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "proof_id": "zkp_aml_123",
            "status": "generated"
        }
        
        # Create proof
        result = await aml_vc_integration.create_aml_zkp_proof(
            credential_id="vc_aml_risk_123",
            holder_did="did:platform:user_123",
            attributes_to_prove=["risk_score_below_threshold", "not_sanctioned"],
            verifier_challenge="challenge_123"
        )
        
        # Verify request
        call_args = mock_http_client.post.call_args
        request_data = call_args[1]["json"]
        
        assert request_data["credential_id"] == "vc_aml_risk_123"
        assert request_data["proof_type"] == "aml_compliance"
        assert "risk_score_below_threshold" in request_data["attributes_to_prove"]
        assert request_data["constraints"]["risk_score"]["max"] == 0.7
        assert request_data["constraints"]["is_sanctioned"]["equals"] is False
        
        # Check result
        assert result["proof_id"] == "zkp_aml_123"
        
    @pytest.mark.asyncio
    async def test_verify_aml_compliance_proof(
        self,
        aml_vc_integration,
        mock_http_client
    ):
        """Test AML compliance proof verification"""
        # Mock response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "valid": True,
            "attributes_verified": ["risk_score_below_threshold", "not_sanctioned"],
            "verified_at": datetime.utcnow().isoformat()
        }
        
        # Verify proof
        result = await aml_vc_integration.verify_aml_compliance_proof(
            proof_id="zkp_aml_123",
            required_checks=["risk_score_below_threshold", "not_sanctioned"],
            max_risk_score=0.7,
            verifier_challenge="challenge_123"
        )
        
        # Verify request
        call_args = mock_http_client.post.call_args
        request_data = call_args[1]["json"]
        
        assert request_data["proof_id"] == "zkp_aml_123"
        assert request_data["proof_type"] == "aml_compliance"
        assert request_data["constraints"]["risk_score"]["max"] == 0.7
        
        # Check result
        assert result["valid"] is True
        assert "risk_score_below_threshold" in result["attributes_verified"]
        
    @pytest.mark.asyncio
    async def test_create_aml_compliance_presentation(
        self,
        aml_vc_integration,
        mock_http_client
    ):
        """Test AML compliance presentation creation"""
        # Mock response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "presentation_id": "pres_aml_123",
            "status": "created"
        }
        
        # Create presentation
        result = await aml_vc_integration.create_aml_compliance_presentation(
            holder_did="did:platform:user_123",
            credential_ids=["vc_aml_risk_123", "vc_sanctions_123"],
            purpose="aml_compliance_check",
            verifier_did="did:platform:exchange",
            validity_minutes=30
        )
        
        # Verify request
        call_args = mock_http_client.post.call_args
        request_data = call_args[1]["json"]
        
        assert request_data["holder_did"] == "did:platform:user_123"
        assert len(request_data["credential_ids"]) == 2
        assert request_data["purpose"] == "aml_compliance_check"
        assert request_data["validity_minutes"] == 30
        assert request_data["selective_disclosure"]["risk_level"] is True
        assert request_data["selective_disclosure"]["is_sanctioned"] is False
        
        # Check derived predicates
        predicates = request_data["derived_predicates"]
        assert any(p["attribute"] == "risk_score" for p in predicates)
        assert any(p["attribute"] == "assessment_date" for p in predicates)
        
        # Check result
        assert result["presentation_id"] == "pres_aml_123"
        
    @pytest.mark.asyncio
    async def test_issue_transaction_monitoring_credential(
        self,
        aml_vc_integration,
        mock_http_client
    ):
        """Test transaction monitoring credential issuance"""
        # Mock response
        mock_http_client.post.return_value.status_code = 200
        mock_http_client.post.return_value.json.return_value = {
            "credential_id": "vc_monitoring_123",
            "status": "issued"
        }
        
        # Issue credential
        monitoring_period = {
            "start": "2024-01-01T00:00:00Z",
            "end": "2024-01-31T23:59:59Z"
        }
        
        summary = {
            "total_transactions": 150,
            "total_volume": "250000",
            "flagged_count": 2,
            "risk_indicators": ["high_frequency", "round_amounts"],
            "status": "compliant"
        }
        
        result = await aml_vc_integration.issue_transaction_monitoring_credential(
            user_did="did:platform:user_123",
            monitoring_period=monitoring_period,
            summary=summary
        )
        
        # Verify request
        call_args = mock_http_client.post.call_args
        request_data = call_args[1]["json"]
        
        assert request_data["type"] == ["VerifiableCredential", "TransactionMonitoringCredential"]
        assert request_data["credentialSubject"]["transactionCount"] == 150
        assert request_data["credentialSubject"]["totalVolume"] == "250000"
        assert request_data["credentialSubject"]["flaggedTransactions"] == 2
        assert request_data["credentialSubject"]["complianceStatus"] == "compliant"
        
        # Check result
        assert result["credential_id"] == "vc_monitoring_123"
        
    @pytest.mark.asyncio
    async def test_handle_http_error(
        self,
        aml_vc_integration,
        mock_http_client,
        sample_risk_assessment
    ):
        """Test HTTP error handling"""
        # Mock error response
        mock_http_client.post.return_value.status_code = 400
        mock_http_client.post.return_value.text = "Bad Request"
        
        # Attempt to issue credential
        with pytest.raises(Exception) as exc_info:
            await aml_vc_integration.issue_aml_risk_credential(
                user_did="did:platform:user_123",
                risk_assessment=sample_risk_assessment
            )
            
        assert "Failed to issue AML risk credential" in str(exc_info.value)
        
    @pytest.mark.asyncio
    async def test_close_client(
        self,
        aml_vc_integration,
        mock_http_client
    ):
        """Test client cleanup"""
        await aml_vc_integration.close()
        mock_http_client.aclose.assert_called_once() 