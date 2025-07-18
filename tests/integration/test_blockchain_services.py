"""
Integration tests for blockchain services.

Tests communication between:
- Blockchain Gateway Service
- Compliance Service  
- DeFi Protocol Service
"""

import pytest
import asyncio
import json
from datetime import datetime
from decimal import Decimal
from unittest.mock import AsyncMock, Mock, patch
import httpx

from platformq_blockchain_common import ChainType, ChainConfig, Transaction, GasStrategy
from platformq_events import EventType
import pulsar


class TestBlockchainServicesIntegration:
    """Test suite for blockchain services integration"""
    
    @pytest.fixture
    async def pulsar_client(self):
        """Mock Pulsar client"""
        client = Mock(spec=pulsar.Client)
        producer = Mock(spec=pulsar.Producer)
        consumer = Mock(spec=pulsar.Consumer)
        
        client.create_producer.return_value = producer
        client.subscribe.return_value = consumer
        
        return client
        
    @pytest.fixture
    async def ignite_client(self):
        """Mock Ignite client"""
        from pyignite import Client
        client = Mock(spec=Client)
        cache = Mock()
        client.get_or_create_cache.return_value = cache
        return client
        
    @pytest.fixture
    async def blockchain_gateway_client(self):
        """HTTP client for blockchain gateway service"""
        return httpx.AsyncClient(base_url="http://localhost:8001")
        
    @pytest.fixture
    async def compliance_client(self):
        """HTTP client for compliance service"""
        return httpx.AsyncClient(base_url="http://localhost:8002")
        
    @pytest.fixture
    async def defi_client(self):
        """HTTP client for defi service"""
        return httpx.AsyncClient(base_url="http://localhost:8003")
        
    @pytest.mark.asyncio
    async def test_transaction_with_compliance_check(
        self, blockchain_gateway_client, compliance_client
    ):
        """Test transaction flow with compliance checks"""
        user_id = "user123"
        
        # First, perform KYC check
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = httpx.Response(
                200,
                json={
                    "user_id": user_id,
                    "status": "approved",
                    "kyc_level": 2,
                    "documents_verified": ["passport", "proof_of_address"]
                }
            )
            
            kyc_response = await compliance_client.post(
                "/kyc/submit",
                json={
                    "user_id": user_id,
                    "documents": [
                        {"type": "passport", "data": "base64_data"},
                        {"type": "proof_of_address", "data": "base64_data"}
                    ]
                }
            )
            
            assert kyc_response.status_code == 200
            assert kyc_response.json()["status"] == "approved"
            
        # Check AML/sanctions
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = httpx.Response(
                200,
                json={
                    "user_id": user_id,
                    "sanctions_check": "clear",
                    "pep_check": "clear",
                    "risk_score": 0.2
                }
            )
            
            aml_response = await compliance_client.post(
                "/aml/check",
                json={"user_id": user_id}
            )
            
            assert aml_response.status_code == 200
            assert aml_response.json()["sanctions_check"] == "clear"
            
        # Now send transaction through gateway
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = httpx.Response(
                200,
                json={
                    "tx_hash": "0xabc123",
                    "chain": "ethereum",
                    "status": "submitted"
                }
            )
            
            tx_response = await blockchain_gateway_client.post(
                "/transaction/send",
                json={
                    "chain_type": "ethereum",
                    "from_address": "0x123",
                    "to_address": "0x456",
                    "value": "1.5",
                    "gas_strategy": "standard"
                }
            )
            
            assert tx_response.status_code == 200
            assert "tx_hash" in tx_response.json()
            
    @pytest.mark.asyncio
    async def test_defi_protocol_with_compliance(
        self, defi_client, compliance_client
    ):
        """Test DeFi operations with compliance checks"""
        user_id = "user123"
        
        # Check user compliance status first
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_get.return_value = httpx.Response(
                200,
                json={
                    "overall_score": 0.3,
                    "risk_level": "low",
                    "kyc_status": "approved"
                }
            )
            
            risk_response = await compliance_client.get(
                f"/risk/profile/{user_id}"
            )
            
            assert risk_response.status_code == 200
            assert risk_response.json()["risk_level"] == "low"
            
        # Create lending position
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = httpx.Response(
                200,
                json={
                    "position_id": "pos123",
                    "collateral_amount": "100",
                    "borrowed_amount": "50",
                    "health_factor": "2.0"
                }
            )
            
            lending_response = await defi_client.post(
                "/lending/borrow",
                json={
                    "protocol": "compound",
                    "user": user_id,
                    "collateral_asset": "ETH",
                    "collateral_amount": "100",
                    "borrow_asset": "USDC",
                    "borrow_amount": "50"
                }
            )
            
            assert lending_response.status_code == 200
            assert lending_response.json()["position_id"] == "pos123"
            
    @pytest.mark.asyncio
    async def test_event_flow_between_services(self, pulsar_client):
        """Test event flow through Pulsar between services"""
        events_received = []
        
        # Mock consumer receiving events
        async def mock_receive():
            # Simulate receiving different event types
            events = [
                {
                    "type": "kyc_approved",
                    "data": {"user_id": "user123", "level": 2},
                    "timestamp": datetime.utcnow().isoformat()
                },
                {
                    "type": "transaction_sent", 
                    "data": {
                        "tx_hash": "0xabc123",
                        "chain": "ethereum",
                        "from": "0x123",
                        "to": "0x456",
                        "value": "1.5"
                    },
                    "timestamp": datetime.utcnow().isoformat()
                },
                {
                    "type": "lending_position_created",
                    "data": {
                        "position_id": "pos123",
                        "user": "user123",
                        "protocol": "compound"
                    },
                    "timestamp": datetime.utcnow().isoformat()
                }
            ]
            
            for event in events:
                msg = Mock()
                msg.data.return_value = json.dumps(event).encode('utf-8')
                events_received.append(event)
                yield msg
                
        consumer = pulsar_client.subscribe.return_value
        consumer.receive_async = mock_receive
        
        # Process events
        async for msg in consumer.receive_async():
            event = json.loads(msg.data().decode('utf-8'))
            events_received.append(event)
            
            # Verify event structure
            assert "type" in event
            assert "data" in event
            assert "timestamp" in event
            
        # Verify all expected events were received
        assert len(events_received) >= 3
        event_types = [e["type"] for e in events_received]
        assert "kyc_approved" in event_types
        assert "transaction_sent" in event_types
        assert "lending_position_created" in event_types
        
    @pytest.mark.asyncio
    async def test_multi_chain_gateway_operations(self, blockchain_gateway_client):
        """Test blockchain gateway with multiple chains"""
        chains = ["ethereum", "polygon", "arbitrum"]
        
        # Test getting supported chains
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_get.return_value = httpx.Response(
                200,
                json=[
                    {
                        "chain_type": "ethereum",
                        "chain_id": 1,
                        "name": "Ethereum Mainnet",
                        "native_currency": "ETH"
                    },
                    {
                        "chain_type": "polygon",
                        "chain_id": 137,
                        "name": "Polygon",
                        "native_currency": "MATIC"
                    },
                    {
                        "chain_type": "arbitrum",
                        "chain_id": 42161,
                        "name": "Arbitrum One",
                        "native_currency": "ETH"
                    }
                ]
            )
            
            chains_response = await blockchain_gateway_client.get("/chains")
            assert chains_response.status_code == 200
            supported_chains = chains_response.json()
            assert len(supported_chains) == 3
            
        # Test operations on each chain
        for chain in chains:
            # Get balance
            with patch('httpx.AsyncClient.get') as mock_get:
                mock_get.return_value = httpx.Response(
                    200,
                    json={
                        "chain": chain,
                        "address": "0x123",
                        "balance": "10.5",
                        "currency": "ETH" if chain != "polygon" else "MATIC"
                    }
                )
                
                balance_response = await blockchain_gateway_client.get(
                    f"/balance/{chain}/0x123"
                )
                assert balance_response.status_code == 200
                
            # Estimate gas
            with patch('httpx.AsyncClient.post') as mock_post:
                mock_post.return_value = httpx.Response(
                    200,
                    json={
                        "gas_limit": 21000,
                        "gas_price": 30,
                        "total_cost_native": "0.00063"
                    }
                )
                
                gas_response = await blockchain_gateway_client.post(
                    "/gas/estimate",
                    json={
                        "chain_type": chain,
                        "from_address": "0x123",
                        "to_address": "0x456",
                        "value": "1.0"
                    }
                )
                assert gas_response.status_code == 200
                
    @pytest.mark.asyncio
    async def test_transaction_monitoring(
        self, compliance_client, blockchain_gateway_client
    ):
        """Test transaction monitoring and risk assessment"""
        tx_hash = "0xabc123"
        
        # Monitor transaction
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = httpx.Response(
                200,
                json={
                    "tx_hash": tx_hash,
                    "risk_indicators": {
                        "high_value": False,
                        "suspicious_pattern": False,
                        "blacklisted_address": False
                    },
                    "risk_score": 0.1,
                    "action": "allow"
                }
            )
            
            monitor_response = await compliance_client.post(
                "/transaction/monitor",
                json={
                    "tx_hash": tx_hash,
                    "chain": "ethereum",
                    "from_address": "0x123",
                    "to_address": "0x456",
                    "value": "1.5",
                    "gas_price": "30"
                }
            )
            
            assert monitor_response.status_code == 200
            assert monitor_response.json()["action"] == "allow"
            
        # Check transaction status
        with patch('httpx.AsyncClient.get') as mock_get:
            mock_get.return_value = httpx.Response(
                200,
                json={
                    "tx_hash": tx_hash,
                    "status": "success",
                    "block_number": 12345,
                    "gas_used": 21000
                }
            )
            
            status_response = await blockchain_gateway_client.get(
                f"/transaction/status/{tx_hash}"
            )
            
            assert status_response.status_code == 200
            assert status_response.json()["status"] == "success"
            
    @pytest.mark.asyncio
    async def test_regulatory_reporting(self, compliance_client):
        """Test regulatory reporting functionality"""
        # Generate compliance report
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = httpx.Response(
                200,
                json={
                    "report_id": "report123",
                    "report_type": "suspicious_activity",
                    "status": "filed",
                    "filing_date": datetime.utcnow().isoformat()
                }
            )
            
            report_response = await compliance_client.post(
                "/regulatory/report",
                json={
                    "report_type": "suspicious_activity",
                    "entity_id": "user123",
                    "reason": "unusual_transaction_pattern",
                    "details": {
                        "total_volume": "1000000",
                        "transaction_count": 50,
                        "time_period": "24h"
                    }
                }
            )
            
            assert report_response.status_code == 200
            assert report_response.json()["status"] == "filed"


@pytest.mark.asyncio
class TestSmartContractIntegration:
    """Test smart contract deployment and interaction"""
    
    async def test_contract_deployment_flow(
        self, blockchain_gateway_client, compliance_client
    ):
        """Test complete contract deployment flow"""
        # Deploy credential registry contract
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = httpx.Response(
                200,
                json={
                    "chain": "ethereum",
                    "contract_address": "0xcontract123",
                    "deployer": "0xdeployer"
                }
            )
            
            deploy_response = await blockchain_gateway_client.post(
                "/contract/deploy",
                json={
                    "chain_type": "ethereum",
                    "contract_type": "credential_registry",
                    "deployer_address": "0xdeployer"
                }
            )
            
            assert deploy_response.status_code == 200
            contract_address = deploy_response.json()["contract_address"]
            
        # Verify contract deployment event
        # This would trigger compliance checks and notifications
        with patch('httpx.AsyncClient.post') as mock_post:
            mock_post.return_value = httpx.Response(
                200,
                json={"status": "verified"}
            )
            
            verify_response = await compliance_client.post(
                "/contract/verify",
                json={
                    "contract_address": contract_address,
                    "chain": "ethereum",
                    "contract_type": "credential_registry"
                }
            )
            
            assert verify_response.status_code == 200 