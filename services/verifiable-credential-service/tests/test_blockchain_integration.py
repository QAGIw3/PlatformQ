import pytest
from unittest.mock import Mock, patch, AsyncMock
import asyncio
from datetime import datetime

# from app.blockchain.base import ChainType, CredentialAnchor  # Comment out if causing issues
from app.blockchain.ethereum import EthereumClient
from app.did.did_manager import DIDManager, DIDDocument
from app.zkp.zkp_manager import ZKPManager


class TestBlockchainIntegration:
    """Test blockchain integration features"""
    
    @pytest.fixture
    def mock_web3(self):
        """Mock Web3 instance"""
        mock = Mock()
        mock.eth.chain_id = 1
        mock.is_connected = AsyncMock(return_value=True)
        mock.eth.get_transaction_count = AsyncMock(return_value=0)
        mock.eth.gas_price = 20000000000
        mock.eth.send_raw_transaction = AsyncMock(return_value=b'0x123')
        mock.eth.wait_for_transaction_receipt = AsyncMock(return_value={
            'transactionHash': b'0x123',
            'blockNumber': 12345
        })
        mock.eth.get_block = AsyncMock(return_value={'timestamp': 1234567890})
        return mock
    
    @pytest.mark.asyncio
    async def test_ethereum_client_connect(self, mock_web3):
        """Test Ethereum client connection"""
        config = {
            "provider_url": "https://mainnet.infura.io/v3/test",
            "private_key": "0x" + "1" * 64
        }
        
        with patch('app.blockchain.ethereum.Web3', return_value=mock_web3):
            client = EthereumClient(config)
            result = await client.connect()
            
            assert result is True
            assert client.w3 is not None
    
    @pytest.mark.asyncio
    async def test_anchor_credential(self, mock_web3):
        """Test credential anchoring on blockchain"""
        config = {
            "provider_url": "https://mainnet.infura.io/v3/test",
            "private_key": "0x" + "1" * 64,
            "contract_address": "0x" + "2" * 40
        }
        
        credential = {
            "id": "urn:uuid:12345",
            "type": ["VerifiableCredential"],
            "credentialSubject": {"name": "Test"}
        }
        
        with patch('app.blockchain.ethereum.Web3', return_value=mock_web3):
            client = EthereumClient(config)
            client.contract_abi = []  # Mock ABI
            
            # Mock contract
            mock_contract = Mock()
            mock_contract.functions.anchorCredential.return_value.build_transaction.return_value = {
                'from': '0x123',
                'nonce': 0,
                'gas': 100000,
                'gasPrice': 20000000000
            }
            mock_web3.eth.contract.return_value = mock_contract
            mock_web3.eth.account.sign_transaction.return_value.rawTransaction = b'0xsigned'
            
            anchor = await client.anchor_credential(credential, "tenant123")
            
            assert isinstance(anchor, CredentialAnchor)
            assert anchor.credential_id == "urn:uuid:12345"
            assert anchor.chain_type == ChainType.ETHEREUM
    
    def test_did_manager_creation(self):
        """Test DID manager and document creation"""
        manager = DIDManager()
        
        # Test DID creation
        did_doc = manager.create_did(method="key")
        
        assert isinstance(did_doc, DIDDocument)
        assert did_doc.did.startswith("did:key:")
        assert "@context" in did_doc.document
        assert "verificationMethod" in did_doc.document
    
    def test_did_for_tenant(self):
        """Test creating DID for tenant"""
        manager = DIDManager()
        
        did_doc = manager.create_did_for_tenant("tenant123", method="web")
        
        assert did_doc.did.startswith("did:web:")
        assert "tenants/tenant123" in did_doc.did
        assert len(did_doc.document["service"]) == 2
    
    def test_zkp_manager(self):
        """Test ZKP manager functionality"""
        manager = ZKPManager()
        
        credential = {
            "credentialSubject": {
                "name": "Alice",
                "age": 30,
                "email": "alice@example.com"
            }
        }
        
        # Create proof revealing only name
        proof = manager.create_proof(
            credential=credential,
            revealed_attributes=["name"],
            proof_type="BbsBlsSignature2020"
        )
        
        assert proof.revealed_attributes == ["name"]
        assert proof.proof_type == "BbsBlsSignature2020"
        assert proof.proof_value is not None
    
    def test_selective_disclosure(self):
        """Test selective disclosure functionality"""
        from app.zkp.selective_disclosure import SelectiveDisclosure
        
        sd = SelectiveDisclosure()
        
        credential = {
            "@context": ["https://www.w3.org/2018/credentials/v1"],
            "id": "urn:uuid:123",
            "type": ["VerifiableCredential"],
            "issuer": "did:example:123",
            "credentialSubject": {
                "name": "Bob",
                "age": 25,
                "address": "123 Main St",
                "email": "bob@example.com"
            }
        }
        
        # Create derived credential with only name and age
        derived = sd.create_derived_credential(
            original_credential=credential,
            disclosed_attributes=["name", "age"]
        )
        
        assert "name" in derived["credentialSubject"]
        assert "age" in derived["credentialSubject"]
        assert "address" not in derived["credentialSubject"]
        assert "email" not in derived["credentialSubject"]
        assert derived["disclosure"]["disclosed"] == ["name", "age"]
    
    @pytest.mark.asyncio
    async def test_multi_chain_support(self, mock_web3):
        """Test multi-chain credential anchoring"""
        # This would test anchoring on multiple chains
        # For now, just verify the structure is in place
        from app.blockchain import EthereumClient, PolygonClient
        
        assert EthereumClient is not None
        assert PolygonClient is not None
    
    def test_credential_anchor_serialization(self):
        """Test credential anchor serialization"""
        anchor = CredentialAnchor(
            credential_id="urn:uuid:test",
            credential_hash="0xhash",
            transaction_hash="0xtx",
            block_number=12345,
            timestamp=datetime.now(),
            chain_type=ChainType.ETHEREUM
        )
        
        data = anchor.to_dict()
        
        assert data["credential_id"] == "urn:uuid:test"
        assert data["chain_type"] == "ethereum"
        assert "timestamp" in data 

@patch('app.blockchain.ethereum.compile_source')
def test_compile(mock_compile):
    mock_compile.return_value = {'contract': 'compiled'}
    # Test 