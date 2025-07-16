from typing import Dict, Any
import logging

from .ethereum import EthereumClient
from .base import ChainType

logger = logging.getLogger(__name__)

class PolygonClient(EthereumClient):
    """Polygon (Matic) blockchain client for credential anchoring"""
    
    def __init__(self, config: Dict[str, Any]):
        # Set default Polygon configuration
        polygon_config = {
            "provider_url": config.get("provider_url", "https://polygon-rpc.com"),
            "chain_id": 137,  # Polygon Mainnet
            "is_poa": True,   # Polygon uses POA consensus
            **config  # Override with any provided config
        }
        
        # Initialize parent with Polygon chain type
        super().__init__(polygon_config)
        self.chain_type = ChainType.POLYGON
        
    async def connect(self) -> bool:
        """Connect to Polygon network"""
        try:
            result = await super().connect()
            if result:
                # Verify we're on Polygon network
                chain_id = await self.w3.eth.chain_id
                if chain_id == 137:
                    logger.info("Connected to Polygon Mainnet")
                elif chain_id == 80001:
                    logger.info("Connected to Polygon Mumbai Testnet")
                else:
                    logger.warning(f"Connected to unknown Polygon network (chain_id: {chain_id})")
            return result
        except Exception as e:
            logger.error(f"Error connecting to Polygon: {e}")
            return False
    
    async def _deploy_credential_registry(self):
        """Deploy optimized credential registry for Polygon"""
        # Polygon-optimized contract with lower gas usage
        contract_code = '''
        pragma solidity ^0.8.0;
        
        contract PolygonCredentialRegistry {
            struct Credential {
                bytes32 credentialHash;  // Using bytes32 for gas efficiency
                uint32 blockNumber;      // uint32 is sufficient for block numbers
                address issuer;
                bool revoked;
            }
            
            mapping(string => Credential) public credentials;
            mapping(string => string) public revocationReasons;  // Separate mapping for gas efficiency
            
            event CredentialAnchored(
                string indexed credentialId,
                bytes32 credentialHash,
                address issuer
            );
            
            event CredentialRevoked(
                string indexed credentialId,
                string reason
            );
            
            function anchorCredential(
                string memory credentialId,
                bytes32 credentialHash,
                string memory tenantId
            ) public {
                require(credentials[credentialId].credentialHash == 0, 
                        "Credential exists");
                
                credentials[credentialId] = Credential({
                    credentialHash: credentialHash,
                    blockNumber: uint32(block.number),
                    issuer: msg.sender,
                    revoked: false
                });
                
                emit CredentialAnchored(credentialId, credentialHash, msg.sender);
            }
            
            function getCredential(string memory credentialId) 
                public view returns (bytes32, uint32, address) {
                Credential memory cred = credentials[credentialId];
                return (cred.credentialHash, cred.blockNumber, cred.issuer);
            }
            
            function revokeCredential(string memory credentialId, string memory reason) 
                public {
                require(credentials[credentialId].credentialHash != 0, 
                        "Not found");
                require(credentials[credentialId].issuer == msg.sender, 
                        "Not issuer");
                
                credentials[credentialId].revoked = true;
                revocationReasons[credentialId] = reason;
                
                emit CredentialRevoked(credentialId, reason);
            }
            
            function isRevoked(string memory credentialId) 
                public view returns (bool) {
                return credentials[credentialId].revoked;
            }
            
            function getRevocationReason(string memory credentialId)
                public view returns (string memory) {
                return revocationReasons[credentialId];
            }
        }
        '''
        
        self.contract_address = await self.deploy_smart_contract(
            "PolygonCredentialRegistry", 
            contract_code
        )
        
        # Store ABI for future use
        from solcx import compile_source
        compiled = compile_source(contract_code)
        self.contract_abi = compiled['<stdin>:PolygonCredentialRegistry']['abi']
        
        logger.info(f"Deployed Polygon-optimized credential registry at: {self.contract_address}") 