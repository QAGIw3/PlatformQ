from web3 import Web3, AsyncWeb3
from web3.middleware import geth_poa_middleware
from eth_account import Account
from solcx import compile_source
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

from .base import BlockchainClient, ChainType, CredentialAnchor

logger = logging.getLogger(__name__)

class EthereumClient(BlockchainClient):
    """Ethereum blockchain client for credential anchoring"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(ChainType.ETHEREUM, config)
        self.w3 = None
        self.account = None
        self.contract_address = config.get("contract_address")
        self.contract_abi = None
        
    async def connect(self) -> bool:
        """Connect to Ethereum network"""
        try:
            # Support both HTTP and WebSocket providers
            provider_url = self.config.get("provider_url", "https://mainnet.infura.io/v3/YOUR_INFURA_KEY")
            
            if provider_url.startswith("ws"):
                self.w3 = Web3(Web3.WebsocketProvider(provider_url))
            else:
                self.w3 = Web3(Web3.HTTPProvider(provider_url))
            
            # Add POA middleware if needed (for certain networks)
            if self.config.get("is_poa", False):
                self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            
            # Load account from private key
            private_key = self.config.get("private_key")
            if private_key:
                self.account = Account.from_key(private_key)
            
            # Check connection
            if await self.w3.is_connected():
                chain_id = await self.w3.eth.chain_id
                logger.info(f"Connected to Ethereum network (chain_id: {chain_id})")
                
                # Deploy contract if not provided
                if not self.contract_address:
                    await self._deploy_credential_registry()
                
                return True
            else:
                logger.error("Failed to connect to Ethereum network")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to Ethereum: {e}")
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from Ethereum network"""
        if self.w3 and hasattr(self.w3.provider, 'disconnect'):
            await self.w3.provider.disconnect()
        logger.info("Disconnected from Ethereum network")
    
    async def anchor_credential(self, credential: Dict[str, Any], 
                              tenant_id: str) -> CredentialAnchor:
        """Anchor credential hash on Ethereum blockchain"""
        try:
            credential_id = credential.get("id")
            credential_hash = self.hash_credential(credential)
            
            # Build transaction
            contract = self.w3.eth.contract(
                address=self.contract_address,
                abi=self.contract_abi
            )
            
            # Call the anchorCredential function
            function = contract.functions.anchorCredential(
                credential_id,
                credential_hash,
                tenant_id
            )
            
            # Build and send transaction
            tx = await self._build_and_send_transaction(function)
            
            # Wait for confirmation
            receipt = await self.w3.eth.wait_for_transaction_receipt(tx)
            
            # Create anchor object
            block = await self.w3.eth.get_block(receipt.blockNumber)
            anchor = CredentialAnchor(
                credential_id=credential_id,
                credential_hash=credential_hash,
                transaction_hash=receipt.transactionHash.hex(),
                block_number=receipt.blockNumber,
                timestamp=datetime.fromtimestamp(block.timestamp),
                chain_type=self.chain_type
            )
            
            logger.info(f"Credential {credential_id} anchored at tx: {anchor.transaction_hash}")
            return anchor
            
        except Exception as e:
            logger.error(f"Error anchoring credential: {e}")
            raise
    
    async def verify_credential_anchor(self, credential_id: str) -> Optional[CredentialAnchor]:
        """Verify credential exists on chain"""
        try:
            contract = self.w3.eth.contract(
                address=self.contract_address,
                abi=self.contract_abi
            )
            
            # Call the getCredential function
            result = await contract.functions.getCredential(credential_id).call()
            
            if result[0] == "":  # Empty hash means not found
                return None
            
            # Get transaction details
            credential_hash = result[0]
            block_number = result[1]
            tx_hash = result[2]
            
            block = await self.w3.eth.get_block(block_number)
            
            return CredentialAnchor(
                credential_id=credential_id,
                credential_hash=credential_hash,
                transaction_hash=tx_hash,
                block_number=block_number,
                timestamp=datetime.fromtimestamp(block.timestamp),
                chain_type=self.chain_type
            )
            
        except Exception as e:
            logger.error(f"Error verifying credential: {e}")
            return None
    
    async def get_credential_history(self, credential_id: str) -> List[CredentialAnchor]:
        """Get credential history including revocations"""
        try:
            contract = self.w3.eth.contract(
                address=self.contract_address,
                abi=self.contract_abi
            )
            
            # Get events for this credential
            event_filter = contract.events.CredentialAnchored.create_filter(
                argument_filters={'credentialId': credential_id},
                fromBlock=0
            )
            
            events = await event_filter.get_all_entries()
            
            history = []
            for event in events:
                block = await self.w3.eth.get_block(event.blockNumber)
                anchor = CredentialAnchor(
                    credential_id=credential_id,
                    credential_hash=event.args.credentialHash,
                    transaction_hash=event.transactionHash.hex(),
                    block_number=event.blockNumber,
                    timestamp=datetime.fromtimestamp(block.timestamp),
                    chain_type=self.chain_type
                )
                history.append(anchor)
            
            return history
            
        except Exception as e:
            logger.error(f"Error getting credential history: {e}")
            return []
    
    async def revoke_credential(self, credential_id: str, reason: str) -> str:
        """Revoke credential on chain"""
        try:
            contract = self.w3.eth.contract(
                address=self.contract_address,
                abi=self.contract_abi
            )
            
            function = contract.functions.revokeCredential(credential_id, reason)
            tx = await self._build_and_send_transaction(function)
            
            receipt = await self.w3.eth.wait_for_transaction_receipt(tx)
            logger.info(f"Credential {credential_id} revoked at tx: {receipt.transactionHash.hex()}")
            
            return receipt.transactionHash.hex()
            
        except Exception as e:
            logger.error(f"Error revoking credential: {e}")
            raise
    
    async def deploy_smart_contract(self, contract_name: str, 
                                  contract_code: str) -> str:
        """Deploy a smart contract"""
        try:
            # Compile contract
            compiled = compile_source(contract_code)
            contract_interface = compiled[f'<stdin>:{contract_name}']
            
            # Deploy contract
            contract = self.w3.eth.contract(
                abi=contract_interface['abi'],
                bytecode=contract_interface['bin']
            )
            
            # Build constructor transaction
            constructor = contract.constructor()
            tx = await self._build_and_send_transaction(constructor)
            
            # Wait for deployment
            receipt = await self.w3.eth.wait_for_transaction_receipt(tx)
            contract_address = receipt.contractAddress
            
            logger.info(f"Contract {contract_name} deployed at: {contract_address}")
            return contract_address
            
        except Exception as e:
            logger.error(f"Error deploying contract: {e}")
            raise
    
    async def call_smart_contract(self, contract_address: str, 
                                method: str, params: List[Any]) -> Any:
        """Call smart contract method"""
        try:
            # For this we'd need the ABI, assuming it's stored
            contract = self.w3.eth.contract(
                address=contract_address,
                abi=self.contract_abi  # This would need to be loaded
            )
            
            result = await contract.functions[method](*params).call()
            return result
            
        except Exception as e:
            logger.error(f"Error calling contract: {e}")
            raise
    
    async def _deploy_credential_registry(self):
        """Deploy the credential registry contract"""
        contract_code = '''
        pragma solidity ^0.8.0;
        
        contract CredentialRegistry {
            struct Credential {
                string credentialHash;
                uint256 blockNumber;
                address issuer;
                bool revoked;
                string revocationReason;
            }
            
            mapping(string => Credential) public credentials;
            
            event CredentialAnchored(
                string indexed credentialId,
                string credentialHash,
                address issuer
            );
            
            event CredentialRevoked(
                string indexed credentialId,
                string reason
            );
            
            function anchorCredential(
                string memory credentialId,
                string memory credentialHash,
                string memory tenantId
            ) public {
                require(bytes(credentials[credentialId].credentialHash).length == 0, 
                        "Credential already exists");
                
                credentials[credentialId] = Credential({
                    credentialHash: credentialHash,
                    blockNumber: block.number,
                    issuer: msg.sender,
                    revoked: false,
                    revocationReason: ""
                });
                
                emit CredentialAnchored(credentialId, credentialHash, msg.sender);
            }
            
            function getCredential(string memory credentialId) 
                public view returns (string memory, uint256, address) {
                Credential memory cred = credentials[credentialId];
                return (cred.credentialHash, cred.blockNumber, cred.issuer);
            }
            
            function revokeCredential(string memory credentialId, string memory reason) 
                public {
                require(bytes(credentials[credentialId].credentialHash).length > 0, 
                        "Credential not found");
                require(credentials[credentialId].issuer == msg.sender, 
                        "Only issuer can revoke");
                
                credentials[credentialId].revoked = true;
                credentials[credentialId].revocationReason = reason;
                
                emit CredentialRevoked(credentialId, reason);
            }
            
            function isRevoked(string memory credentialId) 
                public view returns (bool) {
                return credentials[credentialId].revoked;
            }
        }
        '''
        
        self.contract_address = await self.deploy_smart_contract(
            "CredentialRegistry", 
            contract_code
        )
        
        # Store ABI for future use
        compiled = compile_source(contract_code)
        self.contract_abi = compiled['<stdin>:CredentialRegistry']['abi']
    
    async def _build_and_send_transaction(self, function):
        """Build and send a transaction"""
        # Get nonce
        nonce = await self.w3.eth.get_transaction_count(self.account.address)
        
        # Build transaction
        tx = function.build_transaction({
            'from': self.account.address,
            'nonce': nonce,
            'gas': 2000000,
            'gasPrice': await self.w3.eth.gas_price,
        })
        
        # Sign transaction
        signed_tx = self.w3.eth.account.sign_transaction(tx, self.account.key)
        
        # Send transaction
        tx_hash = await self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        return tx_hash 