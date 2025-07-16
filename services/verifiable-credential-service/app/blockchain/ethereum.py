from web3 import Web3, AsyncWeb3
from web3.middleware import geth_poa_middleware
from eth_account import Account
from solcx import compile_source
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging
from prometheus_client import Counter, Histogram, Gauge # Import Prometheus client
import time # Import time for metrics timing

from .base import BlockchainClient, ChainType, CredentialAnchor

logger = logging.getLogger(__name__)

# Prometheus Metrics for Ethereum Client
ETHEREUM_TRANSACTIONS_TOTAL = Counter(
    'ethereum_transactions_total', 
    'Total number of transactions sent to Ethereum',
    ['chain_type', 'method', 'status']
)
ETHEREUM_TRANSACTION_DURATION_SECONDS = Histogram(
    'ethereum_transaction_duration_seconds', 
    'Duration of Ethereum transactions in seconds',
    ['chain_type', 'method']
)
ETHEREUM_GAS_USED_TOTAL = Counter(
    'ethereum_gas_used_total', 
    'Total gas used on Ethereum transactions',
    ['chain_type', 'method']
)
ETHEREUM_BLOCK_HEIGHT = Gauge(
    'ethereum_block_height', 
    'Current block height of the Ethereum network',
    ['chain_type']
)
ETHEREUM_CONTRACT_DEPLOYMENTS_TOTAL = Counter(
    'ethereum_contract_deployments_total', 
    'Total smart contracts deployed on Ethereum',
    ['chain_type', 'contract_name']
)
ETHEREUM_DAO_PROPOSAL_EXECUTIONS_TOTAL = Counter(
    'ethereum_dao_proposal_executions_total', 
    'Total DAO proposal executions on Ethereum',
    ['chain_type', 'dao_id']
)
ETHEREUM_DAO_VOTES_CAST_TOTAL = Counter(
    'ethereum_dao_votes_cast_total', 
    'Total DAO votes cast on Ethereum',
    ['chain_type', 'dao_id']
)


class EthereumClient(BlockchainClient):
    """
    Ethereum blockchain client for credential anchoring and DAO interactions
    """
    
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
                
                # Update block height metric
                current_block = await self.w3.eth.block_number
                ETHEREUM_BLOCK_HEIGHT.labels(chain_type=self.chain_type.value).set(current_block)

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
        method_name = "anchor_credential"
        start_time = time.time()
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
            
            # Metrics
            ETHEREUM_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='success').inc()
            ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            if receipt.get('gasUsed'):
                ETHEREUM_GAS_USED_TOTAL.labels(chain_type=self.chain_type.value, method=method_name).inc(receipt['gasUsed'])

            return anchor
            
        except Exception as e:
            logger.error(f"Error anchoring credential: {e}")
            ETHEREUM_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='failure').inc()
            ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            raise
    
    async def verify_credential_anchor(self, credential_id: str) -> Optional[CredentialAnchor]:
        """Verify credential exists on chain"""
        method_name = "verify_credential_anchor"
        start_time = time.time()
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
            
            # Metrics (read-only calls don't have gas or full tx hash)
            ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)

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
            ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            return None
    
    async def get_credential_history(self, credential_id: str) -> List[CredentialAnchor]:
        """Get credential history including revocations"""
        method_name = "get_credential_history"
        start_time = time.time()
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
            
            # Metrics
            ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)

            return history
            
        except Exception as e:
            logger.error(f"Error getting credential history: {e}")
            ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            return []
    
    async def revoke_credential(self, credential_id: str, reason: str) -> str:
        """Revoke credential on chain"""
        method_name = "revoke_credential"
        start_time = time.time()
        try:
            contract = self.w3.eth.contract(
                address=self.contract_address,
                abi=self.contract_abi
            )
            
            function = contract.functions.revokeCredential(credential_id, reason)
            tx = await self._build_and_send_transaction(function)
            
            receipt = await self.w3.eth.wait_for_transaction_receipt(tx)
            logger.info(f"Credential {credential_id} revoked at tx: {receipt.transactionHash.hex()}")
            
            # Metrics
            ETHEREUM_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='success').inc()
            ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            if receipt.get('gasUsed'):
                ETHEREUM_GAS_USED_TOTAL.labels(chain_type=self.chain_type.value, method=method_name).inc(receipt['gasUsed'])

            return receipt.transactionHash.hex()
            
        except Exception as e:
            logger.error(f"Error revoking credential: {e}")
            ETHEREUM_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='failure').inc()
            ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            raise
    
    async def deploy_smart_contract(self, contract_name: str, 
                                  contract_code: str) -> str:
        """Deploy a smart contract"""
        method_name = "deploy_smart_contract"
        start_time = time.time()
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
            
            # Metrics
            ETHEREUM_CONTRACT_DEPLOYMENTS_TOTAL.labels(chain_type=self.chain_type.value, contract_name=contract_name).inc()
            ETHEREUM_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='success').inc()
            ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            if receipt.get('gasUsed'):
                ETHEREUM_GAS_USED_TOTAL.labels(chain_type=self.chain_type.value, method=method_name).inc(receipt['gasUsed'])

            return contract_address
            
        except Exception as e:
            logger.error(f"Error deploying contract: {e}")
            ETHEREUM_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name, status='failure').inc()
            ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name).observe(time.time() - start_time)
            raise
    
    async def call_smart_contract(self, contract_address: str, 
                                method: str, params: List[Any]) -> Any:
        """Call smart contract method"""
        method_name_full = f"call_smart_contract_{method}" # Differentiate calls by method
        start_time = time.time()
        try:
            # For this we'd need the ABI, assuming it's stored
            contract = self.w3.eth.contract(
                address=contract_address,
                abi=self.contract_abi  # This would need to be loaded
            )
            
            # Determine if it's a write (transaction) or read (call) operation
            # This is a simplification; a more robust solution would parse ABI for mutability
            is_write_operation = False
            # Heuristic: if method implies state change (e.g., set, execute, create, vote)
            if any(kw in method.lower() for kw in ['set', 'execute', 'create', 'vote', 'mint', 'add', 'remove']):
                is_write_operation = True

            if is_write_operation:
                logger.info(f"Calling write method '{method}' on contract {contract_address}")
                function = contract.functions[method](*params)
                tx = await self._build_and_send_transaction(function)
                receipt = await self.w3.eth.wait_for_transaction_receipt(tx)
                result = receipt.transactionHash.hex() # Return transaction hash for write ops

                # DAO specific metrics for known methods
                if method.lower() == "executeproposal" and len(params) > 0: # Assuming first param is proposalId or DAO ID
                    ETHEREUM_DAO_PROPOSAL_EXECUTIONS_TOTAL.labels(chain_type=self.chain_type.value, dao_id=params[0]).inc() # Assuming params[0] is dao_id or proposalId
                    logger.info(f"Incremented DAO proposal execution metric for DAO: {params[0]}")
                elif method.lower() == "castvote" and len(params) > 0: # Assuming first param is proposalId or DAO ID
                    ETHEREUM_DAO_VOTES_CAST_TOTAL.labels(chain_type=self.chain_type.value, dao_id=params[0]).inc() # Assuming params[0] is dao_id or proposalId
                    logger.info(f"Incremented DAO vote cast metric for DAO: {params[0]}")

                # General transaction metrics for write ops
                ETHEREUM_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name_full, status='success').inc()
                ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name_full).observe(time.time() - start_time)
                if receipt.get('gasUsed'):
                    ETHEREUM_GAS_USED_TOTAL.labels(chain_type=self.chain_type.value, method=method_name_full).inc(receipt['gasUsed'])
            else:
                logger.info(f"Calling read method '{method}' on contract {contract_address}")
                result = await contract.functions[method](*params).call()
                # Metrics for read ops
                ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name_full).observe(time.time() - start_time)

            return result
            
        except Exception as e:
            logger.error(f"Error calling contract: {e}")
            if is_write_operation:
                 ETHEREUM_TRANSACTIONS_TOTAL.labels(chain_type=self.chain_type.value, method=method_name_full, status='failure').inc()
            ETHEREUM_TRANSACTION_DURATION_SECONDS.labels(chain_type=self.chain_type.value, method=method_name_full).observe(time.time() - start_time)
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