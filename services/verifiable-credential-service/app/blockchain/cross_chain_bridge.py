"""
Cross-Chain Bridge integration for exporting VCs to public blockchains.
Supports Ethereum, Polygon, Arbitrum, and other EVM chains.
"""

import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
from web3 import Web3
from eth_account import Account
import asyncio
from dataclasses import dataclass

from platformq_shared.cache import get_cache_client
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class ChainNetwork(str, Enum):
    """Supported blockchain networks"""
    ETHEREUM = "ethereum"
    POLYGON = "polygon"
    ARBITRUM = "arbitrum"
    OPTIMISM = "optimism"
    BSC = "bsc"
    AVALANCHE = "avalanche"
    LOCAL = "local"


@dataclass
class ChainConfig:
    """Configuration for a blockchain network"""
    name: str
    chain_id: int
    rpc_url: str
    bridge_contract: str
    sbt_contract: str
    gas_price_multiplier: float = 1.2
    confirmation_blocks: int = 3


# Network configurations
CHAIN_CONFIGS = {
    ChainNetwork.POLYGON: ChainConfig(
        name="Polygon",
        chain_id=137,
        rpc_url="https://polygon-rpc.com",
        bridge_contract="",  # To be deployed
        sbt_contract="",     # To be deployed
        gas_price_multiplier=1.5,
        confirmation_blocks=30
    ),
    ChainNetwork.ARBITRUM: ChainConfig(
        name="Arbitrum One",
        chain_id=42161,
        rpc_url="https://arb1.arbitrum.io/rpc",
        bridge_contract="",
        sbt_contract="",
        gas_price_multiplier=1.2,
        confirmation_blocks=20
    ),
    ChainNetwork.LOCAL: ChainConfig(
        name="Local Hardhat",
        chain_id=31337,
        rpc_url="http://localhost:8545",
        bridge_contract="",  # Will be set after deployment
        sbt_contract="",     # Will be set after deployment
        gas_price_multiplier=1.0,
        confirmation_blocks=1
    )
}


class CrossChainBridge:
    """
    Manages cross-chain credential transfers and SBT issuance
    """
    
    def __init__(
        self,
        private_key: str,
        event_publisher: Optional[EventPublisher] = None
    ):
        self.account = Account.from_key(private_key)
        self.event_publisher = event_publisher
        self.web3_connections: Dict[ChainNetwork, Web3] = {}
        self.bridge_contracts = {}
        self.sbt_contracts = {}
        
        # Load contract ABIs
        self._load_contract_abis()
        
    def _load_contract_abis(self):
        """Load contract ABIs from compiled artifacts"""
        try:
            # In production, these would be loaded from the compiled contracts
            with open("artifacts/CrossChainBridge.json", "r") as f:
                self.bridge_abi = json.load(f)["abi"]
            
            with open("artifacts/SoulBoundToken.json", "r") as f:
                self.sbt_abi = json.load(f)["abi"]
        except Exception as e:
            logger.warning(f"Could not load contract ABIs: {e}")
            # Use minimal ABIs for development
            self.bridge_abi = [
                {
                    "name": "initiateTransfer",
                    "type": "function",
                    "inputs": [
                        {"name": "credentialHash", "type": "bytes32"},
                        {"name": "targetChain", "type": "string"},
                        {"name": "secretHash", "type": "bytes32"}
                    ],
                    "outputs": [{"name": "transferId", "type": "bytes32"}]
                },
                {
                    "name": "completeTransfer",
                    "type": "function",
                    "inputs": [
                        {"name": "transferId", "type": "bytes32"},
                        {"name": "secret", "type": "bytes"}
                    ]
                },
                {
                    "name": "TransferInitiated",
                    "type": "event",
                    "inputs": [
                        {"name": "transferId", "type": "bytes32", "indexed": True},
                        {"name": "credentialHash", "type": "bytes32", "indexed": True}
                    ]
                }
            ]
            
            self.sbt_abi = [
                {
                    "name": "issue",
                    "type": "function",
                    "inputs": [
                        {"name": "soul", "type": "address"},
                        {"name": "credentialHash", "type": "bytes32"},
                        {"name": "metadataURI", "type": "string"},
                        {"name": "burnAuth", "type": "uint8"}
                    ],
                    "outputs": [{"name": "tokenId", "type": "uint256"}]
                }
            ]
    
    def connect_to_chain(self, network: ChainNetwork) -> Web3:
        """Connect to a blockchain network"""
        if network not in self.web3_connections:
            config = CHAIN_CONFIGS[network]
            w3 = Web3(Web3.HTTPProvider(config.rpc_url))
            
            # Check connection
            if not w3.is_connected():
                raise ConnectionError(f"Failed to connect to {network}")
            
            self.web3_connections[network] = w3
            
            # Initialize contracts if addresses are set
            if config.bridge_contract:
                self.bridge_contracts[network] = w3.eth.contract(
                    address=config.bridge_contract,
                    abi=self.bridge_abi
                )
            
            if config.sbt_contract:
                self.sbt_contracts[network] = w3.eth.contract(
                    address=config.sbt_contract,
                    abi=self.sbt_abi
                )
        
        return self.web3_connections[network]
    
    async def export_credential_to_chain(
        self,
        credential_id: str,
        credential_hash: str,
        metadata_uri: str,
        target_chain: ChainNetwork,
        recipient_address: str
    ) -> Dict[str, Any]:
        """
        Export a verifiable credential to another blockchain as an SBT
        
        Args:
            credential_id: The VC ID
            credential_hash: Hash of the credential data
            metadata_uri: IPFS URI for credential metadata
            target_chain: Target blockchain network
            recipient_address: Address to receive the SBT
            
        Returns:
            Transaction details and SBT token ID
        """
        try:
            # Connect to target chain
            w3 = self.connect_to_chain(target_chain)
            config = CHAIN_CONFIGS[target_chain]
            
            if target_chain not in self.sbt_contracts:
                raise ValueError(f"SBT contract not deployed on {target_chain}")
            
            sbt_contract = self.sbt_contracts[target_chain]
            
            # Prepare transaction
            nonce = w3.eth.get_transaction_count(self.account.address)
            
            # Estimate gas
            try:
                gas_estimate = sbt_contract.functions.issue(
                    recipient_address,
                    Web3.keccak(text=credential_hash),
                    metadata_uri,
                    2  # OWNER_OR_ISSUER burn auth
                ).estimate_gas({'from': self.account.address})
            except Exception as e:
                logger.error(f"Gas estimation failed: {e}")
                gas_estimate = 500000  # Fallback gas limit
            
            # Get current gas price
            gas_price = int(w3.eth.gas_price * config.gas_price_multiplier)
            
            # Build transaction
            tx = sbt_contract.functions.issue(
                recipient_address,
                Web3.keccak(text=credential_hash),
                metadata_uri,
                2  # OWNER_OR_ISSUER burn auth
            ).build_transaction({
                'from': self.account.address,
                'nonce': nonce,
                'gas': int(gas_estimate * 1.2),
                'gasPrice': gas_price,
                'chainId': config.chain_id
            })
            
            # Sign and send transaction
            signed_tx = self.account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            # Wait for confirmation
            logger.info(f"Waiting for transaction {tx_hash.hex()} on {target_chain}")
            receipt = w3.eth.wait_for_transaction_receipt(
                tx_hash,
                timeout=300  # 5 minutes
            )
            
            if receipt['status'] != 1:
                raise Exception(f"Transaction failed: {receipt}")
            
            # Extract token ID from events
            token_id = None
            for log in receipt['logs']:
                try:
                    event = sbt_contract.events.Issued().process_log(log)
                    token_id = event['args']['tokenId']
                    break
                except Exception:
                    continue
            
            result = {
                'success': True,
                'chain': target_chain,
                'tx_hash': tx_hash.hex(),
                'block_number': receipt['blockNumber'],
                'token_id': token_id,
                'sbt_contract': config.sbt_contract,
                'recipient': recipient_address,
                'gas_used': receipt['gasUsed'],
                'timestamp': datetime.utcnow().isoformat()
            }
            
            # Cache the result
            cache_client = await get_cache_client()
            if cache_client:
                await cache_client.setex(
                    f"vc:export:{credential_id}:{target_chain}",
                    86400,  # 24 hours
                    json.dumps(result)
                )
            
            # Publish event
            if self.event_publisher:
                await self._publish_export_event(
                    credential_id,
                    target_chain,
                    result
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to export credential to {target_chain}: {e}")
            return {
                'success': False,
                'chain': target_chain,
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
    
    async def initiate_bridge_transfer(
        self,
        credential_hash: str,
        source_chain: ChainNetwork,
        target_chain: ChainNetwork,
        secret_hash: str
    ) -> Optional[str]:
        """
        Initiate a cross-chain transfer using HTLC pattern
        
        Returns:
            Transfer ID if successful
        """
        try:
            w3 = self.connect_to_chain(source_chain)
            
            if source_chain not in self.bridge_contracts:
                raise ValueError(f"Bridge contract not deployed on {source_chain}")
            
            bridge_contract = self.bridge_contracts[source_chain]
            
            # Build transaction
            nonce = w3.eth.get_transaction_count(self.account.address)
            
            tx = bridge_contract.functions.initiateTransfer(
                Web3.keccak(text=credential_hash),
                target_chain,
                Web3.keccak(text=secret_hash)
            ).build_transaction({
                'from': self.account.address,
                'nonce': nonce,
                'gas': 200000,
                'gasPrice': w3.eth.gas_price,
                'chainId': CHAIN_CONFIGS[source_chain].chain_id
            })
            
            # Sign and send
            signed_tx = self.account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            # Wait for receipt
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
            
            # Extract transfer ID from events
            for log in receipt['logs']:
                try:
                    event = bridge_contract.events.TransferInitiated().process_log(log)
                    return event['args']['transferId']
                except Exception:
                    continue
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to initiate bridge transfer: {e}")
            return None
    
    async def check_export_status(
        self,
        credential_id: str,
        target_chain: ChainNetwork
    ) -> Optional[Dict[str, Any]]:
        """Check the status of a credential export"""
        cache_client = await get_cache_client()
        if cache_client:
            cached = await cache_client.get(f"vc:export:{credential_id}:{target_chain}")
            if cached:
                return json.loads(cached)
        
        return None
    
    async def get_supported_chains(self) -> List[Dict[str, Any]]:
        """Get list of supported chains with their status"""
        chains = []
        
        for network, config in CHAIN_CONFIGS.items():
            try:
                w3 = self.connect_to_chain(network)
                is_connected = w3.is_connected()
                
                chain_info = {
                    'network': network,
                    'name': config.name,
                    'chain_id': config.chain_id,
                    'connected': is_connected,
                    'bridge_deployed': bool(config.bridge_contract),
                    'sbt_deployed': bool(config.sbt_contract)
                }
                
                if is_connected:
                    chain_info['block_number'] = w3.eth.block_number
                    chain_info['gas_price'] = w3.eth.gas_price
                
                chains.append(chain_info)
                
            except Exception as e:
                chains.append({
                    'network': network,
                    'name': config.name,
                    'chain_id': config.chain_id,
                    'connected': False,
                    'error': str(e)
                })
        
        return chains
    
    async def _publish_export_event(
        self,
        credential_id: str,
        target_chain: ChainNetwork,
        result: Dict[str, Any]
    ):
        """Publish credential export event"""
        try:
            from pulsar.schema import Record, String, Long, Boolean
            
            class CredentialExported(Record):
                credential_id = String()
                target_chain = String()
                tx_hash = String()
                token_id = String(required=False)
                success = Boolean()
                timestamp = Long()
            
            event = CredentialExported(
                credential_id=credential_id,
                target_chain=target_chain,
                tx_hash=result.get('tx_hash', ''),
                token_id=str(result.get('token_id', '')),
                success=result.get('success', False),
                timestamp=int(datetime.utcnow().timestamp() * 1000)
            )
            
            self.event_publisher.publish(
                topic_base='credential-exported-events',
                tenant_id='platform',
                schema_class=CredentialExported,
                data=event
            )
        except Exception as e:
            logger.error(f"Failed to publish export event: {e}")
    
    def set_contract_addresses(
        self,
        network: ChainNetwork,
        bridge_address: Optional[str] = None,
        sbt_address: Optional[str] = None
    ):
        """Set contract addresses for a network (used after deployment)"""
        if network in CHAIN_CONFIGS:
            if bridge_address:
                CHAIN_CONFIGS[network].bridge_contract = bridge_address
                
                # Reinitialize contract
                if network in self.web3_connections:
                    w3 = self.web3_connections[network]
                    self.bridge_contracts[network] = w3.eth.contract(
                        address=bridge_address,
                        abi=self.bridge_abi
                    )
            
            if sbt_address:
                CHAIN_CONFIGS[network].sbt_contract = sbt_address
                
                # Reinitialize contract
                if network in self.web3_connections:
                    w3 = self.web3_connections[network]
                    self.sbt_contracts[network] = w3.eth.contract(
                        address=sbt_address,
                        abi=self.sbt_abi
                    ) 