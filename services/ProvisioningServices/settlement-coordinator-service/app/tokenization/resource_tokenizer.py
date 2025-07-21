"""Resource Tokenizer Module

Handles minting, burning, and slashing of resource tokens for the Infrastructure DeFi system.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from decimal import Decimal
from web3 import Web3
from eth_account import Account
import json

from platformq_blockchain_common import (
    ChainType,
    TransactionResult,
    BlockchainError,
    EVMAdapter
)

from ..models.settlement import (
    Settlement,
    ResourceType,
    SettlementStatus,
    ServiceTier
)
from ..config import settings

logger = logging.getLogger(__name__)


class ResourceTokenizer:
    """Manages resource token lifecycle for Infrastructure DeFi"""
    
    def __init__(self, blockchain_adapter: EVMAdapter, contract_address: str, private_key: str):
        self.adapter = blockchain_adapter
        self.contract_address = Web3.toChecksumAddress(contract_address)
        self.account = Account.from_key(private_key)
        
        # Load contract ABI
        self.contract_abi = self._load_contract_abi()
        self.contract = self.adapter.web3.eth.contract(
            address=self.contract_address,
            abi=self.contract_abi
        )
        
        # Cache for gas prices and nonces
        self._gas_price_cache = None
        self._gas_price_timestamp = None
        self._nonce_cache = {}
        
        # Metrics
        self.tokens_minted = 0
        self.tokens_burned = 0
        self.tokens_slashed = 0
        
    def _load_contract_abi(self) -> List[Dict]:
        """Load ResourceToken contract ABI"""
        # In production, this would load from compiled artifacts
        return [
            {
                "name": "mintResource",
                "type": "function",
                "inputs": [
                    {"name": "provider", "type": "address"},
                    {"name": "resourceType", "type": "uint8"},
                    {"name": "amount", "type": "uint256"},
                    {"name": "validFrom", "type": "uint256"},
                    {"name": "validUntil", "type": "uint256"},
                    {"name": "region", "type": "string"},
                    {"name": "tier", "type": "uint8"},
                    {"name": "slaHash", "type": "bytes32"}
                ],
                "outputs": [{"name": "tokenId", "type": "uint256"}]
            },
            {
                "name": "burnResource",
                "type": "function",
                "inputs": [
                    {"name": "tokenId", "type": "uint256"},
                    {"name": "amount", "type": "uint256"}
                ]
            },
            {
                "name": "slashResource",
                "type": "function",
                "inputs": [
                    {"name": "tokenId", "type": "uint256"},
                    {"name": "violationSeverity", "type": "uint256"},
                    {"name": "reason", "type": "string"}
                ]
            },
            {
                "name": "registerProvider",
                "type": "function",
                "inputs": [
                    {"name": "provider", "type": "address"},
                    {"name": "initialReputation", "type": "uint256"}
                ]
            },
            {
                "name": "setProviderCapacity",
                "type": "function",
                "inputs": [
                    {"name": "provider", "type": "address"},
                    {"name": "resourceType", "type": "uint8"},
                    {"name": "capacity", "type": "uint256"}
                ]
            },
            {
                "name": "updatePrice",
                "type": "function",
                "inputs": [
                    {"name": "tokenId", "type": "uint256"},
                    {"name": "price", "type": "uint256"}
                ]
            },
            {
                "name": "getResourceSpec",
                "type": "function",
                "inputs": [{"name": "tokenId", "type": "uint256"}],
                "outputs": [{"name": "", "type": "tuple", "components": [
                    {"name": "resourceType", "type": "uint8"},
                    {"name": "amount", "type": "uint256"},
                    {"name": "validFrom", "type": "uint256"},
                    {"name": "validUntil", "type": "uint256"},
                    {"name": "region", "type": "string"},
                    {"name": "tier", "type": "uint8"},
                    {"name": "provider", "type": "address"},
                    {"name": "slaHash", "type": "bytes32"},
                    {"name": "isActive", "type": "bool"},
                    {"name": "slashedAmount", "type": "uint256"}
                ]}]
            }
        ]
    
    async def mint_resource_token(
        self,
        settlement: Settlement,
        provider_address: str,
        sla_hash: bytes
    ) -> Optional[int]:
        """Mint resource tokens for a committed resource"""
        try:
            # Map resource type
            resource_type_map = {
                ResourceType.CPU: 0,
                ResourceType.GPU: 1,
                ResourceType.STORAGE: 2,
                ResourceType.BANDWIDTH: 3,
                ResourceType.MEMORY: 4
            }
            
            # Map service tier
            tier_map = {
                ServiceTier.STANDARD: 0,
                ServiceTier.PREMIUM: 1,
                ServiceTier.GUARANTEED: 2
            }
            
            # Calculate validity period
            valid_from = int(settlement.delivery_start.timestamp())
            valid_until = int(settlement.delivery_end.timestamp())
            
            # Prepare transaction
            function = self.contract.functions.mintResource(
                Web3.toChecksumAddress(provider_address),
                resource_type_map.get(settlement.resource_type, 0),
                int(settlement.quantity),
                valid_from,
                valid_until,
                settlement.metadata.get('region', 'us-east-1'),
                tier_map.get(settlement.metadata.get('tier', ServiceTier.STANDARD), 0),
                sla_hash
            )
            
            # Build and send transaction
            tx_hash = await self._send_transaction(function)
            
            # Wait for receipt
            receipt = await self._wait_for_receipt(tx_hash)
            
            if receipt['status'] == 1:
                # Extract token ID from events
                token_id = self._extract_token_id_from_receipt(receipt)
                self.tokens_minted += settlement.quantity
                
                logger.info(f"Minted resource token {token_id} for settlement {settlement.id}")
                return token_id
            else:
                logger.error(f"Failed to mint resource token for settlement {settlement.id}")
                return None
                
        except Exception as e:
            logger.error(f"Error minting resource token: {e}")
            return None
    
    async def burn_resource_token(
        self,
        token_id: int,
        amount: int
    ) -> bool:
        """Burn resource tokens upon consumption"""
        try:
            function = self.contract.functions.burnResource(token_id, amount)
            tx_hash = await self._send_transaction(function)
            receipt = await self._wait_for_receipt(tx_hash)
            
            if receipt['status'] == 1:
                self.tokens_burned += amount
                logger.info(f"Burned {amount} units of token {token_id}")
                return True
            else:
                logger.error(f"Failed to burn token {token_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error burning resource token: {e}")
            return False
    
    async def slash_resource_token(
        self,
        token_id: int,
        violation_severity: int,  # In basis points (0-10000)
        reason: str
    ) -> bool:
        """Slash resource tokens for SLA violations"""
        try:
            function = self.contract.functions.slashResource(
                token_id,
                violation_severity,
                reason
            )
            tx_hash = await self._send_transaction(function)
            receipt = await self._wait_for_receipt(tx_hash)
            
            if receipt['status'] == 1:
                # Extract slashed amount from events
                slashed_amount = self._extract_slashed_amount_from_receipt(receipt)
                self.tokens_slashed += slashed_amount
                
                logger.info(f"Slashed token {token_id} for {reason}, amount: {slashed_amount}")
                return True
            else:
                logger.error(f"Failed to slash token {token_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error slashing resource token: {e}")
            return False
    
    async def register_provider(
        self,
        provider_address: str,
        initial_reputation: int = 500
    ) -> bool:
        """Register a new resource provider"""
        try:
            function = self.contract.functions.registerProvider(
                Web3.toChecksumAddress(provider_address),
                initial_reputation
            )
            tx_hash = await self._send_transaction(function)
            receipt = await self._wait_for_receipt(tx_hash)
            
            return receipt['status'] == 1
            
        except Exception as e:
            logger.error(f"Error registering provider: {e}")
            return False
    
    async def set_provider_capacity(
        self,
        provider_address: str,
        resource_type: ResourceType,
        capacity: int
    ) -> bool:
        """Set provider capacity for a resource type"""
        try:
            resource_type_map = {
                ResourceType.CPU: 0,
                ResourceType.GPU: 1,
                ResourceType.STORAGE: 2,
                ResourceType.BANDWIDTH: 3,
                ResourceType.MEMORY: 4
            }
            
            function = self.contract.functions.setProviderCapacity(
                Web3.toChecksumAddress(provider_address),
                resource_type_map.get(resource_type, 0),
                capacity
            )
            tx_hash = await self._send_transaction(function)
            receipt = await self._wait_for_receipt(tx_hash)
            
            return receipt['status'] == 1
            
        except Exception as e:
            logger.error(f"Error setting provider capacity: {e}")
            return False
    
    async def update_token_price(
        self,
        token_id: int,
        price_wei: int
    ) -> bool:
        """Update token price (oracle role required)"""
        try:
            function = self.contract.functions.updatePrice(token_id, price_wei)
            tx_hash = await self._send_transaction(function)
            receipt = await self._wait_for_receipt(tx_hash)
            
            return receipt['status'] == 1
            
        except Exception as e:
            logger.error(f"Error updating token price: {e}")
            return False
    
    async def get_resource_spec(self, token_id: int) -> Optional[Dict]:
        """Get resource specification for a token"""
        try:
            spec = self.contract.functions.getResourceSpec(token_id).call()
            
            return {
                'resource_type': spec[0],
                'amount': spec[1],
                'valid_from': datetime.fromtimestamp(spec[2]),
                'valid_until': datetime.fromtimestamp(spec[3]),
                'region': spec[4],
                'tier': spec[5],
                'provider': spec[6],
                'sla_hash': spec[7].hex(),
                'is_active': spec[8],
                'slashed_amount': spec[9]
            }
            
        except Exception as e:
            logger.error(f"Error getting resource spec: {e}")
            return None
    
    async def _send_transaction(self, function) -> str:
        """Build and send transaction"""
        # Get current gas price
        gas_price = await self._get_gas_price()
        
        # Get nonce
        nonce = await self._get_nonce()
        
        # Build transaction
        tx = function.buildTransaction({
            'from': self.account.address,
            'gas': 500000,  # Estimate in production
            'gasPrice': gas_price,
            'nonce': nonce
        })
        
        # Sign transaction
        signed_tx = self.adapter.web3.eth.account.sign_transaction(
            tx,
            private_key=self.account.key
        )
        
        # Send transaction
        tx_hash = self.adapter.web3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        return tx_hash.hex()
    
    async def _wait_for_receipt(self, tx_hash: str, timeout: int = 120):
        """Wait for transaction receipt"""
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            try:
                receipt = self.adapter.web3.eth.get_transaction_receipt(tx_hash)
                if receipt:
                    return receipt
            except Exception:
                pass
            
            await asyncio.sleep(2)
        
        raise TimeoutError(f"Transaction {tx_hash} not mined after {timeout} seconds")
    
    async def _get_gas_price(self) -> int:
        """Get current gas price with caching"""
        now = datetime.utcnow()
        
        if (self._gas_price_cache is None or 
            self._gas_price_timestamp is None or
            now - self._gas_price_timestamp > timedelta(seconds=30)):
            
            # Get current gas price
            gas_price = self.adapter.web3.eth.gas_price
            
            # Add 10% buffer for faster inclusion
            self._gas_price_cache = int(gas_price * 1.1)
            self._gas_price_timestamp = now
        
        return self._gas_price_cache
    
    async def _get_nonce(self) -> int:
        """Get account nonce with local tracking"""
        address = self.account.address
        
        if address not in self._nonce_cache:
            # Get on-chain nonce
            self._nonce_cache[address] = self.adapter.web3.eth.get_transaction_count(address)
        else:
            # Increment local nonce
            self._nonce_cache[address] += 1
        
        return self._nonce_cache[address]
    
    def _extract_token_id_from_receipt(self, receipt) -> int:
        """Extract token ID from mint transaction receipt"""
        # Parse ResourceMinted event
        for log in receipt['logs']:
            if log['topics'][0] == self.adapter.web3.keccak(text="ResourceMinted(uint256,address,uint8,uint256,string,uint8)"):
                # Token ID is the first indexed parameter
                return int(log['topics'][1].hex(), 16)
        
        return 0
    
    def _extract_slashed_amount_from_receipt(self, receipt) -> int:
        """Extract slashed amount from slash transaction receipt"""
        # Parse ResourceSlashed event
        for log in receipt['logs']:
            if log['topics'][0] == self.adapter.web3.keccak(text="ResourceSlashed(uint256,address,uint256,string)"):
                # Decode non-indexed parameters
                decoded = self.adapter.web3.eth.contract(
                    abi=self.contract_abi
                ).decode_function_input(log['data'])
                return decoded[2]  # slashedAmount
        
        return 0 