"""
EVM (Ethereum Virtual Machine) adapter for Ethereum-compatible chains.
Supports Ethereum, Polygon, Arbitrum, Optimism, Avalanche, BSC, etc.
"""

import logging
from typing import Optional, Dict, Any, List
from decimal import Decimal
from web3 import Web3
from web3.middleware import geth_poa_middleware
from eth_account import Account

from ..types import (
    ChainType, ChainConfig, TransactionResult, GasEstimate,
    TransactionStatus, ChainMetadata
)
from ..models import Transaction, SmartContract
from ..utils import validate_address, normalize_address, to_wei_amount
from .base import BaseAdapter

logger = logging.getLogger(__name__)


class EVMAdapter(BaseAdapter):
    """Adapter for EVM-compatible blockchains"""
    
    def __init__(self, config: ChainConfig):
        super().__init__(config)
        self.w3: Optional[Web3] = None
        
    async def connect(self) -> bool:
        """Connect to the EVM chain"""
        try:
            if self.config.ws_url:
                provider = Web3.WebsocketProvider(self.config.ws_url)
            else:
                provider = Web3.HTTPProvider(self.config.rpc_url)
                
            self.w3 = Web3(provider)
            
            # Add POA middleware for chains like Polygon, BSC
            if self.chain_type in [ChainType.POLYGON, ChainType.BSC]:
                self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)
                
            # Test connection
            if self.w3.is_connected():
                chain_id = self.w3.eth.chain_id
                if chain_id != self.config.chain_id:
                    logger.warning(f"Chain ID mismatch: expected {self.config.chain_id}, got {chain_id}")
                    
                self._connected = True
                logger.info(f"Connected to {self.chain_type.value} at {self.config.rpc_url}")
                return True
            else:
                logger.error(f"Failed to connect to {self.chain_type.value}")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to {self.chain_type.value}: {e}")
            return False
            
    async def disconnect(self) -> None:
        """Disconnect from the chain"""
        if self.w3 and hasattr(self.w3.provider, 'disconnect'):
            self.w3.provider.disconnect()
        self._connected = False
        logger.info(f"Disconnected from {self.chain_type.value}")
        
    async def get_balance(self, address: str, token_address: Optional[str] = None) -> Decimal:
        """Get balance of native currency or token"""
        if not self._connected:
            raise ConnectionError("Not connected to blockchain")
            
        address = normalize_address(address)
        
        if token_address is None:
            # Native currency balance
            balance_wei = self.w3.eth.get_balance(address)
            return Decimal(str(balance_wei)) / Decimal(10 ** self.config.decimals)
        else:
            # ERC20 token balance
            # Simplified - in production, use proper ERC20 ABI
            raise NotImplementedError("Token balance not implemented yet")
            
    async def send_transaction(self, transaction: Transaction) -> TransactionResult:
        """Send a transaction to the blockchain"""
        if not self._connected:
            raise ConnectionError("Not connected to blockchain")
            
        # Build transaction dict
        tx_dict = {
            'from': transaction.from_address,
            'to': transaction.to_address,
            'value': int(transaction.value),
            'nonce': transaction.nonce or self.w3.eth.get_transaction_count(transaction.from_address),
            'chainId': self.config.chain_id
        }
        
        if transaction.data:
            tx_dict['data'] = transaction.data
            
        # Add gas parameters
        if self.config.supports_eip1559 and transaction.max_fee_per_gas:
            tx_dict['maxFeePerGas'] = transaction.max_fee_per_gas
            tx_dict['maxPriorityFeePerGas'] = transaction.max_priority_fee_per_gas
        else:
            tx_dict['gasPrice'] = transaction.gas_price or self.w3.eth.gas_price
            
        tx_dict['gas'] = transaction.gas_limit or await self.w3.eth.estimate_gas(tx_dict)
        
        # Sign and send
        # Note: In production, use secure key management
        if 'private_key' in transaction.metadata:
            signed_tx = self.w3.eth.account.sign_transaction(tx_dict, transaction.metadata['private_key'])
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        else:
            # Assume unlocked account (for testing)
            tx_hash = self.w3.eth.send_transaction(tx_dict)
            
        # Wait for receipt
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        
        return self._build_transaction_result(receipt)
        
    async def estimate_gas(self, transaction: Transaction) -> GasEstimate:
        """Estimate gas for a transaction"""
        if not self._connected:
            raise ConnectionError("Not connected to blockchain")
            
        tx_dict = {
            'from': transaction.from_address,
            'to': transaction.to_address,
            'value': int(transaction.value)
        }
        
        if transaction.data:
            tx_dict['data'] = transaction.data
            
        gas_limit = await self.w3.eth.estimate_gas(tx_dict)
        
        if self.config.supports_eip1559:
            base_fee = self.w3.eth.get_block('latest')['baseFeePerGas']
            priority_fee = self.w3.eth.max_priority_fee
            max_fee = base_fee * 2 + priority_fee
            
            return GasEstimate(
                gas_limit=gas_limit,
                gas_price=base_fee + priority_fee,
                max_fee_per_gas=max_fee,
                max_priority_fee_per_gas=priority_fee,
                total_cost_wei=gas_limit * max_fee,
                total_cost_native=Decimal(str(gas_limit * max_fee)) / Decimal(10 ** 18)
            )
        else:
            gas_price = self.w3.eth.gas_price
            
            return GasEstimate(
                gas_limit=gas_limit,
                gas_price=gas_price,
                total_cost_wei=gas_limit * gas_price,
                total_cost_native=Decimal(str(gas_limit * gas_price)) / Decimal(10 ** 18)
            )
            
    async def get_transaction_receipt(self, tx_hash: str) -> Optional[TransactionResult]:
        """Get transaction receipt by hash"""
        if not self._connected:
            raise ConnectionError("Not connected to blockchain")
            
        try:
            receipt = self.w3.eth.get_transaction_receipt(tx_hash)
            return self._build_transaction_result(receipt)
        except Exception:
            return None
            
    async def deploy_contract(self, contract: SmartContract) -> str:
        """Deploy a smart contract"""
        if not self._connected:
            raise ConnectionError("Not connected to blockchain")
            
        # Create contract instance
        contract_instance = self.w3.eth.contract(
            abi=contract.abi,
            bytecode=contract.bytecode
        )
        
        # Build constructor transaction
        constructor_tx = contract_instance.constructor(*contract.constructor_args)
        
        # Estimate gas
        gas_estimate = constructor_tx.estimate_gas()
        
        # Build transaction
        tx_dict = constructor_tx.build_transaction({
            'from': contract.metadata.get('deployer_address'),
            'gas': gas_estimate,
            'gasPrice': self.w3.eth.gas_price,
            'nonce': self.w3.eth.get_transaction_count(contract.metadata.get('deployer_address'))
        })
        
        # Sign and send (simplified - use secure key management in production)
        if 'private_key' in contract.metadata:
            signed_tx = self.w3.eth.account.sign_transaction(tx_dict, contract.metadata['private_key'])
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        else:
            tx_hash = self.w3.eth.send_transaction(tx_dict)
            
        # Wait for receipt
        receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
        
        return receipt.contractAddress
        
    async def call_contract(self, contract_address: str, method: str,
                          params: List[Any], abi: List[Dict[str, Any]]) -> Any:
        """Call a smart contract method (read-only)"""
        if not self._connected:
            raise ConnectionError("Not connected to blockchain")
            
        contract = self.w3.eth.contract(
            address=normalize_address(contract_address),
            abi=abi
        )
        
        method_fn = getattr(contract.functions, method)
        return method_fn(*params).call()
        
    async def get_block_number(self) -> int:
        """Get current block number"""
        if not self._connected:
            raise ConnectionError("Not connected to blockchain")
            
        return self.w3.eth.block_number
        
    async def get_chain_metadata(self) -> ChainMetadata:
        """Get current chain metadata"""
        if not self._connected:
            raise ConnectionError("Not connected to blockchain")
            
        latest_block = self.w3.eth.get_block('latest')
        
        metadata = ChainMetadata(
            chain_type=self.chain_type,
            latest_block=latest_block['number'],
            syncing=self.w3.eth.syncing,
            peer_count=self.w3.net.peer_count if hasattr(self.w3.net, 'peer_count') else 1,
            gas_price=self.w3.eth.gas_price
        )
        
        if self.config.supports_eip1559:
            metadata.base_fee = latest_block.get('baseFeePerGas')
            metadata.priority_fee = self.w3.eth.max_priority_fee
            
        return metadata
        
    def _build_transaction_result(self, receipt: Dict[str, Any]) -> TransactionResult:
        """Build TransactionResult from receipt"""
        block = self.w3.eth.get_block(receipt['blockNumber'])
        
        status = TransactionStatus.CONFIRMED if receipt['status'] == 1 else TransactionStatus.REVERTED
        
        return TransactionResult(
            transaction_hash=receipt['transactionHash'].hex(),
            block_number=receipt['blockNumber'],
            block_hash=receipt['blockHash'].hex(),
            gas_used=receipt['gasUsed'],
            gas_price=receipt.get('effectiveGasPrice', 0),
            status=status,
            chain_type=self.chain_type,
            timestamp=block['timestamp'],
            from_address=receipt['from'],
            to_address=receipt.get('to'),
            logs=receipt.get('logs', [])
        ) 