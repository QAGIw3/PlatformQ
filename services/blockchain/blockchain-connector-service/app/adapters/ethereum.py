"""
Ethereum/EVM adapter implementation
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from decimal import Decimal

from web3 import Web3
from web3.exceptions import TransactionNotFound
from eth_utils import is_address, to_checksum_address

from .base import BaseChainAdapter
from ..models.chain_types import ChainType
from ..config import ChainConfig

logger = logging.getLogger(__name__)


class EthereumAdapter(BaseChainAdapter):
    """Adapter for Ethereum and EVM-compatible chains"""
    
    def __init__(self, chain_type: ChainType, config: ChainConfig):
        super().__init__(chain_type, config)
        self.w3: Optional[Web3] = None
        
    async def connect(self) -> bool:
        """Connect to Ethereum node"""
        try:
            endpoint = self.get_best_endpoint()
            if not endpoint:
                logger.error(f"No endpoints available for {self.chain_type}")
                return False
                
            # Create Web3 instance
            if endpoint.startswith('ws'):
                self.w3 = Web3(Web3.WebsocketProvider(endpoint))
            else:
                self.w3 = Web3(Web3.HTTPProvider(endpoint))
                
            # Test connection
            if await asyncio.to_thread(self.w3.is_connected):
                self._connected = True
                self.current_endpoint = endpoint
                
                # Verify chain ID
                chain_id = await asyncio.to_thread(lambda: self.w3.eth.chain_id)
                if chain_id != self.config.chain_id:
                    logger.warning(
                        f"Chain ID mismatch for {self.chain_type}: "
                        f"expected {self.config.chain_id}, got {chain_id}"
                    )
                    
                logger.info(f"Connected to {self.chain_type} at {endpoint}")
                return True
            else:
                logger.error(f"Failed to connect to {self.chain_type}")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to {self.chain_type}: {e}")
            return False
            
    async def disconnect(self) -> None:
        """Disconnect from Ethereum node"""
        self._connected = False
        self.current_endpoint = None
        if self.w3 and hasattr(self.w3.provider, 'disconnect'):
            await asyncio.to_thread(self.w3.provider.disconnect)
            
    async def get_latest_block(self) -> int:
        """Get the latest block number"""
        if not self._connected or not self.w3:
            raise ConnectionError("Not connected to blockchain")
            
        return await asyncio.to_thread(lambda: self.w3.eth.block_number)
        
    async def get_balance(
        self,
        address: str,
        token_address: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get balance for an address"""
        if not self._connected or not self.w3:
            raise ConnectionError("Not connected to blockchain")
            
        # Validate address
        if not is_address(address):
            raise ValueError(f"Invalid address: {address}")
            
        address = to_checksum_address(address)
        
        if token_address:
            # ERC20 token balance
            if not is_address(token_address):
                raise ValueError(f"Invalid token address: {token_address}")
                
            token_address = to_checksum_address(token_address)
            
            # ERC20 ABI for balanceOf
            abi = [{
                "constant": True,
                "inputs": [{"name": "_owner", "type": "address"}],
                "name": "balanceOf",
                "outputs": [{"name": "balance", "type": "uint256"}],
                "type": "function"
            }, {
                "constant": True,
                "inputs": [],
                "name": "decimals",
                "outputs": [{"name": "", "type": "uint8"}],
                "type": "function"
            }]
            
            contract = self.w3.eth.contract(address=token_address, abi=abi)
            
            # Get balance and decimals
            balance = await asyncio.to_thread(
                contract.functions.balanceOf(address).call
            )
            decimals = await asyncio.to_thread(
                contract.functions.decimals().call
            )
            
            return {
                "address": address,
                "token_address": token_address,
                "balance": str(balance),
                "decimals": decimals,
                "formatted": str(Decimal(balance) / Decimal(10 ** decimals))
            }
        else:
            # Native ETH balance
            balance_wei = await asyncio.to_thread(
                self.w3.eth.get_balance, address
            )
            
            return {
                "address": address,
                "balance": str(balance_wei),
                "decimals": 18,
                "formatted": str(self.w3.from_wei(balance_wei, 'ether'))
            }
            
    async def get_transaction(self, tx_hash: str) -> Dict[str, Any]:
        """Get transaction details"""
        if not self._connected or not self.w3:
            raise ConnectionError("Not connected to blockchain")
            
        try:
            tx = await asyncio.to_thread(
                self.w3.eth.get_transaction, tx_hash
            )
            receipt = await asyncio.to_thread(
                self.w3.eth.get_transaction_receipt, tx_hash
            )
            
            return {
                "hash": tx_hash,
                "from": tx['from'],
                "to": tx['to'],
                "value": str(tx['value']),
                "gas": tx['gas'],
                "gasPrice": str(tx.get('gasPrice', 0)),
                "nonce": tx['nonce'],
                "blockNumber": receipt['blockNumber'] if receipt else None,
                "blockHash": receipt['blockHash'].hex() if receipt else None,
                "status": receipt['status'] if receipt else None,
                "gasUsed": receipt['gasUsed'] if receipt else None,
                "input": tx['input']
            }
        except TransactionNotFound:
            raise ValueError(f"Transaction not found: {tx_hash}")
            
    async def broadcast_transaction(self, signed_tx: str) -> str:
        """Broadcast a signed transaction"""
        if not self._connected or not self.w3:
            raise ConnectionError("Not connected to blockchain")
            
        # Send raw transaction
        tx_hash = await asyncio.to_thread(
            self.w3.eth.send_raw_transaction, signed_tx
        )
        
        return tx_hash.hex()
        
    async def estimate_gas(
        self,
        from_address: str,
        to_address: str,
        value: str,
        data: Optional[str] = None
    ) -> Dict[str, Any]:
        """Estimate gas for a transaction"""
        if not self._connected or not self.w3:
            raise ConnectionError("Not connected to blockchain")
            
        # Build transaction
        tx = {
            'from': to_checksum_address(from_address),
            'to': to_checksum_address(to_address),
            'value': int(value)
        }
        
        if data:
            tx['data'] = data
            
        # Estimate gas
        gas_limit = await asyncio.to_thread(
            self.w3.eth.estimate_gas, tx
        )
        
        # Get gas price
        gas_price_data = await self.get_gas_price()
        
        return {
            "gasLimit": gas_limit,
            "gasPrice": gas_price_data.get("standard"),
            "maxFeePerGas": gas_price_data.get("maxFeePerGas"),
            "maxPriorityFeePerGas": gas_price_data.get("maxPriorityFeePerGas"),
            "estimatedCost": str(gas_limit * int(gas_price_data.get("standard", 0)))
        }
        
    async def get_gas_price(self) -> Dict[str, Any]:
        """Get current gas price"""
        if not self._connected or not self.w3:
            raise ConnectionError("Not connected to blockchain")
            
        # Get base gas price
        gas_price = await asyncio.to_thread(self.w3.eth.gas_price)
        
        # Check if EIP-1559 is supported
        latest_block = await asyncio.to_thread(
            self.w3.eth.get_block, 'latest'
        )
        
        if 'baseFeePerGas' in latest_block:
            # EIP-1559 chain
            base_fee = latest_block['baseFeePerGas']
            priority_fee = gas_price - base_fee if gas_price > base_fee else 1000000000  # 1 gwei
            
            return {
                "standard": str(gas_price),
                "slow": str(int(gas_price * 0.8)),
                "fast": str(int(gas_price * 1.2)),
                "instant": str(int(gas_price * 1.5)),
                "baseFeePerGas": str(base_fee),
                "maxPriorityFeePerGas": str(priority_fee),
                "maxFeePerGas": str(base_fee * 2 + priority_fee)
            }
        else:
            # Legacy gas pricing
            return {
                "standard": str(gas_price),
                "slow": str(int(gas_price * 0.8)),
                "fast": str(int(gas_price * 1.2)),
                "instant": str(int(gas_price * 1.5))
            }
            
    async def get_nonce(self, address: str) -> int:
        """Get next nonce for an address"""
        if not self._connected or not self.w3:
            raise ConnectionError("Not connected to blockchain")
            
        address = to_checksum_address(address)
        return await asyncio.to_thread(
            self.w3.eth.get_transaction_count, address
        )
        
    async def call_contract(
        self,
        contract_address: str,
        method: str,
        params: List[Any],
        abi: List[Dict[str, Any]]
    ) -> Any:
        """Call a smart contract method"""
        if not self._connected or not self.w3:
            raise ConnectionError("Not connected to blockchain")
            
        contract_address = to_checksum_address(contract_address)
        contract = self.w3.eth.contract(address=contract_address, abi=abi)
        
        # Get the method
        contract_method = getattr(contract.functions, method)
        
        # Call the method
        result = await asyncio.to_thread(
            contract_method(*params).call
        )
        
        return result
        
    async def validate_address(self, address: str) -> bool:
        """Validate Ethereum address"""
        return is_address(address)
        
    async def get_block(self, block_number: int) -> Dict[str, Any]:
        """Get block details"""
        if not self._connected or not self.w3:
            raise ConnectionError("Not connected to blockchain")
            
        block = await asyncio.to_thread(
            self.w3.eth.get_block, block_number
        )
        
        return {
            "number": block['number'],
            "hash": block['hash'].hex(),
            "parentHash": block['parentHash'].hex(),
            "timestamp": block['timestamp'],
            "gasUsed": block['gasUsed'],
            "gasLimit": block['gasLimit'],
            "miner": block['miner'],
            "transactionCount": len(block['transactions'])
        }
        
    async def get_logs(
        self,
        from_block: int,
        to_block: int,
        address: Optional[str] = None,
        topics: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Get logs/events"""
        if not self._connected or not self.w3:
            raise ConnectionError("Not connected to blockchain")
            
        filter_params = {
            'fromBlock': from_block,
            'toBlock': to_block
        }
        
        if address:
            filter_params['address'] = to_checksum_address(address)
            
        if topics:
            filter_params['topics'] = topics
            
        logs = await asyncio.to_thread(
            self.w3.eth.get_logs, filter_params
        )
        
        return [
            {
                "address": log['address'],
                "topics": [topic.hex() for topic in log['topics']],
                "data": log['data'],
                "blockNumber": log['blockNumber'],
                "transactionHash": log['transactionHash'].hex(),
                "transactionIndex": log['transactionIndex'],
                "blockHash": log['blockHash'].hex(),
                "logIndex": log['logIndex']
            }
            for log in logs
        ] 