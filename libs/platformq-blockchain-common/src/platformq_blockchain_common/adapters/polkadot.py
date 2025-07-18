"""
Polkadot/Substrate blockchain adapter implementation.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from decimal import Decimal
from substrateinterface import SubstrateInterface, Keypair
from substrateinterface.exceptions import SubstrateRequestException
import scale_codec

from ..interfaces import IBlockchainAdapter
from ..types import (
    ChainType, TransactionResult, GasEstimate, BlockchainEvent,
    ChainMetadata, TransactionStatus
)
from ..models import Transaction as PlatformTransaction, SmartContract
from ..utils import retry_with_backoff, normalize_address

logger = logging.getLogger(__name__)


class PolkadotAdapter(IBlockchainAdapter):
    """Polkadot/Substrate blockchain adapter"""
    
    def __init__(self, ws_url: str, chain_name: str = "Polkadot"):
        self.ws_url = ws_url
        self.chain_name = chain_name
        self.substrate: Optional[SubstrateInterface] = None
        self._connected = False
        self._chain_id = hash(chain_name) % 1000000  # Numeric representation
        
    @property
    def chain_type(self) -> ChainType:
        return ChainType.POLKADOT
        
    @property
    def chain_id(self) -> int:
        return self._chain_id
        
    @retry_with_backoff(max_retries=3)
    async def connect(self) -> bool:
        """Connect to Substrate/Polkadot network"""
        try:
            # Run in thread pool since substrate-interface is sync
            loop = asyncio.get_event_loop()
            
            def _connect():
                self.substrate = SubstrateInterface(
                    url=self.ws_url,
                    auto_discover=True
                )
                # Test connection
                self.substrate.get_chain_head()
                return True
                
            await loop.run_in_executor(None, _connect)
            self._connected = True
            logger.info(f"Connected to {self.chain_name} network at {self.ws_url}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to {self.chain_name}: {e}")
            self._connected = False
            raise
            
    async def disconnect(self) -> None:
        """Disconnect from network"""
        if self.substrate:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.substrate.close)
            self.substrate = None
            self._connected = False
            logger.info(f"Disconnected from {self.chain_name} network")
            
    async def get_balance(self, address: str, token_address: Optional[str] = None) -> Decimal:
        """Get DOT/KSM balance or asset balance"""
        if not self._connected:
            raise ConnectionError(f"Not connected to {self.chain_name} network")
            
        try:
            loop = asyncio.get_event_loop()
            
            def _get_balance():
                if token_address:
                    # TODO Asset balance query would go here
                    # For now return 0 as placeholder
                    return 0
                else:
                    # Get native balance
                    result = self.substrate.query(
                        module='System',
                        storage_function='Account',
                        params=[address]
                    )
                    if result:
                        # Balance is in planck (10^-10 DOT)
                        free_balance = result['data']['free']
                        return free_balance
                    return 0
                    
            balance_planck = await loop.run_in_executor(None, _get_balance)
            # Convert from planck to DOT (10 decimals)
            return Decimal(balance_planck) / Decimal(10**10)
            
        except Exception as e:
            logger.error(f"Error getting balance: {e}")
            raise
            
    async def send_transaction(self, transaction: PlatformTransaction) -> TransactionResult:
        """Send a transaction to Polkadot network"""
        if not self._connected:
            raise ConnectionError(f"Not connected to {self.chain_name} network")
            
        try:
            # TODO Mock implementation since we don't have the private key
            tx_hash = f"0x{hash(str(transaction)):064x}"
            
            return TransactionResult(
                transaction_hash=tx_hash,
                status=TransactionStatus.PENDING,
                block_number=None,
                gas_used=Decimal("1000000000"),  # Weight units
                gas_price=Decimal("0.01"),
                confirmations=0,
                timestamp=None,
                raw_receipt={"extrinsic_hash": tx_hash}
            )
            
        except Exception as e:
            logger.error(f"Error sending transaction: {e}")
            raise
            
    async def estimate_gas(self, transaction: PlatformTransaction) -> GasEstimate:
        """Estimate weight/fees for transaction"""
        if not self._connected:
            raise ConnectionError(f"Not connected to {self.chain_name} network")
            
        # Basic weight estimation
        base_weight = 1000000000  # 1 unit of weight
        
        if transaction.data:
            # Add weight for data
            base_weight += len(transaction.data) * 1000000
            
        # TODO Fee calculation would involve more complex logic
        # This is simplified
        fee_per_weight = Decimal("0.00000001")  # DOT per weight unit
        
        return GasEstimate(
            gas_limit=base_weight,
            gas_price=fee_per_weight,
            total_cost=Decimal(base_weight) * fee_per_weight,
            priority_fees={
                "slow": fee_per_weight * Decimal("0.8"),
                "standard": fee_per_weight,
                "fast": fee_per_weight * Decimal("1.2")
            }
        )
        
    async def get_transaction_receipt(self, tx_hash: str) -> Optional[TransactionResult]:
        """Get transaction/extrinsic receipt"""
        if not self._connected:
            raise ConnectionError(f"Not connected to {self.chain_name} network")
            
        try:
            loop = asyncio.get_event_loop()
            
            def _get_receipt():
                # Query for extrinsic by hash
                # TODO This is a simplified version
                block_hash = self.substrate.get_block_hash()
                block = self.substrate.get_block(block_hash)
                
                # TODO In reality, we'd search through blocks for the extrinsic
                # For now, return mock data
                return {
                    "block_number": self.substrate.get_block_number(block_hash),
                    "success": True,
                    "weight": 1000000000
                }
                
            receipt_data = await loop.run_in_executor(None, _get_receipt)
            
            if receipt_data:
                return TransactionResult(
                    transaction_hash=tx_hash,
                    status=TransactionStatus.SUCCESS if receipt_data["success"] else TransactionStatus.FAILED,
                    block_number=receipt_data["block_number"],
                    gas_used=Decimal(receipt_data["weight"]),
                    gas_price=Decimal("0.00000001"),
                    confirmations=1,
                    timestamp=None,
                    raw_receipt=receipt_data
                )
            return None
            
        except Exception as e:
            logger.error(f"Error getting transaction receipt: {e}")
            return None
            
    async def deploy_contract(self, contract: SmartContract) -> str:
        """Deploy ink! smart contract"""
        if not self._connected:
            raise ConnectionError(f"Not connected to {self.chain_name} network")
            
        # TODO ink! contract deployment is complex
        # This is a placeholder
        contract_address = f"5{hash(contract.bytecode) % 10**47}"  # Substrate SS58 format
        logger.info(f"Mock deployed ink! contract: {contract_address}")
        return contract_address
        
    async def call_contract(self, contract_address: str, method: str, 
                          params: List[Any], abi: List[Dict[str, Any]]) -> Any:
        """Call ink! smart contract (read-only)"""
        if not self._connected:
            raise ConnectionError(f"Not connected to {self.chain_name} network")
            
        # TODO Contract calls would require the contract pallet
        # This is a placeholder
        logger.info(f"Mock calling contract {contract_address}.{method}")
        return None
        
    async def get_block_number(self) -> int:
        """Get current block number"""
        if not self._connected:
            raise ConnectionError(f"Not connected to {self.chain_name} network")
            
        try:
            loop = asyncio.get_event_loop()
            block_number = await loop.run_in_executor(
                None, 
                self.substrate.get_block_number
            )
            return block_number
            
        except Exception as e:
            logger.error(f"Error getting block number: {e}")
            raise
            
    async def get_chain_metadata(self) -> ChainMetadata:
        """Get current chain metadata"""
        if not self._connected:
            raise ConnectionError(f"Not connected to {self.chain_name} network")
            
        try:
            loop = asyncio.get_event_loop()
            
            def _get_metadata():
                block_number = self.substrate.get_block_number()
                runtime_version = self.substrate.runtime_version
                chain_properties = self.substrate.properties
                
                return {
                    "block_number": block_number,
                    "runtime_version": runtime_version,
                    "token_decimals": chain_properties.get('tokenDecimals', [10])[0],
                    "token_symbol": chain_properties.get('tokenSymbol', ['DOT'])[0],
                    "ss58_format": chain_properties.get('ss58Format', 0)
                }
                
            metadata = await loop.run_in_executor(None, _get_metadata)
            
            return ChainMetadata(
                chain_id=self.chain_id,
                chain_type=self.chain_type,
                latest_block=metadata["block_number"],
                gas_price=Decimal("0.00000001"),  # Weight fee
                network_name=self.chain_name,
                currency_symbol=metadata["token_symbol"],
                currency_decimals=metadata["token_decimals"],
                is_testnet="testnet" in self.chain_name.lower(),
                extra_data={
                    "runtime_version": metadata["runtime_version"],
                    "ss58_format": metadata["ss58_format"]
                }
            )
            
        except Exception as e:
            logger.error(f"Error getting chain metadata: {e}")
            raise 