"""
Solana blockchain adapter implementation.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from decimal import Decimal
from solana.rpc.async_api import AsyncClient
from solana.keypair import Keypair
from solana.publickey import PublicKey
from solana.transaction import Transaction
from solana.system_program import TransferParams, transfer
import base58

from ..interfaces import IBlockchainAdapter
from ..types import (
    ChainType, TransactionResult, GasEstimate, BlockchainEvent,
    ChainMetadata, TransactionStatus
)
from ..models import Transaction as PlatformTransaction, SmartContract
from ..utils import retry_with_backoff, normalize_address

logger = logging.getLogger(__name__)


class SolanaAdapter(IBlockchainAdapter):
    """Solana blockchain adapter"""
    
    def __init__(self, rpc_url: str, chain_id: int = 101):
        self.rpc_url = rpc_url
        self._chain_id = chain_id
        self.client: Optional[AsyncClient] = None
        self._connected = False
        
    @property
    def chain_type(self) -> ChainType:
        return ChainType.SOLANA
        
    @property 
    def chain_id(self) -> int:
        return self._chain_id
        
    @retry_with_backoff(max_retries=3)
    async def connect(self) -> bool:
        """Connect to Solana network"""
        try:
            self.client = AsyncClient(self.rpc_url)
            # Test connection
            await self.client.get_health()
            self._connected = True
            logger.info(f"Connected to Solana network at {self.rpc_url}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Solana: {e}")
            self._connected = False
            raise
            
    async def disconnect(self) -> None:
        """Disconnect from Solana network"""
        if self.client:
            await self.client.close()
            self.client = None
            self._connected = False
            logger.info("Disconnected from Solana network")
            
    async def get_balance(self, address: str, token_address: Optional[str] = None) -> Decimal:
        """Get SOL balance or SPL token balance"""
        if not self._connected:
            raise ConnectionError("Not connected to Solana network")
            
        try:
            pubkey = PublicKey(address)
            
            if token_address:
                # TODO Get SPL token balance
                # This would require additional SPL token program interaction
                # For now, return 0 as placeholder
                return Decimal("0")
            else:
                # Get SOL balance
                response = await self.client.get_balance(pubkey)
                if response["result"]["value"] is not None:
                    # Convert lamports to SOL
                    return Decimal(response["result"]["value"]) / Decimal(10**9)
                return Decimal("0")
                
        except Exception as e:
            logger.error(f"Error getting balance: {e}")
            raise
            
    async def send_transaction(self, transaction: PlatformTransaction) -> TransactionResult:
        """Send a transaction to Solana network"""
        if not self._connected:
            raise ConnectionError("Not connected to Solana network")
            
        try:
            # Create Solana transaction
            from_pubkey = PublicKey(transaction.from_address)
            to_pubkey = PublicKey(transaction.to_address)
            
            # Create transfer instruction
            transfer_ix = transfer(
                TransferParams(
                    from_pubkey=from_pubkey,
                    to_pubkey=to_pubkey,
                    lamports=int(transaction.value * 10**9)  # Convert SOL to lamports
                )
            )
            
            # Create transaction
            txn = Transaction().add(transfer_ix)
            
            # Get recent blockhash
            recent_blockhash = await self.client.get_recent_blockhash()
            txn.recent_blockhash = recent_blockhash["result"]["value"]["blockhash"]
            
            # TODO Sign transaction (would need private key in real implementation)
            # For now, return mock result
            tx_hash = base58.b58encode(b"mock_solana_tx_hash").decode()
            
            return TransactionResult(
                transaction_hash=tx_hash,
                status=TransactionStatus.PENDING,
                block_number=None,
                gas_used=Decimal("5000"),  # Solana uses fixed fees
                gas_price=Decimal("0.000005"),
                confirmations=0,
                timestamp=None,
                raw_receipt={"signature": tx_hash}
            )
            
        except Exception as e:
            logger.error(f"Error sending transaction: {e}")
            raise
            
    async def estimate_gas(self, transaction: PlatformTransaction) -> GasEstimate:
        """Estimate gas (compute units) for Solana transaction"""
        if not self._connected:
            raise ConnectionError("Not connected to Solana network")
            
        # Solana has relatively fixed fees
        return GasEstimate(
            gas_limit=200000,  # Compute units
            gas_price=Decimal("0.000005"),  # SOL per signature
            total_cost=Decimal("0.000005"),  # Fixed fee
            priority_fees={
                "slow": Decimal("0"),
                "standard": Decimal("0.000001"),
                "fast": Decimal("0.000005")
            }
        )
        
    async def get_transaction_receipt(self, tx_hash: str) -> Optional[TransactionResult]:
        """Get transaction receipt by signature"""
        if not self._connected:
            raise ConnectionError("Not connected to Solana network")
            
        try:
            response = await self.client.get_transaction(tx_hash)
            if response["result"]:
                tx_data = response["result"]
                return TransactionResult(
                    transaction_hash=tx_hash,
                    status=TransactionStatus.SUCCESS if tx_data["meta"]["err"] is None else TransactionStatus.FAILED,
                    block_number=tx_data["slot"],
                    gas_used=Decimal(tx_data["meta"]["fee"]) / Decimal(10**9),
                    gas_price=Decimal("0.000005"),
                    confirmations=1,
                    timestamp=tx_data["blockTime"],
                    raw_receipt=tx_data
                )
            return None
            
        except Exception as e:
            logger.error(f"Error getting transaction receipt: {e}")
            return None
            
    async def deploy_contract(self, contract: SmartContract) -> str:
        """Deploy a program to Solana"""
        if not self._connected:
            raise ConnectionError("Not connected to Solana network")
            
        # TODO Solana program deployment is complex and requires BPF bytecode
        # This is a placeholder implementation
        program_id = base58.b58encode(b"mock_program_id").decode()
        logger.info(f"Mock deployed Solana program: {program_id}")
        return program_id
        
    async def call_contract(self, contract_address: str, method: str, 
                          params: List[Any], abi: List[Dict[str, Any]]) -> Any:
        """Call a Solana program (read-only)"""
        if not self._connected:
            raise ConnectionError("Not connected to Solana network")
            
        # TODO Program calls in Solana are different from EVM
        # This would require program-specific implementation
        logger.info(f"Mock calling Solana program {contract_address}.{method}")
        return None
        
    async def get_block_number(self) -> int:
        """Get current slot number"""
        if not self._connected:
            raise ConnectionError("Not connected to Solana network")
            
        try:
            response = await self.client.get_slot()
            return response["result"]
        except Exception as e:
            logger.error(f"Error getting slot number: {e}")
            raise
            
    async def get_chain_metadata(self) -> ChainMetadata:
        """Get current chain metadata"""
        if not self._connected:
            raise ConnectionError("Not connected to Solana network")
            
        try:
            slot = await self.get_block_number()
            version = await self.client.get_version()
            
            return ChainMetadata(
                chain_id=self.chain_id,
                chain_type=self.chain_type,
                latest_block=slot,
                gas_price=Decimal("0.000005"),  # Fixed fee
                network_name="solana-mainnet" if self.chain_id == 101 else "solana-testnet",
                currency_symbol="SOL",
                currency_decimals=9,
                is_testnet=self.chain_id != 101,
                extra_data={
                    "version": version["result"]["solana-core"],
                    "feature_set": version["result"]["feature-set"]
                }
            )
        except Exception as e:
            logger.error(f"Error getting chain metadata: {e}")
            raise 