"""
Transaction management for blockchain gateway.
"""

import logging
import asyncio
from typing import Dict, Optional, List, Any
from decimal import Decimal
from dataclasses import dataclass
from datetime import datetime

from platformq_blockchain_common import (
    ChainType, Transaction, TransactionResult, TransactionStatus,
    GasStrategy, ITransactionManager, ConnectionPool
)
from pyignite import Client as IgniteClient

logger = logging.getLogger(__name__)


@dataclass
class TransactionRecord:
    """Record of a transaction for tracking"""
    tx_hash: str
    chain_type: ChainType
    transaction: Transaction
    status: TransactionStatus
    submitted_at: datetime
    confirmed_at: Optional[datetime] = None
    result: Optional[TransactionResult] = None
    attempts: int = 0
    error: Optional[str] = None


class TransactionManager(ITransactionManager):
    """
    Manages blockchain transactions across multiple chains.
    Features:
    - Transaction preparation with nonce management
    - Secure transaction signing
    - Transaction broadcasting and monitoring
    - Retry logic for failed transactions
    - Transaction history tracking
    """
    
    def __init__(self, connection_pool: ConnectionPool, ignite_client: IgniteClient):
        self.connection_pool = connection_pool
        self.ignite_client = ignite_client
        self._monitoring_tasks: Dict[str, asyncio.Task] = {}
        self._transaction_cache = None
        self._nonce_locks: Dict[str, asyncio.Lock] = {}
        
    async def initialize(self):
        """Initialize transaction manager"""
        # Create Ignite cache for transactions
        self._transaction_cache = await self.ignite_client.get_or_create_cache(
            "blockchain_transactions"
        )
        logger.info("Transaction manager initialized")
        
    async def prepare_transaction(self, from_address: str, to_address: str,
                                value: Decimal, data: Optional[bytes] = None,
                                chain_type: ChainType = ChainType.ETHEREUM,
                                gas_strategy: GasStrategy = GasStrategy.STANDARD) -> Transaction:
        """Prepare a transaction with proper nonce and gas settings"""
        async with self.connection_pool.get_connection(chain_type) as adapter:
            # Get nonce with lock to prevent nonce conflicts
            nonce_key = f"{chain_type.value}:{from_address}"
            if nonce_key not in self._nonce_locks:
                self._nonce_locks[nonce_key] = asyncio.Lock()
                
            async with self._nonce_locks[nonce_key]:
                # Get current nonce from chain
                current_nonce = await self._get_nonce(adapter, from_address)
                
                # Check for pending transactions and adjust nonce
                pending_nonce = await self._get_pending_nonce(chain_type, from_address)
                nonce = max(current_nonce, pending_nonce)
                
                # Create transaction
                transaction = Transaction(
                    from_address=from_address,
                    to_address=to_address,
                    value=value,
                    data=data,
                    nonce=nonce,
                    chain_id=adapter.chain_id
                )
                
                # Estimate gas
                gas_estimate = await adapter.estimate_gas(transaction)
                
                # Apply gas strategy
                if gas_strategy == GasStrategy.FAST:
                    transaction.gas_price = int(gas_estimate.gas_price * 1.2)
                elif gas_strategy == GasStrategy.INSTANT:
                    transaction.gas_price = int(gas_estimate.gas_price * 1.5)
                else:
                    transaction.gas_price = gas_estimate.gas_price
                    
                transaction.gas_limit = int(gas_estimate.gas_limit * 1.1)  # 10% buffer
                
                # For EIP-1559 chains
                if gas_estimate.max_fee_per_gas:
                    transaction.max_fee_per_gas = gas_estimate.max_fee_per_gas
                    transaction.max_priority_fee_per_gas = gas_estimate.max_priority_fee_per_gas
                    
                return transaction
                
    async def sign_transaction(self, transaction: Transaction, private_key: str) -> str:
        """Sign a transaction and return the signed data"""
        # In production, use secure key management (HSM, KMS, etc.)
        # For now, we'll store the key in metadata
        transaction.metadata['private_key'] = private_key
        return "signed"  # Placeholder
        
    async def broadcast_transaction(self, signed_tx: str, chain_type: ChainType,
                                  transaction: Transaction) -> str:
        """Broadcast a signed transaction and return the hash"""
        async with self.connection_pool.get_connection(chain_type) as adapter:
            result = await adapter.send_transaction(transaction)
            
            # Create transaction record
            record = TransactionRecord(
                tx_hash=result.transaction_hash,
                chain_type=chain_type,
                transaction=transaction,
                status=TransactionStatus.SUBMITTED,
                submitted_at=datetime.utcnow()
            )
            
            # Store in cache
            await self._transaction_cache.put(result.transaction_hash, record)
            
            # Start monitoring
            self._monitoring_tasks[result.transaction_hash] = asyncio.create_task(
                self._monitor_transaction(record)
            )
            
            return result.transaction_hash
            
    async def wait_for_confirmation(self, tx_hash: str, confirmations: int = 1) -> TransactionResult:
        """Wait for transaction confirmation"""
        record = await self._transaction_cache.get(tx_hash)
        if not record:
            raise ValueError(f"Transaction {tx_hash} not found")
            
        # Wait for monitoring task to complete
        if tx_hash in self._monitoring_tasks:
            await self._monitoring_tasks[tx_hash]
            
        if record.result and record.status == TransactionStatus.CONFIRMED:
            return record.result
        else:
            raise Exception(f"Transaction failed: {record.error}")
            
    async def get_transaction_status(self, tx_hash: str) -> TransactionResult:
        """Get current status of a transaction"""
        record = await self._transaction_cache.get(tx_hash)
        if not record:
            raise ValueError(f"Transaction {tx_hash} not found")
            
        if not record.result:
            # Try to fetch from chain
            async with self.connection_pool.get_connection(record.chain_type) as adapter:
                record.result = await adapter.get_transaction_receipt(tx_hash)
                if record.result:
                    record.status = record.result.status
                    await self._transaction_cache.put(tx_hash, record)
                    
        return record.result
        
    async def cancel_transaction(self, tx_hash: str, gas_multiplier: float = 1.1) -> str:
        """Attempt to cancel a pending transaction by replacement"""
        record = await self._transaction_cache.get(tx_hash)
        if not record:
            raise ValueError(f"Transaction {tx_hash} not found")
            
        if record.status != TransactionStatus.PENDING:
            raise ValueError("Can only cancel pending transactions")
            
        # Create replacement transaction with same nonce but higher gas
        cancel_tx = Transaction(
            from_address=record.transaction.from_address,
            to_address=record.transaction.from_address,  # Send to self
            value=Decimal(0),
            nonce=record.transaction.nonce,
            gas_price=int(record.transaction.gas_price * gas_multiplier),
            gas_limit=21000,  # Minimum gas for transfer
            chain_id=record.transaction.chain_id
        )
        
        # Copy private key if available
        if 'private_key' in record.transaction.metadata:
            cancel_tx.metadata['private_key'] = record.transaction.metadata['private_key']
            
        # Send cancellation transaction
        async with self.connection_pool.get_connection(record.chain_type) as adapter:
            result = await adapter.send_transaction(cancel_tx)
            
        logger.info(f"Sent cancellation transaction {result.transaction_hash} for {tx_hash}")
        return result.transaction_hash
        
    async def _monitor_transaction(self, record: TransactionRecord):
        """Monitor a transaction until confirmed or failed"""
        max_attempts = 60  # 5 minutes with 5 second intervals
        attempt = 0
        
        while attempt < max_attempts:
            try:
                async with self.connection_pool.get_connection(record.chain_type) as adapter:
                    result = await adapter.get_transaction_receipt(record.tx_hash)
                    
                    if result:
                        record.result = result
                        record.status = result.status
                        record.confirmed_at = datetime.utcnow()
                        await self._transaction_cache.put(record.tx_hash, record)
                        
                        logger.info(f"Transaction {record.tx_hash} confirmed with status {result.status}")
                        return
                        
            except Exception as e:
                logger.error(f"Error monitoring transaction {record.tx_hash}: {e}")
                
            await asyncio.sleep(5)
            attempt += 1
            
        # Timeout - mark as failed
        record.status = TransactionStatus.FAILED
        record.error = "Transaction confirmation timeout"
        await self._transaction_cache.put(record.tx_hash, record)
        
    async def _get_nonce(self, adapter, address: str) -> int:
        """Get current nonce for an address"""
        # This would be implemented in the adapter
        # For now, return a placeholder
        return 0
        
    async def _get_pending_nonce(self, chain_type: ChainType, address: str) -> int:
        """Get the next nonce considering pending transactions"""
        # Query cache for pending transactions
        # For now, return 0
        return 0 