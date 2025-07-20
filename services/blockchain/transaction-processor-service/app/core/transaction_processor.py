"""
Transaction Processor - Manages transaction lifecycle
"""

import asyncio
import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from uuid import uuid4

import aiopulsar
from pyignite import AsyncClient as IgniteClient
from tenacity import retry, stop_after_attempt, wait_exponential
from prometheus_client import Counter, Histogram, Gauge
import httpx

from ..config import Settings
from ..models.transaction import (
    Transaction, TransactionStatus, TransactionEvent,
    TransactionResult, TransactionPriority
)
from .nonce_manager import NonceManager
from .gas_manager import GasManager
from .signing_service import SigningService

logger = logging.getLogger(__name__)

# Metrics
transactions_processed = Counter(
    'transactions_processed_total',
    'Total transactions processed',
    ['chain', 'status']
)

transaction_duration = Histogram(
    'transaction_processing_duration_seconds',
    'Transaction processing duration',
    ['chain', 'type']
)

active_transactions = Gauge(
    'active_transactions',
    'Currently processing transactions',
    ['chain']
)

transaction_errors = Counter(
    'transaction_errors_total',
    'Total transaction errors',
    ['chain', 'error_type']
)


class TransactionProcessor:
    """Manages transaction processing lifecycle"""
    
    def __init__(
        self,
        settings: Settings,
        ignite_client: IgniteClient,
        pulsar_client: aiopulsar.Client,
        blockchain_connector_url: str,
        key_management_url: str
    ):
        self.settings = settings
        self.ignite = ignite_client
        self.pulsar = pulsar_client
        self.blockchain_connector_url = blockchain_connector_url
        self.key_management_url = key_management_url
        
        # Initialize managers
        self.nonce_manager = NonceManager(ignite_client, settings)
        self.gas_manager = GasManager(blockchain_connector_url, settings)
        self.signing_service = SigningService(key_management_url, settings)
        
        # Processing state
        self._running = False
        self._processing_tasks: Dict[str, asyncio.Task] = {}
        self._transaction_cache = None
        self._status_publisher = None
        self._consumer = None
        
        # HTTP client for API calls
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
    async def start(self):
        """Start transaction processor"""
        logger.info("Starting Transaction Processor")
        
        # Initialize caches
        self._transaction_cache = await self.ignite.get_or_create_cache("transactions")
        
        # Initialize Pulsar publisher
        self._status_publisher = await self.pulsar.create_producer(
            self.settings.TRANSACTION_STATUS_TOPIC
        )
        
        # Initialize Pulsar consumer
        self._consumer = await self.pulsar.subscribe(
            self.settings.TRANSACTION_QUEUE_TOPIC,
            subscription_name=f"{self.settings.SERVICE_NAME}-processor",
            consumer_type=aiopulsar.ConsumerType.Shared
        )
        
        # Start managers
        await self.nonce_manager.start()
        await self.gas_manager.start()
        
        # Start processing loop
        self._running = True
        asyncio.create_task(self._process_transactions())
        
        logger.info("Transaction Processor started")
        
    async def stop(self):
        """Stop transaction processor"""
        logger.info("Stopping Transaction Processor")
        self._running = False
        
        # Cancel processing tasks
        for task in self._processing_tasks.values():
            task.cancel()
            
        # Wait for tasks to complete
        if self._processing_tasks:
            await asyncio.gather(*self._processing_tasks.values(), return_exceptions=True)
            
        # Stop managers
        await self.nonce_manager.stop()
        await self.gas_manager.stop()
        
        # Close resources
        if self._consumer:
            await self._consumer.close()
        if self._status_publisher:
            await self._status_publisher.close()
        await self.http_client.aclose()
        
        logger.info("Transaction Processor stopped")
        
    async def _process_transactions(self):
        """Main processing loop"""
        while self._running:
            try:
                # Receive messages with timeout
                message = await asyncio.wait_for(
                    self._consumer.receive(),
                    timeout=self.settings.TRANSACTION_BATCH_TIMEOUT
                )
                
                # Parse transaction
                try:
                    tx_data = json.loads(message.data())
                    transaction = Transaction(**tx_data)
                    
                    # Acknowledge message
                    await self._consumer.acknowledge(message)
                    
                    # Process transaction
                    if len(self._processing_tasks) < self.settings.MAX_CONCURRENT_TRANSACTIONS:
                        task = asyncio.create_task(
                            self._process_single_transaction(transaction)
                        )
                        self._processing_tasks[transaction.id] = task
                        
                        # Clean up completed tasks
                        task.add_done_callback(
                            lambda t: self._processing_tasks.pop(transaction.id, None)
                        )
                    else:
                        # Re-queue if at capacity
                        logger.warning(f"At capacity, re-queuing transaction {transaction.id}")
                        await self._consumer.negative_acknowledge(message)
                        
                except Exception as e:
                    logger.error(f"Error parsing transaction: {e}")
                    await self._consumer.acknowledge(message)
                    
            except asyncio.TimeoutError:
                # No messages, continue
                continue
            except Exception as e:
                logger.error(f"Error in processing loop: {e}")
                await asyncio.sleep(1)
                
    async def _process_single_transaction(self, transaction: Transaction):
        """Process a single transaction"""
        logger.info(f"Processing transaction {transaction.id}")
        active_transactions.labels(chain=transaction.chain).inc()
        
        try:
            with transaction_duration.labels(
                chain=transaction.chain,
                type=transaction.type.value
            ).time():
                # Update status to processing
                await self._update_transaction_status(
                    transaction,
                    TransactionStatus.PROCESSING
                )
                
                # Check expiration
                if transaction.expires_at and transaction.expires_at < datetime.utcnow():
                    raise Exception("Transaction expired")
                    
                # Get nonce
                nonce = await self.nonce_manager.get_nonce(
                    transaction.chain,
                    transaction.from_address
                )
                transaction.nonce = nonce
                
                # Get gas settings
                if not transaction.gas_limit or not transaction.gas_price:
                    gas_params = await self.gas_manager.get_optimal_gas(
                        transaction.chain,
                        transaction
                    )
                    transaction.gas_limit = transaction.gas_limit or gas_params['gas_limit']
                    transaction.gas_price = transaction.gas_price or gas_params['gas_price']
                    transaction.max_fee_per_gas = gas_params.get('max_fee_per_gas')
                    transaction.max_priority_fee_per_gas = gas_params.get('max_priority_fee_per_gas')
                    
                # Build transaction
                built_tx = await self._build_transaction(transaction)
                
                # Sign transaction
                await self._update_transaction_status(
                    transaction,
                    TransactionStatus.SIGNING
                )
                
                signed_tx = await self.signing_service.sign_transaction(
                    transaction.chain,
                    transaction.from_address,
                    built_tx
                )
                
                # Broadcast transaction
                await self._update_transaction_status(
                    transaction,
                    TransactionStatus.BROADCASTING
                )
                
                tx_hash = await self._broadcast_transaction(
                    transaction.chain,
                    signed_tx
                )
                
                # Update transaction with hash
                transaction_result = TransactionResult(
                    id=transaction.id,
                    status=TransactionStatus.BROADCAST,
                    tx_hash=tx_hash,
                    created_at=transaction.created_at,
                    broadcast_at=datetime.utcnow()
                )
                
                await self._update_transaction_status(
                    transaction,
                    TransactionStatus.BROADCAST,
                    tx_hash=tx_hash
                )
                
                # Monitor confirmation
                asyncio.create_task(
                    self._monitor_confirmation(transaction, tx_hash)
                )
                
                # Update metrics
                transactions_processed.labels(
                    chain=transaction.chain,
                    status="success"
                ).inc()
                
                logger.info(f"Transaction {transaction.id} broadcast: {tx_hash}")
                
        except Exception as e:
            logger.error(f"Error processing transaction {transaction.id}: {e}")
            
            # Update status to failed
            await self._update_transaction_status(
                transaction,
                TransactionStatus.FAILED,
                error=str(e)
            )
            
            # Update metrics
            transactions_processed.labels(
                chain=transaction.chain,
                status="failed"
            ).inc()
            
            transaction_errors.labels(
                chain=transaction.chain,
                error_type=type(e).__name__
            ).inc()
            
        finally:
            active_transactions.labels(chain=transaction.chain).dec()
            
    async def _build_transaction(self, transaction: Transaction) -> Dict[str, Any]:
        """Build raw transaction"""
        tx_dict = {
            'from': transaction.from_address,
            'to': transaction.to_address,
            'value': int(transaction.value),
            'nonce': transaction.nonce,
            'gas': transaction.gas_limit,
            'chainId': await self._get_chain_id(transaction.chain)
        }
        
        if transaction.data:
            tx_dict['data'] = transaction.data
            
        # Add gas price based on chain support
        if transaction.max_fee_per_gas:
            # EIP-1559
            tx_dict['maxFeePerGas'] = int(transaction.max_fee_per_gas)
            tx_dict['maxPriorityFeePerGas'] = int(transaction.max_priority_fee_per_gas)
        else:
            # Legacy
            tx_dict['gasPrice'] = int(transaction.gas_price)
            
        return tx_dict
        
    async def _get_chain_id(self, chain: str) -> int:
        """Get chain ID from blockchain connector"""
        response = await self.http_client.get(
            f"{self.blockchain_connector_url}/api/v1/chains/{chain}/info"
        )
        response.raise_for_status()
        return response.json()['chain_id']
        
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10)
    )
    async def _broadcast_transaction(self, chain: str, signed_tx: str) -> str:
        """Broadcast transaction to blockchain"""
        response = await self.http_client.post(
            f"{self.blockchain_connector_url}/api/v1/broadcast",
            json={
                "chain": chain,
                "signed_tx": signed_tx
            }
        )
        response.raise_for_status()
        return response.json()['tx_hash']
        
    async def _monitor_confirmation(self, transaction: Transaction, tx_hash: str):
        """Monitor transaction confirmation"""
        try:
            required_confirmations = self.settings.CHAIN_CONFIRMATION_BLOCKS.get(
                transaction.chain, 12
            )
            
            await self._update_transaction_status(
                transaction,
                TransactionStatus.CONFIRMING
            )
            
            confirmations = 0
            while confirmations < required_confirmations:
                await asyncio.sleep(10)  # Check every 10 seconds
                
                # Get transaction details
                response = await self.http_client.post(
                    f"{self.blockchain_connector_url}/api/v1/transaction",
                    json={
                        "chain": transaction.chain,
                        "tx_hash": tx_hash
                    }
                )
                
                if response.status_code == 200:
                    tx_data = response.json()
                    if tx_data.get('blockNumber'):
                        # Get current block
                        current_block_response = await self.http_client.get(
                            f"{self.blockchain_connector_url}/api/v1/block/{transaction.chain}/latest"
                        )
                        current_block = current_block_response.json()['block_number']
                        
                        confirmations = current_block - tx_data['blockNumber'] + 1
                        logger.info(
                            f"Transaction {transaction.id} has {confirmations} confirmations"
                        )
                        
                        # Send confirmation update
                        await self._send_status_event(
                            TransactionEvent(
                                transaction_id=transaction.id,
                                status=TransactionStatus.CONFIRMING,
                                tx_hash=tx_hash,
                                block_number=tx_data['blockNumber'],
                                confirmations=confirmations
                            )
                        )
                        
            # Mark as confirmed
            await self._update_transaction_status(
                transaction,
                TransactionStatus.CONFIRMED
            )
            
            logger.info(f"Transaction {transaction.id} confirmed")
            
        except Exception as e:
            logger.error(f"Error monitoring confirmation for {transaction.id}: {e}")
            
    async def _update_transaction_status(
        self,
        transaction: Transaction,
        status: TransactionStatus,
        tx_hash: Optional[str] = None,
        error: Optional[str] = None
    ):
        """Update transaction status"""
        # Update in cache
        tx_data = transaction.dict()
        tx_data['status'] = status.value
        if tx_hash:
            tx_data['tx_hash'] = tx_hash
        if error:
            tx_data['error'] = error
            
        await self._transaction_cache.put(transaction.id, tx_data)
        
        # Send status event
        event = TransactionEvent(
            transaction_id=transaction.id,
            status=status,
            previous_status=getattr(transaction, '_last_status', None),
            tx_hash=tx_hash,
            error=error
        )
        
        await self._send_status_event(event)
        
        # Update transaction object
        transaction._last_status = status
        
    async def _send_status_event(self, event: TransactionEvent):
        """Send status event to Pulsar"""
        try:
            await self._status_publisher.send(
                event.json().encode('utf-8')
            )
        except Exception as e:
            logger.error(f"Error sending status event: {e}")
            
    async def submit_transaction(self, transaction: Transaction) -> str:
        """Submit a new transaction for processing"""
        # Store in cache
        await self._transaction_cache.put(
            transaction.id,
            transaction.dict()
        )
        
        # Send to processing queue
        producer = await self.pulsar.create_producer(
            self.settings.TRANSACTION_QUEUE_TOPIC
        )
        
        try:
            await producer.send(
                transaction.json().encode('utf-8')
            )
            
            # Send initial status
            await self._send_status_event(
                TransactionEvent(
                    transaction_id=transaction.id,
                    status=TransactionStatus.QUEUED
                )
            )
            
            return transaction.id
            
        finally:
            await producer.close()
            
    async def get_transaction_status(self, transaction_id: str) -> Optional[TransactionResult]:
        """Get transaction status"""
        tx_data = await self._transaction_cache.get(transaction_id)
        if not tx_data:
            return None
            
        return TransactionResult(**tx_data) 