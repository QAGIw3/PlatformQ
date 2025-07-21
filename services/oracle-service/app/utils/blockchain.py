"""
Blockchain Integration for Oracle Data
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime
import json
from eth_account import Account
from web3 import Web3
import asyncio

from ..models.measurements import OracleFeed, BatchOracleUpdate
from ..config import settings


logger = logging.getLogger(__name__)


class BlockchainOracle:
    """Handles blockchain interactions for oracle data submission"""
    
    def __init__(self):
        self.w3 = None
        self.account = None
        self.oracle_contract = None
        
    async def initialize(self):
        """Initialize blockchain connection"""
        try:
            if not settings.BLOCKCHAIN_RPC_URL:
                logger.warning("Blockchain RPC URL not configured")
                return
            
            # Connect to blockchain
            self.w3 = Web3(Web3.HTTPProvider(settings.BLOCKCHAIN_RPC_URL))
            
            # Check connection
            if not self.w3.is_connected():
                raise Exception("Failed to connect to blockchain")
            
            # Setup account if private key provided
            if settings.BLOCKCHAIN_PRIVATE_KEY:
                self.account = Account.from_key(settings.BLOCKCHAIN_PRIVATE_KEY)
                logger.info(f"Oracle account: {self.account.address}")
            
            # Load oracle contract if address provided
            if settings.ORACLE_CONTRACT_ADDRESS:
                # In production, would load actual ABI
                self.oracle_contract = self.w3.eth.contract(
                    address=settings.ORACLE_CONTRACT_ADDRESS,
                    abi=self._get_oracle_abi()
                )
            
            logger.info("Blockchain Oracle initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Blockchain Oracle: {e}")
            # Don't raise - allow service to run without blockchain
    
    async def submit_measurement(
        self,
        feed: OracleFeed
    ) -> Optional[str]:
        """Submit single measurement to blockchain"""
        if not self._is_ready():
            return None
        
        try:
            # Build transaction
            tx = self.oracle_contract.functions.updateFeed(
                feed.resource_id,
                feed.measurement_type.value,
                int(feed.aggregated_value * 1e6),  # Scale to avoid decimals
                int(feed.timestamp.timestamp()),
                int(feed.confidence * 100)
            ).build_transaction({
                'from': self.account.address,
                'gas': settings.BLOCKCHAIN_GAS_LIMIT,
                'gasPrice': self.w3.eth.gas_price,
                'nonce': self.w3.eth.get_transaction_count(self.account.address),
                'chainId': settings.CHAIN_ID
            })
            
            # Sign transaction
            signed_tx = self.w3.eth.account.sign_transaction(
                tx,
                private_key=settings.BLOCKCHAIN_PRIVATE_KEY
            )
            
            # Send transaction
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            # Wait for confirmation
            receipt = await self._wait_for_confirmation(tx_hash)
            
            if receipt['status'] == 1:
                logger.info(f"Submitted oracle feed: {tx_hash.hex()}")
                return tx_hash.hex()
            else:
                logger.error("Oracle feed submission failed")
                return None
                
        except Exception as e:
            logger.error(f"Failed to submit measurement: {e}")
            return None
    
    async def submit_batch(
        self,
        batch: BatchOracleUpdate
    ) -> Optional[str]:
        """Submit batch of measurements to blockchain"""
        if not self._is_ready():
            return None
        
        try:
            # Prepare batch data
            resource_ids = []
            measurement_types = []
            values = []
            timestamps = []
            confidences = []
            
            for update in batch.updates:
                resource_ids.append(update.resource_id)
                measurement_types.append(update.measurement_type.value)
                values.append(int(update.aggregated_value * 1e6))
                timestamps.append(int(update.timestamp.timestamp()))
                confidences.append(int(update.confidence * 100))
            
            # Build transaction
            tx = self.oracle_contract.functions.batchUpdateFeeds(
                resource_ids,
                measurement_types,
                values,
                timestamps,
                confidences
            ).build_transaction({
                'from': self.account.address,
                'gas': settings.BLOCKCHAIN_GAS_LIMIT * 2,  # Higher gas for batch
                'gasPrice': self.w3.eth.gas_price,
                'nonce': self.w3.eth.get_transaction_count(self.account.address),
                'chainId': settings.CHAIN_ID
            })
            
            # Sign transaction
            signed_tx = self.w3.eth.account.sign_transaction(
                tx,
                private_key=settings.BLOCKCHAIN_PRIVATE_KEY
            )
            
            # Send transaction
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            # Wait for confirmation
            receipt = await self._wait_for_confirmation(tx_hash)
            
            if receipt['status'] == 1:
                logger.info(f"Submitted batch oracle update: {tx_hash.hex()}")
                return tx_hash.hex()
            else:
                logger.error("Batch oracle update failed")
                return None
                
        except Exception as e:
            logger.error(f"Failed to submit batch: {e}")
            return None
    
    async def update_quality_score(
        self,
        resource_id: str,
        quality_score: int,
        timestamp: datetime
    ) -> Optional[str]:
        """Update quality score on blockchain"""
        if not self._is_ready():
            return None
        
        try:
            # Build transaction
            tx = self.oracle_contract.functions.updateQualityScore(
                resource_id,
                quality_score,
                int(timestamp.timestamp())
            ).build_transaction({
                'from': self.account.address,
                'gas': settings.BLOCKCHAIN_GAS_LIMIT,
                'gasPrice': self.w3.eth.gas_price,
                'nonce': self.w3.eth.get_transaction_count(self.account.address),
                'chainId': settings.CHAIN_ID
            })
            
            # Sign and send
            signed_tx = self.w3.eth.account.sign_transaction(
                tx,
                private_key=settings.BLOCKCHAIN_PRIVATE_KEY
            )
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            
            # Wait for confirmation
            receipt = await self._wait_for_confirmation(tx_hash)
            
            if receipt['status'] == 1:
                logger.info(f"Updated quality score: {tx_hash.hex()}")
                return tx_hash.hex()
            else:
                logger.error("Quality score update failed")
                return None
                
        except Exception as e:
            logger.error(f"Failed to update quality score: {e}")
            return None
    
    def _is_ready(self) -> bool:
        """Check if blockchain oracle is ready"""
        return (
            self.w3 is not None and
            self.w3.is_connected() and
            self.account is not None and
            self.oracle_contract is not None
        )
    
    async def _wait_for_confirmation(
        self,
        tx_hash: bytes,
        timeout: int = 120
    ) -> dict:
        """Wait for transaction confirmation"""
        start_time = asyncio.get_event_loop().time()
        
        while asyncio.get_event_loop().time() - start_time < timeout:
            try:
                receipt = self.w3.eth.get_transaction_receipt(tx_hash)
                if receipt is not None:
                    return receipt
            except Exception:
                pass
            
            await asyncio.sleep(1)
        
        raise TimeoutError("Transaction confirmation timeout")
    
    def _get_oracle_abi(self) -> list:
        """Get Oracle contract ABI"""
        # Simplified ABI - in production would load from file
        return [
            {
                "inputs": [
                    {"name": "_resourceId", "type": "string"},
                    {"name": "_measurementType", "type": "string"},
                    {"name": "_value", "type": "uint256"},
                    {"name": "_timestamp", "type": "uint256"},
                    {"name": "_confidence", "type": "uint256"}
                ],
                "name": "updateFeed",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {"name": "_resourceIds", "type": "string[]"},
                    {"name": "_measurementTypes", "type": "string[]"},
                    {"name": "_values", "type": "uint256[]"},
                    {"name": "_timestamps", "type": "uint256[]"},
                    {"name": "_confidences", "type": "uint256[]"}
                ],
                "name": "batchUpdateFeeds",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {"name": "_resourceId", "type": "string"},
                    {"name": "_score", "type": "uint256"},
                    {"name": "_timestamp", "type": "uint256"}
                ],
                "name": "updateQualityScore",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            }
        ]


def sign_oracle_data(
    data: Dict,
    private_key: str
) -> str:
    """Sign oracle data for verification"""
    # Create message
    message = json.dumps(data, sort_keys=True)
    
    # Sign message
    account = Account.from_key(private_key)
    message_hash = Web3.keccak(text=message)
    signature = account.signHash(message_hash)
    
    return signature.signature.hex()


def verify_oracle_signature(
    data: Dict,
    signature: str,
    expected_address: str
) -> bool:
    """Verify oracle data signature"""
    try:
        # Recreate message
        message = json.dumps(data, sort_keys=True)
        message_hash = Web3.keccak(text=message)
        
        # Recover signer
        recovered_address = Account.recover_message(
            message_hash,
            signature=bytes.fromhex(signature)
        )
        
        return recovered_address.lower() == expected_address.lower()
        
    except Exception as e:
        logger.error(f"Signature verification failed: {e}")
        return False 