"""
Main blockchain gateway service.
"""

import logging
from typing import Dict, Optional, List, Any
from decimal import Decimal
from datetime import datetime
import json

from platformq_blockchain_common import (
    ChainType, ChainConfig, Transaction, TransactionResult,
    GasEstimate, GasStrategy, ConnectionPool, ChainMetadata,
    SmartContract
)
from pyignite import Client as IgniteClient
import pulsar

from .adapter_registry import AdapterRegistry
from .transaction_manager import TransactionManager
from .gas_optimizer import GasOptimizer

logger = logging.getLogger(__name__)


class BlockchainGateway:
    """
    Main gateway for all blockchain operations.
    Provides a unified interface for interacting with multiple blockchains.
    """
    
    def __init__(self, ignite_client: IgniteClient, pulsar_client: pulsar.Client):
        self.ignite_client = ignite_client
        self.pulsar_client = pulsar_client
        
        # Initialize components
        self.connection_pool = ConnectionPool()
        self.adapter_registry = AdapterRegistry(self.connection_pool)
        self.transaction_manager = TransactionManager(self.connection_pool, ignite_client)
        self.gas_optimizer = GasOptimizer(self.connection_pool, ignite_client)
        
        # Event producer for blockchain events
        self.event_producer = None
        
    async def initialize(self):
        """Initialize the gateway and all components"""
        logger.info("Initializing blockchain gateway...")
        
        # Initialize components
        await self.connection_pool.initialize()
        await self.transaction_manager.initialize()
        await self.gas_optimizer.initialize()
        
        # Create event producer
        self.event_producer = self.pulsar_client.create_producer(
            'persistent://public/blockchain/events'
        )
        
        logger.info("Blockchain gateway initialized")
        
    async def shutdown(self):
        """Shutdown the gateway and cleanup resources"""
        logger.info("Shutting down blockchain gateway...")
        
        await self.gas_optimizer.shutdown()
        await self.connection_pool.close()
        
        if self.event_producer:
            self.event_producer.close()
            
        logger.info("Blockchain gateway shutdown complete")
        
    def register_chain(self, config: ChainConfig):
        """Register a new blockchain configuration"""
        self.adapter_registry.register_chain_config(config)
        logger.info(f"Registered chain {config.name} ({config.chain_type.value})")
        
    def get_supported_chains(self) -> List[Dict[str, Any]]:
        """Get list of supported chains with their configurations"""
        chains = []
        for chain_type in self.adapter_registry.get_supported_chains():
            config = self.adapter_registry.get_config(chain_type)
            chains.append({
                "chain_type": chain_type.value,
                "chain_id": config.chain_id,
                "name": config.name,
                "native_currency": config.native_currency,
                "rpc_url": config.rpc_url,
                "explorer_url": config.explorer_url
            })
        return chains
        
    async def get_balance(self, chain_type: ChainType, address: str,
                         token_address: Optional[str] = None) -> Dict[str, Any]:
        """Get balance for an address"""
        async with self.connection_pool.get_connection(chain_type) as adapter:
            balance = await adapter.get_balance(address, token_address)
            
            # Get USD value
            usd_price = await self.gas_optimizer._get_native_token_usd_price(chain_type)
            usd_value = balance * usd_price if usd_price else None
            
            return {
                "chain": chain_type.value,
                "address": address,
                "balance": str(balance),
                "currency": adapter.config.native_currency,
                "usd_value": str(usd_value) if usd_value else None
            }
            
    async def send_transaction(self, chain_type: ChainType,
                             from_address: str,
                             to_address: str,
                             value: Decimal,
                             data: Optional[str] = None,
                             gas_strategy: GasStrategy = GasStrategy.STANDARD,
                             private_key: Optional[str] = None) -> Dict[str, Any]:
        """Send a transaction on the specified chain"""
        # Prepare transaction
        transaction = await self.transaction_manager.prepare_transaction(
            from_address=from_address,
            to_address=to_address,
            value=value,
            data=bytes.fromhex(data[2:]) if data and data.startswith('0x') else None,
            chain_type=chain_type,
            gas_strategy=gas_strategy
        )
        
        # Sign if private key provided
        if private_key:
            await self.transaction_manager.sign_transaction(transaction, private_key)
            
        # Broadcast
        tx_hash = await self.transaction_manager.broadcast_transaction(
            "signed", chain_type, transaction
        )
        
        # Emit event
        await self._emit_event("transaction_sent", {
            "tx_hash": tx_hash,
            "chain": chain_type.value,
            "from": from_address,
            "to": to_address,
            "value": str(value)
        })
        
        return {
            "tx_hash": tx_hash,
            "chain": chain_type.value,
            "status": "submitted"
        }
        
    async def get_transaction_status(self, tx_hash: str) -> Dict[str, Any]:
        """Get status of a transaction"""
        result = await self.transaction_manager.get_transaction_status(tx_hash)
        
        return {
            "tx_hash": result.transaction_hash,
            "status": result.status.value,
            "block_number": result.block_number,
            "gas_used": result.gas_used,
            "timestamp": result.timestamp.isoformat()
        }
        
    async def estimate_gas(self, chain_type: ChainType,
                         from_address: str,
                         to_address: str,
                         value: Decimal,
                         data: Optional[str] = None) -> Dict[str, Any]:
        """Estimate gas for a transaction"""
        transaction = Transaction(
            from_address=from_address,
            to_address=to_address,
            value=value,
            data=bytes.fromhex(data[2:]) if data and data.startswith('0x') else None
        )
        
        estimate = await self.gas_optimizer.estimate_transaction_cost(transaction)
        
        return {
            "gas_limit": estimate.gas_limit,
            "gas_price": estimate.gas_price,
            "max_fee_per_gas": estimate.max_fee_per_gas,
            "max_priority_fee_per_gas": estimate.max_priority_fee_per_gas,
            "total_cost_native": str(estimate.total_cost_native),
            "total_cost_usd": str(estimate.total_cost_usd) if estimate.total_cost_usd else None
        }
        
    async def get_optimal_gas_price(self, chain_type: ChainType,
                                   strategy: GasStrategy = GasStrategy.STANDARD) -> Dict[str, Any]:
        """Get optimal gas price for a chain"""
        estimate = await self.gas_optimizer.get_optimal_gas_price(chain_type, strategy)
        
        return {
            "chain": chain_type.value,
            "strategy": strategy.value,
            "gas_price": estimate.gas_price,
            "max_fee_per_gas": estimate.max_fee_per_gas,
            "max_priority_fee_per_gas": estimate.max_priority_fee_per_gas
        }
        
    async def deploy_contract(self, chain_type: ChainType,
                            contract: SmartContract,
                            deployer_address: str,
                            private_key: Optional[str] = None) -> Dict[str, Any]:
        """Deploy a smart contract"""
        contract.metadata['deployer_address'] = deployer_address
        if private_key:
            contract.metadata['private_key'] = private_key
            
        async with self.connection_pool.get_connection(chain_type) as adapter:
            contract_address = await adapter.deploy_contract(contract)
            
        # Emit event
        await self._emit_event("contract_deployed", {
            "chain": chain_type.value,
            "contract_address": contract_address,
            "deployer": deployer_address,
            "name": contract.name
        })
        
        return {
            "chain": chain_type.value,
            "contract_address": contract_address,
            "deployer": deployer_address
        }
        
    async def call_contract(self, chain_type: ChainType,
                          contract_address: str,
                          method: str,
                          params: List[Any],
                          abi: List[Dict[str, Any]]) -> Any:
        """Call a contract method (read-only)"""
        async with self.connection_pool.get_connection(chain_type) as adapter:
            result = await adapter.call_contract(contract_address, method, params, abi)
            
        return result
        
    async def get_chain_metadata(self, chain_type: ChainType) -> Dict[str, Any]:
        """Get current metadata for a chain"""
        async with self.connection_pool.get_connection(chain_type) as adapter:
            metadata = await adapter.get_chain_metadata()
            
        return {
            "chain": chain_type.value,
            "latest_block": metadata.latest_block,
            "syncing": metadata.syncing,
            "gas_price": metadata.gas_price,
            "base_fee": metadata.base_fee,
            "priority_fee": metadata.priority_fee
        }
        
    async def _emit_event(self, event_type: str, data: Dict[str, Any]):
        """Emit an event to Pulsar"""
        if self.event_producer:
            event = {
                "type": event_type,
                "data": data,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.event_producer.send(
                json.dumps(event).encode('utf-8')
            )


# Import required modules
from datetime import datetime
import json 