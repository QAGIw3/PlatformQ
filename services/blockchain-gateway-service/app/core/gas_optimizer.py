"""
Gas optimization service for blockchain transactions.
"""

import logging
import asyncio
from typing import Dict, Optional, List, Any
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass

from platformq_blockchain_common import (
    ChainType, Transaction, GasEstimate, GasStrategy,
    IGasOptimizer, ConnectionPool
)
from pyignite import Client as IgniteClient

logger = logging.getLogger(__name__)


@dataclass
class GasMetricsSnapshot:
    """Snapshot of gas metrics at a point in time"""
    chain_type: ChainType
    timestamp: datetime
    base_fee: Optional[int]
    priority_fee: Optional[int]
    gas_price: int
    block_number: int
    pending_tx_count: int
    

class GasOptimizer(IGasOptimizer):
    """
    Optimizes gas usage for blockchain transactions.
    Features:
    - Real-time gas price monitoring
    - Historical gas price analysis
    - Transaction batching
    - Optimal timing recommendations
    - MEV protection strategies
    """
    
    def __init__(self, connection_pool: ConnectionPool, ignite_client: IgniteClient):
        self.connection_pool = connection_pool
        self.ignite_client = ignite_client
        self._gas_metrics_cache = None
        self._monitoring_tasks: Dict[ChainType, asyncio.Task] = {}
        self._gas_history: Dict[ChainType, List[GasMetricsSnapshot]] = {}
        
    async def initialize(self):
        """Initialize gas optimizer"""
        # Create Ignite cache for gas metrics
        self._gas_metrics_cache = await self.ignite_client.get_or_create_cache(
            "gas_metrics"
        )
        
        # Start monitoring for registered chains
        for chain_type in self.connection_pool._configs.keys():
            self._start_monitoring(chain_type)
            
        logger.info("Gas optimizer initialized")
        
    async def shutdown(self):
        """Shutdown gas optimizer"""
        # Cancel monitoring tasks
        for task in self._monitoring_tasks.values():
            task.cancel()
            
        await asyncio.gather(*self._monitoring_tasks.values(), return_exceptions=True)
        logger.info("Gas optimizer shutdown")
        
    async def get_optimal_gas_price(self, chain_type: ChainType,
                                   strategy: GasStrategy = GasStrategy.STANDARD) -> GasEstimate:
        """Get optimal gas price for a chain and strategy"""
        # Get current metrics
        metrics = await self._get_current_metrics(chain_type)
        if not metrics:
            raise ValueError(f"No gas metrics available for {chain_type.value}")
            
        # Calculate gas based on strategy
        if strategy == GasStrategy.SLOW:
            multiplier = 0.8
            estimated_wait = 300  # 5 minutes
        elif strategy == GasStrategy.FAST:
            multiplier = 1.2
            estimated_wait = 30  # 30 seconds
        elif strategy == GasStrategy.INSTANT:
            multiplier = 1.5
            estimated_wait = 15  # 15 seconds
        else:  # STANDARD
            multiplier = 1.0
            estimated_wait = 60  # 1 minute
            
        if metrics.base_fee is not None:  # EIP-1559
            base_fee = metrics.base_fee
            priority_fee = int(metrics.priority_fee * multiplier)
            max_fee = int(base_fee * 2 + priority_fee)  # 2x base fee buffer
            
            return GasEstimate(
                gas_limit=0,  # Will be set by transaction
                gas_price=base_fee + priority_fee,
                max_fee_per_gas=max_fee,
                max_priority_fee_per_gas=priority_fee
            )
        else:  # Legacy
            gas_price = int(metrics.gas_price * multiplier)
            
            return GasEstimate(
                gas_limit=0,
                gas_price=gas_price
            )
            
    async def estimate_transaction_cost(self, transaction: Transaction) -> GasEstimate:
        """Estimate total cost of a transaction"""
        chain_type = self._get_chain_type_for_id(transaction.chain_id)
        
        async with self.connection_pool.get_connection(chain_type) as adapter:
            # Get gas estimate from chain
            estimate = await adapter.estimate_gas(transaction)
            
            # Add current USD price if available
            usd_price = await self._get_native_token_usd_price(chain_type)
            if usd_price:
                estimate.total_cost_usd = estimate.total_cost_native * usd_price
                
            return estimate
            
    async def batch_transactions(self, transactions: List[Transaction]) -> Transaction:
        """Batch multiple transactions into one for gas savings"""
        if not transactions:
            raise ValueError("No transactions to batch")
            
        # All transactions must be on same chain and from same sender
        chain_id = transactions[0].chain_id
        from_address = transactions[0].from_address
        
        for tx in transactions[1:]:
            if tx.chain_id != chain_id:
                raise ValueError("All transactions must be on same chain")
            if tx.from_address != from_address:
                raise ValueError("All transactions must be from same address")
                
        # Create multicall transaction
        # This is a simplified version - in production, use proper multicall contract
        multicall_data = self._encode_multicall(transactions)
        
        return Transaction(
            from_address=from_address,
            to_address="0x...",  # Multicall contract address
            value=Decimal(0),
            data=multicall_data,
            chain_id=chain_id
        )
        
    async def find_optimal_time(self, chain_type: ChainType) -> Dict[str, Any]:
        """Find optimal time to submit transaction based on gas prices"""
        # Analyze historical data
        history = self._gas_history.get(chain_type, [])
        if len(history) < 24:  # Need at least 24 hours of data
            return {
                "recommendation": "insufficient_data",
                "current_gas_price": await self._get_current_gas_price(chain_type)
            }
            
        # Find patterns (simplified - in production, use ML)
        hourly_averages = self._calculate_hourly_averages(history)
        
        # Find cheapest hour
        cheapest_hour = min(hourly_averages.items(), key=lambda x: x[1])[0]
        current_hour = datetime.utcnow().hour
        current_price = await self._get_current_gas_price(chain_type)
        average_price = sum(hourly_averages.values()) / len(hourly_averages)
        
        return {
            "recommendation": "wait" if current_price > average_price * 1.1 else "send_now",
            "current_gas_price": current_price,
            "average_gas_price": average_price,
            "cheapest_hour": cheapest_hour,
            "hourly_averages": hourly_averages,
            "potential_savings": max(0, (current_price - average_price) / current_price * 100)
        }
        
    def _start_monitoring(self, chain_type: ChainType):
        """Start monitoring gas prices for a chain"""
        if chain_type not in self._monitoring_tasks:
            self._monitoring_tasks[chain_type] = asyncio.create_task(
                self._monitor_gas_prices(chain_type)
            )
            self._gas_history[chain_type] = []
            
    async def _monitor_gas_prices(self, chain_type: ChainType):
        """Monitor gas prices for a chain"""
        while True:
            try:
                async with self.connection_pool.get_connection(chain_type) as adapter:
                    metadata = await adapter.get_chain_metadata()
                    
                    snapshot = GasMetricsSnapshot(
                        chain_type=chain_type,
                        timestamp=datetime.utcnow(),
                        base_fee=metadata.base_fee,
                        priority_fee=metadata.priority_fee,
                        gas_price=metadata.gas_price,
                        block_number=metadata.latest_block,
                        pending_tx_count=0  # Would need mempool access
                    )
                    
                    # Store in cache
                    cache_key = f"{chain_type.value}:current"
                    await self._gas_metrics_cache.put(cache_key, snapshot)
                    
                    # Add to history
                    self._gas_history[chain_type].append(snapshot)
                    
                    # Keep only last 7 days
                    cutoff = datetime.utcnow() - timedelta(days=7)
                    self._gas_history[chain_type] = [
                        s for s in self._gas_history[chain_type]
                        if s.timestamp > cutoff
                    ]
                    
            except Exception as e:
                logger.error(f"Error monitoring gas prices for {chain_type.value}: {e}")
                
            await asyncio.sleep(15)  # Check every 15 seconds
            
    async def _get_current_metrics(self, chain_type: ChainType) -> Optional[GasMetricsSnapshot]:
        """Get current gas metrics for a chain"""
        cache_key = f"{chain_type.value}:current"
        return await self._gas_metrics_cache.get(cache_key)
        
    async def _get_current_gas_price(self, chain_type: ChainType) -> int:
        """Get current gas price for a chain"""
        metrics = await self._get_current_metrics(chain_type)
        if metrics:
            if metrics.base_fee is not None:
                return metrics.base_fee + metrics.priority_fee
            return metrics.gas_price
        return 0
        
    def _get_chain_type_for_id(self, chain_id: int) -> ChainType:
        """Get chain type for a chain ID"""
        # Map chain IDs to types
        chain_map = {
            1: ChainType.ETHEREUM,
            137: ChainType.POLYGON,
            42161: ChainType.ARBITRUM,
            10: ChainType.OPTIMISM,
            43114: ChainType.AVALANCHE,
            56: ChainType.BSC
        }
        return chain_map.get(chain_id, ChainType.ETHEREUM)
        
    async def _get_native_token_usd_price(self, chain_type: ChainType) -> Optional[Decimal]:
        """Get USD price for native token"""
        # In production, integrate with price oracles
        # For now, return mock prices
        prices = {
            ChainType.ETHEREUM: Decimal("2000"),
            ChainType.POLYGON: Decimal("0.8"),
            ChainType.ARBITRUM: Decimal("2000"),  # ETH
            ChainType.OPTIMISM: Decimal("2000"),  # ETH
            ChainType.AVALANCHE: Decimal("25"),
            ChainType.BSC: Decimal("300")
        }
        return prices.get(chain_type)
        
    def _encode_multicall(self, transactions: List[Transaction]) -> bytes:
        """Encode multiple transactions for multicall"""
        # Simplified - in production, use proper multicall encoding
        return b"multicall_data"
        
    def _calculate_hourly_averages(self, history: List[GasMetricsSnapshot]) -> Dict[int, float]:
        """Calculate average gas price by hour"""
        hourly_prices = {}
        hourly_counts = {}
        
        for snapshot in history:
            hour = snapshot.timestamp.hour
            price = snapshot.gas_price
            
            if hour not in hourly_prices:
                hourly_prices[hour] = 0
                hourly_counts[hour] = 0
                
            hourly_prices[hour] += price
            hourly_counts[hour] += 1
            
        return {
            hour: hourly_prices[hour] / hourly_counts[hour]
            for hour in hourly_prices
        } 