"""
Batch Optimization Strategy
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import asyncio

from pyignite import AsyncClient as IgniteClient

from ..config import Settings
from ..models.optimization import (
    OptimizationRequest, GasRecommendation, OptimizationStrategy
)

logger = logging.getLogger(__name__)


class BatchStrategy:
    """Handles batch transaction optimization"""
    
    def __init__(self, settings: Settings, ignite_client: IgniteClient):
        self.settings = settings
        self.ignite = ignite_client
        self._batch_queue: Dict[str, List[OptimizationRequest]] = {}
        self._batch_tasks: Dict[str, asyncio.Task] = {}
        
    async def initialize(self):
        """Initialize batch strategy"""
        logger.info("Initializing Batch Strategy")
        # Initialize batch cache
        self._batch_cache = await self.ignite.get_or_create_cache("batch_transactions")
        
    async def shutdown(self):
        """Shutdown batch strategy"""
        # Cancel all batch tasks
        for task in self._batch_tasks.values():
            task.cancel()
            
    async def evaluate(
        self,
        request: OptimizationRequest,
        gas_prices: Dict[str, Any]
    ) -> Optional[GasRecommendation]:
        """Evaluate batch optimization for a transaction"""
        # Check if batching would save gas
        individual_gas = request.estimated_gas or 100000
        batch_overhead = 50000  # Overhead for batch execution
        
        # Need at least 2 transactions to make batching worthwhile
        queue_key = f"{request.chain}:{request.to_address}"
        current_queue = self._batch_queue.get(queue_key, [])
        
        if len(current_queue) < 1:
            # First transaction in potential batch
            self._batch_queue[queue_key] = [request]
            
            # Start batch timer
            if queue_key not in self._batch_tasks:
                task = asyncio.create_task(
                    self._process_batch(queue_key, gas_prices)
                )
                self._batch_tasks[queue_key] = task
                
            return None  # No immediate recommendation
            
        # Calculate potential savings
        total_individual_gas = (len(current_queue) + 1) * individual_gas
        batch_gas = batch_overhead + (len(current_queue) + 1) * (individual_gas * 0.7)
        savings = total_individual_gas - batch_gas
        
        if savings / total_individual_gas < self.settings.BATCH_GAS_SAVINGS_THRESHOLD:
            return None
            
        # Add to queue
        self._batch_queue[queue_key].append(request)
        
        gas_price = gas_prices.get('standard', '20000000000')
        estimated_cost = int(batch_gas / (len(current_queue) + 1)) * int(gas_price)
        estimated_savings = int(individual_gas * int(gas_price)) - estimated_cost
        
        return GasRecommendation(
            strategy=OptimizationStrategy.BATCH,
            gas_price=gas_price,
            estimated_cost=str(estimated_cost),
            estimated_savings=str(estimated_savings),
            savings_percentage=float(estimated_savings) / (individual_gas * int(gas_price)),
            expected_confirmation_time=300,  # 5 minutes for batch collection
            confidence_score=0.85,
            reasoning=f"Batch with {len(current_queue)} other transactions for {savings/total_individual_gas:.1%} savings",
            recommended_time=datetime.utcnow() + timedelta(seconds=self.settings.BATCH_TIMEOUT)
        )
        
    async def _process_batch(self, queue_key: str, gas_prices: Dict[str, Any]):
        """Process a batch of transactions"""
        try:
            await asyncio.sleep(self.settings.BATCH_TIMEOUT)
            
            queue = self._batch_queue.get(queue_key, [])
            if len(queue) < 2:
                # Not enough transactions for batching
                self._batch_queue.pop(queue_key, None)
                return
                
            # Create batch transaction
            batch_id = f"batch_{datetime.utcnow().timestamp()}"
            
            # Store batch info
            await self._batch_cache.put(batch_id, {
                "transactions": [r.dict() for r in queue],
                "created_at": datetime.utcnow().isoformat(),
                "status": "pending"
            })
            
            # Clear queue
            self._batch_queue.pop(queue_key, None)
            
            logger.info(f"Created batch {batch_id} with {len(queue)} transactions")
            
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
        finally:
            self._batch_tasks.pop(queue_key, None) 