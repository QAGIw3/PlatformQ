"""Ultra-fast copy trading executor using direct communication."""

from typing import Dict, Any, List, Optional
from decimal import Decimal
import asyncio
from datetime import datetime

from ...models import CopyTradingRelation, CopyMode
from pyignite import Client as IgniteClient

# Import shared communication layer
import sys
sys.path.append('/app/services/MarketServices/shared')
from direct_communication import DirectCommunicator, MessageType


class FastCopyExecutor:
    """High-performance copy trading executor with sub-millisecond latency."""
    
    def __init__(self, ignite_client: IgniteClient):
        self.ignite_client = ignite_client
        
        # Direct communicator for trading core
        self.communicator = DirectCommunicator(
            service_id="trading-platform",
            ignite_client=ignite_client
        )
        
        # Cache for copy relations (hot path optimization)
        self._relations_cache: Dict[str, List[CopyTradingRelation]] = {}
        self._cache_refresh_interval = 1.0  # 1 second
        
        # Pre-allocated buffers for batch operations
        self._order_buffer = []
        self._buffer_size = 100
        
    async def initialize(self):
        """Initialize fast executor."""
        
        # Register handlers
        await self.communicator.register_handler(
            MessageType.TRADE_EXECUTE,
            self._handle_leader_trade
        )
        
        # Start background tasks
        asyncio.create_task(self.communicator.process_incoming())
        asyncio.create_task(self._refresh_cache_loop())
        asyncio.create_task(self._batch_processor())
        
    async def _handle_leader_trade(self, data: Dict[str, Any], msg) -> None:
        """Handle leader trade execution with minimal latency."""
        
        trade = data.get("trade")
        if not trade:
            return
            
        leader_id = trade["taker_user_id"]
        
        # Fast path: check cache first
        followers = self._relations_cache.get(leader_id, [])
        
        if not followers:
            # Slow path: check Ignite if not in cache
            followers = await self._get_active_followers(leader_id)
            self._relations_cache[leader_id] = followers
        
        if followers:
            # Prepare follower orders
            follower_orders = await self._prepare_follower_orders(
                leader_trade=trade,
                followers=followers
            )
            
            if follower_orders:
                # Add to buffer for batch processing
                self._order_buffer.extend(follower_orders)
                
                # Process immediately if buffer is full
                if len(self._order_buffer) >= self._buffer_size:
                    await self._flush_order_buffer()
    
    async def _prepare_follower_orders(self,
                                     leader_trade: Dict[str, Any],
                                     followers: List[CopyTradingRelation]) -> List[Dict[str, Any]]:
        """Prepare follower orders with minimal overhead."""
        
        orders = []
        
        # Parallel preparation for all followers
        tasks = []
        for relation in followers:
            if relation.is_active:
                task = self._create_follower_order(leader_trade, relation)
                tasks.append(task)
        
        # Gather all orders
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filter successful orders
        for result in results:
            if isinstance(result, dict) and result:
                orders.append(result)
                
        return orders
    
    async def _create_follower_order(self,
                                   leader_trade: Dict[str, Any],
                                   relation: CopyTradingRelation) -> Optional[Dict[str, Any]]:
        """Create a single follower order."""
        
        try:
            # Calculate follower size based on copy mode
            follower_size = await self._calculate_follower_size(
                leader_size=Decimal(leader_trade["quantity"]),
                leader_price=Decimal(leader_trade["price"]),
                relation=relation
            )
            
            if follower_size <= 0:
                return None
            
            # Quick risk check
            if not await self._quick_risk_check(relation.follower_id, follower_size):
                return None
            
            # Create order
            return {
                "user_id": relation.follower_id,
                "market_id": leader_trade["market_id"],
                "product_type": leader_trade.get("product_type", "spot"),
                "type": "market",  # Always market orders for speed
                "side": "buy" if leader_trade["side"] == "buy" else "sell",
                "quantity": str(follower_size),
                "metadata": {
                    "copy_relation_id": relation.relation_id,
                    "leader_trade_id": leader_trade["trade_id"]
                }
            }
            
        except Exception as e:
            # Log error but don't fail the batch
            print(f"Error creating follower order: {e}")
            return None
    
    async def _calculate_follower_size(self,
                                     leader_size: Decimal,
                                     leader_price: Decimal,
                                     relation: CopyTradingRelation) -> Decimal:
        """Calculate follower position size."""
        
        if relation.copy_mode == CopyMode.FIXED_AMOUNT:
            # Fixed amount per trade
            return relation.allocation_amount / leader_price
            
        elif relation.copy_mode == CopyMode.PERCENTAGE:
            # Percentage of portfolio
            portfolio_value = await self._get_portfolio_value(relation.follower_id)
            if portfolio_value:
                max_trade_value = portfolio_value * Decimal(str(relation.allocation_percent / 100))
                return max_trade_value / leader_price
                
        else:  # PROPORTIONAL
            # Proportional to leader
            leader_portfolio = await self._get_portfolio_value(relation.leader_id)
            follower_portfolio = await self._get_portfolio_value(relation.follower_id)
            
            if leader_portfolio and follower_portfolio:
                ratio = follower_portfolio / leader_portfolio
                return leader_size * ratio * Decimal(str(relation.allocation_percent / 100))
        
        return Decimal("0")
    
    async def _quick_risk_check(self, user_id: str, size: Decimal) -> bool:
        """Ultra-fast risk check using direct communication."""
        
        # Direct risk check with trading core
        result = await self.communicator.send_direct(
            target_service="trading-core",
            msg_type=MessageType.RISK_CHECK,
            data={
                "user_id": user_id,
                "order_value": str(size),
                "market_id": "BTC-USDT"  # TODO: Get from context
            },
            wait_response=True
        )
        
        return result and result.get("approved", False)
    
    async def _batch_processor(self):
        """Process orders in batches for efficiency."""
        
        while True:
            try:
                # Wait for batch interval or buffer full
                await asyncio.sleep(0.01)  # 10ms batch window
                
                if self._order_buffer:
                    await self._flush_order_buffer()
                    
            except Exception as e:
                print(f"Batch processor error: {e}")
    
    async def _flush_order_buffer(self):
        """Flush order buffer to trading core."""
        
        if not self._order_buffer:
            return
            
        # Get orders and clear buffer
        orders = self._order_buffer[:self._buffer_size]
        self._order_buffer = self._order_buffer[self._buffer_size:]
        
        # Group by leader for batch processing
        leader_groups = {}
        for order in orders:
            leader_id = order["metadata"].get("leader_trade_id", "unknown")
            if leader_id not in leader_groups:
                leader_groups[leader_id] = []
            leader_groups[leader_id].append(order)
        
        # Send each group as a batch
        for leader_id, group_orders in leader_groups.items():
            await self.communicator.send_direct(
                target_service="trading-core",
                msg_type=MessageType.COPY_TRADE,
                data={
                    "leader_trade": {"user_id": leader_id, "order_id": leader_id},
                    "follower_orders": group_orders
                },
                wait_response=False  # Fire and forget for speed
            )
    
    async def _refresh_cache_loop(self):
        """Periodically refresh the relations cache."""
        
        while True:
            try:
                await asyncio.sleep(self._cache_refresh_interval)
                
                # Get all active copy relations
                cache = self.ignite_client.get_cache("copy_relations")
                all_relations = await cache.get_all()
                
                # Group by leader
                new_cache = {}
                for relation_id, relation_data in all_relations.items():
                    if relation_data.get("is_active"):
                        leader_id = relation_data["leader_id"]
                        if leader_id not in new_cache:
                            new_cache[leader_id] = []
                        
                        # Convert to relation object
                        relation = CopyTradingRelation(**relation_data)
                        new_cache[leader_id].append(relation)
                
                # Atomic cache update
                self._relations_cache = new_cache
                
            except Exception as e:
                print(f"Cache refresh error: {e}")
    
    async def _get_active_followers(self, leader_id: str) -> List[CopyTradingRelation]:
        """Get active followers from Ignite."""
        
        cache = self.ignite_client.get_cache("copy_relations")
        
        # Use SQL query for efficiency
        query = f"SELECT * FROM CopyTradingRelation WHERE leader_id = ? AND is_active = true"
        cursor = cache.query(query, [leader_id])
        
        relations = []
        for row in cursor:
            relations.append(CopyTradingRelation(**row))
            
        return relations
    
    async def _get_portfolio_value(self, user_id: str) -> Optional[Decimal]:
        """Get user portfolio value from cache."""
        
        cache = self.ignite_client.get_cache("user_state")
        user_state = await cache.get_async(user_id)
        
        if user_state:
            return Decimal(str(user_state.get("portfolio_value", 0)))
        
        return None 