"""Copy trading executor for replicating leader trades to followers."""

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Optional, Set
import logging
import json

import httpx
from platformq_trading_common import publish_event, EventType

from ..models import CopyTradingRelation, CopyMode


logger = logging.getLogger(__name__)


class CopyTradingExecutor:
    """Executes copy trading operations from leaders to followers."""
    
    def __init__(self, settings):
        self.settings = settings
        self._active_relations: Dict[str, CopyTradingRelation] = {}
        self._leader_followers: Dict[str, Set[str]] = {}  # leader_id -> set of follower_ids
        self._follower_allocations: Dict[str, Dict[str, Decimal]] = {}  # follower_id -> asset -> amount
        
        # HTTP client for external services
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
    async def start(self):
        """Start the copy trading executor."""
        logger.info("Copy trading executor started")
        
    async def stop(self):
        """Stop the copy trading executor."""
        await self.http_client.aclose()
        logger.info("Copy trading executor stopped")
        
    async def register_copy_relation(self, relation: CopyTradingRelation):
        """Register a new copy trading relationship."""
        if not relation.is_active:
            logger.warning(f"Attempting to register inactive relation {relation.relation_id}")
            return
            
        # Store relation
        self._active_relations[relation.relation_id] = relation
        
        # Update leader-follower mapping
        if relation.leader_id not in self._leader_followers:
            self._leader_followers[relation.leader_id] = set()
        self._leader_followers[relation.leader_id].add(relation.follower_id)
        
        logger.info(
            f"Registered copy relation: {relation.follower_id} -> {relation.leader_id} "
            f"({relation.copy_mode.value})"
        )
        
    async def unregister_copy_relation(self, relation_id: str):
        """Unregister a copy trading relationship."""
        relation = self._active_relations.get(relation_id)
        if not relation:
            return
            
        # Remove from mappings
        del self._active_relations[relation_id]
        self._leader_followers[relation.leader_id].discard(relation.follower_id)
        
        logger.info(f"Unregistered copy relation {relation_id}")
        
    async def execute_leader_trade(
        self,
        leader_id: str,
        trade_data: Dict
    ) -> List[Dict]:
        """Execute a leader's trade for all active followers."""
        followers = self._leader_followers.get(leader_id, set())
        if not followers:
            return []
            
        logger.info(
            f"Executing trade for {len(followers)} followers of leader {leader_id}: "
            f"{trade_data['side']} {trade_data['quantity']} {trade_data['symbol']}"
        )
        
        copy_results = []
        
        # Execute for each follower
        for follower_id in followers:
            # Find the relation
            relation = None
            for rel in self._active_relations.values():
                if rel.leader_id == leader_id and rel.follower_id == follower_id:
                    relation = rel
                    break
                    
            if not relation or not relation.is_active:
                continue
                
            try:
                result = await self._execute_copy_trade(
                    relation, follower_id, trade_data
                )
                copy_results.append(result)
                
            except Exception as e:
                logger.error(f"Failed to copy trade for {follower_id}: {e}")
                
                # Publish failure event
                await publish_event(
                    EventType.COPY_TRADE_FAILED,
                    {
                        "relation_id": relation.relation_id,
                        "leader_id": leader_id,
                        "follower_id": follower_id,
                        "error": str(e),
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
        return copy_results
        
    async def _execute_copy_trade(
        self,
        relation: CopyTradingRelation,
        follower_id: str,
        leader_trade: Dict
    ) -> Dict:
        """Execute a single copy trade for a follower."""
        # Calculate copy trade size based on mode
        copy_size = await self._calculate_copy_size(
            relation, follower_id, leader_trade
        )
        
        if copy_size <= 0:
            return {
                "status": "skipped",
                "reason": "Calculated size is zero or negative"
            }
            
        # Check risk limits
        if not await self._check_risk_limits(relation, follower_id, leader_trade, copy_size):
            return {
                "status": "skipped",
                "reason": "Risk limits exceeded"
            }
            
        # Prepare copy trade request
        copy_trade = {
            "symbol": leader_trade["symbol"],
            "side": leader_trade["side"],
            "quantity": float(copy_size),
            "order_type": "market",  # Always use market orders for copy trades
            "metadata": {
                "is_copy_trade": True,
                "leader_id": relation.leader_id,
                "relation_id": relation.relation_id,
                "leader_order_id": leader_trade.get("order_id")
            }
        }
        
        # Submit to order matching service
        try:
            response = await self.http_client.post(
                f"{self.settings.order_matching_service_url}/api/v1/orders",
                json=copy_trade,
                headers={"X-User-Id": follower_id}
            )
            response.raise_for_status()
            
            order_result = response.json()
            
            # Update relation stats
            relation.total_copied_trades += 1
            
            # Publish success event
            await publish_event(
                EventType.COPY_TRADE_EXECUTED,
                {
                    "relation_id": relation.relation_id,
                    "leader_id": relation.leader_id,
                    "follower_id": follower_id,
                    "order_id": order_result["order_id"],
                    "symbol": copy_trade["symbol"],
                    "side": copy_trade["side"],
                    "quantity": copy_trade["quantity"],
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            return {
                "status": "success",
                "order_id": order_result["order_id"],
                "quantity": copy_size
            }
            
        except Exception as e:
            logger.error(f"Failed to submit copy trade: {e}")
            raise
            
    async def _calculate_copy_size(
        self,
        relation: CopyTradingRelation,
        follower_id: str,
        leader_trade: Dict
    ) -> Decimal:
        """Calculate the size of the copy trade based on copy mode."""
        if relation.copy_mode == CopyMode.FIXED_AMOUNT:
            # Fixed amount per trade
            return relation.allocation_amount or Decimal("0")
            
        elif relation.copy_mode == CopyMode.PERCENTAGE:
            # Percentage of follower's portfolio
            portfolio_value = await self._get_portfolio_value(follower_id)
            if not portfolio_value:
                return Decimal("0")
                
            allocation_percent = relation.allocation_percent or 0
            trade_value = portfolio_value * Decimal(str(allocation_percent / 100))
            
            # Convert to quantity based on current price
            price = await self._get_current_price(leader_trade["symbol"])
            if not price:
                return Decimal("0")
                
            return trade_value / price
            
        elif relation.copy_mode == CopyMode.PROPORTIONAL:
            # Proportional to leader's position
            leader_portfolio = await self._get_portfolio_value(relation.leader_id)
            follower_portfolio = await self._get_portfolio_value(follower_id)
            
            if not leader_portfolio or not follower_portfolio:
                return Decimal("0")
                
            # Calculate proportion
            leader_trade_value = Decimal(str(leader_trade["quantity"])) * leader_trade.get("price", Decimal("1"))
            leader_trade_percent = leader_trade_value / leader_portfolio
            
            # Apply proportion to follower's portfolio
            follower_trade_value = follower_portfolio * leader_trade_percent
            
            # Apply allocation limit
            if relation.allocation_percent:
                max_value = follower_portfolio * Decimal(str(relation.allocation_percent / 100))
                follower_trade_value = min(follower_trade_value, max_value)
                
            # Convert to quantity
            price = await self._get_current_price(leader_trade["symbol"])
            if not price:
                return Decimal("0")
                
            return follower_trade_value / price
            
        return Decimal("0")
        
    async def _check_risk_limits(
        self,
        relation: CopyTradingRelation,
        follower_id: str,
        trade: Dict,
        size: Decimal
    ) -> bool:
        """Check if the copy trade meets risk limits."""
        # Check max position size
        if relation.max_position_size and size > relation.max_position_size:
            logger.warning(
                f"Copy trade size {size} exceeds max position size "
                f"{relation.max_position_size} for {follower_id}"
            )
            return False
            
        # Check daily trade limit
        if relation.max_daily_trades:
            daily_trades = await self._get_daily_trade_count(follower_id)
            if daily_trades >= relation.max_daily_trades:
                logger.warning(
                    f"Daily trade limit {relation.max_daily_trades} "
                    f"reached for {follower_id}"
                )
                return False
                
        # Check drawdown limit
        if relation.max_drawdown_percent:
            current_drawdown = await self._get_current_drawdown(follower_id)
            if current_drawdown > relation.max_drawdown_percent:
                logger.warning(
                    f"Current drawdown {current_drawdown}% exceeds limit "
                    f"{relation.max_drawdown_percent}% for {follower_id}"
                )
                return False
                
        return True
        
    async def _get_portfolio_value(self, user_id: str) -> Optional[Decimal]:
        """Get user's portfolio value from risk service."""
        try:
            response = await self.http_client.get(
                f"{self.settings.risk_service_url}/api/v1/portfolio/{user_id}/value"
            )
            response.raise_for_status()
            return Decimal(str(response.json()["total_value"]))
        except Exception as e:
            logger.error(f"Failed to get portfolio value for {user_id}: {e}")
            return None
            
    async def _get_current_price(self, symbol: str) -> Optional[Decimal]:
        """Get current price for a symbol."""
        try:
            response = await self.http_client.get(
                f"{self.settings.order_matching_service_url}/api/v1/market/{symbol}/price"
            )
            response.raise_for_status()
            return Decimal(str(response.json()["price"]))
        except Exception as e:
            logger.error(f"Failed to get price for {symbol}: {e}")
            return None
            
    async def _get_daily_trade_count(self, user_id: str) -> int:
        """Get number of trades executed today by user."""
        # In production, query from database
        # For now, return mock value
        return 0
        
    async def _get_current_drawdown(self, user_id: str) -> float:
        """Get current drawdown percentage for user."""
        # In production, calculate from portfolio history
        # For now, return mock value
        return 0.0
        
    async def handle_position_closed(
        self,
        leader_id: str,
        position_data: Dict
    ):
        """Handle when a leader closes a position."""
        followers = self._leader_followers.get(leader_id, set())
        if not followers:
            return
            
        logger.info(
            f"Leader {leader_id} closed position in {position_data['symbol']}, "
            f"notifying {len(followers)} followers"
        )
        
        # For each follower, check if they have a corresponding position
        for follower_id in followers:
            # In production, check follower's positions and close if needed
            # This ensures followers don't hold positions after leader exits
            pass 