"""Settlement engine for futures contracts."""

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional
import logging
import uuid

from app.config import Settings
from app.models.futures import (
    FuturesContract, FuturesPosition, SettlementRecord,
    SettlementType, ContractType
)
from app.cache.ignite_manager import FuturesCacheManager
from platformq_trading_common import publish_event, EventType


logger = logging.getLogger(__name__)


class SettlementEngine:
    """Manages settlement of expiring futures contracts."""
    
    def __init__(self, settings: Settings, cache_manager: FuturesCacheManager):
        self.settings = settings
        self.cache = cache_manager
        self._running = False
        self._settlement_tasks: Dict[str, asyncio.Task] = {}
        
    async def start(self):
        """Start the settlement engine."""
        self._running = True
        
        # Start monitoring for expiring contracts
        asyncio.create_task(self._monitor_expirations())
        
        logger.info("Settlement engine started")
        
    async def stop(self):
        """Stop the settlement engine."""
        self._running = False
        
        # Cancel all settlement tasks
        for task in self._settlement_tasks.values():
            task.cancel()
            
        # Wait for tasks to complete
        if self._settlement_tasks:
            await asyncio.gather(*self._settlement_tasks.values(), return_exceptions=True)
            
        logger.info("Settlement engine stopped")
        
    async def _monitor_expirations(self):
        """Monitor contracts for expiration."""
        while self._running:
            try:
                # Get all active contracts
                contracts = await self.cache.get_active_contracts()
                
                for contract in contracts:
                    if contract.contract_type == ContractType.PERPETUAL:
                        continue  # Skip perpetuals
                        
                    if contract.expiry_date:
                        # Check if contract is expiring soon
                        time_to_expiry = contract.expiry_date - datetime.utcnow()
                        
                        if time_to_expiry <= timedelta(hours=1):
                            # Start settlement process if not already started
                            if contract.symbol not in self._settlement_tasks:
                                task = asyncio.create_task(
                                    self._settle_contract(contract)
                                )
                                self._settlement_tasks[contract.symbol] = task
                                
                # Check every minute
                await asyncio.sleep(60)
                
            except Exception as e:
                logger.error(f"Error monitoring expirations: {e}")
                await asyncio.sleep(60)
                
    async def _settle_contract(self, contract: FuturesContract):
        """Settle an expiring futures contract."""
        try:
            # Wait until exact expiry time
            wait_time = (contract.expiry_date - datetime.utcnow()).total_seconds()
            if wait_time > 0:
                await asyncio.sleep(wait_time)
                
            logger.info(f"Starting settlement for {contract.symbol}")
            
            # Create settlement record
            settlement = SettlementRecord(
                settlement_id=str(uuid.uuid4()),
                symbol=contract.symbol,
                settlement_price=Decimal("0"),  # Will be set later
                settlement_type=contract.settlement_type,
                positions_settled=0,
                total_volume=Decimal("0"),
                timestamp=datetime.utcnow(),
                status="processing"
            )
            
            await self.cache.store_settlement_record(settlement)
            
            # Get settlement price
            settlement_price = await self._get_settlement_price(contract.symbol)
            settlement.settlement_price = settlement_price
            
            # Process based on settlement type
            if contract.settlement_type == SettlementType.CASH:
                await self._cash_settlement(contract, settlement, settlement_price)
            else:
                await self._physical_settlement(contract, settlement, settlement_price)
                
            # Mark contract as settled
            contract.is_active = False
            await self.cache.update_contract(contract)
            
            # Update settlement record
            settlement.status = "completed"
            await self.cache.update_settlement_record(settlement)
            
            # Publish settlement event
            await publish_event(
                EventType.CONTRACT_SETTLED,
                {
                    "symbol": contract.symbol,
                    "settlement_id": settlement.settlement_id,
                    "settlement_price": str(settlement_price),
                    "positions_settled": settlement.positions_settled,
                    "total_volume": str(settlement.total_volume),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            logger.info(
                f"Completed settlement for {contract.symbol}: "
                f"price={settlement_price}, positions={settlement.positions_settled}"
            )
            
        except Exception as e:
            logger.error(f"Error settling contract {contract.symbol}: {e}")
            
            # Update settlement record with error
            if 'settlement' in locals():
                settlement.status = "failed"
                settlement.details = {"error": str(e)}
                await self.cache.update_settlement_record(settlement)
                
        finally:
            # Remove from active tasks
            self._settlement_tasks.pop(contract.symbol, None)
            
    async def _cash_settlement(
        self,
        contract: FuturesContract,
        settlement: SettlementRecord,
        settlement_price: Decimal
    ):
        """Process cash settlement for positions."""
        # Get all open positions
        positions = await self.cache.get_all_positions(contract.symbol)
        
        settlements = []
        total_volume = Decimal("0")
        
        # Process in batches
        batch_size = self.settings.settlement_batch_size
        for i in range(0, len(positions), batch_size):
            batch = positions[i:i + batch_size]
            
            batch_settlements = await asyncio.gather(
                *[self._settle_position_cash(p, settlement_price) for p in batch],
                return_exceptions=True
            )
            
            for result in batch_settlements:
                if isinstance(result, dict):
                    settlements.append(result)
                    total_volume += result["volume"]
                    
        # Update settlement record
        settlement.positions_settled = len(settlements)
        settlement.total_volume = total_volume
        settlement.details = {
            "settlements": settlements[:100]  # Store first 100 for reference
        }
        
    async def _physical_settlement(
        self,
        contract: FuturesContract,
        settlement: SettlementRecord,
        settlement_price: Decimal
    ):
        """Process physical settlement for positions."""
        if not self.settings.physical_delivery_enabled:
            # Fall back to cash settlement
            await self._cash_settlement(contract, settlement, settlement_price)
            return
            
        # Get all open positions
        positions = await self.cache.get_all_positions(contract.symbol)
        
        settlements = []
        total_volume = Decimal("0")
        
        for position in positions:
            try:
                # Create delivery instruction
                delivery = await self._create_delivery_instruction(
                    position, contract, settlement_price
                )
                
                if delivery:
                    settlements.append(delivery)
                    total_volume += abs(position.size * contract.contract_size)
                    
                    # Close the position
                    await self.cache.close_position(position.position_id)
                    
            except Exception as e:
                logger.error(f"Error in physical settlement for {position.position_id}: {e}")
                
        # Update settlement record
        settlement.positions_settled = len(settlements)
        settlement.total_volume = total_volume
        settlement.details = {
            "delivery_instructions": settlements[:100]
        }
        
    async def _settle_position_cash(
        self,
        position: FuturesPosition,
        settlement_price: Decimal
    ) -> Dict:
        """Settle a position with cash."""
        try:
            # Calculate final P&L
            if position.side.value == "long":
                pnl = (settlement_price - position.entry_price) * position.size
            else:
                pnl = (position.entry_price - settlement_price) * position.size
                
            # Add to realized P&L
            total_pnl = position.realized_pnl + pnl + position.unrealized_pnl
            
            # Update user balance
            await self._update_user_balance(position.user_id, total_pnl)
            
            # Return margin
            await self._return_margin(position.user_id, position.margin_used)
            
            # Close position
            await self.cache.close_position(position.position_id)
            
            return {
                "position_id": position.position_id,
                "user_id": position.user_id,
                "side": position.side.value,
                "size": float(position.size),
                "entry_price": float(position.entry_price),
                "settlement_price": float(settlement_price),
                "pnl": float(total_pnl),
                "volume": float(position.size)
            }
            
        except Exception as e:
            logger.error(f"Error settling position {position.position_id}: {e}")
            raise
            
    async def _create_delivery_instruction(
        self,
        position: FuturesPosition,
        contract: FuturesContract,
        settlement_price: Decimal
    ) -> Optional[Dict]:
        """Create physical delivery instruction."""
        try:
            delivery_id = str(uuid.uuid4())
            
            # Calculate delivery amount
            delivery_amount = position.size * contract.contract_size
            
            instruction = {
                "delivery_id": delivery_id,
                "position_id": position.position_id,
                "user_id": position.user_id,
                "symbol": contract.symbol,
                "underlying_asset": contract.underlying_asset,
                "delivery_amount": float(delivery_amount),
                "settlement_price": float(settlement_price),
                "side": "deliver" if position.side.value == "short" else "receive",
                "status": "pending",
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Store delivery instruction
            await self.cache.store_delivery_instruction(instruction)
            
            # Publish delivery event
            await publish_event(
                EventType.PHYSICAL_DELIVERY,
                instruction
            )
            
            return instruction
            
        except Exception as e:
            logger.error(f"Error creating delivery instruction: {e}")
            return None
            
    async def _get_settlement_price(self, symbol: str) -> Decimal:
        """Get settlement price for a contract."""
        # In production, this would use TWAP or index price
        # For now, use last traded price
        price = await self.cache.get_latest_price(symbol)
        if price:
            return Decimal(str(price))
            
        # Fallback to mark price
        market_stats = await self.cache.get_market_stats(symbol)
        if market_stats:
            return market_stats.mark_price
            
        raise ValueError(f"No settlement price available for {symbol}")
        
    async def _update_user_balance(self, user_id: str, amount: Decimal):
        """Update user balance after settlement."""
        # In production, this would update the user's wallet
        logger.info(f"Settlement balance update for {user_id}: {amount}")
        
    async def _return_margin(self, user_id: str, margin: Decimal):
        """Return margin to user after position closure."""
        # In production, this would release locked margin
        logger.info(f"Returning margin to {user_id}: {margin}")
        
    async def get_settlement_history(
        self,
        symbol: Optional[str] = None,
        limit: int = 100
    ) -> List[SettlementRecord]:
        """Get settlement history."""
        return await self.cache.get_settlement_history(symbol, limit) 