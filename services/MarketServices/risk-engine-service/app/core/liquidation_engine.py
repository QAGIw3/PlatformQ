"""Liquidation engine for margin breaches and risk violations."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class LiquidationReason(Enum):
    """Reasons for liquidation."""
    MARGIN_BREACH = "margin_breach"
    RISK_LIMIT = "risk_limit"
    MANUAL = "manual"
    STOP_LOSS = "stop_loss"
    SYSTEM_RISK = "system_risk"


class LiquidationStatus(Enum):
    """Status of liquidation order."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class LiquidationEngine:
    """Handles automated and manual position liquidations."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.auto_liquidation_enabled = config.get("auto_liquidation_enabled", True)
        self.liquidation_batch_size = config.get("liquidation_batch_size", 10)
        self.liquidation_margin_ratio = Decimal(str(config.get("liquidation_margin_ratio", "1.1")))
        
    async def check_liquidation_conditions(
        self,
        position: Dict[str, Any],
        market_data: Dict[str, Any],
        margin_info: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Check if a position should be liquidated."""
        position_id = position.get("position_id")
        
        # Check margin ratio
        margin_ratio = margin_info.get("margin_ratio", Decimal("999"))
        if margin_ratio < self.liquidation_margin_ratio:
            return {
                "position_id": position_id,
                "reason": LiquidationReason.MARGIN_BREACH,
                "margin_ratio": margin_ratio,
                "required_ratio": self.liquidation_margin_ratio,
                "priority": 1  # High priority
            }
        
        # Check liquidation price
        mark_price = Decimal(str(market_data.get("price", "0")))
        liquidation_price = margin_info.get("liquidation_price")
        
        if liquidation_price:
            side = position.get("side", "long")
            if (side == "long" and mark_price <= liquidation_price) or \
               (side == "short" and mark_price >= liquidation_price):
                return {
                    "position_id": position_id,
                    "reason": LiquidationReason.MARGIN_BREACH,
                    "mark_price": mark_price,
                    "liquidation_price": liquidation_price,
                    "priority": 1
                }
        
        return None
    
    async def create_liquidation_order(
        self,
        position: Dict[str, Any],
        reason: LiquidationReason,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a liquidation order for a position."""
        liquidation_order = {
            "order_id": f"liq_{position.get('position_id')}_{datetime.utcnow().timestamp()}",
            "position_id": position.get("position_id"),
            "user_id": position.get("user_id"),
            "market_id": position.get("market_id"),
            "side": "sell" if position.get("side") == "long" else "buy",
            "quantity": abs(Decimal(str(position.get("quantity", "0")))),
            "order_type": "market",
            "reason": reason.value,
            "status": LiquidationStatus.PENDING.value,
            "created_at": datetime.utcnow(),
            "metadata": metadata or {}
        }
        
        return liquidation_order
    
    async def execute_liquidation(
        self,
        liquidation_order: Dict[str, Any],
        market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute a liquidation order."""
        order_id = liquidation_order.get("order_id")
        
        # Update status
        liquidation_order["status"] = LiquidationStatus.IN_PROGRESS.value
        liquidation_order["execution_started_at"] = datetime.utcnow()
        
        # Calculate execution details
        mark_price = Decimal(str(market_data.get("price", "0")))
        quantity = liquidation_order.get("quantity")
        
        # Apply liquidation penalty/slippage
        slippage = Decimal("0.002")  # 0.2% slippage
        side = liquidation_order.get("side")
        
        if side == "sell":
            execution_price = mark_price * (Decimal("1") - slippage)
        else:
            execution_price = mark_price * (Decimal("1") + slippage)
        
        # Calculate proceeds
        proceeds = quantity * execution_price
        
        # Update order with execution details
        liquidation_order.update({
            "status": LiquidationStatus.COMPLETED.value,
            "execution_price": execution_price,
            "execution_quantity": quantity,
            "proceeds": proceeds,
            "slippage": slippage,
            "executed_at": datetime.utcnow()
        })
        
        return liquidation_order
    
    async def batch_liquidation(
        self,
        positions_to_liquidate: List[Dict[str, Any]],
        market_data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute batch liquidation of multiple positions."""
        results = {
            "total_positions": len(positions_to_liquidate),
            "successful": 0,
            "failed": 0,
            "orders": []
        }
        
        # Sort by priority
        sorted_positions = sorted(
            positions_to_liquidate,
            key=lambda x: x.get("priority", 999)
        )
        
        # Process in batches
        for i in range(0, len(sorted_positions), self.liquidation_batch_size):
            batch = sorted_positions[i:i + self.liquidation_batch_size]
            
            for position_info in batch:
                position = position_info.get("position")
                reason = position_info.get("reason", LiquidationReason.MARGIN_BREACH)
                
                try:
                    # Create liquidation order
                    order = await self.create_liquidation_order(
                        position,
                        reason,
                        position_info.get("metadata")
                    )
                    
                    # Execute liquidation
                    market_id = position.get("market_id")
                    if market_id in market_data:
                        executed_order = await self.execute_liquidation(
                            order,
                            market_data[market_id]
                        )
                        results["orders"].append(executed_order)
                        results["successful"] += 1
                    else:
                        order["status"] = LiquidationStatus.FAILED.value
                        order["error"] = "Market data not available"
                        results["orders"].append(order)
                        results["failed"] += 1
                        
                except Exception as e:
                    logger.error(f"Liquidation failed for position {position.get('position_id')}: {e}")
                    results["failed"] += 1
        
        return results
    
    async def cancel_liquidation(
        self,
        order_id: str,
        reason: Optional[str] = None
    ) -> Dict[str, Any]:
        """Cancel a pending liquidation order."""
        # In a real implementation, this would update the order in storage
        return {
            "order_id": order_id,
            "status": LiquidationStatus.CANCELLED.value,
            "cancelled_at": datetime.utcnow(),
            "cancellation_reason": reason
        }
    
    def calculate_liquidation_impact(
        self,
        position: Dict[str, Any],
        market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate the impact of liquidating a position."""
        quantity = abs(Decimal(str(position.get("quantity", "0"))))
        mark_price = Decimal(str(market_data.get("price", "0")))
        entry_price = Decimal(str(position.get("entry_price", mark_price)))
        side = position.get("side", "long")
        
        # Calculate P&L
        if side == "long":
            pnl = (mark_price - entry_price) * quantity
        else:
            pnl = (entry_price - mark_price) * quantity
        
        # Apply slippage
        slippage = Decimal("0.002")
        if side == "long":
            execution_price = mark_price * (Decimal("1") - slippage)
        else:
            execution_price = mark_price * (Decimal("1") + slippage)
        
        # Calculate actual proceeds
        proceeds = quantity * execution_price
        slippage_cost = quantity * mark_price * slippage
        
        return {
            "position_id": position.get("position_id"),
            "mark_price": mark_price,
            "execution_price": execution_price,
            "quantity": quantity,
            "proceeds": proceeds,
            "pnl": pnl,
            "slippage_cost": slippage_cost,
            "net_pnl": pnl - slippage_cost
        }
