"""
Grid Trading Strategy for Compute Markets via Trading Core

Implements automated grid trading with dynamic grid adjustment, risk management,
and integration with the trading-core-service.
"""

import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import deque

from app.integrations.trading_core_integration import TradingCoreIntegration
from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient

logger = logging.getLogger(__name__)


class GridType(Enum):
    ARITHMETIC = "arithmetic"  # Fixed price intervals
    GEOMETRIC = "geometric"    # Percentage-based intervals
    DYNAMIC = "dynamic"       # Adjusts based on volatility
    FIBONACCI = "fibonacci"   # Fibonacci-based levels


class GridState(Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    STOPPED = "stopped"
    LIQUIDATING = "liquidating"


@dataclass
class GridLevel:
    """Represents a single level in the grid"""
    price: Decimal
    buy_order_id: Optional[str] = None
    sell_order_id: Optional[str] = None
    filled_buy_quantity: Decimal = Decimal("0")
    filled_sell_quantity: Decimal = Decimal("0")
    last_filled_time: Optional[datetime] = None
    profit_taken: Decimal = Decimal("0")


@dataclass
class GridConfig:
    """Configuration for grid trading strategy"""
    grid_type: GridType
    lower_price: Decimal
    upper_price: Decimal
    grid_levels: int
    order_size: Decimal
    resource_type: str
    
    # Risk management
    max_position: Decimal
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    max_drawdown: Decimal = Decimal("0.2")  # 20%
    
    # Grid adjustment
    adjust_on_volatility: bool = True
    volatility_threshold: Decimal = Decimal("0.1")  # 10%
    rebalance_interval: int = 3600  # seconds
    
    # Order management
    use_limit_orders: bool = True
    post_only: bool = True  # Maker only orders
    time_in_force: str = "GTC"  # Good Till Cancelled
    
    # Profit taking
    compound_profits: bool = True
    profit_target_per_grid: Decimal = Decimal("0.01")  # 1%
    
    # Advanced features
    use_trailing_grid: bool = False
    trailing_distance: Decimal = Decimal("0.05")  # 5%
    use_martingale: bool = False
    martingale_multiplier: Decimal = Decimal("1.5")


@dataclass
class GridStatistics:
    """Performance statistics for the grid"""
    total_trades: int = 0
    winning_trades: int = 0
    total_profit: Decimal = Decimal("0")
    total_volume: Decimal = Decimal("0")
    current_position: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    max_drawdown: Decimal = Decimal("0")
    sharpe_ratio: Decimal = Decimal("0")
    win_rate: Decimal = Decimal("0")
    average_profit_per_trade: Decimal = Decimal("0")
    grid_efficiency: Decimal = Decimal("0")  # Filled levels / total levels
    daily_returns: deque = field(default_factory=lambda: deque(maxlen=30))


class GridTradingStrategy:
    """
    Automated grid trading strategy for compute markets via trading-core
    """
    
    def __init__(self,
                 ignite: IgniteCache,
                 pulsar: PulsarEventPublisher,
                 oracle: OracleAggregatorClient):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        
        # Trading core integration
        self.trading_core = TradingCoreIntegration()
        
        self.grids: Dict[str, Dict[str, Any]] = {}  # grid_id -> grid data
        self.grid_levels: Dict[str, List[GridLevel]] = {}  # grid_id -> levels
        self.grid_stats: Dict[str, GridStatistics] = {}  # grid_id -> stats
        
        # Market IDs for different resource types
        self.market_ids: Dict[str, str] = {}  # resource_type -> market_id
        
        self._monitoring_task: Optional[asyncio.Task] = None
        self._rebalancing_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start grid trading strategy"""
        logger.info("Starting grid trading strategy with trading-core integration")
        
        # Initialize trading core
        await self.trading_core.initialize()
        
        self._monitoring_task = asyncio.create_task(self._monitor_grids())
        self._rebalancing_task = asyncio.create_task(self._rebalance_grids())
        
    async def stop(self):
        """Stop grid trading strategy"""
        logger.info("Stopping grid trading strategy")
        if self._monitoring_task:
            self._monitoring_task.cancel()
        if self._rebalancing_task:
            self._rebalancing_task.cancel()
            
        # Liquidate all grids
        for grid_id in list(self.grids.keys()):
            await self.stop_grid(grid_id, liquidate=True)
            
    async def create_grid(self,
                         config: GridConfig,
                         user_id: str) -> str:
        """Create a new grid trading strategy"""
        grid_id = f"GRID_{user_id}_{datetime.utcnow().timestamp()}"
        
        # Ensure market is registered
        market_id = await self._ensure_market_registered(config.resource_type)
        
        # Calculate grid levels
        levels = self._calculate_grid_levels(config)
        
        # Initialize grid
        self.grids[grid_id] = {
            "config": config,
            "user_id": user_id,
            "state": GridState.ACTIVE,
            "created_at": datetime.utcnow(),
            "last_rebalance": datetime.utcnow(),
            "initial_capital": Decimal("0"),
            "current_capital": Decimal("0"),
            "market_id": market_id
        }
        
        self.grid_levels[grid_id] = levels
        self.grid_stats[grid_id] = GridStatistics()
        
        # Place initial orders
        await self._place_grid_orders(grid_id)
        
        # Publish event
        await self.pulsar.publish('grid.created', {
            'grid_id': grid_id,
            'user_id': user_id,
            'market_id': market_id,
            'config': config.__dict__
        })
        
        logger.info(f"Created grid {grid_id} with {len(levels)} levels")
        
        return grid_id
        
    async def _ensure_market_registered(self, resource_type: str) -> str:
        """Ensure market is registered with trading-core"""
        market_id = f"COMPUTE_SPOT_{resource_type.upper()}_GRID"
        
        if resource_type not in self.market_ids:
            # Register compute market
            result = await self.trading_core.create_compute_market(
                resource_type=resource_type,
                market_type="spot",
                specifications={
                    "grid_trading": True,
                    "allow_limit_orders": True,
                    "maker_fee": "-0.0001",  # Negative fee (rebate)
                    "taker_fee": "0.0005"
                }
            )
            
            if result["success"]:
                self.market_ids[resource_type] = market_id
                logger.info(f"Registered market {market_id} for {resource_type}")
            else:
                raise RuntimeError(f"Failed to register market: {result}")
                
        return market_id
        
    async def stop_grid(self, grid_id: str, liquidate: bool = False) -> Dict[str, Any]:
        """Stop a grid and optionally liquidate positions"""
        if grid_id not in self.grids:
            raise ValueError(f"Grid {grid_id} not found")
            
        grid = self.grids[grid_id]
        grid["state"] = GridState.STOPPED if not liquidate else GridState.LIQUIDATING
        
        # Cancel all orders via trading-core
        levels = self.grid_levels[grid_id]
        cancel_tasks = []
        
        for level in levels:
            if level.buy_order_id:
                cancel_tasks.append(
                    self.trading_core.cancel_order(
                        order_id=level.buy_order_id,
                        user_id=grid["user_id"]
                    )
                )
            if level.sell_order_id:
                cancel_tasks.append(
                    self.trading_core.cancel_order(
                        order_id=level.sell_order_id,
                        user_id=grid["user_id"]
                    )
                )
                
        # Wait for all cancellations
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
                
        # Liquidate position if requested
        final_position = Decimal("0")
        liquidation_proceeds = Decimal("0")
        
        if liquidate:
            stats = self.grid_stats[grid_id]
            if stats.current_position != 0:
                # Submit market order to liquidate
                result = await self.trading_core.submit_compute_order(
                    user_id=grid["user_id"],
                    resource_type=grid["config"].resource_type,
                    market_type="spot",
                    quantity=str(abs(stats.current_position)),
                    specifications={
                        "side": "sell" if stats.current_position > 0 else "buy",
                        "order_type": "market",
                        "grid_liquidation": grid_id
                    }
                )
                
                if result.get("success"):
                    liquidation_proceeds = Decimal(result.get("executed_value", "0"))
                    final_position = Decimal("0")
                else:
                    logger.error(f"Failed to liquidate grid {grid_id}: {result}")
                    
        # Calculate final performance
        stats = self.grid_stats[grid_id]
        total_return = (stats.total_profit + liquidation_proceeds) / grid["initial_capital"] if grid["initial_capital"] > 0 else Decimal("0")
        
        result = {
            "grid_id": grid_id,
            "final_position": stats.current_position,
            "total_profit": stats.total_profit,
            "liquidation_proceeds": liquidation_proceeds,
            "total_return": total_return,
            "total_trades": stats.total_trades,
            "win_rate": stats.win_rate,
            "max_drawdown": stats.max_drawdown
        }
        
        # Clean up
        del self.grids[grid_id]
        del self.grid_levels[grid_id]
        del self.grid_stats[grid_id]
        
        # Publish event
        await self.pulsar.publish('grid.stopped', result)
        
        return result
        
    async def adjust_grid(self,
                         grid_id: str,
                         new_config: Optional[GridConfig] = None) -> None:
        """Adjust grid parameters or recalculate based on market conditions"""
        if grid_id not in self.grids:
            raise ValueError(f"Grid {grid_id} not found")
            
        grid = self.grids[grid_id]
        old_config = grid["config"]
        
        if new_config:
            grid["config"] = new_config
        else:
            # Auto-adjust based on volatility
            new_config = await self._calculate_dynamic_grid_params(grid_id)
            if new_config:
                grid["config"] = new_config
                
        # Cancel existing orders
        await self._cancel_grid_orders(grid_id)
        
        # Recalculate levels
        self.grid_levels[grid_id] = self._calculate_grid_levels(grid["config"])
        
        # Place new orders
        await self._place_grid_orders(grid_id)
        
        grid["last_rebalance"] = datetime.utcnow()
        
        logger.info(f"Adjusted grid {grid_id}")
        
    async def get_grid_status(self, grid_id: str) -> Dict[str, Any]:
        """Get current status and statistics for a grid"""
        if grid_id not in self.grids:
            raise ValueError(f"Grid {grid_id} not found")
            
        grid = self.grids[grid_id]
        stats = self.grid_stats[grid_id]
        levels = self.grid_levels[grid_id]
        
        # Count active orders
        active_buy_orders = sum(1 for l in levels if l.buy_order_id)
        active_sell_orders = sum(1 for l in levels if l.sell_order_id)
        
        return {
            "grid_id": grid_id,
            "state": grid["state"].value,
            "created_at": grid["created_at"].isoformat(),
            "config": grid["config"].__dict__,
            "statistics": {
                "total_trades": stats.total_trades,
                "winning_trades": stats.winning_trades,
                "win_rate": str(stats.win_rate),
                "total_profit": str(stats.total_profit),
                "unrealized_pnl": str(stats.unrealized_pnl),
                "current_position": str(stats.current_position),
                "grid_efficiency": str(stats.grid_efficiency),
                "average_profit_per_trade": str(stats.average_profit_per_trade)
            },
            "levels": {
                "total": len(levels),
                "active_buy_orders": active_buy_orders,
                "active_sell_orders": active_sell_orders,
                "filled_levels": sum(1 for l in levels if l.filled_buy_quantity > 0)
            }
        }
        
    # Internal methods
    
    def _calculate_grid_levels(self, config: GridConfig) -> List[GridLevel]:
        """Calculate grid price levels based on configuration"""
        levels = []
        
        if config.grid_type == GridType.ARITHMETIC:
            # Fixed price intervals
            price_step = (config.upper_price - config.lower_price) / (config.grid_levels - 1)
            for i in range(config.grid_levels):
                price = config.lower_price + (price_step * i)
                levels.append(GridLevel(price=price))
                
        elif config.grid_type == GridType.GEOMETRIC:
            # Percentage-based intervals
            ratio = (config.upper_price / config.lower_price) ** (1 / (config.grid_levels - 1))
            for i in range(config.grid_levels):
                price = config.lower_price * (ratio ** i)
                levels.append(GridLevel(price=price))
                
        elif config.grid_type == GridType.FIBONACCI:
            # Fibonacci-based levels
            fib_ratios = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
            price_range = config.upper_price - config.lower_price
            
            for ratio in fib_ratios[:config.grid_levels]:
                price = config.lower_price + (price_range * Decimal(str(ratio)))
                levels.append(GridLevel(price=price))
                
        elif config.grid_type == GridType.DYNAMIC:
            # Will be adjusted based on volatility
            # Start with arithmetic for initial placement
            levels = self._calculate_grid_levels(
                GridConfig(
                    grid_type=GridType.ARITHMETIC,
                    lower_price=config.lower_price,
                    upper_price=config.upper_price,
                    grid_levels=config.grid_levels,
                    order_size=config.order_size,
                    resource_type=config.resource_type,
                    max_position=config.max_position
                )
            )
            
        return levels
        
    async def _place_grid_orders(self, grid_id: str):
        """Place buy and sell orders for all grid levels"""
        grid = self.grids[grid_id]
        config = grid["config"]
        levels = self.grid_levels[grid_id]
        
        # Get current market price
        orderbook = await self.trading_core.get_orderbook(grid["market_id"], depth=1)
        if not orderbook:
            logger.error(f"Failed to get orderbook for grid {grid_id}")
            return
            
        mid_price = Decimal("0")
        if orderbook.get("bids") and orderbook.get("asks"):
            best_bid = Decimal(orderbook["bids"][0]["price"])
            best_ask = Decimal(orderbook["asks"][0]["price"])
            mid_price = (best_bid + best_ask) / 2
            
        # Place orders at each level
        order_tasks = []
        
        for level in levels:
            # Skip if orders already exist
            if level.buy_order_id or level.sell_order_id:
                continue
                
            # Place buy order if price is below current market
            if level.price < mid_price:
                order_tasks.append(
                    self._place_grid_order(
                        grid_id, level, "buy", config.order_size
                    )
                )
                
            # Place sell order if price is above current market
            elif level.price > mid_price:
                # Only if we have position to sell
                stats = self.grid_stats[grid_id]
                if stats.current_position >= config.order_size:
                    order_tasks.append(
                        self._place_grid_order(
                            grid_id, level, "sell", config.order_size
                        )
                    )
                    
        # Execute all order placements
        if order_tasks:
            results = await asyncio.gather(*order_tasks, return_exceptions=True)
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Failed to place grid order: {result}")
                    
    async def _place_grid_order(self,
                               grid_id: str,
                               level: GridLevel,
                               side: str,
                               size: Decimal) -> bool:
        """Place a single grid order"""
        grid = self.grids[grid_id]
        config = grid["config"]
        
        # Submit order via trading-core
        result = await self.trading_core.submit_compute_order(
            user_id=grid["user_id"],
            resource_type=config.resource_type,
            market_type="spot",
            quantity=str(size),
            specifications={
                "side": side,
                "order_type": "limit" if config.use_limit_orders else "market",
                "price": str(level.price) if config.use_limit_orders else None,
                "post_only": config.post_only,
                "time_in_force": config.time_in_force,
                "grid_id": grid_id,
                "grid_level": str(level.price)
            }
        )
        
        if result.get("success"):
            order_id = result.get("order_id")
            if side == "buy":
                level.buy_order_id = order_id
            else:
                level.sell_order_id = order_id
                
            logger.debug(f"Placed {side} order {order_id} at {level.price}")
            return True
        else:
            logger.error(f"Failed to place {side} order at {level.price}: {result}")
            return False
            
    async def _monitor_grids(self):
        """Monitor grid orders and handle fills"""
        while True:
            try:
                for grid_id, grid in self.grids.items():
                    if grid["state"] != GridState.ACTIVE:
                        continue
                        
                    await self._check_grid_fills(grid_id)
                    await self._update_grid_statistics(grid_id)
                    await self._check_risk_limits(grid_id)
                    
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in grid monitoring: {e}")
                await asyncio.sleep(10)
                
    async def _check_grid_fills(self, grid_id: str):
        """Check for filled orders and place new ones"""
        grid = self.grids[grid_id]
        levels = self.grid_levels[grid_id]
        config = grid["config"]
        
        # Get order statuses from trading-core
        order_ids = []
        level_map = {}
        
        for level in levels:
            if level.buy_order_id:
                order_ids.append(level.buy_order_id)
                level_map[level.buy_order_id] = (level, "buy")
            if level.sell_order_id:
                order_ids.append(level.sell_order_id)
                level_map[level.sell_order_id] = (level, "sell")
                
        if not order_ids:
            return
            
        # Get order statuses
        statuses = await self.trading_core.get_order_statuses(order_ids)
        
        for order_id, status in statuses.items():
            if status.get("status") == "filled":
                level, side = level_map[order_id]
                
                # Update level
                if side == "buy":
                    level.buy_order_id = None
                    level.filled_buy_quantity += Decimal(status.get("filled_quantity", "0"))
                    level.last_filled_time = datetime.utcnow()
                    
                    # Update position
                    self.grid_stats[grid_id].current_position += Decimal(status.get("filled_quantity", "0"))
                    
                    # Place corresponding sell order
                    await self._place_grid_order(
                        grid_id, level, "sell", config.order_size
                    )
                    
                else:  # sell
                    level.sell_order_id = None
                    level.filled_sell_quantity += Decimal(status.get("filled_quantity", "0"))
                    level.last_filled_time = datetime.utcnow()
                    
                    # Update position
                    self.grid_stats[grid_id].current_position -= Decimal(status.get("filled_quantity", "0"))
                    
                    # Calculate profit
                    buy_price = level.price * (Decimal("1") - config.profit_target_per_grid)
                    sell_price = level.price
                    profit = (sell_price - buy_price) * Decimal(status.get("filled_quantity", "0"))
                    level.profit_taken += profit
                    self.grid_stats[grid_id].total_profit += profit
                    
                    # Place corresponding buy order
                    await self._place_grid_order(
                        grid_id, level, "buy", config.order_size
                    )
                    
                # Update statistics
                self.grid_stats[grid_id].total_trades += 1
                if side == "sell":
                    self.grid_stats[grid_id].winning_trades += 1
                    
    async def _update_grid_statistics(self, grid_id: str):
        """Update grid performance statistics"""
        stats = self.grid_stats[grid_id]
        levels = self.grid_levels[grid_id]
        
        # Calculate efficiency
        active_levels = sum(1 for l in levels if l.buy_order_id or l.sell_order_id)
        filled_levels = sum(1 for l in levels if l.filled_buy_quantity > 0 or l.filled_sell_quantity > 0)
        stats.grid_efficiency = Decimal(str(filled_levels / len(levels))) if levels else Decimal("0")
        
        # Calculate win rate
        if stats.total_trades > 0:
            stats.win_rate = Decimal(str(stats.winning_trades / stats.total_trades))
            stats.average_profit_per_trade = stats.total_profit / stats.total_trades
            
        # Calculate unrealized PnL
        if stats.current_position != 0:
            # Get current market price
            orderbook = await self.trading_core.get_orderbook(
                self.grids[grid_id]["market_id"], 
                depth=1
            )
            
            if orderbook and orderbook.get("bids") and orderbook.get("asks"):
                mid_price = (Decimal(orderbook["bids"][0]["price"]) + 
                           Decimal(orderbook["asks"][0]["price"])) / 2
                
                # Calculate average entry price
                total_buy_value = sum(
                    l.price * l.filled_buy_quantity 
                    for l in levels 
                    if l.filled_buy_quantity > 0
                )
                total_buy_quantity = sum(
                    l.filled_buy_quantity 
                    for l in levels 
                    if l.filled_buy_quantity > 0
                )
                
                if total_buy_quantity > 0:
                    avg_entry_price = total_buy_value / total_buy_quantity
                    stats.unrealized_pnl = (mid_price - avg_entry_price) * stats.current_position
                    
    async def _check_risk_limits(self, grid_id: str):
        """Check and enforce risk limits"""
        grid = self.grids[grid_id]
        config = grid["config"]
        stats = self.grid_stats[grid_id]
        
        # Check max position
        if abs(stats.current_position) > config.max_position:
            logger.warning(f"Grid {grid_id} exceeded max position: {stats.current_position}")
            grid["state"] = GridState.PAUSED
            
        # Check max drawdown
        total_pnl = stats.total_profit + stats.unrealized_pnl
        if total_pnl < -config.max_drawdown * grid["initial_capital"]:
            logger.warning(f"Grid {grid_id} hit max drawdown")
            await self.stop_grid(grid_id, liquidate=True)
            
        # Check stop loss
        if config.stop_loss and stats.unrealized_pnl < -config.stop_loss:
            logger.info(f"Grid {grid_id} hit stop loss")
            await self.stop_grid(grid_id, liquidate=True)
            
        # Check take profit
        if config.take_profit and stats.total_profit > config.take_profit:
            logger.info(f"Grid {grid_id} hit take profit")
            await self.stop_grid(grid_id, liquidate=True)
            
    async def _rebalance_grids(self):
        """Periodically rebalance grids based on market conditions"""
        while True:
            try:
                for grid_id, grid in self.grids.items():
                    if grid["state"] != GridState.ACTIVE:
                        continue
                        
                    config = grid["config"]
                    
                    # Check if rebalance is due
                    time_since_rebalance = datetime.utcnow() - grid["last_rebalance"]
                    if time_since_rebalance.seconds < config.rebalance_interval:
                        continue
                        
                    # Rebalance based on volatility if enabled
                    if config.adjust_on_volatility:
                        await self._adjust_grid_for_volatility(grid_id)
                        
                    # Update trailing grid if enabled
                    if config.use_trailing_grid:
                        await self._update_trailing_grid(grid_id)
                        
                    grid["last_rebalance"] = datetime.utcnow()
                    
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in grid rebalancing: {e}")
                await asyncio.sleep(60)
                
    async def _adjust_grid_for_volatility(self, grid_id: str):
        """Adjust grid spacing based on market volatility"""
        grid = self.grids[grid_id]
        config = grid["config"]
        
        # Get recent price data
        candles = await self.trading_core.get_candles(
            market_id=grid["market_id"],
            interval="5m",
            limit=100
        )
        
        if not candles:
            return
            
        # Calculate volatility
        prices = [Decimal(c["close"]) for c in candles]
        returns = [(prices[i] - prices[i-1]) / prices[i-1] 
                  for i in range(1, len(prices))]
        
        if returns:
            volatility = Decimal(str(np.std([float(r) for r in returns])))
            
            # Adjust grid if volatility changed significantly
            if abs(volatility - config.volatility_threshold) > config.volatility_threshold * Decimal("0.2"):
                logger.info(f"Adjusting grid {grid_id} for volatility: {volatility}")
                
                # Cancel all orders
                await self._cancel_all_grid_orders(grid_id)
                
                # Recalculate levels with new spacing
                if volatility > config.volatility_threshold:
                    # Widen grid in high volatility
                    new_range = (config.upper_price - config.lower_price) * (Decimal("1") + volatility)
                    config.upper_price = config.lower_price + new_range
                else:
                    # Tighten grid in low volatility
                    new_range = (config.upper_price - config.lower_price) * (Decimal("1") - volatility / 2)
                    config.upper_price = config.lower_price + new_range
                    
                # Recalculate and place new levels
                self.grid_levels[grid_id] = self._calculate_grid_levels(config)
                await self._place_grid_orders(grid_id)
                
    async def _update_trailing_grid(self, grid_id: str):
        """Update grid boundaries to follow price movement"""
        grid = self.grids[grid_id]
        config = grid["config"]
        
        # Get current market price
        orderbook = await self.trading_core.get_orderbook(grid["market_id"], depth=1)
        if not orderbook or not orderbook.get("bids") or not orderbook.get("asks"):
            return
            
        mid_price = (Decimal(orderbook["bids"][0]["price"]) + 
                    Decimal(orderbook["asks"][0]["price"])) / 2
        
        # Check if price moved beyond trailing distance
        grid_center = (config.upper_price + config.lower_price) / 2
        price_move = (mid_price - grid_center) / grid_center
        
        if abs(price_move) > config.trailing_distance:
            logger.info(f"Trailing grid {grid_id} to follow price movement")
            
            # Cancel all orders
            await self._cancel_all_grid_orders(grid_id)
            
            # Move grid boundaries
            grid_range = config.upper_price - config.lower_price
            config.lower_price = mid_price - (grid_range / 2)
            config.upper_price = mid_price + (grid_range / 2)
            
            # Recalculate and place new levels
            self.grid_levels[grid_id] = self._calculate_grid_levels(config)
            await self._place_grid_orders(grid_id)
            
    async def _cancel_all_grid_orders(self, grid_id: str):
        """Cancel all orders for a grid"""
        levels = self.grid_levels[grid_id]
        cancel_tasks = []
        
        for level in levels:
            if level.buy_order_id:
                cancel_tasks.append(
                    self.trading_core.cancel_order(
                        order_id=level.buy_order_id,
                        user_id=self.grids[grid_id]["user_id"]
                    )
                )
                level.buy_order_id = None
                
            if level.sell_order_id:
                cancel_tasks.append(
                    self.trading_core.cancel_order(
                        order_id=level.sell_order_id,
                        user_id=self.grids[grid_id]["user_id"]
                    )
                )
                level.sell_order_id = None
                
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)
            
    async def _calculate_dynamic_grid_params(self, grid_id: str) -> Optional[GridConfig]:
        """Calculate new grid parameters based on market conditions"""
        grid = self.grids[grid_id]
        config = grid["config"]
        
        # Get market data
        current_price = await self._get_current_price(config.resource_type)
        volatility = await self._calculate_volatility(config.resource_type)
        
        # Adjust grid range based on volatility
        price_range_multiplier = Decimal("1") + volatility
        
        new_config = GridConfig(
            grid_type=config.grid_type,
            lower_price=current_price * (Decimal("1") - (Decimal("0.1") * price_range_multiplier)),
            upper_price=current_price * (Decimal("1") + (Decimal("0.1") * price_range_multiplier)),
            grid_levels=config.grid_levels,
            order_size=config.order_size,
            resource_type=config.resource_type,
            max_position=config.max_position,
            stop_loss=config.stop_loss,
            take_profit=config.take_profit,
            max_drawdown=config.max_drawdown,
            adjust_on_volatility=config.adjust_on_volatility,
            volatility_threshold=config.volatility_threshold,
            rebalance_interval=config.rebalance_interval,
            use_limit_orders=config.use_limit_orders,
            post_only=config.post_only,
            time_in_force=config.time_in_force,
            compound_profits=config.compound_profits,
            profit_target_per_grid=config.profit_target_per_grid,
            use_trailing_grid=config.use_trailing_grid,
            trailing_distance=config.trailing_distance,
            use_martingale=config.use_martingale,
            martingale_multiplier=config.martingale_multiplier
        )
        
        return new_config
        
    async def _get_current_price(self, resource_type: str) -> Decimal:
        """Get current market price for resource"""
        # This method is no longer needed as prices are fetched from trading-core
        # Keeping it for now as it might be used elsewhere or for context
        # For now, return a placeholder or raise an error if not implemented
        raise NotImplementedError("Prices are now fetched from trading-core")
        
    async def _calculate_volatility(self, resource_type: str) -> Decimal:
        """Calculate recent price volatility"""
        # Simplified - in production would use historical data
        # This method is no longer needed as volatility is fetched from trading-core
        # Keeping it for now as it might be used elsewhere or for context
        # For now, return a placeholder or raise an error if not implemented
        raise NotImplementedError("Volatility is now fetched from trading-core")
        
    async def _get_recent_prices(self, resource_type: str, hours: int) -> List[Decimal]:
        """Get recent price history"""
        # Simplified - would query from database/cache
        # This method is no longer needed as prices are fetched from trading-core
        # Keeping it for now as it might be used elsewhere or for context
        # For now, return a placeholder or raise an error if not implemented
        raise NotImplementedError("Price history is now fetched from trading-core")
        
    def _has_inventory_for_sell(self, grid_id: str, sell_price: Decimal) -> bool:
        """Check if we have inventory bought below the sell price"""
        levels = self.grid_levels[grid_id]
        
        for level in levels:
            if level.price < sell_price and level.filled_buy_quantity > level.filled_sell_quantity:
                return True
                
        return False
        
    def _find_buy_cost(self, grid_id: str, sell_price: Decimal) -> Decimal:
        """Find the cost basis for a sell order"""
        levels = self.grid_levels[grid_id]
        config = self.grids[grid_id]["config"]
        
        # Find the corresponding buy level
        for level in levels:
            expected_sell_price = level.price * (Decimal("1") + config.profit_target_per_grid)
            if abs(expected_sell_price - sell_price) < Decimal("0.01"):
                return level.price * config.order_size
                
        # Fallback to average cost
        return self._calculate_average_entry_price(grid_id) * config.order_size
        
    def _calculate_average_entry_price(self, grid_id: str) -> Decimal:
        """Calculate average entry price for current position"""
        levels = self.grid_levels[grid_id]
        
        total_cost = Decimal("0")
        total_quantity = Decimal("0")
        
        for level in levels:
            net_quantity = level.filled_buy_quantity - level.filled_sell_quantity
            if net_quantity > 0:
                total_cost += level.price * net_quantity
                total_quantity += net_quantity
                
        return total_cost / total_quantity if total_quantity > 0 else Decimal("0") 