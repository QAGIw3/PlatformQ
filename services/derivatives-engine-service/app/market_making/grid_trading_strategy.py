"""
Grid Trading Strategy for Compute Spot Markets

Implements automated grid trading with dynamic grid adjustment, risk management,
and integration with the compute spot market engine.
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

from app.engines.compute_spot_market import ComputeSpotMarket, Order, OrderType, OrderSide
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
    Automated grid trading strategy for compute spot markets
    """
    
    def __init__(self,
                 spot_market: ComputeSpotMarket,
                 ignite: IgniteCache,
                 pulsar: PulsarEventPublisher,
                 oracle: OracleAggregatorClient):
        self.spot_market = spot_market
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        
        self.grids: Dict[str, Dict[str, Any]] = {}  # grid_id -> grid data
        self.grid_levels: Dict[str, List[GridLevel]] = {}  # grid_id -> levels
        self.grid_stats: Dict[str, GridStatistics] = {}  # grid_id -> stats
        
        self._monitoring_task: Optional[asyncio.Task] = None
        self._rebalancing_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start grid trading strategy"""
        logger.info("Starting grid trading strategy")
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
            "current_capital": Decimal("0")
        }
        
        self.grid_levels[grid_id] = levels
        self.grid_stats[grid_id] = GridStatistics()
        
        # Place initial orders
        await self._place_grid_orders(grid_id)
        
        # Publish event
        await self.pulsar.publish('grid.created', {
            'grid_id': grid_id,
            'user_id': user_id,
            'config': config.__dict__
        })
        
        logger.info(f"Created grid {grid_id} with {len(levels)} levels")
        
        return grid_id
        
    async def stop_grid(self, grid_id: str, liquidate: bool = False) -> Dict[str, Any]:
        """Stop a grid and optionally liquidate positions"""
        if grid_id not in self.grids:
            raise ValueError(f"Grid {grid_id} not found")
            
        grid = self.grids[grid_id]
        grid["state"] = GridState.STOPPED if not liquidate else GridState.LIQUIDATING
        
        # Cancel all orders
        levels = self.grid_levels[grid_id]
        for level in levels:
            if level.buy_order_id:
                await self.spot_market.cancel_order(level.buy_order_id, grid["user_id"])
            if level.sell_order_id:
                await self.spot_market.cancel_order(level.sell_order_id, grid["user_id"])
                
        # Liquidate position if requested
        final_position = Decimal("0")
        liquidation_proceeds = Decimal("0")
        
        if liquidate:
            stats = self.grid_stats[grid_id]
            if stats.current_position > 0:
                # Market sell remaining position
                order = await self.spot_market.place_order(
                    user_id=grid["user_id"],
                    order_type=OrderType.MARKET,
                    side=OrderSide.SELL,
                    resource_type=grid["config"].resource_type,
                    quantity=stats.current_position,
                    price=None
                )
                
                # Wait for execution
                await asyncio.sleep(1)
                
                # Get execution price
                order_info = await self.spot_market.get_order(order.order_id)
                if order_info and order_info.filled_quantity > 0:
                    liquidation_proceeds = order_info.filled_quantity * order_info.average_price
                    
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
        """Get current status and performance of a grid"""
        if grid_id not in self.grids:
            raise ValueError(f"Grid {grid_id} not found")
            
        grid = self.grids[grid_id]
        stats = self.grid_stats[grid_id]
        levels = self.grid_levels[grid_id]
        
        # Calculate current metrics
        active_buy_orders = sum(1 for l in levels if l.buy_order_id is not None)
        active_sell_orders = sum(1 for l in levels if l.sell_order_id is not None)
        filled_levels = sum(1 for l in levels if l.filled_buy_quantity > 0 or l.filled_sell_quantity > 0)
        
        # Get current price
        current_price = await self._get_current_price(grid["config"].resource_type)
        
        # Calculate unrealized PnL
        if stats.current_position > 0:
            avg_entry_price = self._calculate_average_entry_price(grid_id)
            stats.unrealized_pnl = (current_price - avg_entry_price) * stats.current_position
            
        return {
            "grid_id": grid_id,
            "state": grid["state"].value,
            "config": grid["config"].__dict__,
            "created_at": grid["created_at"].isoformat(),
            "last_rebalance": grid["last_rebalance"].isoformat(),
            "current_price": float(current_price),
            "active_buy_orders": active_buy_orders,
            "active_sell_orders": active_sell_orders,
            "filled_levels": filled_levels,
            "total_levels": len(levels),
            "statistics": {
                "total_trades": stats.total_trades,
                "winning_trades": stats.winning_trades,
                "win_rate": float(stats.win_rate),
                "total_profit": float(stats.total_profit),
                "unrealized_pnl": float(stats.unrealized_pnl),
                "current_position": float(stats.current_position),
                "total_volume": float(stats.total_volume),
                "max_drawdown": float(stats.max_drawdown),
                "sharpe_ratio": float(stats.sharpe_ratio),
                "grid_efficiency": float(stats.grid_efficiency)
            }
        }
        
    # Internal methods
    
    def _calculate_grid_levels(self, config: GridConfig) -> List[GridLevel]:
        """Calculate grid price levels based on configuration"""
        levels = []
        
        if config.grid_type == GridType.ARITHMETIC:
            # Fixed price intervals
            interval = (config.upper_price - config.lower_price) / (config.grid_levels - 1)
            for i in range(config.grid_levels):
                price = config.lower_price + (interval * i)
                levels.append(GridLevel(price=price))
                
        elif config.grid_type == GridType.GEOMETRIC:
            # Percentage-based intervals
            ratio = (config.upper_price / config.lower_price) ** (1 / (config.grid_levels - 1))
            for i in range(config.grid_levels):
                price = config.lower_price * (ratio ** i)
                levels.append(GridLevel(price=price))
                
        elif config.grid_type == GridType.FIBONACCI:
            # Fibonacci levels
            fib_ratios = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0]
            price_range = config.upper_price - config.lower_price
            
            for ratio in fib_ratios[:config.grid_levels]:
                price = config.lower_price + (price_range * ratio)
                levels.append(GridLevel(price=price))
                
        elif config.grid_type == GridType.DYNAMIC:
            # Will be calculated based on volatility
            levels = self._calculate_dynamic_levels(config)
            
        return sorted(levels, key=lambda x: x.price)
        
    def _calculate_dynamic_levels(self, config: GridConfig) -> List[GridLevel]:
        """Calculate dynamic grid levels based on market volatility"""
        # Simplified implementation - in production would use volatility data
        # For now, use geometric with adjusted spacing
        volatility_factor = Decimal("1.5")  # Higher volatility = wider spacing
        
        adjusted_levels = int(config.grid_levels / volatility_factor)
        ratio = (config.upper_price / config.lower_price) ** (1 / (adjusted_levels - 1))
        
        levels = []
        for i in range(adjusted_levels):
            price = config.lower_price * (ratio ** i)
            levels.append(GridLevel(price=price))
            
        return levels
        
    async def _place_grid_orders(self, grid_id: str) -> None:
        """Place orders for all grid levels"""
        grid = self.grids[grid_id]
        config = grid["config"]
        levels = self.grid_levels[grid_id]
        
        # Get current price
        current_price = await self._get_current_price(config.resource_type)
        
        for level in levels:
            # Place buy order if price is below current
            if level.price < current_price and not level.buy_order_id:
                order = await self.spot_market.place_order(
                    user_id=grid["user_id"],
                    order_type=OrderType.LIMIT if config.use_limit_orders else OrderType.MARKET,
                    side=OrderSide.BUY,
                    resource_type=config.resource_type,
                    quantity=config.order_size,
                    price=level.price,
                    time_in_force=config.time_in_force,
                    post_only=config.post_only
                )
                level.buy_order_id = order.order_id
                
            # Place sell order if price is above current and we have inventory
            elif level.price > current_price and not level.sell_order_id:
                # Check if we have bought at a lower level
                if self._has_inventory_for_sell(grid_id, level.price):
                    order = await self.spot_market.place_order(
                        user_id=grid["user_id"],
                        order_type=OrderType.LIMIT,
                        side=OrderSide.SELL,
                        resource_type=config.resource_type,
                        quantity=config.order_size,
                        price=level.price,
                        time_in_force=config.time_in_force,
                        post_only=config.post_only
                    )
                    level.sell_order_id = order.order_id
                    
    async def _cancel_grid_orders(self, grid_id: str) -> None:
        """Cancel all orders for a grid"""
        grid = self.grids[grid_id]
        levels = self.grid_levels[grid_id]
        
        for level in levels:
            if level.buy_order_id:
                try:
                    await self.spot_market.cancel_order(level.buy_order_id, grid["user_id"])
                    level.buy_order_id = None
                except Exception as e:
                    logger.error(f"Failed to cancel buy order {level.buy_order_id}: {e}")
                    
            if level.sell_order_id:
                try:
                    await self.spot_market.cancel_order(level.sell_order_id, grid["user_id"])
                    level.sell_order_id = None
                except Exception as e:
                    logger.error(f"Failed to cancel sell order {level.sell_order_id}: {e}")
                    
    async def _monitor_grids(self) -> None:
        """Monitor grids for filled orders and performance"""
        while True:
            try:
                for grid_id, grid in self.grids.items():
                    if grid["state"] != GridState.ACTIVE:
                        continue
                        
                    await self._check_filled_orders(grid_id)
                    await self._update_statistics(grid_id)
                    await self._check_risk_limits(grid_id)
                    
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring grids: {e}")
                await asyncio.sleep(5)
                
    async def _check_filled_orders(self, grid_id: str) -> None:
        """Check for filled orders and place new ones"""
        grid = self.grids[grid_id]
        config = grid["config"]
        levels = self.grid_levels[grid_id]
        stats = self.grid_stats[grid_id]
        
        for level in levels:
            # Check buy orders
            if level.buy_order_id:
                order = await self.spot_market.get_order(level.buy_order_id)
                
                if order and order.filled_quantity > 0:
                    # Update level
                    level.filled_buy_quantity += order.filled_quantity
                    level.last_filled_time = datetime.utcnow()
                    
                    # Update statistics
                    stats.total_trades += 1
                    stats.total_volume += order.filled_quantity * order.average_price
                    stats.current_position += order.filled_quantity
                    
                    # Place corresponding sell order
                    sell_price = level.price * (Decimal("1") + config.profit_target_per_grid)
                    
                    sell_order = await self.spot_market.place_order(
                        user_id=grid["user_id"],
                        order_type=OrderType.LIMIT,
                        side=OrderSide.SELL,
                        resource_type=config.resource_type,
                        quantity=order.filled_quantity,
                        price=sell_price,
                        time_in_force=config.time_in_force,
                        post_only=config.post_only
                    )
                    
                    # Find or create sell level
                    sell_level = next((l for l in levels if abs(l.price - sell_price) < Decimal("0.01")), None)
                    if sell_level:
                        sell_level.sell_order_id = sell_order.order_id
                    
                    level.buy_order_id = None
                    
            # Check sell orders
            if level.sell_order_id:
                order = await self.spot_market.get_order(level.sell_order_id)
                
                if order and order.filled_quantity > 0:
                    # Update level
                    level.filled_sell_quantity += order.filled_quantity
                    level.last_filled_time = datetime.utcnow()
                    
                    # Calculate profit
                    buy_cost = self._find_buy_cost(grid_id, level.price)
                    sell_revenue = order.filled_quantity * order.average_price
                    profit = sell_revenue - buy_cost
                    
                    level.profit_taken += profit
                    
                    # Update statistics
                    stats.total_trades += 1
                    stats.total_volume += sell_revenue
                    stats.current_position -= order.filled_quantity
                    stats.total_profit += profit
                    
                    if profit > 0:
                        stats.winning_trades += 1
                        
                    # Place new buy order at original level
                    buy_order = await self.spot_market.place_order(
                        user_id=grid["user_id"],
                        order_type=OrderType.LIMIT,
                        side=OrderSide.BUY,
                        resource_type=config.resource_type,
                        quantity=config.order_size,
                        price=level.price,
                        time_in_force=config.time_in_force,
                        post_only=config.post_only
                    )
                    
                    level.buy_order_id = buy_order.order_id
                    level.sell_order_id = None
                    
    async def _update_statistics(self, grid_id: str) -> None:
        """Update grid performance statistics"""
        stats = self.grid_stats[grid_id]
        levels = self.grid_levels[grid_id]
        
        # Calculate win rate
        if stats.total_trades > 0:
            stats.win_rate = Decimal(stats.winning_trades) / Decimal(stats.total_trades)
            stats.average_profit_per_trade = stats.total_profit / Decimal(stats.total_trades)
            
        # Calculate grid efficiency
        filled_levels = sum(1 for l in levels if l.filled_buy_quantity > 0 or l.filled_sell_quantity > 0)
        stats.grid_efficiency = Decimal(filled_levels) / Decimal(len(levels)) if levels else Decimal("0")
        
        # Calculate Sharpe ratio (simplified)
        if len(stats.daily_returns) >= 7:
            returns = np.array([float(r) for r in stats.daily_returns])
            if returns.std() > 0:
                stats.sharpe_ratio = Decimal(str((returns.mean() / returns.std()) * np.sqrt(365)))
                
    async def _check_risk_limits(self, grid_id: str) -> None:
        """Check and enforce risk limits"""
        grid = self.grids[grid_id]
        config = grid["config"]
        stats = self.grid_stats[grid_id]
        
        # Check max position
        if stats.current_position > config.max_position:
            # Cancel buy orders
            levels = self.grid_levels[grid_id]
            for level in levels:
                if level.buy_order_id:
                    await self.spot_market.cancel_order(level.buy_order_id, grid["user_id"])
                    level.buy_order_id = None
                    
        # Check stop loss
        if config.stop_loss and stats.unrealized_pnl < -config.stop_loss:
            await self.stop_grid(grid_id, liquidate=True)
            logger.warning(f"Stop loss triggered for grid {grid_id}")
            
        # Check max drawdown
        if stats.max_drawdown > config.max_drawdown:
            grid["state"] = GridState.PAUSED
            await self._cancel_grid_orders(grid_id)
            logger.warning(f"Max drawdown exceeded for grid {grid_id}, pausing")
            
    async def _rebalance_grids(self) -> None:
        """Periodically rebalance grids based on market conditions"""
        while True:
            try:
                for grid_id, grid in self.grids.items():
                    if grid["state"] != GridState.ACTIVE:
                        continue
                        
                    config = grid["config"]
                    
                    # Check if rebalance is needed
                    if config.adjust_on_volatility:
                        time_since_rebalance = (datetime.utcnow() - grid["last_rebalance"]).total_seconds()
                        
                        if time_since_rebalance >= config.rebalance_interval:
                            volatility = await self._calculate_volatility(config.resource_type)
                            
                            if volatility > config.volatility_threshold:
                                await self.adjust_grid(grid_id)
                                logger.info(f"Rebalanced grid {grid_id} due to volatility: {volatility}")
                                
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error rebalancing grids: {e}")
                await asyncio.sleep(60)
                
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
        market_data = await self.spot_market.get_market_data(resource_type)
        return market_data.get("last_price", Decimal("0"))
        
    async def _calculate_volatility(self, resource_type: str) -> Decimal:
        """Calculate recent price volatility"""
        # Simplified - in production would use historical data
        prices = await self._get_recent_prices(resource_type, 24)  # 24 hours
        
        if len(prices) < 2:
            return Decimal("0.1")  # Default 10% volatility
            
        returns = [(prices[i] - prices[i-1]) / prices[i-1] for i in range(1, len(prices))]
        volatility = Decimal(str(np.std([float(r) for r in returns])))
        
        return volatility
        
    async def _get_recent_prices(self, resource_type: str, hours: int) -> List[Decimal]:
        """Get recent price history"""
        # Simplified - would query from database/cache
        current_price = await self._get_current_price(resource_type)
        
        # Generate synthetic price history for demo
        prices = [current_price]
        for i in range(hours - 1):
            change = Decimal(str(np.random.normal(0, 0.02)))  # 2% volatility
            prices.append(prices[-1] * (Decimal("1") + change))
            
        return prices
        
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