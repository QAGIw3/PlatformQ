"""
Market Maker Incentives System

Provides incentives for market makers to provide liquidity including:
- Maker rebates
- Volume-based tiers  
- Performance bonuses
- Liquidity mining rewards
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any, Set
from decimal import Decimal, getcontext
from datetime import datetime, timedelta
from enum import Enum
import logging
from dataclasses import dataclass, field
import numpy as np
from collections import defaultdict

from platformq_shared import ServiceClient, ProcessingResult, ProcessingStatus
from ..integrations import IgniteCache as IgniteClient, PulsarEventPublisher as PulsarClient
from ..models.order import Order, OrderType, OrderSide
from ..models.market import Market

# Set high precision
getcontext().prec = 28

logger = logging.getLogger(__name__)


class MakerTier(Enum):
    """Market maker tier levels"""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    PLATINUM = "platinum"
    DIAMOND = "diamond"


@dataclass
class TierRequirements:
    """Requirements for each maker tier"""
    min_volume_30d: Decimal
    min_depth_score: Decimal
    min_uptime_percent: Decimal
    maker_rebate_bps: Decimal  # Negative fee = rebate
    bonus_multiplier: Decimal


@dataclass
class MarketMakerMetrics:
    """Metrics tracking for a market maker"""
    maker_id: str
    volume_30d: Decimal = Decimal("0")
    volume_24h: Decimal = Decimal("0")
    
    # Depth provision metrics
    depth_score: Decimal = Decimal("0")
    avg_spread: Decimal = Decimal("0")
    time_at_best: Decimal = Decimal("0")  # % time at best bid/ask
    
    # Performance metrics
    uptime_percent: Decimal = Decimal("0")
    filled_orders: int = 0
    cancelled_orders: int = 0
    
    # Rewards tracking
    total_rebates_earned: Decimal = Decimal("0")
    total_bonuses_earned: Decimal = Decimal("0")
    current_tier: MakerTier = MakerTier.BRONZE
    
    # Historical data
    daily_volumes: List[Tuple[datetime, Decimal]] = field(default_factory=list)
    depth_history: List[Tuple[datetime, Decimal]] = field(default_factory=list)


@dataclass
class LiquidityMiningProgram:
    """Configuration for liquidity mining rewards"""
    program_id: str
    market_id: str
    start_time: datetime
    end_time: datetime
    total_rewards: Decimal
    reward_token: str
    
    # Distribution parameters
    volume_weight: Decimal = Decimal("0.4")
    depth_weight: Decimal = Decimal("0.3")
    uptime_weight: Decimal = Decimal("0.3")
    
    # Tracking
    distributed_rewards: Decimal = Decimal("0")
    participants: Set[str] = field(default_factory=set)


class MarketMakerIncentives:
    """Market maker incentives and rewards system"""
    
    def __init__(
        self,
        ignite_client: IgniteClient,
        pulsar_client: PulsarClient
    ):
        self.ignite_client = ignite_client
        self.pulsar_client = pulsar_client
        
        # Tier configuration
        self.tier_requirements = {
            MakerTier.BRONZE: TierRequirements(
                min_volume_30d=Decimal("100000"),      # $100k
                min_depth_score=Decimal("0.5"),
                min_uptime_percent=Decimal("80"),
                maker_rebate_bps=Decimal("-5"),        # 0.05% rebate
                bonus_multiplier=Decimal("1.0")
            ),
            MakerTier.SILVER: TierRequirements(
                min_volume_30d=Decimal("1000000"),     # $1M
                min_depth_score=Decimal("0.7"),
                min_uptime_percent=Decimal("85"),
                maker_rebate_bps=Decimal("-10"),       # 0.10% rebate
                bonus_multiplier=Decimal("1.2")
            ),
            MakerTier.GOLD: TierRequirements(
                min_volume_30d=Decimal("10000000"),    # $10M
                min_depth_score=Decimal("0.8"),
                min_uptime_percent=Decimal("90"),
                maker_rebate_bps=Decimal("-15"),       # 0.15% rebate
                bonus_multiplier=Decimal("1.5")
            ),
            MakerTier.PLATINUM: TierRequirements(
                min_volume_30d=Decimal("50000000"),    # $50M
                min_depth_score=Decimal("0.9"),
                min_uptime_percent=Decimal("95"),
                maker_rebate_bps=Decimal("-20"),       # 0.20% rebate
                bonus_multiplier=Decimal("2.0")
            ),
            MakerTier.DIAMOND: TierRequirements(
                min_volume_30d=Decimal("100000000"),   # $100M
                min_depth_score=Decimal("0.95"),
                min_uptime_percent=Decimal("98"),
                maker_rebate_bps=Decimal("-25"),       # 0.25% rebate
                bonus_multiplier=Decimal("3.0")
            )
        }
        
        # Maker metrics storage
        self.maker_metrics: Dict[str, MarketMakerMetrics] = {}
        
        # Active liquidity mining programs
        self.mining_programs: Dict[str, LiquidityMiningProgram] = {}
        
        # Order tracking for spread/depth calculation
        self.maker_orders: Dict[str, List[Order]] = defaultdict(list)
        self.order_timestamps: Dict[str, datetime] = {}
        
        # Performance tracking
        self.performance_snapshots: List[Dict] = []
        self.last_snapshot_time = datetime.now()
        
        # Background tasks
        self._running = True
        self._tasks = []
    
    async def start(self):
        """Start incentive system background tasks"""
        self._tasks.append(asyncio.create_task(self._update_metrics_loop()))
        self._tasks.append(asyncio.create_task(self._distribute_rewards_loop()))
        self._tasks.append(asyncio.create_task(self._tier_evaluation_loop()))
        logger.info("Market maker incentives system started")
    
    async def stop(self):
        """Stop background tasks"""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("Market maker incentives system stopped")
    
    async def record_maker_order(
        self,
        order: Order,
        maker_id: str,
        market: Market
    ) -> ProcessingResult:
        """Record a maker order for metrics tracking"""
        try:
            # Initialize metrics if needed
            if maker_id not in self.maker_metrics:
                self.maker_metrics[maker_id] = MarketMakerMetrics(maker_id=maker_id)
            
            # Track order
            self.maker_orders[maker_id].append(order)
            self.order_timestamps[order.order_id] = datetime.now()
            
            # Update depth score in real-time
            await self._update_depth_score(maker_id, market)
            
            # Emit event
            await self.pulsar_client.publish(
                "maker-order-placed",
                {
                    "maker_id": maker_id,
                    "order": order.__dict__,
                    "market_id": market.market_id,
                    "timestamp": datetime.now()
                }
            )
            
            return ProcessingResult(
                status=ProcessingStatus.COMPLETED,
                data={"order_tracked": True}
            )
            
        except Exception as e:
            logger.error(f"Error recording maker order: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def record_trade_execution(
        self,
        maker_id: str,
        order_id: str,
        volume: Decimal,
        price: Decimal,
        is_maker: bool
    ) -> ProcessingResult:
        """Record trade execution for rebate calculation"""
        try:
            if maker_id not in self.maker_metrics:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="Unknown maker ID"
                )
            
            metrics = self.maker_metrics[maker_id]
            
            if is_maker:
                # Calculate rebate based on tier
                tier_req = self.tier_requirements[metrics.current_tier]
                rebate = volume * price * abs(tier_req.maker_rebate_bps) / Decimal("10000")
                
                # Update metrics
                metrics.filled_orders += 1
                metrics.volume_24h += volume * price
                metrics.volume_30d += volume * price
                metrics.total_rebates_earned += rebate
                
                # Track daily volume
                today = datetime.now().date()
                if not metrics.daily_volumes or metrics.daily_volumes[-1][0].date() != today:
                    metrics.daily_volumes.append((datetime.now(), volume * price))
                else:
                    # Update today's volume
                    last_date, last_vol = metrics.daily_volumes[-1]
                    metrics.daily_volumes[-1] = (last_date, last_vol + volume * price)
                
                # Clean old daily volumes (keep 30 days)
                cutoff = datetime.now() - timedelta(days=30)
                metrics.daily_volumes = [
                    (dt, vol) for dt, vol in metrics.daily_volumes 
                    if dt > cutoff
                ]
                
                # Store updated metrics
                await self.ignite_client.put(
                    f"maker_metrics_{maker_id}",
                    metrics
                )
                
                # Emit rebate event
                await self.pulsar_client.publish(
                    "maker-rebate-earned",
                    {
                        "maker_id": maker_id,
                        "order_id": order_id,
                        "volume": str(volume),
                        "price": str(price),
                        "rebate": str(rebate),
                        "tier": metrics.current_tier.value,
                        "timestamp": datetime.now()
                    }
                )
                
                return ProcessingResult(
                    status=ProcessingStatus.COMPLETED,
                    data={
                        "rebate_earned": str(rebate),
                        "current_tier": metrics.current_tier.value
                    }
                )
            else:
                # Taker order - no rebate
                return ProcessingResult(
                    status=ProcessingStatus.COMPLETED,
                    data={"rebate_earned": "0"}
                )
                
        except Exception as e:
            logger.error(f"Error recording trade execution: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def create_liquidity_mining_program(
        self,
        market_id: str,
        duration_days: int,
        total_rewards: Decimal,
        reward_token: str
    ) -> ProcessingResult:
        """Create a new liquidity mining program"""
        try:
            program_id = f"LM-{market_id}-{datetime.now().timestamp()}"
            
            program = LiquidityMiningProgram(
                program_id=program_id,
                market_id=market_id,
                start_time=datetime.now(),
                end_time=datetime.now() + timedelta(days=duration_days),
                total_rewards=total_rewards,
                reward_token=reward_token
            )
            
            self.mining_programs[program_id] = program
            
            # Store in Ignite
            await self.ignite_client.put(
                f"mining_program_{program_id}",
                program
            )
            
            # Announce program
            await self.pulsar_client.publish(
                "liquidity-mining-started",
                {
                    "program_id": program_id,
                    "market_id": market_id,
                    "duration_days": duration_days,
                    "total_rewards": str(total_rewards),
                    "reward_token": reward_token,
                    "start_time": program.start_time,
                    "end_time": program.end_time
                }
            )
            
            logger.info(f"Created liquidity mining program: {program_id}")
            
            return ProcessingResult(
                status=ProcessingStatus.COMPLETED,
                data={"program_id": program_id}
            )
            
        except Exception as e:
            logger.error(f"Error creating mining program: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def _update_depth_score(self, maker_id: str, market: Market):
        """Update depth score for a market maker"""
        try:
            orders = self.maker_orders.get(maker_id, [])
            if not orders:
                return
            
            # Calculate depth provision metrics
            bid_orders = [o for o in orders if o.side == OrderSide.BUY and o.is_active]
            ask_orders = [o for o in orders if o.side == OrderSide.SELL and o.is_active]
            
            if not bid_orders or not ask_orders:
                return
            
            # Best bid/ask
            best_bid = max(o.price for o in bid_orders)
            best_ask = min(o.price for o in ask_orders)
            
            # Spread
            spread = (best_ask - best_bid) / ((best_ask + best_bid) / 2) * Decimal("10000")  # in bps
            
            # Depth at various levels (1%, 2%, 5% from mid)
            mid_price = (best_bid + best_ask) / 2
            
            depth_1pct = Decimal("0")
            depth_2pct = Decimal("0")
            depth_5pct = Decimal("0")
            
            for order in bid_orders:
                price_diff = (mid_price - order.price) / mid_price
                if price_diff <= Decimal("0.01"):
                    depth_1pct += order.size * order.price
                if price_diff <= Decimal("0.02"):
                    depth_2pct += order.size * order.price
                if price_diff <= Decimal("0.05"):
                    depth_5pct += order.size * order.price
            
            for order in ask_orders:
                price_diff = (order.price - mid_price) / mid_price
                if price_diff <= Decimal("0.01"):
                    depth_1pct += order.size * order.price
                if price_diff <= Decimal("0.02"):
                    depth_2pct += order.size * order.price
                if price_diff <= Decimal("0.05"):
                    depth_5pct += order.size * order.price
            
            # Calculate depth score (0-1)
            # Based on depth provision and spread tightness
            depth_score = Decimal("0")
            
            # Depth component (60% weight)
            target_depth = market.base_currency_volume * Decimal("0.01")  # 1% of daily volume
            depth_ratio = min(Decimal("1"), depth_1pct / target_depth)
            depth_score += depth_ratio * Decimal("0.6")
            
            # Spread component (40% weight)
            max_spread = Decimal("50")  # 50 bps max
            spread_score = max(Decimal("0"), (max_spread - spread) / max_spread)
            depth_score += spread_score * Decimal("0.4")
            
            # Update metrics
            metrics = self.maker_metrics[maker_id]
            metrics.depth_score = depth_score
            metrics.avg_spread = spread
            
            # Track depth history
            metrics.depth_history.append((datetime.now(), depth_score))
            
            # Keep last 24 hours
            cutoff = datetime.now() - timedelta(hours=24)
            metrics.depth_history = [
                (dt, score) for dt, score in metrics.depth_history 
                if dt > cutoff
            ]
            
        except Exception as e:
            logger.error(f"Error updating depth score: {str(e)}")
    
    async def _calculate_performance_bonus(
        self,
        maker_id: str,
        period_start: datetime,
        period_end: datetime
    ) -> Decimal:
        """Calculate performance bonus for a market maker"""
        
        metrics = self.maker_metrics.get(maker_id)
        if not metrics:
            return Decimal("0")
        
        # Base bonus pool (could be configured per market)
        base_bonus = Decimal("10000")  # $10k monthly bonus pool
        
        # Performance multipliers
        multipliers = []
        
        # 1. Volume achievement
        volume_target = self.tier_requirements[metrics.current_tier].min_volume_30d
        volume_achievement = min(Decimal("2"), metrics.volume_30d / volume_target)
        multipliers.append(volume_achievement)
        
        # 2. Uptime achievement
        uptime_target = self.tier_requirements[metrics.current_tier].min_uptime_percent
        uptime_achievement = min(Decimal("1.5"), metrics.uptime_percent / uptime_target)
        multipliers.append(uptime_achievement)
        
        # 3. Depth score achievement
        depth_target = self.tier_requirements[metrics.current_tier].min_depth_score
        depth_achievement = min(Decimal("1.5"), metrics.depth_score / depth_target)
        multipliers.append(depth_achievement)
        
        # 4. Fill rate (filled vs cancelled orders)
        total_orders = metrics.filled_orders + metrics.cancelled_orders
        if total_orders > 0:
            fill_rate = Decimal(str(metrics.filled_orders)) / Decimal(str(total_orders))
            fill_multiplier = min(Decimal("1.2"), fill_rate * Decimal("1.5"))
            multipliers.append(fill_multiplier)
        
        # Calculate final bonus
        total_multiplier = sum(multipliers) / len(multipliers)
        tier_multiplier = self.tier_requirements[metrics.current_tier].bonus_multiplier
        
        bonus = base_bonus * total_multiplier * tier_multiplier
        
        # Pro-rate for period
        days_in_period = (period_end - period_start).days
        bonus = bonus * Decimal(str(days_in_period)) / Decimal("30")
        
        return bonus
    
    async def _update_metrics_loop(self):
        """Background task to update maker metrics"""
        while self._running:
            try:
                await asyncio.sleep(60)  # Update every minute
                
                for maker_id, metrics in self.maker_metrics.items():
                    # Update 24h volume
                    cutoff_24h = datetime.now() - timedelta(hours=24)
                    volume_24h = sum(
                        vol for dt, vol in metrics.daily_volumes 
                        if dt > cutoff_24h
                    )
                    metrics.volume_24h = volume_24h
                    
                    # Update 30d volume
                    cutoff_30d = datetime.now() - timedelta(days=30)
                    volume_30d = sum(
                        vol for dt, vol in metrics.daily_volumes 
                        if dt > cutoff_30d
                    )
                    metrics.volume_30d = volume_30d
                    
                    # Update uptime
                    active_orders = len([
                        o for o in self.maker_orders.get(maker_id, [])
                        if o.is_active
                    ])
                    
                    if active_orders > 0:
                        # Simple uptime tracking - has active orders
                        if not hasattr(metrics, '_uptime_samples'):
                            metrics._uptime_samples = []
                        
                        metrics._uptime_samples.append(1)
                        
                        # Keep last 1440 samples (24 hours of minutes)
                        metrics._uptime_samples = metrics._uptime_samples[-1440:]
                        
                        # Calculate uptime percentage
                        if metrics._uptime_samples:
                            metrics.uptime_percent = (
                                Decimal(str(sum(metrics._uptime_samples))) / 
                                Decimal(str(len(metrics._uptime_samples))) * 
                                Decimal("100")
                            )
                    
                    # Cache updated metrics
                    await self.ignite_client.put(
                        f"maker_metrics_{maker_id}",
                        metrics
                    )
                
                # Take performance snapshot
                if datetime.now() - self.last_snapshot_time > timedelta(hours=1):
                    await self._take_performance_snapshot()
                    self.last_snapshot_time = datetime.now()
                    
            except Exception as e:
                logger.error(f"Error in metrics update loop: {str(e)}")
    
    async def _distribute_rewards_loop(self):
        """Background task to distribute liquidity mining rewards"""
        while self._running:
            try:
                await asyncio.sleep(3600)  # Check every hour
                
                for program_id, program in self.mining_programs.items():
                    if program.end_time < datetime.now():
                        continue
                    
                    # Calculate hourly distribution
                    hours_total = (program.end_time - program.start_time).total_seconds() / 3600
                    hourly_rewards = program.total_rewards / Decimal(str(hours_total))
                    
                    # Get eligible makers for this market
                    eligible_makers = []
                    total_score = Decimal("0")
                    
                    for maker_id, metrics in self.maker_metrics.items():
                        # Check if maker is active in this market
                        # (simplified - would check actual market activity)
                        if metrics.volume_24h > 0:
                            # Calculate maker score
                            score = (
                                metrics.volume_24h * program.volume_weight +
                                metrics.depth_score * Decimal("1000000") * program.depth_weight +
                                metrics.uptime_percent * Decimal("10000") * program.uptime_weight
                            )
                            
                            eligible_makers.append((maker_id, score))
                            total_score += score
                            program.participants.add(maker_id)
                    
                    if total_score > 0 and eligible_makers:
                        # Distribute proportionally
                        distributions = []
                        
                        for maker_id, score in eligible_makers:
                            share = score / total_score
                            reward = hourly_rewards * share
                            
                            distributions.append({
                                "maker_id": maker_id,
                                "reward": reward,
                                "share": share
                            })
                            
                            # Update maker metrics
                            if maker_id in self.maker_metrics:
                                self.maker_metrics[maker_id].total_bonuses_earned += reward
                        
                        # Update program tracking
                        program.distributed_rewards += hourly_rewards
                        
                        # Emit distribution event
                        await self.pulsar_client.publish(
                            "liquidity-rewards-distributed",
                            {
                                "program_id": program_id,
                                "hour_rewards": str(hourly_rewards),
                                "distributions": distributions,
                                "timestamp": datetime.now()
                            }
                        )
                        
                        logger.info(
                            f"Distributed {hourly_rewards} {program.reward_token} "
                            f"to {len(distributions)} makers"
                        )
                        
            except Exception as e:
                logger.error(f"Error in rewards distribution: {str(e)}")
    
    async def _tier_evaluation_loop(self):
        """Background task to evaluate and update maker tiers"""
        while self._running:
            try:
                await asyncio.sleep(3600)  # Evaluate every hour
                
                tier_changes = []
                
                for maker_id, metrics in self.maker_metrics.items():
                    current_tier = metrics.current_tier
                    new_tier = self._evaluate_tier(metrics)
                    
                    if new_tier != current_tier:
                        old_tier = current_tier
                        metrics.current_tier = new_tier
                        
                        tier_changes.append({
                            "maker_id": maker_id,
                            "old_tier": old_tier.value,
                            "new_tier": new_tier.value,
                            "volume_30d": str(metrics.volume_30d),
                            "depth_score": str(metrics.depth_score),
                            "uptime": str(metrics.uptime_percent)
                        })
                        
                        # Store updated metrics
                        await self.ignite_client.put(
                            f"maker_metrics_{maker_id}",
                            metrics
                        )
                        
                        logger.info(
                            f"Maker {maker_id} tier changed: "
                            f"{old_tier.value} -> {new_tier.value}"
                        )
                
                if tier_changes:
                    # Emit tier change events
                    await self.pulsar_client.publish(
                        "maker-tier-changes",
                        {
                            "changes": tier_changes,
                            "timestamp": datetime.now()
                        }
                    )
                    
            except Exception as e:
                logger.error(f"Error in tier evaluation: {str(e)}")
    
    def _evaluate_tier(self, metrics: MarketMakerMetrics) -> MakerTier:
        """Evaluate appropriate tier for maker based on metrics"""
        
        # Check from highest to lowest tier
        for tier in [
            MakerTier.DIAMOND,
            MakerTier.PLATINUM,
            MakerTier.GOLD,
            MakerTier.SILVER,
            MakerTier.BRONZE
        ]:
            requirements = self.tier_requirements[tier]
            
            if (metrics.volume_30d >= requirements.min_volume_30d and
                metrics.depth_score >= requirements.min_depth_score and
                metrics.uptime_percent >= requirements.min_uptime_percent):
                return tier
        
        return MakerTier.BRONZE  # Default tier
    
    async def _take_performance_snapshot(self):
        """Take snapshot of all maker performance"""
        snapshot = {
            "timestamp": datetime.now(),
            "makers": {}
        }
        
        for maker_id, metrics in self.maker_metrics.items():
            snapshot["makers"][maker_id] = {
                "tier": metrics.current_tier.value,
                "volume_24h": str(metrics.volume_24h),
                "volume_30d": str(metrics.volume_30d),
                "depth_score": str(metrics.depth_score),
                "uptime": str(metrics.uptime_percent),
                "total_rebates": str(metrics.total_rebates_earned),
                "total_bonuses": str(metrics.total_bonuses_earned)
            }
        
        self.performance_snapshots.append(snapshot)
        
        # Keep last 30 days of snapshots
        cutoff = datetime.now() - timedelta(days=30)
        self.performance_snapshots = [
            s for s in self.performance_snapshots 
            if s["timestamp"] > cutoff
        ]
        
        # Store in Ignite
        await self.ignite_client.put(
            f"maker_performance_snapshot_{snapshot['timestamp'].timestamp()}",
            snapshot
        )
    
    async def get_maker_stats(self, maker_id: str) -> Optional[Dict]:
        """Get current stats for a market maker"""
        
        metrics = self.maker_metrics.get(maker_id)
        if not metrics:
            return None
        
        # Calculate pending bonuses
        month_start = datetime.now().replace(day=1, hour=0, minute=0, second=0)
        pending_bonus = await self._calculate_performance_bonus(
            maker_id,
            month_start,
            datetime.now()
        )
        
        return {
            "maker_id": maker_id,
            "current_tier": metrics.current_tier.value,
            "tier_requirements": self.tier_requirements[metrics.current_tier].__dict__,
            "metrics": {
                "volume_24h": str(metrics.volume_24h),
                "volume_30d": str(metrics.volume_30d),
                "depth_score": str(metrics.depth_score),
                "avg_spread": str(metrics.avg_spread),
                "uptime_percent": str(metrics.uptime_percent),
                "filled_orders": metrics.filled_orders,
                "cancelled_orders": metrics.cancelled_orders
            },
            "earnings": {
                "total_rebates": str(metrics.total_rebates_earned),
                "total_bonuses": str(metrics.total_bonuses_earned),
                "pending_bonus": str(pending_bonus)
            },
            "active_orders": len([
                o for o in self.maker_orders.get(maker_id, [])
                if o.is_active
            ])
        }
    
    async def get_leaderboard(
        self,
        metric: str = "volume_30d",
        limit: int = 10
    ) -> List[Dict]:
        """Get maker leaderboard by various metrics"""
        
        makers = []
        for maker_id, metrics in self.maker_metrics.items():
            value = getattr(metrics, metric, Decimal("0"))
            makers.append({
                "maker_id": maker_id,
                "tier": metrics.current_tier.value,
                metric: str(value),
                "total_earnings": str(
                    metrics.total_rebates_earned + 
                    metrics.total_bonuses_earned
                )
            })
        
        # Sort by metric
        makers.sort(key=lambda x: Decimal(x[metric]), reverse=True)
        
        return makers[:limit] 