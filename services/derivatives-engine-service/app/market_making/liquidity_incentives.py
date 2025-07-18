"""
Liquidity Incentive Programs for Compute Markets

Implements various incentive mechanisms to attract and reward liquidity providers:
- Maker rebates
- Volume-based rewards
- Spread tightness incentives
- Uptime rewards
- LP token staking
"""

import asyncio
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
from collections import defaultdict, deque

from app.engines.compute_spot_market import ComputeSpotMarket
from app.engines.compute_futures_engine import ComputeFuturesEngine
from app.engines.compute_options_engine import ComputeOptionsEngine
from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient

logger = logging.getLogger(__name__)


class IncentiveType(Enum):
    MAKER_REBATE = "maker_rebate"
    VOLUME_REWARD = "volume_reward"
    SPREAD_REWARD = "spread_reward"
    UPTIME_REWARD = "uptime_reward"
    LP_STAKING = "lp_staking"
    REFERRAL = "referral"


class RewardTier(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    PLATINUM = "platinum"
    DIAMOND = "diamond"


@dataclass
class LiquidityProvider:
    """Represents a liquidity provider in the incentive program"""
    provider_id: str
    user_id: str
    joined_at: datetime
    
    # Activity metrics
    total_volume: Decimal = Decimal("0")
    maker_volume: Decimal = Decimal("0")
    taker_volume: Decimal = Decimal("0")
    
    # Performance metrics
    average_spread: Decimal = Decimal("0")
    uptime_percentage: Decimal = Decimal("0")
    order_count: int = 0
    filled_orders: int = 0
    
    # Rewards
    total_rewards_earned: Decimal = Decimal("0")
    pending_rewards: Decimal = Decimal("0")
    rewards_by_type: Dict[IncentiveType, Decimal] = field(default_factory=dict)
    
    # Status
    current_tier: RewardTier = RewardTier.BRONZE
    is_active: bool = True
    last_activity: datetime = field(default_factory=datetime.utcnow)
    
    # Staking
    staked_amount: Decimal = Decimal("0")
    staking_multiplier: Decimal = Decimal("1")


@dataclass
class IncentiveProgram:
    """Configuration for an incentive program"""
    program_id: str
    name: str
    program_type: IncentiveType
    start_date: datetime
    end_date: Optional[datetime]
    
    # Reward parameters
    base_reward_rate: Decimal  # Base reward percentage
    max_reward_rate: Decimal   # Maximum reward percentage
    reward_budget: Decimal     # Total reward budget
    reward_token: str         # Token used for rewards
    
    # Tier requirements
    tier_thresholds: Dict[RewardTier, Dict[str, Decimal]] = field(default_factory=dict)
    tier_multipliers: Dict[RewardTier, Decimal] = field(default_factory=dict)
    
    # Specific parameters by type
    parameters: Dict[str, Any] = field(default_factory=dict)
    
    # Status
    is_active: bool = True
    total_distributed: Decimal = Decimal("0")
    participants: int = 0


@dataclass
class LiquidityMetrics:
    """Real-time liquidity metrics for a market"""
    resource_type: str
    timestamp: datetime
    
    # Depth metrics
    bid_depth: Decimal
    ask_depth: Decimal
    total_depth: Decimal
    
    # Spread metrics
    best_bid: Decimal
    best_ask: Decimal
    spread: Decimal
    spread_percentage: Decimal
    
    # Activity metrics
    makers_count: int
    orders_count: int
    volume_24h: Decimal
    
    # Quality metrics
    average_fill_rate: Decimal
    average_fill_time: timedelta


class LiquidityIncentiveManager:
    """
    Manages liquidity incentive programs for compute markets
    """
    
    def __init__(self,
                 spot_market: ComputeSpotMarket,
                 futures_engine: ComputeFuturesEngine,
                 options_engine: ComputeOptionsEngine,
                 ignite: IgniteCache,
                 pulsar: PulsarEventPublisher,
                 oracle: OracleAggregatorClient):
        self.spot_market = spot_market
        self.futures_engine = futures_engine
        self.options_engine = options_engine
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        
        self.programs: Dict[str, IncentiveProgram] = {}
        self.providers: Dict[str, LiquidityProvider] = {}
        self.liquidity_metrics: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1440))  # 24h of minute data
        
        # Activity tracking
        self.provider_activity: Dict[str, Dict[str, Any]] = {}
        self.reward_history: List[Dict[str, Any]] = []
        
        self._monitoring_task: Optional[asyncio.Task] = None
        self._reward_distribution_task: Optional[asyncio.Task] = None
        self._metrics_collection_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start liquidity incentive manager"""
        logger.info("Starting liquidity incentive manager")
        
        # Initialize default programs
        await self._initialize_default_programs()
        
        self._monitoring_task = asyncio.create_task(self._monitor_providers())
        self._reward_distribution_task = asyncio.create_task(self._distribute_rewards())
        self._metrics_collection_task = asyncio.create_task(self._collect_metrics())
        
    async def stop(self):
        """Stop liquidity incentive manager"""
        logger.info("Stopping liquidity incentive manager")
        
        for task in [self._monitoring_task, self._reward_distribution_task, self._metrics_collection_task]:
            if task:
                task.cancel()
                
    async def create_program(self, program: IncentiveProgram) -> str:
        """Create a new incentive program"""
        self.programs[program.program_id] = program
        
        # Publish event
        await self.pulsar.publish('incentive.program_created', {
            'program_id': program.program_id,
            'name': program.name,
            'type': program.program_type.value,
            'budget': float(program.reward_budget)
        })
        
        logger.info(f"Created incentive program {program.program_id}")
        return program.program_id
        
    async def register_provider(self, user_id: str) -> str:
        """Register a new liquidity provider"""
        provider_id = f"LP_{user_id}_{datetime.utcnow().timestamp()}"
        
        provider = LiquidityProvider(
            provider_id=provider_id,
            user_id=user_id,
            joined_at=datetime.utcnow()
        )
        
        self.providers[provider_id] = provider
        self.provider_activity[provider_id] = {
            "orders": deque(maxlen=1000),
            "spreads": deque(maxlen=1000),
            "uptime_windows": deque(maxlen=24)  # 24 hour windows
        }
        
        # Publish event
        await self.pulsar.publish('incentive.provider_registered', {
            'provider_id': provider_id,
            'user_id': user_id
        })
        
        return provider_id
        
    async def record_maker_order(self,
                                provider_id: str,
                                resource_type: str,
                                price: Decimal,
                                quantity: Decimal,
                                side: str) -> None:
        """Record a maker order for tracking"""
        if provider_id not in self.providers:
            return
            
        provider = self.providers[provider_id]
        provider.order_count += 1
        provider.last_activity = datetime.utcnow()
        
        # Track order details
        self.provider_activity[provider_id]["orders"].append({
            "timestamp": datetime.utcnow(),
            "resource_type": resource_type,
            "price": price,
            "quantity": quantity,
            "side": side
        })
        
        # Calculate spread contribution
        market_data = await self.spot_market.get_market_data(resource_type)
        if market_data:
            mid_price = (market_data["best_bid"] + market_data["best_ask"]) / 2
            spread_contribution = abs(price - mid_price) / mid_price
            
            self.provider_activity[provider_id]["spreads"].append(spread_contribution)
            
    async def record_filled_order(self,
                                 provider_id: str,
                                 resource_type: str,
                                 price: Decimal,
                                 quantity: Decimal,
                                 is_maker: bool) -> None:
        """Record a filled order for rewards calculation"""
        if provider_id not in self.providers:
            return
            
        provider = self.providers[provider_id]
        volume = price * quantity
        
        provider.total_volume += volume
        if is_maker:
            provider.maker_volume += volume
            provider.filled_orders += 1
        else:
            provider.taker_volume += volume
            
        # Update tier based on volume
        await self._update_provider_tier(provider_id)
        
        # Calculate immediate rewards
        for program in self.programs.values():
            if program.is_active and program.program_type == IncentiveType.MAKER_REBATE:
                await self._calculate_maker_rebate(provider_id, volume, program)
                
    async def stake_tokens(self,
                          provider_id: str,
                          amount: Decimal) -> bool:
        """Stake tokens for increased rewards"""
        if provider_id not in self.providers:
            return False
            
        provider = self.providers[provider_id]
        provider.staked_amount += amount
        
        # Calculate staking multiplier
        provider.staking_multiplier = self._calculate_staking_multiplier(provider.staked_amount)
        
        # Publish event
        await self.pulsar.publish('incentive.tokens_staked', {
            'provider_id': provider_id,
            'amount': float(amount),
            'new_multiplier': float(provider.staking_multiplier)
        })
        
        return True
        
    async def claim_rewards(self, provider_id: str) -> Decimal:
        """Claim pending rewards"""
        if provider_id not in self.providers:
            return Decimal("0")
            
        provider = self.providers[provider_id]
        rewards = provider.pending_rewards
        
        if rewards > 0:
            provider.pending_rewards = Decimal("0")
            provider.total_rewards_earned += rewards
            
            # Record distribution
            self.reward_history.append({
                "provider_id": provider_id,
                "amount": rewards,
                "timestamp": datetime.utcnow(),
                "tier": provider.current_tier.value
            })
            
            # Publish event
            await self.pulsar.publish('incentive.rewards_claimed', {
                'provider_id': provider_id,
                'amount': float(rewards),
                'total_earned': float(provider.total_rewards_earned)
            })
            
        return rewards
        
    async def get_provider_stats(self, provider_id: str) -> Dict[str, Any]:
        """Get comprehensive statistics for a liquidity provider"""
        if provider_id not in self.providers:
            raise ValueError(f"Provider {provider_id} not found")
            
        provider = self.providers[provider_id]
        activity = self.provider_activity[provider_id]
        
        # Calculate average metrics
        avg_spread = np.mean(list(activity["spreads"])) if activity["spreads"] else 0
        recent_uptime = self._calculate_recent_uptime(provider_id)
        
        return {
            "provider_id": provider_id,
            "user_id": provider.user_id,
            "joined_at": provider.joined_at.isoformat(),
            "current_tier": provider.current_tier.value,
            "metrics": {
                "total_volume": float(provider.total_volume),
                "maker_volume": float(provider.maker_volume),
                "taker_volume": float(provider.taker_volume),
                "maker_ratio": float(provider.maker_volume / provider.total_volume) if provider.total_volume > 0 else 0,
                "order_count": provider.order_count,
                "filled_orders": provider.filled_orders,
                "fill_rate": provider.filled_orders / provider.order_count if provider.order_count > 0 else 0,
                "average_spread": float(avg_spread),
                "uptime_24h": float(recent_uptime)
            },
            "rewards": {
                "total_earned": float(provider.total_rewards_earned),
                "pending": float(provider.pending_rewards),
                "by_type": {
                    incentive_type.value: float(amount)
                    for incentive_type, amount in provider.rewards_by_type.items()
                }
            },
            "staking": {
                "staked_amount": float(provider.staked_amount),
                "multiplier": float(provider.staking_multiplier)
            }
        }
        
    async def get_market_liquidity_score(self, resource_type: str) -> Dict[str, Any]:
        """Calculate overall liquidity score for a market"""
        recent_metrics = list(self.liquidity_metrics[resource_type])
        
        if not recent_metrics:
            return {"score": 0, "grade": "F"}
            
        # Calculate component scores
        depth_score = self._calculate_depth_score(recent_metrics)
        spread_score = self._calculate_spread_score(recent_metrics)
        activity_score = self._calculate_activity_score(recent_metrics)
        stability_score = self._calculate_stability_score(recent_metrics)
        
        # Weighted average
        total_score = (
            depth_score * Decimal("0.3") +
            spread_score * Decimal("0.3") +
            activity_score * Decimal("0.2") +
            stability_score * Decimal("0.2")
        )
        
        # Grade assignment
        if total_score >= 90:
            grade = "A"
        elif total_score >= 80:
            grade = "B"
        elif total_score >= 70:
            grade = "C"
        elif total_score >= 60:
            grade = "D"
        else:
            grade = "F"
            
        return {
            "score": float(total_score),
            "grade": grade,
            "components": {
                "depth": float(depth_score),
                "spread": float(spread_score),
                "activity": float(activity_score),
                "stability": float(stability_score)
            },
            "metrics": {
                "avg_depth": float(np.mean([m.total_depth for m in recent_metrics])),
                "avg_spread": float(np.mean([m.spread_percentage for m in recent_metrics])),
                "makers_count": recent_metrics[-1].makers_count if recent_metrics else 0,
                "volume_24h": float(recent_metrics[-1].volume_24h if recent_metrics else 0)
            }
        }
        
    # Internal methods
    
    async def _initialize_default_programs(self) -> None:
        """Initialize default incentive programs"""
        # Maker rebate program
        maker_program = IncentiveProgram(
            program_id="DEFAULT_MAKER_REBATE",
            name="Maker Rebate Program",
            program_type=IncentiveType.MAKER_REBATE,
            start_date=datetime.utcnow(),
            end_date=None,
            base_reward_rate=Decimal("0.0002"),  # 0.02% rebate
            max_reward_rate=Decimal("0.0005"),   # 0.05% max rebate
            reward_budget=Decimal("1000000"),    # 1M tokens
            reward_token="PLATFORM",
            tier_thresholds={
                RewardTier.BRONZE: {"volume": Decimal("10000")},
                RewardTier.SILVER: {"volume": Decimal("100000")},
                RewardTier.GOLD: {"volume": Decimal("1000000")},
                RewardTier.PLATINUM: {"volume": Decimal("10000000")},
                RewardTier.DIAMOND: {"volume": Decimal("100000000")}
            },
            tier_multipliers={
                RewardTier.BRONZE: Decimal("1.0"),
                RewardTier.SILVER: Decimal("1.5"),
                RewardTier.GOLD: Decimal("2.0"),
                RewardTier.PLATINUM: Decimal("2.5"),
                RewardTier.DIAMOND: Decimal("3.0")
            }
        )
        await self.create_program(maker_program)
        
        # Spread tightness program
        spread_program = IncentiveProgram(
            program_id="DEFAULT_SPREAD_REWARD",
            name="Tight Spread Rewards",
            program_type=IncentiveType.SPREAD_REWARD,
            start_date=datetime.utcnow(),
            end_date=None,
            base_reward_rate=Decimal("100"),    # 100 tokens per hour base
            max_reward_rate=Decimal("500"),      # 500 tokens per hour max
            reward_budget=Decimal("500000"),     # 500k tokens
            reward_token="PLATFORM",
            parameters={
                "target_spread": Decimal("0.001"),  # 0.1% target spread
                "max_spread": Decimal("0.005")      # 0.5% max qualifying spread
            }
        )
        await self.create_program(spread_program)
        
        # Uptime rewards program
        uptime_program = IncentiveProgram(
            program_id="DEFAULT_UPTIME_REWARD",
            name="Market Maker Uptime Rewards",
            program_type=IncentiveType.UPTIME_REWARD,
            start_date=datetime.utcnow(),
            end_date=None,
            base_reward_rate=Decimal("50"),     # 50 tokens per day base
            max_reward_rate=Decimal("200"),     # 200 tokens per day max
            reward_budget=Decimal("300000"),    # 300k tokens
            reward_token="PLATFORM",
            parameters={
                "min_uptime": Decimal("0.95"),   # 95% minimum uptime
                "check_interval": 300,            # Check every 5 minutes
                "min_orders": 10                  # Minimum 10 orders per interval
            }
        )
        await self.create_program(uptime_program)
        
    async def _monitor_providers(self) -> None:
        """Monitor provider activity and update metrics"""
        while True:
            try:
                for provider_id, provider in self.providers.items():
                    if not provider.is_active:
                        continue
                        
                    # Check uptime
                    await self._check_provider_uptime(provider_id)
                    
                    # Update average spread
                    activity = self.provider_activity[provider_id]
                    if activity["spreads"]:
                        provider.average_spread = Decimal(str(np.mean(list(activity["spreads"]))))
                        
                    # Check for inactivity
                    if datetime.utcnow() - provider.last_activity > timedelta(hours=24):
                        provider.is_active = False
                        logger.info(f"Provider {provider_id} marked inactive")
                        
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring providers: {e}")
                await asyncio.sleep(60)
                
    async def _distribute_rewards(self) -> None:
        """Periodically distribute rewards to providers"""
        while True:
            try:
                # Process each active program
                for program in self.programs.values():
                    if not program.is_active:
                        continue
                        
                    if program.program_type == IncentiveType.SPREAD_REWARD:
                        await self._distribute_spread_rewards(program)
                    elif program.program_type == IncentiveType.UPTIME_REWARD:
                        await self._distribute_uptime_rewards(program)
                    elif program.program_type == IncentiveType.VOLUME_REWARD:
                        await self._distribute_volume_rewards(program)
                        
                await asyncio.sleep(3600)  # Distribute hourly
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error distributing rewards: {e}")
                await asyncio.sleep(3600)
                
    async def _collect_metrics(self) -> None:
        """Collect real-time liquidity metrics"""
        while True:
            try:
                resource_types = await self._get_active_markets()
                
                for resource_type in resource_types:
                    metrics = await self._calculate_liquidity_metrics(resource_type)
                    self.liquidity_metrics[resource_type].append(metrics)
                    
                await asyncio.sleep(60)  # Collect every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error collecting metrics: {e}")
                await asyncio.sleep(60)
                
    async def _calculate_maker_rebate(self,
                                     provider_id: str,
                                     volume: Decimal,
                                     program: IncentiveProgram) -> None:
        """Calculate and add maker rebate rewards"""
        provider = self.providers[provider_id]
        
        # Base rebate rate
        rebate_rate = program.base_reward_rate
        
        # Apply tier multiplier
        tier_multiplier = program.tier_multipliers.get(provider.current_tier, Decimal("1"))
        rebate_rate *= tier_multiplier
        
        # Apply staking multiplier
        rebate_rate *= provider.staking_multiplier
        
        # Cap at max rate
        rebate_rate = min(rebate_rate, program.max_reward_rate)
        
        # Calculate reward
        reward = volume * rebate_rate
        
        # Check budget
        if program.total_distributed + reward <= program.reward_budget:
            provider.pending_rewards += reward
            provider.rewards_by_type[IncentiveType.MAKER_REBATE] = provider.rewards_by_type.get(
                IncentiveType.MAKER_REBATE, Decimal("0")
            ) + reward
            
            program.total_distributed += reward
            
    async def _distribute_spread_rewards(self, program: IncentiveProgram) -> None:
        """Distribute rewards for maintaining tight spreads"""
        target_spread = program.parameters["target_spread"]
        max_spread = program.parameters["max_spread"]
        
        for provider_id, provider in self.providers.items():
            if not provider.is_active:
                continue
                
            # Calculate average spread over last hour
            activity = self.provider_activity[provider_id]
            recent_spreads = list(activity["spreads"])[-60:]  # Last 60 entries
            
            if not recent_spreads:
                continue
                
            avg_spread = Decimal(str(np.mean(recent_spreads)))
            
            if avg_spread <= max_spread:
                # Calculate reward based on how tight the spread is
                spread_quality = (max_spread - avg_spread) / (max_spread - target_spread)
                spread_quality = max(Decimal("0"), min(spread_quality, Decimal("1")))
                
                # Base hourly reward
                hourly_reward = program.base_reward_rate
                
                # Apply quality multiplier
                hourly_reward *= spread_quality
                
                # Apply tier multiplier
                tier_multiplier = program.tier_multipliers.get(provider.current_tier, Decimal("1"))
                hourly_reward *= tier_multiplier
                
                # Apply staking multiplier
                hourly_reward *= provider.staking_multiplier
                
                # Cap at max rate
                hourly_reward = min(hourly_reward, program.max_reward_rate)
                
                # Check budget and distribute
                if program.total_distributed + hourly_reward <= program.reward_budget:
                    provider.pending_rewards += hourly_reward
                    provider.rewards_by_type[IncentiveType.SPREAD_REWARD] = provider.rewards_by_type.get(
                        IncentiveType.SPREAD_REWARD, Decimal("0")
                    ) + hourly_reward
                    
                    program.total_distributed += hourly_reward
                    
    async def _distribute_uptime_rewards(self, program: IncentiveProgram) -> None:
        """Distribute rewards for maintaining consistent uptime"""
        min_uptime = program.parameters["min_uptime"]
        min_orders = program.parameters["min_orders"]
        
        for provider_id, provider in self.providers.items():
            if not provider.is_active:
                continue
                
            # Calculate uptime over last 24 hours
            uptime = self._calculate_recent_uptime(provider_id)
            
            if uptime >= min_uptime:
                # Check order count
                activity = self.provider_activity[provider_id]
                recent_orders = [o for o in activity["orders"] 
                               if datetime.utcnow() - o["timestamp"] < timedelta(hours=24)]
                
                if len(recent_orders) >= min_orders * 24:  # Min orders per hour
                    # Calculate daily reward
                    daily_reward = program.base_reward_rate
                    
                    # Apply uptime quality bonus
                    uptime_bonus = (uptime - min_uptime) / (Decimal("1") - min_uptime)
                    daily_reward *= (Decimal("1") + uptime_bonus)
                    
                    # Apply tier multiplier
                    tier_multiplier = program.tier_multipliers.get(provider.current_tier, Decimal("1"))
                    daily_reward *= tier_multiplier
                    
                    # Apply staking multiplier
                    daily_reward *= provider.staking_multiplier
                    
                    # Cap at max rate
                    daily_reward = min(daily_reward, program.max_reward_rate)
                    
                    # Prorate to hourly
                    hourly_reward = daily_reward / 24
                    
                    # Check budget and distribute
                    if program.total_distributed + hourly_reward <= program.reward_budget:
                        provider.pending_rewards += hourly_reward
                        provider.rewards_by_type[IncentiveType.UPTIME_REWARD] = provider.rewards_by_type.get(
                            IncentiveType.UPTIME_REWARD, Decimal("0")
                        ) + hourly_reward
                        
                        program.total_distributed += hourly_reward
                        
    async def _distribute_volume_rewards(self, program: IncentiveProgram) -> None:
        """Distribute volume-based rewards"""
        # Calculate total volume across all providers
        total_volume = sum(p.maker_volume for p in self.providers.values() if p.is_active)
        
        if total_volume == 0:
            return
            
        # Distribute proportionally
        hourly_budget = program.reward_budget / (30 * 24)  # Monthly budget / hours
        
        for provider_id, provider in self.providers.items():
            if not provider.is_active or provider.maker_volume == 0:
                continue
                
            # Calculate share
            volume_share = provider.maker_volume / total_volume
            reward = hourly_budget * volume_share
            
            # Apply tier multiplier
            tier_multiplier = program.tier_multipliers.get(provider.current_tier, Decimal("1"))
            reward *= tier_multiplier
            
            # Apply staking multiplier
            reward *= provider.staking_multiplier
            
            # Check budget and distribute
            if program.total_distributed + reward <= program.reward_budget:
                provider.pending_rewards += reward
                provider.rewards_by_type[IncentiveType.VOLUME_REWARD] = provider.rewards_by_type.get(
                    IncentiveType.VOLUME_REWARD, Decimal("0")
                ) + reward
                
                program.total_distributed += reward
                
    async def _update_provider_tier(self, provider_id: str) -> None:
        """Update provider tier based on activity"""
        provider = self.providers[provider_id]
        
        # Find highest qualifying tier
        for program in self.programs.values():
            if not program.is_active:
                continue
                
            for tier in [RewardTier.DIAMOND, RewardTier.PLATINUM, RewardTier.GOLD, 
                        RewardTier.SILVER, RewardTier.BRONZE]:
                thresholds = program.tier_thresholds.get(tier, {})
                
                # Check volume threshold
                volume_threshold = thresholds.get("volume", Decimal("0"))
                if provider.total_volume >= volume_threshold:
                    if provider.current_tier != tier:
                        provider.current_tier = tier
                        
                        # Publish tier change event
                        await self.pulsar.publish('incentive.tier_changed', {
                            'provider_id': provider_id,
                            'new_tier': tier.value,
                            'total_volume': float(provider.total_volume)
                        })
                    break
                    
    async def _check_provider_uptime(self, provider_id: str) -> None:
        """Check if provider is maintaining uptime"""
        activity = self.provider_activity[provider_id]
        
        # Check if provider has recent orders
        recent_orders = [o for o in activity["orders"] 
                        if datetime.utcnow() - o["timestamp"] < timedelta(minutes=5)]
        
        # Mark this time window
        current_window = datetime.utcnow().replace(second=0, microsecond=0)
        activity["uptime_windows"].append({
            "window": current_window,
            "has_orders": len(recent_orders) > 0
        })
        
    def _calculate_recent_uptime(self, provider_id: str) -> Decimal:
        """Calculate uptime percentage over recent period"""
        activity = self.provider_activity[provider_id]
        windows = list(activity["uptime_windows"])
        
        if not windows:
            return Decimal("0")
            
        # Count active windows
        active_windows = sum(1 for w in windows if w["has_orders"])
        total_windows = len(windows)
        
        return Decimal(active_windows) / Decimal(total_windows)
        
    def _calculate_staking_multiplier(self, staked_amount: Decimal) -> Decimal:
        """Calculate reward multiplier based on staked amount"""
        # Logarithmic curve with diminishing returns
        if staked_amount <= 0:
            return Decimal("1.0")
            
        # 10k tokens = 1.1x, 100k = 1.5x, 1M = 2x
        multiplier = Decimal("1") + Decimal(str(np.log10(float(staked_amount) / 1000 + 1))) * Decimal("0.3")
        
        return min(multiplier, Decimal("3.0"))  # Cap at 3x
        
    async def _calculate_liquidity_metrics(self, resource_type: str) -> LiquidityMetrics:
        """Calculate current liquidity metrics for a market"""
        # Get order book
        order_book = await self.spot_market.get_order_book(resource_type)
        
        # Calculate depths
        bid_depth = sum(order["quantity"] * order["price"] for order in order_book.get("bids", []))
        ask_depth = sum(order["quantity"] * order["price"] for order in order_book.get("asks", []))
        
        # Get best prices
        best_bid = order_book["bids"][0]["price"] if order_book.get("bids") else Decimal("0")
        best_ask = order_book["asks"][0]["price"] if order_book.get("asks") else Decimal("0")
        
        # Calculate spread
        if best_bid > 0 and best_ask > 0:
            spread = best_ask - best_bid
            spread_percentage = spread / ((best_bid + best_ask) / 2)
        else:
            spread = Decimal("0")
            spread_percentage = Decimal("0")
            
        # Count active makers
        makers_count = len([p for p in self.providers.values() if p.is_active])
        
        # Get 24h volume
        market_data = await self.spot_market.get_market_data(resource_type)
        volume_24h = market_data.get("volume_24h", Decimal("0"))
        
        return LiquidityMetrics(
            resource_type=resource_type,
            timestamp=datetime.utcnow(),
            bid_depth=bid_depth,
            ask_depth=ask_depth,
            total_depth=bid_depth + ask_depth,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=spread,
            spread_percentage=spread_percentage,
            makers_count=makers_count,
            orders_count=len(order_book.get("bids", [])) + len(order_book.get("asks", [])),
            volume_24h=volume_24h,
            average_fill_rate=Decimal("0.95"),  # Placeholder
            average_fill_time=timedelta(seconds=2)  # Placeholder
        )
        
    def _calculate_depth_score(self, metrics: List[LiquidityMetrics]) -> Decimal:
        """Calculate depth score (0-100)"""
        if not metrics:
            return Decimal("0")
            
        avg_depth = np.mean([float(m.total_depth) for m in metrics])
        
        # Score based on depth relative to volume
        avg_volume = np.mean([float(m.volume_24h) for m in metrics])
        if avg_volume > 0:
            depth_ratio = avg_depth / avg_volume
            # Good liquidity = depth is at least 10% of daily volume
            score = min(Decimal(str(depth_ratio * 1000)), Decimal("100"))
        else:
            score = Decimal("0")
            
        return score
        
    def _calculate_spread_score(self, metrics: List[LiquidityMetrics]) -> Decimal:
        """Calculate spread score (0-100)"""
        if not metrics:
            return Decimal("0")
            
        avg_spread_pct = np.mean([float(m.spread_percentage) for m in metrics])
        
        # Lower spread = higher score
        # 0.1% spread = 100 score, 1% spread = 0 score
        score = max(Decimal("0"), Decimal("100") - Decimal(str(avg_spread_pct * 10000)))
        
        return score
        
    def _calculate_activity_score(self, metrics: List[LiquidityMetrics]) -> Decimal:
        """Calculate activity score (0-100)"""
        if not metrics:
            return Decimal("0")
            
        avg_makers = np.mean([m.makers_count for m in metrics])
        avg_orders = np.mean([m.orders_count for m in metrics])
        
        # Score based on number of makers and orders
        makers_score = min(Decimal(str(avg_makers * 10)), Decimal("50"))  # Max 50 points
        orders_score = min(Decimal(str(avg_orders)), Decimal("50"))  # Max 50 points
        
        return makers_score + orders_score
        
    def _calculate_stability_score(self, metrics: List[LiquidityMetrics]) -> Decimal:
        """Calculate stability score (0-100)"""
        if not metrics or len(metrics) < 2:
            return Decimal("0")
            
        # Calculate volatility of spread
        spreads = [float(m.spread_percentage) for m in metrics]
        spread_volatility = np.std(spreads)
        
        # Lower volatility = higher score
        score = max(Decimal("0"), Decimal("100") - Decimal(str(spread_volatility * 1000)))
        
        return score
        
    async def _get_active_markets(self) -> List[str]:
        """Get list of active markets"""
        # Simplified - would query from spot market
        return ["GPU", "TPU", "CPU", "MEMORY", "STORAGE"] 