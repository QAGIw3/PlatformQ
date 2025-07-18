from decimal import Decimal
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from enum import Enum
import asyncio

from app.models.insurance import PoolTier, StakePosition, DeficitEvent

class RiskTier(Enum):
    STABLE = "stable"      # Low risk, low reward
    BALANCED = "balanced"  # Medium risk, medium reward  
    AGGRESSIVE = "aggressive"  # High risk, high reward

class MultiTierInsurancePool:
    """
    Three-tier insurance pool system for optimal risk/reward
    Most advantageous: Allows users to choose their risk appetite
    """
    
    def __init__(self, ignite_client, pulsar_client):
        self.ignite = ignite_client
        self.pulsar = pulsar_client
        
        # Pool configurations
        self.tiers = {
            RiskTier.STABLE: {
                "name": "Stable Pool",
                "supported_markets": ["major_crypto", "forex", "commodities"],
                "max_leverage_covered": 10,
                "base_apy": Decimal("0.05"),  # 5% base APY
                "risk_multiplier": Decimal("1.0"),
                "loss_priority": 3,  # Last to take losses
                "min_stake": Decimal("100"),  # $100 minimum
            },
            RiskTier.BALANCED: {
                "name": "Balanced Pool",
                "supported_markets": ["all_crypto", "stocks", "indices"],
                "max_leverage_covered": 50,
                "base_apy": Decimal("0.12"),  # 12% base APY
                "risk_multiplier": Decimal("2.0"),
                "loss_priority": 2,  # Second to take losses
                "min_stake": Decimal("1000"),  # $1000 minimum
            },
            RiskTier.AGGRESSIVE: {
                "name": "Aggressive Pool",
                "supported_markets": ["all"],  # Covers everything including exotics
                "max_leverage_covered": 100,
                "base_apy": Decimal("0.25"),  # 25% base APY
                "risk_multiplier": Decimal("5.0"),
                "loss_priority": 1,  # First to take losses
                "min_stake": Decimal("10000"),  # $10K minimum
            }
        }
        
        # Pool state
        self.pool_balances = {
            RiskTier.STABLE: Decimal("0"),
            RiskTier.BALANCED: Decimal("0"),
            RiskTier.AGGRESSIVE: Decimal("0")
        }
        
        # Dynamic APY adjustments based on pool utilization
        self.utilization_curve = {
            Decimal("0"): Decimal("0.5"),    # 50% of base APY at 0% utilization
            Decimal("0.5"): Decimal("1.0"),   # 100% of base APY at 50% utilization
            Decimal("0.8"): Decimal("2.0"),   # 200% of base APY at 80% utilization
            Decimal("0.95"): Decimal("5.0"),  # 500% of base APY at 95% utilization
        }
        
    async def stake_liquidity(
        self,
        user_id: str,
        amount: Decimal,
        tier: RiskTier,
        lock_period_days: int = 0
    ) -> StakePosition:
        """
        Stake liquidity in insurance pool
        """
        tier_config = self.tiers[tier]
        
        # Validate minimum stake
        if amount < tier_config["min_stake"]:
            raise ValueError(f"Minimum stake is {tier_config['min_stake']}")
        
        # Calculate bonus for lock period
        lock_bonus = self._calculate_lock_bonus(lock_period_days)
        
        # Create stake position
        position = StakePosition(
            id=self._generate_position_id(),
            user_id=user_id,
            tier=tier,
            amount=amount,
            staked_at=datetime.utcnow(),
            lock_until=datetime.utcnow() + timedelta(days=lock_period_days) if lock_period_days > 0 else None,
            base_apy=tier_config["base_apy"],
            lock_bonus=lock_bonus,
            rewards_earned=Decimal("0"),
            last_reward_claim=datetime.utcnow()
        )
        
        # Update pool balance
        self.pool_balances[tier] += amount
        
        # Store position
        await self._store_stake_position(position)
        
        # Emit staking event
        await self._emit_staking_event(position, "staked")
        
        # Update pool metrics
        await self._update_pool_metrics(tier)
        
        return position
    
    async def cover_liquidation_loss(
        self,
        market_id: str,
        loss_amount: Decimal,
        liquidation_details: Dict
    ) -> Dict:
        """
        Cover losses from liquidations using waterfall approach
        """
        # Determine which pools cover this market
        covering_pools = self._get_covering_pools(market_id, liquidation_details["leverage"])
        
        if not covering_pools:
            raise ValueError("No insurance pools cover this market/leverage combination")
        
        # Sort by loss priority (aggressive first)
        covering_pools.sort(key=lambda p: self.tiers[p]["loss_priority"])
        
        remaining_loss = loss_amount
        loss_distribution = {}
        
        # Waterfall loss distribution
        for pool_tier in covering_pools:
            if remaining_loss <= 0:
                break
            
            pool_balance = self.pool_balances[pool_tier]
            pool_share = min(remaining_loss, pool_balance)
            
            if pool_share > 0:
                # Deduct from pool
                self.pool_balances[pool_tier] -= pool_share
                remaining_loss -= pool_share
                loss_distribution[pool_tier] = pool_share
                
                # Update staker positions proportionally
                await self._distribute_loss_to_stakers(pool_tier, pool_share)
        
        # If still loss remaining, it's a deficit event
        if remaining_loss > 0:
            await self._handle_deficit_event(remaining_loss, loss_distribution)
        
        # Record loss event
        await self._record_loss_event({
            "market_id": market_id,
            "total_loss": loss_amount,
            "loss_distribution": loss_distribution,
            "deficit": remaining_loss,
            "timestamp": datetime.utcnow()
        })
        
        return {
            "loss_covered": loss_amount - remaining_loss,
            "deficit": remaining_loss,
            "distribution": loss_distribution
        }
    
    async def calculate_current_apy(self, tier: RiskTier) -> Decimal:
        """
        Calculate current APY based on pool utilization
        """
        tier_config = self.tiers[tier]
        base_apy = tier_config["base_apy"]
        
        # Get utilization rate
        utilization = await self._calculate_pool_utilization(tier)
        
        # Apply utilization curve
        apy_multiplier = self._interpolate_utilization_curve(utilization)
        
        # Additional multipliers
        risk_premium = await self._calculate_risk_premium(tier)
        
        current_apy = base_apy * apy_multiplier * (Decimal("1") + risk_premium)
        
        return current_apy
    
    def _calculate_lock_bonus(self, lock_days: int) -> Decimal:
        """
        Calculate bonus APY for locking stake
        """
        if lock_days == 0:
            return Decimal("0")
        elif lock_days <= 30:
            return Decimal("0.01")  # +1%
        elif lock_days <= 90:
            return Decimal("0.03")  # +3%
        elif lock_days <= 180:
            return Decimal("0.05")  # +5%
        elif lock_days <= 365:
            return Decimal("0.10")  # +10%
        else:
            return Decimal("0.15")  # +15%
    
    def _get_covering_pools(self, market_id: str, leverage: int) -> List[RiskTier]:
        """
        Determine which pools cover a market/leverage combination
        """
        covering_pools = []
        
        market_type = self._get_market_type(market_id)
        
        for tier, config in self.tiers.items():
            # Check market support
            if "all" in config["supported_markets"] or market_type in config["supported_markets"]:
                # Check leverage support
                if leverage <= config["max_leverage_covered"]:
                    covering_pools.append(tier)
        
        return covering_pools
    
    async def _distribute_loss_to_stakers(self, tier: RiskTier, loss_amount: Decimal):
        """
        Distribute losses proportionally to all stakers in tier
        """
        # Get all active positions in tier
        positions = await self._get_active_positions(tier)
        
        total_staked = sum(p.amount for p in positions)
        
        if total_staked == 0:
            return
        
        # Distribute loss proportionally
        for position in positions:
            position_loss = (position.amount / total_staked) * loss_amount
            position.amount -= position_loss
            
            # Record loss
            await self._record_position_loss(position.id, position_loss)
            
            # Notify staker
            await self._notify_staker_loss(position.user_id, position_loss, tier)
    
    async def _calculate_pool_utilization(self, tier: RiskTier) -> Decimal:
        """
        Calculate what percentage of pool is being used for coverage
        """
        pool_balance = self.pool_balances[tier]
        
        # Get total potential liabilities
        covered_positions = await self._get_covered_positions(tier)
        
        potential_liability = Decimal("0")
        for position in covered_positions:
            # Calculate worst-case loss
            max_loss = position.size * position.liquidation_price * Decimal("0.1")  # 10% slippage
            potential_liability += max_loss
        
        if pool_balance == 0:
            return Decimal("1")  # 100% utilized if no balance
        
        utilization = potential_liability / pool_balance
        
        return min(utilization, Decimal("1"))  # Cap at 100%
    
    def _interpolate_utilization_curve(self, utilization: Decimal) -> Decimal:
        """
        Interpolate APY multiplier from utilization curve
        """
        points = sorted(self.utilization_curve.items())
        
        # Find surrounding points
        for i in range(len(points) - 1):
            x1, y1 = points[i]
            x2, y2 = points[i + 1]
            
            if x1 <= utilization <= x2:
                # Linear interpolation
                slope = (y2 - y1) / (x2 - x1)
                return y1 + slope * (utilization - x1)
        
        # If beyond last point, use last value
        return points[-1][1]
    
    async def claim_rewards(self, user_id: str, position_id: str) -> Decimal:
        """
        Claim accumulated staking rewards
        """
        position = await self._get_stake_position(position_id)
        
        if position.user_id != user_id:
            raise ValueError("Not position owner")
        
        # Calculate rewards
        time_staked = datetime.utcnow() - position.last_reward_claim
        days_staked = Decimal(time_staked.total_seconds()) / Decimal("86400")
        
        # Get current APY
        current_apy = await self.calculate_current_apy(position.tier)
        total_apy = current_apy + position.lock_bonus
        
        # Calculate rewards
        rewards = position.amount * total_apy * days_staked / Decimal("365")
        
        # Update position
        position.rewards_earned += rewards
        position.last_reward_claim = datetime.utcnow()
        
        await self._update_stake_position(position)
        
        # Transfer rewards
        await self._transfer_rewards(user_id, rewards)
        
        return rewards
    
    async def get_pool_stats(self) -> Dict:
        """
        Get comprehensive pool statistics
        """
        stats = {}
        
        for tier in RiskTier:
            tier_stats = {
                "balance": self.pool_balances[tier],
                "stakers": await self._count_stakers(tier),
                "current_apy": await self.calculate_current_apy(tier),
                "utilization": await self._calculate_pool_utilization(tier),
                "total_losses_covered": await self._get_total_losses_covered(tier),
                "active_coverage": await self._get_active_coverage_value(tier)
            }
            stats[tier.value] = tier_stats
        
        stats["total_tvl"] = sum(self.pool_balances.values())
        
        return stats 