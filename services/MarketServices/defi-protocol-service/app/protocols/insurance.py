"""
Insurance Protocol Implementation

Handles multi-tier insurance pools, liquidation coverage, and risk management.
Integrated from the standalone insurance-pool-service.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
import asyncio

from platformq_blockchain_common import (
    IBlockchainAdapter,
    Transaction,
    TransactionType,
    GasStrategy
)

from ..core.defi_manager import DeFiManager
from ..models.insurance import (
    PoolTier, StakePosition, DeficitEvent, 
    InsuranceClaim, RiskTier
)

logger = logging.getLogger(__name__)


class InsuranceProtocol:
    """
    Manages insurance pools and coverage for DeFi operations.
    
    Features:
    - Three-tier risk pools (Stable, Balanced, Aggressive)
    - Dynamic APY based on utilization
    - Waterfall loss distribution
    - Integration with lending liquidations
    - Automated claim processing
    """
    
    def __init__(self, 
                 defi_manager: DeFiManager,
                 lending_protocol: 'LendingProtocol' = None,
                 yield_protocol: 'YieldFarmingProtocol' = None):
        self.defi_manager = defi_manager
        self.lending_protocol = lending_protocol
        self.yield_protocol = yield_protocol
        
        self._insurance_contracts: Dict[str, str] = {}  # chain -> contract
        self._pool_state: Dict[str, Dict[RiskTier, Decimal]] = {}  # chain -> tier -> balance
        self._stake_positions: Dict[str, List[StakePosition]] = {}  # user -> positions
        
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
                "coverage_ratio": Decimal("0.2")  # Covers 20% of position
            },
            RiskTier.BALANCED: {
                "name": "Balanced Pool",
                "supported_markets": ["all_crypto", "stocks", "indices"],
                "max_leverage_covered": 50,
                "base_apy": Decimal("0.12"),  # 12% base APY
                "risk_multiplier": Decimal("2.0"),
                "loss_priority": 2,  # Second to take losses
                "min_stake": Decimal("1000"),  # $1000 minimum
                "coverage_ratio": Decimal("0.5")  # Covers 50% of position
            },
            RiskTier.AGGRESSIVE: {
                "name": "Aggressive Pool",
                "supported_markets": ["all"],  # Covers everything
                "max_leverage_covered": 100,
                "base_apy": Decimal("0.25"),  # 25% base APY
                "risk_multiplier": Decimal("5.0"),
                "loss_priority": 1,  # First to take losses
                "min_stake": Decimal("10000"),  # $10K minimum
                "coverage_ratio": Decimal("1.0")  # Full coverage
            }
        }
        
        # Dynamic APY curve based on utilization
        self.utilization_curve = {
            Decimal("0"): Decimal("0.5"),    # 50% of base APY at 0%
            Decimal("0.5"): Decimal("1.0"),   # 100% of base APY at 50%
            Decimal("0.8"): Decimal("2.0"),   # 200% of base APY at 80%
            Decimal("0.95"): Decimal("5.0"),  # 500% of base APY at 95%
        }
        
    async def initialize(self):
        """Initialize insurance protocol contracts"""
        logger.info("Initializing Insurance Protocol")
        
        # Load insurance contract addresses for each chain
        for chain_type in self.defi_manager.get_supported_chains():
            chain = chain_type.value
            self._insurance_contracts[chain] = await self._get_contract_address(
                chain_type, "InsurancePool"
            )
            
            # Initialize pool state
            self._pool_state[chain] = {
                RiskTier.STABLE: Decimal("0"),
                RiskTier.BALANCED: Decimal("0"),
                RiskTier.AGGRESSIVE: Decimal("0")
            }
            
        # Start monitoring tasks
        asyncio.create_task(self._monitor_covered_positions())
        asyncio.create_task(self._distribute_rewards())
        
    async def shutdown(self):
        """Shutdown insurance protocol"""
        logger.info("Shutting down Insurance Protocol")
        self._stake_positions.clear()
        
    async def stake_liquidity(
        self,
        chain: str,
        user: str,
        amount: Decimal,
        tier: RiskTier,
        lock_period_days: int = 0
    ) -> Dict[str, Any]:
        """
        Stake liquidity in insurance pool for rewards.
        
        Args:
            chain: Blockchain identifier
            user: User's wallet address
            amount: Amount to stake
            tier: Risk tier to stake in
            lock_period_days: Optional lock period for bonus APY
            
        Returns:
            Transaction result with stake position
        """
        try:
            tier_config = self.tiers[tier]
            
            # Validate minimum stake
            if amount < tier_config["min_stake"]:
                raise ValueError(f"Minimum stake is {tier_config['min_stake']}")
                
            async with self.defi_manager.get_adapter(chain) as adapter:
                contract_address = self._insurance_contracts[chain]
                
                # Calculate lock bonus
                lock_bonus = self._calculate_lock_bonus(lock_period_days)
                
                # Prepare stake transaction
                params = [
                    tier.value,
                    int(amount * 10**18),
                    lock_period_days * 86400  # Convert to seconds
                ]
                
                tx = await self._prepare_transaction(
                    adapter,
                    user,
                    contract_address,
                    "stakeLiquidity",
                    params
                )
                
                # Send transaction
                result = await adapter.send_transaction(tx)
                
                # Create stake position
                position = StakePosition(
                    id=f"stake_{result.transaction_hash[:8]}",
                    user_id=user,
                    chain=chain,
                    tier=tier,
                    amount=amount,
                    staked_at=datetime.utcnow(),
                    lock_until=datetime.utcnow() + timedelta(days=lock_period_days) if lock_period_days > 0 else None,
                    base_apy=tier_config["base_apy"],
                    lock_bonus=lock_bonus,
                    rewards_earned=Decimal("0"),
                    last_reward_claim=datetime.utcnow()
                )
                
                # Store position
                if user not in self._stake_positions:
                    self._stake_positions[user] = []
                self._stake_positions[user].append(position)
                
                # Update pool balance
                self._pool_state[chain][tier] += amount
                
                # If integrated with yield farming, create a yield position too
                if self.yield_protocol:
                    await self.yield_protocol.stake_tokens(
                        chain=chain,
                        pool_id=f"insurance_{tier.value}",
                        amount=amount,
                        staker=user
                    )
                
                logger.info(f"Staked {amount} in {tier.value} pool on {chain}")
                
                return {
                    "position_id": position.id,
                    "tx_hash": result.transaction_hash,
                    "gas_used": str(result.gas_used),
                    "apy": str(tier_config["base_apy"] + lock_bonus),
                    "lock_until": position.lock_until.isoformat() if position.lock_until else None
                }
                
        except Exception as e:
            logger.error(f"Error staking liquidity: {e}")
            raise
            
    async def cover_liquidation_loss(
        self,
        chain: str,
        loan_id: str,
        loss_amount: Decimal,
        liquidation_details: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Cover losses from loan liquidations using insurance pools.
        
        Integrates with lending protocol to automatically cover bad debt.
        
        Args:
            chain: Blockchain identifier
            loan_id: Loan that was liquidated
            loss_amount: Amount of loss to cover
            liquidation_details: Details about the liquidation
            
        Returns:
            Coverage result with distribution details
        """
        try:
            # Determine which pools cover this liquidation
            market_type = liquidation_details.get("market_type", "crypto")
            leverage = liquidation_details.get("leverage", 1)
            
            covering_pools = self._get_covering_pools(market_type, leverage)
            
            if not covering_pools:
                raise ValueError("No insurance pools cover this market/leverage")
                
            # Sort by loss priority (aggressive first)
            covering_pools.sort(key=lambda p: self.tiers[p]["loss_priority"])
            
            remaining_loss = loss_amount
            loss_distribution = {}
            
            async with self.defi_manager.get_adapter(chain) as adapter:
                contract_address = self._insurance_contracts[chain]
                
                # Waterfall loss distribution
                for pool_tier in covering_pools:
                    if remaining_loss <= 0:
                        break
                        
                    pool_balance = self._pool_state[chain][pool_tier]
                    coverage_ratio = self.tiers[pool_tier]["coverage_ratio"]
                    max_coverage = loss_amount * coverage_ratio
                    
                    pool_share = min(remaining_loss, pool_balance, max_coverage)
                    
                    if pool_share > 0:
                        # Process coverage on-chain
                        params = [
                            loan_id,
                            pool_tier.value,
                            int(pool_share * 10**18)
                        ]
                        
                        tx = await self._prepare_transaction(
                            adapter,
                            contract_address,  # Contract initiates
                            contract_address,
                            "processLiquidationCoverage",
                            params
                        )
                        
                        result = await adapter.send_transaction(tx)
                        
                        # Update state
                        self._pool_state[chain][pool_tier] -= pool_share
                        remaining_loss -= pool_share
                        loss_distribution[pool_tier] = pool_share
                        
                        # Distribute loss to stakers
                        await self._distribute_loss_to_stakers(chain, pool_tier, pool_share)
                        
                # Handle deficit if any
                if remaining_loss > 0:
                    await self._handle_deficit_event(
                        chain, loan_id, remaining_loss, loss_distribution
                    )
                    
                # Notify lending protocol of coverage
                if self.lending_protocol:
                    await self.lending_protocol.record_insurance_coverage(
                        chain, loan_id, loss_amount - remaining_loss
                    )
                    
                logger.info(f"Covered {loss_amount - remaining_loss} of {loss_amount} loss")
                
                return {
                    "loan_id": loan_id,
                    "total_loss": str(loss_amount),
                    "amount_covered": str(loss_amount - remaining_loss),
                    "deficit": str(remaining_loss),
                    "distribution": {
                        tier.value: str(amount) for tier, amount in loss_distribution.items()
                    },
                    "status": "partial" if remaining_loss > 0 else "full"
                }
                
        except Exception as e:
            logger.error(f"Error covering liquidation loss: {e}")
            raise
            
    async def claim_rewards(
        self,
        chain: str,
        user: str,
        position_id: str
    ) -> Dict[str, Any]:
        """
        Claim accumulated staking rewards.
        
        Args:
            chain: Blockchain identifier
            user: User's wallet address
            position_id: Stake position ID
            
        Returns:
            Transaction result with claimed amount
        """
        try:
            # Find position
            user_positions = self._stake_positions.get(user, [])
            position = next((p for p in user_positions if p.id == position_id), None)
            
            if not position:
                raise ValueError("Position not found")
                
            if position.chain != chain:
                raise ValueError("Position not on specified chain")
                
            # Calculate rewards
            time_staked = datetime.utcnow() - position.last_reward_claim
            days_staked = Decimal(time_staked.total_seconds()) / Decimal("86400")
            
            # Get current APY
            current_apy = await self.calculate_current_apy(chain, position.tier)
            total_apy = current_apy + position.lock_bonus
            
            # Calculate rewards
            rewards = position.amount * total_apy * days_staked / Decimal("365")
            
            async with self.defi_manager.get_adapter(chain) as adapter:
                contract_address = self._insurance_contracts[chain]
                
                # Claim rewards transaction
                params = [position_id]
                
                tx = await self._prepare_transaction(
                    adapter,
                    user,
                    contract_address,
                    "claimRewards",
                    params
                )
                
                result = await adapter.send_transaction(tx)
                
                # Update position
                position.rewards_earned += rewards
                position.last_reward_claim = datetime.utcnow()
                
                logger.info(f"Claimed {rewards} rewards for position {position_id}")
                
                return {
                    "tx_hash": result.transaction_hash,
                    "gas_used": str(result.gas_used),
                    "rewards_claimed": str(rewards),
                    "total_rewards": str(position.rewards_earned)
                }
                
        except Exception as e:
            logger.error(f"Error claiming rewards: {e}")
            raise
            
    async def calculate_current_apy(
        self,
        chain: str,
        tier: RiskTier
    ) -> Decimal:
        """
        Calculate current APY based on pool utilization.
        
        Args:
            chain: Blockchain identifier
            tier: Risk tier
            
        Returns:
            Current APY as decimal
        """
        tier_config = self.tiers[tier]
        base_apy = tier_config["base_apy"]
        
        # Calculate utilization
        utilization = await self._calculate_pool_utilization(chain, tier)
        
        # Apply utilization curve
        apy_multiplier = self._interpolate_utilization_curve(utilization)
        
        # Get risk premium
        risk_premium = await self._calculate_risk_premium(chain, tier)
        
        current_apy = base_apy * apy_multiplier * (Decimal("1") + risk_premium)
        
        return current_apy
        
    async def get_pool_stats(self, chain: Optional[str] = None) -> Dict[str, Any]:
        """Get comprehensive pool statistics"""
        stats = {}
        
        chains = [chain] if chain else self._pool_state.keys()
        
        for chain_id in chains:
            chain_stats = {}
            
            for tier in RiskTier:
                balance = self._pool_state[chain_id][tier]
                
                tier_stats = {
                    "balance": str(balance),
                    "current_apy": str(await self.calculate_current_apy(chain_id, tier)),
                    "utilization": str(await self._calculate_pool_utilization(chain_id, tier)),
                    "stakers": len([p for p in self._get_all_positions() 
                                  if p.chain == chain_id and p.tier == tier]),
                    "coverage_active": str(await self._get_active_coverage_value(chain_id, tier))
                }
                chain_stats[tier.value] = tier_stats
                
            chain_stats["total_tvl"] = str(
                sum(self._pool_state[chain_id].values())
            )
            stats[chain_id] = chain_stats
            
        return stats
        
    def _calculate_lock_bonus(self, lock_days: int) -> Decimal:
        """Calculate bonus APY for locking stake"""
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
            
    def _get_covering_pools(
        self,
        market_type: str,
        leverage: int
    ) -> List[RiskTier]:
        """Determine which pools cover a market/leverage combination"""
        covering_pools = []
        
        for tier, config in self.tiers.items():
            # Check market support
            if "all" in config["supported_markets"] or market_type in config["supported_markets"]:
                # Check leverage support
                if leverage <= config["max_leverage_covered"]:
                    covering_pools.append(tier)
                    
        return covering_pools
        
    async def _distribute_loss_to_stakers(
        self,
        chain: str,
        tier: RiskTier,
        loss_amount: Decimal
    ):
        """Distribute losses proportionally to stakers"""
        # Get all positions for this tier
        tier_positions = [
            p for p in self._get_all_positions()
            if p.chain == chain and p.tier == tier
        ]
        
        if not tier_positions:
            return
            
        total_staked = sum(p.amount for p in tier_positions)
        
        # Distribute proportionally
        for position in tier_positions:
            position_loss = (position.amount / total_staked) * loss_amount
            position.amount -= position_loss
            
            # Record loss event
            logger.info(f"Position {position.id} took {position_loss} loss")
            
    async def _handle_deficit_event(
        self,
        chain: str,
        loan_id: str,
        deficit: Decimal,
        partial_coverage: Dict[RiskTier, Decimal]
    ):
        """Handle deficit when pools can't cover full loss"""
        logger.warning(f"Deficit event: {deficit} uncovered for loan {loan_id}")
        
        # Record deficit event
        # In production, this would trigger additional mechanisms
        # like minting deficit tokens or initiating recovery
        
    async def _calculate_pool_utilization(
        self,
        chain: str,
        tier: RiskTier
    ) -> Decimal:
        """Calculate pool utilization ratio"""
        pool_balance = self._pool_state[chain][tier]
        
        if pool_balance == 0:
            return Decimal("1")  # 100% utilized if no balance
            
        # Get covered value
        covered_value = await self._get_active_coverage_value(chain, tier)
        
        utilization = covered_value / pool_balance
        return min(utilization, Decimal("1"))
        
    async def _get_active_coverage_value(
        self,
        chain: str,
        tier: RiskTier
    ) -> Decimal:
        """Calculate total value currently being covered"""
        # This would query active loans and positions
        # For now, return mock value
        return self._pool_state[chain][tier] * Decimal("0.7")
        
    def _interpolate_utilization_curve(self, utilization: Decimal) -> Decimal:
        """Interpolate APY multiplier from utilization curve"""
        points = sorted(self.utilization_curve.items())
        
        for i in range(len(points) - 1):
            x1, y1 = points[i]
            x2, y2 = points[i + 1]
            
            if x1 <= utilization <= x2:
                # Linear interpolation
                slope = (y2 - y1) / (x2 - x1)
                return y1 + slope * (utilization - x1)
                
        # Beyond last point
        return points[-1][1]
        
    async def _calculate_risk_premium(
        self,
        chain: str,
        tier: RiskTier
    ) -> Decimal:
        """Calculate additional risk premium"""
        # This would calculate based on recent claims
        # For now, return small premium
        return Decimal("0.05")  # 5% premium
        
    def _get_all_positions(self) -> List[StakePosition]:
        """Get all stake positions"""
        all_positions = []
        for positions in self._stake_positions.values():
            all_positions.extend(positions)
        return all_positions
        
    async def _monitor_covered_positions(self):
        """Monitor positions covered by insurance"""
        while True:
            try:
                # Monitor lending positions if integrated
                if self.lending_protocol:
                    # Check for at-risk positions
                    # This would integrate with lending protocol
                    pass
                    
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error monitoring positions: {e}")
                await asyncio.sleep(60)
                
    async def _distribute_rewards(self):
        """Background task to distribute rewards"""
        while True:
            try:
                # Auto-compound rewards for locked positions
                # This would process reward distributions
                
                await asyncio.sleep(3600)  # Every hour
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error distributing rewards: {e}")
                await asyncio.sleep(3600)
                
    async def _prepare_transaction(
        self,
        adapter: IBlockchainAdapter,
        from_address: str,
        to_address: str,
        method: str,
        params: List[Any],
        value: Decimal = Decimal("0")
    ) -> Transaction:
        """Prepare a transaction for insurance contract interaction"""
        return Transaction(
            from_address=from_address,
            to_address=to_address,
            value=value,
            data=f"{method}({params})".encode(),
            type=TransactionType.CONTRACT_CALL,
            gas_strategy=GasStrategy.STANDARD
        )
        
    async def _get_contract_address(
        self,
        chain_type,
        contract_name: str
    ) -> str:
        """Get deployed contract address for chain"""
        return f"0x{hash(f'{chain_type.value}_{contract_name}') % 16**40:040x}"
    
    async def get_position(self, position_id: str, user: str) -> Optional[StakePosition]:
        """Get a specific stake position"""
        positions = self._stake_positions.get(user, [])
        return next((p for p in positions if p.id == position_id), None)
    
    async def unstake_liquidity(
        self,
        chain: str,
        user: str,
        position_id: str,
        amount: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """Unstake liquidity from insurance pool"""
        position = await self.get_position(position_id, user)
        if not position:
            raise ValueError("Position not found")
            
        if position.is_locked:
            raise ValueError("Position is locked")
            
        unstake_amount = amount or position.amount
        if unstake_amount > position.amount:
            raise ValueError("Unstake amount exceeds position")
            
        # Update position
        position.amount -= unstake_amount
        if position.amount == 0:
            position.is_active = False
            
        # Update pool balance
        self._pool_state[chain][position.tier] -= unstake_amount
        
        return {
            "amount_unstaked": str(unstake_amount),
            "remaining_stake": str(position.amount),
            "rewards_claimed": str(position.rewards_earned)
        }
    
    async def submit_claim(
        self,
        chain: str,
        claimant: str,
        claim_type: str,
        reference_id: str,
        amount: Decimal,
        evidence: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Submit an insurance claim"""
        from ..models.insurance import InsuranceClaim, ClaimStatus
        
        claim = InsuranceClaim(
            id=f"claim_{reference_id[:8]}_{int(datetime.utcnow().timestamp())}",
            chain=chain,
            claimant=claimant,
            claim_type=claim_type,
            reference_id=reference_id,
            amount_claimed=amount,
            amount_approved=None,
            status=ClaimStatus.PENDING,
            submitted_at=datetime.utcnow(),
            processed_at=None,
            evidence=evidence
        )
        
        # For liquidation claims, process automatically
        if claim_type == "liquidation":
            # Auto-approve liquidation claims
            claim.status = ClaimStatus.APPROVED
            claim.amount_approved = amount
            claim.processed_at = datetime.utcnow()
            
        return {
            "claim_id": claim.id,
            "status": claim.status.value
        }
    
    async def get_user_positions(
        self,
        user: str,
        chain: Optional[str] = None
    ) -> List[StakePosition]:
        """Get all positions for a user"""
        positions = self._stake_positions.get(user, [])
        if chain:
            positions = [p for p in positions if p.chain == chain]
        return positions
    
    async def get_claim(self, claim_id: str) -> Optional[InsuranceClaim]:
        """Get claim details"""
        # In production, this would query from database
        # For now, return None
        return None
    
    async def get_available_coverage(
        self,
        chain: str,
        market_type: str,
        leverage: int
    ) -> Dict[RiskTier, Decimal]:
        """Get available coverage for each tier"""
        covering_pools = self._get_covering_pools(market_type, leverage)
        
        coverage = {}
        for tier in covering_pools:
            pool_balance = self._pool_state[chain][tier]
            coverage_ratio = self.tiers[tier]["coverage_ratio"]
            coverage[tier] = pool_balance * coverage_ratio
            
        return coverage 