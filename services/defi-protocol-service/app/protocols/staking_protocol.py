"""
Staking Protocol

Manages resource token staking, delegation pools, and auto-compounding.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum

from web3 import Web3
from eth_account import Account
from platformq_shared.blockchain import BlockchainClient
from platformq_shared.models import ResourceType, ServiceTier
from ..models import (
    StakingPool, DelegationPool, UserStake, StakeStatus,
    StakingStats, DelegationPoolInfo
)
from ..contracts import ResourceStakingContract

logger = logging.getLogger(__name__)


class StakingProtocol:
    """Protocol for managing resource token staking and delegation"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        staking_contract_address: str,
        resource_token_address: str
    ):
        self.blockchain = blockchain_client
        self.staking_address = staking_contract_address
        self.resource_token_address = resource_token_address
        
        # Contract interfaces
        self.staking_contract = None
        self.resource_token_contract = None
        
        # Cache for pool data
        self._staking_pools = {}
        self._delegation_pools = {}
        self._user_stakes = {}
        
        # Auto-compound tracking
        self._compound_queue = asyncio.Queue()
        self._compound_task = None
        
    async def initialize(self):
        """Initialize the staking protocol"""
        # Load contract interfaces
        self.staking_contract = await self.blockchain.get_contract(
            self.staking_address,
            "ResourceStaking"
        )
        
        self.resource_token_contract = await self.blockchain.get_contract(
            self.resource_token_address,
            "ResourceToken"
        )
        
        # Start monitoring tasks
        asyncio.create_task(self._monitor_stakes())
        asyncio.create_task(self._monitor_rewards())
        self._compound_task = asyncio.create_task(self._auto_compound_worker())
        
        logger.info("Staking Protocol initialized")
        
    async def create_staking_pool(
        self,
        token_id: int,
        min_stake_amount: int,
        is_lp: bool = False,
        lp_token_address: Optional[str] = None,
        operator_address: str = None
    ) -> Dict[str, Any]:
        """
        Create a new staking pool
        
        Args:
            token_id: Resource token ID (0 for LP tokens)
            min_stake_amount: Minimum stake amount
            is_lp: Whether this is an LP token pool
            lp_token_address: LP token address if is_lp
            operator_address: Operator who can manage the pool
            
        Returns:
            Pool creation result
        """
        try:
            # Validate inputs
            if is_lp and not lp_token_address:
                raise ValueError("LP token address required for LP pools")
                
            # Create pool transaction
            tx = await self.staking_contract.functions.createStakingPool(
                token_id,
                min_stake_amount,
                is_lp,
                lp_token_address or "0x0000000000000000000000000000000000000000"
            ).transact({"from": operator_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Extract pool ID from events
            pool_id = None
            for log in receipt.logs:
                if log.topics[0] == self.staking_contract.events.PoolCreated.topic:
                    pool_id = int(log.data[0])
                    break
                    
            if pool_id is None:
                raise ValueError("Pool creation failed - no pool ID in events")
                
            # Cache pool data
            pool = StakingPool(
                pool_id=pool_id,
                token_id=token_id,
                total_staked=0,
                reward_per_token=0,
                min_stake_amount=min_stake_amount,
                is_lp=is_lp,
                lp_token_address=lp_token_address,
                created_at=datetime.utcnow()
            )
            
            self._staking_pools[pool_id] = pool
            
            return {
                "pool_id": pool_id,
                "tx_hash": receipt.transactionHash.hex(),
                "token_id": token_id,
                "is_lp": is_lp,
                "min_stake_amount": min_stake_amount
            }
            
        except Exception as e:
            logger.error(f"Error creating staking pool: {e}")
            raise
            
    async def create_delegation_pool(
        self,
        operator_address: str,
        operator_fee: int,
        min_delegation: int,
        metadata: str
    ) -> Dict[str, Any]:
        """
        Create a delegation pool
        
        Args:
            operator_address: Pool operator address
            operator_fee: Fee in basis points
            min_delegation: Minimum delegation amount
            metadata: Pool description/strategy
            
        Returns:
            Delegation pool creation result
        """
        try:
            # Validate fee
            if operator_fee > 2000:  # 20% max
                raise ValueError("Operator fee too high (max 20%)")
                
            # Create delegation pool
            tx = await self.staking_contract.functions.createDelegationPool(
                operator_fee,
                min_delegation,
                metadata
            ).transact({"from": operator_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Extract pool ID
            pool_id = None
            for log in receipt.logs:
                if log.topics[0] == self.staking_contract.events.DelegationPoolCreated.topic:
                    pool_id = int(log.data[0])
                    break
                    
            if pool_id is None:
                raise ValueError("Delegation pool creation failed")
                
            # Cache pool data
            pool = DelegationPool(
                pool_id=pool_id,
                operator=operator_address,
                total_delegated=0,
                operator_fee=operator_fee,
                min_delegation=min_delegation,
                accepting_delegations=True,
                metadata=metadata,
                performance_score=50,
                created_at=datetime.utcnow()
            )
            
            self._delegation_pools[pool_id] = pool
            
            return {
                "pool_id": pool_id,
                "tx_hash": receipt.transactionHash.hex(),
                "operator": operator_address,
                "fee": operator_fee
            }
            
        except Exception as e:
            logger.error(f"Error creating delegation pool: {e}")
            raise
            
    async def stake(
        self,
        user_address: str,
        pool_id: int,
        amount: int,
        lock_duration: int
    ) -> Dict[str, Any]:
        """
        Stake tokens in a pool
        
        Args:
            user_address: User's address
            pool_id: Staking pool ID
            amount: Amount to stake
            lock_duration: Lock duration in seconds
            
        Returns:
            Staking result
        """
        try:
            # Get pool info
            pool = await self._get_pool_info(pool_id)
            
            # Validate stake
            if amount < pool["min_stake_amount"]:
                raise ValueError(f"Amount below minimum: {pool['min_stake_amount']}")
                
            if lock_duration < 86400:  # 1 day minimum
                raise ValueError("Lock duration must be at least 1 day")
                
            # Approve token transfer
            if pool["is_lp"]:
                # Approve LP token
                lp_contract = await self.blockchain.get_contract(
                    pool["lp_token_address"],
                    "ERC20"
                )
                
                approve_tx = await lp_contract.functions.approve(
                    self.staking_address,
                    amount
                ).transact({"from": user_address})
                
                await self.blockchain.wait_for_transaction(approve_tx)
            else:
                # Approve resource token
                approve_tx = await self.resource_token_contract.functions.setApprovalForAll(
                    self.staking_address,
                    True
                ).transact({"from": user_address})
                
                await self.blockchain.wait_for_transaction(approve_tx)
                
            # Execute stake
            tx = await self.staking_contract.functions.stake(
                pool_id,
                amount,
                lock_duration
            ).transact({"from": user_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Extract stake ID
            stake_id = None
            for log in receipt.logs:
                if log.topics[0] == self.staking_contract.events.Staked.topic:
                    stake_id = int(log.data[3])
                    break
                    
            if stake_id is None:
                raise ValueError("Staking failed - no stake ID")
                
            # Create stake record
            stake = UserStake(
                stake_id=stake_id,
                user=user_address,
                pool_id=pool_id,
                amount=amount,
                lock_end_time=datetime.utcnow() + timedelta(seconds=lock_duration),
                status=StakeStatus.ACTIVE,
                created_at=datetime.utcnow()
            )
            
            if user_address not in self._user_stakes:
                self._user_stakes[user_address] = {}
            self._user_stakes[user_address][stake_id] = stake
            
            return {
                "stake_id": stake_id,
                "tx_hash": receipt.transactionHash.hex(),
                "amount": amount,
                "lock_end_time": stake.lock_end_time.isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error staking: {e}")
            raise
            
    async def delegate_stake(
        self,
        user_address: str,
        stake_id: int,
        delegation_pool_id: int
    ) -> Dict[str, Any]:
        """
        Delegate a stake to an operator pool
        
        Args:
            user_address: User's address
            stake_id: Stake ID to delegate
            delegation_pool_id: Delegation pool ID
            
        Returns:
            Delegation result
        """
        try:
            # Validate stake ownership
            if (user_address not in self._user_stakes or 
                stake_id not in self._user_stakes[user_address]):
                raise ValueError("Stake not found")
                
            stake = self._user_stakes[user_address][stake_id]
            
            if stake.is_delegated:
                raise ValueError("Stake already delegated")
                
            # Get delegation pool
            delegation_pool = await self._get_delegation_pool_info(delegation_pool_id)
            
            if not delegation_pool["accepting_delegations"]:
                raise ValueError("Pool not accepting delegations")
                
            if stake.amount < delegation_pool["min_delegation"]:
                raise ValueError("Amount below minimum delegation")
                
            # Execute delegation
            tx = await self.staking_contract.functions.delegateStake(
                stake_id,
                delegation_pool_id
            ).transact({"from": user_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Update stake record
            stake.is_delegated = True
            stake.delegation_pool_id = delegation_pool_id
            
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "stake_id": stake_id,
                "delegation_pool_id": delegation_pool_id,
                "operator_fee": delegation_pool["operator_fee"]
            }
            
        except Exception as e:
            logger.error(f"Error delegating stake: {e}")
            raise
            
    async def withdraw(
        self,
        user_address: str,
        stake_id: int
    ) -> Dict[str, Any]:
        """
        Withdraw staked tokens
        
        Args:
            user_address: User's address
            stake_id: Stake ID to withdraw
            
        Returns:
            Withdrawal result
        """
        try:
            # Validate stake
            if (user_address not in self._user_stakes or 
                stake_id not in self._user_stakes[user_address]):
                raise ValueError("Stake not found")
                
            stake = self._user_stakes[user_address][stake_id]
            
            if datetime.utcnow() < stake.lock_end_time:
                raise ValueError("Stake still locked")
                
            # Execute withdrawal
            tx = await self.staking_contract.functions.withdraw(
                stake_id
            ).transact({"from": user_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Update stake status
            stake.status = StakeStatus.WITHDRAWN
            stake.withdrawn_at = datetime.utcnow()
            
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "amount": stake.amount,
                "rewards_claimed": True
            }
            
        except Exception as e:
            logger.error(f"Error withdrawing stake: {e}")
            raise
            
    async def claim_rewards(
        self,
        user_address: str,
        stake_id: int
    ) -> Dict[str, Any]:
        """
        Claim rewards for a stake
        
        Args:
            user_address: User's address
            stake_id: Stake ID to claim rewards for
            
        Returns:
            Claim result
        """
        try:
            # Calculate pending rewards
            pending = await self.get_pending_rewards(user_address, stake_id)
            
            if pending == 0:
                return {
                    "rewards": 0,
                    "message": "No rewards to claim"
                }
                
            # Execute claim
            tx = await self.staking_contract.functions.claimReward(
                stake_id
            ).transact({"from": user_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Update last claim time
            if user_address in self._user_stakes and stake_id in self._user_stakes[user_address]:
                self._user_stakes[user_address][stake_id].last_claim_time = datetime.utcnow()
                
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "rewards": pending,
                "claimed_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error claiming rewards: {e}")
            raise
            
    async def enable_auto_compound(
        self,
        user_address: str,
        enable: bool
    ) -> Dict[str, Any]:
        """
        Enable or disable auto-compounding for a user
        
        Args:
            user_address: User's address
            enable: Whether to enable auto-compounding
            
        Returns:
            Update result
        """
        try:
            tx = await self.staking_contract.functions.setAutoCompound(
                enable
            ).transact({"from": user_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Add to compound queue if enabled
            if enable:
                await self._compound_queue.put(user_address)
                
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "auto_compound": enable
            }
            
        except Exception as e:
            logger.error(f"Error setting auto-compound: {e}")
            raise
            
    async def execute_auto_compound(
        self,
        user_address: str,
        stake_ids: List[int]
    ) -> Dict[str, Any]:
        """
        Execute auto-compound for user's stakes
        
        Args:
            user_address: User's address
            stake_ids: List of stake IDs to compound
            
        Returns:
            Compound result
        """
        try:
            # Check if auto-compound is enabled
            enabled = await self.staking_contract.functions.autoCompoundEnabled(
                user_address
            ).call()
            
            if not enabled:
                raise ValueError("Auto-compound not enabled")
                
            # Execute compound
            tx = await self.staking_contract.functions.executeAutoCompound(
                user_address,
                stake_ids
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Calculate total compounded
            total_compounded = 0
            for log in receipt.logs:
                if log.topics[0] == self.staking_contract.events.CompoundExecuted.topic:
                    total_compounded = int(log.data[1])
                    break
                    
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "total_compounded": total_compounded,
                "stakes_compounded": len(stake_ids)
            }
            
        except Exception as e:
            logger.error(f"Error executing auto-compound: {e}")
            raise
            
    async def update_delegation_pool_fee(
        self,
        operator_address: str,
        pool_id: int,
        new_fee: int
    ) -> Dict[str, Any]:
        """
        Update delegation pool operator fee
        
        Args:
            operator_address: Pool operator address
            pool_id: Delegation pool ID
            new_fee: New fee in basis points
            
        Returns:
            Update result
        """
        try:
            if new_fee > 2000:  # 20% max
                raise ValueError("Fee too high (max 20%)")
                
            tx = await self.staking_contract.functions.updateOperatorFee(
                pool_id,
                new_fee
            ).transact({"from": operator_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Update cache
            if pool_id in self._delegation_pools:
                self._delegation_pools[pool_id].operator_fee = new_fee
                
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "pool_id": pool_id,
                "new_fee": new_fee
            }
            
        except Exception as e:
            logger.error(f"Error updating delegation fee: {e}")
            raise
            
    async def get_user_stakes(
        self,
        user_address: str
    ) -> List[Dict[str, Any]]:
        """Get all stakes for a user"""
        stakes = []
        
        # Get from contract
        stake_ids = await self.staking_contract.functions.getUserStakes(
            user_address
        ).call()
        
        for stake_id in stake_ids:
            stake_data = await self.staking_contract.functions.userStakes(
                user_address,
                stake_id
            ).call()
            
            if stake_data[0] > 0:  # amount > 0
                stakes.append({
                    "stake_id": stake_id,
                    "amount": stake_data[0],
                    "pool_id": stake_data[4],
                    "lock_end_time": datetime.fromtimestamp(stake_data[2]),
                    "is_delegated": stake_data[5],
                    "delegation_pool_id": stake_data[6] if stake_data[5] else None,
                    "pending_rewards": await self.get_pending_rewards(user_address, stake_id)
                })
                
        return stakes
        
    async def get_pending_rewards(
        self,
        user_address: str,
        stake_id: int
    ) -> int:
        """Get pending rewards for a stake"""
        try:
            rewards = await self.staking_contract.functions.pendingReward(
                stake_id
            ).call()
            return rewards
        except:
            return 0
            
    async def get_staking_stats(self) -> StakingStats:
        """Get overall staking statistics"""
        # Aggregate stats from pools
        total_staked = 0
        total_rewards = 0
        active_stakers = set()
        
        for pool_id in range(1, 10):  # Check first 10 pools
            try:
                pool_data = await self.staking_contract.functions.stakingPools(pool_id).call()
                if pool_data[6] > 0:  # min_stake_amount > 0 means pool exists
                    total_staked += pool_data[1]  # total_staked
            except:
                break
                
        return StakingStats(
            total_staked=total_staked,
            total_rewards_distributed=total_rewards,
            active_stakers=len(active_stakers),
            total_pools=len(self._staking_pools),
            total_delegation_pools=len(self._delegation_pools)
        )
        
    async def get_delegation_pools(self) -> List[DelegationPoolInfo]:
        """Get all delegation pools"""
        pools = []
        
        for pool_id in range(1, 20):  # Check first 20 pools
            try:
                pool_data = await self.staking_contract.functions.delegationPools(pool_id).call()
                if pool_data[0] != "0x0000000000000000000000000000000000000000":  # operator exists
                    pools.append(DelegationPoolInfo(
                        pool_id=pool_id,
                        operator=pool_data[0],
                        total_delegated=pool_data[1],
                        operator_fee=pool_data[2],
                        min_delegation=pool_data[3],
                        accepting_delegations=pool_data[4],
                        metadata=pool_data[5],
                        performance_score=pool_data[6]
                    ))
            except:
                break
                
        return pools
        
    async def _monitor_stakes(self):
        """Monitor active stakes for expirations and rewards"""
        while True:
            try:
                current_time = datetime.utcnow()
                
                # Check for unlocked stakes
                for user_stakes in self._user_stakes.values():
                    for stake in user_stakes.values():
                        if (stake.status == StakeStatus.ACTIVE and 
                            current_time >= stake.lock_end_time):
                            stake.status = StakeStatus.UNLOCKED
                            logger.info(f"Stake {stake.stake_id} unlocked")
                            
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error monitoring stakes: {e}")
                await asyncio.sleep(300)
                
    async def _monitor_rewards(self):
        """Monitor and distribute rewards"""
        while True:
            try:
                # This would be triggered by reward additions to pools
                # For now, just log stats
                stats = await self.get_staking_stats()
                logger.info(f"Staking stats: {stats.total_staked} total staked")
                
                await asyncio.sleep(3600)  # Check hourly
                
            except Exception as e:
                logger.error(f"Error monitoring rewards: {e}")
                await asyncio.sleep(3600)
                
    async def _auto_compound_worker(self):
        """Worker to process auto-compound queue"""
        while True:
            try:
                user_address = await self._compound_queue.get()
                
                # Get user's stakes
                stakes = await self.get_user_stakes(user_address)
                stake_ids = [s["stake_id"] for s in stakes if s["pending_rewards"] > 100]
                
                if stake_ids:
                    await self.execute_auto_compound(user_address, stake_ids)
                    logger.info(f"Auto-compounded {len(stake_ids)} stakes for {user_address}")
                    
                # Re-queue for next compound cycle (daily)
                await asyncio.sleep(86400)
                await self._compound_queue.put(user_address)
                
            except Exception as e:
                logger.error(f"Error in auto-compound worker: {e}")
                await asyncio.sleep(60)
                
    async def _get_pool_info(self, pool_id: int) -> Dict[str, Any]:
        """Get staking pool information"""
        pool_data = await self.staking_contract.functions.stakingPools(pool_id).call()
        
        return {
            "token_id": pool_data[0],
            "total_staked": pool_data[1],
            "min_stake_amount": pool_data[6],
            "is_lp": pool_data[7],
            "lp_token_address": pool_data[8]
        }
        
    async def _get_delegation_pool_info(self, pool_id: int) -> Dict[str, Any]:
        """Get delegation pool information"""
        pool_data = await self.staking_contract.functions.delegationPools(pool_id).call()
        
        return {
            "operator": pool_data[0],
            "total_delegated": pool_data[1],
            "operator_fee": pool_data[2],
            "min_delegation": pool_data[3],
            "accepting_delegations": pool_data[4],
            "metadata": pool_data[5],
            "performance_score": pool_data[6]
        } 