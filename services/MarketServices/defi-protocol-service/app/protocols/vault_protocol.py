"""
Vault Protocol

Manages infrastructure vaults and automated yield strategies.
"""

from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum

from web3 import Web3
from platformq_shared.blockchain import BlockchainClient
from platformq_shared.models import ResourceType, ServiceTier
from ..models import (
    Vault, VaultStrategy, VaultDeposit, VaultWithdrawal,
    StrategyReport, VaultStats
)

logger = logging.getLogger(__name__)


class StrategyType(str, Enum):
    ARBITRAGE = "arbitrage"
    LENDING_OPTIMIZER = "lending_optimizer"
    FLASH_PROVISIONING = "flash_provisioning"
    HEDGED_MINING = "hedged_mining"
    MULTI_STRATEGY = "multi_strategy"


class VaultProtocol:
    """Protocol for managing infrastructure vaults"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        vault_factory_address: str,
        resource_token_address: str,
        amm_address: str,
        lending_address: str,
        staking_address: str
    ):
        self.blockchain = blockchain_client
        self.vault_factory_address = vault_factory_address
        self.resource_token_address = resource_token_address
        self.amm_address = amm_address
        self.lending_address = lending_address
        self.staking_address = staking_address
        
        # Vault tracking
        self._vaults = {}  # vault_address -> Vault
        self._strategies = {}  # strategy_address -> VaultStrategy
        self._user_deposits = {}  # user -> vault -> deposits
        
        # Harvest queue
        self._harvest_queue = asyncio.Queue()
        self._harvest_task = None
        
    async def initialize(self):
        """Initialize the vault protocol"""
        # Start monitoring tasks
        asyncio.create_task(self._monitor_vaults())
        asyncio.create_task(self._monitor_strategies())
        self._harvest_task = asyncio.create_task(self._harvest_worker())
        
        logger.info("Vault Protocol initialized")
        
    async def create_vault(
        self,
        resource_token_id: int,
        name: str,
        symbol: str,
        management_fee: int = 200,  # 2%
        performance_fee: int = 1000  # 10%
    ) -> Dict[str, Any]:
        """
        Create a new infrastructure vault
        
        Args:
            resource_token_id: Resource token ID the vault manages
            name: Vault token name
            symbol: Vault token symbol
            management_fee: Annual management fee in basis points
            performance_fee: Performance fee in basis points
            
        Returns:
            Vault creation result
        """
        try:
            # Deploy vault contract
            vault_factory = await self.blockchain.get_contract(
                self.vault_factory_address,
                "VaultFactory"
            )
            
            tx = await vault_factory.functions.createVault(
                self.resource_token_address,
                resource_token_id,
                self.amm_address,
                self.lending_address,
                self.staking_address,
                name,
                symbol
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Extract vault address from events
            vault_address = None
            for log in receipt.logs:
                if log.topics[0] == vault_factory.events.VaultCreated.topic:
                    vault_address = log.data[0]
                    break
                    
            if not vault_address:
                raise ValueError("Vault creation failed")
                
            # Load vault contract
            vault_contract = await self.blockchain.get_contract(
                vault_address,
                "InfrastructureVault"
            )
            
            # Set fees
            if management_fee != 200:
                await vault_contract.functions.setManagementFee(management_fee).transact()
                
            if performance_fee != 1000:
                await vault_contract.functions.setPerformanceFee(performance_fee).transact()
                
            # Create vault record
            vault = Vault(
                address=vault_address,
                resource_token_id=resource_token_id,
                name=name,
                symbol=symbol,
                total_assets=0,
                total_debt=0,
                price_per_share=10**18,  # Starting at 1:1
                management_fee=management_fee,
                performance_fee=performance_fee,
                created_at=datetime.utcnow()
            )
            
            self._vaults[vault_address] = vault
            
            return {
                "vault_address": vault_address,
                "tx_hash": receipt.transactionHash.hex(),
                "name": name,
                "symbol": symbol
            }
            
        except Exception as e:
            logger.error(f"Error creating vault: {e}")
            raise
            
    async def add_strategy(
        self,
        vault_address: str,
        strategy_type: StrategyType,
        strategy_config: Dict[str, Any],
        debt_ratio: int = 5000,  # 50% allocation
        min_debt_per_harvest: int = 0,
        max_debt_per_harvest: int = 10**18
    ) -> Dict[str, Any]:
        """
        Add a strategy to a vault
        
        Args:
            vault_address: Vault address
            strategy_type: Type of strategy
            strategy_config: Strategy-specific configuration
            debt_ratio: Target allocation in basis points
            min_debt_per_harvest: Minimum change per harvest
            max_debt_per_harvest: Maximum change per harvest
            
        Returns:
            Strategy addition result
        """
        try:
            vault = self._vaults.get(vault_address)
            if not vault:
                raise ValueError("Vault not found")
                
            # Deploy strategy contract based on type
            strategy_address = await self._deploy_strategy(
                vault_address,
                vault.resource_token_id,
                strategy_type,
                strategy_config
            )
            
            # Add strategy to vault
            vault_contract = await self.blockchain.get_contract(
                vault_address,
                "InfrastructureVault"
            )
            
            tx = await vault_contract.functions.addStrategy(
                strategy_address,
                debt_ratio,
                min_debt_per_harvest,
                max_debt_per_harvest
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Create strategy record
            strategy = VaultStrategy(
                address=strategy_address,
                vault_address=vault_address,
                strategy_type=strategy_type,
                debt_ratio=debt_ratio,
                total_debt=0,
                total_gain=0,
                total_loss=0,
                last_report=datetime.utcnow(),
                is_active=True
            )
            
            self._strategies[strategy_address] = strategy
            
            # Add to harvest queue
            await self._harvest_queue.put(strategy_address)
            
            return {
                "strategy_address": strategy_address,
                "tx_hash": receipt.transactionHash.hex(),
                "strategy_type": strategy_type.value,
                "debt_ratio": debt_ratio
            }
            
        except Exception as e:
            logger.error(f"Error adding strategy: {e}")
            raise
            
    async def deposit(
        self,
        vault_address: str,
        user_address: str,
        amount: int
    ) -> Dict[str, Any]:
        """
        Deposit resources into a vault
        
        Args:
            vault_address: Vault address
            user_address: User's address
            amount: Amount to deposit
            
        Returns:
            Deposit result
        """
        try:
            vault = self._vaults.get(vault_address)
            if not vault:
                raise ValueError("Vault not found")
                
            vault_contract = await self.blockchain.get_contract(
                vault_address,
                "InfrastructureVault"
            )
            
            # Check if emergency shutdown
            emergency_shutdown = await vault_contract.functions.emergencyShutdown().call()
            if emergency_shutdown:
                raise ValueError("Vault in emergency shutdown")
                
            # Approve token transfer
            resource_token = await self.blockchain.get_contract(
                self.resource_token_address,
                "ResourceToken"
            )
            
            approve_tx = await resource_token.functions.setApprovalForAll(
                vault_address,
                True
            ).transact({"from": user_address})
            
            await self.blockchain.wait_for_transaction(approve_tx)
            
            # Get expected shares
            expected_shares = await vault_contract.functions.expectedSharesForAmount(amount).call()
            
            # Execute deposit
            tx = await vault_contract.functions.deposit(
                amount,
                user_address
            ).transact({"from": user_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Record deposit
            deposit = VaultDeposit(
                vault_address=vault_address,
                user_address=user_address,
                amount=amount,
                shares=expected_shares,
                timestamp=datetime.utcnow()
            )
            
            if user_address not in self._user_deposits:
                self._user_deposits[user_address] = {}
            if vault_address not in self._user_deposits[user_address]:
                self._user_deposits[user_address][vault_address] = []
                
            self._user_deposits[user_address][vault_address].append(deposit)
            
            # Update vault stats
            vault.total_assets += amount
            
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "shares": expected_shares,
                "price_per_share": await self._get_price_per_share(vault_address)
            }
            
        except Exception as e:
            logger.error(f"Error depositing to vault: {e}")
            raise
            
    async def withdraw(
        self,
        vault_address: str,
        user_address: str,
        shares: int,
        max_loss: int = 100  # 1% max loss
    ) -> Dict[str, Any]:
        """
        Withdraw from a vault
        
        Args:
            vault_address: Vault address
            user_address: User's address
            shares: Amount of shares to burn
            max_loss: Maximum acceptable loss in basis points
            
        Returns:
            Withdrawal result
        """
        try:
            vault_contract = await self.blockchain.get_contract(
                vault_address,
                "InfrastructureVault"
            )
            
            # Get user's balance
            user_shares = await vault_contract.functions.balanceOf(user_address).call()
            if shares > user_shares:
                shares = user_shares
                
            # Calculate expected output
            price_per_share = await vault_contract.functions.pricePerShare().call()
            expected_amount = shares * price_per_share // 10**18
            
            # Execute withdrawal
            tx = await vault_contract.functions.withdraw(
                shares,
                user_address,
                max_loss
            ).transact({"from": user_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Extract actual amount from events
            actual_amount = expected_amount  # Would parse from events
            
            # Record withdrawal
            withdrawal = VaultWithdrawal(
                vault_address=vault_address,
                user_address=user_address,
                shares=shares,
                amount=actual_amount,
                timestamp=datetime.utcnow()
            )
            
            # Update vault stats
            vault = self._vaults.get(vault_address)
            if vault:
                vault.total_assets -= actual_amount
                
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "amount": actual_amount,
                "shares_burned": shares
            }
            
        except Exception as e:
            logger.error(f"Error withdrawing from vault: {e}")
            raise
            
    async def harvest_strategy(
        self,
        strategy_address: str
    ) -> Dict[str, Any]:
        """
        Harvest a strategy
        
        Args:
            strategy_address: Strategy address
            
        Returns:
            Harvest result
        """
        try:
            strategy = self._strategies.get(strategy_address)
            if not strategy:
                raise ValueError("Strategy not found")
                
            strategy_contract = await self.blockchain.get_contract(
                strategy_address,
                strategy.strategy_type.value.title().replace("_", "") + "Vault"
            )
            
            # Execute harvest
            tx = await strategy_contract.functions.harvest().transact()
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Extract results from events
            profit = 0
            loss = 0
            debt_payment = 0
            
            for log in receipt.logs:
                if log.topics[0].hex() == "Harvested":
                    profit = int(log.data[0])
                    loss = int(log.data[1])
                    debt_payment = int(log.data[2])
                    break
                    
            # Update strategy stats
            strategy.total_gain += profit
            strategy.total_loss += loss
            strategy.last_report = datetime.utcnow()
            
            # Create report
            report = StrategyReport(
                strategy_address=strategy_address,
                profit=profit,
                loss=loss,
                debt_payment=debt_payment,
                total_debt=strategy.total_debt,
                timestamp=datetime.utcnow()
            )
            
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "profit": profit,
                "loss": loss,
                "apy": await self._calculate_strategy_apy(strategy_address)
            }
            
        except Exception as e:
            logger.error(f"Error harvesting strategy: {e}")
            raise
            
    async def update_strategy_debt_ratio(
        self,
        vault_address: str,
        strategy_address: str,
        new_debt_ratio: int
    ) -> Dict[str, Any]:
        """
        Update a strategy's debt ratio
        
        Args:
            vault_address: Vault address
            strategy_address: Strategy address
            new_debt_ratio: New debt ratio in basis points
            
        Returns:
            Update result
        """
        try:
            vault_contract = await self.blockchain.get_contract(
                vault_address,
                "InfrastructureVault"
            )
            
            # Get current params
            current_params = await vault_contract.functions.strategies(strategy_address).call()
            
            tx = await vault_contract.functions.updateStrategyParams(
                strategy_address,
                new_debt_ratio,
                current_params[2],  # min_debt_per_harvest
                current_params[3]   # max_debt_per_harvest
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Update local state
            strategy = self._strategies.get(strategy_address)
            if strategy:
                strategy.debt_ratio = new_debt_ratio
                
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "new_debt_ratio": new_debt_ratio
            }
            
        except Exception as e:
            logger.error(f"Error updating strategy debt ratio: {e}")
            raise
            
    async def emergency_shutdown(
        self,
        vault_address: str,
        active: bool
    ) -> Dict[str, Any]:
        """
        Activate or deactivate emergency shutdown
        
        Args:
            vault_address: Vault address
            active: Whether to activate shutdown
            
        Returns:
            Shutdown result
        """
        try:
            vault_contract = await self.blockchain.get_contract(
                vault_address,
                "InfrastructureVault"
            )
            
            tx = await vault_contract.functions.setEmergencyShutdown(
                active
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Update vault state
            vault = self._vaults.get(vault_address)
            if vault:
                vault.emergency_shutdown = active
                
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "emergency_shutdown": active
            }
            
        except Exception as e:
            logger.error(f"Error setting emergency shutdown: {e}")
            raise
            
    async def get_vault_stats(
        self,
        vault_address: str
    ) -> VaultStats:
        """Get vault statistics"""
        vault_contract = await self.blockchain.get_contract(
            vault_address,
            "InfrastructureVault"
        )
        
        total_assets = await vault_contract.functions.totalAssets().call()
        price_per_share = await vault_contract.functions.pricePerShare().call()
        total_supply = await vault_contract.functions.totalSupply().call()
        
        # Calculate TVL
        tvl = total_assets
        
        # Calculate APY (simplified)
        apy = await self._calculate_vault_apy(vault_address)
        
        return VaultStats(
            total_assets=total_assets,
            total_debt=await self._get_total_debt(vault_address),
            price_per_share=price_per_share,
            total_shares=total_supply,
            tvl=tvl,
            apy=apy,
            active_strategies=await self._count_active_strategies(vault_address)
        )
        
    async def get_user_balance(
        self,
        vault_address: str,
        user_address: str
    ) -> Dict[str, Any]:
        """Get user's vault balance"""
        vault_contract = await self.blockchain.get_contract(
            vault_address,
            "InfrastructureVault"
        )
        
        shares = await vault_contract.functions.balanceOf(user_address).call()
        price_per_share = await vault_contract.functions.pricePerShare().call()
        
        value = shares * price_per_share // 10**18
        
        return {
            "shares": shares,
            "value": value,
            "price_per_share": price_per_share
        }
        
    async def _deploy_strategy(
        self,
        vault_address: str,
        resource_token_id: int,
        strategy_type: StrategyType,
        config: Dict[str, Any]
    ) -> str:
        """Deploy a strategy contract"""
        # In production, this would deploy the actual strategy
        # For now, return mock address
        strategy_address = f"0x{strategy_type.value}...{vault_address[-6:]}"
        
        logger.info(f"Deployed {strategy_type.value} strategy at {strategy_address}")
        return strategy_address
        
    async def _monitor_vaults(self):
        """Monitor vault health and performance"""
        while True:
            try:
                for vault_address, vault in self._vaults.items():
                    stats = await self.get_vault_stats(vault_address)
                    
                    # Update vault data
                    vault.total_assets = stats.total_assets
                    vault.total_debt = stats.total_debt
                    vault.price_per_share = stats.price_per_share
                    
                    # Check for issues
                    if stats.total_debt > stats.total_assets:
                        logger.warning(f"Vault {vault_address} has negative balance")
                        
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"Error monitoring vaults: {e}")
                await asyncio.sleep(600)
                
    async def _monitor_strategies(self):
        """Monitor strategy performance"""
        while True:
            try:
                for strategy_address, strategy in self._strategies.items():
                    if not strategy.is_active:
                        continue
                        
                    # Check if harvest needed
                    time_since_report = datetime.utcnow() - strategy.last_report
                    if time_since_report > timedelta(hours=12):
                        await self._harvest_queue.put(strategy_address)
                        
                await asyncio.sleep(600)  # Check every 10 minutes
                
            except Exception as e:
                logger.error(f"Error monitoring strategies: {e}")
                await asyncio.sleep(600)
                
    async def _harvest_worker(self):
        """Worker to process harvest queue"""
        while True:
            try:
                strategy_address = await self._harvest_queue.get()
                
                strategy = self._strategies.get(strategy_address)
                if strategy and strategy.is_active:
                    await self.harvest_strategy(strategy_address)
                    logger.info(f"Harvested strategy {strategy_address}")
                    
            except Exception as e:
                logger.error(f"Error in harvest worker: {e}")
                await asyncio.sleep(60)
                
    async def _get_price_per_share(self, vault_address: str) -> int:
        """Get current price per share"""
        vault_contract = await self.blockchain.get_contract(
            vault_address,
            "InfrastructureVault"
        )
        
        return await vault_contract.functions.pricePerShare().call()
        
    async def _get_total_debt(self, vault_address: str) -> int:
        """Get total debt across all strategies"""
        total_debt = 0
        
        for strategy in self._strategies.values():
            if strategy.vault_address == vault_address:
                total_debt += strategy.total_debt
                
        return total_debt
        
    async def _count_active_strategies(self, vault_address: str) -> int:
        """Count active strategies for a vault"""
        count = 0
        
        for strategy in self._strategies.values():
            if strategy.vault_address == vault_address and strategy.is_active:
                count += 1
                
        return count
        
    async def _calculate_vault_apy(self, vault_address: str) -> float:
        """Calculate vault APY"""
        # Simplified calculation
        # In production would track historical price per share
        return 12.5  # Mock 12.5% APY
        
    async def _calculate_strategy_apy(self, strategy_address: str) -> float:
        """Calculate strategy APY"""
        strategy = self._strategies.get(strategy_address)
        if not strategy:
            return 0.0
            
        # Calculate based on gains vs debt over time
        if strategy.total_debt == 0:
            return 0.0
            
        time_active = (datetime.utcnow() - strategy.last_report).total_seconds()
        if time_active == 0:
            return 0.0
            
        annual_gain = (strategy.total_gain / strategy.total_debt) * (365 * 24 * 3600 / time_active)
        return annual_gain * 100  # Convert to percentage 