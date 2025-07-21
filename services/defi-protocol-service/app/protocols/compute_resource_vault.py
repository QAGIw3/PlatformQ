"""
Compute Resource Vault Protocol

Specialized vaults for quantum, AI, and network compute resources.
Integrates with compute markets for yield optimization and arbitrage.
"""

from typing import Dict, Any, List, Optional, Tuple, Union
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum
import numpy as np

from web3 import Web3
from platformq_shared.blockchain import BlockchainClient
from platformq_shared.models import ResourceType, ServiceTier
from ..models import (
    Vault, VaultStrategy, VaultDeposit, VaultWithdrawal,
    StrategyReport, VaultStats
)
from .vault_protocol import VaultProtocol, StrategyType

logger = logging.getLogger(__name__)


class ComputeResourceType(str, Enum):
    QUANTUM = "quantum"
    AI = "ai"
    NETWORK = "network"
    HYBRID = "hybrid"  # Multi-resource bundles


class ComputeStrategyType(str, Enum):
    MARKET_ARBITRAGE = "market_arbitrage"  # Cross-market arbitrage
    QUALITY_ARBITRAGE = "quality_arbitrage"  # Quality vs price arbitrage
    TIME_ARBITRAGE = "time_arbitrage"  # Spot vs futures arbitrage
    BUNDLE_OPTIMIZATION = "bundle_optimization"  # Optimal resource bundling
    FUTURES_HEDGING = "futures_hedging"  # Hedge spot with futures
    RESERVED_OPTIMIZATION = "reserved_optimization"  # Reserved vs on-demand
    WORKLOAD_MATCHING = "workload_matching"  # Match resources to workloads
    CROSS_CHAIN_ARBITRAGE = "cross_chain_arbitrage"  # Cross-chain opportunities


class ComputeResourceVault(VaultProtocol):
    """Extended vault protocol for compute resources"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        vault_factory_address: str,
        resource_token_address: str,
        amm_address: str,
        lending_address: str,
        staking_address: str,
        quantum_market_address: str,
        ai_market_address: str,
        network_market_address: str,
        oracle_address: str,
        aggregator_address: str
    ):
        super().__init__(
            blockchain_client,
            vault_factory_address,
            resource_token_address,
            amm_address,
            lending_address,
            staking_address
        )
        
        # Compute market contracts
        self.quantum_market_address = quantum_market_address
        self.ai_market_address = ai_market_address
        self.network_market_address = network_market_address
        self.oracle_address = oracle_address
        self.aggregator_address = aggregator_address
        
        # Compute-specific tracking
        self._compute_vaults = {}  # resource_type -> vault_address -> data
        self._active_strategies = {}  # strategy_id -> strategy_data
        self._resource_allocations = {}  # vault -> resource_id -> allocation
        self._quality_scores = {}  # resource_id -> quality_data
        
        # Strategy parameters
        self.min_arbitrage_profit = Decimal("0.02")  # 2% minimum
        self.max_quality_discount = Decimal("0.15")  # 15% max discount for quality
        self.bundle_discount_threshold = Decimal("0.05")  # 5% bundle discount
        
    async def create_compute_vault(
        self,
        resource_type: ComputeResourceType,
        name: str,
        symbol: str,
        strategies: List[ComputeStrategyType],
        management_fee: int = 200,  # 2%
        performance_fee: int = 1500,  # 15% for active strategies
        min_deposit: int = 100
    ) -> Dict[str, Any]:
        """
        Create a specialized compute resource vault
        
        Args:
            resource_type: Type of compute resource
            name: Vault name
            symbol: Vault symbol
            strategies: List of strategies to enable
            management_fee: Annual management fee in basis points
            performance_fee: Performance fee in basis points
            min_deposit: Minimum deposit amount
            
        Returns:
            Vault creation result
        """
        try:
            # Get resource token ID based on type
            resource_token_id = await self._get_resource_token_id(resource_type)
            
            # Create base vault
            vault_result = await self.create_vault(
                resource_token_id,
                name,
                symbol,
                management_fee,
                performance_fee
            )
            
            vault_address = vault_result['vault_address']
            
            # Initialize compute-specific features
            vault_data = {
                'resource_type': resource_type,
                'strategies': strategies,
                'created_at': datetime.utcnow(),
                'min_deposit': min_deposit,
                'total_value_locked': Decimal("0"),
                'active_allocations': 0,
                'performance_history': []
            }
            
            if resource_type not in self._compute_vaults:
                self._compute_vaults[resource_type] = {}
            
            self._compute_vaults[resource_type][vault_address] = vault_data
            
            # Deploy strategies
            for strategy in strategies:
                await self._deploy_compute_strategy(
                    vault_address,
                    resource_type,
                    strategy
                )
            
            logger.info(f"Created compute vault {vault_address} for {resource_type}")
            
            return {
                'vault_address': vault_address,
                'resource_type': resource_type,
                'strategies': strategies,
                **vault_result
            }
            
        except Exception as e:
            logger.error(f"Failed to create compute vault: {e}")
            raise
    
    async def deposit_compute_resources(
        self,
        vault_address: str,
        user_address: str,
        resource_ids: List[int],
        amounts: List[int],
        lock_period_days: int = 0
    ) -> Dict[str, Any]:
        """
        Deposit compute resources into vault
        
        Args:
            vault_address: Target vault address
            user_address: User's address
            resource_ids: List of resource token IDs
            amounts: List of amounts to deposit
            lock_period_days: Optional lock period for bonus yield
            
        Returns:
            Deposit result
        """
        try:
            # Validate resources match vault type
            vault_type = await self._get_vault_resource_type(vault_address)
            
            # Calculate deposit value
            total_value = Decimal("0")
            resource_details = []
            
            for resource_id, amount in zip(resource_ids, amounts):
                # Get quality score from oracle
                quality = await self._get_resource_quality(resource_id)
                
                # Get current market price
                price = await self._get_resource_price(resource_id, vault_type)
                
                # Quality-adjusted value
                quality_multiplier = Decimal("1") + (quality['overall_score'] / 100 - Decimal("0.8")) * Decimal("0.2")
                adjusted_value = price * amount * quality_multiplier
                
                total_value += adjusted_value
                
                resource_details.append({
                    'resource_id': resource_id,
                    'amount': amount,
                    'quality_score': quality['overall_score'],
                    'market_price': price,
                    'adjusted_value': adjusted_value
                })
            
            # Calculate vault shares
            share_price = await self._get_vault_share_price(vault_address)
            shares_minted = total_value / share_price
            
            # Apply lock bonus if applicable
            if lock_period_days > 0:
                lock_bonus = min(lock_period_days / 365, Decimal("0.5"))  # Max 50% bonus
                shares_minted *= (Decimal("1") + lock_bonus)
            
            # Execute deposit on-chain
            vault_contract = await self.blockchain.get_contract(
                vault_address,
                "ComputeResourceVault"
            )
            
            tx = await vault_contract.functions.deposit(
                resource_ids,
                amounts,
                user_address,
                lock_period_days * 86400  # Convert to seconds
            ).transact()
            
            await self.blockchain.wait_for_transaction(tx)
            
            # Update internal tracking
            if user_address not in self._user_deposits:
                self._user_deposits[user_address] = {}
            
            if vault_address not in self._user_deposits[user_address]:
                self._user_deposits[user_address][vault_address] = []
            
            deposit = VaultDeposit(
                vault_address=vault_address,
                user_address=user_address,
                amount=total_value,
                shares=shares_minted,
                timestamp=datetime.utcnow(),
                lock_expires=datetime.utcnow() + timedelta(days=lock_period_days) if lock_period_days > 0 else None
            )
            
            self._user_deposits[user_address][vault_address].append(deposit)
            
            # Trigger strategy rebalancing
            await self._rebalance_vault_strategies(vault_address)
            
            return {
                'tx_hash': tx,
                'shares_minted': shares_minted,
                'total_value': total_value,
                'share_price': share_price,
                'lock_bonus_applied': lock_period_days > 0,
                'resource_details': resource_details
            }
            
        except Exception as e:
            logger.error(f"Failed to deposit compute resources: {e}")
            raise
    
    async def execute_market_arbitrage(
        self,
        vault_address: str,
        opportunity_id: str
    ) -> Dict[str, Any]:
        """
        Execute market arbitrage opportunity
        
        Args:
            vault_address: Vault executing the arbitrage
            opportunity_id: Arbitrage opportunity ID
            
        Returns:
            Execution result
        """
        try:
            # Get opportunity details from aggregator
            aggregator = await self.blockchain.get_contract(
                self.aggregator_address,
                "MarketAggregator"
            )
            
            opportunity = await aggregator.functions.getArbitrageOpportunity(
                opportunity_id
            ).call()
            
            if opportunity['executed']:
                return {'success': False, 'reason': 'Already executed'}
            
            if opportunity['expiresAt'] < datetime.utcnow().timestamp():
                return {'success': False, 'reason': 'Opportunity expired'}
            
            # Calculate required capital
            required_capital = opportunity['quantity'] * min(
                opportunity['priceA'],
                opportunity['priceB']
            )
            
            # Check vault has sufficient liquidity
            vault_liquidity = await self._get_vault_liquidity(vault_address)
            
            if vault_liquidity < required_capital:
                return {'success': False, 'reason': 'Insufficient liquidity'}
            
            # Execute arbitrage
            tx = await aggregator.functions.executeArbitrage(
                opportunity_id
            ).transact({'from': vault_address})
            
            await self.blockchain.wait_for_transaction(tx)
            
            # Calculate actual profit
            receipt = await self.blockchain.get_transaction_receipt(tx)
            profit_event = receipt.events.get('ArbitrageExecuted')
            
            if profit_event:
                actual_profit = profit_event['actualProfit']
                fee_paid = profit_event['fee']
                net_profit = actual_profit - fee_paid
                
                # Update vault performance
                await self._record_strategy_performance(
                    vault_address,
                    ComputeStrategyType.MARKET_ARBITRAGE,
                    net_profit,
                    required_capital
                )
                
                return {
                    'success': True,
                    'tx_hash': tx,
                    'gross_profit': actual_profit,
                    'fee_paid': fee_paid,
                    'net_profit': net_profit,
                    'roi': net_profit / required_capital
                }
            
            return {'success': False, 'reason': 'Execution failed'}
            
        except Exception as e:
            logger.error(f"Failed to execute arbitrage: {e}")
            raise
    
    async def optimize_resource_bundle(
        self,
        vault_address: str,
        workload_template: str,
        budget_limit: Decimal
    ) -> Dict[str, Any]:
        """
        Optimize resource bundle for workload
        
        Args:
            vault_address: Vault address
            workload_template: Template ID (e.g., 'quantum_ml_hybrid')
            budget_limit: Maximum budget
            
        Returns:
            Optimization result
        """
        try:
            # Get vault resources
            vault_resources = await self._get_vault_resources(vault_address)
            
            # Get template requirements
            template_reqs = await self._get_workload_template(workload_template)
            
            # Build optimization problem
            resource_options = {
                'quantum': [],
                'ai': [],
                'network': []
            }
            
            for resource in vault_resources:
                resource_type = resource['type'].lower()
                if resource_type in resource_options:
                    quality = await self._get_resource_quality(resource['id'])
                    price = await self._get_resource_price(resource['id'], resource_type)
                    
                    resource_options[resource_type].append({
                        'id': resource['id'],
                        'quality': quality['overall_score'],
                        'price': price,
                        'available': resource['available']
                    })
            
            # Run optimization
            optimal_bundle = await self._run_bundle_optimization(
                template_reqs,
                resource_options,
                budget_limit
            )
            
            if not optimal_bundle:
                return {'success': False, 'reason': 'No feasible solution'}
            
            # Create bundle in aggregator
            aggregator = await self.blockchain.get_contract(
                self.aggregator_address,
                "MarketAggregator"
            )
            
            bundle_id = f"vault_{vault_address}_{workload_template}_{datetime.utcnow().timestamp()}"
            
            tx = await aggregator.functions.createBundle(
                bundle_id,
                optimal_bundle['resource_types'],
                optimal_bundle['resource_ids'],
                optimal_bundle['costs'],
                86400  # 24 hour duration
            ).transact({'from': vault_address})
            
            await self.blockchain.wait_for_transaction(tx)
            
            return {
                'success': True,
                'bundle_id': bundle_id,
                'total_cost': optimal_bundle['total_cost'],
                'final_cost': optimal_bundle['final_cost'],
                'discount_applied': optimal_bundle['discount'],
                'resources': optimal_bundle['resources'],
                'expected_performance': optimal_bundle['expected_performance']
            }
            
        except Exception as e:
            logger.error(f"Failed to optimize bundle: {e}")
            raise
    
    async def harvest_yields(
        self,
        vault_address: str
    ) -> Dict[str, Any]:
        """
        Harvest yields from all active strategies
        
        Args:
            vault_address: Vault to harvest
            
        Returns:
            Harvest result
        """
        try:
            total_harvested = Decimal("0")
            strategy_results = []
            
            # Get vault's active strategies
            vault_data = await self._get_vault_data(vault_address)
            
            for strategy_type in vault_data['strategies']:
                if strategy_type == ComputeStrategyType.MARKET_ARBITRAGE:
                    # Check for arbitrage opportunities
                    opportunities = await self._find_arbitrage_opportunities(
                        vault_data['resource_type']
                    )
                    
                    for opp in opportunities[:3]:  # Execute top 3
                        result = await self.execute_market_arbitrage(
                            vault_address,
                            opp['opportunity_id']
                        )
                        
                        if result['success']:
                            total_harvested += result['net_profit']
                            strategy_results.append({
                                'strategy': strategy_type,
                                'profit': result['net_profit']
                            })
                
                elif strategy_type == ComputeStrategyType.QUALITY_ARBITRAGE:
                    # Find underpriced high-quality resources
                    result = await self._execute_quality_arbitrage(vault_address)
                    if result['profit'] > 0:
                        total_harvested += result['profit']
                        strategy_results.append({
                            'strategy': strategy_type,
                            'profit': result['profit']
                        })
                
                elif strategy_type == ComputeStrategyType.BUNDLE_OPTIMIZATION:
                    # Optimize bundles for better pricing
                    result = await self._optimize_vault_bundles(vault_address)
                    if result['savings'] > 0:
                        total_harvested += result['savings']
                        strategy_results.append({
                            'strategy': strategy_type,
                            'profit': result['savings']
                        })
            
            # Distribute harvested yields
            if total_harvested > 0:
                # Calculate fees
                performance_fee = total_harvested * vault_data['performance_fee'] / 10000
                net_yield = total_harvested - performance_fee
                
                # Update share price
                await self._distribute_yield(vault_address, net_yield)
                
                # Record harvest
                await self._record_harvest(
                    vault_address,
                    total_harvested,
                    performance_fee,
                    strategy_results
                )
            
            return {
                'total_harvested': total_harvested,
                'performance_fee': performance_fee if total_harvested > 0 else 0,
                'net_yield': net_yield if total_harvested > 0 else 0,
                'strategy_results': strategy_results,
                'timestamp': datetime.utcnow()
            }
            
        except Exception as e:
            logger.error(f"Failed to harvest yields: {e}")
            raise
    
    # Helper methods
    
    async def _get_resource_token_id(
        self,
        resource_type: ComputeResourceType
    ) -> int:
        """Get resource token ID for compute type"""
        # In production, would query token contracts
        type_to_id = {
            ComputeResourceType.QUANTUM: 1,
            ComputeResourceType.AI: 2,
            ComputeResourceType.NETWORK: 3,
            ComputeResourceType.HYBRID: 4
        }
        return type_to_id.get(resource_type, 0)
    
    async def _get_resource_quality(
        self,
        resource_id: int
    ) -> Dict[str, Any]:
        """Get resource quality from oracle"""
        oracle = await self.blockchain.get_contract(
            self.oracle_address,
            "ComputeResourceOracle"
        )
        
        quality_data = await oracle.functions.getQualityScore(
            resource_id
        ).call()
        
        return {
            'overall_score': Decimal(str(quality_data['overallScore'])) / 100,
            'components': quality_data['components'],
            'timestamp': quality_data['timestamp']
        }
    
    async def _get_resource_price(
        self,
        resource_id: int,
        resource_type: str
    ) -> Decimal:
        """Get current market price for resource"""
        market_address = {
            'quantum': self.quantum_market_address,
            'ai': self.ai_market_address,
            'network': self.network_market_address
        }.get(resource_type.lower())
        
        if not market_address:
            raise ValueError(f"Unknown resource type: {resource_type}")
        
        market = await self.blockchain.get_contract(
            market_address,
            f"{resource_type.title()}Market"
        )
        
        price = await market.functions.getCurrentPrice(resource_id).call()
        return Decimal(str(price)) / 10**18  # Convert from wei
    
    async def _run_bundle_optimization(
        self,
        requirements: Dict[str, Any],
        resources: Dict[str, List[Dict[str, Any]]],
        budget: Decimal
    ) -> Optional[Dict[str, Any]]:
        """Run optimization algorithm for bundle"""
        # Simplified genetic algorithm for demonstration
        # In production, would use more sophisticated optimization
        
        best_solution = None
        best_score = float('-inf')
        
        # Generate candidate solutions
        for _ in range(100):
            solution = {
                'resource_types': [],
                'resource_ids': [],
                'costs': [],
                'resources': []
            }
            
            total_cost = Decimal("0")
            
            # Select resources for each type
            for resource_type, options in resources.items():
                if resource_type in requirements:
                    if options:
                        # Random selection weighted by quality/price ratio
                        weights = [
                            float(opt['quality'] / opt['price'])
                            for opt in options
                        ]
                        
                        if sum(weights) > 0:
                            probabilities = np.array(weights) / sum(weights)
                            selected_idx = np.random.choice(
                                len(options),
                                p=probabilities
                            )
                            
                            selected = options[selected_idx]
                            
                            solution['resource_types'].append(resource_type.upper())
                            solution['resource_ids'].append(selected['id'])
                            solution['costs'].append(float(selected['price']))
                            solution['resources'].append(selected)
                            
                            total_cost += selected['price']
            
            # Check budget constraint
            if total_cost <= budget:
                # Calculate solution score
                quality_score = sum(
                    r['quality'] for r in solution['resources']
                ) / len(solution['resources']) if solution['resources'] else 0
                
                cost_score = float(budget - total_cost) / float(budget)
                
                # Combined score (weighted)
                score = 0.7 * quality_score + 0.3 * cost_score
                
                if score > best_score:
                    best_score = score
                    solution['total_cost'] = total_cost
                    
                    # Apply bundle discount
                    discount = Decimal("0.05")  # Base 5%
                    if len(set(solution['resource_types'])) > 1:
                        discount += Decimal("0.03")  # Additional 3% for multi-type
                    
                    solution['discount'] = discount
                    solution['final_cost'] = total_cost * (Decimal("1") - discount)
                    solution['expected_performance'] = quality_score
                    
                    best_solution = solution
        
        return best_solution
    
    async def _deploy_compute_strategy(
        self,
        vault_address: str,
        resource_type: ComputeResourceType,
        strategy_type: ComputeStrategyType
    ):
        """Deploy a compute strategy for vault"""
        # In production, would deploy actual strategy contracts
        strategy_id = f"{vault_address}_{strategy_type}_{datetime.utcnow().timestamp()}"
        
        self._active_strategies[strategy_id] = {
            'vault_address': vault_address,
            'resource_type': resource_type,
            'strategy_type': strategy_type,
            'deployed_at': datetime.utcnow(),
            'performance': Decimal("0"),
            'active': True
        }
        
        logger.info(f"Deployed strategy {strategy_type} for vault {vault_address}") 