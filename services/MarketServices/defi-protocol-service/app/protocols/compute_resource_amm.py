"""
Automated Market Maker (AMM) for Compute Resources

Provides liquidity pools and automated trading for compute resources.
Supports both stable pairs (resource/USDC) and cross-resource pairs.
"""

from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum
import math
from collections import defaultdict

from web3 import Web3
from platformq_shared.blockchain import BlockchainClient
from platformq_shared.models import ResourceType
from ..models import (
    LiquidityPool, LiquidityPosition, SwapTransaction,
    PoolReserves, FeeStructure
)

logger = logging.getLogger(__name__)


class PoolType(str, Enum):
    STABLE = "stable"           # Resource/USDC pairs
    VOLATILE = "volatile"       # Cross-resource pairs
    CONCENTRATED = "concentrated"  # Concentrated liquidity (v3 style)


class SwapDirection(str, Enum):
    EXACT_IN = "exact_in"      # Know input amount, calculate output
    EXACT_OUT = "exact_out"    # Know output amount, calculate input


class AMM:
    """Automated Market Maker for compute resources"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        factory_address: str,
        router_address: str,
        price_oracle_address: str,
        weth_address: str,
        usdc_address: str
    ):
        self.blockchain = blockchain_client
        self.factory_address = factory_address
        self.router_address = router_address
        self.price_oracle_address = price_oracle_address
        self.weth_address = weth_address
        self.usdc_address = usdc_address
        
        # Fee structure
        self.fee_tiers = {
            PoolType.STABLE: {
                'swap_fee': Decimal("0.0005"),    # 0.05% for stable pairs
                'protocol_fee': Decimal("0.0001"), # 0.01% to protocol
                'lp_fee': Decimal("0.0004")        # 0.04% to LPs
            },
            PoolType.VOLATILE: {
                'swap_fee': Decimal("0.003"),      # 0.3% for volatile pairs
                'protocol_fee': Decimal("0.0005"), # 0.05% to protocol
                'lp_fee': Decimal("0.0025")        # 0.25% to LPs
            },
            PoolType.CONCENTRATED: {
                'swap_fee': Decimal("0.001"),      # 0.1% for concentrated
                'protocol_fee': Decimal("0.0002"), # 0.02% to protocol
                'lp_fee': Decimal("0.0008")        # 0.08% to LPs
            }
        }
        
        # Pool tracking
        self._pools = {}  # pool_address -> pool_data
        self._user_positions = defaultdict(list)  # user -> [positions]
        
        # Price impact thresholds
        self.max_price_impact = Decimal("0.1")  # 10% max price impact
        self.warning_price_impact = Decimal("0.05")  # 5% warning threshold
        
        # Minimum liquidity for new pools
        self.min_initial_liquidity = {
            'quantum': Decimal("100"),   # $100 minimum
            'ai': Decimal("100"),
            'network': Decimal("50")
        }
        
    async def create_pool(
        self,
        token0: str,
        token1: str,
        pool_type: PoolType,
        initial_price: Optional[Decimal] = None,
        fee_tier: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """
        Create a new liquidity pool
        
        Args:
            token0: First token address (compute resource or USDC)
            token1: Second token address
            pool_type: Type of pool (stable/volatile/concentrated)
            initial_price: Initial price ratio (token1/token0)
            fee_tier: Custom fee tier (uses default if not specified)
            
        Returns:
            Pool creation details
        """
        try:
            # Get factory contract
            factory = await self.blockchain.get_contract(
                self.factory_address,
                "AMM_Factory"
            )
            
            # Use default fee if not specified
            if fee_tier is None:
                fee_tier = self.fee_tiers[pool_type]['swap_fee']
            
            # Create pool
            tx = await factory.functions.createPool(
                token0,
                token1,
                pool_type,
                int(fee_tier * 10000),  # Convert to basis points
                Web3.toWei(initial_price or 1, 'ether') if initial_price else 0
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get pool address from event
            pool_created = receipt.events.get('PoolCreated')
            pool_address = pool_created['pool']
            
            # Initialize pool data
            pool_data = {
                'address': pool_address,
                'token0': token0,
                'token1': token1,
                'pool_type': pool_type,
                'fee_tier': fee_tier,
                'created_at': datetime.utcnow(),
                'reserves': {
                    'reserve0': Decimal("0"),
                    'reserve1': Decimal("0")
                },
                'total_liquidity': Decimal("0"),
                'volume_24h': Decimal("0"),
                'fees_24h': Decimal("0"),
                'price_oracle': initial_price or Decimal("1")
            }
            
            self._pools[pool_address] = pool_data
            
            logger.info(f"Created {pool_type} pool {pool_address} for {token0}/{token1}")
            
            return {
                'pool_address': pool_address,
                'token0': token0,
                'token1': token1,
                'pool_type': pool_type,
                'fee_tier': fee_tier,
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to create pool: {e}")
            raise
    
    async def add_liquidity(
        self,
        pool_address: str,
        amount0: Decimal,
        amount1: Decimal,
        min_amount0: Optional[Decimal] = None,
        min_amount1: Optional[Decimal] = None,
        recipient: str = None,
        deadline: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Add liquidity to a pool
        
        Args:
            pool_address: Pool to add liquidity to
            amount0: Amount of token0
            amount1: Amount of token1
            min_amount0: Minimum token0 to add (slippage protection)
            min_amount1: Minimum token1 to add
            recipient: LP token recipient
            deadline: Transaction deadline
            
        Returns:
            Liquidity addition details
        """
        try:
            pool_data = self._pools.get(pool_address)
            if not pool_data:
                raise ValueError("Pool not found")
            
            # Get pool contract
            pool = await self.blockchain.get_contract(
                pool_address,
                "AMM_Pool"
            )
            
            # Calculate optimal amounts if pool has existing liquidity
            if pool_data['total_liquidity'] > 0:
                optimal_amounts = await self._calculate_optimal_amounts(
                    pool_data,
                    amount0,
                    amount1
                )
                amount0 = optimal_amounts['amount0']
                amount1 = optimal_amounts['amount1']
            
            # Check minimum amounts
            if min_amount0 and amount0 < min_amount0:
                raise ValueError("Amount0 below minimum")
            if min_amount1 and amount1 < min_amount1:
                raise ValueError("Amount1 below minimum")
            
            # Add liquidity through router
            router = await self.blockchain.get_contract(
                self.router_address,
                "AMM_Router"
            )
            
            tx = await router.functions.addLiquidity(
                pool_data['token0'],
                pool_data['token1'],
                Web3.toWei(amount0, 'ether'),
                Web3.toWei(amount1, 'ether'),
                Web3.toWei(min_amount0 or amount0 * Decimal("0.95"), 'ether'),
                Web3.toWei(min_amount1 or amount1 * Decimal("0.95"), 'ether'),
                recipient or self.blockchain.account,
                deadline or int((datetime.utcnow() + timedelta(minutes=10)).timestamp())
            ).transact({
                'value': Web3.toWei(amount0, 'ether') if pool_data['token0'] == self.weth_address else 0
            })
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get LP tokens minted from event
            liquidity_added = receipt.events.get('LiquidityAdded')
            lp_tokens = Decimal(str(liquidity_added['liquidity'])) / 10**18
            
            # Update pool reserves
            await self._update_pool_reserves(pool_address)
            
            # Track user position
            position = {
                'pool_address': pool_address,
                'lp_tokens': lp_tokens,
                'token0_amount': amount0,
                'token1_amount': amount1,
                'timestamp': datetime.utcnow()
            }
            
            self._user_positions[recipient or self.blockchain.account].append(position)
            
            return {
                'success': True,
                'pool_address': pool_address,
                'lp_tokens': lp_tokens,
                'token0_added': amount0,
                'token1_added': amount1,
                'share_of_pool': lp_tokens / (pool_data['total_liquidity'] + lp_tokens),
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to add liquidity: {e}")
            raise
    
    async def remove_liquidity(
        self,
        pool_address: str,
        lp_tokens: Decimal,
        min_amount0: Optional[Decimal] = None,
        min_amount1: Optional[Decimal] = None,
        recipient: str = None,
        deadline: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Remove liquidity from a pool
        
        Args:
            pool_address: Pool to remove liquidity from
            lp_tokens: Amount of LP tokens to burn
            min_amount0: Minimum token0 to receive
            min_amount1: Minimum token1 to receive
            recipient: Token recipient
            deadline: Transaction deadline
            
        Returns:
            Liquidity removal details
        """
        try:
            pool_data = self._pools.get(pool_address)
            if not pool_data:
                raise ValueError("Pool not found")
            
            # Calculate expected amounts
            share = lp_tokens / pool_data['total_liquidity']
            expected_amount0 = pool_data['reserves']['reserve0'] * share
            expected_amount1 = pool_data['reserves']['reserve1'] * share
            
            # Remove liquidity through router
            router = await self.blockchain.get_contract(
                self.router_address,
                "AMM_Router"
            )
            
            tx = await router.functions.removeLiquidity(
                pool_data['token0'],
                pool_data['token1'],
                Web3.toWei(lp_tokens, 'ether'),
                Web3.toWei(min_amount0 or expected_amount0 * Decimal("0.95"), 'ether'),
                Web3.toWei(min_amount1 or expected_amount1 * Decimal("0.95"), 'ether'),
                recipient or self.blockchain.account,
                deadline or int((datetime.utcnow() + timedelta(minutes=10)).timestamp())
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get amounts from event
            liquidity_removed = receipt.events.get('LiquidityRemoved')
            amount0 = Decimal(str(liquidity_removed['amount0'])) / 10**18
            amount1 = Decimal(str(liquidity_removed['amount1'])) / 10**18
            
            # Update pool reserves
            await self._update_pool_reserves(pool_address)
            
            return {
                'success': True,
                'pool_address': pool_address,
                'lp_tokens_burned': lp_tokens,
                'token0_received': amount0,
                'token1_received': amount1,
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to remove liquidity: {e}")
            raise
    
    async def swap(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        direction: SwapDirection = SwapDirection.EXACT_IN,
        max_slippage: Decimal = Decimal("0.01"),  # 1% default
        recipient: str = None,
        deadline: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Execute a swap
        
        Args:
            token_in: Token to swap from
            token_out: Token to swap to
            amount: Amount (input or output based on direction)
            direction: Whether amount is input or output
            max_slippage: Maximum acceptable slippage
            recipient: Token recipient
            deadline: Transaction deadline
            
        Returns:
            Swap execution details
        """
        try:
            # Find best path
            path_result = await self._find_best_path(
                token_in,
                token_out,
                amount,
                direction
            )
            
            if not path_result['path']:
                raise ValueError("No valid swap path found")
            
            # Check price impact
            if path_result['price_impact'] > self.max_price_impact:
                raise ValueError(
                    f"Price impact too high: {path_result['price_impact']*100:.2f}%"
                )
            
            # Get router contract
            router = await self.blockchain.get_contract(
                self.router_address,
                "AMM_Router"
            )
            
            # Execute swap based on direction
            if direction == SwapDirection.EXACT_IN:
                min_out = path_result['expected_output'] * (1 - max_slippage)
                
                tx = await router.functions.swapExactTokensForTokens(
                    Web3.toWei(amount, 'ether'),
                    Web3.toWei(min_out, 'ether'),
                    path_result['path'],
                    recipient or self.blockchain.account,
                    deadline or int((datetime.utcnow() + timedelta(minutes=10)).timestamp())
                ).transact({
                    'value': Web3.toWei(amount, 'ether') if token_in == self.weth_address else 0
                })
                
            else:  # EXACT_OUT
                max_in = path_result['expected_input'] * (1 + max_slippage)
                
                tx = await router.functions.swapTokensForExactTokens(
                    Web3.toWei(amount, 'ether'),
                    Web3.toWei(max_in, 'ether'),
                    path_result['path'],
                    recipient or self.blockchain.account,
                    deadline or int((datetime.utcnow() + timedelta(minutes=10)).timestamp())
                ).transact({
                    'value': Web3.toWei(max_in, 'ether') if token_in == self.weth_address else 0
                })
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get swap details from event
            swap_event = receipt.events.get('Swap')
            
            # Update pool data
            for pool in path_result['pools']:
                await self._update_pool_reserves(pool)
                await self._update_pool_volume(pool, amount)
            
            return {
                'success': True,
                'token_in': token_in,
                'token_out': token_out,
                'amount_in': swap_event['amountIn'] / 10**18,
                'amount_out': swap_event['amountOut'] / 10**18,
                'path': path_result['path'],
                'pools': path_result['pools'],
                'price_impact': path_result['price_impact'],
                'effective_price': swap_event['amountOut'] / swap_event['amountIn'],
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to execute swap: {e}")
            raise
    
    async def get_pool_info(
        self,
        pool_address: str
    ) -> Dict[str, Any]:
        """
        Get detailed pool information
        
        Args:
            pool_address: Pool address
            
        Returns:
            Pool information including reserves, liquidity, fees
        """
        try:
            pool_data = self._pools.get(pool_address)
            if not pool_data:
                # Fetch from blockchain
                pool_data = await self._fetch_pool_data(pool_address)
            
            # Get current price from oracle
            oracle_price = await self._get_oracle_price(
                pool_data['token0'],
                pool_data['token1']
            )
            
            # Calculate pool metrics
            reserve0 = pool_data['reserves']['reserve0']
            reserve1 = pool_data['reserves']['reserve1']
            
            # Pool price (token1/token0)
            pool_price = reserve1 / reserve0 if reserve0 > 0 else Decimal("0")
            
            # Total value locked (in USD)
            token0_usd = await self._get_token_usd_value(pool_data['token0'], reserve0)
            token1_usd = await self._get_token_usd_value(pool_data['token1'], reserve1)
            tvl = token0_usd + token1_usd
            
            # APR calculation (based on 24h fees)
            apr = (pool_data['fees_24h'] * 365 / tvl) if tvl > 0 else Decimal("0")
            
            return {
                'pool_address': pool_address,
                'token0': pool_data['token0'],
                'token1': pool_data['token1'],
                'pool_type': pool_data['pool_type'],
                'fee_tier': pool_data['fee_tier'],
                'reserves': {
                    'token0': reserve0,
                    'token1': reserve1
                },
                'total_liquidity': pool_data['total_liquidity'],
                'pool_price': pool_price,
                'oracle_price': oracle_price,
                'price_deviation': abs(pool_price - oracle_price) / oracle_price if oracle_price > 0 else Decimal("0"),
                'tvl_usd': tvl,
                'volume_24h': pool_data['volume_24h'],
                'fees_24h': pool_data['fees_24h'],
                'apr': apr * 100,  # As percentage
                'utilization': pool_data['volume_24h'] / tvl if tvl > 0 else Decimal("0")
            }
            
        except Exception as e:
            logger.error(f"Failed to get pool info: {e}")
            raise
    
    async def get_user_positions(
        self,
        user_address: str
    ) -> List[Dict[str, Any]]:
        """
        Get all liquidity positions for a user
        
        Args:
            user_address: User's address
            
        Returns:
            List of user positions with current values
        """
        try:
            positions = []
            user_positions = self._user_positions.get(user_address, [])
            
            for position in user_positions:
                pool_info = await self.get_pool_info(position['pool_address'])
                
                # Calculate current position value
                share = position['lp_tokens'] / pool_info['total_liquidity']
                current_token0 = pool_info['reserves']['token0'] * share
                current_token1 = pool_info['reserves']['token1'] * share
                
                # Calculate impermanent loss
                il = await self._calculate_impermanent_loss(
                    position['token0_amount'],
                    position['token1_amount'],
                    current_token0,
                    current_token1
                )
                
                # Get USD values
                token0_usd = await self._get_token_usd_value(
                    pool_info['token0'],
                    current_token0
                )
                token1_usd = await self._get_token_usd_value(
                    pool_info['token1'],
                    current_token1
                )
                
                # Calculate fees earned (simplified)
                position_age = (datetime.utcnow() - position['timestamp']).days
                estimated_fees = share * pool_info['fees_24h'] * position_age
                
                positions.append({
                    'pool_address': position['pool_address'],
                    'lp_tokens': position['lp_tokens'],
                    'share_of_pool': share * 100,  # As percentage
                    'initial_deposit': {
                        'token0': position['token0_amount'],
                        'token1': position['token1_amount']
                    },
                    'current_value': {
                        'token0': current_token0,
                        'token1': current_token1,
                        'usd': token0_usd + token1_usd
                    },
                    'impermanent_loss': il * 100,  # As percentage
                    'estimated_fees_earned': estimated_fees,
                    'net_position': (token0_usd + token1_usd + estimated_fees) / 
                                   (position['token0_amount'] + position['token1_amount']) - 1,
                    'position_age_days': position_age
                })
            
            return positions
            
        except Exception as e:
            logger.error(f"Failed to get user positions: {e}")
            raise
    
    async def get_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: Decimal
    ) -> Dict[str, Any]:
        """
        Get a swap quote without executing
        
        Args:
            token_in: Token to swap from
            token_out: Token to swap to  
            amount_in: Amount to swap
            
        Returns:
            Quote details including output amount and price impact
        """
        try:
            # Find best path
            path_result = await self._find_best_path(
                token_in,
                token_out,
                amount_in,
                SwapDirection.EXACT_IN
            )
            
            if not path_result['path']:
                return {
                    'error': "No valid swap path found",
                    'token_in': token_in,
                    'token_out': token_out,
                    'amount_in': amount_in
                }
            
            # Get current oracle prices
            oracle_price_in = await self._get_oracle_price(token_in, self.usdc_address)
            oracle_price_out = await self._get_oracle_price(token_out, self.usdc_address)
            
            # Calculate execution price
            execution_price = path_result['expected_output'] / amount_in
            oracle_ratio = oracle_price_out / oracle_price_in if oracle_price_in > 0 else Decimal("0")
            
            return {
                'token_in': token_in,
                'token_out': token_out,
                'amount_in': amount_in,
                'amount_out': path_result['expected_output'],
                'path': path_result['path'],
                'pools': path_result['pools'],
                'execution_price': execution_price,
                'oracle_price': oracle_ratio,
                'price_impact': path_result['price_impact'] * 100,  # As percentage
                'price_deviation': abs(execution_price - oracle_ratio) / oracle_ratio if oracle_ratio > 0 else Decimal("0"),
                'minimum_received': path_result['expected_output'] * Decimal("0.99"),  # 1% slippage
                'route': path_result['route_description']
            }
            
        except Exception as e:
            logger.error(f"Failed to get quote: {e}")
            raise
    
    # Private helper methods
    
    async def _find_best_path(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        direction: SwapDirection
    ) -> Dict[str, Any]:
        """Find the best swap path"""
        
        # Direct path
        direct_pool = await self._find_direct_pool(token_in, token_out)
        if direct_pool:
            direct_result = await self._calculate_swap_result(
                direct_pool,
                token_in,
                amount,
                direction
            )
            
            # Check if direct path is acceptable
            if direct_result['price_impact'] < self.warning_price_impact:
                return {
                    'path': [token_in, token_out],
                    'pools': [direct_pool],
                    'expected_output': direct_result['output'] if direction == SwapDirection.EXACT_IN else amount,
                    'expected_input': amount if direction == SwapDirection.EXACT_IN else direct_result['input'],
                    'price_impact': direct_result['price_impact'],
                    'route_description': f"Direct: {token_in} -> {token_out}"
                }
        
        # Try paths through USDC
        usdc_path_result = await self._find_path_through_hub(
            token_in,
            token_out,
            amount,
            direction,
            self.usdc_address
        )
        
        # Try paths through WETH
        weth_path_result = await self._find_path_through_hub(
            token_in,
            token_out,
            amount,
            direction,
            self.weth_address
        )
        
        # Compare and return best path
        best_result = None
        
        if usdc_path_result and (not best_result or 
            usdc_path_result['expected_output'] > best_result['expected_output']):
            best_result = usdc_path_result
        
        if weth_path_result and (not best_result or 
            weth_path_result['expected_output'] > best_result['expected_output']):
            best_result = weth_path_result
        
        return best_result or {
            'path': [],
            'pools': [],
            'expected_output': Decimal("0"),
            'expected_input': Decimal("0"),
            'price_impact': Decimal("1"),  # 100% impact = no liquidity
            'route_description': "No path found"
        }
    
    async def _calculate_swap_result(
        self,
        pool_address: str,
        token_in: str,
        amount_in: Decimal,
        direction: SwapDirection
    ) -> Dict[str, Any]:
        """Calculate swap output and price impact"""
        
        pool_data = self._pools[pool_address]
        reserve_in = pool_data['reserves']['reserve0'] if pool_data['token0'] == token_in else pool_data['reserves']['reserve1']
        reserve_out = pool_data['reserves']['reserve1'] if pool_data['token0'] == token_in else pool_data['reserves']['reserve0']
        
        if direction == SwapDirection.EXACT_IN:
            # Calculate output amount using constant product formula
            amount_in_with_fee = amount_in * (1 - pool_data['fee_tier'])
            amount_out = (amount_in_with_fee * reserve_out) / (reserve_in + amount_in_with_fee)
            
            # Calculate price impact
            price_before = reserve_out / reserve_in
            price_after = (reserve_out - amount_out) / (reserve_in + amount_in)
            price_impact = abs(price_after - price_before) / price_before
            
            return {
                'output': amount_out,
                'price_impact': price_impact
            }
        else:
            # Calculate required input for exact output
            amount_in = (reserve_in * amount_out) / ((reserve_out - amount_out) * (1 - pool_data['fee_tier']))
            
            # Calculate price impact
            price_before = reserve_out / reserve_in
            price_after = (reserve_out - amount_out) / (reserve_in + amount_in)
            price_impact = abs(price_after - price_before) / price_before
            
            return {
                'input': amount_in,
                'price_impact': price_impact
            }
    
    async def _calculate_optimal_amounts(
        self,
        pool_data: Dict[str, Any],
        desired0: Decimal,
        desired1: Decimal
    ) -> Dict[str, Decimal]:
        """Calculate optimal amounts to maintain pool ratio"""
        
        reserve0 = pool_data['reserves']['reserve0']
        reserve1 = pool_data['reserves']['reserve1']
        
        if reserve0 == 0 or reserve1 == 0:
            return {'amount0': desired0, 'amount1': desired1}
        
        # Calculate optimal amounts to maintain ratio
        optimal1 = desired0 * reserve1 / reserve0
        
        if optimal1 <= desired1:
            return {'amount0': desired0, 'amount1': optimal1}
        else:
            optimal0 = desired1 * reserve0 / reserve1
            return {'amount0': optimal0, 'amount1': desired1}
    
    async def _calculate_impermanent_loss(
        self,
        initial0: Decimal,
        initial1: Decimal,
        current0: Decimal,
        current1: Decimal
    ) -> Decimal:
        """Calculate impermanent loss percentage"""
        
        # Initial value in terms of token1
        initial_value = initial0 * (initial1 / initial0) + initial1
        
        # Current value if held
        price_ratio = current1 / current0
        hold_value = initial0 * price_ratio + initial1
        
        # Current LP value
        lp_value = current0 * price_ratio + current1
        
        # IL = (LP Value / Hold Value) - 1
        if hold_value > 0:
            return (lp_value / hold_value) - 1
        return Decimal("0")
    
    async def _update_pool_reserves(self, pool_address: str):
        """Update pool reserves from blockchain"""
        
        pool = await self.blockchain.get_contract(pool_address, "AMM_Pool")
        reserves = await pool.functions.getReserves().call()
        
        self._pools[pool_address]['reserves'] = {
            'reserve0': Decimal(str(reserves[0])) / 10**18,
            'reserve1': Decimal(str(reserves[1])) / 10**18
        }
        
        # Update total liquidity
        total_supply = await pool.functions.totalSupply().call()
        self._pools[pool_address]['total_liquidity'] = Decimal(str(total_supply)) / 10**18
    
    async def _update_pool_volume(self, pool_address: str, volume: Decimal):
        """Update 24h volume tracking"""
        
        # Simple approximation - in production would track properly
        self._pools[pool_address]['volume_24h'] += volume
        
        # Update fees
        fee_tier = self._pools[pool_address]['fee_tier']
        self._pools[pool_address]['fees_24h'] += volume * fee_tier
    
    async def _get_oracle_price(
        self,
        token0: str,
        token1: str
    ) -> Decimal:
        """Get price from oracle"""
        
        oracle = await self.blockchain.get_contract(
            self.price_oracle_address,
            "PriceOracle"
        )
        
        # Get prices in USD
        price0 = await oracle.functions.getPrice(token0).call()
        price1 = await oracle.functions.getPrice(token1).call()
        
        # Return ratio (token1/token0)
        return Decimal(str(price1)) / Decimal(str(price0))
    
    async def _get_token_usd_value(
        self,
        token: str,
        amount: Decimal
    ) -> Decimal:
        """Get USD value of token amount"""
        
        oracle = await self.blockchain.get_contract(
            self.price_oracle_address,
            "PriceOracle"
        )
        
        price = await oracle.functions.getPrice(token).call()
        return amount * (Decimal(str(price)) / 10**8)  # Oracle uses 8 decimals
    
    async def _find_direct_pool(
        self,
        token0: str,
        token1: str
    ) -> Optional[str]:
        """Find direct pool between two tokens"""
        
        factory = await self.blockchain.get_contract(
            self.factory_address,
            "AMM_Factory"
        )
        
        # Try all fee tiers
        for pool_type in PoolType:
            pool_address = await factory.functions.getPool(
                token0,
                token1,
                pool_type
            ).call()
            
            if pool_address != "0x0000000000000000000000000000000000000000":
                return pool_address
        
        return None
    
    async def _find_path_through_hub(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        direction: SwapDirection,
        hub_token: str
    ) -> Optional[Dict[str, Any]]:
        """Find path through a hub token (USDC or WETH)"""
        
        if token_in == hub_token or token_out == hub_token:
            return None  # Not a multi-hop path
        
        # Find pools
        pool1 = await self._find_direct_pool(token_in, hub_token)
        pool2 = await self._find_direct_pool(hub_token, token_out)
        
        if not pool1 or not pool2:
            return None
        
        # Calculate two-hop swap
        if direction == SwapDirection.EXACT_IN:
            # First swap
            result1 = await self._calculate_swap_result(
                pool1,
                token_in,
                amount,
                SwapDirection.EXACT_IN
            )
            
            # Second swap
            result2 = await self._calculate_swap_result(
                pool2,
                hub_token,
                result1['output'],
                SwapDirection.EXACT_IN
            )
            
            total_impact = result1['price_impact'] + result2['price_impact']
            
            return {
                'path': [token_in, hub_token, token_out],
                'pools': [pool1, pool2],
                'expected_output': result2['output'],
                'expected_input': amount,
                'price_impact': total_impact,
                'route_description': f"{token_in} -> {hub_token} -> {token_out}"
            }
        
        else:  # EXACT_OUT
            # Work backwards
            result2 = await self._calculate_swap_result(
                pool2,
                hub_token,
                amount,
                SwapDirection.EXACT_OUT
            )
            
            result1 = await self._calculate_swap_result(
                pool1,
                token_in,
                result2['input'],
                SwapDirection.EXACT_OUT
            )
            
            total_impact = result1['price_impact'] + result2['price_impact']
            
            return {
                'path': [token_in, hub_token, token_out],
                'pools': [pool1, pool2],
                'expected_output': amount,
                'expected_input': result1['input'],
                'price_impact': total_impact,
                'route_description': f"{token_in} -> {hub_token} -> {token_out}"
            }
    
    async def _fetch_pool_data(self, pool_address: str) -> Dict[str, Any]:
        """Fetch pool data from blockchain"""
        
        pool = await self.blockchain.get_contract(pool_address, "AMM_Pool")
        
        token0 = await pool.functions.token0().call()
        token1 = await pool.functions.token1().call()
        fee = await pool.functions.fee().call()
        reserves = await pool.functions.getReserves().call()
        total_supply = await pool.functions.totalSupply().call()
        
        pool_data = {
            'address': pool_address,
            'token0': token0,
            'token1': token1,
            'pool_type': PoolType.VOLATILE,  # Default, would need to check factory
            'fee_tier': Decimal(str(fee)) / 10000,
            'reserves': {
                'reserve0': Decimal(str(reserves[0])) / 10**18,
                'reserve1': Decimal(str(reserves[1])) / 10**18
            },
            'total_liquidity': Decimal(str(total_supply)) / 10**18,
            'volume_24h': Decimal("0"),  # Would need event tracking
            'fees_24h': Decimal("0"),
            'created_at': datetime.utcnow()
        }
        
        self._pools[pool_address] = pool_data
        return pool_data 