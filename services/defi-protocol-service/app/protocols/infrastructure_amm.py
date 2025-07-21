"""
Infrastructure AMM Protocol

Manages AMM pools for infrastructure resource tokens.
"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
import logging
import asyncio
from datetime import datetime, timedelta

from ..models import ResourceType, ServiceTier, ChainId
from ..services.blockchain_pool import BlockchainPool
from ..config import Config

logger = logging.getLogger(__name__)


class InfrastructureAMMProtocol:
    """Protocol for infrastructure resource token AMM pools"""
    
    def __init__(self, blockchain_pool: BlockchainPool, config: Config):
        self.blockchain_pool = blockchain_pool
        self.config = config
        self._pools = {}  # pool_id -> pool details
        self._next_pool_id = 0
        self._pool_analytics = {}  # pool_id -> analytics data
        
    async def initialize(self):
        """Initialize AMM protocol"""
        # In production, this would connect to smart contracts
        logger.info("Initialized Infrastructure AMM Protocol")
        
    async def create_pool(
        self,
        creator: str,
        resource_token_id: int,
        payment_token: str,
        initial_resource_amount: int,
        initial_payment_amount: Decimal,
        fee_rate: int = 30  # basis points
    ) -> Dict[str, Any]:
        """
        Create a new AMM pool for resource tokens
        
        Args:
            creator: Pool creator address
            resource_token_id: ID of the resource token
            payment_token: Address of payment token (e.g., USDC)
            initial_resource_amount: Initial resource token liquidity
            initial_payment_amount: Initial payment token liquidity
            fee_rate: Trading fee in basis points
            
        Returns:
            Pool creation details
        """
        try:
            pool_id = self._next_pool_id
            self._next_pool_id += 1
            
            # Calculate initial k constant
            k_constant = Decimal(initial_resource_amount) * initial_payment_amount
            
            # Create LP token (in production, this would deploy a contract)
            lp_token_address = f"LP-{pool_id}-{resource_token_id}"
            
            # Calculate initial LP tokens (sqrt of k)
            lp_tokens_minted = Decimal(str(k_constant ** Decimal('0.5')))
            
            # Create pool
            pool = {
                "pool_id": pool_id,
                "creator": creator,
                "resource_token_id": resource_token_id,
                "payment_token": payment_token,
                "resource_reserves": initial_resource_amount,
                "payment_reserves": initial_payment_amount,
                "k_constant": k_constant,
                "fee_rate": fee_rate,
                "total_lp_tokens": lp_tokens_minted,
                "lp_token_address": lp_token_address,
                "created_at": datetime.utcnow(),
                "is_active": True,
                "liquidity_providers": {
                    creator: {
                        "lp_tokens": lp_tokens_minted,
                        "resource_deposited": initial_resource_amount,
                        "payment_deposited": initial_payment_amount
                    }
                }
            }
            
            self._pools[pool_id] = pool
            
            # Initialize analytics
            self._pool_analytics[pool_id] = {
                "volume_24h": Decimal("0"),
                "fees_24h": Decimal("0"),
                "trades_24h": 0,
                "price_history": [(datetime.utcnow(), initial_payment_amount / initial_resource_amount)]
            }
            
            return {
                "pool_id": pool_id,
                "lp_tokens_minted": lp_tokens_minted,
                "lp_token_address": lp_token_address,
                "initial_price": initial_payment_amount / initial_resource_amount
            }
            
        except Exception as e:
            logger.error(f"Error creating pool: {e}")
            raise
            
    async def add_liquidity(
        self,
        pool_id: int,
        provider: str,
        resource_amount: int,
        max_payment_amount: Decimal,
        slippage_tolerance: Decimal = Decimal("0.01")
    ) -> Dict[str, Any]:
        """
        Add liquidity to an existing pool
        
        Args:
            pool_id: AMM pool ID
            provider: Liquidity provider address
            resource_amount: Amount of resource tokens to add
            max_payment_amount: Maximum payment tokens to add
            slippage_tolerance: Maximum acceptable slippage
            
        Returns:
            Liquidity addition details
        """
        try:
            pool = self._pools.get(pool_id)
            if not pool:
                raise ValueError("Pool not found")
                
            if not pool["is_active"]:
                raise ValueError("Pool is not active")
                
            # Calculate required payment amount based on current ratio
            current_ratio = pool["payment_reserves"] / pool["resource_reserves"]
            required_payment = Decimal(resource_amount) * current_ratio
            
            # Check slippage
            if required_payment > max_payment_amount:
                if (required_payment - max_payment_amount) / max_payment_amount > slippage_tolerance:
                    raise ValueError("Slippage tolerance exceeded")
                    
            # Calculate LP tokens to mint
            total_lp = pool["total_lp_tokens"]
            resource_ratio = Decimal(resource_amount) / Decimal(pool["resource_reserves"])
            lp_tokens_to_mint = total_lp * resource_ratio
            
            # Update pool reserves
            pool["resource_reserves"] += resource_amount
            pool["payment_reserves"] += required_payment
            pool["k_constant"] = Decimal(pool["resource_reserves"]) * pool["payment_reserves"]
            pool["total_lp_tokens"] += lp_tokens_to_mint
            
            # Update provider's position
            if provider not in pool["liquidity_providers"]:
                pool["liquidity_providers"][provider] = {
                    "lp_tokens": Decimal("0"),
                    "resource_deposited": 0,
                    "payment_deposited": Decimal("0")
                }
                
            position = pool["liquidity_providers"][provider]
            position["lp_tokens"] += lp_tokens_to_mint
            position["resource_deposited"] += resource_amount
            position["payment_deposited"] += required_payment
            
            # Calculate share percentage
            share_percentage = (position["lp_tokens"] / pool["total_lp_tokens"]) * 100
            
            return {
                "resource_added": resource_amount,
                "payment_added": required_payment,
                "lp_tokens_minted": lp_tokens_to_mint,
                "share_percentage": float(share_percentage),
                "tx_hash": f"0x{pool_id:064x}"  # Mock tx hash
            }
            
        except Exception as e:
            logger.error(f"Error adding liquidity: {e}")
            raise
            
    async def remove_liquidity(
        self,
        pool_id: int,
        provider: str,
        lp_token_amount: Decimal,
        min_resource_amount: int = 0,
        min_payment_amount: Decimal = Decimal("0")
    ) -> Dict[str, Any]:
        """
        Remove liquidity from a pool
        
        Args:
            pool_id: AMM pool ID
            provider: Liquidity provider address
            lp_token_amount: Amount of LP tokens to burn
            min_resource_amount: Minimum resource tokens to receive
            min_payment_amount: Minimum payment tokens to receive
            
        Returns:
            Liquidity removal details
        """
        try:
            pool = self._pools.get(pool_id)
            if not pool:
                raise ValueError("Pool not found")
                
            position = pool["liquidity_providers"].get(provider)
            if not position:
                raise ValueError("No liquidity position found")
                
            if position["lp_tokens"] < lp_token_amount:
                raise ValueError("Insufficient LP tokens")
                
            # Calculate tokens to return
            lp_ratio = lp_token_amount / pool["total_lp_tokens"]
            resource_to_return = int(Decimal(pool["resource_reserves"]) * lp_ratio)
            payment_to_return = pool["payment_reserves"] * lp_ratio
            
            # Check minimum amounts
            if resource_to_return < min_resource_amount:
                raise ValueError("Resource amount below minimum")
            if payment_to_return < min_payment_amount:
                raise ValueError("Payment amount below minimum")
                
            # Update pool
            pool["resource_reserves"] -= resource_to_return
            pool["payment_reserves"] -= payment_to_return
            pool["k_constant"] = Decimal(pool["resource_reserves"]) * pool["payment_reserves"]
            pool["total_lp_tokens"] -= lp_token_amount
            
            # Update position
            position["lp_tokens"] -= lp_token_amount
            
            # Calculate remaining share
            remaining_share = 0
            if pool["total_lp_tokens"] > 0:
                remaining_share = (position["lp_tokens"] / pool["total_lp_tokens"]) * 100
                
            return {
                "resource_returned": resource_to_return,
                "payment_returned": payment_to_return,
                "remaining_share": float(remaining_share),
                "tx_hash": f"0x{pool_id:064x}"
            }
            
        except Exception as e:
            logger.error(f"Error removing liquidity: {e}")
            raise
            
    async def swap(
        self,
        pool_id: int,
        trader: str,
        input_is_resource: bool,
        input_amount: Decimal,
        min_output_amount: Decimal
    ) -> Dict[str, Any]:
        """
        Swap tokens in an AMM pool
        
        Args:
            pool_id: AMM pool ID
            trader: Trader address
            input_is_resource: True if swapping resource for payment
            input_amount: Amount of input tokens
            min_output_amount: Minimum output tokens to receive
            
        Returns:
            Swap details
        """
        try:
            pool = self._pools.get(pool_id)
            if not pool:
                raise ValueError("Pool not found")
                
            if not pool["is_active"]:
                raise ValueError("Pool is not active")
                
            # Calculate output amount using constant product formula
            if input_is_resource:
                # Swapping resource tokens for payment tokens
                input_reserve = Decimal(pool["resource_reserves"])
                output_reserve = pool["payment_reserves"]
                input_amount_int = int(input_amount)
            else:
                # Swapping payment tokens for resource tokens
                input_reserve = pool["payment_reserves"]
                output_reserve = Decimal(pool["resource_reserves"])
                input_amount_int = input_amount
                
            # Apply fee
            fee_rate = Decimal(pool["fee_rate"]) / Decimal("10000")
            input_with_fee = input_amount * (Decimal("1") - fee_rate)
            fee_amount = input_amount - input_with_fee
            
            # Calculate output amount (x * y = k)
            # output = (output_reserve * input_with_fee) / (input_reserve + input_with_fee)
            output_amount = (output_reserve * input_with_fee) / (input_reserve + input_with_fee)
            
            if input_is_resource:
                output_amount_final = output_amount
            else:
                output_amount_final = int(output_amount)
                
            # Check minimum output
            if output_amount < min_output_amount:
                raise ValueError("Output amount below minimum")
                
            # Calculate price impact
            spot_price = output_reserve / input_reserve
            effective_price = output_amount / input_amount
            price_impact = abs(spot_price - effective_price) / spot_price
            
            # Update pool reserves
            if input_is_resource:
                pool["resource_reserves"] += input_amount_int
                pool["payment_reserves"] -= output_amount_final
                input_token = f"RESOURCE-{pool['resource_token_id']}"
                output_token = pool["payment_token"]
            else:
                pool["payment_reserves"] += input_amount
                pool["resource_reserves"] -= output_amount_final
                input_token = pool["payment_token"]
                output_token = f"RESOURCE-{pool['resource_token_id']}"
                
            # Update analytics
            analytics = self._pool_analytics[pool_id]
            analytics["volume_24h"] += input_amount
            analytics["fees_24h"] += fee_amount
            analytics["trades_24h"] += 1
            
            # Update price history
            new_price = pool["payment_reserves"] / Decimal(pool["resource_reserves"])
            analytics["price_history"].append((datetime.utcnow(), new_price))
            
            return {
                "input_token": input_token,
                "output_token": output_token,
                "output_amount": output_amount_final,
                "fee_amount": fee_amount,
                "price_impact": float(price_impact),
                "effective_price": float(effective_price),
                "tx_hash": f"0x{pool_id:064x}"
            }
            
        except Exception as e:
            logger.error(f"Error swapping tokens: {e}")
            raise
            
    async def get_swap_quote(
        self,
        pool_id: int,
        input_is_resource: bool,
        input_amount: Decimal
    ) -> Dict[str, Any]:
        """Get a quote for a potential swap without executing"""
        pool = self._pools.get(pool_id)
        if not pool:
            raise ValueError("Pool not found")
            
        # Calculate output amount
        if input_is_resource:
            input_reserve = Decimal(pool["resource_reserves"])
            output_reserve = pool["payment_reserves"]
        else:
            input_reserve = pool["payment_reserves"]
            output_reserve = Decimal(pool["resource_reserves"])
            
        # Apply fee
        fee_rate = Decimal(pool["fee_rate"]) / Decimal("10000")
        input_with_fee = input_amount * (Decimal("1") - fee_rate)
        fee_amount = input_amount - input_with_fee
        
        # Calculate output
        output_amount = (output_reserve * input_with_fee) / (input_reserve + input_with_fee)
        
        # Calculate price impact
        spot_price = output_reserve / input_reserve
        effective_price = output_amount / input_amount
        price_impact = abs(spot_price - effective_price) / spot_price
        
        return {
            "input_amount": float(input_amount),
            "output_amount": float(output_amount),
            "fee_amount": float(fee_amount),
            "price_impact": float(price_impact),
            "effective_price": float(effective_price),
            "spot_price": float(spot_price)
        }
        
    async def list_pools(
        self,
        resource_type: Optional[ResourceType] = None,
        payment_token: Optional[str] = None,
        active_only: bool = True
    ) -> List[Dict[str, Any]]:
        """List AMM pools with optional filters"""
        pools = []
        
        for pool_id, pool in self._pools.items():
            # Apply filters
            if active_only and not pool["is_active"]:
                continue
                
            if payment_token and pool["payment_token"] != payment_token:
                continue
                
            # TODO: Filter by resource type (requires mapping token ID to type)
            
            # Get analytics
            analytics = self._pool_analytics.get(pool_id, {})
            
            # Calculate current price
            price = pool["payment_reserves"] / Decimal(pool["resource_reserves"])
            
            # Calculate APY based on fees
            if pool["total_lp_tokens"] > 0:
                daily_fees = analytics.get("fees_24h", Decimal("0"))
                pool_value = pool["payment_reserves"] * 2  # Rough estimate
                daily_yield = daily_fees / pool_value if pool_value > 0 else Decimal("0")
                apy = daily_yield * Decimal("365") * Decimal("100")  # Annual percentage
            else:
                apy = Decimal("0")
                
            pools.append({
                "pool_id": pool_id,
                "resource_token_id": pool["resource_token_id"],
                "payment_token": pool["payment_token"],
                "resource_reserves": pool["resource_reserves"],
                "payment_reserves": float(pool["payment_reserves"]),
                "total_lp_tokens": float(pool["total_lp_tokens"]),
                "lp_token_address": pool["lp_token_address"],
                "fee_rate": pool["fee_rate"] / 100,  # Convert to percentage
                "price": float(price),
                "volume_24h": float(analytics.get("volume_24h", 0)),
                "fees_24h": float(analytics.get("fees_24h", 0)),
                "apy": float(apy)
            })
            
        return pools
        
    async def get_pool_details(self, pool_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific pool"""
        pool = self._pools.get(pool_id)
        if not pool:
            raise ValueError("Pool not found")
            
        analytics = self._pool_analytics.get(pool_id, {})
        price = pool["payment_reserves"] / Decimal(pool["resource_reserves"])
        
        # Calculate APY
        if pool["total_lp_tokens"] > 0:
            daily_fees = analytics.get("fees_24h", Decimal("0"))
            pool_value = pool["payment_reserves"] * 2
            daily_yield = daily_fees / pool_value if pool_value > 0 else Decimal("0")
            apy = daily_yield * Decimal("365") * Decimal("100")
        else:
            apy = Decimal("0")
            
        return {
            "pool_id": pool_id,
            "resource_token_id": pool["resource_token_id"],
            "payment_token": pool["payment_token"],
            "resource_reserves": pool["resource_reserves"],
            "payment_reserves": float(pool["payment_reserves"]),
            "total_lp_tokens": float(pool["total_lp_tokens"]),
            "lp_token_address": pool["lp_token_address"],
            "fee_rate": pool["fee_rate"] / 100,
            "price": float(price),
            "volume_24h": float(analytics.get("volume_24h", 0)),
            "fees_24h": float(analytics.get("fees_24h", 0)),
            "apy": float(apy),
            "created_at": pool["created_at"].isoformat(),
            "liquidity_providers": len(pool["liquidity_providers"])
        }
        
    async def get_user_positions(self, user: str) -> List[Dict[str, Any]]:
        """Get all liquidity positions for a user"""
        positions = []
        
        for pool_id, pool in self._pools.items():
            if user in pool["liquidity_providers"]:
                position = pool["liquidity_providers"][user]
                share_percentage = (position["lp_tokens"] / pool["total_lp_tokens"]) * 100 if pool["total_lp_tokens"] > 0 else 0
                
                # Calculate current value
                resource_value = (Decimal(pool["resource_reserves"]) * share_percentage / 100)
                payment_value = (pool["payment_reserves"] * share_percentage / 100)
                
                positions.append({
                    "pool_id": pool_id,
                    "lp_tokens": float(position["lp_tokens"]),
                    "share_percentage": float(share_percentage),
                    "resource_amount": int(resource_value),
                    "payment_amount": float(payment_value),
                    "resource_deposited": position["resource_deposited"],
                    "payment_deposited": float(position["payment_deposited"]),
                    "unclaimed_fees": 0  # TODO: Track fees separately
                })
                
        return positions
        
    async def get_protocol_stats(self) -> Dict[str, Any]:
        """Get overall AMM protocol statistics"""
        total_pools = len(self._pools)
        active_pools = sum(1 for p in self._pools.values() if p["is_active"])
        
        total_volume_24h = Decimal("0")
        total_fees_24h = Decimal("0")
        total_value_locked = Decimal("0")
        
        resource_breakdown = {}
        top_pools = []
        
        for pool_id, pool in self._pools.items():
            if not pool["is_active"]:
                continue
                
            analytics = self._pool_analytics.get(pool_id, {})
            
            # Aggregate volume and fees
            total_volume_24h += analytics.get("volume_24h", Decimal("0"))
            total_fees_24h += analytics.get("fees_24h", Decimal("0"))
            
            # Calculate TVL (payment reserves * 2 as approximation)
            pool_tvl = pool["payment_reserves"] * 2
            total_value_locked += pool_tvl
            
            # Track by resource type (TODO: map token ID to type)
            resource_key = f"RESOURCE-{pool['resource_token_id']}"
            if resource_key not in resource_breakdown:
                resource_breakdown[resource_key] = Decimal("0")
            resource_breakdown[resource_key] += pool_tvl
            
            # Track top pools
            top_pools.append({
                "pool_id": pool_id,
                "tvl": float(pool_tvl),
                "volume_24h": float(analytics.get("volume_24h", 0))
            })
            
        # Sort top pools by TVL
        top_pools.sort(key=lambda x: x["tvl"], reverse=True)
        
        return {
            "total_pools": total_pools,
            "active_pools": active_pools,
            "total_volume_24h": float(total_volume_24h),
            "total_fees_24h": float(total_fees_24h),
            "tvl": float(total_value_locked),
            "top_pools": top_pools[:10],  # Top 10 pools
            "resource_breakdown": {k: float(v) for k, v in resource_breakdown.items()}
        } 