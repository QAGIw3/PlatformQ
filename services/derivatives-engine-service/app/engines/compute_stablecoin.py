"""
Compute-Backed Stablecoin Engine

Stablecoins pegged to computational units (FLOPS, GPU-hours, etc.)
"""

from typing import Dict, List, Optional, Tuple, Any, Set
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
import uuid
from collections import defaultdict
import numpy as np

from app.integrations import (
    IgniteCache,
    PulsarEventPublisher,
    OracleAggregatorClient,
    BlockchainEventBridgeClient
)
from app.engines.compute_spot_market import ComputeSpotMarket
from app.collateral.multi_tier_engine import MultiTierCollateralEngine

logger = logging.getLogger(__name__)


class StablecoinType(Enum):
    """Types of compute-backed stablecoins"""
    FLOPS_COIN = "flops_coin"       # Pegged to TFLOPS
    GPU_HOUR_COIN = "gpu_hour_coin" # Pegged to GPU compute hours
    STORAGE_COIN = "storage_coin"   # Pegged to TB storage
    BANDWIDTH_COIN = "bandwidth_coin" # Pegged to Gbps bandwidth
    COMPUTE_BASKET = "compute_basket" # Basket of compute resources


class PegMechanism(Enum):
    """Stabilization mechanisms"""
    ALGORITHMIC = "algorithmic"      # Pure algorithmic stabilization
    COLLATERALIZED = "collateralized" # Backed by compute collateral
    HYBRID = "hybrid"               # Mix of algorithmic and collateral
    REBASE = "rebase"              # Elastic supply rebase


class StabilizationAction(Enum):
    """Actions to maintain peg"""
    MINT = "mint"                   # Mint new coins
    BURN = "burn"                   # Burn coins
    REBASE_EXPAND = "rebase_expand" # Expand supply
    REBASE_CONTRACT = "rebase_contract" # Contract supply
    ADJUST_COLLATERAL = "adjust_collateral" # Change collateral ratio
    ARBITRAGE = "arbitrage"         # Enable arbitrage


@dataclass
class ComputeStablecoin:
    """Compute-backed stablecoin"""
    coin_id: str
    symbol: str
    name: str
    coin_type: StablecoinType
    peg_mechanism: PegMechanism
    peg_target: Decimal  # Target value in compute units
    total_supply: Decimal
    circulating_supply: Decimal
    collateral_ratio: Decimal  # 0 to 1
    rebase_enabled: bool
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def market_cap(self) -> Decimal:
        """Market cap in compute units"""
        return self.circulating_supply * self.peg_target


@dataclass
class CollateralVault:
    """Vault holding compute collateral"""
    vault_id: str
    coin_id: str
    collateral_type: str  # Resource type
    locked_amount: Decimal  # Amount of compute resources locked
    value_locked: Decimal  # Value in target units
    last_update: datetime = field(default_factory=datetime.utcnow)
    providers: List[str] = field(default_factory=list)  # Compute providers


@dataclass
class MintRequest:
    """Request to mint stablecoins"""
    request_id: str
    user_id: str
    coin_id: str
    amount_to_mint: Decimal
    collateral_provided: Dict[str, Decimal]  # Resource type -> amount
    timestamp: datetime = field(default_factory=datetime.utcnow)
    status: str = "pending"  # pending, approved, rejected, completed


@dataclass
class RedemptionRequest:
    """Request to redeem stablecoins for compute"""
    request_id: str
    user_id: str
    coin_id: str
    amount_to_redeem: Decimal
    preferred_resource: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    status: str = "pending"


@dataclass
class StabilizationEvent:
    """Record of stabilization action"""
    event_id: str
    coin_id: str
    action: StabilizationAction
    trigger_price: Decimal
    target_price: Decimal
    amount: Decimal
    effectiveness: Optional[Decimal] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)


class ComputeStablecoinEngine:
    """
    Engine for compute-backed stablecoins pegged to computational units
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        blockchain_bridge: BlockchainEventBridgeClient,
        spot_market: ComputeSpotMarket,
        collateral_engine: MultiTierCollateralEngine
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.blockchain_bridge = blockchain_bridge
        self.spot_market = spot_market
        self.collateral_engine = collateral_engine
        
        # Stablecoin registry
        self.stablecoins: Dict[str, ComputeStablecoin] = {}
        
        # Collateral vaults
        self.vaults: Dict[str, List[CollateralVault]] = defaultdict(list)
        
        # Pending requests
        self.mint_requests: Dict[str, MintRequest] = {}
        self.redemption_requests: Dict[str, RedemptionRequest] = {}
        
        # User balances
        self.balances: Dict[str, Dict[str, Decimal]] = defaultdict(lambda: defaultdict(Decimal))
        
        # Price history for stabilization
        self.price_history: Dict[str, List[Tuple[datetime, Decimal]]] = defaultdict(list)
        
        # Stabilization parameters
        self.stabilization_params = {
            "deviation_threshold": Decimal("0.01"),  # 1% deviation triggers action
            "rebase_threshold": Decimal("0.05"),     # 5% deviation triggers rebase
            "arbitrage_incentive": Decimal("0.005"), # 0.5% arbitrage profit
            "stabilization_interval": 60,            # seconds
            "price_sample_interval": 10              # seconds
        }
        
        # Background tasks
        self._price_monitoring_task = None
        self._stabilization_task = None
        self._collateral_management_task = None
        
    async def start(self):
        """Start stablecoin engine"""
        # Initialize default stablecoins
        await self._initialize_default_coins()
        
        # Load existing state
        await self._load_state()
        
        # Start background tasks
        self._price_monitoring_task = asyncio.create_task(self._price_monitoring_loop())
        self._stabilization_task = asyncio.create_task(self._stabilization_loop())
        self._collateral_management_task = asyncio.create_task(self._collateral_management_loop())
        
        logger.info("Compute stablecoin engine started")
        
    async def stop(self):
        """Stop stablecoin engine"""
        if self._price_monitoring_task:
            self._price_monitoring_task.cancel()
        if self._stabilization_task:
            self._stabilization_task.cancel()
        if self._collateral_management_task:
            self._collateral_management_task.cancel()
            
    async def create_stablecoin(
        self,
        symbol: str,
        name: str,
        coin_type: StablecoinType,
        peg_mechanism: PegMechanism,
        peg_target: Decimal,
        initial_collateral_ratio: Decimal = Decimal("1.0"),
        rebase_enabled: bool = False
    ) -> ComputeStablecoin:
        """Create a new compute-backed stablecoin"""
        
        # Validate parameters
        if peg_target <= 0:
            raise ValueError("Peg target must be positive")
            
        if not 0 <= initial_collateral_ratio <= 1:
            raise ValueError("Collateral ratio must be between 0 and 1")
            
        # Create stablecoin
        coin = ComputeStablecoin(
            coin_id=f"STABLE_{symbol}_{uuid.uuid4().hex[:8]}",
            symbol=symbol,
            name=name,
            coin_type=coin_type,
            peg_mechanism=peg_mechanism,
            peg_target=peg_target,
            total_supply=Decimal("0"),
            circulating_supply=Decimal("0"),
            collateral_ratio=initial_collateral_ratio,
            rebase_enabled=rebase_enabled
        )
        
        # Store
        self.stablecoins[coin.coin_id] = coin
        await self._store_stablecoin(coin)
        
        # Deploy on blockchain if needed
        if peg_mechanism != PegMechanism.ALGORITHMIC:
            await self._deploy_stablecoin_contract(coin)
            
        # Emit event
        await self.pulsar.publish('compute.stablecoin.created', {
            'coin_id': coin.coin_id,
            'symbol': symbol,
            'name': name,
            'type': coin_type.value,
            'peg_target': str(peg_target),
            'mechanism': peg_mechanism.value
        })
        
        return coin
        
    async def mint_stablecoin(
        self,
        user_id: str,
        coin_id: str,
        amount: Decimal,
        collateral: Dict[str, Decimal]
    ) -> Dict[str, Any]:
        """Mint new stablecoins against compute collateral"""
        
        coin = self.stablecoins.get(coin_id)
        if not coin:
            raise ValueError(f"Stablecoin {coin_id} not found")
            
        # Calculate required collateral value
        required_value = amount * coin.peg_target * coin.collateral_ratio
        
        # Value provided collateral
        collateral_value = await self._value_collateral(collateral, coin.coin_type)
        
        if collateral_value < required_value:
            return {
                "success": False,
                "error": "Insufficient collateral",
                "required": str(required_value),
                "provided": str(collateral_value)
            }
            
        # Create mint request
        request = MintRequest(
            request_id=f"MINT_{uuid.uuid4().hex}",
            user_id=user_id,
            coin_id=coin_id,
            amount_to_mint=amount,
            collateral_provided=collateral
        )
        
        # Lock collateral
        vault = await self._lock_collateral(coin, collateral, collateral_value)
        
        # Mint coins
        coin.total_supply += amount
        coin.circulating_supply += amount
        self.balances[user_id][coin_id] += amount
        
        # Update state
        request.status = "completed"
        self.mint_requests[request.request_id] = request
        await self._update_stablecoin(coin)
        await self._store_mint_request(request)
        
        # Emit event
        await self.pulsar.publish('compute.stablecoin.minted', {
            'request_id': request.request_id,
            'user_id': user_id,
            'coin_id': coin_id,
            'amount': str(amount),
            'collateral_value': str(collateral_value),
            'vault_id': vault.vault_id
        })
        
        return {
            "success": True,
            "request_id": request.request_id,
            "amount_minted": str(amount),
            "collateral_locked": str(collateral_value),
            "vault_id": vault.vault_id,
            "new_balance": str(self.balances[user_id][coin_id])
        }
        
    async def redeem_stablecoin(
        self,
        user_id: str,
        coin_id: str,
        amount: Decimal,
        preferred_resource: Optional[str] = None
    ) -> Dict[str, Any]:
        """Redeem stablecoins for compute resources"""
        
        coin = self.stablecoins.get(coin_id)
        if not coin:
            raise ValueError(f"Stablecoin {coin_id} not found")
            
        # Check balance
        user_balance = self.balances[user_id][coin_id]
        if user_balance < amount:
            return {
                "success": False,
                "error": "Insufficient balance",
                "balance": str(user_balance),
                "requested": str(amount)
            }
            
        # Create redemption request
        request = RedemptionRequest(
            request_id=f"REDEEM_{uuid.uuid4().hex}",
            user_id=user_id,
            coin_id=coin_id,
            amount_to_redeem=amount,
            preferred_resource=preferred_resource
        )
        
        # Calculate compute value
        compute_value = amount * coin.peg_target
        
        # Allocate compute resources
        allocation = await self._allocate_compute_for_redemption(
            coin,
            compute_value,
            preferred_resource
        )
        
        if not allocation["success"]:
            return {
                "success": False,
                "error": "Unable to allocate compute resources",
                "details": allocation.get("error")
            }
            
        # Burn coins
        self.balances[user_id][coin_id] -= amount
        coin.circulating_supply -= amount
        coin.total_supply -= amount
        
        # Release collateral proportionally
        await self._release_collateral(coin, amount)
        
        # Update state
        request.status = "completed"
        self.redemption_requests[request.request_id] = request
        await self._update_stablecoin(coin)
        await self._store_redemption_request(request)
        
        # Emit event
        await self.pulsar.publish('compute.stablecoin.redeemed', {
            'request_id': request.request_id,
            'user_id': user_id,
            'coin_id': coin_id,
            'amount': str(amount),
            'compute_allocated': allocation["resources"]
        })
        
        return {
            "success": True,
            "request_id": request.request_id,
            "amount_redeemed": str(amount),
            "compute_allocated": allocation["resources"],
            "access_details": allocation.get("access_details", {}),
            "new_balance": str(self.balances[user_id][coin_id])
        }
        
    async def get_coin_price(
        self,
        coin_id: str
    ) -> Dict[str, Any]:
        """Get current price of stablecoin in compute units"""
        
        coin = self.stablecoins.get(coin_id)
        if not coin:
            raise ValueError(f"Stablecoin {coin_id} not found")
            
        # Get market price based on trading activity
        market_price = await self._get_market_price(coin)
        
        # Get collateral backing
        collateral_value = await self._get_total_collateral_value(coin)
        
        # Calculate metrics
        deviation = abs(market_price - coin.peg_target) / coin.peg_target
        collateral_coverage = collateral_value / (coin.circulating_supply * coin.peg_target) if coin.circulating_supply > 0 else Decimal("0")
        
        return {
            "coin_id": coin_id,
            "symbol": coin.symbol,
            "peg_target": str(coin.peg_target),
            "market_price": str(market_price),
            "deviation": str(deviation),
            "deviation_percent": str(deviation * 100),
            "collateral_ratio": str(coin.collateral_ratio),
            "collateral_coverage": str(collateral_coverage),
            "circulating_supply": str(coin.circulating_supply),
            "market_cap": str(coin.market_cap),
            "last_update": datetime.utcnow().isoformat()
        }
        
    async def rebase_supply(
        self,
        coin_id: str
    ) -> Dict[str, Any]:
        """Rebase coin supply to maintain peg"""
        
        coin = self.stablecoins.get(coin_id)
        if not coin:
            raise ValueError(f"Stablecoin {coin_id} not found")
            
        if not coin.rebase_enabled:
            return {"success": False, "error": "Rebase not enabled for this coin"}
            
        # Get current price
        market_price = await self._get_market_price(coin)
        deviation = (market_price - coin.peg_target) / coin.peg_target
        
        if abs(deviation) < self.stabilization_params["rebase_threshold"]:
            return {
                "success": False,
                "error": "Deviation below rebase threshold",
                "deviation": str(deviation)
            }
            
        # Calculate rebase factor
        rebase_factor = coin.peg_target / market_price
        
        # Apply rebase to all balances
        old_supply = coin.circulating_supply
        new_supply = old_supply * rebase_factor
        
        for user_id in self.balances:
            if coin_id in self.balances[user_id]:
                old_balance = self.balances[user_id][coin_id]
                new_balance = old_balance * rebase_factor
                self.balances[user_id][coin_id] = new_balance
                
        # Update supply
        coin.circulating_supply = new_supply
        coin.total_supply = coin.total_supply * rebase_factor
        
        # Store rebase event
        event = StabilizationEvent(
            event_id=f"REBASE_{uuid.uuid4().hex}",
            coin_id=coin_id,
            action=StabilizationAction.REBASE_EXPAND if rebase_factor > 1 else StabilizationAction.REBASE_CONTRACT,
            trigger_price=market_price,
            target_price=coin.peg_target,
            amount=abs(new_supply - old_supply)
        )
        
        await self._store_stabilization_event(event)
        await self._update_stablecoin(coin)
        
        # Emit event
        await self.pulsar.publish('compute.stablecoin.rebased', {
            'coin_id': coin_id,
            'rebase_factor': str(rebase_factor),
            'old_supply': str(old_supply),
            'new_supply': str(new_supply),
            'deviation': str(deviation)
        })
        
        return {
            "success": True,
            "coin_id": coin_id,
            "rebase_factor": str(rebase_factor),
            "old_supply": str(old_supply),
            "new_supply": str(new_supply),
            "supply_change": str(new_supply - old_supply),
            "supply_change_percent": str((rebase_factor - 1) * 100)
        }
        
    async def create_compute_basket_coin(
        self,
        symbol: str,
        name: str,
        basket_composition: Dict[str, Decimal]  # Resource type -> weight
    ) -> ComputeStablecoin:
        """Create a stablecoin backed by a basket of compute resources"""
        
        # Validate weights sum to 1
        total_weight = sum(basket_composition.values())
        if abs(total_weight - Decimal("1")) > Decimal("0.001"):
            raise ValueError("Basket weights must sum to 1")
            
        # Calculate initial peg target based on basket
        peg_target = Decimal("0")
        for resource, weight in basket_composition.items():
            resource_price = await self._get_compute_unit_price(resource)
            peg_target += resource_price * weight
            
        # Create basket coin
        coin = await self.create_stablecoin(
            symbol=symbol,
            name=name,
            coin_type=StablecoinType.COMPUTE_BASKET,
            peg_mechanism=PegMechanism.HYBRID,
            peg_target=peg_target,
            initial_collateral_ratio=Decimal("1.0"),
            rebase_enabled=True
        )
        
        # Store basket composition
        await self.ignite.put(
            f"basket_composition:{coin.coin_id}",
            basket_composition
        )
        
        return coin
        
    async def _value_collateral(
        self,
        collateral: Dict[str, Decimal],
        target_coin_type: StablecoinType
    ) -> Decimal:
        """Value collateral in terms of target coin units"""
        
        total_value = Decimal("0")
        
        for resource_type, amount in collateral.items():
            # Get resource price
            spot_data = await self.spot_market.get_spot_price(resource_type)
            resource_price = Decimal(spot_data.get("last_trade_price", "0"))
            
            if resource_price == 0:
                continue
                
            # Convert to target units
            if target_coin_type == StablecoinType.FLOPS_COIN:
                # Convert to TFLOPS
                flops_per_unit = await self._get_flops_per_unit(resource_type)
                value_in_flops = amount * flops_per_unit
                total_value += value_in_flops
                
            elif target_coin_type == StablecoinType.GPU_HOUR_COIN:
                # Convert to GPU hours
                if "gpu" in resource_type.lower():
                    total_value += amount
                else:
                    # Convert other resources to GPU hour equivalent
                    conversion_rate = await self._get_gpu_hour_equivalent(resource_type)
                    total_value += amount * conversion_rate
                    
            else:
                # Default to USD value
                total_value += amount * resource_price
                
        return total_value
        
    async def _lock_collateral(
        self,
        coin: ComputeStablecoin,
        collateral: Dict[str, Decimal],
        total_value: Decimal
    ) -> CollateralVault:
        """Lock collateral in vault"""
        
        # Create vault
        vault = CollateralVault(
            vault_id=f"VAULT_{uuid.uuid4().hex}",
            coin_id=coin.coin_id,
            collateral_type="mixed",  # Multiple resource types
            locked_amount=sum(collateral.values()),
            value_locked=total_value
        )
        
        # Lock each resource type
        for resource_type, amount in collateral.items():
            # Reserve capacity with partners
            providers = await self._reserve_compute_capacity(resource_type, amount)
            vault.providers.extend(providers)
            
        # Store vault
        self.vaults[coin.coin_id].append(vault)
        await self._store_vault(vault)
        
        return vault
        
    async def _allocate_compute_for_redemption(
        self,
        coin: ComputeStablecoin,
        compute_value: Decimal,
        preferred_resource: Optional[str]
    ) -> Dict[str, Any]:
        """Allocate compute resources for redemption"""
        
        allocated_resources = {}
        
        if coin.coin_type == StablecoinType.COMPUTE_BASKET:
            # Get basket composition
            composition = await self.ignite.get(f"basket_composition:{coin.coin_id}")
            
            for resource_type, weight in composition.items():
                amount_needed = compute_value * weight
                
                # Try to allocate from spot market
                spot_order = await self.spot_market.submit_order({
                    "user_id": "STABLECOIN_REDEMPTION",
                    "order_type": "market",
                    "side": "buy",
                    "resource_type": resource_type,
                    "quantity": amount_needed
                })
                
                if spot_order["success"]:
                    allocated_resources[resource_type] = {
                        "amount": str(amount_needed),
                        "order_id": spot_order["order_id"],
                        "trades": spot_order.get("trades", [])
                    }
                    
        else:
            # Single resource type
            resource_type = preferred_resource or self._get_default_resource(coin.coin_type)
            
            spot_order = await self.spot_market.submit_order({
                "user_id": "STABLECOIN_REDEMPTION",
                "order_type": "market",
                "side": "buy",
                "resource_type": resource_type,
                "quantity": compute_value
            })
            
            if spot_order["success"]:
                allocated_resources[resource_type] = {
                    "amount": str(compute_value),
                    "order_id": spot_order["order_id"],
                    "trades": spot_order.get("trades", [])
                }
                
        if not allocated_resources:
            return {"success": False, "error": "Unable to allocate compute"}
            
        return {
            "success": True,
            "resources": allocated_resources,
            "total_value": str(compute_value)
        }
        
    async def _release_collateral(
        self,
        coin: ComputeStablecoin,
        amount_redeemed: Decimal
    ):
        """Release collateral proportionally"""
        
        if coin.circulating_supply == 0:
            return
            
        release_ratio = amount_redeemed / coin.circulating_supply
        
        for vault in self.vaults[coin.coin_id]:
            release_amount = vault.locked_amount * release_ratio
            vault.locked_amount -= release_amount
            vault.value_locked -= vault.value_locked * release_ratio
            
            await self._update_vault(vault)
            
    async def _get_market_price(
        self,
        coin: ComputeStablecoin
    ) -> Decimal:
        """Get market price from DEX or oracle"""
        
        # Check if coin is traded on DEX
        # For now, simulate with small random deviation
        base_price = coin.peg_target
        deviation = Decimal(str(np.random.normal(0, 0.01)))  # 1% std dev
        
        market_price = base_price * (Decimal("1") + deviation)
        
        # Store price sample
        self.price_history[coin.coin_id].append((datetime.utcnow(), market_price))
        
        # Clean old history
        cutoff = datetime.utcnow() - timedelta(hours=24)
        self.price_history[coin.coin_id] = [
            (t, p) for t, p in self.price_history[coin.coin_id]
            if t > cutoff
        ]
        
        return market_price
        
    async def _get_total_collateral_value(
        self,
        coin: ComputeStablecoin
    ) -> Decimal:
        """Get total value of locked collateral"""
        
        total = Decimal("0")
        for vault in self.vaults[coin.coin_id]:
            total += vault.value_locked
            
        return total
        
    async def _price_monitoring_loop(self):
        """Monitor stablecoin prices"""
        while True:
            try:
                for coin in self.stablecoins.values():
                    # Sample price
                    market_price = await self._get_market_price(coin)
                    
                    # Check deviation
                    deviation = abs(market_price - coin.peg_target) / coin.peg_target
                    
                    if deviation > self.stabilization_params["deviation_threshold"]:
                        logger.info(
                            f"Price deviation detected for {coin.symbol}: "
                            f"{deviation*100:.2f}% (price: {market_price}, target: {coin.peg_target})"
                        )
                        
                await asyncio.sleep(self.stabilization_params["price_sample_interval"])
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in price monitoring: {e}")
                await asyncio.sleep(30)
                
    async def _stabilization_loop(self):
        """Run stabilization mechanisms"""
        while True:
            try:
                for coin in self.stablecoins.values():
                    # Get current price
                    market_price = await self._get_market_price(coin)
                    deviation = (market_price - coin.peg_target) / coin.peg_target
                    
                    # Skip if within threshold
                    if abs(deviation) < self.stabilization_params["deviation_threshold"]:
                        continue
                        
                    # Choose stabilization action
                    if coin.peg_mechanism == PegMechanism.ALGORITHMIC:
                        await self._algorithmic_stabilization(coin, market_price)
                        
                    elif coin.peg_mechanism == PegMechanism.COLLATERALIZED:
                        await self._collateral_stabilization(coin, market_price)
                        
                    elif coin.peg_mechanism == PegMechanism.HYBRID:
                        await self._hybrid_stabilization(coin, market_price)
                        
                    elif coin.peg_mechanism == PegMechanism.REBASE:
                        if abs(deviation) > self.stabilization_params["rebase_threshold"]:
                            await self.rebase_supply(coin.coin_id)
                            
                await asyncio.sleep(self.stabilization_params["stabilization_interval"])
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in stabilization loop: {e}")
                await asyncio.sleep(60)
                
    async def _algorithmic_stabilization(
        self,
        coin: ComputeStablecoin,
        market_price: Decimal
    ):
        """Pure algorithmic stabilization through mint/burn"""
        
        deviation = (market_price - coin.peg_target) / coin.peg_target
        
        if deviation > 0:
            # Price too high - mint more coins
            mint_amount = coin.circulating_supply * abs(deviation) * Decimal("0.1")  # 10% of deviation
            
            coin.total_supply += mint_amount
            coin.circulating_supply += mint_amount
            
            # Distribute to arbitrageurs
            await self._enable_arbitrage(coin, mint_amount, "mint")
            
            event = StabilizationEvent(
                event_id=f"STAB_{uuid.uuid4().hex}",
                coin_id=coin.coin_id,
                action=StabilizationAction.MINT,
                trigger_price=market_price,
                target_price=coin.peg_target,
                amount=mint_amount
            )
            
        else:
            # Price too low - burn coins
            burn_amount = coin.circulating_supply * abs(deviation) * Decimal("0.1")
            burn_amount = min(burn_amount, coin.circulating_supply * Decimal("0.05"))  # Max 5% burn
            
            coin.total_supply -= burn_amount
            coin.circulating_supply -= burn_amount
            
            # Buy and burn from market
            await self._buy_and_burn(coin, burn_amount)
            
            event = StabilizationEvent(
                event_id=f"STAB_{uuid.uuid4().hex}",
                coin_id=coin.coin_id,
                action=StabilizationAction.BURN,
                trigger_price=market_price,
                target_price=coin.peg_target,
                amount=burn_amount
            )
            
        await self._store_stabilization_event(event)
        await self._update_stablecoin(coin)
        
    async def _collateral_stabilization(
        self,
        coin: ComputeStablecoin,
        market_price: Decimal
    ):
        """Collateral-based stabilization"""
        
        deviation = (market_price - coin.peg_target) / coin.peg_target
        
        # Adjust collateral ratio
        if deviation > 0:
            # Price too high - reduce collateral ratio to encourage minting
            new_ratio = coin.collateral_ratio * Decimal("0.95")
            coin.collateral_ratio = max(new_ratio, Decimal("0.5"))  # Min 50%
            
        else:
            # Price too low - increase collateral ratio
            new_ratio = coin.collateral_ratio * Decimal("1.05")
            coin.collateral_ratio = min(new_ratio, Decimal("1.5"))  # Max 150%
            
        await self._update_stablecoin(coin)
        
        # Enable arbitrage opportunities
        await self._create_arbitrage_incentive(coin, market_price)
        
    async def _hybrid_stabilization(
        self,
        coin: ComputeStablecoin,
        market_price: Decimal
    ):
        """Hybrid algorithmic + collateral stabilization"""
        
        deviation = (market_price - coin.peg_target) / coin.peg_target
        
        # Use algorithmic for small deviations
        if abs(deviation) < Decimal("0.03"):
            await self._algorithmic_stabilization(coin, market_price)
        else:
            # Use collateral adjustment for larger deviations
            await self._collateral_stabilization(coin, market_price)
            
            # Also apply mild algorithmic action
            if deviation > 0:
                mint_amount = coin.circulating_supply * abs(deviation) * Decimal("0.05")
                coin.total_supply += mint_amount
                coin.circulating_supply += mint_amount
            else:
                burn_amount = coin.circulating_supply * abs(deviation) * Decimal("0.05")
                burn_amount = min(burn_amount, coin.circulating_supply * Decimal("0.02"))
                coin.total_supply -= burn_amount
                coin.circulating_supply -= burn_amount
                
            await self._update_stablecoin(coin)
            
    async def _enable_arbitrage(
        self,
        coin: ComputeStablecoin,
        amount: Decimal,
        action: str
    ):
        """Enable arbitrage to restore peg"""
        
        # Create arbitrage pool
        await self.ignite.put(
            f"arbitrage_pool:{coin.coin_id}",
            {
                "amount": str(amount),
                "action": action,
                "incentive": str(self.stabilization_params["arbitrage_incentive"]),
                "expires": datetime.utcnow() + timedelta(hours=1)
            }
        )
        
        # Emit arbitrage opportunity
        await self.pulsar.publish('compute.stablecoin.arbitrage', {
            'coin_id': coin.coin_id,
            'action': action,
            'amount': str(amount),
            'incentive': str(self.stabilization_params["arbitrage_incentive"])
        })
        
    async def _buy_and_burn(
        self,
        coin: ComputeStablecoin,
        amount: Decimal
    ):
        """Buy coins from market and burn them"""
        
        # In production, interact with DEX
        # For now, simulate by reducing from largest holders
        
        # Find holders
        holders = [
            (user_id, balance)
            for user_id, balances in self.balances.items()
            for coin_id, balance in balances.items()
            if coin_id == coin.coin_id and balance > 0
        ]
        
        # Sort by balance
        holders.sort(key=lambda x: x[1], reverse=True)
        
        # Buy from holders
        remaining = amount
        for user_id, balance in holders:
            if remaining <= 0:
                break
                
            buy_amount = min(remaining, balance * Decimal("0.1"))  # Max 10% from each
            self.balances[user_id][coin.coin_id] -= buy_amount
            remaining -= buy_amount
            
            # Pay premium to seller
            # In production, transfer value
            
    async def _create_arbitrage_incentive(
        self,
        coin: ComputeStablecoin,
        market_price: Decimal
    ):
        """Create incentive for arbitrageurs"""
        
        if market_price > coin.peg_target:
            # Incentivize minting and selling
            incentive_type = "mint_and_sell"
        else:
            # Incentivize buying and redeeming
            incentive_type = "buy_and_redeem"
            
        incentive = {
            "coin_id": coin.coin_id,
            "type": incentive_type,
            "market_price": str(market_price),
            "peg_target": str(coin.peg_target),
            "profit_opportunity": str(abs(market_price - coin.peg_target)),
            "expires": datetime.utcnow() + timedelta(minutes=30)
        }
        
        await self.ignite.put(f"arbitrage_incentive:{coin.coin_id}", incentive)
        
    async def _collateral_management_loop(self):
        """Manage collateral health"""
        while True:
            try:
                for coin in self.stablecoins.values():
                    if coin.collateral_ratio == 0:
                        continue
                        
                    # Check collateral health
                    total_collateral = await self._get_total_collateral_value(coin)
                    required_collateral = coin.circulating_supply * coin.peg_target * coin.collateral_ratio
                    
                    if total_collateral < required_collateral * Decimal("0.95"):
                        # Under-collateralized
                        logger.warning(
                            f"Under-collateralization detected for {coin.symbol}: "
                            f"{total_collateral} < {required_collateral}"
                        )
                        
                        # Trigger liquidations or request more collateral
                        await self._handle_under_collateralization(coin)
                        
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in collateral management: {e}")
                await asyncio.sleep(600)
                
    async def _handle_under_collateralization(
        self,
        coin: ComputeStablecoin
    ):
        """Handle under-collateralization"""
        
        # Increase collateral requirements for new mints
        coin.collateral_ratio = min(coin.collateral_ratio * Decimal("1.1"), Decimal("2.0"))
        
        # Emit warning
        await self.pulsar.publish('compute.stablecoin.collateral_warning', {
            'coin_id': coin.coin_id,
            'symbol': coin.symbol,
            'collateral_ratio': str(coin.collateral_ratio),
            'status': 'under_collateralized'
        })
        
        await self._update_stablecoin(coin)
        
    async def _initialize_default_coins(self):
        """Initialize default stablecoins"""
        
        # FLOPS coin - pegged to TFLOPS
        try:
            await self.create_stablecoin(
                symbol="cFLOPS",
                name="Compute FLOPS Coin",
                coin_type=StablecoinType.FLOPS_COIN,
                peg_mechanism=PegMechanism.HYBRID,
                peg_target=Decimal("1"),  # 1 cFLOPS = 1 TFLOPS
                initial_collateral_ratio=Decimal("1.0")
            )
        except:
            pass  # Already exists
            
        # GPU Hour coin
        try:
            await self.create_stablecoin(
                symbol="cGPUH",
                name="Compute GPU Hour Coin",
                coin_type=StablecoinType.GPU_HOUR_COIN,
                peg_mechanism=PegMechanism.COLLATERALIZED,
                peg_target=Decimal("1"),  # 1 cGPUH = 1 GPU hour
                initial_collateral_ratio=Decimal("1.2")  # 120% collateralized
            )
        except:
            pass
            
    async def _get_compute_unit_price(self, resource_type: str) -> Decimal:
        """Get price of compute unit"""
        spot_data = await self.spot_market.get_spot_price(resource_type)
        return Decimal(spot_data.get("last_trade_price", "10"))
        
    async def _get_flops_per_unit(self, resource_type: str) -> Decimal:
        """Get FLOPS per unit of resource"""
        # Simplified conversion
        flops_map = {
            "gpu": Decimal("100"),  # 100 TFLOPS per GPU
            "cpu": Decimal("1"),    # 1 TFLOPS per CPU
            "tpu": Decimal("420"),  # 420 TFLOPS per TPU
        }
        
        for key, value in flops_map.items():
            if key in resource_type.lower():
                return value
                
        return Decimal("1")
        
    async def _get_gpu_hour_equivalent(self, resource_type: str) -> Decimal:
        """Convert resource to GPU hour equivalent"""
        # Simplified conversion
        if "gpu" in resource_type.lower():
            return Decimal("1")
        elif "cpu" in resource_type.lower():
            return Decimal("0.01")  # 1 CPU hour = 0.01 GPU hour
        elif "tpu" in resource_type.lower():
            return Decimal("4.2")   # 1 TPU hour = 4.2 GPU hours
        else:
            return Decimal("0.1")
            
    def _get_default_resource(self, coin_type: StablecoinType) -> str:
        """Get default resource type for coin"""
        if coin_type == StablecoinType.GPU_HOUR_COIN:
            return "gpu"
        elif coin_type == StablecoinType.STORAGE_COIN:
            return "storage"
        elif coin_type == StablecoinType.BANDWIDTH_COIN:
            return "bandwidth"
        else:
            return "gpu"  # Default
            
    async def _reserve_compute_capacity(
        self,
        resource_type: str,
        amount: Decimal
    ) -> List[str]:
        """Reserve compute capacity for collateral"""
        # In production, actually reserve with providers
        # For now, return mock providers
        return [f"provider_{i}" for i in range(3)]
        
    async def _deploy_stablecoin_contract(self, coin: ComputeStablecoin):
        """Deploy stablecoin smart contract"""
        # Deploy ERC20 contract on blockchain
        contract_data = {
            "name": coin.name,
            "symbol": coin.symbol,
            "decimals": 18,
            "initial_supply": 0,
            "minter": "COMPUTE_STABLECOIN_ENGINE"
        }
        
        # In production, deploy actual contract
        await self.blockchain_bridge.deploy_contract(
            "ERC20Stablecoin",
            contract_data
        )
        
    async def _store_stablecoin(self, coin: ComputeStablecoin):
        """Store stablecoin in cache"""
        await self.ignite.put(f"stablecoin:{coin.coin_id}", coin)
        
    async def _update_stablecoin(self, coin: ComputeStablecoin):
        """Update stablecoin in cache"""
        await self.ignite.put(f"stablecoin:{coin.coin_id}", coin)
        
    async def _store_vault(self, vault: CollateralVault):
        """Store vault in cache"""
        await self.ignite.put(f"vault:{vault.vault_id}", vault)
        
    async def _update_vault(self, vault: CollateralVault):
        """Update vault in cache"""
        await self.ignite.put(f"vault:{vault.vault_id}", vault)
        
    async def _store_mint_request(self, request: MintRequest):
        """Store mint request"""
        await self.ignite.put(f"mint_request:{request.request_id}", request)
        
    async def _store_redemption_request(self, request: RedemptionRequest):
        """Store redemption request"""
        await self.ignite.put(f"redemption_request:{request.request_id}", request)
        
    async def _store_stabilization_event(self, event: StabilizationEvent):
        """Store stabilization event"""
        await self.ignite.put(f"stabilization_event:{event.event_id}", event)
        
    async def _load_state(self):
        """Load state from cache"""
        # In production, load stablecoins, vaults, balances from cache
        pass
        
    async def get_user_balance(
        self,
        user_id: str,
        coin_id: Optional[str] = None
    ) -> Dict[str, str]:
        """Get user's stablecoin balances"""
        if coin_id:
            balance = self.balances[user_id].get(coin_id, Decimal("0"))
            return {coin_id: str(balance)}
        else:
            return {
                coin_id: str(balance)
                for coin_id, balance in self.balances[user_id].items()
            }
            
    async def get_stablecoin_stats(self) -> Dict[str, Any]:
        """Get overall stablecoin statistics"""
        
        total_market_cap = Decimal("0")
        total_collateral = Decimal("0")
        coins_data = []
        
        for coin in self.stablecoins.values():
            market_cap = coin.market_cap
            total_market_cap += market_cap
            
            collateral_value = await self._get_total_collateral_value(coin)
            total_collateral += collateral_value
            
            price_data = await self.get_coin_price(coin.coin_id)
            
            coins_data.append({
                "symbol": coin.symbol,
                "name": coin.name,
                "type": coin.coin_type.value,
                "market_cap": str(market_cap),
                "circulating_supply": str(coin.circulating_supply),
                "collateral_value": str(collateral_value),
                "price": price_data["market_price"],
                "deviation": price_data["deviation_percent"] + "%"
            })
            
        return {
            "total_market_cap": str(total_market_cap),
            "total_collateral_locked": str(total_collateral),
            "collateralization_ratio": str(
                total_collateral / total_market_cap if total_market_cap > 0 else Decimal("0")
            ),
            "active_coins": len(self.stablecoins),
            "coins": coins_data
        } 