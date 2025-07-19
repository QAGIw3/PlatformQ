"""
Synthetic Derivatives Engine

Universal synthetic derivatives platform for mirroring any asset performance
without requiring ownership of the underlying asset.
"""

from typing import Dict, List, Optional, Tuple, Any, Set
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import uuid
import logging
from collections import defaultdict
import math

from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient
from app.integrations.price_oracle import PriceOracleClient
from app.models.market import Market
from app.collateral.multi_tier_engine import MultiTierCollateralEngine

logger = logging.getLogger(__name__)


class SyntheticAssetType(Enum):
    """Types of synthetic assets"""
    STOCK = "stock"
    COMMODITY = "commodity"
    CRYPTO = "crypto"
    FOREX = "forex"
    INDEX = "index"
    REAL_ESTATE = "real_estate"
    COLLECTIBLE = "collectible"
    CUSTOM = "custom"


class SyntheticInstrumentType(Enum):
    """Types of synthetic instruments"""
    FUTURE = "future"
    OPTION = "option"
    SWAP = "swap"
    PERPETUAL = "perpetual"
    STRUCTURED = "structured"


class CollateralType(Enum):
    """Types of collateral for synthetic positions"""
    STABLECOIN = "stablecoin"
    CRYPTO = "crypto"
    TOKENIZED_ASSET = "tokenized_asset"
    COMPUTE_CREDIT = "compute_credit"
    MULTI_ASSET = "multi_asset"


@dataclass
class SyntheticAsset:
    """Definition of a synthetic asset"""
    asset_id: str = field(default_factory=lambda: f"syn_{uuid.uuid4().hex[:8]}")
    name: str = ""
    symbol: str = ""
    asset_type: SyntheticAssetType = SyntheticAssetType.CUSTOM
    
    # Oracle configuration
    price_feeds: List[str] = field(default_factory=list)  # Oracle IDs
    price_aggregation: str = "median"  # median, mean, vwap
    
    # Reference data
    underlying_reference: str = ""  # E.g., "AAPL", "GOLD", "BTC/USD"
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Risk parameters
    volatility_30d: Decimal = Decimal("0")
    liquidity_score: Decimal = Decimal("0")
    
    # Status
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    

@dataclass
class SyntheticPosition:
    """A synthetic position mirroring an asset"""
    position_id: str = field(default_factory=lambda: f"sp_{uuid.uuid4().hex[:8]}")
    user_id: str = ""
    
    # Asset being mirrored
    synthetic_asset_id: str = ""
    instrument_type: SyntheticInstrumentType = SyntheticInstrumentType.PERPETUAL
    
    # Position details
    size: Decimal = Decimal("0")  # Notional size
    direction: str = "long"  # long or short
    entry_price: Decimal = Decimal("0")
    
    # Collateral
    collateral_type: CollateralType = CollateralType.STABLECOIN
    collateral_amount: Decimal = Decimal("0")
    collateral_assets: Dict[str, Decimal] = field(default_factory=dict)
    
    # Risk metrics
    leverage: Decimal = Decimal("1")
    margin_ratio: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    
    # Dates
    opened_at: datetime = field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    closed_at: Optional[datetime] = None
    
    # Status
    is_active: bool = True
    is_liquidated: bool = False


@dataclass
class SyntheticIndex:
    """Custom synthetic index composed of multiple assets"""
    index_id: str = field(default_factory=lambda: f"idx_{uuid.uuid4().hex[:8]}")
    name: str = ""
    symbol: str = ""
    
    # Components with weights
    components: List[Tuple[str, Decimal]] = field(default_factory=list)  # [(asset_id, weight)]
    
    # Rebalancing
    rebalance_frequency: str = "monthly"  # daily, weekly, monthly, quarterly
    last_rebalance: datetime = field(default_factory=datetime.utcnow)
    
    # Index value
    base_value: Decimal = Decimal("1000")
    current_value: Decimal = Decimal("1000")
    
    # Metadata
    methodology: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True


@dataclass
class SyntheticDerivative:
    """A derivative on a synthetic asset"""
    derivative_id: str = field(default_factory=lambda: f"sd_{uuid.uuid4().hex[:8]}")
    
    # Underlying
    underlying_asset_id: str = ""  # Can be synthetic asset or index
    instrument_type: SyntheticInstrumentType = SyntheticInstrumentType.FUTURE
    
    # Contract specs
    contract_size: Decimal = Decimal("1")
    tick_size: Decimal = Decimal("0.01")
    
    # For options
    strike_price: Optional[Decimal] = None
    option_type: Optional[str] = None  # call or put
    
    # Dates
    listing_date: datetime = field(default_factory=datetime.utcnow)
    expiry_date: Optional[datetime] = None
    
    # Trading stats
    open_interest: Decimal = Decimal("0")
    volume_24h: Decimal = Decimal("0")
    
    # Status
    is_tradable: bool = True


class SyntheticDerivativesEngine:
    """
    Main engine for synthetic derivatives
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        collateral_engine: MultiTierCollateralEngine,
        price_oracle: Optional[PriceOracleClient] = None
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.collateral_engine = collateral_engine
        self.price_oracle = price_oracle or PriceOracleClient()
        
        # Registries
        self.synthetic_assets: Dict[str, SyntheticAsset] = {}
        self.synthetic_positions: Dict[str, SyntheticPosition] = {}
        self.synthetic_indices: Dict[str, SyntheticIndex] = {}
        self.synthetic_derivatives: Dict[str, SyntheticDerivative] = {}
        
        # Price cache
        self.price_cache: Dict[str, Tuple[Decimal, datetime]] = {}
        self.price_cache_ttl = timedelta(seconds=5)
        
        # Risk parameters
        self.min_collateral_ratio = Decimal("1.5")  # 150%
        self.liquidation_threshold = Decimal("1.2")  # 120%
        self.max_leverage = Decimal("10")
        
        # Background tasks
        self._monitoring_task = None
        self._settlement_task = None
        self._index_rebalance_task = None
        
    async def start(self):
        """Start background tasks"""
        self._monitoring_task = asyncio.create_task(self._monitor_positions())
        self._settlement_task = asyncio.create_task(self._settlement_loop())
        self._index_rebalance_task = asyncio.create_task(self._rebalance_indices())
        
    async def stop(self):
        """Stop background tasks"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
        if self._settlement_task:
            self._settlement_task.cancel()
        if self._index_rebalance_task:
            self._index_rebalance_task.cancel()
            
    async def create_synthetic_asset(
        self,
        name: str,
        symbol: str,
        asset_type: SyntheticAssetType,
        price_feeds: List[str],
        underlying_reference: str,
        metadata: Optional[Dict] = None
    ) -> SyntheticAsset:
        """Create a new synthetic asset"""
        asset = SyntheticAsset(
            name=name,
            symbol=symbol,
            asset_type=asset_type,
            price_feeds=price_feeds,
            underlying_reference=underlying_reference,
            metadata=metadata or {}
        )
        
        # Calculate initial risk metrics
        asset.volatility_30d = await self._calculate_volatility(underlying_reference)
        asset.liquidity_score = await self._assess_liquidity(price_feeds)
        
        # Store asset
        self.synthetic_assets[asset.asset_id] = asset
        await self._store_asset(asset)
        
        # Publish event
        await self.pulsar.publish(
            "synthetic.asset.created",
            {
                "asset_id": asset.asset_id,
                "symbol": asset.symbol,
                "type": asset.asset_type.value,
                "underlying": underlying_reference,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Created synthetic asset {asset.symbol} ({asset.asset_id})")
        return asset
        
    async def open_synthetic_position(
        self,
        user_id: str,
        synthetic_asset_id: str,
        size: Decimal,
        direction: str,
        collateral: Dict[str, Decimal],
        instrument_type: SyntheticInstrumentType = SyntheticInstrumentType.PERPETUAL,
        expires_at: Optional[datetime] = None
    ) -> SyntheticPosition:
        """Open a synthetic position"""
        # Validate asset
        asset = self.synthetic_assets.get(synthetic_asset_id)
        if not asset or not asset.is_active:
            raise ValueError(f"Invalid synthetic asset: {synthetic_asset_id}")
            
        # Get current price
        current_price = await self._get_asset_price(asset)
        
        # Calculate required collateral
        notional = size * current_price
        required_collateral = notional / self.max_leverage
        
        # Validate collateral
        total_collateral_value = await self._value_collateral(collateral)
        if total_collateral_value < required_collateral:
            raise ValueError(f"Insufficient collateral: {total_collateral_value} < {required_collateral}")
            
        # Determine collateral type
        collateral_type = self._determine_collateral_type(collateral)
        
        # Create position
        position = SyntheticPosition(
            user_id=user_id,
            synthetic_asset_id=synthetic_asset_id,
            instrument_type=instrument_type,
            size=size,
            direction=direction,
            entry_price=current_price,
            collateral_type=collateral_type,
            collateral_amount=total_collateral_value,
            collateral_assets=collateral,
            leverage=notional / total_collateral_value,
            margin_ratio=total_collateral_value / notional,
            expires_at=expires_at
        )
        
        # Lock collateral
        await self.collateral_engine.lock_collateral(
            user_id,
            collateral,
            position.position_id
        )
        
        # Store position
        self.synthetic_positions[position.position_id] = position
        await self._store_position(position)
        
        # Publish event
        await self.pulsar.publish(
            "synthetic.position.opened",
            {
                "position_id": position.position_id,
                "user_id": user_id,
                "asset_id": synthetic_asset_id,
                "size": str(size),
                "direction": direction,
                "entry_price": str(current_price),
                "leverage": str(position.leverage),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Opened synthetic position {position.position_id} for user {user_id}")
        return position
        
    async def close_synthetic_position(
        self,
        position_id: str,
        user_id: str
    ) -> Dict[str, Any]:
        """Close a synthetic position"""
        position = self.synthetic_positions.get(position_id)
        if not position or position.user_id != user_id:
            raise ValueError(f"Invalid position: {position_id}")
            
        if not position.is_active:
            raise ValueError(f"Position already closed: {position_id}")
            
        # Get current price
        asset = self.synthetic_assets[position.synthetic_asset_id]
        current_price = await self._get_asset_price(asset)
        
        # Calculate PnL
        pnl = self._calculate_pnl(position, current_price)
        
        # Update collateral
        final_collateral = position.collateral_amount + pnl
        
        # Release collateral
        if final_collateral > 0:
            await self.collateral_engine.release_collateral(
                position.position_id,
                position.user_id,
                final_collateral
            )
        
        # Update position
        position.is_active = False
        position.closed_at = datetime.utcnow()
        position.unrealized_pnl = Decimal("0")
        
        # Store updated position
        await self._store_position(position)
        
        # Publish event
        await self.pulsar.publish(
            "synthetic.position.closed",
            {
                "position_id": position_id,
                "user_id": user_id,
                "exit_price": str(current_price),
                "pnl": str(pnl),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return {
            "position_id": position_id,
            "entry_price": position.entry_price,
            "exit_price": current_price,
            "size": position.size,
            "direction": position.direction,
            "pnl": pnl,
            "return_pct": (pnl / position.collateral_amount * 100) if position.collateral_amount > 0 else Decimal("0")
        }
        
    async def create_synthetic_index(
        self,
        name: str,
        symbol: str,
        components: List[Tuple[str, Decimal]],
        rebalance_frequency: str = "monthly",
        methodology: Optional[Dict] = None
    ) -> SyntheticIndex:
        """Create a custom synthetic index"""
        # Validate components
        total_weight = sum(weight for _, weight in components)
        if abs(total_weight - Decimal("1")) > Decimal("0.001"):
            raise ValueError("Component weights must sum to 1")
            
        # Validate all component assets exist
        for asset_id, _ in components:
            if asset_id not in self.synthetic_assets:
                raise ValueError(f"Invalid component asset: {asset_id}")
                
        # Create index
        index = SyntheticIndex(
            name=name,
            symbol=symbol,
            components=components,
            rebalance_frequency=rebalance_frequency,
            methodology=methodology or {}
        )
        
        # Calculate initial value
        index.current_value = await self._calculate_index_value(index)
        
        # Store index
        self.synthetic_indices[index.index_id] = index
        await self._store_index(index)
        
        # Publish event
        await self.pulsar.publish(
            "synthetic.index.created",
            {
                "index_id": index.index_id,
                "symbol": index.symbol,
                "components": len(components),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        logger.info(f"Created synthetic index {index.symbol} ({index.index_id})")
        return index
        
    async def create_synthetic_derivative(
        self,
        underlying_asset_id: str,
        instrument_type: SyntheticInstrumentType,
        contract_size: Decimal = Decimal("1"),
        expiry_date: Optional[datetime] = None,
        strike_price: Optional[Decimal] = None,
        option_type: Optional[str] = None
    ) -> SyntheticDerivative:
        """Create a derivative on a synthetic asset or index"""
        # Validate underlying
        if underlying_asset_id not in self.synthetic_assets and \
           underlying_asset_id not in self.synthetic_indices:
            raise ValueError(f"Invalid underlying: {underlying_asset_id}")
            
        # Create derivative
        derivative = SyntheticDerivative(
            underlying_asset_id=underlying_asset_id,
            instrument_type=instrument_type,
            contract_size=contract_size,
            expiry_date=expiry_date,
            strike_price=strike_price,
            option_type=option_type
        )
        
        # Store derivative
        self.synthetic_derivatives[derivative.derivative_id] = derivative
        await self._store_derivative(derivative)
        
        # Publish event
        await self.pulsar.publish(
            "synthetic.derivative.created",
            {
                "derivative_id": derivative.derivative_id,
                "underlying": underlying_asset_id,
                "type": instrument_type.value,
                "expiry": expiry_date.isoformat() if expiry_date else None,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return derivative
        
    async def _get_asset_price(self, asset: SyntheticAsset) -> Decimal:
        """Get current price for a synthetic asset"""
        # Check cache
        cache_key = f"price:{asset.asset_id}"
        if cache_key in self.price_cache:
            price, timestamp = self.price_cache[cache_key]
            if datetime.utcnow() - timestamp < self.price_cache_ttl:
                return price
                
        # Get prices from all feeds
        prices = []
        for feed_id in asset.price_feeds:
            try:
                price = await self.oracle.get_price(feed_id)
                if price:
                    prices.append(price)
            except Exception as e:
                logger.error(f"Error getting price from feed {feed_id}: {e}")
                
        if not prices:
            # Fallback to price oracle
            price = await self.price_oracle.get_price(asset.underlying_reference)
            if price:
                prices.append(price)
                
        if not prices:
            raise ValueError(f"No price available for asset {asset.asset_id}")
            
        # Aggregate prices
        if asset.price_aggregation == "median":
            final_price = sorted(prices)[len(prices) // 2]
        elif asset.price_aggregation == "mean":
            final_price = sum(prices) / len(prices)
        else:  # vwap or other
            final_price = prices[0]  # Simplified
            
        # Cache price
        self.price_cache[cache_key] = (final_price, datetime.utcnow())
        
        return final_price
        
    def _calculate_pnl(self, position: SyntheticPosition, current_price: Decimal) -> Decimal:
        """Calculate position PnL"""
        price_change = current_price - position.entry_price
        
        if position.direction == "long":
            pnl = position.size * price_change
        else:  # short
            pnl = position.size * -price_change
            
        return pnl
        
    async def _value_collateral(self, collateral: Dict[str, Decimal]) -> Decimal:
        """Calculate total value of collateral"""
        total_value = Decimal("0")
        
        for asset, amount in collateral.items():
            # Get asset price
            if asset in ["USDC", "USDT", "DAI"]:
                price = Decimal("1")  # Stablecoins
            else:
                price = await self.price_oracle.get_price(asset)
                
            total_value += amount * price
            
        return total_value
        
    def _determine_collateral_type(self, collateral: Dict[str, Decimal]) -> CollateralType:
        """Determine collateral type from assets"""
        assets = set(collateral.keys())
        
        if assets.issubset({"USDC", "USDT", "DAI"}):
            return CollateralType.STABLECOIN
        elif assets.issubset({"BTC", "ETH", "SOL"}):
            return CollateralType.CRYPTO
        elif any("compute" in asset.lower() for asset in assets):
            return CollateralType.COMPUTE_CREDIT
        elif any("nft" in asset.lower() or "token" in asset.lower() for asset in assets):
            return CollateralType.TOKENIZED_ASSET
        else:
            return CollateralType.MULTI_ASSET
            
    async def _calculate_volatility(self, reference: str) -> Decimal:
        """Calculate 30-day volatility for an asset"""
        # Simplified - would calculate from historical prices
        volatility_map = {
            "AAPL": Decimal("0.25"),
            "GOLD": Decimal("0.15"),
            "BTC": Decimal("0.80"),
            "EUR/USD": Decimal("0.10"),
            "SP500": Decimal("0.20")
        }
        
        # Check if reference matches any known pattern
        for pattern, vol in volatility_map.items():
            if pattern in reference.upper():
                return vol
                
        return Decimal("0.30")  # Default 30% volatility
        
    async def _assess_liquidity(self, price_feeds: List[str]) -> Decimal:
        """Assess liquidity score based on price feeds"""
        # More feeds = better liquidity
        feed_count = len(price_feeds)
        
        if feed_count >= 5:
            return Decimal("90")
        elif feed_count >= 3:
            return Decimal("70")
        elif feed_count >= 1:
            return Decimal("50")
        else:
            return Decimal("30")
            
    async def _calculate_index_value(self, index: SyntheticIndex) -> Decimal:
        """Calculate current value of an index"""
        weighted_sum = Decimal("0")
        
        for asset_id, weight in index.components:
            asset = self.synthetic_assets.get(asset_id)
            if asset:
                price = await self._get_asset_price(asset)
                weighted_sum += price * weight
                
        # Scale to base value
        return weighted_sum * (index.base_value / Decimal("100"))
        
    async def _monitor_positions(self):
        """Monitor synthetic positions for liquidations"""
        while True:
            try:
                for position_id, position in list(self.synthetic_positions.items()):
                    if not position.is_active:
                        continue
                        
                    # Get current price
                    asset = self.synthetic_assets.get(position.synthetic_asset_id)
                    if not asset:
                        continue
                        
                    current_price = await self._get_asset_price(asset)
                    
                    # Calculate unrealized PnL
                    pnl = self._calculate_pnl(position, current_price)
                    position.unrealized_pnl = pnl
                    
                    # Calculate margin ratio
                    notional = position.size * current_price
                    effective_collateral = position.collateral_amount + pnl
                    margin_ratio = effective_collateral / notional if notional > 0 else Decimal("0")
                    position.margin_ratio = margin_ratio
                    
                    # Check liquidation
                    if margin_ratio < self.liquidation_threshold / self.max_leverage:
                        await self._liquidate_position(position)
                        
                    # Update position
                    await self._store_position(position)
                    
                await asyncio.sleep(5)  # Check every 5 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in position monitoring: {e}")
                await asyncio.sleep(10)
                
    async def _liquidate_position(self, position: SyntheticPosition):
        """Liquidate an underwater position"""
        logger.warning(f"Liquidating position {position.position_id}")
        
        # Close position at current price
        await self.close_synthetic_position(
            position.position_id,
            position.user_id
        )
        
        # Mark as liquidated
        position.is_liquidated = True
        
        # Publish liquidation event
        await self.pulsar.publish(
            "synthetic.position.liquidated",
            {
                "position_id": position.position_id,
                "user_id": position.user_id,
                "margin_ratio": str(position.margin_ratio),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
    async def _settlement_loop(self):
        """Handle expiring positions and derivatives"""
        while True:
            try:
                now = datetime.utcnow()
                
                # Check expiring positions
                for position_id, position in list(self.synthetic_positions.items()):
                    if position.is_active and position.expires_at and position.expires_at <= now:
                        # Auto-close at expiry
                        await self.close_synthetic_position(
                            position_id,
                            position.user_id
                        )
                        
                # Check expiring derivatives
                for derivative_id, derivative in list(self.synthetic_derivatives.items()):
                    if derivative.is_tradable and derivative.expiry_date and derivative.expiry_date <= now:
                        derivative.is_tradable = False
                        await self._settle_derivative(derivative)
                        
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in settlement loop: {e}")
                await asyncio.sleep(60)
                
    async def _settle_derivative(self, derivative: SyntheticDerivative):
        """Settle an expiring derivative"""
        # Get settlement price
        if derivative.underlying_asset_id in self.synthetic_assets:
            asset = self.synthetic_assets[derivative.underlying_asset_id]
            settlement_price = await self._get_asset_price(asset)
        else:
            index = self.synthetic_indices[derivative.underlying_asset_id]
            settlement_price = await self._calculate_index_value(index)
            
        # Publish settlement event
        await self.pulsar.publish(
            "synthetic.derivative.settled",
            {
                "derivative_id": derivative.derivative_id,
                "settlement_price": str(settlement_price),
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
    async def _rebalance_indices(self):
        """Rebalance synthetic indices"""
        while True:
            try:
                now = datetime.utcnow()
                
                for index_id, index in self.synthetic_indices.items():
                    if not index.is_active:
                        continue
                        
                    # Check if rebalance is due
                    days_since_rebalance = (now - index.last_rebalance).days
                    
                    should_rebalance = False
                    if index.rebalance_frequency == "daily" and days_since_rebalance >= 1:
                        should_rebalance = True
                    elif index.rebalance_frequency == "weekly" and days_since_rebalance >= 7:
                        should_rebalance = True
                    elif index.rebalance_frequency == "monthly" and days_since_rebalance >= 30:
                        should_rebalance = True
                    elif index.rebalance_frequency == "quarterly" and days_since_rebalance >= 90:
                        should_rebalance = True
                        
                    if should_rebalance:
                        await self._rebalance_index(index)
                        
                await asyncio.sleep(3600)  # Check every hour
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in index rebalancing: {e}")
                await asyncio.sleep(3600)
                
    async def _rebalance_index(self, index: SyntheticIndex):
        """Rebalance an index"""
        logger.info(f"Rebalancing index {index.symbol}")
        
        # Recalculate weights based on methodology
        if "market_cap_weighted" in index.methodology:
            # Simplified - would use actual market cap data
            pass
        elif "equal_weighted" in index.methodology:
            # Equal weight all components
            num_components = len(index.components)
            new_weight = Decimal("1") / num_components
            index.components = [(asset_id, new_weight) for asset_id, _ in index.components]
            
        # Update last rebalance time
        index.last_rebalance = datetime.utcnow()
        
        # Recalculate index value
        index.current_value = await self._calculate_index_value(index)
        
        # Store updated index
        await self._store_index(index)
        
        # Publish event
        await self.pulsar.publish(
            "synthetic.index.rebalanced",
            {
                "index_id": index.index_id,
                "symbol": index.symbol,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
    # Storage methods
    async def _store_asset(self, asset: SyntheticAsset):
        """Store synthetic asset in cache"""
        await self.ignite.set(
            f"synthetic_asset:{asset.asset_id}",
            asset.__dict__,
            ttl=None  # Permanent
        )
        
    async def _store_position(self, position: SyntheticPosition):
        """Store position in cache"""
        await self.ignite.set(
            f"synthetic_position:{position.position_id}",
            position.__dict__,
            ttl=86400 * 30  # 30 days
        )
        
    async def _store_index(self, index: SyntheticIndex):
        """Store index in cache"""
        await self.ignite.set(
            f"synthetic_index:{index.index_id}",
            index.__dict__,
            ttl=None  # Permanent
        )
        
    async def _store_derivative(self, derivative: SyntheticDerivative):
        """Store derivative in cache"""
        await self.ignite.set(
            f"synthetic_derivative:{derivative.derivative_id}",
            derivative.__dict__,
            ttl=86400 * 365  # 1 year
        ) 