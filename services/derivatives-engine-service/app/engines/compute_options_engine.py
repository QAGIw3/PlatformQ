"""
Compute Options Engine

Options market for compute resources with specialized pricing and hedging
"""

from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
import uuid
import numpy as np
from scipy.stats import norm

from app.integrations import (
    IgniteCache,
    PulsarEventPublisher,
    OracleAggregatorClient
)
from app.engines.options_amm import OptionsAMM, AMMConfig
from app.engines.pricing import BlackScholesEngine, Greeks
from app.engines.compute_spot_market import ComputeSpotMarket
from app.engines.compute_futures_engine import ComputeFuturesEngine

logger = logging.getLogger(__name__)


class ComputeOptionType(Enum):
    """Types of compute options"""
    CALL = "call"  # Right to buy compute at strike
    PUT = "put"    # Right to sell compute at strike
    BURST = "burst"  # Right to surge capacity
    THROTTLE = "throttle"  # Right to reduce capacity


class ExerciseStyle(Enum):
    """Option exercise styles"""
    EUROPEAN = "european"  # Exercise only at expiry
    AMERICAN = "american"  # Exercise any time
    BERMUDAN = "bermudan"  # Exercise at specific dates
    ASIAN = "asian"       # Based on average price


@dataclass
class ComputeOption:
    """Compute option contract"""
    option_id: str
    underlying: str  # Resource type (GPU_A100, CPU_EPYC, etc.)
    option_type: ComputeOptionType
    exercise_style: ExerciseStyle
    strike_price: Decimal
    expiry: datetime
    contract_size: Decimal  # Units of compute
    location: Optional[str] = None
    quality_tier: Optional[str] = None  # "standard", "premium", "guaranteed"
    created_at: datetime = field(default_factory=datetime.utcnow)
    creator: Optional[str] = None
    
    @property
    def time_to_expiry(self) -> float:
        """Time to expiry in years"""
        ttm = (self.expiry - datetime.utcnow()).total_seconds() / (365.25 * 24 * 3600)
        return max(0, ttm)
    
    @property
    def is_expired(self) -> bool:
        """Check if option has expired"""
        return datetime.utcnow() >= self.expiry


@dataclass
class ComputeOptionPosition:
    """User's option position"""
    position_id: str
    user_id: str
    option: ComputeOption
    quantity: Decimal  # Positive for long, negative for short
    entry_price: Decimal
    current_price: Optional[Decimal] = None
    unrealized_pnl: Optional[Decimal] = None
    realized_pnl: Decimal = Decimal("0")
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    @property
    def notional_value(self) -> Decimal:
        """Notional value of position"""
        return abs(self.quantity * self.option.contract_size * self.option.strike_price)


@dataclass
class ComputeVolatilitySurface:
    """Implied volatility surface for compute options"""
    underlying: str
    timestamp: datetime
    strikes: List[Decimal]  # Strike prices
    expiries: List[float]   # Time to expiry in years
    ivs: Dict[Tuple[Decimal, float], Decimal]  # (strike, expiry) -> IV
    spot_price: Decimal
    
    def get_iv(self, strike: Decimal, expiry: float) -> Decimal:
        """Get interpolated IV for strike and expiry"""
        # Simple linear interpolation for now
        key = (strike, expiry)
        if key in self.ivs:
            return self.ivs[key]
            
        # Find surrounding points and interpolate
        # In production, use more sophisticated interpolation
        return Decimal("0.5")  # Default 50% vol


class ComputeOptionsEngine:
    """
    Engine for compute resource options with specialized features
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        spot_market: ComputeSpotMarket,
        futures_engine: ComputeFuturesEngine,
        pricing_engine: BlackScholesEngine,
        options_amm: OptionsAMM
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.spot_market = spot_market
        self.futures_engine = futures_engine
        self.pricing_engine = pricing_engine
        self.options_amm = options_amm
        
        # Options registry
        self.options: Dict[str, ComputeOption] = {}
        
        # Positions tracking
        self.positions: Dict[str, ComputeOptionPosition] = {}
        self.user_positions: Dict[str, List[str]] = {}
        
        # Volatility surfaces
        self.vol_surfaces: Dict[str, ComputeVolatilitySurface] = {}
        
        # Market maker inventory
        self.mm_inventory: Dict[str, Decimal] = {}
        
        # Hedging parameters
        self.hedge_enabled = True
        self.hedge_threshold = Decimal("100")  # Delta threshold
        self.hedge_interval = 60  # seconds
        
        # Background tasks
        self._pricing_task = None
        self._hedging_task = None
        self._settlement_task = None
        
    async def start(self):
        """Start options engine"""
        # Load existing options
        await self._load_active_options()
        
        # Start background tasks
        self._pricing_task = asyncio.create_task(self._pricing_update_loop())
        self._hedging_task = asyncio.create_task(self._hedging_loop())
        self._settlement_task = asyncio.create_task(self._settlement_loop())
        
        logger.info("Compute options engine started")
        
    async def stop(self):
        """Stop options engine"""
        if self._pricing_task:
            self._pricing_task.cancel()
        if self._hedging_task:
            self._hedging_task.cancel()
        if self._settlement_task:
            self._settlement_task.cancel()
            
    async def create_option(
        self,
        underlying: str,
        option_type: ComputeOptionType,
        exercise_style: ExerciseStyle,
        strike_price: Decimal,
        expiry: datetime,
        contract_size: Decimal,
        location: Optional[str] = None,
        quality_tier: Optional[str] = None,
        creator: Optional[str] = None
    ) -> ComputeOption:
        """Create a new compute option"""
        
        # Validate parameters
        if expiry <= datetime.utcnow():
            raise ValueError("Expiry must be in the future")
            
        if strike_price <= 0:
            raise ValueError("Strike price must be positive")
            
        if contract_size <= 0:
            raise ValueError("Contract size must be positive")
            
        # Create option
        option = ComputeOption(
            option_id=f"OPT_{underlying}_{datetime.utcnow().timestamp()}",
            underlying=underlying,
            option_type=option_type,
            exercise_style=exercise_style,
            strike_price=strike_price,
            expiry=expiry,
            contract_size=contract_size,
            location=location,
            quality_tier=quality_tier,
            creator=creator
        )
        
        # Store option
        self.options[option.option_id] = option
        await self._store_option(option)
        
        # Register with AMM if standard option
        if location is None and quality_tier == "standard":
            self.options_amm.add_liquidity_pool(
                underlying=underlying,
                strike=strike_price,
                expiry=expiry,
                is_call=(option_type == ComputeOptionType.CALL)
            )
            
        # Emit event
        await self.pulsar.publish('compute.options.created', {
            'option_id': option.option_id,
            'underlying': underlying,
            'option_type': option_type.value,
            'strike': str(strike_price),
            'expiry': expiry.isoformat(),
            'contract_size': str(contract_size),
            'location': location,
            'quality_tier': quality_tier
        })
        
        return option
        
    async def price_option(
        self,
        option: ComputeOption,
        spot_price: Optional[Decimal] = None,
        volatility: Optional[Decimal] = None,
        risk_free_rate: Decimal = Decimal("0.05")
    ) -> Dict[str, Any]:
        """Price a compute option"""
        
        # Get spot price if not provided
        if spot_price is None:
            spot_data = await self.spot_market.get_spot_price(
                option.underlying,
                option.location
            )
            spot_price = Decimal(spot_data.get("last_trade_price", "0"))
            
        if spot_price == 0:
            raise ValueError("Cannot price option without spot price")
            
        # Get volatility if not provided
        if volatility is None:
            volatility = await self._get_implied_volatility(
                option.underlying,
                option.strike_price,
                option.time_to_expiry
            )
            
        # Price based on exercise style
        if option.exercise_style == ExerciseStyle.EUROPEAN:
            price = self._price_european_option(
                option,
                spot_price,
                volatility,
                risk_free_rate
            )
        elif option.exercise_style == ExerciseStyle.AMERICAN:
            price = self._price_american_option(
                option,
                spot_price,
                volatility,
                risk_free_rate
            )
        elif option.exercise_style == ExerciseStyle.ASIAN:
            price = await self._price_asian_option(
                option,
                spot_price,
                volatility,
                risk_free_rate
            )
        else:
            # Default to European pricing
            price = self._price_european_option(
                option,
                spot_price,
                volatility,
                risk_free_rate
            )
            
        # Calculate Greeks
        greeks = self._calculate_greeks(
            option,
            spot_price,
            volatility,
            risk_free_rate,
            price
        )
        
        return {
            "option_id": option.option_id,
            "price": str(price),
            "spot_price": str(spot_price),
            "volatility": str(volatility),
            "time_to_expiry": option.time_to_expiry,
            "greeks": {
                "delta": str(greeks["delta"]),
                "gamma": str(greeks["gamma"]),
                "vega": str(greeks["vega"]),
                "theta": str(greeks["theta"]),
                "rho": str(greeks["rho"])
            },
            "intrinsic_value": str(self._calculate_intrinsic_value(option, spot_price)),
            "time_value": str(price - self._calculate_intrinsic_value(option, spot_price))
        }
        
    async def trade_option(
        self,
        user_id: str,
        option_id: str,
        quantity: Decimal,
        side: str,  # "buy" or "sell"
        order_type: str = "market",  # "market" or "limit"
        limit_price: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """Trade a compute option"""
        
        # Get option
        option = self.options.get(option_id)
        if not option:
            raise ValueError(f"Option {option_id} not found")
            
        # Check if expired
        if option.is_expired:
            raise ValueError("Option has expired")
            
        # Get current price
        price_data = await self.price_option(option)
        current_price = Decimal(price_data["price"])
        
        # Execute trade based on order type
        if order_type == "market":
            execution_price = current_price
        else:
            if not limit_price:
                raise ValueError("Limit price required for limit orders")
            if side == "buy" and limit_price < current_price:
                return {"success": False, "reason": "Limit price below market"}
            if side == "sell" and limit_price > current_price:
                return {"success": False, "reason": "Limit price above market"}
            execution_price = limit_price
            
        # Calculate trade value
        trade_quantity = quantity if side == "buy" else -quantity
        trade_value = abs(trade_quantity * execution_price)
        
        # Check user margin/collateral
        # TODO: Implement margin check
        
        # Create or update position
        position_id = f"{user_id}_{option_id}"
        
        if position_id in self.positions:
            # Update existing position
            position = self.positions[position_id]
            old_quantity = position.quantity
            position.quantity += trade_quantity
            
            # Update average price
            if (old_quantity > 0 and trade_quantity > 0) or \
               (old_quantity < 0 and trade_quantity < 0):
                # Adding to position
                total_value = (abs(old_quantity) * position.entry_price + 
                             abs(trade_quantity) * execution_price)
                position.entry_price = total_value / abs(position.quantity)
            else:
                # Reducing or flipping position
                if abs(trade_quantity) >= abs(old_quantity):
                    # Position flipped
                    position.entry_price = execution_price
                    # Calculate realized PnL
                    position.realized_pnl += (execution_price - position.entry_price) * min(abs(old_quantity), abs(trade_quantity))
        else:
            # Create new position
            position = ComputeOptionPosition(
                position_id=position_id,
                user_id=user_id,
                option=option,
                quantity=trade_quantity,
                entry_price=execution_price
            )
            self.positions[position_id] = position
            
            if user_id not in self.user_positions:
                self.user_positions[user_id] = []
            self.user_positions[user_id].append(position_id)
            
        # Update position
        await self._update_position(position)
        
        # Update market maker inventory
        self.mm_inventory[option_id] = self.mm_inventory.get(option_id, Decimal("0")) - trade_quantity
        
        # Emit trade event
        await self.pulsar.publish('compute.options.trade', {
            'user_id': user_id,
            'option_id': option_id,
            'quantity': str(quantity),
            'side': side,
            'execution_price': str(execution_price),
            'trade_value': str(trade_value),
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return {
            "success": True,
            "option_id": option_id,
            "quantity": str(quantity),
            "side": side,
            "execution_price": str(execution_price),
            "trade_value": str(trade_value),
            "position": {
                "position_id": position.position_id,
                "total_quantity": str(position.quantity),
                "average_price": str(position.entry_price),
                "unrealized_pnl": str(position.unrealized_pnl or 0),
                "realized_pnl": str(position.realized_pnl)
            }
        }
        
    async def exercise_option(
        self,
        user_id: str,
        option_id: str,
        quantity: Decimal
    ) -> Dict[str, Any]:
        """Exercise a compute option"""
        
        # Get option and position
        option = self.options.get(option_id)
        if not option:
            raise ValueError(f"Option {option_id} not found")
            
        position_id = f"{user_id}_{option_id}"
        position = self.positions.get(position_id)
        if not position or position.quantity <= 0:
            raise ValueError("No long position to exercise")
            
        if quantity > position.quantity:
            raise ValueError("Cannot exercise more than position quantity")
            
        # Check exercise style
        if option.exercise_style == ExerciseStyle.EUROPEAN:
            if not option.is_expired:
                raise ValueError("European option can only be exercised at expiry")
        elif option.exercise_style == ExerciseStyle.AMERICAN:
            # Can exercise any time
            pass
        else:
            raise ValueError(f"Exercise not supported for {option.exercise_style.value} options")
            
        # Get current spot price
        spot_data = await self.spot_market.get_spot_price(
            option.underlying,
            option.location
        )
        spot_price = Decimal(spot_data.get("last_trade_price", "0"))
        
        # Check if exercise is profitable
        intrinsic_value = self._calculate_intrinsic_value(option, spot_price)
        if intrinsic_value <= 0:
            return {
                "success": False,
                "reason": "Option is out of the money",
                "intrinsic_value": "0"
            }
            
        # Calculate exercise value
        exercise_value = quantity * intrinsic_value
        
        # For calls: buy underlying at strike, sell at spot
        # For puts: sell underlying at strike, buy at spot
        if option.option_type == ComputeOptionType.CALL:
            # User buys compute at strike price
            compute_cost = quantity * option.contract_size * option.strike_price
            compute_value = quantity * option.contract_size * spot_price
            profit = compute_value - compute_cost
            
            # Create spot order to acquire compute
            spot_order = await self.spot_market.submit_order({
                "user_id": user_id,
                "order_type": "market",
                "side": "buy",
                "resource_type": option.underlying,
                "quantity": quantity * option.contract_size,
                "location_preference": option.location,
                "metadata": {
                    "option_exercise": option_id
                }
            })
        else:
            # Put option - user sells compute at strike price
            compute_value = quantity * option.contract_size * option.strike_price
            compute_cost = quantity * option.contract_size * spot_price
            profit = compute_value - compute_cost
            
            # Create spot order to sell compute
            spot_order = await self.spot_market.submit_order({
                "user_id": user_id,
                "order_type": "market",
                "side": "sell",
                "resource_type": option.underlying,
                "quantity": quantity * option.contract_size,
                "location_preference": option.location,
                "metadata": {
                    "option_exercise": option_id
                }
            })
            
        # Update position
        position.quantity -= quantity
        position.realized_pnl += profit
        await self._update_position(position)
        
        # Emit exercise event
        await self.pulsar.publish('compute.options.exercised', {
            'user_id': user_id,
            'option_id': option_id,
            'quantity': str(quantity),
            'strike_price': str(option.strike_price),
            'spot_price': str(spot_price),
            'profit': str(profit),
            'timestamp': datetime.utcnow().isoformat()
        })
        
        return {
            "success": True,
            "option_id": option_id,
            "quantity_exercised": str(quantity),
            "strike_price": str(option.strike_price),
            "spot_price": str(spot_price),
            "exercise_value": str(exercise_value),
            "profit": str(profit),
            "spot_order": spot_order
        }
        
    async def create_volatility_surface(
        self,
        underlying: str
    ) -> ComputeVolatilitySurface:
        """Create implied volatility surface from market prices"""
        
        # Get spot price
        spot_data = await self.spot_market.get_spot_price(underlying)
        spot_price = Decimal(spot_data.get("last_trade_price", "0"))
        
        if spot_price == 0:
            raise ValueError("Cannot create vol surface without spot price")
            
        # Define strike range (70% to 130% of spot)
        strikes = [
            spot_price * Decimal(str(k))
            for k in [0.7, 0.8, 0.9, 0.95, 1.0, 1.05, 1.1, 1.2, 1.3]
        ]
        
        # Define expiry range (1 week to 3 months)
        expiries = [7/365.25, 14/365.25, 30/365.25, 60/365.25, 90/365.25]
        
        # Calculate implied volatilities
        ivs = {}
        
        for strike in strikes:
            for expiry in expiries:
                # Get market price for this strike/expiry
                # In production, this would come from actual market data
                # For now, use a volatility smile model
                moneyness = strike / spot_price
                base_vol = Decimal("0.5")  # 50% base volatility
                
                # Add smile effect
                if moneyness < 1:
                    # OTM puts have higher IV
                    smile_adj = (Decimal("1") - moneyness) * Decimal("0.3")
                else:
                    # OTM calls have higher IV
                    smile_adj = (moneyness - Decimal("1")) * Decimal("0.2")
                    
                # Add term structure
                term_adj = Decimal(str(expiry)) * Decimal("-0.1")
                
                iv = base_vol + smile_adj + term_adj
                ivs[(strike, expiry)] = max(Decimal("0.1"), iv)  # Min 10% vol
                
        # Create surface
        surface = ComputeVolatilitySurface(
            underlying=underlying,
            timestamp=datetime.utcnow(),
            strikes=strikes,
            expiries=expiries,
            ivs=ivs,
            spot_price=spot_price
        )
        
        # Store surface
        self.vol_surfaces[underlying] = surface
        await self._store_vol_surface(surface)
        
        return surface
        
    def _price_european_option(
        self,
        option: ComputeOption,
        spot: Decimal,
        vol: Decimal,
        r: Decimal
    ) -> Decimal:
        """Price European option using Black-Scholes"""
        
        S = float(spot)
        K = float(option.strike_price)
        T = option.time_to_expiry
        sigma = float(vol)
        r_float = float(r)
        
        if T <= 0:
            # Option expired
            return max(0, self._calculate_intrinsic_value(option, spot))
            
        # Calculate d1 and d2
        d1 = (np.log(S/K) + (r_float + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
        d2 = d1 - sigma*np.sqrt(T)
        
        if option.option_type == ComputeOptionType.CALL:
            price = S*norm.cdf(d1) - K*np.exp(-r_float*T)*norm.cdf(d2)
        else:
            price = K*np.exp(-r_float*T)*norm.cdf(-d2) - S*norm.cdf(-d1)
            
        return Decimal(str(price)) * option.contract_size
        
    def _price_american_option(
        self,
        option: ComputeOption,
        spot: Decimal,
        vol: Decimal,
        r: Decimal
    ) -> Decimal:
        """Price American option using binomial tree"""
        # Simplified - in production use proper American option pricing
        # For now, use European price with early exercise premium
        european_price = self._price_european_option(option, spot, vol, r)
        
        # Add early exercise premium (rough approximation)
        intrinsic = self._calculate_intrinsic_value(option, spot)
        early_ex_premium = max(Decimal("0"), intrinsic - european_price) * Decimal("0.1")
        
        return european_price + early_ex_premium
        
    async def _price_asian_option(
        self,
        option: ComputeOption,
        spot: Decimal,
        vol: Decimal,
        r: Decimal
    ) -> Decimal:
        """Price Asian option based on average price"""
        # Get historical prices
        lookback_days = min(30, (option.expiry - option.created_at).days)
        
        # In production, get actual historical prices
        # For now, simulate with current spot
        avg_price = spot
        
        # Price as European option with adjusted spot
        adjusted_option = ComputeOption(
            option_id=option.option_id,
            underlying=option.underlying,
            option_type=option.option_type,
            exercise_style=ExerciseStyle.EUROPEAN,
            strike_price=option.strike_price,
            expiry=option.expiry,
            contract_size=option.contract_size
        )
        
        # Use reduced volatility for averaging effect
        asian_vol = vol * Decimal("0.7")
        
        return self._price_european_option(adjusted_option, avg_price, asian_vol, r)
        
    def _calculate_intrinsic_value(
        self,
        option: ComputeOption,
        spot: Decimal
    ) -> Decimal:
        """Calculate intrinsic value of option"""
        if option.option_type == ComputeOptionType.CALL:
            return max(Decimal("0"), spot - option.strike_price) * option.contract_size
        else:
            return max(Decimal("0"), option.strike_price - spot) * option.contract_size
            
    def _calculate_greeks(
        self,
        option: ComputeOption,
        spot: Decimal,
        vol: Decimal,
        r: Decimal,
        price: Decimal
    ) -> Dict[str, Decimal]:
        """Calculate option Greeks"""
        
        S = float(spot)
        K = float(option.strike_price)
        T = option.time_to_expiry
        sigma = float(vol)
        r_float = float(r)
        
        if T <= 0:
            # Option expired
            return {
                "delta": Decimal("1") if S > K and option.option_type == ComputeOptionType.CALL else Decimal("0"),
                "gamma": Decimal("0"),
                "vega": Decimal("0"),
                "theta": Decimal("0"),
                "rho": Decimal("0")
            }
            
        # Calculate d1 and d2
        d1 = (np.log(S/K) + (r_float + 0.5*sigma**2)*T) / (sigma*np.sqrt(T))
        d2 = d1 - sigma*np.sqrt(T)
        
        # Delta
        if option.option_type == ComputeOptionType.CALL:
            delta = norm.cdf(d1)
        else:
            delta = norm.cdf(d1) - 1
            
        # Gamma
        gamma = norm.pdf(d1) / (S * sigma * np.sqrt(T))
        
        # Vega
        vega = S * norm.pdf(d1) * np.sqrt(T) / 100  # Per 1% vol move
        
        # Theta
        if option.option_type == ComputeOptionType.CALL:
            theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) 
                    - r_float * K * np.exp(-r_float * T) * norm.cdf(d2)) / 365
        else:
            theta = (-S * norm.pdf(d1) * sigma / (2 * np.sqrt(T)) 
                    + r_float * K * np.exp(-r_float * T) * norm.cdf(-d2)) / 365
            
        # Rho
        if option.option_type == ComputeOptionType.CALL:
            rho = K * T * np.exp(-r_float * T) * norm.cdf(d2) / 100  # Per 1% rate move
        else:
            rho = -K * T * np.exp(-r_float * T) * norm.cdf(-d2) / 100
            
        return {
            "delta": Decimal(str(delta)) * option.contract_size,
            "gamma": Decimal(str(gamma)) * option.contract_size,
            "vega": Decimal(str(vega)) * option.contract_size,
            "theta": Decimal(str(theta)) * option.contract_size,
            "rho": Decimal(str(rho)) * option.contract_size
        }
        
    async def _get_implied_volatility(
        self,
        underlying: str,
        strike: Decimal,
        expiry: float
    ) -> Decimal:
        """Get implied volatility from surface"""
        surface = self.vol_surfaces.get(underlying)
        
        if not surface:
            # Create new surface
            try:
                surface = await self.create_volatility_surface(underlying)
            except:
                # Default volatility
                return Decimal("0.5")
                
        return surface.get_iv(strike, expiry)
        
    async def _pricing_update_loop(self):
        """Update option prices periodically"""
        while True:
            try:
                # Update prices for all active options
                for option_id, option in self.options.items():
                    if not option.is_expired:
                        try:
                            price_data = await self.price_option(option)
                            
                            # Update position MTM
                            await self._update_position_mtm(option_id, Decimal(price_data["price"]))
                            
                        except Exception as e:
                            logger.error(f"Error pricing option {option_id}: {e}")
                            
                await asyncio.sleep(10)  # Update every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in pricing loop: {e}")
                await asyncio.sleep(30)
                
    async def _hedging_loop(self):
        """Dynamic hedging of option positions"""
        while True:
            try:
                if not self.hedge_enabled:
                    await asyncio.sleep(self.hedge_interval)
                    continue
                    
                # Calculate aggregate Greeks
                total_delta = {}
                total_gamma = {}
                
                for option_id, inventory in self.mm_inventory.items():
                    if inventory == 0:
                        continue
                        
                    option = self.options.get(option_id)
                    if not option or option.is_expired:
                        continue
                        
                    # Get current Greeks
                    try:
                        price_data = await self.price_option(option)
                        greeks = price_data["greeks"]
                        
                        underlying = option.underlying
                        if underlying not in total_delta:
                            total_delta[underlying] = Decimal("0")
                            total_gamma[underlying] = Decimal("0")
                            
                        # Market maker is short options if inventory < 0
                        total_delta[underlying] -= inventory * Decimal(greeks["delta"])
                        total_gamma[underlying] -= inventory * Decimal(greeks["gamma"])
                        
                    except Exception as e:
                        logger.error(f"Error calculating Greeks for {option_id}: {e}")
                        
                # Hedge positions
                for underlying, delta in total_delta.items():
                    if abs(delta) > self.hedge_threshold:
                        # Hedge delta with spot
                        hedge_size = delta  # Buy delta amount of underlying
                        
                        await self.spot_market.submit_order({
                            "user_id": "OPTION_HEDGER",
                            "order_type": "market",
                            "side": "buy" if hedge_size > 0 else "sell",
                            "resource_type": underlying,
                            "quantity": abs(hedge_size),
                            "metadata": {
                                "hedge_type": "delta",
                                "total_delta": str(delta),
                                "total_gamma": str(total_gamma.get(underlying, 0))
                            }
                        })
                        
                        logger.info(f"Hedged {hedge_size} delta for {underlying}")
                        
                await asyncio.sleep(self.hedge_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in hedging loop: {e}")
                await asyncio.sleep(self.hedge_interval * 2)
                
    async def _settlement_loop(self):
        """Settle expired options"""
        while True:
            try:
                # Check for expired options
                for option_id, option in list(self.options.items()):
                    if option.is_expired and option_id in self.options:
                        await self._settle_option(option)
                        
                        # Remove from active options
                        del self.options[option_id]
                        
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in settlement loop: {e}")
                await asyncio.sleep(300)
                
    async def _settle_option(self, option: ComputeOption):
        """Settle an expired option"""
        # Get final spot price
        spot_data = await self.spot_market.get_spot_price(
            option.underlying,
            option.location
        )
        spot_price = Decimal(spot_data.get("last_trade_price", "0"))
        
        # Calculate settlement value
        settlement_value = self._calculate_intrinsic_value(option, spot_price)
        
        # Settle all positions
        for position_id, position in self.positions.items():
            if position.option.option_id == option.option_id:
                if position.quantity > 0:
                    # Long position receives settlement
                    position.realized_pnl += position.quantity * settlement_value
                    
                    # Auto-exercise if ITM
                    if settlement_value > 0:
                        await self.exercise_option(
                            position.user_id,
                            option.option_id,
                            position.quantity
                        )
                        
                # Update position
                position.quantity = Decimal("0")
                await self._update_position(position)
                
        # Emit settlement event
        await self.pulsar.publish('compute.options.settled', {
            'option_id': option.option_id,
            'underlying': option.underlying,
            'strike_price': str(option.strike_price),
            'spot_price': str(spot_price),
            'settlement_value': str(settlement_value),
            'timestamp': datetime.utcnow().isoformat()
        })
        
        logger.info(f"Settled option {option.option_id} at {settlement_value}")
        
    async def _update_position_mtm(self, option_id: str, current_price: Decimal):
        """Update mark-to-market for positions"""
        for position in self.positions.values():
            if position.option.option_id == option_id:
                position.current_price = current_price
                position.unrealized_pnl = position.quantity * (current_price - position.entry_price)
                
    async def _store_option(self, option: ComputeOption):
        """Store option in cache"""
        await self.ignite.put(f"compute_option:{option.option_id}", option)
        
    async def _update_position(self, position: ComputeOptionPosition):
        """Update position in cache"""
        await self.ignite.put(f"option_position:{position.position_id}", position)
        
    async def _store_vol_surface(self, surface: ComputeVolatilitySurface):
        """Store volatility surface"""
        await self.ignite.put(f"vol_surface:{surface.underlying}", surface)
        
    async def _load_active_options(self):
        """Load active options from cache"""
        # In production, scan cache for active options
        pass
        
    async def get_portfolio_risk(self, user_id: str) -> Dict[str, Any]:
        """Get portfolio risk metrics for user"""
        positions = []
        total_delta = Decimal("0")
        total_gamma = Decimal("0")
        total_vega = Decimal("0")
        total_theta = Decimal("0")
        
        for position_id in self.user_positions.get(user_id, []):
            position = self.positions.get(position_id)
            if position and position.quantity != 0:
                positions.append(position)
                
                # Get Greeks
                try:
                    price_data = await self.price_option(position.option)
                    greeks = price_data["greeks"]
                    
                    total_delta += position.quantity * Decimal(greeks["delta"])
                    total_gamma += position.quantity * Decimal(greeks["gamma"])
                    total_vega += position.quantity * Decimal(greeks["vega"])
                    total_theta += position.quantity * Decimal(greeks["theta"])
                    
                except Exception as e:
                    logger.error(f"Error calculating portfolio Greeks: {e}")
                    
        return {
            "positions": len(positions),
            "total_delta": str(total_delta),
            "total_gamma": str(total_gamma),
            "total_vega": str(total_vega),
            "total_theta": str(total_theta),
            "net_exposure": str(sum(p.notional_value for p in positions))
        } 