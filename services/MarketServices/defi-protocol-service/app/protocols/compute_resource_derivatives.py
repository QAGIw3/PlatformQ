"""
Compute Resource Derivatives Protocol

Enables futures, options, and structured products for compute resources.
"""

from typing import Dict, Any, List, Optional, Tuple, Union
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum
import numpy as np
from scipy import stats

from web3 import Web3
from platformq_shared.blockchain import BlockchainClient
from ..models import (
    DerivativeContract, OptionContract, FutureContract,
    StructuredProduct, PricingModel
)
from .derivatives_protocol import DerivativesProtocol

logger = logging.getLogger(__name__)


class ComputeDerivativeType(str, Enum):
    COMPUTE_FUTURE = "compute_future"  # Future delivery of compute
    COMPUTE_OPTION = "compute_option"  # Option on compute resources
    QUALITY_FUTURE = "quality_future"  # Future on quality scores
    BUNDLE_FUTURE = "bundle_future"  # Future on resource bundles
    VOLATILITY_SWAP = "volatility_swap"  # Volatility on compute prices
    STRUCTURED_NOTE = "structured_note"  # Structured compute products


class ComputeOptionType(str, Enum):
    CALL = "call"  # Right to buy compute
    PUT = "put"  # Right to sell compute
    QUALITY_CALL = "quality_call"  # Call on quality threshold
    BUNDLE_OPTION = "bundle_option"  # Option on resource bundle


class ComputeResourceDerivatives(DerivativesProtocol):
    """Extended derivatives protocol for compute resources"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        derivatives_factory_address: str,
        oracle_address: str,
        quantum_market_address: str,
        ai_market_address: str,
        network_market_address: str,
        pricing_engine_address: str
    ):
        super().__init__(
            blockchain_client,
            derivatives_factory_address,
            oracle_address,
            pricing_engine_address
        )
        
        # Compute market addresses
        self.quantum_market_address = quantum_market_address
        self.ai_market_address = ai_market_address
        self.network_market_address = network_market_address
        
        # Pricing parameters
        self.volatility_window_days = 30
        self.risk_free_rate = Decimal("0.02")  # 2% annual
        
        # Greeks calculation intervals
        self.greeks_update_interval = 300  # 5 minutes
        
        # Active contracts tracking
        self._compute_futures = {}  # contract_id -> future_data
        self._compute_options = {}  # contract_id -> option_data
        self._structured_products = {}  # product_id -> product_data
        
    async def create_compute_future(
        self,
        resource_type: str,
        resource_specs: Dict[str, Any],
        quantity: int,
        delivery_date: datetime,
        settlement_type: str = "physical"  # or "cash"
    ) -> Dict[str, Any]:
        """
        Create a future contract for compute resources
        
        Args:
            resource_type: Type of compute (quantum/ai/network)
            resource_specs: Specifications for the resource
            quantity: Amount of compute resources
            delivery_date: Future delivery date
            settlement_type: Physical delivery or cash settlement
            
        Returns:
            Future contract details
        """
        try:
            # Calculate initial margin requirements
            spot_price = await self._get_spot_price(resource_type, resource_specs)
            notional_value = spot_price * quantity
            
            # Historical volatility for margin calculation
            volatility = await self._calculate_volatility(resource_type)
            
            # Initial margin: 5-15% based on volatility
            margin_rate = Decimal("0.05") + volatility * Decimal("0.5")
            initial_margin = notional_value * margin_rate
            
            # Create future contract on-chain
            futures_factory = await self.blockchain.get_contract(
                self.derivatives_factory_address,
                "ComputeFuturesFactory"
            )
            
            tx = await futures_factory.functions.createComputeFuture(
                resource_type,
                Web3.toJSON(resource_specs),
                quantity,
                int(delivery_date.timestamp()),
                settlement_type == "physical",
                Web3.toWei(initial_margin, 'ether')
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get contract details from event
            future_created = receipt.events.get('ComputeFutureCreated')
            contract_id = future_created['contractId']
            
            # Calculate fair value using cost-of-carry model
            time_to_maturity = (delivery_date - datetime.utcnow()).days / 365
            storage_cost = await self._get_storage_cost(resource_type)
            convenience_yield = await self._get_convenience_yield(resource_type)
            
            fair_value = spot_price * np.exp(
                (self.risk_free_rate + storage_cost - convenience_yield) * float(time_to_maturity)
            )
            
            # Store future data
            future_data = {
                'contract_id': contract_id,
                'resource_type': resource_type,
                'resource_specs': resource_specs,
                'quantity': quantity,
                'delivery_date': delivery_date,
                'settlement_type': settlement_type,
                'spot_price': spot_price,
                'fair_value': Decimal(str(fair_value)),
                'initial_margin': initial_margin,
                'created_at': datetime.utcnow(),
                'open_interest': quantity
            }
            
            self._compute_futures[contract_id] = future_data
            
            logger.info(f"Created compute future {contract_id} for {resource_type}")
            
            return {
                'contract_id': contract_id,
                'notional_value': notional_value,
                'spot_price': spot_price,
                'fair_value': future_data['fair_value'],
                'initial_margin': initial_margin,
                'delivery_date': delivery_date,
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to create compute future: {e}")
            raise
    
    async def create_compute_option(
        self,
        resource_type: str,
        resource_specs: Dict[str, Any],
        option_type: ComputeOptionType,
        strike_price: Decimal,
        quantity: int,
        expiration_date: datetime,
        american_style: bool = False
    ) -> Dict[str, Any]:
        """
        Create an option contract for compute resources
        
        Args:
            resource_type: Type of compute resource
            resource_specs: Resource specifications
            option_type: Call or put option
            strike_price: Strike price per unit
            quantity: Number of units
            expiration_date: Option expiration
            american_style: American (True) or European (False) style
            
        Returns:
            Option contract details
        """
        try:
            # Get current spot price
            spot_price = await self._get_spot_price(resource_type, resource_specs)
            
            # Calculate option premium using Black-Scholes-Merton
            time_to_expiry = (expiration_date - datetime.utcnow()).days / 365
            volatility = await self._calculate_volatility(resource_type)
            
            premium = await self._black_scholes_compute(
                spot_price,
                strike_price,
                time_to_expiry,
                self.risk_free_rate,
                volatility,
                option_type in [ComputeOptionType.CALL, ComputeOptionType.QUALITY_CALL]
            )
            
            # Adjust for American style if applicable
            if american_style:
                premium *= Decimal("1.1")  # Simple adjustment, use binomial in production
            
            total_premium = premium * quantity
            
            # Create option contract on-chain
            options_factory = await self.blockchain.get_contract(
                self.derivatives_factory_address,
                "ComputeOptionsFactory"
            )
            
            tx = await options_factory.functions.createComputeOption(
                resource_type,
                Web3.toJSON(resource_specs),
                option_type,
                Web3.toWei(strike_price, 'ether'),
                quantity,
                int(expiration_date.timestamp()),
                american_style,
                Web3.toWei(total_premium, 'ether')
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get contract details
            option_created = receipt.events.get('ComputeOptionCreated')
            contract_id = option_created['contractId']
            
            # Calculate Greeks
            greeks = await self._calculate_greeks(
                spot_price,
                strike_price,
                time_to_expiry,
                self.risk_free_rate,
                volatility,
                option_type in [ComputeOptionType.CALL, ComputeOptionType.QUALITY_CALL]
            )
            
            # Store option data
            option_data = {
                'contract_id': contract_id,
                'resource_type': resource_type,
                'resource_specs': resource_specs,
                'option_type': option_type,
                'strike_price': strike_price,
                'quantity': quantity,
                'expiration_date': expiration_date,
                'american_style': american_style,
                'premium': premium,
                'total_premium': total_premium,
                'spot_price': spot_price,
                'volatility': volatility,
                'greeks': greeks,
                'created_at': datetime.utcnow()
            }
            
            self._compute_options[contract_id] = option_data
            
            # Start Greeks monitoring
            asyncio.create_task(self._monitor_option_greeks(contract_id))
            
            return {
                'contract_id': contract_id,
                'premium_per_unit': premium,
                'total_premium': total_premium,
                'spot_price': spot_price,
                'strike_price': strike_price,
                'greeks': greeks,
                'implied_volatility': volatility,
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to create compute option: {e}")
            raise
    
    async def create_quality_linked_note(
        self,
        resource_type: str,
        principal: Decimal,
        quality_threshold: int,
        coupon_rate: Decimal,
        maturity_date: datetime,
        barrier_type: str = "european"  # or "american"
    ) -> Dict[str, Any]:
        """
        Create a structured note linked to compute quality scores
        
        Args:
            resource_type: Type of compute resource
            principal: Note principal amount
            quality_threshold: Quality score threshold (0-100)
            coupon_rate: Coupon rate if quality maintained
            maturity_date: Note maturity
            barrier_type: European (at maturity) or American (continuous)
            
        Returns:
            Structured note details
        """
        try:
            # Get current quality statistics
            quality_stats = await self._get_quality_statistics(resource_type)
            current_quality = quality_stats['average']
            quality_volatility = quality_stats['volatility']
            
            # Price the structured note
            # Base value + option value for quality protection
            time_to_maturity = (maturity_date - datetime.utcnow()).days / 365
            
            # Quality as underlying with barrier option pricing
            barrier_option_value = await self._price_barrier_option(
                current_quality,
                quality_threshold,
                quality_volatility,
                time_to_maturity,
                barrier_type
            )
            
            # Expected coupon payments
            quality_probability = await self._calculate_quality_probability(
                current_quality,
                quality_threshold,
                quality_volatility,
                time_to_maturity
            )
            
            expected_coupons = principal * coupon_rate * time_to_maturity * quality_probability
            
            # Note fair value
            fair_value = principal * Decimal(str(np.exp(-float(self.risk_free_rate * time_to_maturity))))
            fair_value += expected_coupons * Decimal(str(np.exp(-float(self.risk_free_rate * time_to_maturity / 2))))
            fair_value += barrier_option_value
            
            # Create structured note on-chain
            structured_factory = await self.blockchain.get_contract(
                self.derivatives_factory_address,
                "StructuredProductsFactory"
            )
            
            tx = await structured_factory.functions.createQualityLinkedNote(
                resource_type,
                Web3.toWei(principal, 'ether'),
                quality_threshold,
                int(coupon_rate * 10000),  # Basis points
                int(maturity_date.timestamp()),
                barrier_type == "american"
            ).transact({'value': Web3.toWei(fair_value, 'ether')})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get product details
            product_created = receipt.events.get('QualityNoteCreated')
            product_id = product_created['productId']
            
            # Store product data
            product_data = {
                'product_id': product_id,
                'type': 'quality_linked_note',
                'resource_type': resource_type,
                'principal': principal,
                'quality_threshold': quality_threshold,
                'coupon_rate': coupon_rate,
                'maturity_date': maturity_date,
                'barrier_type': barrier_type,
                'fair_value': fair_value,
                'current_quality': current_quality,
                'quality_probability': quality_probability,
                'created_at': datetime.utcnow()
            }
            
            self._structured_products[product_id] = product_data
            
            # Start quality monitoring
            asyncio.create_task(self._monitor_quality_barrier(product_id))
            
            return {
                'product_id': product_id,
                'fair_value': fair_value,
                'principal': principal,
                'expected_return': expected_coupons / principal,
                'quality_probability': quality_probability,
                'current_quality': current_quality,
                'protection_cost': barrier_option_value,
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to create quality-linked note: {e}")
            raise
    
    async def create_compute_volatility_swap(
        self,
        resource_type: str,
        notional: Decimal,
        strike_volatility: Decimal,
        maturity_date: datetime,
        observation_frequency: str = "daily"
    ) -> Dict[str, Any]:
        """
        Create a volatility swap on compute resource prices
        
        Args:
            resource_type: Type of compute resource
            notional: Notional amount
            strike_volatility: Strike volatility (annualized)
            maturity_date: Swap maturity
            observation_frequency: Price observation frequency
            
        Returns:
            Volatility swap details
        """
        try:
            # Calculate current implied and realized volatility
            implied_vol = await self._get_implied_volatility(resource_type)
            realized_vol = await self._calculate_volatility(resource_type)
            
            # Fair strike calculation
            vol_risk_premium = Decimal("0.02")  # 2% volatility risk premium
            fair_strike = implied_vol - vol_risk_premium
            
            # Calculate swap value
            time_to_maturity = (maturity_date - datetime.utcnow()).days / 365
            expected_variance = implied_vol ** 2 * time_to_maturity
            strike_variance = strike_volatility ** 2 * time_to_maturity
            
            swap_value = notional * (expected_variance - strike_variance)
            
            # Create volatility swap on-chain
            vol_swap_factory = await self.blockchain.get_contract(
                self.derivatives_factory_address,
                "VolatilitySwapFactory"
            )
            
            observation_days = {
                'daily': 1,
                'weekly': 7,
                'monthly': 30
            }.get(observation_frequency, 1)
            
            tx = await vol_swap_factory.functions.createComputeVolSwap(
                resource_type,
                Web3.toWei(notional, 'ether'),
                int(strike_volatility * 10000),  # Basis points
                int(maturity_date.timestamp()),
                observation_days
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Get swap details
            swap_created = receipt.events.get('VolSwapCreated')
            swap_id = swap_created['swapId']
            
            # Store swap data
            swap_data = {
                'swap_id': swap_id,
                'resource_type': resource_type,
                'notional': notional,
                'strike_volatility': strike_volatility,
                'fair_strike': fair_strike,
                'maturity_date': maturity_date,
                'observation_frequency': observation_frequency,
                'current_implied_vol': implied_vol,
                'current_realized_vol': realized_vol,
                'swap_value': swap_value,
                'created_at': datetime.utcnow()
            }
            
            self._structured_products[swap_id] = swap_data
            
            # Start variance observation
            asyncio.create_task(self._observe_variance(swap_id))
            
            return {
                'swap_id': swap_id,
                'notional': notional,
                'strike_volatility': strike_volatility,
                'fair_strike': fair_strike,
                'current_implied_vol': implied_vol,
                'current_realized_vol': realized_vol,
                'swap_value': swap_value,
                'vega_notional': notional * 2 * strike_volatility * time_to_maturity,
                'tx_hash': tx
            }
            
        except Exception as e:
            logger.error(f"Failed to create volatility swap: {e}")
            raise
    
    async def hedge_compute_portfolio(
        self,
        portfolio: List[Dict[str, Any]],
        hedge_objective: str = "delta_neutral",
        constraints: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Create optimal hedge for compute resource portfolio
        
        Args:
            portfolio: List of compute positions
            hedge_objective: Hedging objective (delta_neutral, vega_neutral, etc.)
            constraints: Additional constraints (max_cost, allowed_instruments, etc.)
            
        Returns:
            Hedge recommendation
        """
        try:
            # Calculate portfolio Greeks
            portfolio_greeks = {
                'delta': Decimal("0"),
                'gamma': Decimal("0"),
                'vega': Decimal("0"),
                'theta': Decimal("0")
            }
            
            for position in portfolio:
                if position['type'] == 'spot':
                    portfolio_greeks['delta'] += position['quantity']
                elif position['type'] == 'option':
                    option_data = self._compute_options.get(position['contract_id'])
                    if option_data:
                        for greek, value in option_data['greeks'].items():
                            portfolio_greeks[greek] += value * position['quantity']
                elif position['type'] == 'future':
                    portfolio_greeks['delta'] += position['quantity']
            
            # Generate hedge based on objective
            hedge_instruments = []
            
            if hedge_objective == "delta_neutral":
                # Hedge delta using futures
                if portfolio_greeks['delta'] != 0:
                    hedge_delta = -portfolio_greeks['delta']
                    
                    # Find appropriate future contract
                    future_recommendation = {
                        'instrument': 'future',
                        'action': 'buy' if hedge_delta > 0 else 'sell',
                        'quantity': abs(hedge_delta),
                        'resource_type': portfolio[0]['resource_type'],
                        'maturity': 'nearest_liquid'
                    }
                    
                    hedge_instruments.append(future_recommendation)
            
            elif hedge_objective == "vega_neutral":
                # Hedge vega using options
                if portfolio_greeks['vega'] != 0:
                    hedge_vega = -portfolio_greeks['vega']
                    
                    # Use ATM options for vega hedge
                    option_recommendation = {
                        'instrument': 'option',
                        'action': 'buy' if hedge_vega > 0 else 'sell',
                        'quantity': abs(hedge_vega) / Decimal("0.4"),  # Approximate ATM vega
                        'type': 'straddle',  # Buy/sell both call and put
                        'strike': 'atm',
                        'maturity': '30_days'
                    }
                    
                    hedge_instruments.append(option_recommendation)
            
            elif hedge_objective == "tail_risk":
                # Hedge tail risk using OTM puts
                portfolio_value = sum(
                    p['quantity'] * p.get('price', 100)
                    for p in portfolio
                )
                
                put_recommendation = {
                    'instrument': 'option',
                    'action': 'buy',
                    'type': 'put',
                    'strike': '0.8x_spot',  # 20% OTM
                    'quantity': portfolio_value / 100,  # Rough sizing
                    'maturity': '90_days'
                }
                
                hedge_instruments.append(put_recommendation)
            
            # Calculate hedge cost
            total_hedge_cost = Decimal("0")
            for hedge in hedge_instruments:
                if hedge['instrument'] == 'option':
                    # Rough option cost estimate
                    total_hedge_cost += hedge['quantity'] * Decimal("5")
                elif hedge['instrument'] == 'future':
                    # Margin requirement
                    total_hedge_cost += hedge['quantity'] * Decimal("10")
            
            # Apply constraints if provided
            if constraints:
                max_cost = constraints.get('max_cost')
                if max_cost and total_hedge_cost > max_cost:
                    # Scale down hedge
                    scale_factor = max_cost / total_hedge_cost
                    for hedge in hedge_instruments:
                        hedge['quantity'] *= scale_factor
                    total_hedge_cost = max_cost
            
            return {
                'portfolio_greeks': portfolio_greeks,
                'hedge_objective': hedge_objective,
                'hedge_instruments': hedge_instruments,
                'estimated_cost': total_hedge_cost,
                'residual_risk': {
                    'delta': portfolio_greeks['delta'] + sum(
                        h['quantity'] * (1 if h['action'] == 'buy' else -1)
                        for h in hedge_instruments
                        if h['instrument'] == 'future'
                    ),
                    'vega': portfolio_greeks['vega']  # Simplified
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to hedge portfolio: {e}")
            raise
    
    # Pricing helper methods
    
    async def _black_scholes_compute(
        self,
        spot: Decimal,
        strike: Decimal,
        time_to_expiry: float,
        risk_free_rate: Decimal,
        volatility: Decimal,
        is_call: bool
    ) -> Decimal:
        """Black-Scholes pricing for compute options"""
        S = float(spot)
        K = float(strike)
        T = time_to_expiry
        r = float(risk_free_rate)
        sigma = float(volatility)
        
        # Handle edge cases
        if T <= 0:
            return Decimal(str(max(0, S - K if is_call else K - S)))
        
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        if is_call:
            price = S * stats.norm.cdf(d1) - K * np.exp(-r * T) * stats.norm.cdf(d2)
        else:
            price = K * np.exp(-r * T) * stats.norm.cdf(-d2) - S * stats.norm.cdf(-d1)
        
        return Decimal(str(max(0, price)))
    
    async def _calculate_greeks(
        self,
        spot: Decimal,
        strike: Decimal,
        time_to_expiry: float,
        risk_free_rate: Decimal,
        volatility: Decimal,
        is_call: bool
    ) -> Dict[str, Decimal]:
        """Calculate option Greeks"""
        S = float(spot)
        K = float(strike)
        T = max(0.001, time_to_expiry)  # Avoid division by zero
        r = float(risk_free_rate)
        sigma = float(volatility)
        
        d1 = (np.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        # Delta
        if is_call:
            delta = stats.norm.cdf(d1)
        else:
            delta = stats.norm.cdf(d1) - 1
        
        # Gamma
        gamma = stats.norm.pdf(d1) / (S * sigma * np.sqrt(T))
        
        # Vega
        vega = S * stats.norm.pdf(d1) * np.sqrt(T) / 100  # Per 1% vol change
        
        # Theta
        if is_call:
            theta = (
                -S * stats.norm.pdf(d1) * sigma / (2 * np.sqrt(T))
                - r * K * np.exp(-r * T) * stats.norm.cdf(d2)
            ) / 365  # Per day
        else:
            theta = (
                -S * stats.norm.pdf(d1) * sigma / (2 * np.sqrt(T))
                + r * K * np.exp(-r * T) * stats.norm.cdf(-d2)
            ) / 365
        
        return {
            'delta': Decimal(str(delta)),
            'gamma': Decimal(str(gamma)),
            'vega': Decimal(str(vega)),
            'theta': Decimal(str(theta))
        }
    
    async def _calculate_volatility(
        self,
        resource_type: str,
        window_days: Optional[int] = None
    ) -> Decimal:
        """Calculate historical volatility for resource type"""
        if not window_days:
            window_days = self.volatility_window_days
        
        # In production, would fetch historical prices
        # For now, return estimated volatilities
        base_volatilities = {
            'quantum': Decimal("0.40"),  # 40% - high volatility
            'ai': Decimal("0.25"),  # 25% - medium volatility
            'network': Decimal("0.15")  # 15% - lower volatility
        }
        
        return base_volatilities.get(resource_type.lower(), Decimal("0.30"))
    
    async def _get_spot_price(
        self,
        resource_type: str,
        specs: Dict[str, Any]
    ) -> Decimal:
        """Get current spot price for resource"""
        market_address = {
            'quantum': self.quantum_market_address,
            'ai': self.ai_market_address,
            'network': self.network_market_address
        }.get(resource_type.lower())
        
        # In production, would query actual market
        # For now, return mock prices
        base_prices = {
            'quantum': Decimal("500"),  # $500/hour for quantum
            'ai': Decimal("100"),  # $100/hour for AI
            'network': Decimal("50")  # $50/hour for network
        }
        
        return base_prices.get(resource_type.lower(), Decimal("100")) 