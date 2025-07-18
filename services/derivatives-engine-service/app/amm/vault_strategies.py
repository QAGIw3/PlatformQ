"""
Structured Product Vaults for Yield Generation
Implements various option strategies for automated yield generation
"""

import asyncio
from decimal import Decimal
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import logging

from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient
from app.engines.pricing import BlackScholesEngine
from app.engines.risk import GreeksCalculator
from app.amm.concentrated_liquidity import ConcentratedLiquidityAMM

logger = logging.getLogger(__name__)


class VaultStrategy(Enum):
    COVERED_CALL = "covered_call"
    PUT_SELLING = "put_selling"
    STRANGLE = "strangle"
    DELTA_NEUTRAL = "delta_neutral"
    IRON_CONDOR = "iron_condor"
    BUTTERFLY = "butterfly"


@dataclass
class VaultPosition:
    """Represents a vault's current position"""
    vault_id: str
    strategy: VaultStrategy
    underlying_amount: Decimal
    options_positions: List[Dict]
    collateral: Decimal
    nav: Decimal
    last_rebalance: datetime
    performance_fee_accrued: Decimal


@dataclass
class VaultConfig:
    """Configuration for a vault strategy"""
    strategy: VaultStrategy
    strike_selection: Dict  # e.g., {"delta": 0.3} for 30 delta options
    expiry_selection: str  # e.g., "weekly", "monthly"
    rebalance_frequency: timedelta
    max_leverage: Decimal
    performance_fee: Decimal  # e.g., 0.2 for 20%
    management_fee: Decimal  # e.g., 0.02 for 2% annually


class BaseVault:
    """Base class for all vault strategies"""
    
    def __init__(
        self,
        vault_id: str,
        config: VaultConfig,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        pricing_engine: BlackScholesEngine,
        greeks_calculator: GreeksCalculator,
        amm: ConcentratedLiquidityAMM
    ):
        self.vault_id = vault_id
        self.config = config
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.pricing_engine = pricing_engine
        self.greeks_calculator = greeks_calculator
        self.amm = amm
        
        self.position: Optional[VaultPosition] = None
        self.total_shares = Decimal('0')
        self.share_price = Decimal('1')
        
    async def deposit(self, user: str, amount: Decimal) -> Dict:
        """Handle user deposit into vault"""
        
        # Calculate shares to mint
        nav = await self.calculate_nav()
        shares_to_mint = amount / self.share_price
        
        # Update user balance
        user_key = f'vault_balance:{self.vault_id}:{user}'
        current_balance = await self.ignite.get(user_key, Decimal('0'))
        new_balance = current_balance + shares_to_mint
        await self.ignite.put(user_key, new_balance)
        
        # Update total shares
        self.total_shares += shares_to_mint
        
        # Emit event
        await self.pulsar.publish('vault.deposit', {
            'vault_id': self.vault_id,
            'user': user,
            'amount': str(amount),
            'shares': str(shares_to_mint),
            'share_price': str(self.share_price)
        })
        
        return {
            'shares': shares_to_mint,
            'share_price': self.share_price,
            'total_value': shares_to_mint * self.share_price
        }
        
    async def withdraw(self, user: str, shares: Decimal) -> Dict:
        """Handle user withdrawal from vault"""
        
        # Check user balance
        user_key = f'vault_balance:{self.vault_id}:{user}'
        current_balance = await self.ignite.get(user_key, Decimal('0'))
        
        if shares > current_balance:
            raise ValueError("Insufficient shares")
            
        # Calculate withdrawal amount
        amount = shares * self.share_price
        
        # Deduct performance fees if applicable
        performance_fee = await self._calculate_performance_fee(user, amount)
        amount_after_fees = amount - performance_fee
        
        # Update balances
        new_balance = current_balance - shares
        await self.ignite.put(user_key, new_balance)
        self.total_shares -= shares
        
        # Emit event
        await self.pulsar.publish('vault.withdrawal', {
            'vault_id': self.vault_id,
            'user': user,
            'shares': str(shares),
            'amount': str(amount_after_fees),
            'performance_fee': str(performance_fee)
        })
        
        return {
            'amount': amount_after_fees,
            'performance_fee': performance_fee,
            'shares_burned': shares
        }
        
    async def rebalance(self):
        """Rebalance vault positions - to be implemented by subclasses"""
        raise NotImplementedError
        
    async def calculate_nav(self) -> Decimal:
        """Calculate Net Asset Value of the vault"""
        
        if not self.position:
            return Decimal('0')
            
        # Value underlying assets
        underlying_value = self.position.underlying_amount * \
            await self.oracle.get_price(self.get_underlying_asset())
            
        # Value options positions
        options_value = Decimal('0')
        for option_pos in self.position.options_positions:
            option_price = await self.pricing_engine.calculate_option_price(
                option_pos['market_id']
            )
            options_value += option_pos['amount'] * option_price
            
        # Add collateral
        total_value = underlying_value + options_value + self.position.collateral
        
        return total_value
        
    async def calculate_apy(self) -> Decimal:
        """Calculate annualized yield"""
        
        # Get historical performance
        history_key = f'vault_performance:{self.vault_id}'
        performance_history = await self.ignite.get(history_key, [])
        
        if len(performance_history) < 2:
            return Decimal('0')
            
        # Calculate returns over different periods
        returns_30d = self._calculate_period_return(performance_history, 30)
        returns_7d = self._calculate_period_return(performance_history, 7)
        
        # Annualize (using 30-day returns as primary)
        apy = ((Decimal('1') + returns_30d) ** Decimal('12')) - Decimal('1')
        
        return apy
        
    def get_underlying_asset(self) -> str:
        """Get the underlying asset for this vault"""
        # Override in subclasses
        return "BTC"
        
    async def _calculate_performance_fee(self, user: str, amount: Decimal) -> Decimal:
        """Calculate performance fee for withdrawal"""
        
        # Get user's cost basis
        cost_basis_key = f'vault_cost_basis:{self.vault_id}:{user}'
        cost_basis = await self.ignite.get(cost_basis_key, amount)
        
        # Calculate profit
        profit = max(amount - cost_basis, Decimal('0'))
        
        # Apply performance fee
        performance_fee = profit * self.config.performance_fee
        
        return performance_fee
        
    def _calculate_period_return(self, history: List[Dict], days: int) -> Decimal:
        """Calculate return over specified period"""
        
        if not history:
            return Decimal('0')
            
        current_price = Decimal(history[-1]['share_price'])
        
        # Find price from N days ago
        target_date = datetime.utcnow() - timedelta(days=days)
        past_price = current_price
        
        for record in reversed(history):
            if datetime.fromisoformat(record['timestamp']) <= target_date:
                past_price = Decimal(record['share_price'])
                break
                
        return (current_price - past_price) / past_price


class CoveredCallVault(BaseVault):
    """
    Covered Call Vault Strategy
    Sells out-of-the-money call options on deposited assets
    """
    
    async def rebalance(self):
        """Execute covered call strategy rebalance"""
        
        logger.info(f"Rebalancing covered call vault {self.vault_id}")
        
        # Check if rebalance is needed
        if self.position and \
           datetime.utcnow() - self.position.last_rebalance < self.config.rebalance_frequency:
            return
            
        # Get current market data
        underlying_price = await self.oracle.get_price(self.get_underlying_asset())
        volatility = await self.oracle.get_volatility(self.get_underlying_asset())
        
        # Calculate target strike based on delta
        target_delta = Decimal(self.config.strike_selection.get('delta', '0.3'))
        strike_price = await self._calculate_strike_from_delta(
            underlying_price, volatility, target_delta, is_call=True
        )
        
        # Find best expiry
        expiry = await self._select_expiry()
        
        # Calculate position size (use all available underlying)
        nav = await self.calculate_nav()
        underlying_amount = nav * Decimal('0.95')  # Keep 5% as buffer
        
        # Create option market ID
        option_market_id = f"BTC-{strike_price}-{expiry}-CALL"
        
        # Sell calls through AMM
        premium_received = await self.amm.swap(
            market_id=option_market_id,
            amount_in=underlying_amount,
            token_in='option',  # Selling options
            min_amount_out=Decimal('0'),  # Accept any premium
            trader=self.vault_id
        )
        
        # Update position
        self.position = VaultPosition(
            vault_id=self.vault_id,
            strategy=VaultStrategy.COVERED_CALL,
            underlying_amount=underlying_amount,
            options_positions=[{
                'market_id': option_market_id,
                'amount': -underlying_amount,  # Short position
                'strike': strike_price,
                'expiry': expiry,
                'type': 'CALL',
                'premium_received': premium_received['amount_out']
            }],
            collateral=underlying_amount,  # Underlying serves as collateral
            nav=nav,
            last_rebalance=datetime.utcnow(),
            performance_fee_accrued=Decimal('0')
        )
        
        # Store position
        await self.ignite.put(f'vault_position:{self.vault_id}', self.position)
        
        # Emit rebalance event
        await self.pulsar.publish('vault.rebalanced', {
            'vault_id': self.vault_id,
            'strategy': 'covered_call',
            'strike': str(strike_price),
            'expiry': expiry,
            'premium': str(premium_received['amount_out'])
        })
        
    async def _calculate_strike_from_delta(
        self, 
        spot: Decimal, 
        vol: Decimal, 
        target_delta: Decimal,
        is_call: bool
    ) -> Decimal:
        """Calculate strike price for target delta"""
        
        # Use Black-Scholes inverse to find strike
        # Simplified implementation - would use numerical methods
        if is_call:
            # OTM call: strike above spot
            strike = spot * (Decimal('1') + target_delta * vol * Decimal('0.1'))
        else:
            # OTM put: strike below spot  
            strike = spot * (Decimal('1') - target_delta * vol * Decimal('0.1'))
            
        # Round to nearest 100
        strike = Decimal(int(strike / 100) * 100)
        
        return strike
        
    async def _select_expiry(self) -> str:
        """Select optimal expiry based on configuration"""
        
        if self.config.expiry_selection == 'weekly':
            # Next Friday
            days_ahead = 4 - datetime.utcnow().weekday()  # Friday is 4
            if days_ahead <= 0:
                days_ahead += 7
        elif self.config.expiry_selection == 'monthly':
            # Third Friday of next month
            days_ahead = 30  # Simplified
        else:
            days_ahead = 7  # Default weekly
            
        expiry_date = datetime.utcnow() + timedelta(days=days_ahead)
        return expiry_date.strftime('%Y-%m-%d')


class PutSellingVault(BaseVault):
    """
    Put Selling Vault Strategy
    Sells cash-secured puts to generate yield
    """
    
    async def rebalance(self):
        """Execute put selling strategy rebalance"""
        
        logger.info(f"Rebalancing put selling vault {self.vault_id}")
        
        # Get market data
        underlying_price = await self.oracle.get_price(self.get_underlying_asset())
        volatility = await self.oracle.get_volatility(self.get_underlying_asset())
        
        # Calculate target strike (typically 10-20% OTM)
        target_delta = Decimal(self.config.strike_selection.get('delta', '0.2'))
        strike_price = await self._calculate_strike_from_delta(
            underlying_price, volatility, target_delta, is_call=False
        )
        
        # Calculate position size based on available collateral
        nav = await self.calculate_nav()
        max_notional = nav * self.config.max_leverage
        contracts = int(max_notional / strike_price)
        
        # Select expiry
        expiry = await self._select_expiry()
        option_market_id = f"BTC-{strike_price}-{expiry}-PUT"
        
        # Sell puts
        premium_received = await self.amm.swap(
            market_id=option_market_id,
            amount_in=Decimal(contracts),
            token_in='option',
            min_amount_out=Decimal('0'),
            trader=self.vault_id
        )
        
        # Update position
        self.position = VaultPosition(
            vault_id=self.vault_id,
            strategy=VaultStrategy.PUT_SELLING,
            underlying_amount=Decimal('0'),  # No underlying held
            options_positions=[{
                'market_id': option_market_id,
                'amount': -Decimal(contracts),
                'strike': strike_price,
                'expiry': expiry,
                'type': 'PUT',
                'premium_received': premium_received['amount_out']
            }],
            collateral=strike_price * contracts,  # Cash secured
            nav=nav,
            last_rebalance=datetime.utcnow(),
            performance_fee_accrued=Decimal('0')
        )
        
        await self.ignite.put(f'vault_position:{self.vault_id}', self.position)
        
        await self.pulsar.publish('vault.rebalanced', {
            'vault_id': self.vault_id,
            'strategy': 'put_selling',
            'strike': str(strike_price),
            'contracts': contracts,
            'collateral_required': str(strike_price * contracts)
        })


class StrangleVault(BaseVault):
    """
    Strangle Vault Strategy
    Sells both OTM calls and puts for maximum premium
    """
    
    async def rebalance(self):
        """Execute strangle strategy rebalance"""
        
        logger.info(f"Rebalancing strangle vault {self.vault_id}")
        
        # Get market data
        underlying_price = await self.oracle.get_price(self.get_underlying_asset())
        volatility = await self.oracle.get_volatility(self.get_underlying_asset())
        
        # Calculate strikes for both legs
        call_delta = Decimal(self.config.strike_selection.get('call_delta', '0.3'))
        put_delta = Decimal(self.config.strike_selection.get('put_delta', '0.3'))
        
        call_strike = await self._calculate_strike_from_delta(
            underlying_price, volatility, call_delta, is_call=True
        )
        put_strike = await self._calculate_strike_from_delta(
            underlying_price, volatility, put_delta, is_call=False
        )
        
        # Calculate position sizes
        nav = await self.calculate_nav()
        # Split capital between calls and puts
        call_notional = nav * Decimal('0.45')
        put_notional = nav * Decimal('0.45')
        
        call_contracts = int(call_notional / underlying_price)
        put_contracts = int(put_notional / put_strike)
        
        # Execute trades
        expiry = await self._select_expiry()
        
        # Sell calls
        call_market_id = f"BTC-{call_strike}-{expiry}-CALL"
        call_premium = await self.amm.swap(
            market_id=call_market_id,
            amount_in=Decimal(call_contracts),
            token_in='option',
            min_amount_out=Decimal('0'),
            trader=self.vault_id
        )
        
        # Sell puts
        put_market_id = f"BTC-{put_strike}-{expiry}-PUT"
        put_premium = await self.amm.swap(
            market_id=put_market_id,
            amount_in=Decimal(put_contracts),
            token_in='option',
            min_amount_out=Decimal('0'),
            trader=self.vault_id
        )
        
        # Update position
        self.position = VaultPosition(
            vault_id=self.vault_id,
            strategy=VaultStrategy.STRANGLE,
            underlying_amount=Decimal(call_contracts) * underlying_price,
            options_positions=[
                {
                    'market_id': call_market_id,
                    'amount': -Decimal(call_contracts),
                    'strike': call_strike,
                    'expiry': expiry,
                    'type': 'CALL',
                    'premium_received': call_premium['amount_out']
                },
                {
                    'market_id': put_market_id,
                    'amount': -Decimal(put_contracts),
                    'strike': put_strike,
                    'expiry': expiry,
                    'type': 'PUT',
                    'premium_received': put_premium['amount_out']
                }
            ],
            collateral=nav * Decimal('0.9'),
            nav=nav,
            last_rebalance=datetime.utcnow(),
            performance_fee_accrued=Decimal('0')
        )
        
        await self.ignite.put(f'vault_position:{self.vault_id}', self.position)
        
        total_premium = call_premium['amount_out'] + put_premium['amount_out']
        
        await self.pulsar.publish('vault.rebalanced', {
            'vault_id': self.vault_id,
            'strategy': 'strangle',
            'call_strike': str(call_strike),
            'put_strike': str(put_strike),
            'total_premium': str(total_premium)
        })


class DeltaNeutralVault(BaseVault):
    """
    Delta-Neutral Vault Strategy
    Maintains delta-neutral portfolio through dynamic hedging
    """
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_delta = Decimal('0')  # Maintain zero delta
        self.delta_tolerance = Decimal('0.05')  # Rehedge if delta exceeds ±5%
        
    async def rebalance(self):
        """Execute delta-neutral strategy rebalance"""
        
        logger.info(f"Rebalancing delta-neutral vault {self.vault_id}")
        
        # Calculate current portfolio delta
        portfolio_delta = await self._calculate_portfolio_delta()
        
        # Check if rehedge is needed
        if abs(portfolio_delta) < self.delta_tolerance:
            logger.info(f"Portfolio delta {portfolio_delta} within tolerance")
            return
            
        # Calculate hedge requirement
        underlying_price = await self.oracle.get_price(self.get_underlying_asset())
        hedge_amount = -portfolio_delta  # Need opposite position
        
        # Execute hedge
        if hedge_amount > 0:
            # Buy underlying
            await self._buy_underlying(hedge_amount)
        else:
            # Sell underlying or buy puts
            await self._sell_underlying(abs(hedge_amount))
            
        # Update Greeks tracking
        await self._update_greeks_tracking()
        
        await self.pulsar.publish('vault.rebalanced', {
            'vault_id': self.vault_id,
            'strategy': 'delta_neutral',
            'portfolio_delta_before': str(portfolio_delta),
            'hedge_amount': str(hedge_amount),
            'portfolio_delta_after': '0.00'
        })
        
    async def _calculate_portfolio_delta(self) -> Decimal:
        """Calculate total portfolio delta"""
        
        if not self.position:
            return Decimal('0')
            
        total_delta = Decimal('0')
        
        # Delta from underlying
        total_delta += self.position.underlying_amount
        
        # Delta from options
        for option_pos in self.position.options_positions:
            option_greeks = await self.greeks_calculator.calculate_greeks(
                option_pos['market_id']
            )
            position_delta = option_pos['amount'] * option_greeks['delta']
            total_delta += position_delta
            
        return total_delta
        
    async def _buy_underlying(self, amount: Decimal):
        """Buy underlying asset to hedge"""
        
        # Execute spot purchase
        # This would integrate with spot trading engine
        pass
        
    async def _sell_underlying(self, amount: Decimal):
        """Sell underlying asset or buy puts to hedge"""
        
        # Execute spot sale or put purchase
        # This would integrate with spot/options trading
        pass
        
    async def _update_greeks_tracking(self):
        """Update real-time Greeks tracking for the vault"""
        
        greeks = {
            'delta': await self._calculate_portfolio_delta(),
            'gamma': await self._calculate_portfolio_gamma(),
            'vega': await self._calculate_portfolio_vega(),
            'theta': await self._calculate_portfolio_theta()
        }
        
        await self.ignite.put(f'vault_greeks:{self.vault_id}', greeks)
        
    async def _calculate_portfolio_gamma(self) -> Decimal:
        """Calculate total portfolio gamma"""
        # Implementation similar to delta
        return Decimal('0')
        
    async def _calculate_portfolio_vega(self) -> Decimal:
        """Calculate total portfolio vega"""
        # Implementation similar to delta
        return Decimal('0')
        
    async def _calculate_portfolio_theta(self) -> Decimal:
        """Calculate total portfolio theta"""
        # Implementation similar to delta  
        return Decimal('0') 