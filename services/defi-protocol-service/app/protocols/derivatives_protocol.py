"""
Derivatives Protocol

Manages options, perpetual futures, and automated market making for derivatives.
"""

from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
from enum import Enum
import math

from web3 import Web3
from platformq_shared.blockchain import BlockchainClient
from platformq_shared.models import ResourceType
from ..models.derivatives import (
    OptionType, OptionStyle, Option, OptionGreeks,
    PerpetualPosition, PerpetualMarket, FundingRate,
    OptionsPool, OptionOrder, PerpetualOrder,
    DerivativesStats
)

logger = logging.getLogger(__name__)


class PositionSide(str, Enum):
    LONG = "long"
    SHORT = "short"


class DerivativesProtocol:
    """Protocol for managing derivatives operations"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        options_address: str,
        perpetuals_address: str,
        options_amm_address: str,
        resource_token_address: str,
        settlement_token_address: str
    ):
        self.blockchain = blockchain_client
        self.options_address = options_address
        self.perpetuals_address = perpetuals_address
        self.options_amm_address = options_amm_address
        self.resource_token_address = resource_token_address
        self.settlement_token_address = settlement_token_address
        
        # Contract interfaces
        self.options_contract = None
        self.perpetuals_contract = None
        self.options_amm_contract = None
        self.resource_token = None
        self.settlement_token = None
        
        # Market data cache
        self._spot_prices = {}
        self._mark_prices = {}
        self._index_prices = {}
        self._implied_volatilities = {}
        self._funding_rates = {}
        
        # Position tracking
        self._options = {}  # optionId -> Option
        self._perpetual_positions = {}  # user -> resourceId -> Position
        self._options_pools = {}  # resourceId -> Pool
        
        # Price oracle task
        self._oracle_task = None
        self._liquidation_task = None
        
    async def initialize(self):
        """Initialize the derivatives protocol"""
        # Load contract interfaces
        self.options_contract = await self.blockchain.get_contract(
            self.options_address,
            "ResourceOptions"
        )
        
        self.perpetuals_contract = await self.blockchain.get_contract(
            self.perpetuals_address,
            "ResourcePerpetuals"
        )
        
        self.options_amm_contract = await self.blockchain.get_contract(
            self.options_amm_address,
            "OptionsAMM"
        )
        
        self.resource_token = await self.blockchain.get_contract(
            self.resource_token_address,
            "ResourceToken"
        )
        
        self.settlement_token = await self.blockchain.get_contract(
            self.settlement_token_address,
            "ERC20"
        )
        
        # Start monitoring tasks
        self._oracle_task = asyncio.create_task(self._price_oracle_worker())
        self._liquidation_task = asyncio.create_task(self._liquidation_monitor())
        asyncio.create_task(self._funding_rate_worker())
        
        logger.info("Derivatives Protocol initialized")
        
    # Options Methods
    
    async def write_option(
        self,
        writer_address: str,
        resource_token_id: int,
        strike_price: int,
        expiry: datetime,
        option_type: OptionType,
        style: OptionStyle,
        amount: int
    ) -> Dict[str, Any]:
        """
        Write a new option
        
        Args:
            writer_address: Option writer's address
            resource_token_id: Resource token ID
            strike_price: Strike price in settlement token
            expiry: Expiration datetime
            option_type: CALL or PUT
            style: EUROPEAN or AMERICAN
            amount: Amount of resource tokens
            
        Returns:
            Option creation result
        """
        try:
            # Validate inputs
            if expiry <= datetime.utcnow() + timedelta(days=1):
                raise ValueError("Expiry must be at least 1 day in future")
                
            if expiry > datetime.utcnow() + timedelta(days=365):
                raise ValueError("Expiry cannot exceed 1 year")
                
            # Convert option type and style to contract enums
            contract_option_type = 0 if option_type == OptionType.CALL else 1
            contract_style = 0 if style == OptionStyle.EUROPEAN else 1
            
            # Approve collateral
            if option_type == OptionType.CALL:
                # Approve resource tokens
                approve_tx = await self.resource_token.functions.setApprovalForAll(
                    self.options_address,
                    True
                ).transact({"from": writer_address})
            else:
                # Approve settlement tokens for put
                collateral = strike_price * amount // 10**18
                approve_tx = await self.settlement_token.functions.approve(
                    self.options_address,
                    collateral
                ).transact({"from": writer_address})
                
            await self.blockchain.wait_for_transaction(approve_tx)
            
            # Write option
            tx = await self.options_contract.functions.writeOption(
                resource_token_id,
                strike_price,
                int(expiry.timestamp()),
                contract_option_type,
                contract_style,
                amount
            ).transact({"from": writer_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Extract option ID from events
            option_id = None
            for log in receipt.logs:
                if log.topics[0] == self.options_contract.events.OptionWritten.topic:
                    option_id = int(log.data[0])
                    break
                    
            if option_id is None:
                raise ValueError("Failed to create option")
                
            # Create option record
            option = Option(
                option_id=option_id,
                resource_token_id=resource_token_id,
                strike_price=strike_price,
                expiry=expiry,
                amount=amount,
                option_type=option_type,
                style=style,
                writer=writer_address,
                holder=writer_address,
                premium=0,
                created_at=datetime.utcnow()
            )
            
            self._options[option_id] = option
            
            return {
                "option_id": option_id,
                "tx_hash": receipt.transactionHash.hex(),
                "collateral_locked": strike_price * amount // 10**18 if option_type == OptionType.PUT else amount
            }
            
        except Exception as e:
            logger.error(f"Error writing option: {e}")
            raise
            
    async def buy_option(
        self,
        buyer_address: str,
        option_id: int
    ) -> Dict[str, Any]:
        """
        Buy an option from the writer
        
        Args:
            buyer_address: Buyer's address
            option_id: Option ID to buy
            
        Returns:
            Purchase result
        """
        try:
            # Get premium
            premium = await self.options_contract.functions.calculatePremium(option_id).call()
            
            # Approve premium payment
            approve_tx = await self.settlement_token.functions.approve(
                self.options_address,
                premium
            ).transact({"from": buyer_address})
            
            await self.blockchain.wait_for_transaction(approve_tx)
            
            # Buy option
            tx = await self.options_contract.functions.buyOption(
                option_id
            ).transact({"from": buyer_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Update option holder
            if option_id in self._options:
                self._options[option_id].holder = buyer_address
                self._options[option_id].premium = premium
                
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "premium_paid": premium,
                "option_id": option_id
            }
            
        except Exception as e:
            logger.error(f"Error buying option: {e}")
            raise
            
    async def exercise_option(
        self,
        holder_address: str,
        option_id: int
    ) -> Dict[str, Any]:
        """
        Exercise an option
        
        Args:
            holder_address: Option holder's address
            option_id: Option ID to exercise
            
        Returns:
            Exercise result
        """
        try:
            option = self._options.get(option_id)
            if not option:
                # Fetch from contract
                option_data = await self.options_contract.functions.getOption(option_id).call()
                option = self._parse_option_data(option_id, option_data)
                
            # Check if in the money
            is_itm = await self.options_contract.functions.isInTheMoney(option_id).call()
            if not is_itm:
                raise ValueError("Option is not in the money")
                
            # Approve tokens if needed
            if option.option_type == OptionType.CALL:
                # Approve strike payment
                cost = option.strike_price * option.amount // 10**18
                approve_tx = await self.settlement_token.functions.approve(
                    self.options_address,
                    cost
                ).transact({"from": holder_address})
            else:
                # Approve resource tokens for put
                approve_tx = await self.resource_token.functions.setApprovalForAll(
                    self.options_address,
                    True
                ).transact({"from": holder_address})
                
            await self.blockchain.wait_for_transaction(approve_tx)
            
            # Exercise option
            tx = await self.options_contract.functions.exerciseOption(
                option_id
            ).transact({"from": holder_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Calculate payout
            spot_price = self._spot_prices.get(option.resource_token_id, 0)
            if option.option_type == OptionType.CALL:
                payout = (spot_price - option.strike_price) * option.amount // 10**18
            else:
                payout = (option.strike_price - spot_price) * option.amount // 10**18
                
            # Mark as exercised
            if option_id in self._options:
                self._options[option_id].exercised = True
                
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "payout": payout,
                "profit": payout - option.premium if option.premium else payout
            }
            
        except Exception as e:
            logger.error(f"Error exercising option: {e}")
            raise
            
    async def calculate_option_greeks(
        self,
        option_id: int
    ) -> OptionGreeks:
        """Calculate option Greeks"""
        try:
            greeks_data = await self.options_contract.functions.getGreeks(option_id).call()
            
            return OptionGreeks(
                delta=greeks_data[0] / 10000,  # Convert from basis points
                gamma=greeks_data[1] / 10000,
                theta=greeks_data[2] / 10**18,  # Per day
                vega=greeks_data[3] / 10000,
                rho=0  # Not implemented in contract
            )
            
        except Exception as e:
            logger.error(f"Error calculating Greeks: {e}")
            return OptionGreeks(delta=0, gamma=0, theta=0, vega=0, rho=0)
            
    # Perpetuals Methods
    
    async def open_perpetual_position(
        self,
        trader_address: str,
        resource_token_id: int,
        size: int,
        margin: int,
        is_long: bool
    ) -> Dict[str, Any]:
        """
        Open a perpetual futures position
        
        Args:
            trader_address: Trader's address
            resource_token_id: Resource token ID
            size: Position size
            margin: Margin amount
            is_long: True for long, False for short
            
        Returns:
            Position opening result
        """
        try:
            # Check leverage
            mark_price = await self._get_mark_price(resource_token_id)
            notional = size * mark_price // 10**18
            leverage = notional * 10**18 // margin
            
            if leverage > 20 * 10**18:  # 20x max
                raise ValueError("Leverage too high (max 20x)")
                
            # Approve margin
            fee = notional * 30 // 10000  # 0.3% trading fee
            total_payment = margin + fee
            
            approve_tx = await self.settlement_token.functions.approve(
                self.perpetuals_address,
                total_payment
            ).transact({"from": trader_address})
            
            await self.blockchain.wait_for_transaction(approve_tx)
            
            # Open position
            tx = await self.perpetuals_contract.functions.openPosition(
                resource_token_id,
                size,
                margin,
                is_long
            ).transact({"from": trader_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Create position record
            position = PerpetualPosition(
                user=trader_address,
                resource_token_id=resource_token_id,
                size=size,
                entry_price=mark_price,
                margin=margin,
                is_long=is_long,
                opened_at=datetime.utcnow()
            )
            
            if trader_address not in self._perpetual_positions:
                self._perpetual_positions[trader_address] = {}
            self._perpetual_positions[trader_address][resource_token_id] = position
            
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "position_size": size,
                "entry_price": mark_price,
                "leverage": leverage // 10**18,
                "margin": margin,
                "fee": fee
            }
            
        except Exception as e:
            logger.error(f"Error opening perpetual position: {e}")
            raise
            
    async def close_perpetual_position(
        self,
        trader_address: str,
        resource_token_id: int,
        size: int = 0
    ) -> Dict[str, Any]:
        """
        Close a perpetual position
        
        Args:
            trader_address: Trader's address
            resource_token_id: Resource token ID
            size: Size to close (0 for full close)
            
        Returns:
            Position closing result
        """
        try:
            # Get position info
            position_data = await self.perpetuals_contract.functions.getPositionInfo(
                trader_address,
                resource_token_id
            ).call()
            
            if position_data[0][0] == 0:  # size == 0
                raise ValueError("No position found")
                
            # Close position
            tx = await self.perpetuals_contract.functions.closePosition(
                resource_token_id,
                size
            ).transact({"from": trader_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Extract PnL from events
            pnl = 0
            fee = 0
            for log in receipt.logs:
                if log.topics[0] == self.perpetuals_contract.events.PositionClosed.topic:
                    pnl = int(log.data[2])
                    fee = int(log.data[3])
                    break
                    
            # Update or remove position
            if trader_address in self._perpetual_positions:
                if resource_token_id in self._perpetual_positions[trader_address]:
                    if size == 0 or size >= position_data[0][0]:
                        del self._perpetual_positions[trader_address][resource_token_id]
                    else:
                        self._perpetual_positions[trader_address][resource_token_id].size -= size
                        
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "realized_pnl": pnl,
                "fee": fee,
                "closed_size": size if size > 0 else position_data[0][0]
            }
            
        except Exception as e:
            logger.error(f"Error closing perpetual position: {e}")
            raise
            
    async def add_margin(
        self,
        trader_address: str,
        resource_token_id: int,
        amount: int
    ) -> Dict[str, Any]:
        """Add margin to a position"""
        try:
            # Approve margin
            approve_tx = await self.settlement_token.functions.approve(
                self.perpetuals_address,
                amount
            ).transact({"from": trader_address})
            
            await self.blockchain.wait_for_transaction(approve_tx)
            
            # Add margin
            tx = await self.perpetuals_contract.functions.addMargin(
                resource_token_id,
                amount
            ).transact({"from": trader_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Update position
            if (trader_address in self._perpetual_positions and 
                resource_token_id in self._perpetual_positions[trader_address]):
                self._perpetual_positions[trader_address][resource_token_id].margin += amount
                
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "margin_added": amount
            }
            
        except Exception as e:
            logger.error(f"Error adding margin: {e}")
            raise
            
    async def get_position_info(
        self,
        trader_address: str,
        resource_token_id: int
    ) -> Dict[str, Any]:
        """Get detailed position information"""
        try:
            position_data = await self.perpetuals_contract.functions.getPositionInfo(
                trader_address,
                resource_token_id
            ).call()
            
            position = position_data[0]
            unrealized_pnl = position_data[1]
            margin_ratio = position_data[2]
            
            return {
                "size": position[0],
                "entry_price": position[1],
                "margin": position[2],
                "is_long": position[5],
                "unrealized_pnl": unrealized_pnl,
                "margin_ratio": margin_ratio / 100,  # Convert from basis points
                "liquidation_price": self._calculate_liquidation_price(
                    position[1],  # entry_price
                    position[2],  # margin
                    position[0],  # size
                    position[5]   # is_long
                )
            }
            
        except Exception as e:
            logger.error(f"Error getting position info: {e}")
            raise
            
    # Options AMM Methods
    
    async def create_options_pool(
        self,
        creator_address: str,
        resource_token_id: int,
        resource_amount: int,
        stablecoin_amount: int,
        base_iv: int
    ) -> Dict[str, Any]:
        """Create a new options AMM pool"""
        try:
            # Approve tokens
            approve_resource_tx = await self.resource_token.functions.setApprovalForAll(
                self.options_amm_address,
                True
            ).transact({"from": creator_address})
            
            approve_stable_tx = await self.settlement_token.functions.approve(
                self.options_amm_address,
                stablecoin_amount
            ).transact({"from": creator_address})
            
            await self.blockchain.wait_for_transaction(approve_resource_tx)
            await self.blockchain.wait_for_transaction(approve_stable_tx)
            
            # Create pool
            tx = await self.options_amm_contract.functions.createPool(
                resource_token_id,
                resource_amount,
                stablecoin_amount,
                base_iv
            ).transact({"from": creator_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            # Create pool record
            pool = OptionsPool(
                resource_token_id=resource_token_id,
                total_liquidity=int((resource_amount * stablecoin_amount) ** 0.5),
                resource_reserve=resource_amount,
                stablecoin_reserve=stablecoin_amount,
                base_iv=base_iv,
                created_at=datetime.utcnow()
            )
            
            self._options_pools[resource_token_id] = pool
            
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "pool_id": resource_token_id,
                "liquidity": pool.total_liquidity
            }
            
        except Exception as e:
            logger.error(f"Error creating options pool: {e}")
            raise
            
    async def add_options_liquidity(
        self,
        provider_address: str,
        resource_token_id: int,
        resource_amount: int,
        stablecoin_amount: int
    ) -> Dict[str, Any]:
        """Add liquidity to options AMM pool"""
        try:
            # Get minimum liquidity
            pool = await self.options_amm_contract.functions.pools(resource_token_id).call()
            resource_liquidity = resource_amount * pool[1] // pool[2]
            stablecoin_liquidity = stablecoin_amount * pool[1] // pool[3]
            min_liquidity = min(resource_liquidity, stablecoin_liquidity) * 95 // 100
            
            # Approve tokens
            approve_resource_tx = await self.resource_token.functions.setApprovalForAll(
                self.options_amm_address,
                True
            ).transact({"from": provider_address})
            
            approve_stable_tx = await self.settlement_token.functions.approve(
                self.options_amm_address,
                stablecoin_amount
            ).transact({"from": provider_address})
            
            await self.blockchain.wait_for_transaction(approve_resource_tx)
            await self.blockchain.wait_for_transaction(approve_stable_tx)
            
            # Add liquidity
            tx = await self.options_amm_contract.functions.addLiquidity(
                resource_token_id,
                resource_amount,
                stablecoin_amount,
                min_liquidity
            ).transact({"from": provider_address})
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            return {
                "tx_hash": receipt.transactionHash.hex(),
                "liquidity_added": min_liquidity
            }
            
        except Exception as e:
            logger.error(f"Error adding options liquidity: {e}")
            raise
            
    async def sell_option_via_amm(
        self,
        buyer_address: str,
        resource_token_id: int,
        strike_price: int,
        expiry: datetime,
        option_type: OptionType,
        amount: int
    ) -> Dict[str, Any]:
        """Buy an option from the AMM"""
        try:
            # Calculate premium
            premium = await self.options_amm_contract.functions.calculatePremium(
                resource_token_id,
                strike_price,
                int(expiry.timestamp()),
                0 if option_type == OptionType.CALL else 1,
                amount
            ).call()
            
            # This would be called by a keeper in practice
            # For now, return the premium quote
            return {
                "premium": premium,
                "strike_price": strike_price,
                "expiry": expiry.isoformat(),
                "option_type": option_type.value,
                "amount": amount
            }
            
        except Exception as e:
            logger.error(f"Error selling option via AMM: {e}")
            raise
            
    async def get_derivatives_stats(self) -> DerivativesStats:
        """Get overall derivatives statistics"""
        total_options_volume = 0
        total_perpetuals_volume = 0
        open_interest = 0
        
        # Get perpetuals stats
        for resource_id in range(1, 4):  # CPU, GPU, Storage
            try:
                market = await self.perpetuals_contract.functions.markets(resource_id).call()
                if market[7]:  # isActive
                    open_interest += market[1]  # openInterest
            except:
                pass
                
        return DerivativesStats(
            total_options_volume=total_options_volume,
            total_perpetuals_volume=total_perpetuals_volume,
            open_interest=open_interest,
            active_options=len(self._options),
            active_positions=sum(
                len(positions) for positions in self._perpetual_positions.values()
            ),
            total_pools=len(self._options_pools)
        )
        
    # Internal Methods
    
    async def _get_mark_price(self, resource_token_id: int) -> int:
        """Get mark price for a resource"""
        mark_price = await self.perpetuals_contract.functions.getMarkPrice(
            resource_token_id
        ).call()
        
        self._mark_prices[resource_token_id] = mark_price
        return mark_price
        
    def _calculate_liquidation_price(
        self,
        entry_price: int,
        margin: int,
        size: int,
        is_long: bool
    ) -> int:
        """Calculate liquidation price for a position"""
        # Liquidation occurs at 2.5% margin ratio
        liquidation_margin_ratio = 250  # basis points
        
        if is_long:
            # Long liquidation: price drops
            # margin + (current_price - entry_price) * size = liquidation_margin_ratio * size * current_price / 10000
            # Solving for current_price:
            liquidation_price = (
                entry_price * size - margin * 10000 // liquidation_margin_ratio
            ) * 10**18 // size
        else:
            # Short liquidation: price rises
            liquidation_price = (
                entry_price * size + margin * 10000 // liquidation_margin_ratio
            ) * 10**18 // size
            
        return max(0, liquidation_price)
        
    def _parse_option_data(self, option_id: int, data: tuple) -> Option:
        """Parse option data from contract"""
        return Option(
            option_id=option_id,
            resource_token_id=data[0],
            strike_price=data[1],
            expiry=datetime.fromtimestamp(data[2]),
            amount=data[3],
            option_type=OptionType.CALL if data[4] == 0 else OptionType.PUT,
            style=OptionStyle.EUROPEAN if data[5] == 0 else OptionStyle.AMERICAN,
            writer=data[6],
            holder=data[7],
            exercised=data[8],
            expired=data[9],
            premium=data[10],
            created_at=datetime.utcnow()
        )
        
    async def _price_oracle_worker(self):
        """Update prices from various sources"""
        while True:
            try:
                # In production, this would fetch from multiple sources
                # For now, calculate from AMM
                for resource_id in range(1, 4):  # CPU, GPU, Storage
                    try:
                        # Get AMM price
                        from ..protocols.infrastructure_amm_protocol import InfrastructureAMMProtocol
                        # This is simplified - would need proper dependency injection
                        spot_price = self._spot_prices.get(resource_id, 50000000000000000)  # 0.05 ETH default
                        
                        # Update contracts
                        if hasattr(self, "oracle_role"):
                            await self.options_contract.functions.updateSpotPrice(
                                resource_id,
                                spot_price
                            ).transact()
                            
                            # Mark price can have funding premium/discount
                            mark_price = spot_price  # Simplified
                            index_price = spot_price
                            
                            await self.perpetuals_contract.functions.updatePrices(
                                resource_id,
                                index_price,
                                mark_price
                            ).transact()
                            
                    except Exception as e:
                        logger.error(f"Error updating price for resource {resource_id}: {e}")
                        
                await asyncio.sleep(60)  # Update every minute
                
            except Exception as e:
                logger.error(f"Error in price oracle: {e}")
                await asyncio.sleep(300)
                
    async def _liquidation_monitor(self):
        """Monitor positions for liquidation"""
        while True:
            try:
                # Check all positions
                for trader, positions in self._perpetual_positions.items():
                    for resource_id, position in positions.items():
                        try:
                            # Get position info
                            position_data = await self.perpetuals_contract.functions.getPositionInfo(
                                trader,
                                resource_id
                            ).call()
                            
                            margin_ratio = position_data[2]
                            
                            # Check if liquidatable (< 2.5% margin)
                            if margin_ratio < 250:
                                logger.warning(
                                    f"Position liquidatable: {trader} on resource {resource_id}, "
                                    f"margin ratio: {margin_ratio/100}%"
                                )
                                # In production, would trigger liquidation
                                
                        except Exception as e:
                            logger.error(f"Error checking position: {e}")
                            
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except Exception as e:
                logger.error(f"Error in liquidation monitor: {e}")
                await asyncio.sleep(60)
                
    async def _funding_rate_worker(self):
        """Update funding rates periodically"""
        while True:
            try:
                # Update funding every 8 hours
                await asyncio.sleep(8 * 3600)
                
                for resource_id in range(1, 4):
                    try:
                        if hasattr(self, "keeper_role"):
                            await self.perpetuals_contract.functions.updateFunding(
                                resource_id
                            ).transact()
                            
                            # Get updated funding rate
                            market = await self.perpetuals_contract.functions.markets(
                                resource_id
                            ).call()
                            
                            self._funding_rates[resource_id] = market[3]  # fundingRate
                            
                    except Exception as e:
                        logger.error(f"Error updating funding for resource {resource_id}: {e}")
                        
            except Exception as e:
                logger.error(f"Error in funding rate worker: {e}")
                await asyncio.sleep(3600) 