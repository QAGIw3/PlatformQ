"""Margin calculator for various trading products."""

import logging
from datetime import datetime
from decimal import Decimal
from typing import Dict, Any, Optional

from platformq_risk_common import RiskMetric

logger = logging.getLogger(__name__)


class MarginCalculator:
    """Calculates margin requirements for positions across different products."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.initial_margin_multiplier = Decimal(str(config.get("initial_margin_multiplier", "1.0")))
        self.maintenance_margin_multiplier = Decimal(str(config.get("maintenance_margin_multiplier", "0.5")))
        
        # Product-specific parameters
        self.futures_params = config.get("futures_risk_params", {})
        self.options_params = config.get("options_risk_params", {})
        
    async def calculate_margin(
        self,
        position: Dict[str, Any],
        market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate margin requirements for a position."""
        product_type = position.get("product_type", "spot")
        
        if product_type == "futures":
            return await self._calculate_futures_margin(position, market_data)
        elif product_type == "options":
            return await self._calculate_options_margin(position, market_data)
        elif product_type == "perpetual":
            return await self._calculate_perpetual_margin(position, market_data)
        else:
            return await self._calculate_spot_margin(position, market_data)
    
    async def _calculate_futures_margin(
        self,
        position: Dict[str, Any],
        market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate margin for futures positions."""
        mark_price = Decimal(str(market_data.get("price", "0")))
        quantity = abs(Decimal(str(position.get("quantity", "0"))))
        contract_size = Decimal(str(position.get("contract_size", "1")))
        
        notional_value = quantity * contract_size * mark_price
        
        # Base margin calculation
        base_margin_rate = Decimal(str(market_data.get("base_margin_rate", "0.1")))
        volatility = Decimal(str(market_data.get("volatility", "0.2")))
        
        # Adjust margin based on volatility
        volatility_multiplier = 1 + (volatility - Decimal("0.2")) * Decimal("2")
        volatility_multiplier = max(Decimal("0.5"), min(Decimal("3"), volatility_multiplier))
        
        initial_margin = notional_value * base_margin_rate * volatility_multiplier * self.initial_margin_multiplier
        maintenance_margin = initial_margin * self.maintenance_margin_multiplier
        
        # Calculate liquidation price
        collateral = Decimal(str(position.get("collateral", "0")))
        side = position.get("side", "long")
        
        liquidation_price = self._calculate_liquidation_price(
            mark_price,
            quantity,
            contract_size,
            collateral,
            maintenance_margin,
            side
        )
        
        # Calculate margin ratio
        margin_ratio = collateral / maintenance_margin if maintenance_margin > 0 else Decimal("999")
        margin_usage = maintenance_margin / collateral if collateral > 0 else Decimal("999")
        
        return {
            "initial_margin": initial_margin,
            "maintenance_margin": maintenance_margin,
            "margin_ratio": margin_ratio,
            "margin_usage": margin_usage,
            "liquidation_price": liquidation_price,
            "collateral": collateral,
            "notional_value": notional_value
        }
    
    async def _calculate_options_margin(
        self,
        position: Dict[str, Any],
        market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate margin for options positions."""
        option_type = position.get("option_type", "call")
        is_long = position.get("side", "long") == "long"
        
        mark_price = Decimal(str(market_data.get("price", "0")))
        strike_price = Decimal(str(position.get("strike_price", "0")))
        quantity = abs(Decimal(str(position.get("quantity", "0"))))
        contract_size = Decimal(str(position.get("contract_size", "1")))
        
        # For long options, margin is just the premium paid
        if is_long:
            premium = Decimal(str(position.get("premium", "0")))
            initial_margin = premium * quantity * contract_size
            maintenance_margin = initial_margin  # No maintenance for long options
        else:
            # For short options, use SPAN-like calculation
            underlying_price = Decimal(str(market_data.get("underlying_price", mark_price)))
            volatility = Decimal(str(market_data.get("implied_volatility", "0.3")))
            
            # Simplified margin calculation for short options
            intrinsic_value = max(Decimal("0"), 
                underlying_price - strike_price if option_type == "call" 
                else strike_price - underlying_price
            )
            
            otm_amount = max(Decimal("0"),
                strike_price - underlying_price if option_type == "call"
                else underlying_price - strike_price
            )
            
            # Base margin: greater of (price + intrinsic) or percentage of underlying
            price_based = mark_price + intrinsic_value
            percentage_based = underlying_price * Decimal("0.15") - otm_amount * Decimal("0.5")
            
            margin_per_contract = max(price_based, percentage_based, underlying_price * Decimal("0.1"))
            
            initial_margin = margin_per_contract * quantity * contract_size
            maintenance_margin = initial_margin * self.maintenance_margin_multiplier
        
        collateral = Decimal(str(position.get("collateral", "0")))
        margin_ratio = collateral / maintenance_margin if maintenance_margin > 0 else Decimal("999")
        margin_usage = maintenance_margin / collateral if collateral > 0 else Decimal("999")
        
        return {
            "initial_margin": initial_margin,
            "maintenance_margin": maintenance_margin,
            "margin_ratio": margin_ratio,
            "margin_usage": margin_usage,
            "liquidation_price": None,  # Options don't have traditional liquidation price
            "collateral": collateral,
            "notional_value": quantity * contract_size * underlying_price
        }
    
    async def _calculate_perpetual_margin(
        self,
        position: Dict[str, Any],
        market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate margin for perpetual contracts."""
        # Similar to futures but with funding rate considerations
        result = await self._calculate_futures_margin(position, market_data)
        
        # Adjust for funding rate
        funding_rate = Decimal(str(market_data.get("funding_rate", "0")))
        funding_adjustment = abs(funding_rate) * result["notional_value"] * Decimal("8")  # 8-hour funding
        
        result["initial_margin"] += funding_adjustment
        result["maintenance_margin"] = result["initial_margin"] * self.maintenance_margin_multiplier
        
        return result
    
    async def _calculate_spot_margin(
        self,
        position: Dict[str, Any],
        market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Calculate margin for spot positions."""
        mark_price = Decimal(str(market_data.get("price", "0")))
        quantity = abs(Decimal(str(position.get("quantity", "0"))))
        leverage = Decimal(str(position.get("leverage", "1")))
        
        notional_value = quantity * mark_price
        
        # For spot with leverage
        if leverage > 1:
            initial_margin = notional_value / leverage
            maintenance_margin = initial_margin * self.maintenance_margin_multiplier
        else:
            # No leverage, full collateral required
            initial_margin = notional_value
            maintenance_margin = notional_value
        
        collateral = Decimal(str(position.get("collateral", "0")))
        margin_ratio = collateral / maintenance_margin if maintenance_margin > 0 else Decimal("999")
        margin_usage = maintenance_margin / collateral if collateral > 0 else Decimal("999")
        
        # Calculate liquidation price for leveraged positions
        liquidation_price = None
        if leverage > 1:
            side = position.get("side", "long")
            liquidation_price = self._calculate_liquidation_price(
                mark_price,
                quantity,
                Decimal("1"),  # contract_size
                collateral,
                maintenance_margin,
                side
            )
        
        return {
            "initial_margin": initial_margin,
            "maintenance_margin": maintenance_margin,
            "margin_ratio": margin_ratio,
            "margin_usage": margin_usage,
            "liquidation_price": liquidation_price,
            "collateral": collateral,
            "notional_value": notional_value
        }
    
    def _calculate_liquidation_price(
        self,
        mark_price: Decimal,
        quantity: Decimal,
        contract_size: Decimal,
        collateral: Decimal,
        maintenance_margin: Decimal,
        side: str
    ) -> Optional[Decimal]:
        """Calculate liquidation price for a position."""
        if quantity == 0 or contract_size == 0:
            return None
        
        # Liquidation happens when collateral falls to maintenance margin
        # For long: collateral + (liq_price - entry_price) * quantity = maintenance_margin
        # For short: collateral + (entry_price - liq_price) * quantity = maintenance_margin
        
        margin_buffer = self.config.get("liquidation_margin_ratio", Decimal("1.1"))
        required_margin = maintenance_margin * margin_buffer
        
        if side == "long":
            # Long liquidation: price drops
            price_loss_allowed = (collateral - required_margin) / (quantity * contract_size)
            liquidation_price = mark_price - price_loss_allowed
        else:
            # Short liquidation: price rises
            price_loss_allowed = (collateral - required_margin) / (quantity * contract_size)
            liquidation_price = mark_price + price_loss_allowed
        
        return max(Decimal("0"), liquidation_price)
