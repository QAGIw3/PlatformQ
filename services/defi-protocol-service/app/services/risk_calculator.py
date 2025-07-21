"""
Risk Calculator Service

Calculates risk scores and manages risk parameters for DeFi operations.
"""

import logging
from typing import Dict, Any
from decimal import Decimal

logger = logging.getLogger(__name__)


class RiskCalculator:
    """Calculates risk metrics for DeFi operations"""
    
    def __init__(self, price_oracle: 'PriceOracle', volatility_window: int = 30, insurance_protocol: 'InsuranceProtocol' = None):
        self.price_oracle = price_oracle
        self.volatility_window = volatility_window
        self.insurance_protocol = insurance_protocol
        
    async def calculate_risk(
        self,
        chain: str,
        user: str,
        operation: str,
        amount: float
    ) -> float:
        """
        Calculate risk score for an operation.
        
        Returns:
            Risk score between 0 (low risk) and 1 (high risk)
        """
        # Placeholder implementation
        # In production, would consider:
        # - Historical volatility
        # - Liquidity depth
        # - User history
        # - Protocol risk
        # - Smart contract audits
        
        base_risk = 0.3  # Base risk level
        
        # Adjust based on amount
        if amount > 100000:
            base_risk += 0.2
        elif amount > 10000:
            base_risk += 0.1
            
        # Adjust based on operation
        operation_risks = {
            "lending": 0.1,
            "borrowing": 0.2,
            "yield_farming": 0.15,
            "liquidity_provision": 0.1,
            "leverage": 0.3
        }
        base_risk += operation_risks.get(operation, 0.1)
        
        # Cap at 1.0
        return min(base_risk, 1.0)
        
    async def calculate_collateral_ratio(
        self,
        collateral_token: str,
        collateral_amount: Decimal,
        debt_token: str,
        debt_amount: Decimal
    ) -> Decimal:
        """Calculate collateralization ratio"""
        collateral_price = await self.price_oracle.get_price(collateral_token)
        debt_price = await self.price_oracle.get_price(debt_token)
        
        collateral_value = collateral_amount * collateral_price
        debt_value = debt_amount * debt_price
        
        if debt_value == 0:
            return Decimal("999999")  # No debt
            
        return collateral_value / debt_value
        
    async def check_liquidation_risk(
        self,
        collateral_ratio: Decimal,
        liquidation_threshold: Decimal = Decimal("1.5")
    ) -> Dict[str, Any]:
        """Check if position is at risk of liquidation"""
        is_safe = collateral_ratio >= liquidation_threshold
        margin = collateral_ratio - liquidation_threshold
        
        return {
            "is_safe": is_safe,
            "collateral_ratio": float(collateral_ratio),
            "liquidation_threshold": float(liquidation_threshold),
            "safety_margin": float(margin),
            "risk_level": "safe" if is_safe else "at_risk"
        }
    
    async def calculate_risk_with_insurance(
        self,
        chain: str,
        user: str,
        operation: str,
        amount: float,
        market_type: str = "crypto",
        leverage: int = 1
    ) -> Dict[str, Any]:
        """
        Calculate risk score considering insurance coverage.
        
        Returns enhanced risk assessment including insurance availability.
        """
        # Get base risk score
        base_risk = await self.calculate_risk(chain, user, operation, amount)
        
        # Check insurance coverage if available
        insurance_info = {
            "coverage_available": False,
            "coverage_amount": 0.0,
            "effective_risk": base_risk
        }
        
        if self.insurance_protocol:
            try:
                # Get available coverage
                coverage = await self.insurance_protocol.get_available_coverage(
                    chain=chain,
                    market_type=market_type,
                    leverage=leverage
                )
                
                total_coverage = sum(float(v) for v in coverage.values())
                
                if total_coverage > 0:
                    insurance_info["coverage_available"] = True
                    insurance_info["coverage_amount"] = min(total_coverage, amount)
                    
                    # Reduce effective risk based on coverage
                    coverage_ratio = min(1.0, total_coverage / amount)
                    insurance_info["effective_risk"] = base_risk * (1 - coverage_ratio * 0.5)
                    insurance_info["coverage_tiers"] = {
                        tier.value: float(amount) for tier, amount in coverage.items()
                    }
                    
            except Exception as e:
                logger.warning(f"Could not calculate insurance coverage: {e}")
        
        return {
            "base_risk": base_risk,
            "insurance": insurance_info,
            "recommended_action": self._get_risk_recommendation(
                insurance_info["effective_risk"],
                insurance_info["coverage_available"]
            )
        }
    
    def _get_risk_recommendation(self, effective_risk: float, has_insurance: bool) -> str:
        """Get risk-based recommendation"""
        if effective_risk < 0.3:
            return "Low risk - proceed normally"
        elif effective_risk < 0.5:
            if has_insurance:
                return "Moderate risk - insurance coverage available"
            else:
                return "Moderate risk - consider reducing position size"
        elif effective_risk < 0.7:
            if has_insurance:
                return "High risk - ensure insurance coverage is active"
            else:
                return "High risk - strongly consider insurance or reduce position"
        else:
            return "Very high risk - not recommended even with insurance" 