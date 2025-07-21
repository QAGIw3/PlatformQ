"""
Resource Valuation Service

Calculates the value of infrastructure resource tokens based on oracle prices,
time decay, and other factors.
"""

from typing import Dict, Any, List
from decimal import Decimal
from datetime import datetime
import logging

from ..models import ResourceType, ServiceTier
from .price_oracle import PriceOracle

logger = logging.getLogger(__name__)


class ResourceValuationService:
    """Service for valuing infrastructure resource tokens"""
    
    # Time decay factor (1% per day)
    TIME_DECAY_FACTOR = Decimal('0.01')
    
    # Base prices in USD per unit hour (can be overridden by oracle)
    BASE_PRICES = {
        ResourceType.CPU: {
            ServiceTier.STANDARD: Decimal('50'),
            ServiceTier.PREMIUM: Decimal('75'),
            ServiceTier.GUARANTEED: Decimal('100')
        },
        ResourceType.GPU: {
            ServiceTier.STANDARD: Decimal('500'),
            ServiceTier.PREMIUM: Decimal('750'),
            ServiceTier.GUARANTEED: Decimal('1000')
        },
        ResourceType.STORAGE: {  # Per GB
            ServiceTier.STANDARD: Decimal('1'),
            ServiceTier.PREMIUM: Decimal('1.5'),
            ServiceTier.GUARANTEED: Decimal('2')
        },
        ResourceType.BANDWIDTH: {  # Per TB
            ServiceTier.STANDARD: Decimal('10'),
            ServiceTier.PREMIUM: Decimal('15'),
            ServiceTier.GUARANTEED: Decimal('20')
        },
        ResourceType.MEMORY: {  # Per GB
            ServiceTier.STANDARD: Decimal('20'),
            ServiceTier.PREMIUM: Decimal('30'),
            ServiceTier.GUARANTEED: Decimal('40')
        }
    }
    
    # Loan-to-value ratios (basis points)
    LTV_RATIOS = {
        ResourceType.CPU: {
            ServiceTier.STANDARD: Decimal('5000'),  # 50%
            ServiceTier.PREMIUM: Decimal('6000'),   # 60%
            ServiceTier.GUARANTEED: Decimal('7000') # 70%
        },
        ResourceType.GPU: {
            ServiceTier.STANDARD: Decimal('4000'),  # 40%
            ServiceTier.PREMIUM: Decimal('5000'),   # 50%
            ServiceTier.GUARANTEED: Decimal('6000') # 60%
        },
        ResourceType.STORAGE: {
            ServiceTier.STANDARD: Decimal('6000'),  # 60%
            ServiceTier.PREMIUM: Decimal('7000'),   # 70%
            ServiceTier.GUARANTEED: Decimal('8000') # 80%
        },
        ResourceType.BANDWIDTH: {
            ServiceTier.STANDARD: Decimal('4500'),  # 45%
            ServiceTier.PREMIUM: Decimal('5500'),   # 55%
            ServiceTier.GUARANTEED: Decimal('6500') # 65%
        },
        ResourceType.MEMORY: {
            ServiceTier.STANDARD: Decimal('5500'),  # 55%
            ServiceTier.PREMIUM: Decimal('6500'),   # 65%
            ServiceTier.GUARANTEED: Decimal('7500') # 75%
        }
    }
    
    # Regional price adjustments
    REGIONAL_MULTIPLIERS = {
        "us-east-1": Decimal('1.0'),
        "us-west-1": Decimal('1.1'),
        "eu-west-1": Decimal('1.15'),
        "ap-southeast-1": Decimal('0.95'),
        "ap-northeast-1": Decimal('1.2'),
        "sa-east-1": Decimal('0.85'),
        "eu-central-1": Decimal('1.1'),
        "us-central-1": Decimal('0.95')
    }
    
    def __init__(self, price_oracle: PriceOracle):
        self.price_oracle = price_oracle
        
    async def calculate_value(
        self,
        resource_type: ResourceType,
        service_tier: ServiceTier,
        region: str,
        amount: int,
        valid_until: datetime
    ) -> Dict[str, Any]:
        """
        Calculate the current value of resource tokens
        
        Args:
            resource_type: Type of resource
            service_tier: Service quality tier
            region: Geographic region
            amount: Amount of resource units
            valid_until: Expiration timestamp
            
        Returns:
            Dictionary with valuation details
        """
        try:
            # Get base price from oracle or use default
            base_price = await self._get_resource_price(resource_type, service_tier, region)
            
            # Calculate time remaining
            now = datetime.utcnow()
            time_remaining = valid_until - now
            days_remaining = max(0, time_remaining.days)
            
            # Apply time decay
            time_decay_factor = self._calculate_time_decay(days_remaining)
            
            # Calculate total value
            total_value = base_price * Decimal(amount) * time_decay_factor
            
            # Get LTV ratio
            ltv_ratio = self.LTV_RATIOS[resource_type][service_tier] / Decimal('10000')
            
            # Calculate maximum loan amount
            max_loan_amount = total_value * ltv_ratio
            
            return {
                "base_price": base_price,
                "time_decay_factor": time_decay_factor,
                "total_value": total_value,
                "max_loan_amount": max_loan_amount,
                "ltv_ratio": ltv_ratio,
                "days_until_expiry": days_remaining
            }
            
        except Exception as e:
            logger.error(f"Error calculating resource value: {e}")
            raise
            
    async def _get_resource_price(
        self,
        resource_type: ResourceType,
        service_tier: ServiceTier,
        region: str
    ) -> Decimal:
        """Get resource price from oracle or use default"""
        try:
            # Try to get price from oracle
            oracle_key = f"RESOURCE_{resource_type.value}_{service_tier.value}_{region}"
            oracle_price = await self.price_oracle.get_price(oracle_key)
            
            if oracle_price and oracle_price > 0:
                return Decimal(str(oracle_price))
                
        except Exception as e:
            logger.warning(f"Failed to get oracle price: {e}")
            
        # Fall back to base price with regional adjustment
        base_price = self.BASE_PRICES[resource_type][service_tier]
        regional_multiplier = self.REGIONAL_MULTIPLIERS.get(region, Decimal('1.0'))
        
        return base_price * regional_multiplier
        
    def _calculate_time_decay(self, days_remaining: int) -> Decimal:
        """
        Calculate time decay factor for expiring resources
        
        Linear decay: loses 1% value per day from 100 days out
        """
        if days_remaining >= 100:
            return Decimal('1.0')  # No decay beyond 100 days
            
        # Calculate decay
        days_decayed = 100 - days_remaining
        decay_amount = Decimal(days_decayed) * self.TIME_DECAY_FACTOR
        
        # Ensure we don't go negative
        time_value = Decimal('1.0') - decay_amount
        return max(Decimal('0'), time_value)
        
    async def batch_valuate(
        self,
        resources: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Value multiple resources in batch
        
        Args:
            resources: List of resource specifications
            
        Returns:
            List of valuation results
        """
        results = []
        
        for resource in resources:
            try:
                result = await self.calculate_value(
                    resource_type=ResourceType(resource['resource_type']),
                    service_tier=ServiceTier(resource['service_tier']),
                    region=resource.get('region', 'us-east-1'),
                    amount=resource['amount'],
                    valid_until=resource['valid_until']
                )
                
                results.append({
                    **resource,
                    **result,
                    "success": True
                })
                
            except Exception as e:
                logger.error(f"Error valuating resource: {e}")
                results.append({
                    **resource,
                    "success": False,
                    "error": str(e)
                })
                
        return results
        
    def get_volatility_factor(self, resource_type: ResourceType) -> Decimal:
        """Get volatility factor for a resource type"""
        volatility_factors = {
            ResourceType.CPU: Decimal('100'),
            ResourceType.GPU: Decimal('150'),  # Most volatile
            ResourceType.STORAGE: Decimal('50'),  # Least volatile
            ResourceType.BANDWIDTH: Decimal('120'),
            ResourceType.MEMORY: Decimal('80')
        }
        
        return volatility_factors.get(resource_type, Decimal('100'))
        
    def calculate_interest_rate(
        self,
        resource_type: ResourceType,
        service_tier: ServiceTier,
        duration_days: int
    ) -> Decimal:
        """
        Calculate interest rate based on resource type and duration
        
        Args:
            resource_type: Type of resource
            service_tier: Service quality tier
            duration_days: Loan duration in days
            
        Returns:
            Annual interest rate as decimal (e.g., 0.05 for 5%)
        """
        # Base rate
        base_rate = Decimal('0.05')  # 5%
        
        # Add volatility premium
        volatility_factor = self.get_volatility_factor(resource_type)
        volatility_premium = volatility_factor / Decimal('10000')  # Convert basis points
        
        # Duration premium (higher rates for longer loans)
        duration_premium = Decimal('0')
        if duration_days > 30:
            duration_premium = Decimal('0.01')  # +1%
        if duration_days > 90:
            duration_premium = Decimal('0.02')  # +2%
        if duration_days > 180:
            duration_premium = Decimal('0.03')  # +3%
            
        # Tier discount (better tiers get lower rates)
        tier_discount = Decimal('0')
        if service_tier == ServiceTier.PREMIUM:
            tier_discount = Decimal('0.005')  # -0.5%
        elif service_tier == ServiceTier.GUARANTEED:
            tier_discount = Decimal('0.01')   # -1%
            
        # Calculate final rate
        total_rate = base_rate + volatility_premium + duration_premium - tier_discount
        
        # Ensure minimum rate
        return max(total_rate, Decimal('0.01'))  # Minimum 1% 