from enum import Enum
from typing import Optional, Dict, Any, List
from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass, field


class TierLevel(Enum):
    """Trading tier levels"""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    PLATINUM = "platinum"
    DIAMOND = "diamond"
    MARKET_MAKER = "market_maker"


class DiscountType(Enum):
    """Fee discount types"""
    VOLUME = "volume"
    TOKEN_HOLD = "token_hold"
    TOKEN_STAKE = "token_stake"
    REFERRAL = "referral"
    CAMPAIGN = "campaign"
    VIP = "vip"


@dataclass
class TradingTier:
    """
    User trading tier with associated benefits
    """
    tier_level: TierLevel = TierLevel.BRONZE
    
    # Volume requirements (30-day)
    min_volume_usd: Decimal = Decimal("0")
    max_volume_usd: Optional[Decimal] = None
    
    # Fee rates
    maker_fee: Decimal = Decimal("0.0002")  # 0.02%
    taker_fee: Decimal = Decimal("0.0005")  # 0.05%
    
    # Additional benefits
    withdrawal_limit_daily: Decimal = Decimal("10000")  # USD
    api_rate_limit: int = 100  # requests per second
    priority_support: bool = False
    
    # Rebates and rewards
    maker_rebate: Decimal = Decimal("0")  # Can be negative (rebate)
    platform_token_rewards_multiplier: Decimal = Decimal("1.0")
    
    # Position limits
    max_leverage: Decimal = Decimal("20")
    max_position_size_usd: Decimal = Decimal("1000000")
    
    def get_effective_fee(self, is_maker: bool) -> Decimal:
        """Get effective fee after rebates"""
        base_fee = self.maker_fee if is_maker else self.taker_fee
        if is_maker and self.maker_rebate > 0:
            return base_fee - self.maker_rebate
        return base_fee


@dataclass
class FeeDiscount:
    """
    Fee discount configuration
    """
    id: str = ""
    discount_type: DiscountType = DiscountType.VOLUME
    
    # Discount parameters
    discount_percentage: Decimal = Decimal("0")  # 0-100
    min_requirement: Decimal = Decimal("0")  # Min tokens held, volume, etc.
    
    # Validity
    valid_from: datetime = field(default_factory=datetime.utcnow)
    valid_until: Optional[datetime] = None
    is_active: bool = True
    
    # Stacking rules
    stackable: bool = False
    max_stack_percentage: Decimal = Decimal("50")  # Max 50% discount
    priority: int = 0  # Higher priority applies first
    
    # Conditions
    markets: List[str] = field(default_factory=list)  # Empty = all markets
    user_groups: List[str] = field(default_factory=list)  # Empty = all users
    
    def is_valid(self) -> bool:
        """Check if discount is currently valid"""
        if not self.is_active:
            return False
        now = datetime.utcnow()
        if self.valid_until and now > self.valid_until:
            return False
        return now >= self.valid_from
        
    def calculate_discount(self, base_fee: Decimal) -> Decimal:
        """Calculate discounted fee"""
        if not self.is_valid():
            return base_fee
        discount = base_fee * (self.discount_percentage / Decimal("100"))
        return base_fee - discount


@dataclass
class FeeStructure:
    """
    Complete fee structure for a market or user
    """
    # Base fees
    base_maker_fee: Decimal = Decimal("0.0002")
    base_taker_fee: Decimal = Decimal("0.0005")
    
    # Current tier
    trading_tier: Optional[TradingTier] = None
    
    # Active discounts
    active_discounts: List[FeeDiscount] = field(default_factory=list)
    
    # Computed fees (after all discounts)
    effective_maker_fee: Decimal = Decimal("0")
    effective_taker_fee: Decimal = Decimal("0")
    
    # Fee distribution
    insurance_fund_share: Decimal = Decimal("0.1")  # 10% of fees
    treasury_share: Decimal = Decimal("0.3")  # 30% of fees
    liquidity_provider_share: Decimal = Decimal("0.2")  # 20% of fees
    burn_share: Decimal = Decimal("0.1")  # 10% burned
    referrer_share: Decimal = Decimal("0.1")  # 10% to referrer
    platform_share: Decimal = Decimal("0.2")  # 20% to platform
    
    # Special fee modes
    pay_fee_in_platform_token: bool = False
    platform_token_discount: Decimal = Decimal("0.25")  # 25% discount
    
    # Metadata
    last_updated: datetime = field(default_factory=datetime.utcnow)
    
    def calculate_effective_fees(self):
        """Calculate effective fees after all discounts"""
        # Start with tier fees or base fees
        if self.trading_tier:
            maker_fee = self.trading_tier.maker_fee
            taker_fee = self.trading_tier.taker_fee
        else:
            maker_fee = self.base_maker_fee
            taker_fee = self.base_taker_fee
            
        # Sort discounts by priority
        sorted_discounts = sorted(
            [d for d in self.active_discounts if d.is_valid()],
            key=lambda x: x.priority,
            reverse=True
        )
        
        # Apply discounts
        total_maker_discount = Decimal("0")
        total_taker_discount = Decimal("0")
        
        for discount in sorted_discounts:
            if discount.stackable:
                # Stack with previous discounts up to max
                maker_discount = maker_fee * (discount.discount_percentage / Decimal("100"))
                taker_discount = taker_fee * (discount.discount_percentage / Decimal("100"))
                
                # Check max stack limit
                if total_maker_discount + maker_discount <= maker_fee * (discount.max_stack_percentage / Decimal("100")):
                    total_maker_discount += maker_discount
                if total_taker_discount + taker_discount <= taker_fee * (discount.max_stack_percentage / Decimal("100")):
                    total_taker_discount += taker_discount
            else:
                # Non-stackable - use the best discount
                maker_discount = maker_fee * (discount.discount_percentage / Decimal("100"))
                taker_discount = taker_fee * (discount.discount_percentage / Decimal("100"))
                total_maker_discount = max(total_maker_discount, maker_discount)
                total_taker_discount = max(total_taker_discount, taker_discount)
                
        # Apply platform token discount if enabled
        if self.pay_fee_in_platform_token:
            token_maker_discount = maker_fee * self.platform_token_discount
            token_taker_discount = taker_fee * self.platform_token_discount
            total_maker_discount = max(total_maker_discount, token_maker_discount)
            total_taker_discount = max(total_taker_discount, token_taker_discount)
            
        # Calculate final fees
        self.effective_maker_fee = max(maker_fee - total_maker_discount, Decimal("0"))
        self.effective_taker_fee = max(taker_fee - total_taker_discount, Decimal("0"))
        
        # Allow negative maker fees (rebates) for market makers
        if self.trading_tier and self.trading_tier.tier_level == TierLevel.MARKET_MAKER:
            self.effective_maker_fee = maker_fee - total_maker_discount  # Can be negative
            
        self.last_updated = datetime.utcnow()
        
    def distribute_fees(self, total_fee: Decimal) -> Dict[str, Decimal]:
        """Calculate fee distribution"""
        distribution = {
            "insurance_fund": total_fee * self.insurance_fund_share,
            "treasury": total_fee * self.treasury_share,
            "liquidity_providers": total_fee * self.liquidity_provider_share,
            "burn": total_fee * self.burn_share,
            "referrer": total_fee * self.referrer_share,
            "platform": total_fee * self.platform_share
        }
        
        # Ensure total equals input (handle rounding)
        total_distributed = sum(distribution.values())
        if total_distributed != total_fee:
            # Add difference to platform share
            distribution["platform"] += (total_fee - total_distributed)
            
        return distribution 