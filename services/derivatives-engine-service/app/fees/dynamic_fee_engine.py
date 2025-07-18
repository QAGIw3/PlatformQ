from decimal import Decimal
from typing import Dict, Optional, Tuple, List
from datetime import datetime, timedelta
import math

from app.models.fees import FeeStructure, TradingTier, FeeDiscount
from app.integrations.graph_intelligence import GraphIntelligenceClient

class DynamicFeeEngine:
    """
    Advanced maker/taker fee structure with dynamic adjustments
    Most advantageous: Rewards liquidity providers and high-volume traders
    """
    
    def __init__(self, graph_client: GraphIntelligenceClient, ignite_client):
        self.graph = graph_client
        self.ignite = ignite_client
        
        # Base fee structure (basis points: 1 bp = 0.01%)
        self.base_fees = {
            "maker": Decimal("-0.0002"),  # -0.02% (rebate)
            "taker": Decimal("0.0005"),   # 0.05%
        }
        
        # Volume-based tiers (30-day rolling volume in USD)
        self.volume_tiers = [
            TradingTier(
                name="bronze",
                min_volume=Decimal("0"),
                maker_fee=Decimal("-0.0002"),  # -0.02%
                taker_fee=Decimal("0.0005"),   # 0.05%
            ),
            TradingTier(
                name="silver", 
                min_volume=Decimal("100000"),  # $100k
                maker_fee=Decimal("-0.00025"), # -0.025%
                taker_fee=Decimal("0.00045"),  # 0.045%
            ),
            TradingTier(
                name="gold",
                min_volume=Decimal("1000000"),  # $1M
                maker_fee=Decimal("-0.0003"),   # -0.03%
                taker_fee=Decimal("0.0004"),    # 0.04%
            ),
            TradingTier(
                name="platinum",
                min_volume=Decimal("10000000"),  # $10M
                maker_fee=Decimal("-0.00035"),   # -0.035%
                taker_fee=Decimal("0.00035"),    # 0.035%
            ),
            TradingTier(
                name="diamond",
                min_volume=Decimal("100000000"),  # $100M
                maker_fee=Decimal("-0.0004"),     # -0.04%
                taker_fee=Decimal("0.0003"),      # 0.03%
            )
        ]
        
        # Special fee adjustments
        self.adjustments = {
            "high_volatility_multiplier": Decimal("1.5"),  # 50% higher fees
            "low_liquidity_multiplier": Decimal("2.0"),    # 2x fees
            "new_market_discount": Decimal("0.5"),         # 50% discount
            "reputation_max_discount": Decimal("0.5"),     # Up to 50% off
            "platform_token_discount": Decimal("0.25"),    # 25% off if paying in platform token
        }
        
    async def calculate_trading_fee(
        self,
        user_id: str,
        market_id: str,
        order_type: str,  # "maker" or "taker"
        trade_size: Decimal,
        pay_in_platform_token: bool = False
    ) -> Dict[str, Decimal]:
        """
        Calculate dynamic trading fee based on multiple factors
        """
        # Get user's trading tier
        trading_tier = await self._get_user_tier(user_id)
        
        # Base fee from tier
        if order_type == "maker":
            base_fee = trading_tier.maker_fee
        else:
            base_fee = trading_tier.taker_fee
        
        # Apply market-specific adjustments
        market_multiplier = await self._get_market_fee_multiplier(market_id)
        adjusted_fee = base_fee * market_multiplier
        
        # Apply reputation discount
        reputation_discount = await self._calculate_reputation_discount(user_id)
        adjusted_fee *= (Decimal("1") - reputation_discount)
        
        # Apply platform token discount
        if pay_in_platform_token:
            adjusted_fee *= (Decimal("1") - self.adjustments["platform_token_discount"])
        
        # Special incentives
        incentives = await self._check_special_incentives(user_id, market_id)
        for incentive in incentives:
            adjusted_fee *= incentive["multiplier"]
        
        # Calculate absolute fee amount
        fee_amount = abs(adjusted_fee * trade_size)
        
        # For makers, this could be negative (rebate)
        if adjusted_fee < 0:
            fee_amount = -fee_amount
        
        return {
            "fee_rate": adjusted_fee,
            "fee_amount": fee_amount,
            "tier": trading_tier.name,
            "base_rate": base_fee,
            "market_multiplier": market_multiplier,
            "reputation_discount": reputation_discount,
            "platform_token_discount": self.adjustments["platform_token_discount"] if pay_in_platform_token else Decimal("0"),
            "special_incentives": incentives,
            "effective_rate": adjusted_fee,
            "breakdown": {
                "base_fee": base_fee * trade_size,
                "market_adjustment": (base_fee * market_multiplier - base_fee) * trade_size,
                "discounts": base_fee * trade_size - fee_amount
            }
        }
    
    async def _get_user_tier(self, user_id: str) -> TradingTier:
        """
        Determine user's trading tier based on 30-day volume
        """
        # Get 30-day rolling volume
        volume_30d = await self._get_user_volume_30d(user_id)
        
        # Find appropriate tier
        user_tier = self.volume_tiers[0]  # Default to bronze
        
        for tier in reversed(self.volume_tiers):
            if volume_30d >= tier.min_volume:
                user_tier = tier
                break
        
        return user_tier
    
    async def _get_market_fee_multiplier(self, market_id: str) -> Decimal:
        """
        Calculate market-specific fee multiplier based on conditions
        """
        market_data = await self._get_market_data(market_id)
        
        multiplier = Decimal("1.0")
        
        # High volatility adjustment
        if market_data["volatility_24h"] > Decimal("0.1"):  # > 10% daily volatility
            volatility_factor = min(
                market_data["volatility_24h"] / Decimal("0.1"),
                self.adjustments["high_volatility_multiplier"]
            )
            multiplier *= volatility_factor
        
        # Low liquidity adjustment
        if market_data["liquidity_score"] < Decimal("0.5"):
            liquidity_factor = Decimal("2") - market_data["liquidity_score"]
            multiplier *= liquidity_factor
        
        # New market discount
        market_age_days = (datetime.utcnow() - market_data["created_at"]).days
        if market_age_days < 30:
            new_market_factor = self.adjustments["new_market_discount"] + (
                (Decimal("1") - self.adjustments["new_market_discount"]) * 
                (Decimal(market_age_days) / Decimal("30"))
            )
            multiplier *= new_market_factor
        
        return multiplier
    
    async def _calculate_reputation_discount(self, user_id: str) -> Decimal:
        """
        Calculate fee discount based on platform reputation
        """
        reputation = await self.graph.get_user_reputation(user_id)
        
        if not reputation:
            return Decimal("0")
        
        # Weighted reputation score (0-100)
        weighted_score = (
            reputation.technical_prowess * Decimal("0.1") +
            reputation.collaboration_rating * Decimal("0.1") +
            reputation.governance_influence * Decimal("0.3") +  # Governance participation weighted higher
            reputation.creativity_index * Decimal("0.1") +
            reputation.reliability_score * Decimal("0.4")  # Reliability most important for trading
        )
        
        # Convert to discount (0-50%)
        max_discount = self.adjustments["reputation_max_discount"]
        
        # Non-linear scaling - higher reputation gets disproportionally better discounts
        if weighted_score >= 90:
            discount = max_discount
        elif weighted_score >= 70:
            discount = max_discount * Decimal("0.7")
        elif weighted_score >= 50:
            discount = max_discount * Decimal("0.4")
        else:
            discount = max_discount * (weighted_score / Decimal("100"))
        
        return discount
    
    async def _check_special_incentives(
        self,
        user_id: str,
        market_id: str
    ) -> List[Dict]:
        """
        Check for special promotional incentives
        """
        incentives = []
        
        # Market maker incentive program
        if await self._is_market_maker(user_id):
            incentives.append({
                "name": "market_maker_program",
                "multiplier": Decimal("0.5"),  # 50% discount
                "description": "Market maker incentive"
            })
        
        # First trade on new market
        if await self._is_first_trade_on_market(user_id, market_id):
            incentives.append({
                "name": "first_trade_bonus",
                "multiplier": Decimal("0"),  # Free first trade
                "description": "First trade on new market"
            })
        
        # Referral program
        referral_discount = await self._get_referral_discount(user_id)
        if referral_discount > 0:
            incentives.append({
                "name": "referral_program",
                "multiplier": Decimal("1") - referral_discount,
                "description": f"Referral program {referral_discount*100}% discount"
            })
        
        # Competition participant
        if await self._is_competition_participant(user_id):
            incentives.append({
                "name": "trading_competition",
                "multiplier": Decimal("0.2"),  # 80% discount during competition
                "description": "Trading competition participant"
            })
        
        return incentives
    
    async def _get_user_volume_30d(self, user_id: str) -> Decimal:
        """
        Get user's 30-day rolling trading volume
        """
        cache_key = f"volume_30d:{user_id}"
        cached = await self.ignite.get_async(cache_key)
        
        if cached:
            return Decimal(cached)
        
        # Calculate from trading history
        # Implementation would query database
        volume = Decimal("500000")  # Placeholder
        
        # Cache for 1 hour
        await self.ignite.put_async(cache_key, str(volume), ttl=3600)
        
        return volume
    
    async def _is_market_maker(self, user_id: str) -> bool:
        """
        Check if user qualifies as market maker
        """
        # Requirements:
        # 1. Maker volume > 70% of total volume
        # 2. Provides liquidity on both sides
        # 3. Tight spreads
        
        stats = await self._get_user_trading_stats(user_id)
        
        maker_ratio = stats["maker_volume"] / max(stats["total_volume"], Decimal("1"))
        
        return (
            maker_ratio > Decimal("0.7") and
            stats["bid_count"] > 1000 and
            stats["ask_count"] > 1000 and
            stats["avg_spread"] < Decimal("0.002")  # < 0.2%
        )
    
    # Fee distribution system
    async def distribute_fees(self, trade_id: str, fee_amount: Decimal):
        """
        Distribute collected fees to various parties
        """
        distribution = {
            "insurance_pool": fee_amount * Decimal("0.3"),    # 30% to insurance
            "platform_treasury": fee_amount * Decimal("0.3"), # 30% to treasury
            "liquidity_rewards": fee_amount * Decimal("0.2"), # 20% to LPs
            "referrer": Decimal("0"),                         # Variable
            "burn": fee_amount * Decimal("0.1"),              # 10% burn
        }
        
        # Check for referrer
        referrer = await self._get_trade_referrer(trade_id)
        if referrer:
            referrer_share = fee_amount * Decimal("0.1")  # 10% to referrer
            distribution["referrer"] = referrer_share
            distribution["platform_treasury"] -= referrer_share
        
        # Execute distribution
        for recipient, amount in distribution.items():
            if amount > 0:
                await self._transfer_fee_share(recipient, amount)
        
        return distribution 