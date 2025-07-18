from decimal import Decimal
from typing import Dict, List, Optional
from datetime import datetime
import asyncio

from app.models.collateral import CollateralTier, UserCollateral
from app.integrations.graph_intelligence import GraphIntelligenceClient
from app.integrations.price_oracle import PriceOracleClient

class MultiTierCollateralEngine:
    """
    Advanced collateral system with reputation integration
    Most advantageous approach: Multi-tier with progressive benefits
    """
    
    def __init__(self, ignite_client, graph_client, oracle_client, graph_intelligence=None, asset_compute_nexus=None):
        self.ignite = ignite_client
        self.graph = graph_client
        self.oracle = oracle_client
        self.graph_intelligence = graph_intelligence
        self.asset_compute_nexus = asset_compute_nexus
        
        # Tier definitions (most advantageous configuration)
        self.tiers = {
            "TIER_1_STABLE": {
                "assets": ["USDC", "USDT", "DAI"],
                "ltv_ratio": Decimal("0.95"),  # 95% capital efficiency
                "liquidation_threshold": Decimal("0.97"),
                "interest_rate": Decimal("0.01"),  # 1% APR
                "liquidation_penalty": Decimal("0.02"),  # 2%
            },
            "TIER_2_BLUE_CHIP": {
                "assets": ["ETH", "BTC", "WBTC"],
                "ltv_ratio": Decimal("0.85"),  # 85% capital efficiency
                "liquidation_threshold": Decimal("0.90"),
                "interest_rate": Decimal("0.03"),  # 3% APR
                "liquidation_penalty": Decimal("0.05"),  # 5%
            },
            "TIER_3_PLATFORM": {
                "assets": ["PLATFORM_TOKEN", "vePLATFORM"],  # Locked tokens get better rates
                "ltv_ratio": Decimal("0.75"),
                "liquidation_threshold": Decimal("0.80"),
                "interest_rate": Decimal("0.0"),  # 0% - incentivize platform token use
                "liquidation_penalty": Decimal("0.10"),
            },
            "TIER_4_DIGITAL_ASSETS": {
                "assets": ["DIGITAL_ASSET_NFT", "IP_TOKEN"],
                "ltv_ratio": Decimal("0.50"),
                "liquidation_threshold": Decimal("0.65"),
                "interest_rate": Decimal("0.05"),
                "liquidation_penalty": Decimal("0.15"),
            },
            "TIER_5_REPUTATION": {  # Revolutionary: Reputation as collateral
                "assets": ["REPUTATION_SCORE"],
                "ltv_ratio": Decimal("0.0"),  # Base 0, scales with reputation
                "liquidation_threshold": Decimal("1.0"),  # Can't be liquidated
                "interest_rate": Decimal("0.0"),
                "liquidation_penalty": Decimal("0.0"),
            }
        }
        
    async def calculate_total_collateral_value(
        self,
        user_id: str,
        include_reputation: bool = True
    ) -> Dict[str, Decimal]:
        """
        Calculate total collateral value including reputation bonus
        """
        # Get user's collateral positions
        collateral_positions = await self._get_user_collateral(user_id)
        
        total_value = Decimal("0")
        total_borrowing_power = Decimal("0")
        breakdown = {}
        
        # Calculate traditional collateral
        for position in collateral_positions:
            asset = position["asset"]
            amount = Decimal(str(position["amount"]))
            
            # Get current price
            price = await self.oracle.get_price(asset)
            value_usd = amount * price
            
            # Find tier and apply LTV
            tier = self._get_asset_tier(asset)
            if tier:
                ltv_ratio = tier["ltv_ratio"]
                borrowing_power = value_usd * ltv_ratio
                
                total_value += value_usd
                total_borrowing_power += borrowing_power
                
                breakdown[asset] = {
                    "amount": amount,
                    "value_usd": value_usd,
                    "ltv_ratio": ltv_ratio,
                    "borrowing_power": borrowing_power
                }
        
        # Add reputation-based collateral (revolutionary feature)
        if include_reputation:
            reputation_credit = await self._calculate_reputation_credit(user_id)
            if reputation_credit > 0:
                total_borrowing_power += reputation_credit
                breakdown["REPUTATION_CREDIT"] = {
                    "amount": Decimal("1"),
                    "value_usd": reputation_credit,
                    "ltv_ratio": Decimal("1.0"),
                    "borrowing_power": reputation_credit
                }
        
        # Apply cross-collateral synergies (advantageous feature)
        synergy_bonus = self._calculate_synergy_bonus(breakdown)
        total_borrowing_power *= (Decimal("1") + synergy_bonus)
        
        return {
            "total_value": total_value,
            "total_borrowing_power": total_borrowing_power,
            "breakdown": breakdown,
            "synergy_bonus": synergy_bonus,
            "reputation_credit": reputation_credit if include_reputation else Decimal("0")
        }
    
    async def _calculate_reputation_credit(self, user_id: str) -> Decimal:
        """
        Calculate unsecured credit based on platform reputation
        Revolutionary: Top users can trade with minimal collateral
        Enhanced with Graph Intelligence risk assessment
        """
        # Use Graph Intelligence for comprehensive risk assessment if available
        if self.graph_intelligence:
            try:
                # Get risk assessment
                risk_assessment = await self.graph_intelligence.assess_counterparty_risk(
                    trader_id=user_id,
                    transaction_type="collateral_credit"
                )
                
                # Low risk users get more credit
                if risk_assessment.risk_score < 30:  # Very low risk
                    base_credit = Decimal("50000")  # $50k unsecured
                elif risk_assessment.risk_score < 50:  # Low risk
                    base_credit = Decimal("20000")  # $20k unsecured
                elif risk_assessment.risk_score < 70:  # Medium risk
                    base_credit = Decimal("5000")   # $5k unsecured
                else:
                    base_credit = Decimal("0")      # High risk, no credit
                
                # Adjust by trust score components
                trust_score = risk_assessment.trust_score
                multiplier = (
                    trust_score.reliability / 100 * Decimal("0.3") +
                    trust_score.competence / 100 * Decimal("0.2") +
                    trust_score.integrity / 100 * Decimal("0.3") +
                    trust_score.collaboration / 100 * Decimal("0.2")
                )
                
                # Apply network effects
                if risk_assessment.network_centrality > 0.7:
                    multiplier *= Decimal("1.2")  # 20% bonus for well-connected users
                
                return base_credit * multiplier
                
            except Exception as e:
                # Fallback to basic reputation if graph intelligence fails
                pass
        
        # Fallback: Get multi-dimensional reputation scores
        reputation = await self.graph.get_user_reputation(user_id)
        
        if not reputation:
            return Decimal("0")
        
        # Weighted reputation score (0-100)
        weighted_score = (
            reputation.technical_prowess * Decimal("0.2") +
            reputation.collaboration_rating * Decimal("0.2") +
            reputation.governance_influence * Decimal("0.2") +
            reputation.creativity_index * Decimal("0.2") +
            reputation.reliability_score * Decimal("0.2")
        )
        
        # Trading-specific reputation
        trading_history = await self._get_trading_history(user_id)
        
        # Credit calculation (exponential scaling for top users)
        base_credit = Decimal("0")
        
        if weighted_score >= 90:  # Top 1% users
            base_credit = Decimal("1000000")  # $1M unsecured
        elif weighted_score >= 80:  # Top 10% users
            base_credit = Decimal("100000")   # $100K unsecured
        elif weighted_score >= 70:  # Top 25% users
            base_credit = Decimal("10000")    # $10K unsecured
        elif weighted_score >= 60:  # Active users
            base_credit = Decimal("1000")     # $1K unsecured
        
        # Adjust based on trading history
        if trading_history:
            # Perfect track record multiplier
            if trading_history["liquidation_count"] == 0:
                base_credit *= Decimal("1.5")
            
            # Profitable trader bonus
            if trading_history["total_pnl"] > 0:
                profit_multiplier = min(
                    Decimal("2.0"),
                    Decimal("1") + (trading_history["total_pnl"] / Decimal("1000000"))
                )
                base_credit *= profit_multiplier
            
            # Market maker bonus
            if trading_history["maker_volume"] > Decimal("10000000"):  # $10M
                base_credit *= Decimal("2.0")
        
        # Platform contribution bonus
        platform_contributions = await self._get_platform_contributions(user_id)
        if platform_contributions["assets_created"] > 100:
            base_credit *= Decimal("1.2")
        if platform_contributions["dao_participation"] > 50:
            base_credit *= Decimal("1.1")
        
        return base_credit
    
    def _calculate_synergy_bonus(self, breakdown: Dict) -> Decimal:
        """
        Reward diversified collateral (risk reduction)
        """
        if len(breakdown) < 2:
            return Decimal("0")
        
        # Count different asset classes
        asset_classes = set()
        for asset in breakdown:
            if asset in ["USDC", "USDT", "DAI"]:
                asset_classes.add("stable")
            elif asset in ["ETH", "BTC", "WBTC"]:
                asset_classes.add("crypto")
            elif asset in ["PLATFORM_TOKEN", "vePLATFORM"]:
                asset_classes.add("platform")
            elif asset == "REPUTATION_CREDIT":
                asset_classes.add("reputation")
            else:
                asset_classes.add("other")
        
        # Bonus based on diversification
        if len(asset_classes) >= 4:
            return Decimal("0.1")  # 10% bonus
        elif len(asset_classes) >= 3:
            return Decimal("0.05")  # 5% bonus
        elif len(asset_classes) >= 2:
            return Decimal("0.02")  # 2% bonus
        
        return Decimal("0")
    
    async def check_health_factor(self, user_id: str, borrowed_amount: Decimal) -> Decimal:
        """
        Calculate health factor for liquidation monitoring
        Health Factor = Total Collateral Value * Liquidation Threshold / Borrowed Amount
        """
        collateral_info = await self.calculate_total_collateral_value(user_id)
        
        if borrowed_amount == 0:
            return Decimal("999999")  # Infinite health
        
        # Calculate weighted liquidation threshold
        weighted_threshold = Decimal("0")
        total_value = Decimal("0")
        
        for asset, info in collateral_info["breakdown"].items():
            if asset != "REPUTATION_CREDIT":  # Reputation can't be liquidated
                tier = self._get_asset_tier(asset)
                if tier:
                    value = info["value_usd"]
                    threshold = tier["liquidation_threshold"]
                    weighted_threshold += value * threshold
                    total_value += value
        
        if total_value > 0:
            avg_threshold = weighted_threshold / total_value
        else:
            avg_threshold = Decimal("1.0")
        
        # Include reputation credit in health calculation
        total_collateral = collateral_info["total_value"] + collateral_info["reputation_credit"]
        
        health_factor = (total_collateral * avg_threshold) / borrowed_amount
        
        return health_factor
    
    def _get_asset_tier(self, asset: str) -> Optional[Dict]:
        """
        Get tier configuration for an asset
        """
        for tier_name, tier_config in self.tiers.items():
            if asset in tier_config["assets"]:
                return tier_config
        return None
    
    async def _get_user_collateral(self, user_id: str) -> List[Dict]:
        """
        Fetch user's collateral positions from database
        """
        # Implementation would fetch from database
        # Placeholder for example
        return []
    
    async def _get_trading_history(self, user_id: str) -> Optional[Dict]:
        """
        Get user's trading history metrics
        """
        # Implementation would fetch from database
        # Placeholder for example
        return {
            "liquidation_count": 0,
            "total_pnl": Decimal("50000"),
            "maker_volume": Decimal("15000000"),
            "taker_volume": Decimal("5000000")
        }
    
    async def _get_platform_contributions(self, user_id: str) -> Dict:
        """
        Get user's platform contribution metrics
        """
        # Implementation would fetch from graph service
        # Placeholder for example
        return {
            "assets_created": 150,
            "dao_participation": 75,
            "compute_contributed": 10000  # GPU hours
        }
    
    async def add_digital_asset_collateral(
        self,
        user_id: str,
        asset_id: str,
        asset_type: str
    ) -> Dict[str, Any]:
        """
        Add a digital asset as collateral
        Enhanced with Asset-Compute Nexus integration
        """
        if not self.asset_compute_nexus:
            return {
                "success": False,
                "error": "Asset-compute nexus not initialized"
            }
        
        try:
            # Import AssetType enum
            from app.integrations.asset_compute_nexus import AssetType
            
            # Convert string to AssetType
            asset_type_enum = AssetType(asset_type)
            
            # Value the asset
            valuation = await self.asset_compute_nexus.value_digital_asset_as_collateral(
                asset_id=asset_id,
                asset_type=asset_type_enum,
                owner_id=user_id
            )
            
            # Check minimum collateral value
            if valuation.collateral_value < Decimal("100"):
                return {
                    "success": False,
                    "error": "Asset value too low for collateral (minimum $100)"
                }
            
            # Store collateral record
            collateral_record = {
                "user_id": user_id,
                "asset_id": asset_id,
                "asset_type": asset_type,
                "collateral_value": str(valuation.collateral_value),
                "intrinsic_value": str(valuation.intrinsic_value),
                "liquidity_discount": str(valuation.liquidity_discount),
                "volatility_adjustment": str(valuation.volatility_adjustment),
                "confidence_score": valuation.confidence_score,
                "added_at": datetime.utcnow().isoformat()
            }
            
            # Cache collateral record
            await self.ignite.set(
                f"digital_collateral:{user_id}:{asset_id}",
                collateral_record,
                ttl=86400 * 30  # 30 days
            )
            
            # Update user's total collateral
            await self._update_user_collateral_totals(user_id, valuation.collateral_value)
            
            return {
                "success": True,
                "asset_id": asset_id,
                "collateral_value": str(valuation.collateral_value),
                "confidence_score": valuation.confidence_score,
                "valuation_details": {
                    "intrinsic_value": str(valuation.intrinsic_value),
                    "liquidity_discount": str(valuation.liquidity_discount),
                    "volatility_adjustment": str(valuation.volatility_adjustment),
                    "metadata": valuation.metadata
                }
            }
            
        except Exception as e:
            logger.error(f"Error adding digital asset collateral: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _update_user_collateral_totals(
        self,
        user_id: str,
        additional_value: Decimal
    ):
        """Update user's total collateral value"""
        key = f"user_collateral_total:{user_id}"
        
        # Get current total
        current_total = await self.ignite.get(key) or Decimal("0")
        if isinstance(current_total, str):
            current_total = Decimal(current_total)
        
        # Update total
        new_total = current_total + additional_value
        await self.ignite.set(key, str(new_total), ttl=86400 * 30) 