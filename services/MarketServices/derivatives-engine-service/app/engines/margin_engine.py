"""
Margin Engine for Derivatives Trading

Handles margin calculations, collateral management, and risk assessment
"""

from decimal import Decimal, getcontext
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
from collections import defaultdict

# Set high precision
getcontext().prec = 28

logger = logging.getLogger(__name__)


class CollateralType(Enum):
    """Types of accepted collateral"""
    CASH = "cash"
    COMPUTE_TOKEN = "compute_token"
    DIGITAL_ASSET = "digital_asset"
    ML_MODEL = "ml_model"
    DATASET = "dataset"
    COMPUTE_VOUCHER = "compute_voucher"
    STABLECOIN = "stablecoin"
    LP_TOKEN = "lp_token"


@dataclass
class CollateralAsset:
    """Represents a collateral asset"""
    asset_id: str
    asset_type: CollateralType
    amount: Decimal
    value_usd: Decimal
    haircut: Decimal  # Percentage reduction in value for margin
    last_updated: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def margin_value(self) -> Decimal:
        """Value after haircut"""
        return self.value_usd * (Decimal("1") - self.haircut)


@dataclass
class MarginRequirement:
    """Margin requirements for a position"""
    initial_margin: Decimal
    maintenance_margin: Decimal
    variation_margin: Decimal
    stress_test_margin: Decimal
    total_required: Decimal
    currency: str = "USD"
    calculation_time: datetime = field(default_factory=datetime.utcnow)
    breakdown: Dict[str, Decimal] = field(default_factory=dict)


@dataclass
class MarginAccount:
    """User's margin account"""
    user_id: str
    account_id: str
    collateral: List[CollateralAsset] = field(default_factory=list)
    positions: Dict[str, Any] = field(default_factory=dict)  # position_id -> position
    
    # Margin metrics
    total_collateral_value: Decimal = Decimal("0")
    used_margin: Decimal = Decimal("0")
    available_margin: Decimal = Decimal("0")
    margin_ratio: Decimal = Decimal("0")
    
    # Risk metrics
    portfolio_var: Decimal = Decimal("0")
    stress_loss: Decimal = Decimal("0")
    liquidation_price: Dict[str, Decimal] = field(default_factory=dict)
    
    # Trust score integration
    trust_score: Decimal = Decimal("0")
    unsecured_limit: Decimal = Decimal("0")
    
    # Status
    is_liquidatable: bool = False
    last_margin_call: Optional[datetime] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


class MarginEngine:
    """Advanced margin calculation and management engine"""
    
    def __init__(self, graph_intelligence_client=None, oracle_client=None,
                 ignite_cache=None, pulsar_publisher=None):
        self.graph_client = graph_intelligence_client
        self.oracle = oracle_client
        self.cache = ignite_cache
        self.pulsar = pulsar_publisher
        
        # Margin parameters by product type
        self.margin_parameters = {
            "futures": {
                "initial_margin_rate": Decimal("0.1"),  # 10%
                "maintenance_margin_rate": Decimal("0.05"),  # 5%
                "min_margin": Decimal("100")
            },
            "options": {
                "initial_margin_rate": Decimal("0.15"),  # 15%
                "maintenance_margin_rate": Decimal("0.075"),  # 7.5%
                "min_margin": Decimal("50")
            },
            "perpetuals": {
                "initial_margin_rate": Decimal("0.05"),  # 5%
                "maintenance_margin_rate": Decimal("0.025"),  # 2.5%
                "min_margin": Decimal("200")
            }
        }
        
        # Collateral haircuts by type
        self.haircuts = {
            CollateralType.CASH: Decimal("0"),  # No haircut
            CollateralType.STABLECOIN: Decimal("0.02"),  # 2%
            CollateralType.COMPUTE_TOKEN: Decimal("0.2"),  # 20%
            CollateralType.DIGITAL_ASSET: Decimal("0.3"),  # 30%
            CollateralType.ML_MODEL: Decimal("0.4"),  # 40%
            CollateralType.DATASET: Decimal("0.35"),  # 35%
            CollateralType.COMPUTE_VOUCHER: Decimal("0.1"),  # 10%
            CollateralType.LP_TOKEN: Decimal("0.25")  # 25%
        }
        
        # Stress test scenarios
        self.stress_scenarios = [
            {"name": "market_crash", "spot_move": Decimal("-0.3"), "vol_move": Decimal("0.5")},
            {"name": "flash_crash", "spot_move": Decimal("-0.5"), "vol_move": Decimal("1.0")},
            {"name": "volatility_spike", "spot_move": Decimal("0"), "vol_move": Decimal("2.0")}
        ]
        
        # Trust-based adjustments
        self.trust_tiers = [
            {"min_score": Decimal("0.9"), "margin_discount": Decimal("0.5"), "unsecured_limit": Decimal("50000")},
            {"min_score": Decimal("0.8"), "margin_discount": Decimal("0.3"), "unsecured_limit": Decimal("20000")},
            {"min_score": Decimal("0.7"), "margin_discount": Decimal("0.2"), "unsecured_limit": Decimal("10000")},
            {"min_score": Decimal("0.6"), "margin_discount": Decimal("0.1"), "unsecured_limit": Decimal("5000")},
            {"min_score": Decimal("0"), "margin_discount": Decimal("0"), "unsecured_limit": Decimal("0")}
        ]
        
        # Margin accounts
        self.accounts: Dict[str, MarginAccount] = {}
        
        # Background tasks
        self._monitoring_task = None
        self._running = False
        
    async def start(self):
        """Start margin monitoring"""
        self._running = True
        self._monitoring_task = asyncio.create_task(self._monitor_margins())
        logger.info("Margin engine started")
        
    async def stop(self):
        """Stop margin monitoring"""
        self._running = False
        if self._monitoring_task:
            await self._monitoring_task
            
    async def get_or_create_account(self, user_id: str) -> MarginAccount:
        """Get or create margin account"""
        account_id = f"margin_{user_id}"
        
        if account_id not in self.accounts:
            # Get trust score
            trust_score = await self._get_trust_score(user_id)
            
            account = MarginAccount(
                user_id=user_id,
                account_id=account_id,
                trust_score=trust_score,
                unsecured_limit=self._calculate_unsecured_limit(trust_score)
            )
            
            self.accounts[account_id] = account
            
            # Load from cache if exists
            cached = await self.cache.get(f"margin_account:{account_id}")
            if cached:
                self.accounts[account_id] = cached
                
        return self.accounts[account_id]
        
    async def calculate_margin_requirement(
        self,
        user_id: str,
        position_type: str,  # "futures", "options", etc
        position_value: Decimal,
        underlying_price: Decimal,
        volatility: Optional[Decimal] = None,
        position_details: Optional[Dict] = None
    ) -> MarginRequirement:
        """Calculate margin requirements for a position"""
        
        account = await self.get_or_create_account(user_id)
        
        # Base margin rates
        params = self.margin_parameters.get(position_type, self.margin_parameters["futures"])
        initial_rate = params["initial_margin_rate"]
        maintenance_rate = params["maintenance_margin_rate"]
        
        # Apply trust-based discount
        discount = self._get_trust_discount(account.trust_score)
        initial_rate *= (Decimal("1") - discount)
        maintenance_rate *= (Decimal("1") - discount)
        
        # Calculate base margins
        initial_margin = max(position_value * initial_rate, params["min_margin"])
        maintenance_margin = max(position_value * maintenance_rate, params["min_margin"] / 2)
        
        # Variation margin (mark-to-market)
        variation_margin = Decimal("0")
        if position_details and "current_mtm" in position_details:
            variation_margin = position_details["current_mtm"] - position_details.get("entry_value", Decimal("0"))
            
        # Stress test margin
        stress_test_margin = await self._calculate_stress_margin(
            position_type, position_value, underlying_price, volatility
        )
        
        # Total required
        total_required = max(initial_margin + variation_margin, stress_test_margin)
        
        return MarginRequirement(
            initial_margin=initial_margin,
            maintenance_margin=maintenance_margin,
            variation_margin=variation_margin,
            stress_test_margin=stress_test_margin,
            total_required=total_required,
            breakdown={
                "base_initial": position_value * params["initial_margin_rate"],
                "trust_discount": position_value * params["initial_margin_rate"] * discount,
                "final_initial": initial_margin,
                "stress_scenarios": stress_test_margin
            }
        )
        
    async def check_margin_sufficiency(
        self,
        user_id: str,
        required_margin: Decimal,
        position_type: str = "futures"
    ) -> Tuple[bool, Dict[str, Any]]:
        """Check if user has sufficient margin"""
        
        account = await self.get_or_create_account(user_id)
        await self._update_account_metrics(account)
        
        # Calculate available margin including unsecured limit
        total_available = account.available_margin + account.unsecured_limit
        
        is_sufficient = total_available >= required_margin
        
        details = {
            "is_sufficient": is_sufficient,
            "required_margin": str(required_margin),
            "available_margin": str(account.available_margin),
            "unsecured_limit": str(account.unsecured_limit),
            "total_available": str(total_available),
            "shortfall": str(max(Decimal("0"), required_margin - total_available)),
            "margin_ratio": str(account.margin_ratio),
            "collateral_breakdown": [
                {
                    "type": c.asset_type.value,
                    "amount": str(c.amount),
                    "value_usd": str(c.value_usd),
                    "margin_value": str(c.margin_value)
                }
                for c in account.collateral
            ]
        }
        
        # Emit margin check event
        await self._emit_margin_event("margin_check", user_id, details)
        
        return is_sufficient, details
        
    async def add_collateral(
        self,
        user_id: str,
        asset_type: CollateralType,
        asset_id: str,
        amount: Decimal,
        metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Add collateral to margin account"""
        
        account = await self.get_or_create_account(user_id)
        
        # Get asset value
        value_usd = await self._get_asset_value(asset_type, asset_id, amount)
        
        # Apply haircut
        haircut = self.haircuts.get(asset_type, Decimal("0.5"))
        
        # Create collateral asset
        collateral = CollateralAsset(
            asset_id=asset_id,
            asset_type=asset_type,
            amount=amount,
            value_usd=value_usd,
            haircut=haircut,
            last_updated=datetime.utcnow(),
            metadata=metadata or {}
        )
        
        # Add to account
        account.collateral.append(collateral)
        
        # Update metrics
        await self._update_account_metrics(account)
        
        # Save to cache
        await self.cache.set(f"margin_account:{account.account_id}", account)
        
        return {
            "success": True,
            "collateral_added": {
                "asset_id": asset_id,
                "type": asset_type.value,
                "amount": str(amount),
                "value_usd": str(value_usd),
                "margin_value": str(collateral.margin_value)
            },
            "account_summary": {
                "total_collateral": str(account.total_collateral_value),
                "available_margin": str(account.available_margin),
                "margin_ratio": str(account.margin_ratio)
            }
        }
        
    async def remove_collateral(
        self,
        user_id: str,
        asset_id: str,
        amount: Optional[Decimal] = None
    ) -> Dict[str, Any]:
        """Remove collateral from margin account"""
        
        account = await self.get_or_create_account(user_id)
        
        # Find collateral
        collateral_item = None
        for c in account.collateral:
            if c.asset_id == asset_id:
                collateral_item = c
                break
                
        if not collateral_item:
            return {"success": False, "error": "Collateral not found"}
            
        # Check if removal would cause margin deficiency
        if amount is None:
            amount = collateral_item.amount
            
        test_value = collateral_item.margin_value * (amount / collateral_item.amount)
        if account.available_margin - test_value < 0:
            return {
                "success": False,
                "error": "Cannot remove collateral - would cause margin deficiency",
                "available_for_removal": str(max(Decimal("0"), account.available_margin))
            }
            
        # Remove or reduce collateral
        if amount >= collateral_item.amount:
            account.collateral.remove(collateral_item)
        else:
            collateral_item.amount -= amount
            collateral_item.value_usd = await self._get_asset_value(
                collateral_item.asset_type,
                collateral_item.asset_id,
                collateral_item.amount
            )
            
        # Update metrics
        await self._update_account_metrics(account)
        
        return {
            "success": True,
            "removed": {
                "asset_id": asset_id,
                "amount": str(amount)
            },
            "account_summary": {
                "total_collateral": str(account.total_collateral_value),
                "available_margin": str(account.available_margin),
                "margin_ratio": str(account.margin_ratio)
            }
        }
        
    async def calculate_liquidation_price(
        self,
        user_id: str,
        position_id: str,
        position_details: Dict
    ) -> Decimal:
        """Calculate liquidation price for a position"""
        
        account = await self.get_or_create_account(user_id)
        
        # Position details
        size = Decimal(position_details.get("size", "0"))
        entry_price = Decimal(position_details.get("entry_price", "0"))
        is_long = position_details.get("is_long", True)
        position_type = position_details.get("type", "futures")
        
        # Get maintenance margin rate
        params = self.margin_parameters.get(position_type, self.margin_parameters["futures"])
        maintenance_rate = params["maintenance_margin_rate"]
        
        # Apply trust discount
        discount = self._get_trust_discount(account.trust_score)
        maintenance_rate *= (Decimal("1") - discount)
        
        # Calculate liquidation price
        # For long: liquidation_price = entry_price * (1 - maintenance_rate)
        # For short: liquidation_price = entry_price * (1 + maintenance_rate)
        
        if is_long:
            liquidation_price = entry_price * (Decimal("1") - maintenance_rate)
        else:
            liquidation_price = entry_price * (Decimal("1") + maintenance_rate)
            
        # Store in account
        account.liquidation_price[position_id] = liquidation_price
        
        return liquidation_price
        
    async def process_margin_call(self, user_id: str) -> Dict[str, Any]:
        """Process margin call for user"""
        
        account = await self.get_or_create_account(user_id)
        
        # Check if already in margin call
        if account.last_margin_call and \
           datetime.utcnow() - account.last_margin_call < timedelta(hours=1):
            return {
                "success": False,
                "error": "Margin call already active",
                "time_remaining": str(timedelta(hours=1) - (datetime.utcnow() - account.last_margin_call))
            }
            
        # Calculate required additional margin
        shortfall = account.used_margin - account.total_collateral_value
        
        if shortfall <= 0:
            return {
                "success": False,
                "error": "No margin call needed"
            }
            
        # Set margin call
        account.last_margin_call = datetime.utcnow()
        account.is_liquidatable = True
        
        # Emit margin call event
        await self._emit_margin_event("margin_call", user_id, {
            "shortfall": str(shortfall),
            "deadline": (datetime.utcnow() + timedelta(hours=1)).isoformat(),
            "positions_at_risk": len(account.positions)
        })
        
        return {
            "success": True,
            "margin_call": {
                "shortfall": str(shortfall),
                "deadline": (datetime.utcnow() + timedelta(hours=1)).isoformat(),
                "action_required": "Add collateral or close positions",
                "liquidation_warning": "Positions will be liquidated if margin not restored"
            }
        }
        
    async def _calculate_stress_margin(
        self,
        position_type: str,
        position_value: Decimal,
        underlying_price: Decimal,
        volatility: Optional[Decimal]
    ) -> Decimal:
        """Calculate stress test margin"""
        
        max_loss = Decimal("0")
        
        for scenario in self.stress_scenarios:
            # Calculate potential loss
            price_move = underlying_price * scenario["spot_move"]
            
            if volatility:
                vol_impact = position_value * scenario["vol_move"] * volatility * Decimal("0.1")
            else:
                vol_impact = Decimal("0")
                
            scenario_loss = abs(price_move) + vol_impact
            max_loss = max(max_loss, scenario_loss)
            
        return max_loss
        
    async def _update_account_metrics(self, account: MarginAccount):
        """Update account metrics"""
        
        # Calculate total collateral value
        account.total_collateral_value = sum(c.margin_value for c in account.collateral)
        
        # Calculate used margin from positions
        used_margin = Decimal("0")
        for position in account.positions.values():
            margin_req = await self.calculate_margin_requirement(
                account.user_id,
                position.get("type", "futures"),
                Decimal(position.get("value", "0")),
                Decimal(position.get("underlying_price", "0"))
            )
            used_margin += margin_req.total_required
            
        account.used_margin = used_margin
        account.available_margin = max(Decimal("0"), account.total_collateral_value - used_margin)
        
        # Calculate margin ratio
        if account.total_collateral_value > 0:
            account.margin_ratio = used_margin / account.total_collateral_value
        else:
            account.margin_ratio = Decimal("0")
            
        # Update liquidation status
        account.is_liquidatable = account.margin_ratio > Decimal("0.8")  # 80% threshold
        
        account.updated_at = datetime.utcnow()
        
    async def _get_asset_value(
        self,
        asset_type: CollateralType,
        asset_id: str,
        amount: Decimal
    ) -> Decimal:
        """Get USD value of an asset"""
        
        if asset_type == CollateralType.CASH:
            return amount
            
        elif asset_type == CollateralType.STABLECOIN:
            # Assume 1:1 with USD
            return amount
            
        elif asset_type == CollateralType.COMPUTE_TOKEN:
            # Get from oracle
            if self.oracle:
                price = await self.oracle.get_price("COMPUTE/USD")
                return amount * Decimal(str(price))
            return amount * Decimal("10")  # Default
            
        elif asset_type == CollateralType.DIGITAL_ASSET:
            # Get from digital asset service
            # For now, use a default valuation
            return amount * Decimal("1000")
            
        elif asset_type == CollateralType.ML_MODEL:
            # Complex valuation based on performance metrics
            # For now, use a default
            return Decimal("5000")
            
        elif asset_type == CollateralType.DATASET:
            # Value based on size, quality, uniqueness
            return amount * Decimal("100")
            
        elif asset_type == CollateralType.COMPUTE_VOUCHER:
            # Value based on compute hours
            return amount * Decimal("50")  # $50 per compute hour
            
        elif asset_type == CollateralType.LP_TOKEN:
            # Get from AMM
            return amount * Decimal("100")
            
        return Decimal("0")
        
    async def _get_trust_score(self, user_id: str) -> Decimal:
        """Get user's trust score from graph intelligence"""
        
        if self.graph_client:
            try:
                trust_data = await self.graph_client.get_trust_score(user_id)
                return Decimal(str(trust_data.get("overall_score", "0.5")))
            except:
                pass
                
        return Decimal("0.5")  # Default medium trust
        
    def _get_trust_discount(self, trust_score: Decimal) -> Decimal:
        """Get margin discount based on trust score"""
        
        for tier in self.trust_tiers:
            if trust_score >= tier["min_score"]:
                return tier["margin_discount"]
                
        return Decimal("0")
        
    def _calculate_unsecured_limit(self, trust_score: Decimal) -> Decimal:
        """Calculate unsecured credit limit based on trust"""
        
        for tier in self.trust_tiers:
            if trust_score >= tier["min_score"]:
                return tier["unsecured_limit"]
                
        return Decimal("0")
        
    async def _monitor_margins(self):
        """Background task to monitor margin levels"""
        
        while self._running:
            try:
                # Check all accounts
                for account in self.accounts.values():
                    await self._update_account_metrics(account)
                    
                    # Check for margin calls
                    if account.margin_ratio > Decimal("0.75") and not account.is_liquidatable:
                        await self.process_margin_call(account.user_id)
                        
                    # Check for liquidations
                    elif account.is_liquidatable and account.margin_ratio > Decimal("0.9"):
                        await self._emit_margin_event("liquidation_triggered", account.user_id, {
                            "margin_ratio": str(account.margin_ratio),
                            "positions": list(account.positions.keys())
                        })
                        
                # Save to cache
                for account in self.accounts.values():
                    await self.cache.set(f"margin_account:{account.account_id}", account)
                    
                await asyncio.sleep(10)  # Check every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in margin monitoring: {e}")
                await asyncio.sleep(30)
                
    async def _emit_margin_event(self, event_type: str, user_id: str, data: Dict):
        """Emit margin event to Pulsar"""
        
        if self.pulsar:
            await self.pulsar.publish(f"margin.{event_type}", {
                "event_type": event_type,
                "user_id": user_id,
                "timestamp": datetime.utcnow().isoformat(),
                "data": data
            })
            
    def get_margin_summary(self) -> Dict[str, Any]:
        """Get overall margin system summary"""
        
        total_collateral = sum(a.total_collateral_value for a in self.accounts.values())
        total_used = sum(a.used_margin for a in self.accounts.values())
        at_risk_accounts = sum(1 for a in self.accounts.values() if a.margin_ratio > Decimal("0.7"))
        
        return {
            "total_accounts": len(self.accounts),
            "total_collateral_value": str(total_collateral),
            "total_margin_used": str(total_used),
            "utilization_rate": str(total_used / total_collateral if total_collateral > 0 else 0),
            "at_risk_accounts": at_risk_accounts,
            "liquidatable_accounts": sum(1 for a in self.accounts.values() if a.is_liquidatable),
            "collateral_breakdown": self._get_collateral_breakdown()
        }
        
    def _get_collateral_breakdown(self) -> Dict[str, str]:
        """Get breakdown of collateral by type"""
        
        breakdown = defaultdict(Decimal)
        
        for account in self.accounts.values():
            for collateral in account.collateral:
                breakdown[collateral.asset_type.value] += collateral.margin_value
                
        return {k: str(v) for k, v in breakdown.items()} 