from enum import Enum
from typing import Optional, Dict, Any, List
from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass, field
import uuid


class CollateralType(Enum):
    """Collateral asset types"""
    STABLECOIN = "stablecoin"
    CRYPTO = "crypto"
    PLATFORM_TOKEN = "platform_token"
    LP_TOKEN = "lp_token"
    SYNTHETIC = "synthetic"
    NFT = "nft"
    REAL_WORLD_ASSET = "rwa"


@dataclass
class CollateralTier:
    """
    Collateral tier configuration with risk parameters
    """
    tier_name: str = ""
    tier_level: int = 1  # 1 = highest quality, higher = riskier
    
    # Supported assets
    asset_types: List[CollateralType] = field(default_factory=list)
    specific_assets: List[str] = field(default_factory=list)  # Asset IDs/symbols
    
    # Risk parameters
    ltv_ratio: Decimal = Decimal("0.85")  # Loan-to-Value ratio
    liquidation_threshold: Decimal = Decimal("0.90")  # When liquidation starts
    liquidation_penalty: Decimal = Decimal("0.05")  # 5% penalty
    
    # Capital efficiency
    margin_multiplier: Decimal = Decimal("1.0")  # How much margin this provides
    cross_margin_eligible: bool = True
    
    # Interest rates
    borrow_rate_base: Decimal = Decimal("0.02")  # 2% APR base
    supply_rate_base: Decimal = Decimal("0.015")  # 1.5% APR base
    
    # Limits
    max_collateral_amount: Optional[Decimal] = None  # Per user
    global_cap: Optional[Decimal] = None  # Total platform limit
    
    # Price parameters
    price_source: str = "oracle"  # oracle, amm, fixed
    price_deviation_threshold: Decimal = Decimal("0.05")  # 5% max deviation
    
    # Special features
    auto_compound: bool = False  # Auto-compound rewards
    yield_bearing: bool = False  # Earns yield while collateralized
    
    def calculate_borrowing_power(self, collateral_value: Decimal) -> Decimal:
        """Calculate how much can be borrowed against collateral"""
        return collateral_value * self.ltv_ratio * self.margin_multiplier
        
    def is_liquidatable(self, collateral_value: Decimal, debt_value: Decimal) -> bool:
        """Check if position should be liquidated"""
        if collateral_value == 0:
            return debt_value > 0
        health_factor = collateral_value / debt_value
        return health_factor < self.liquidation_threshold


@dataclass
class UserCollateral:
    """
    User's collateral position
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str = ""
    
    # Collateral details
    asset_id: str = ""  # Asset identifier
    asset_symbol: str = ""
    amount: Decimal = Decimal("0")
    
    # Valuation
    current_price: Decimal = Decimal("0")
    collateral_value_usd: Decimal = Decimal("0")
    last_price_update: datetime = field(default_factory=datetime.utcnow)
    
    # Tier assignment
    collateral_tier: Optional[CollateralTier] = None
    tier_name: str = ""
    
    # Usage
    locked_amount: Decimal = Decimal("0")  # Used as collateral
    available_amount: Decimal = Decimal("0")  # Can be withdrawn
    
    # Borrowing
    borrowed_value_usd: Decimal = Decimal("0")
    health_factor: Decimal = Decimal("0")  # collateral_value / borrowed_value
    liquidation_price: Optional[Decimal] = None
    
    # Cross-margin
    is_cross_margin: bool = False
    allocated_to_positions: Dict[str, Decimal] = field(default_factory=dict)  # position_id -> amount
    
    # Yield tracking
    accumulated_yield: Decimal = Decimal("0")
    last_yield_update: datetime = field(default_factory=datetime.utcnow)
    
    # Status
    is_active: bool = True
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Metadata
    deposit_tx_hash: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Initialize calculated fields"""
        self.update_calculations()
        
    def update_calculations(self):
        """Update calculated fields"""
        # Update value
        self.collateral_value_usd = self.amount * self.current_price
        
        # Update available amount
        self.available_amount = self.amount - self.locked_amount
        
        # Calculate health factor
        if self.borrowed_value_usd > 0:
            self.health_factor = self.collateral_value_usd / self.borrowed_value_usd
        else:
            self.health_factor = Decimal("999")  # Infinite (no debt)
            
        # Calculate liquidation price
        if self.collateral_tier and self.amount > 0 and self.borrowed_value_usd > 0:
            # Price at which health factor = liquidation threshold
            self.liquidation_price = (
                self.borrowed_value_usd * self.collateral_tier.liquidation_threshold
            ) / self.amount
        else:
            self.liquidation_price = None
            
        self.updated_at = datetime.utcnow()
        
    def deposit(self, amount: Decimal):
        """Add collateral"""
        self.amount += amount
        self.update_calculations()
        
    def withdraw(self, amount: Decimal) -> bool:
        """Withdraw collateral if possible"""
        if amount > self.available_amount:
            return False
            
        # Check if withdrawal would cause liquidation
        new_amount = self.amount - amount
        new_value = new_amount * self.current_price
        
        if self.collateral_tier and self.borrowed_value_usd > 0:
            new_health = new_value / self.borrowed_value_usd
            if new_health < self.collateral_tier.liquidation_threshold * Decimal("1.1"):  # 10% buffer
                return False
                
        self.amount = new_amount
        self.update_calculations()
        return True
        
    def lock_for_position(self, position_id: str, amount: Decimal) -> bool:
        """Lock collateral for a position"""
        if amount > self.available_amount:
            return False
            
        self.locked_amount += amount
        self.allocated_to_positions[position_id] = (
            self.allocated_to_positions.get(position_id, Decimal("0")) + amount
        )
        self.update_calculations()
        return True
        
    def unlock_from_position(self, position_id: str, amount: Optional[Decimal] = None):
        """Unlock collateral from a position"""
        if position_id not in self.allocated_to_positions:
            return
            
        if amount is None:
            # Unlock all
            amount = self.allocated_to_positions[position_id]
            
        amount = min(amount, self.allocated_to_positions[position_id])
        self.locked_amount -= amount
        self.allocated_to_positions[position_id] -= amount
        
        if self.allocated_to_positions[position_id] == 0:
            del self.allocated_to_positions[position_id]
            
        self.update_calculations()
        
    def update_price(self, new_price: Decimal):
        """Update collateral price"""
        self.current_price = new_price
        self.last_price_update = datetime.utcnow()
        self.update_calculations()
        
    def accrue_yield(self, yield_amount: Decimal):
        """Add yield to collateral"""
        self.accumulated_yield += yield_amount
        self.amount += yield_amount  # Auto-compound
        self.last_yield_update = datetime.utcnow()
        self.update_calculations()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "asset_id": self.asset_id,
            "asset_symbol": self.asset_symbol,
            "amount": str(self.amount),
            "current_price": str(self.current_price),
            "collateral_value_usd": str(self.collateral_value_usd),
            "last_price_update": self.last_price_update.isoformat(),
            "tier_name": self.tier_name,
            "locked_amount": str(self.locked_amount),
            "available_amount": str(self.available_amount),
            "borrowed_value_usd": str(self.borrowed_value_usd),
            "health_factor": str(self.health_factor),
            "liquidation_price": str(self.liquidation_price) if self.liquidation_price else None,
            "is_cross_margin": self.is_cross_margin,
            "allocated_to_positions": {
                k: str(v) for k, v in self.allocated_to_positions.items()
            },
            "accumulated_yield": str(self.accumulated_yield),
            "last_yield_update": self.last_yield_update.isoformat(),
            "is_active": self.is_active,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "deposit_tx_hash": self.deposit_tx_hash,
            "metadata": self.metadata
        } 