from enum import Enum
from typing import Optional, Dict, Any, List
from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass, field
import uuid


class PositionSide(Enum):
    """Position side"""
    LONG = "long"
    SHORT = "short"


class PositionStatus(Enum):
    """Position status"""
    OPEN = "open"
    CLOSED = "closed"
    LIQUIDATED = "liquidated"
    ADL = "adl"  # Auto-deleveraged


@dataclass
class Position:
    """
    Derivative position representation
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str = ""
    market_id: str = ""
    side: PositionSide = PositionSide.LONG
    status: PositionStatus = PositionStatus.OPEN
    
    # Position sizing
    size: Decimal = Decimal("0")
    entry_price: Decimal = Decimal("0")
    mark_price: Decimal = Decimal("0")
    liquidation_price: Optional[Decimal] = None
    
    # Margin and collateral
    initial_margin: Decimal = Decimal("0")
    maintenance_margin: Decimal = Decimal("0")
    margin_ratio: Decimal = Decimal("0")  # Current margin / maintenance margin
    collateral: Decimal = Decimal("0")
    collateral_asset: str = "USDC"  # Default collateral asset
    
    # PnL tracking
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    funding_paid: Decimal = Decimal("0")
    fees_paid: Decimal = Decimal("0")
    
    # Risk metrics
    leverage: Decimal = Decimal("1")
    notional_value: Decimal = Decimal("0")
    bankruptcy_price: Optional[Decimal] = None
    adl_ranking: Optional[int] = None  # Auto-deleverage ranking
    
    # Timestamps
    opened_at: datetime = field(default_factory=datetime.utcnow)
    closed_at: Optional[datetime] = None
    last_funding_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    # Cross-margin settings
    cross_margin_enabled: bool = False
    isolated_margin: bool = True
    margin_mode: str = "isolated"  # isolated or cross
    
    # Advanced features
    take_profit_price: Optional[Decimal] = None
    stop_loss_price: Optional[Decimal] = None
    trailing_stop_distance: Optional[Decimal] = None
    reduce_only: bool = False
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Calculate derived fields"""
        self.update_calculations()
        
    def update_calculations(self):
        """Update calculated fields"""
        # Calculate notional value
        self.notional_value = abs(self.size * self.mark_price)
        
        # Calculate unrealized PnL
        if self.side == PositionSide.LONG:
            self.unrealized_pnl = (self.mark_price - self.entry_price) * self.size
        else:
            self.unrealized_pnl = (self.entry_price - self.mark_price) * self.size
            
        # Calculate leverage
        if self.collateral > 0:
            self.leverage = self.notional_value / self.collateral
        
        # Calculate margin ratio
        if self.maintenance_margin > 0:
            available_margin = self.collateral + self.unrealized_pnl
            self.margin_ratio = available_margin / self.maintenance_margin
            
        self.updated_at = datetime.utcnow()
        
    def update_mark_price(self, new_mark_price: Decimal):
        """Update position with new mark price"""
        self.mark_price = new_mark_price
        self.update_calculations()
        
    def add_collateral(self, amount: Decimal):
        """Add collateral to position"""
        self.collateral += amount
        self.update_calculations()
        
    def remove_collateral(self, amount: Decimal) -> bool:
        """Remove collateral from position if margin allows"""
        new_collateral = self.collateral - amount
        # Check if removal would cause liquidation
        test_margin_ratio = (new_collateral + self.unrealized_pnl) / self.maintenance_margin
        if test_margin_ratio >= Decimal("1.1"):  # 10% buffer
            self.collateral = new_collateral
            self.update_calculations()
            return True
        return False
        
    def apply_funding(self, funding_rate: Decimal, funding_amount: Decimal):
        """Apply funding payment to position"""
        self.funding_paid += funding_amount
        self.realized_pnl -= funding_amount  # Funding is deducted from PnL
        self.last_funding_at = datetime.utcnow()
        self.update_calculations()
        
    def close_position(self, exit_price: Decimal, fees: Decimal):
        """Close the position"""
        # Calculate final PnL
        if self.side == PositionSide.LONG:
            pnl = (exit_price - self.entry_price) * self.size
        else:
            pnl = (self.entry_price - exit_price) * self.size
            
        self.realized_pnl += pnl - fees
        self.fees_paid += fees
        self.status = PositionStatus.CLOSED
        self.closed_at = datetime.utcnow()
        self.size = Decimal("0")
        self.update_calculations()
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "user_id": self.user_id,
            "market_id": self.market_id,
            "side": self.side.value,
            "status": self.status.value,
            "size": str(self.size),
            "entry_price": str(self.entry_price),
            "mark_price": str(self.mark_price),
            "liquidation_price": str(self.liquidation_price) if self.liquidation_price else None,
            "initial_margin": str(self.initial_margin),
            "maintenance_margin": str(self.maintenance_margin),
            "margin_ratio": str(self.margin_ratio),
            "collateral": str(self.collateral),
            "collateral_asset": self.collateral_asset,
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "funding_paid": str(self.funding_paid),
            "fees_paid": str(self.fees_paid),
            "leverage": str(self.leverage),
            "notional_value": str(self.notional_value),
            "bankruptcy_price": str(self.bankruptcy_price) if self.bankruptcy_price else None,
            "adl_ranking": self.adl_ranking,
            "opened_at": self.opened_at.isoformat(),
            "closed_at": self.closed_at.isoformat() if self.closed_at else None,
            "last_funding_at": self.last_funding_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "margin_mode": self.margin_mode,
            "take_profit_price": str(self.take_profit_price) if self.take_profit_price else None,
            "stop_loss_price": str(self.stop_loss_price) if self.stop_loss_price else None,
            "metadata": self.metadata
        }


class LiquidationType(Enum):
    """Type of liquidation"""
    PARTIAL = "partial"
    FULL = "full"
    ADL = "adl"  # Auto-deleverage


@dataclass
class LiquidationEvent:
    """
    Liquidation event details
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    position_id: str = ""
    user_id: str = ""
    market_id: str = ""
    liquidation_type: LiquidationType = LiquidationType.FULL
    
    # Liquidation details
    liquidated_size: Decimal = Decimal("0")
    liquidation_price: Decimal = Decimal("0")
    mark_price: Decimal = Decimal("0")
    bankruptcy_price: Decimal = Decimal("0")
    
    # Financial impact
    collateral_liquidated: Decimal = Decimal("0")
    liquidation_fee: Decimal = Decimal("0")
    insurance_fund_contribution: Decimal = Decimal("0")
    socialized_loss: Decimal = Decimal("0")  # If insurance fund depleted
    
    # Liquidator details
    liquidator_id: Optional[str] = None
    liquidator_reward: Decimal = Decimal("0")
    
    # Timestamps
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Metadata
    trigger_reason: str = ""  # margin_call, adl, manual
    margin_ratio_at_liquidation: Decimal = Decimal("0")
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "position_id": self.position_id,
            "user_id": self.user_id,
            "market_id": self.market_id,
            "liquidation_type": self.liquidation_type.value,
            "liquidated_size": str(self.liquidated_size),
            "liquidation_price": str(self.liquidation_price),
            "mark_price": str(self.mark_price),
            "bankruptcy_price": str(self.bankruptcy_price),
            "collateral_liquidated": str(self.collateral_liquidated),
            "liquidation_fee": str(self.liquidation_fee),
            "insurance_fund_contribution": str(self.insurance_fund_contribution),
            "socialized_loss": str(self.socialized_loss),
            "liquidator_id": self.liquidator_id,
            "liquidator_reward": str(self.liquidator_reward),
            "timestamp": self.timestamp.isoformat(),
            "trigger_reason": self.trigger_reason,
            "margin_ratio_at_liquidation": str(self.margin_ratio_at_liquidation),
            "metadata": self.metadata
        } 