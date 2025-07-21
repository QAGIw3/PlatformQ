from enum import Enum
from typing import Optional, Dict, Any, List
from decimal import Decimal
from datetime import datetime
from dataclasses import dataclass, field
import uuid


class MarketType(Enum):
    """Market types"""
    PERPETUAL = "perpetual"
    FUTURES = "futures"
    OPTIONS = "options"
    BINARY_OPTIONS = "binary_options"
    SWAPS = "swaps"
    STRUCTURED = "structured"
    INDEX = "index"
    SYNTHETIC = "synthetic"


class MarketStatus(Enum):
    """Market status"""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    SETTLING = "settling"
    EXPIRED = "expired"
    DELISTED = "delisted"


class SettlementType(Enum):
    """Settlement type"""
    CASH = "cash"
    PHYSICAL = "physical"
    HYBRID = "hybrid"


@dataclass
class Market:
    """
    Derivative market representation
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str = ""  # e.g., "BTC-PERP", "ETH-CALL-3000-DEC24"
    name: str = ""
    market_type: MarketType = MarketType.PERPETUAL
    status: MarketStatus = MarketStatus.ACTIVE
    
    # Underlying asset
    underlying_asset: str = ""  # Asset ID or symbol
    quote_asset: str = "USDC"  # Settlement currency
    settlement_type: SettlementType = SettlementType.CASH
    
    # Contract specifications
    contract_size: Decimal = Decimal("1")  # Size of one contract
    tick_size: Decimal = Decimal("0.01")  # Minimum price increment
    lot_size: Decimal = Decimal("0.001")  # Minimum order size
    max_order_size: Optional[Decimal] = None
    max_position_size: Optional[Decimal] = None
    
    # Margin requirements
    initial_margin_rate: Decimal = Decimal("0.05")  # 5%
    maintenance_margin_rate: Decimal = Decimal("0.025")  # 2.5%
    max_leverage: Decimal = Decimal("20")
    
    # Funding (for perpetuals)
    funding_interval: Optional[int] = 28800  # 8 hours in seconds
    funding_rate_cap: Optional[Decimal] = Decimal("0.003")  # 0.3% per interval
    last_funding_time: Optional[datetime] = None
    current_funding_rate: Optional[Decimal] = None
    
    # Options specific
    strike_price: Optional[Decimal] = None
    expiry_time: Optional[datetime] = None
    option_type: Optional[str] = None  # "call" or "put"
    is_european: bool = True  # European vs American style
    
    # Price data
    mark_price: Decimal = Decimal("0")
    index_price: Decimal = Decimal("0")
    last_trade_price: Decimal = Decimal("0")
    best_bid: Optional[Decimal] = None
    best_ask: Optional[Decimal] = None
    
    # Volume and OI
    volume_24h: Decimal = Decimal("0")
    volume_usd_24h: Decimal = Decimal("0")
    open_interest: Decimal = Decimal("0")
    open_interest_usd: Decimal = Decimal("0")
    
    # Fee structure
    maker_fee_rate: Decimal = Decimal("0.0002")  # 0.02%
    taker_fee_rate: Decimal = Decimal("0.0005")  # 0.05%
    
    # Oracle configuration
    price_oracles: List[str] = field(default_factory=list)  # Oracle IDs
    oracle_aggregation_method: str = "median"  # median, mean, twap
    
    # Risk parameters
    insurance_fund_contribution: Decimal = Decimal("0.001")  # 0.1% of fees
    adl_enabled: bool = True  # Auto-deleveraging
    position_limit_enabled: bool = True
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    listed_at: Optional[datetime] = None
    delisted_at: Optional[datetime] = None
    
    # Metadata
    tags: List[str] = field(default_factory=list)  # e.g., ["defi", "layer2"]
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_margin_requirement(self, position_size: Decimal, is_initial: bool = True) -> Decimal:
        """Calculate margin requirement for a position size"""
        notional = abs(position_size * self.mark_price)
        rate = self.initial_margin_rate if is_initial else self.maintenance_margin_rate
        return notional * rate
        
    def get_liquidation_price(
        self, 
        position_size: Decimal, 
        entry_price: Decimal,
        collateral: Decimal,
        is_long: bool
    ) -> Optional[Decimal]:
        """Calculate liquidation price for a position"""
        if position_size == 0:
            return None
            
        # Basic liquidation price calculation
        # LP = Entry - (Collateral - Fees) / Size  (for long)
        # LP = Entry + (Collateral - Fees) / Size  (for short)
        
        maintenance_margin = self.maintenance_margin_rate
        fees = self.taker_fee_rate * 2  # Assume liquidation + closing fees
        
        if is_long:
            liq_price = entry_price * (1 - (collateral / (position_size * entry_price)) + maintenance_margin + fees)
        else:
            liq_price = entry_price * (1 + (collateral / (position_size * entry_price)) - maintenance_margin - fees)
            
        return max(liq_price, Decimal("0"))
        
    def calculate_funding_payment(
        self,
        position_size: Decimal,
        funding_rate: Decimal,
        is_long: bool
    ) -> Decimal:
        """Calculate funding payment for a position"""
        # Longs pay shorts when funding is positive
        payment = position_size * self.mark_price * funding_rate
        return -payment if is_long else payment
        
    def validate_order_size(self, size: Decimal) -> bool:
        """Check if order size is valid"""
        if size < self.lot_size:
            return False
        if self.max_order_size and size > self.max_order_size:
            return False
        # Check if size is multiple of lot size
        return (size % self.lot_size) == 0
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "name": self.name,
            "market_type": self.market_type.value,
            "status": self.status.value,
            "underlying_asset": self.underlying_asset,
            "quote_asset": self.quote_asset,
            "settlement_type": self.settlement_type.value,
            "contract_size": str(self.contract_size),
            "tick_size": str(self.tick_size),
            "lot_size": str(self.lot_size),
            "max_order_size": str(self.max_order_size) if self.max_order_size else None,
            "max_position_size": str(self.max_position_size) if self.max_position_size else None,
            "initial_margin_rate": str(self.initial_margin_rate),
            "maintenance_margin_rate": str(self.maintenance_margin_rate),
            "max_leverage": str(self.max_leverage),
            "funding_interval": self.funding_interval,
            "funding_rate_cap": str(self.funding_rate_cap) if self.funding_rate_cap else None,
            "last_funding_time": self.last_funding_time.isoformat() if self.last_funding_time else None,
            "current_funding_rate": str(self.current_funding_rate) if self.current_funding_rate else None,
            "strike_price": str(self.strike_price) if self.strike_price else None,
            "expiry_time": self.expiry_time.isoformat() if self.expiry_time else None,
            "option_type": self.option_type,
            "is_european": self.is_european,
            "mark_price": str(self.mark_price),
            "index_price": str(self.index_price),
            "last_trade_price": str(self.last_trade_price),
            "best_bid": str(self.best_bid) if self.best_bid else None,
            "best_ask": str(self.best_ask) if self.best_ask else None,
            "volume_24h": str(self.volume_24h),
            "volume_usd_24h": str(self.volume_usd_24h),
            "open_interest": str(self.open_interest),
            "open_interest_usd": str(self.open_interest_usd),
            "maker_fee_rate": str(self.maker_fee_rate),
            "taker_fee_rate": str(self.taker_fee_rate),
            "price_oracles": self.price_oracles,
            "oracle_aggregation_method": self.oracle_aggregation_method,
            "insurance_fund_contribution": str(self.insurance_fund_contribution),
            "adl_enabled": self.adl_enabled,
            "position_limit_enabled": self.position_limit_enabled,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "listed_at": self.listed_at.isoformat() if self.listed_at else None,
            "delisted_at": self.delisted_at.isoformat() if self.delisted_at else None,
            "tags": self.tags,
            "metadata": self.metadata
        } 