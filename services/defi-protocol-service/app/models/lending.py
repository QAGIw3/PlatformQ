"""
Lending data models for DeFi protocol service.
"""

from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any


class LoanStatus(Enum):
    """Loan status states"""
    ACTIVE = "active"
    REPAID = "repaid"
    LIQUIDATED = "liquidated"
    DEFAULTED = "defaulted"
    CANCELLED = "cancelled"


class CollateralType(Enum):
    """Types of collateral accepted"""
    NFT = "nft"
    ERC20 = "erc20"
    MULTI_ASSET = "multi_asset"


@dataclass
class LoanOffer:
    """Represents a loan offer from a lender"""
    id: str
    chain: str
    lender: str
    nft_contract: str  # Accepted NFT contract or "any"
    max_loan_amount: Decimal
    interest_rate: Decimal  # Annual rate as decimal (e.g., 0.15 for 15%)
    min_duration: int  # Seconds
    max_duration: int  # Seconds
    payment_token: str  # Token address for loan payments
    is_active: bool
    created_at: datetime
    accepted_collateral_types: List[str] = field(default_factory=list)
    total_lent: Decimal = Decimal("0")
    active_loans: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Loan:
    """Represents an active or historical loan"""
    id: str
    chain: str
    offer_id: str
    borrower: str
    lender: str
    collateral_contract: str
    collateral_token_id: int
    principal: Decimal
    interest_rate: Decimal
    repayment_amount: Decimal
    start_time: datetime
    due_time: datetime
    status: LoanStatus
    payment_token: str
    repaid_at: Optional[datetime] = None
    liquidated_at: Optional[datetime] = None
    late_fees: Decimal = Decimal("0")
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_overdue(self) -> bool:
        """Check if loan is overdue"""
        return (
            self.status == LoanStatus.ACTIVE and
            datetime.utcnow() > self.due_time
        )
    
    @property
    def days_overdue(self) -> int:
        """Calculate days overdue"""
        if not self.is_overdue:
            return 0
        return (datetime.utcnow() - self.due_time).days
    
    @property
    def health_factor(self) -> float:
        """Calculate loan health factor (placeholder)"""
        # This would normally calculate based on collateral value
        # For now, return 1.0 if active, 0 if overdue
        if self.is_overdue:
            return 0.0
        return 1.0


@dataclass
class LiquidationEvent:
    """Represents a loan liquidation event"""
    loan_id: str
    liquidator: str
    timestamp: datetime
    collateral_value: Decimal
    debt_amount: Decimal
    liquidation_bonus: Decimal
    tx_hash: str 