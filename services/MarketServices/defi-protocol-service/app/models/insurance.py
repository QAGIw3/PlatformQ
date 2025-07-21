"""
Insurance data models for DeFi protocol service.
"""

from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any


class RiskTier(Enum):
    """Insurance pool risk tiers"""
    STABLE = "stable"      # Low risk, low reward
    BALANCED = "balanced"  # Medium risk, medium reward  
    AGGRESSIVE = "aggressive"  # High risk, high reward


class ClaimStatus(Enum):
    """Insurance claim status"""
    PENDING = "pending"
    APPROVED = "approved"
    PAID = "paid"
    REJECTED = "rejected"
    EXPIRED = "expired"


@dataclass
class PoolTier:
    """Represents an insurance pool tier configuration"""
    tier: RiskTier
    name: str
    supported_markets: List[str]
    max_leverage_covered: int
    base_apy: Decimal
    risk_multiplier: Decimal
    loss_priority: int
    min_stake: Decimal
    coverage_ratio: Decimal
    total_staked: Decimal = Decimal("0")
    total_coverage_provided: Decimal = Decimal("0")
    active_claims: int = 0


@dataclass
class StakePosition:
    """Represents a user's stake in an insurance pool"""
    id: str
    user_id: str
    chain: str
    tier: RiskTier
    amount: Decimal
    staked_at: datetime
    lock_until: Optional[datetime]
    base_apy: Decimal
    lock_bonus: Decimal
    rewards_earned: Decimal
    last_reward_claim: datetime
    is_active: bool = True
    losses_absorbed: Decimal = Decimal("0")
    
    @property
    def is_locked(self) -> bool:
        """Check if position is still locked"""
        if not self.lock_until:
            return False
        return datetime.utcnow() < self.lock_until
    
    @property
    def effective_apy(self) -> Decimal:
        """Get total APY including bonuses"""
        return self.base_apy + self.lock_bonus
    
    @property
    def current_value(self) -> Decimal:
        """Get current value including rewards"""
        return self.amount + self.rewards_earned


@dataclass
class InsuranceClaim:
    """Represents an insurance claim"""
    id: str
    chain: str
    claimant: str
    claim_type: str  # "liquidation", "hack", "impermanent_loss"
    reference_id: str  # loan_id, pool_id, etc.
    amount_claimed: Decimal
    amount_approved: Optional[Decimal]
    status: ClaimStatus
    submitted_at: datetime
    processed_at: Optional[datetime]
    evidence: Dict[str, Any] = field(default_factory=dict)
    processors: List[str] = field(default_factory=list)  # Tiers that processed
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_pending(self) -> bool:
        """Check if claim is still pending"""
        return self.status == ClaimStatus.PENDING
    
    @property
    def processing_time(self) -> Optional[int]:
        """Get processing time in seconds"""
        if not self.processed_at:
            return None
        return int((self.processed_at - self.submitted_at).total_seconds())


@dataclass
class DeficitEvent:
    """Represents a deficit event when pools can't cover losses"""
    id: str
    chain: str
    reference_id: str  # claim_id or loan_id
    total_loss: Decimal
    amount_covered: Decimal
    deficit_amount: Decimal
    occurred_at: datetime
    partial_coverage: Dict[RiskTier, Decimal] = field(default_factory=dict)
    recovery_actions: List[Dict[str, Any]] = field(default_factory=list)
    is_resolved: bool = False
    resolved_at: Optional[datetime] = None
    
    @property
    def coverage_ratio(self) -> Decimal:
        """Get the ratio of loss that was covered"""
        if self.total_loss == 0:
            return Decimal("0")
        return self.amount_covered / self.total_loss
    
    @property
    def deficit_ratio(self) -> Decimal:
        """Get the ratio of loss that was NOT covered"""
        return Decimal("1") - self.coverage_ratio


@dataclass
class InsuranceMetrics:
    """Aggregate metrics for insurance pools"""
    chain: str
    timestamp: datetime
    total_value_locked: Decimal
    total_claims_paid: Decimal
    active_coverage: Decimal
    utilization_rate: Decimal
    average_apy: Decimal
    claims_last_24h: int
    deficits_last_30d: int
    health_score: Decimal  # 0-1 score of pool health 