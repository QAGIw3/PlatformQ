"""Social trading models."""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional, Dict, List, Any
from pydantic import BaseModel, Field


class TraderStatus(str, Enum):
    """Trader account status."""
    ACTIVE = "active"
    SUSPENDED = "suspended"
    RESTRICTED = "restricted"
    VERIFIED = "verified"


class StrategyStatus(str, Enum):
    """Trading strategy status."""
    ACTIVE = "active"
    PAUSED = "paused"
    DEPRECATED = "deprecated"
    BACKTESTING = "backtesting"


class CopyMode(str, Enum):
    """Copy trading modes."""
    PROPORTIONAL = "proportional"  # Copy proportional to leader's position
    FIXED_AMOUNT = "fixed_amount"  # Fixed amount per trade
    PERCENTAGE = "percentage"  # Percentage of follower's portfolio


class TraderProfile(BaseModel):
    """Trader profile with social features."""
    user_id: str
    username: str
    display_name: str
    bio: Optional[str] = None
    avatar_url: Optional[str] = None
    status: TraderStatus = TraderStatus.ACTIVE
    
    # Trading stats
    total_trades: int = 0
    win_rate: float = 0.0
    total_pnl: Decimal = Decimal("0")
    sharpe_ratio: Optional[float] = None
    max_drawdown: Optional[float] = None
    
    # Social stats
    followers_count: int = 0
    following_count: int = 0
    reputation_score: float = 0.0
    is_verified: bool = False
    
    # Copy trading
    allows_copy_trading: bool = False
    copy_trading_fee: float = 0.0  # Percentage fee
    min_copy_amount: Decimal = Decimal("100")
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class TradingStrategy(BaseModel):
    """Trading strategy that can be shared or sold."""
    strategy_id: str
    owner_id: str
    name: str
    description: str
    category: str  # momentum, mean_reversion, arbitrage, etc.
    
    # Performance metrics
    backtest_results: Optional[Dict[str, Any]] = None
    live_performance: Optional[Dict[str, Any]] = None
    risk_metrics: Optional[Dict[str, Any]] = None
    
    # Strategy details
    assets: List[str]
    timeframe: str
    indicators: List[str]
    entry_rules: Optional[str] = None
    exit_rules: Optional[str] = None
    risk_management: Optional[str] = None
    
    # NFT details
    is_nft: bool = False
    nft_token_id: Optional[str] = None
    nft_price: Optional[Decimal] = None
    
    # Access control
    is_public: bool = True
    subscriber_count: int = 0
    subscription_fee: Optional[Decimal] = None
    
    status: StrategyStatus = StrategyStatus.ACTIVE
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class CopyTradingRelation(BaseModel):
    """Copy trading relationship between leader and follower."""
    relation_id: str
    leader_id: str
    follower_id: str
    
    # Copy settings
    copy_mode: CopyMode
    allocation_amount: Optional[Decimal] = None  # For fixed amount mode
    allocation_percent: Optional[float] = None  # For percentage mode
    
    # Risk controls
    max_position_size: Optional[Decimal] = None
    max_daily_trades: Optional[int] = None
    stop_loss_percent: Optional[float] = None
    max_drawdown_percent: Optional[float] = None
    
    # Performance tracking
    total_copied_trades: int = 0
    successful_trades: int = 0
    total_pnl: Decimal = Decimal("0")
    fees_paid: Decimal = Decimal("0")
    
    # Status
    is_active: bool = True
    paused_reason: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        use_enum_values = True


class SocialPost(BaseModel):
    """Social media post in trading community."""
    post_id: str
    author_id: str
    content: str
    
    # Post metadata
    tags: List[str] = []
    mentions: List[str] = []
    assets_mentioned: List[str] = []
    
    # Attached content
    strategy_id: Optional[str] = None
    trade_id: Optional[str] = None
    chart_url: Optional[str] = None
    
    # Engagement
    likes_count: int = 0
    comments_count: int = 0
    shares_count: int = 0
    
    # Visibility
    is_public: bool = True
    is_educational: bool = False
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    edited_at: Optional[datetime] = None


class TraderReputation(BaseModel):
    """Trader reputation metrics."""
    user_id: str
    
    # Performance metrics (40% weight)
    performance_score: float = 0.0
    consistency_score: float = 0.0
    risk_management_score: float = 0.0
    
    # Social metrics (30% weight)
    follower_satisfaction: float = 0.0
    community_engagement: float = 0.0
    content_quality: float = 0.0
    
    # Trust metrics (30% weight)
    transparency_score: float = 0.0
    reliability_score: float = 0.0
    dispute_resolution: float = 0.0
    
    # Overall reputation
    overall_score: float = 0.0
    rank: Optional[int] = None
    percentile: Optional[float] = None
    
    # History
    score_history: List[Dict[str, Any]] = []
    last_calculated: datetime = Field(default_factory=datetime.utcnow)


class PerformanceMetrics(BaseModel):
    """Detailed performance metrics for a trader."""
    user_id: str
    period: str  # daily, weekly, monthly, yearly, all-time
    
    # Returns
    total_return: Decimal
    average_daily_return: Decimal
    volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    
    # Risk metrics
    max_drawdown: float
    max_drawdown_duration: int  # days
    value_at_risk: float  # 95% VaR
    beta: Optional[float] = None
    
    # Trading activity
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    average_win: Decimal
    average_loss: Decimal
    profit_factor: float
    
    # Position metrics
    average_position_size: Decimal
    average_holding_period: float  # hours
    long_short_ratio: float
    
    # Asset allocation
    asset_distribution: Dict[str, float]
    sector_distribution: Optional[Dict[str, float]] = None
    
    calculated_at: datetime = Field(default_factory=datetime.utcnow)


class LeaderboardEntry(BaseModel):
    """Entry in trader leaderboard."""
    rank: int
    user_id: str
    username: str
    display_name: str
    
    # Key metrics
    total_return: Decimal
    sharpe_ratio: float
    win_rate: float
    reputation_score: float
    
    # Social metrics
    followers_count: int
    copy_traders_count: int
    
    # Badges/achievements
    badges: List[str] = []
    is_verified: bool = False
    
    # Period
    period: str  # daily, weekly, monthly, all-time 