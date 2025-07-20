from pydantic import BaseModel, Field, validator
from typing import Optional, Dict, Any, List, Union
from datetime import datetime, date
from enum import Enum
import uuid


class TimeInterval(str, Enum):
    """Time intervals for aggregation"""
    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    ONE_HOUR = "1h"
    FOUR_HOURS = "4h"
    ONE_DAY = "1d"
    ONE_WEEK = "1w"
    ONE_MONTH = "1M"


class MetricType(str, Enum):
    """Types of metrics"""
    TRANSACTION_COUNT = "transaction_count"
    TRANSACTION_VOLUME = "transaction_volume"
    GAS_USED = "gas_used"
    GAS_PRICE = "gas_price"
    ACTIVE_ADDRESSES = "active_addresses"
    TOKEN_PRICE = "token_price"
    TOKEN_VOLUME = "token_volume"
    TVL = "tvl"
    NFT_SALES = "nft_sales"
    CUSTOM = "custom"


class DataPoint(BaseModel):
    """Single data point in time series"""
    timestamp: datetime
    value: float
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)


class TimeSeries(BaseModel):
    """Time series data"""
    metric: str
    chain: str
    interval: TimeInterval
    start_time: datetime
    end_time: datetime
    data_points: List[DataPoint]
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    @property
    def point_count(self) -> int:
        return len(self.data_points)
    
    @property
    def average(self) -> Optional[float]:
        if not self.data_points:
            return None
        return sum(dp.value for dp in self.data_points) / len(self.data_points)


class ChainMetrics(BaseModel):
    """Blockchain metrics for a specific chain"""
    chain: str
    timestamp: datetime
    
    # Transaction metrics
    transaction_count: int = Field(default=0)
    transaction_volume: str = Field(default="0")
    gas_used: str = Field(default="0")
    average_gas_price: str = Field(default="0")
    
    # Network metrics
    block_number: int = Field(default=0)
    block_time: float = Field(default=0.0)
    tps: float = Field(default=0.0)  # Transactions per second
    
    # User metrics
    active_addresses: int = Field(default=0)
    new_addresses: int = Field(default=0)
    
    # Economic metrics
    total_value_locked: Optional[str] = None
    defi_volume: Optional[str] = None
    
    metadata: Dict[str, Any] = Field(default_factory=dict)


class WalletAnalytics(BaseModel):
    """Analytics for a specific wallet"""
    address: str
    chain: str
    
    # Balance information
    native_balance: str
    token_balances: List[Dict[str, Any]] = Field(default_factory=list)
    total_value_usd: Optional[float] = None
    
    # Activity metrics
    transaction_count: int = Field(default=0)
    first_transaction: Optional[datetime] = None
    last_transaction: Optional[datetime] = None
    
    # Gas metrics
    total_gas_spent: str = Field(default="0")
    average_gas_price: str = Field(default="0")
    
    # Interaction metrics
    unique_contracts_interacted: int = Field(default=0)
    defi_protocols_used: List[str] = Field(default_factory=list)
    nft_collections_held: int = Field(default=0)
    
    # Risk metrics
    risk_score: Optional[float] = None
    is_contract: bool = Field(default=False)
    
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class TokenAnalytics(BaseModel):
    """Analytics for a specific token"""
    token_address: str
    chain: str
    symbol: str
    name: str
    decimals: int
    
    # Price metrics
    current_price: Optional[float] = None
    price_change_24h: Optional[float] = None
    price_change_7d: Optional[float] = None
    all_time_high: Optional[float] = None
    all_time_low: Optional[float] = None
    
    # Volume metrics
    volume_24h: Optional[str] = None
    market_cap: Optional[str] = None
    fully_diluted_market_cap: Optional[str] = None
    
    # Supply metrics
    total_supply: str
    circulating_supply: Optional[str] = None
    
    # Holder metrics
    holder_count: int = Field(default=0)
    top_holders: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Activity metrics
    transfer_count_24h: int = Field(default=0)
    unique_addresses_24h: int = Field(default=0)
    
    # Liquidity metrics
    liquidity_usd: Optional[float] = None
    liquidity_pools: List[Dict[str, Any]] = Field(default_factory=list)
    
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class DeFiProtocolAnalytics(BaseModel):
    """Analytics for DeFi protocols"""
    protocol_id: str
    name: str
    chain: str
    category: str  # lending, dex, yield, etc.
    
    # TVL metrics
    tvl_usd: float
    tvl_change_24h: float = Field(default=0.0)
    tvl_change_7d: float = Field(default=0.0)
    
    # Volume metrics
    volume_24h: Optional[float] = None
    volume_7d: Optional[float] = None
    
    # User metrics
    unique_users_24h: int = Field(default=0)
    unique_users_7d: int = Field(default=0)
    total_users: int = Field(default=0)
    
    # Transaction metrics
    transaction_count_24h: int = Field(default=0)
    transaction_count_7d: int = Field(default=0)
    
    # Revenue metrics
    fees_24h: Optional[float] = None
    revenue_24h: Optional[float] = None
    
    # Token metrics
    token_address: Optional[str] = None
    token_price: Optional[float] = None
    token_market_cap: Optional[float] = None
    
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class NFTCollectionAnalytics(BaseModel):
    """Analytics for NFT collections"""
    collection_address: str
    chain: str
    name: str
    symbol: Optional[str] = None
    
    # Price metrics
    floor_price: Optional[float] = None
    average_price: Optional[float] = None
    volume_24h: Optional[float] = None
    volume_7d: Optional[float] = None
    
    # Supply metrics
    total_supply: int
    owners: int = Field(default=0)
    
    # Activity metrics
    sales_24h: int = Field(default=0)
    sales_7d: int = Field(default=0)
    unique_buyers_24h: int = Field(default=0)
    unique_sellers_24h: int = Field(default=0)
    
    # Market metrics
    market_cap: Optional[float] = None
    listed_count: int = Field(default=0)
    
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class AnalyticsReport(BaseModel):
    """Generated analytics report"""
    report_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: Optional[str] = None
    report_type: str  # daily, weekly, monthly, custom
    
    # Time range
    start_date: datetime
    end_date: datetime
    
    # Filters
    chains: List[str] = Field(default_factory=list)
    metrics: List[str] = Field(default_factory=list)
    
    # Report data
    sections: List[Dict[str, Any]] = Field(default_factory=list)
    charts: List[Dict[str, Any]] = Field(default_factory=list)
    tables: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None
    format: str = Field(default="json")  # json, pdf, html
    file_path: Optional[str] = None
    file_size_bytes: Optional[int] = None


class AnalyticsQuery(BaseModel):
    """Query for analytics data"""
    metric_type: MetricType
    chains: List[str]
    start_time: datetime
    end_time: datetime
    interval: Optional[TimeInterval] = None
    
    # Filters
    addresses: Optional[List[str]] = None
    tokens: Optional[List[str]] = None
    protocols: Optional[List[str]] = None
    
    # Aggregation
    group_by: Optional[List[str]] = None
    order_by: Optional[str] = None
    limit: int = Field(default=1000, ge=1, le=10000)
    
    @validator('end_time')
    def validate_time_range(cls, v, values):
        if 'start_time' in values and v <= values['start_time']:
            raise ValueError("end_time must be after start_time")
        return v


class PredictionModel(BaseModel):
    """ML prediction model metadata"""
    model_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    model_type: str  # price_prediction, volume_forecast, etc.
    target_metric: str
    
    # Model info
    algorithm: str
    features: List[str]
    parameters: Dict[str, Any] = Field(default_factory=dict)
    
    # Performance metrics
    accuracy: Optional[float] = None
    rmse: Optional[float] = None
    mae: Optional[float] = None
    r2_score: Optional[float] = None
    
    # Training info
    training_start: datetime
    training_end: datetime
    training_samples: int
    validation_samples: int
    
    # Model files
    model_path: str
    version: str
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class Prediction(BaseModel):
    """Model prediction result"""
    prediction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    model_id: str
    
    # Prediction details
    target_metric: str
    predicted_value: float
    confidence_interval: Optional[tuple[float, float]] = None
    prediction_time: datetime
    
    # Input features
    features: Dict[str, Any]
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Alert(BaseModel):
    """Analytics alert configuration"""
    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: Optional[str] = None
    
    # Alert conditions
    metric: str
    chain: str
    condition: str  # gt, lt, eq, change_percent
    threshold: float
    
    # Time window
    window_minutes: int = Field(default=60)
    
    # Actions
    webhook_url: Optional[str] = None
    email_recipients: List[str] = Field(default_factory=list)
    
    # State
    is_active: bool = Field(default=True)
    last_triggered: Optional[datetime] = None
    trigger_count: int = Field(default=0)
    
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow) 