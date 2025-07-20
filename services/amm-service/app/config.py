"""Configuration settings for AMM Service."""

from decimal import Decimal
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # Service info
    service_name: str = "amm-service"
    service_version: str = "1.0.0"
    
    # Apache Ignite
    ignite_host: str = "localhost"
    ignite_port: int = 10800
    ignite_cache_name: str = "amm_cache"
    
    # Apache Pulsar
    pulsar_url: str = "pulsar://localhost:6650"
    pulsar_amm_topic: str = "persistent://public/default/amm-events"
    pulsar_liquidity_topic: str = "persistent://public/default/liquidity-events"
    pulsar_pricing_topic: str = "persistent://public/default/pricing-events"
    
    # Cassandra
    cassandra_hosts: list[str] = ["localhost"]
    cassandra_keyspace: str = "amm_data"
    cassandra_port: int = 9042
    
    # AMM Configuration
    # Fee tiers
    base_fee_bps: int = 30  # 0.3% base fee
    min_fee_bps: int = 1    # 0.01% minimum
    max_fee_bps: int = 100  # 1% maximum
    
    # Concentrated liquidity
    tick_spacing: int = 60  # 0.6% between ticks
    max_tick: int = 887272
    min_tick: int = -887272
    
    # Risk parameters
    max_price_impact: Decimal = Decimal("0.05")  # 5% max price impact
    max_slippage: Decimal = Decimal("0.02")      # 2% max slippage
    
    # Liquidity parameters
    base_liquidity_size: Decimal = Decimal("10000")
    liquidity_depth_levels: int = 5
    liquidity_level_spacing_bps: int = 25  # 0.25% between levels
    
    # Volatility-based fee adjustment
    volatility_window_hours: int = 24
    volatility_multiplier: Decimal = Decimal("2.0")  # Max 2x fee for high volatility
    
    # Volume-based fee tiers (volume in USD, discount percentage)
    volume_fee_tiers: list[tuple[int, float]] = [
        (0, 0.0),           # No discount
        (100000, 0.1),      # 10% discount > $100k
        (1000000, 0.2),     # 20% discount > $1M
        (10000000, 0.3),    # 30% discount > $10M
    ]
    
    # Pool imbalance parameters
    imbalance_threshold: Decimal = Decimal("0.2")  # 20% imbalance threshold
    imbalance_fee_multiplier: Decimal = Decimal("1.3")  # 30% fee increase
    
    # StableSwap parameters (for correlated assets)
    stableswap_amplification: int = 100
    stableswap_fee_bps: int = 5  # 0.05% for stable pairs
    
    # Options AMM specific
    options_base_spread_bps: int = 50  # 0.5% base spread
    options_max_net_delta: Decimal = Decimal("1000")
    options_max_net_gamma: Decimal = Decimal("100")
    options_max_net_vega: Decimal = Decimal("500")
    options_hedge_interval: int = 60  # seconds
    
    # External services
    oracle_service_url: str = "http://localhost:8010"
    options_service_url: str = "http://localhost:8006"
    futures_service_url: str = "http://localhost:8005"
    
    # Caching
    price_cache_ttl: int = 5
    liquidity_cache_ttl: int = 60
    fee_cache_ttl: int = 300
    
    # Update intervals
    fee_update_interval: int = 300  # 5 minutes
    liquidity_rebalance_interval: int = 3600  # 1 hour
    
    class Config:
        env_prefix = "AMM_"
        case_sensitive = False 