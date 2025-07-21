"""Configuration settings for Options Service."""

from typing import List
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # Service info
    service_name: str = "options-service"
    service_version: str = "1.0.0"
    
    # Apache Ignite
    ignite_host: str = "localhost"
    ignite_port: int = 10800
    ignite_cache_name: str = "options_cache"
    
    # Apache Pulsar
    pulsar_url: str = "pulsar://localhost:6650"
    pulsar_options_topic: str = "persistent://public/default/options-events"
    pulsar_greeks_topic: str = "persistent://public/default/greeks-updates"
    pulsar_market_data_topic: str = "persistent://public/default/market-data"
    
    # Cassandra
    cassandra_hosts: list[str] = ["localhost"]
    cassandra_keyspace: str = "options_data"
    cassandra_port: int = 9042
    
    # Pricing parameters
    risk_free_rate: float = 0.05  # 5% annual
    dividend_yield: float = 0.0
    pricing_model: str = "black_scholes"  # black_scholes, binomial, monte_carlo
    
    # Volatility surface
    vol_surface_update_interval: int = 300  # seconds
    implied_vol_iterations: int = 100
    implied_vol_tolerance: float = 0.0001
    
    # Greeks calculation
    greeks_update_interval: int = 60  # seconds
    delta_hedge_threshold: float = 0.1
    gamma_scalping_enabled: bool = True
    
    # AMM parameters
    amm_enabled: bool = True
    amm_pool_fee: float = 0.003  # 0.3%
    amm_max_slippage: float = 0.05  # 5%
    amm_liquidity_factor: float = 0.8
    
    # Risk parameters
    max_position_size: int = 1000
    max_open_interest: int = 100000
    margin_multiplier: float = 1.5
    
    # Performance tuning
    pricing_cache_ttl: int = 60
    greeks_cache_ttl: int = 30
    vol_surface_cache_ttl: int = 300
    order_book_depth: int = 50
    
    # External services
    oracle_service_url: str = "http://localhost:8010"
    risk_service_url: str = "http://localhost:8004"
    futures_service_url: str = "http://localhost:8005"
    
    # WebSocket
    ws_heartbeat_interval: int = 30
    ws_max_connections_per_user: int = 5
    
    # Strike price generation
    strike_intervals: List[float] = [50, 100, 250, 500, 1000]
    strikes_above_below: int = 20  # Number of strikes above/below spot
    
    class Config:
        env_prefix = "OPTIONS_"
        case_sensitive = False 