"""Configuration settings for Social Trading Service."""

from typing import List
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # Service info
    service_name: str = "social-trading-service"
    service_version: str = "1.0.0"
    
    # Apache Ignite
    ignite_host: str = "localhost"
    ignite_port: int = 10800
    ignite_cache_name: str = "social_trading_cache"
    
    # Apache Pulsar
    pulsar_url: str = "pulsar://localhost:6650"
    pulsar_social_topic: str = "persistent://public/default/social-trading-events"
    pulsar_copy_trading_topic: str = "persistent://public/default/copy-trading-events"
    pulsar_reputation_topic: str = "persistent://public/default/reputation-events"
    
    # Cassandra
    cassandra_hosts: list[str] = ["localhost"]
    cassandra_keyspace: str = "social_trading"
    cassandra_port: int = 9042
    
    # JanusGraph
    janusgraph_host: str = "localhost"
    janusgraph_port: int = 8182
    
    # Copy Trading Parameters
    max_copy_allocation: float = 0.5  # Max 50% of portfolio for copy trading
    min_leader_track_record: int = 90  # Days
    max_followers_per_leader: int = 1000
    copy_trade_slippage: float = 0.01  # 1% max slippage
    
    # Reputation System
    reputation_update_interval: int = 3600  # seconds
    reputation_decay_rate: float = 0.95  # Monthly decay
    min_trades_for_reputation: int = 10
    
    # Performance Tracking
    performance_window_days: int = 365
    sharpe_ratio_risk_free_rate: float = 0.02
    max_drawdown_calculation_period: int = 30  # days
    
    # Social Features
    max_posts_per_day: int = 50
    max_follow_count: int = 1000
    trending_calculation_interval: int = 300  # seconds
    
    # Strategy NFTs
    strategy_nft_contract: str = ""  # Will be set from env
    min_performance_for_nft: float = 0.1  # 10% return
    nft_royalty_percent: float = 0.025  # 2.5%
    
    # Risk Limits
    max_leverage_copy_trading: float = 3.0
    max_position_concentration: float = 0.2  # 20% per asset
    risk_score_update_interval: int = 300  # seconds
    
    # External Services
    order_matching_service_url: str = "http://localhost:8003"
    risk_service_url: str = "http://localhost:8004"
    blockchain_gateway_url: str = "http://localhost:8002"
    graph_intelligence_url: str = "http://localhost:8009"
    
    # Caching
    trader_profile_cache_ttl: int = 300
    leaderboard_cache_ttl: int = 60
    performance_cache_ttl: int = 600
    
    class Config:
        env_prefix = "SOCIAL_TRADING_"
        case_sensitive = False 