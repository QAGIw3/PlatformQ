"""Configuration for Risk Engine Service."""

from pydantic_settings import BaseSettings
from typing import List, Dict, Any
from decimal import Decimal


class Settings(BaseSettings):
    """Risk Engine Service configuration."""
    
    # Service info
    service_name: str = "risk-engine-service"
    service_version: str = "1.0.0"
    
    # API Configuration
    api_prefix: str = "/api/v1"
    host: str = "0.0.0.0"
    port: int = 8021
    
    # Apache Ignite
    ignite_host: str = "localhost"
    ignite_port: int = 10800
    ignite_cache_prefix: str = "risk_engine"
    
    # Apache Pulsar
    pulsar_url: str = "pulsar://localhost:6650"
    pulsar_risk_events_topic: str = "persistent://public/default/risk-events"
    pulsar_position_events_topic: str = "persistent://public/default/position-events"
    pulsar_market_events_topic: str = "persistent://public/default/market-events"
    
    # Apache Flink
    flink_jobmanager_url: str = "http://localhost:8081"
    flink_checkpoint_interval_ms: int = 5000
    flink_parallelism: int = 4
    
    # Risk Parameters
    # Margin requirements
    initial_margin_multiplier: Decimal = Decimal("1.0")
    maintenance_margin_multiplier: Decimal = Decimal("0.5")
    
    # Position limits
    max_position_value: Decimal = Decimal("10000000")  # $10M
    max_leverage: Decimal = Decimal("20")
    max_open_positions: int = 100
    
    # Liquidation parameters
    liquidation_margin_ratio: Decimal = Decimal("1.1")  # 110%
    auto_liquidation_enabled: bool = True
    liquidation_batch_size: int = 10
    
    # Risk monitoring
    risk_check_interval_seconds: int = 10
    position_update_interval_seconds: int = 5
    
    # VaR (Value at Risk) parameters
    var_confidence_level: float = 0.95
    var_time_horizon_days: int = 1
    var_lookback_days: int = 30
    
    # Stress testing
    stress_test_scenarios: List[Dict[str, Any]] = [
        {"name": "market_crash", "price_change": -0.20},
        {"name": "flash_crash", "price_change": -0.10},
        {"name": "volatility_spike", "vol_multiplier": 2.0}
    ]
    
    # Product-specific risk parameters
    futures_risk_params: Dict[str, Any] = {
        "max_notional": 5000000,
        "max_contracts": 1000,
        "funding_rate_limit": 0.01
    }
    
    options_risk_params: Dict[str, Any] = {
        "max_notional": 2000000,
        "max_contracts": 500,
        "max_gamma": 1000,
        "max_vega": 5000
    }
    
    # ML Risk Model Configuration
    ml_model_enabled: bool = True
    ml_model_path: str = "/models/risk_model.pkl"
    ml_feature_window: int = 24  # hours
    ml_prediction_threshold: float = 0.7
    
    # External services
    trading_core_url: str = "http://localhost:8020"
    market_intelligence_url: str = "http://localhost:8022"
    
    # Caching
    risk_cache_ttl: int = 60  # seconds
    position_cache_ttl: int = 30
    
    # Monitoring
    metrics_enabled: bool = True
    metrics_port: int = 9021
    
    class Config:
        env_prefix = "RISK_ENGINE_"
        case_sensitive = False 