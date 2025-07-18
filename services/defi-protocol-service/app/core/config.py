"""
Configuration settings for DeFi Protocol Service.
"""

from typing import List, Dict, Any, Optional
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """DeFi Protocol Service configuration"""
    
    # Service info
    SERVICE_NAME: str = "defi-protocol-service"
    VERSION: str = "1.0.0"
    
    # API configuration
    API_PREFIX: str = "/api/v1"
    DEBUG: bool = Field(False, env="DEBUG")
    
    # Blockchain configurations
    SUPPORTED_CHAINS: List[Dict[str, Any]] = [
        {
            "type": "ethereum",
            "rpc_url": Field("http://ethereum-node:8545", env="ETHEREUM_RPC_URL"),
            "chain_id": 1,
            "extras": {
                "gas_limit": 8000000,
                "confirmation_blocks": 12
            }
        },
        {
            "type": "polygon",
            "rpc_url": Field("http://polygon-node:8545", env="POLYGON_RPC_URL"),
            "chain_id": 137,
            "extras": {
                "gas_limit": 30000000,
                "confirmation_blocks": 128
            }
        },
        {
            "type": "arbitrum",
            "rpc_url": Field("http://arbitrum-node:8545", env="ARBITRUM_RPC_URL"),
            "chain_id": 42161,
            "extras": {
                "gas_limit": 50000000,
                "confirmation_blocks": 1
            }
        },
        {
            "type": "optimism",
            "rpc_url": Field("http://optimism-node:8545", env="OPTIMISM_RPC_URL"),
            "chain_id": 10,
            "extras": {
                "gas_limit": 30000000,
                "confirmation_blocks": 1
            }
        },
        {
            "type": "avalanche",
            "rpc_url": Field("http://avalanche-node:9650", env="AVALANCHE_RPC_URL"),
            "chain_id": 43114,
            "extras": {
                "gas_limit": 8000000,
                "confirmation_blocks": 1
            }
        },
        {
            "type": "bsc",
            "rpc_url": Field("http://bsc-node:8545", env="BSC_RPC_URL"),
            "chain_id": 56,
            "extras": {
                "gas_limit": 60000000,
                "confirmation_blocks": 15
            }
        },
        {
            "type": "solana",
            "rpc_url": Field("http://solana-node:8899", env="SOLANA_RPC_URL"),
            "chain_id": 101,
            "extras": {
                "commitment": "confirmed"
            }
        },
        {
            "type": "cosmos",
            "rpc_url": Field("http://cosmos-node:26657", env="COSMOS_RPC_URL"),
            "chain_id": "cosmoshub-4",
            "extras": {
                "prefix": "cosmos",
                "denom": "uatom"
            }
        },
        {
            "type": "polkadot",
            "rpc_url": Field("ws://polkadot-node:9944", env="POLKADOT_RPC_URL"),
            "chain_name": "Polkadot",
            "extras": {}
        }
    ]
    
    # Connection pool settings
    MAX_CONNECTIONS_PER_CHAIN: int = Field(5, env="MAX_CONNECTIONS_PER_CHAIN")
    CONNECTION_TIMEOUT: int = Field(30, env="CONNECTION_TIMEOUT")
    HEALTH_CHECK_INTERVAL: int = Field(60, env="HEALTH_CHECK_INTERVAL")
    
    # Price oracle settings
    PRICE_PROVIDERS: List[str] = Field(
        ["coingecko", "chainlink", "uniswap"],
        env="PRICE_PROVIDERS"
    )
    PRICE_CACHE_TTL: int = Field(300, env="PRICE_CACHE_TTL")  # 5 minutes
    
    # Risk management settings
    MAX_LEVERAGE: float = Field(3.0, env="MAX_LEVERAGE")
    LIQUIDATION_THRESHOLD: float = Field(0.8, env="LIQUIDATION_THRESHOLD")
    MIN_COLLATERAL_RATIO: float = Field(1.5, env="MIN_COLLATERAL_RATIO")
    VOLATILITY_WINDOW: int = Field(30, env="VOLATILITY_WINDOW")  # days
    
    # Protocol-specific settings
    LENDING_SETTINGS: Dict[str, Any] = {
        "max_loan_duration": 365 * 24 * 3600,  # 1 year in seconds
        "min_loan_amount": 100,  # USD
        "interest_rate_model": "compound",
        "compound_frequency": 86400  # Daily
    }
    
    AUCTION_SETTINGS: Dict[str, Any] = {
        "max_auction_duration": 30 * 24 * 3600,  # 30 days
        "min_auction_duration": 3600,  # 1 hour
        "platform_fee_percentage": 2.5,  # 2.5%
        "min_bid_increment_percentage": 5.0  # 5%
    }
    
    YIELD_FARMING_SETTINGS: Dict[str, Any] = {
        "reward_distribution_frequency": 3600,  # Hourly
        "min_stake_duration": 86400,  # 1 day
        "early_withdrawal_penalty": 10.0,  # 10%
        "max_pools_per_token": 5
    }
    
    LIQUIDITY_SETTINGS: Dict[str, Any] = {
        "min_liquidity": 1000,  # USD
        "swap_fee_percentage": 0.3,  # 0.3%
        "protocol_fee_percentage": 0.05,  # 0.05%
        "slippage_tolerance": 0.5  # 0.5%
    }
    
    # External service URLs
    BLOCKCHAIN_GATEWAY_URL: str = Field(
        "http://blockchain-gateway-service:8000",
        env="BLOCKCHAIN_GATEWAY_URL"
    )
    EVENT_ROUTER_URL: str = Field(
        "http://event-router-service:8000",
        env="EVENT_ROUTER_URL"
    )
    
    # Database settings
    DATABASE_URL: str = Field(
        "postgresql://defi:defi123@postgres:5432/defi_protocol",
        env="DATABASE_URL"
    )
    
    # Redis settings
    REDIS_URL: str = Field("redis://redis:6379/2", env="REDIS_URL")
    
    # Monitoring
    ENABLE_METRICS: bool = Field(True, env="ENABLE_METRICS")
    METRICS_PORT: int = Field(9092, env="METRICS_PORT")
    
    # Security
    ENABLE_RATE_LIMITING: bool = Field(True, env="ENABLE_RATE_LIMITING")
    RATE_LIMIT_PER_MINUTE: int = Field(60, env="RATE_LIMIT_PER_MINUTE")
    
    class Config:
        env_file = ".env"
        case_sensitive = True


# Create global settings instance
settings = Settings() 