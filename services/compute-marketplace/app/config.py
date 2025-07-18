"""
Configuration for Web3/DeFi Compute Marketplace

Manages settings for blockchain integration, service URLs, and marketplace parameters.
"""

import os
from typing import Optional, Dict, Any
from pydantic import BaseSettings, validator


class Settings(BaseSettings):
    """Application settings with Web3/DeFi configuration"""
    
    # Service Information
    service_name: str = "compute-marketplace-service"
    service_version: str = "2.0.0"
    environment: str = os.getenv("ENVIRONMENT", "development")
    
    # Database
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost/compute_marketplace"
    )
    
    # Apache Pulsar
    pulsar_url: str = os.getenv("PULSAR_URL", "pulsar://localhost:6650")
    
    # Apache Ignite
    ignite_hosts: str = os.getenv("IGNITE_HOSTS", "localhost:10800")
    
    # Service URLs
    neuromorphic_service_url: str = os.getenv(
        "NEUROMORPHIC_SERVICE_URL",
        "http://neuromorphic-service:8000"
    )
    graph_intelligence_url: str = os.getenv(
        "GRAPH_INTELLIGENCE_URL",
        "http://graph-intelligence-service:8000"
    )
    federated_learning_url: str = os.getenv(
        "FEDERATED_LEARNING_URL",
        "http://federated-learning-service:8000"
    )
    quantum_optimization_url: Optional[str] = os.getenv(
        "QUANTUM_OPTIMIZATION_URL",
        "http://quantum-optimization-service:8000"
    )
    vc_service_url: str = os.getenv(
        "VC_SERVICE_URL",
        "http://verifiable-credential-service:8000"
    )
    
    # Web3 Configuration
    web3_provider_url: str = os.getenv(
        "WEB3_PROVIDER_URL",
        "https://polygon-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_KEY"
    )
    chain_id: int = int(os.getenv("CHAIN_ID", "137"))  # Polygon Mainnet
    
    # Smart Contract Addresses
    token_factory_address: str = os.getenv(
        "TOKEN_FACTORY_ADDRESS",
        "0x0000000000000000000000000000000000000000"  # Deploy and update
    )
    amm_router_address: str = os.getenv(
        "AMM_ROUTER_ADDRESS",
        "0x0000000000000000000000000000000000000000"  # Deploy and update
    )
    lending_pool_address: str = os.getenv(
        "LENDING_POOL_ADDRESS",
        "0x0000000000000000000000000000000000000000"  # Deploy and update
    )
    price_oracle_address: str = os.getenv(
        "PRICE_ORACLE_ADDRESS",
        "0x0000000000000000000000000000000000000000"  # Deploy and update
    )
    
    # Operator Configuration (for automated operations)
    operator_private_key: Optional[str] = os.getenv("OPERATOR_PRIVATE_KEY")
    operator_address: Optional[str] = os.getenv("OPERATOR_ADDRESS")
    
    # Marketplace Parameters
    min_offering_duration_minutes: int = 60
    max_offering_duration_minutes: int = 10080  # 1 week
    default_tokenization_hours: int = 1000
    min_fraction_size_hours: float = 0.1
    
    # Pricing Parameters
    base_transaction_fee_percentage: float = 2.5  # Platform fee
    fl_premium_percentage: float = 20.0  # Premium for FL compute
    trust_discount_max_percentage: float = 20.0
    new_user_penalty_percentage: float = 10.0
    
    # DeFi Parameters
    default_collateral_factor: float = 0.75
    liquidation_threshold: float = 0.85
    liquidation_penalty: float = 0.05
    base_borrow_rate_apr: float = 0.02  # 2% APR
    
    # Liquidity Incentives
    liquidity_mining_enabled: bool = True
    liquidity_rewards_per_day: float = 1000.0  # Platform tokens
    
    # Cache Configuration
    cache_ttl_seconds: int = 300  # 5 minutes
    trust_cache_size: int = 10000
    
    # Monitoring
    enable_metrics: bool = True
    metrics_port: int = 9090
    
    # Security
    enable_rate_limiting: bool = True
    rate_limit_requests_per_minute: int = 60
    
    @validator("chain_id")
    def validate_chain_id(cls, v):
        """Validate supported chain IDs"""
        supported_chains = {
            1: "Ethereum Mainnet",
            137: "Polygon Mainnet",
            80001: "Polygon Mumbai Testnet",
            42161: "Arbitrum One",
            10: "Optimism"
        }
        if v not in supported_chains:
            raise ValueError(f"Unsupported chain ID: {v}")
        return v
    
    @validator("operator_private_key")
    def validate_private_key(cls, v):
        """Validate private key format"""
        if v and not v.startswith("0x"):
            return f"0x{v}"
        return v
    
    def get_contract_addresses(self) -> Dict[str, str]:
        """Get all contract addresses"""
        return {
            "token_factory": self.token_factory_address,
            "amm_router": self.amm_router_address,
            "lending_pool": self.lending_pool_address,
            "price_oracle": self.price_oracle_address
        }
    
    def get_ignite_nodes(self) -> list:
        """Parse Ignite hosts into list"""
        return self.ignite_hosts.split(",")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Singleton instance
settings = Settings()


# Chain-specific configurations
CHAIN_CONFIG = {
    137: {  # Polygon Mainnet
        "name": "Polygon",
        "native_token": "MATIC",
        "stable_coins": ["USDC", "USDT", "DAI"],
        "block_time": 2,  # seconds
        "finality_blocks": 128
    },
    1: {  # Ethereum Mainnet
        "name": "Ethereum",
        "native_token": "ETH",
        "stable_coins": ["USDC", "USDT", "DAI", "FRAX"],
        "block_time": 12,
        "finality_blocks": 32
    },
    42161: {  # Arbitrum
        "name": "Arbitrum One",
        "native_token": "ETH",
        "stable_coins": ["USDC", "USDT", "DAI"],
        "block_time": 0.25,
        "finality_blocks": 1
    }
}


# Token standards for compute hours
COMPUTE_TOKEN_CONFIG = {
    "decimals": 18,
    "base_symbol": "CHT",  # Compute Hour Token
    "name_template": "{provider} {resource_type} Compute Hours",
    "min_liquidity_eth": 0.1,  # Minimum ETH/MATIC for initial liquidity
    "min_liquidity_usd": 100  # Minimum USD value for initial liquidity
}


# Supported resource types for tokenization
TOKENIZABLE_RESOURCES = {
    "cpu": {
        "min_vcpus": 2,
        "min_memory_gb": 4,
        "base_price_multiplier": 1.0
    },
    "gpu": {
        "supported_models": ["A100", "V100", "T4", "RTX3090", "RTX4090", "H100"],
        "min_memory_gb": 8,
        "base_price_multiplier": 5.0
    },
    "tpu": {
        "min_cores": 8,
        "base_price_multiplier": 10.0
    }
}


# Federated Learning specific config
FL_CONFIG = {
    "privacy_levels": {
        "basic": {"price_multiplier": 1.0},
        "enhanced": {"price_multiplier": 1.5},
        "maximum": {"price_multiplier": 2.0}
    },
    "encryption_schemes": {
        "CKKS": {"overhead_factor": 3.0},
        "Paillier": {"overhead_factor": 2.5},
        "BFV": {"overhead_factor": 2.8}
    },
    "min_providers_per_session": 1,
    "max_providers_per_session": 10,
    "auto_provision_threshold": 0.3  # Auto-provision if < 30% capacity
}


# Get current chain config
def get_chain_config() -> Dict[str, Any]:
    """Get configuration for current chain"""
    return CHAIN_CONFIG.get(settings.chain_id, CHAIN_CONFIG[137])