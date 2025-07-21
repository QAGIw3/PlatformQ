from pydantic import BaseSettings, Field
from typing import Dict, List, Optional
import os


class BridgeChainConfig(BaseSettings):
    """Configuration for a blockchain in the bridge"""
    rpc_url: str
    chain_id: int
    confirmations_required: int = Field(default=12)
    max_gas_price: Optional[float] = None
    bridge_contract: Optional[str] = None
    wrapped_token_contracts: Dict[str, str] = Field(default_factory=dict)


class BridgeConfig(BaseSettings):
    """Bridge-specific configuration"""
    name: str
    source_chain: str
    target_chain: str
    min_amount: float = Field(default=0.01)
    max_amount: float = Field(default=1000000.0)
    fee_percentage: float = Field(default=0.1)
    relayer_address: Optional[str] = None
    attestation_threshold: int = Field(default=2)


class ServiceConfig(BaseSettings):
    """Cross-chain bridge service configuration"""
    
    # Service identification
    service_name: str = Field(default="cross-chain-bridge-service")
    service_version: str = Field(default="1.0.0")
    environment: str = Field(default="development")
    
    # API configuration
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8090)
    api_prefix: str = Field(default="/api/v1")
    
    # Supported chains configuration
    chains: Dict[str, BridgeChainConfig] = Field(
        default_factory=lambda: {
            "ethereum": BridgeChainConfig(
                rpc_url=os.getenv("ETHEREUM_RPC_URL", "http://localhost:8545"),
                chain_id=1,
                confirmations_required=12
            ),
            "polygon": BridgeChainConfig(
                rpc_url=os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com"),
                chain_id=137,
                confirmations_required=128
            ),
            "bsc": BridgeChainConfig(
                rpc_url=os.getenv("BSC_RPC_URL", "https://bsc-dataseed.binance.org"),
                chain_id=56,
                confirmations_required=15
            ),
            "avalanche": BridgeChainConfig(
                rpc_url=os.getenv("AVALANCHE_RPC_URL", "https://api.avax.network/ext/bc/C/rpc"),
                chain_id=43114,
                confirmations_required=12
            ),
            "arbitrum": BridgeChainConfig(
                rpc_url=os.getenv("ARBITRUM_RPC_URL", "https://arb1.arbitrum.io/rpc"),
                chain_id=42161,
                confirmations_required=1
            ),
            "optimism": BridgeChainConfig(
                rpc_url=os.getenv("OPTIMISM_RPC_URL", "https://mainnet.optimism.io"),
                chain_id=10,
                confirmations_required=1
            ),
            "solana": BridgeChainConfig(
                rpc_url=os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com"),
                chain_id=0,  # Solana doesn't use chain IDs
                confirmations_required=32
            ),
            "cosmos": BridgeChainConfig(
                rpc_url=os.getenv("COSMOS_RPC_URL", "https://cosmos-rpc.quickapi.com"),
                chain_id=0,
                confirmations_required=10
            )
        }
    )
    
    # Bridge configurations
    bridges: List[BridgeConfig] = Field(
        default_factory=lambda: [
            BridgeConfig(
                name="eth-polygon",
                source_chain="ethereum",
                target_chain="polygon",
                fee_percentage=0.1
            ),
            BridgeConfig(
                name="eth-bsc",
                source_chain="ethereum",
                target_chain="bsc",
                fee_percentage=0.15
            ),
            BridgeConfig(
                name="polygon-bsc",
                source_chain="polygon",
                target_chain="bsc",
                fee_percentage=0.1
            ),
            BridgeConfig(
                name="eth-arbitrum",
                source_chain="ethereum",
                target_chain="arbitrum",
                fee_percentage=0.05
            ),
            BridgeConfig(
                name="eth-optimism",
                source_chain="ethereum",
                target_chain="optimism",
                fee_percentage=0.05
            )
        ]
    )
    
    # Bridge operation settings
    max_pending_transfers: int = Field(default=1000)
    transfer_timeout_seconds: int = Field(default=3600)
    attestation_window_seconds: int = Field(default=300)
    cleanup_interval_seconds: int = Field(default=3600)
    
    # Security settings
    require_attestations: bool = Field(default=True)
    min_attestations: int = Field(default=2)
    max_attestation_age_seconds: int = Field(default=600)
    rate_limit_per_address: int = Field(default=10)
    rate_limit_window_seconds: int = Field(default=3600)
    
    # Pulsar configuration
    pulsar_url: str = Field(default="pulsar://localhost:6650")
    bridge_events_topic: str = Field(default="persistent://public/default/bridge-events")
    transfer_requests_topic: str = Field(default="persistent://public/default/transfer-requests")
    attestation_topic: str = Field(default="persistent://public/default/bridge-attestations")
    
    # Ignite cache configuration
    ignite_host: str = Field(default="localhost")
    ignite_port: int = Field(default=10800)
    cache_ttl_seconds: int = Field(default=3600)
    
    # Key management service
    key_management_url: str = Field(default="http://key-management-service:8088")
    signing_timeout: int = Field(default=30)
    
    # Blockchain connector service
    blockchain_connector_url: str = Field(default="http://blockchain-connector-service:8086")
    
    # Transaction processor service
    transaction_processor_url: str = Field(default="http://transaction-processor-service:8087")
    
    # Monitoring
    metrics_enabled: bool = Field(default=True)
    metrics_port: int = Field(default=9095)
    log_level: str = Field(default="INFO")
    
    # Consul configuration
    consul_host: str = Field(default="localhost")
    consul_port: int = Field(default=8500)
    service_health_interval: int = Field(default=10)
    
    class Config:
        env_prefix = "BRIDGE_"
        case_sensitive = False
        
    def get_bridge(self, name: str) -> Optional[BridgeConfig]:
        """Get bridge configuration by name"""
        for bridge in self.bridges:
            if bridge.name == name:
                return bridge
        return None
    
    def get_chain(self, name: str) -> Optional[BridgeChainConfig]:
        """Get chain configuration by name"""
        return self.chains.get(name)
    
    def get_supported_bridges(self) -> List[str]:
        """Get list of supported bridge names"""
        return [bridge.name for bridge in self.bridges]


# Global configuration instance
config = ServiceConfig() 