from pydantic import BaseSettings, Field
from typing import Dict, List, Optional, Any
import os


class EventFilterConfig(BaseSettings):
    """Configuration for event filters"""
    contract_address: str
    event_name: str
    abi: List[Dict[str, Any]]
    from_block: Optional[int] = None
    to_block: Optional[int] = None
    topics: Optional[List[str]] = None


class WebhookConfig(BaseSettings):
    """Configuration for webhooks"""
    url: str
    events: List[str]  # Event types to send
    headers: Dict[str, str] = Field(default_factory=dict)
    retry_count: int = Field(default=3)
    timeout_seconds: int = Field(default=30)
    secret: Optional[str] = None  # For HMAC signature


class MonitorConfig(BaseSettings):
    """Configuration for blockchain monitors"""
    chain: str
    rpc_url: str
    start_block: Optional[int] = None
    block_confirmations: int = Field(default=12)
    batch_size: int = Field(default=100)
    polling_interval: int = Field(default=5)
    event_filters: List[EventFilterConfig] = Field(default_factory=list)


class ServiceConfig(BaseSettings):
    """Event monitoring service configuration"""
    
    # Service identification
    service_name: str = Field(default="event-monitoring-service")
    service_version: str = Field(default="1.0.0")
    environment: str = Field(default="development")
    
    # API configuration
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8091)
    api_prefix: str = Field(default="/api/v1")
    
    # Database configuration
    database_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost/event_monitoring"
    )
    database_pool_size: int = Field(default=10)
    database_max_overflow: int = Field(default=20)
    
    # Blockchain monitors
    monitors: List[MonitorConfig] = Field(
        default_factory=lambda: [
            MonitorConfig(
                chain="ethereum",
                rpc_url=os.getenv("ETHEREUM_RPC_URL", "http://localhost:8545"),
                block_confirmations=12,
                event_filters=[
                    EventFilterConfig(
                        contract_address="0x0000000000000000000000000000000000000000",
                        event_name="Transfer",
                        abi=[{
                            "anonymous": False,
                            "inputs": [
                                {"indexed": True, "name": "from", "type": "address"},
                                {"indexed": True, "name": "to", "type": "address"},
                                {"indexed": False, "name": "value", "type": "uint256"}
                            ],
                            "name": "Transfer",
                            "type": "event"
                        }]
                    )
                ]
            ),
            MonitorConfig(
                chain="polygon",
                rpc_url=os.getenv("POLYGON_RPC_URL", "https://polygon-rpc.com"),
                block_confirmations=128
            )
        ]
    )
    
    # Webhook configuration
    webhooks: List[WebhookConfig] = Field(default_factory=list)
    webhook_max_retries: int = Field(default=3)
    webhook_retry_delay: int = Field(default=60)
    
    # Event processing
    event_batch_size: int = Field(default=100)
    event_retention_days: int = Field(default=30)
    process_historical_blocks: bool = Field(default=True)
    max_blocks_per_scan: int = Field(default=1000)
    
    # Pulsar configuration
    pulsar_url: str = Field(default="pulsar://localhost:6650")
    events_topic: str = Field(default="persistent://public/default/blockchain-events")
    webhook_topic: str = Field(default="persistent://public/default/webhook-deliveries")
    
    # Redis configuration
    redis_url: str = Field(default="redis://localhost:6379")
    redis_prefix: str = Field(default="event_monitor:")
    
    # Ignite cache configuration
    ignite_host: str = Field(default="localhost")
    ignite_port: int = Field(default=10800)
    cache_ttl_seconds: int = Field(default=3600)
    
    # Blockchain connector service
    blockchain_connector_url: str = Field(default="http://blockchain-connector-service:8086")
    
    # Monitoring
    metrics_enabled: bool = Field(default=True)
    metrics_port: int = Field(default=9096)
    log_level: str = Field(default="INFO")
    
    # Consul configuration
    consul_host: str = Field(default="localhost")
    consul_port: int = Field(default=8500)
    service_health_interval: int = Field(default=10)
    
    # Performance tuning
    max_concurrent_monitors: int = Field(default=10)
    max_concurrent_webhooks: int = Field(default=50)
    event_queue_size: int = Field(default=10000)
    
    class Config:
        env_prefix = "EVENT_MONITOR_"
        case_sensitive = False
        
    def get_monitor(self, chain: str) -> Optional[MonitorConfig]:
        """Get monitor configuration by chain"""
        for monitor in self.monitors:
            if monitor.chain == chain:
                return monitor
        return None
    
    def get_webhook_for_event(self, event_type: str) -> List[WebhookConfig]:
        """Get webhooks that should receive a specific event type"""
        matching_webhooks = []
        for webhook in self.webhooks:
            if event_type in webhook.events or "*" in webhook.events:
                matching_webhooks.append(webhook)
        return matching_webhooks


# Global configuration instance
config = ServiceConfig() 