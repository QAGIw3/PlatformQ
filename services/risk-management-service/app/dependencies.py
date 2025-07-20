"""Dependencies for Risk Management Service"""

from typing import Dict, Optional
from fastapi import Header, HTTPException

from platformq_trading_common.events.trading_events import EventPublisher
from pulsar import Client as PulsarClient

from .config import RiskManagementConfig
from .core.risk_monitor import RiskMonitor
from .integrations.market_data import MarketDataClient
from .integrations.position_service import PositionServiceClient


# Global instances
_config: Optional[RiskManagementConfig] = None
_risk_monitor: Optional[RiskMonitor] = None
_pulsar_client: Optional[PulsarClient] = None
_event_publisher: Optional[EventPublisher] = None
_market_data_client: Optional[MarketDataClient] = None
_position_client: Optional[PositionServiceClient] = None


def get_config() -> RiskManagementConfig:
    """Get configuration instance"""
    global _config
    if _config is None:
        _config = RiskManagementConfig()
    return _config


def get_pulsar_client() -> PulsarClient:
    """Get Pulsar client"""
    global _pulsar_client
    if _pulsar_client is None:
        config = get_config()
        _pulsar_client = PulsarClient(config.PULSAR_URL)
    return _pulsar_client


def get_event_publisher() -> EventPublisher:
    """Get event publisher"""
    global _event_publisher
    if _event_publisher is None:
        _event_publisher = EventPublisher(get_pulsar_client())
    return _event_publisher


def get_market_data_client() -> MarketDataClient:
    """Get market data client"""
    global _market_data_client
    if _market_data_client is None:
        _market_data_client = MarketDataClient()
    return _market_data_client


def get_position_client() -> PositionServiceClient:
    """Get position service client"""
    global _position_client
    if _position_client is None:
        _position_client = PositionServiceClient()
    return _position_client


def get_risk_monitor() -> RiskMonitor:
    """Get risk monitor instance"""
    global _risk_monitor
    if _risk_monitor is None:
        config = get_config()
        _risk_monitor = RiskMonitor(
            config=config,
            market_data_client=get_market_data_client(),
            position_client=get_position_client(),
            event_publisher=get_event_publisher()
        )
    return _risk_monitor


def get_current_user(
    x_user_id: str = Header(None),
    x_tenant_id: str = Header(None),
    x_roles: str = Header(None)
) -> Dict:
    """Extract user information from headers"""
    if not x_user_id or not x_tenant_id:
        raise HTTPException(
            status_code=401,
            detail="Missing authentication headers"
        )
    
    return {
        "user_id": x_user_id,
        "tenant_id": x_tenant_id,
        "roles": x_roles.split(",") if x_roles else []
    } 