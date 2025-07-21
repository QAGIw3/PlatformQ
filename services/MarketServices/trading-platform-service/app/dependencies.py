"""
FastAPI Dependencies

Common dependencies for the trading platform service.
"""

from typing import Optional
from fastapi import Depends, HTTPException, Header, Request
import logging

from platformq_shared import ServiceClient
from app.social_trading.copy import CopyTradingExecutor
from app.social_trading.reputation import ReputationEngine

logger = logging.getLogger(__name__)


def get_trading_core_client(request: Request) -> ServiceClient:
    """Get the trading core service client from app state"""
    client = getattr(request.app.state, "trading_core_client", None)
    if not client:
        # Create a new client if not available
        client = ServiceClient(
            service_name="trading-core-service",
            circuit_breaker_threshold=5,
            rate_limit=100.0
        )
        request.app.state.trading_core_client = client
    return client


def get_current_user(authorization: str = Header(...)) -> dict:
    """Extract current user from auth header"""
    # This would normally validate JWT and extract user info
    # For now, return a mock user
    return {
        "user_id": "test_user",
        "tenant_id": "default",
        "roles": ["trader"]
    }


def get_vault_consul(request: Request):
    """Get Vault/Consul integration from app state"""
    vault_consul = getattr(request.app.state, "vault_consul", None)
    if not vault_consul:
        raise HTTPException(status_code=503, detail="Vault/Consul integration not available")
    return vault_consul


def get_strategy_engine(request: Request):
    """Get strategy engine from app state"""
    engine = getattr(request.app.state, "strategy_engine", None)
    if not engine:
        raise HTTPException(status_code=503, detail="Strategy engine not available")
    return engine


def get_copy_executor(request: Request) -> CopyTradingExecutor:
    """Get copy trading executor from app state"""
    executor = getattr(request.app.state, "copy_executor", None)
    if not executor:
        raise HTTPException(status_code=503, detail="Copy trading executor not available")
    return executor


def get_market_engine(request: Request):
    """Get prediction market engine from app state"""
    engine = getattr(request.app.state, "market_engine", None)
    if not engine:
        raise HTTPException(status_code=503, detail="Market engine not available")
    return engine


def get_reputation_engine(request: Request) -> ReputationEngine:
    """Get reputation engine from app state"""
    engine = getattr(request.app.state, "reputation_engine", None)
    if not engine:
        raise HTTPException(status_code=503, detail="Reputation engine not available")
    return engine 