"""Dependency injection for Trading Platform Service."""

from typing import Optional
from fastapi import Request, Header, HTTPException

from .shared.service_client import ServiceClient
from .social_trading.copy.fast_copy_executor import FastCopyExecutor
from .social_trading.copy.copy_executor import CopyTradingExecutor
from .social_trading.reputation.reputation_engine import ReputationEngine


def get_trading_core_client(request: Request) -> ServiceClient:
    """Get Trading Core Service client."""
    if hasattr(request.app.state, 'trading_core_client'):
        return request.app.state.trading_core_client
    
    # Fallback if not initialized
    return ServiceClient(
        base_url="http://localhost:8020",
        service_name="trading-core"
    )


def get_current_user(authorization: str = Header(...)) -> dict:
    """Get current user from auth token."""
    # Simple mock implementation
    # In production, this would validate the JWT token
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Invalid authorization header")
    
    # Mock user for development
    return {
        "user_id": "test_user",
        "username": "test_trader",
        "is_verified": True
    }


def get_vault_consul(request: Request):
    """Get Vault/Consul integration."""
    if hasattr(request.app.state, 'vault_consul'):
        return request.app.state.vault_consul
    
    # Return None if not initialized
    return None


def get_strategy_engine(request: Request):
    """Get strategy engine."""
    # Not implemented yet
    return None


def get_copy_executor(request: Request):
    """Get copy trading executor."""
    if hasattr(request.app.state, 'copy_executor'):
        return request.app.state.copy_executor
    
    # Return None if not initialized
    return None


def get_market_engine(request: Request):
    """Get market engine."""
    # Not implemented yet
    return None


def get_reputation_engine(request: Request) -> ReputationEngine:
    """Get reputation engine."""
    if hasattr(request.app.state, 'reputation_engine'):
        return request.app.state.reputation_engine
    
    # Return None if not initialized
    return None 