from typing import Dict, Optional
from fastapi import Header, HTTPException, Request

from .core.matching_engine import MatchingEngine
from .config import OrderMatchingConfig


# Global instances
_matching_engine: Optional[MatchingEngine] = None
_config: Optional[OrderMatchingConfig] = None


def get_config() -> OrderMatchingConfig:
    """Get configuration instance"""
    global _config
    if _config is None:
        _config = OrderMatchingConfig()
    return _config


def get_matching_engine() -> MatchingEngine:
    """Get matching engine instance"""
    global _matching_engine
    if _matching_engine is None:
        config = get_config()
        _matching_engine = MatchingEngine(config)
    return _matching_engine


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


async def verify_market_access(
    user: Dict,
    market_id: str
) -> bool:
    """Verify user has access to market"""
    # TODO: Implement market access control
    # For now, allow all authenticated users
    return True


async def verify_trading_permissions(
    user: Dict
) -> bool:
    """Verify user has trading permissions"""
    # TODO: Implement permission checks
    # Check if user has trading role
    return "trader" in user.get("roles", []) or "admin" in user.get("roles", []) 