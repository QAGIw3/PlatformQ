"""Social Trading Service dependencies for FastAPI."""

from typing import Annotated, Optional

from fastapi import Depends, Header, HTTPException

from app.config import Settings
from app.copy.copy_executor import CopyTradingExecutor
from app.reputation.reputation_engine import ReputationEngine


# Global instances (initialized in main.py)
_settings: Optional[Settings] = None
_copy_executor: Optional[CopyTradingExecutor] = None
_reputation_engine: Optional[ReputationEngine] = None


def init_dependencies(
    settings: Settings,
    copy_executor: CopyTradingExecutor,
    reputation_engine: ReputationEngine
):
    """Initialize global dependencies."""
    global _settings, _copy_executor, _reputation_engine
    _settings = settings
    _copy_executor = copy_executor
    _reputation_engine = reputation_engine


def get_settings() -> Settings:
    """Get application settings."""
    if _settings is None:
        raise RuntimeError("Dependencies not initialized")
    return _settings


async def get_copy_executor() -> CopyTradingExecutor:
    """Get copy trading executor instance."""
    if _copy_executor is None:
        raise RuntimeError("Dependencies not initialized")
    return _copy_executor


async def get_reputation_engine() -> ReputationEngine:
    """Get reputation engine instance."""
    if _reputation_engine is None:
        raise RuntimeError("Dependencies not initialized")
    return _reputation_engine


async def get_cache_manager():
    """Get cache manager instance."""
    # In production, return actual cache manager
    # For now, return a mock
    class MockCacheManager:
        async def get_trader_profile(self, user_id: str):
            return None
        async def store_trader_profile(self, profile):
            pass
        async def update_trader_profile(self, profile):
            pass
        async def add_follow_relationship(self, follower_id: str, leader_id: str):
            pass
        async def remove_follow_relationship(self, follower_id: str, leader_id: str):
            pass
        async def get_copy_relation(self, follower_id: str, leader_id: str):
            return None
        async def get_copy_relation_by_id(self, relation_id: str):
            return None
        async def store_copy_relation(self, relation):
            pass
        async def update_copy_relation(self, relation):
            pass
        async def get_user_copy_relations(self, user_id: str):
            return []
        async def get_user_daily_posts(self, user_id: str):
            return 0
        async def store_social_post(self, post):
            pass
        async def get_following_list(self, user_id: str):
            return []
        async def get_posts_by_authors(self, authors, limit, offset):
            return []
        async def get_leaderboard(self, period: str, limit: int):
            return None
        async def get_performance_metrics(self, user_id: str, period: str):
            return None
        async def get_copy_trader_count(self, user_id: str):
            return 0
        async def cache_leaderboard(self, period: str, entries):
            pass
            
    return MockCacheManager()


async def get_current_user(
    x_user_id: Annotated[Optional[str], Header()] = None
) -> str:
    """Get current user from headers."""
    if not x_user_id:
        raise HTTPException(status_code=401, detail="User ID header required")
    return x_user_id 