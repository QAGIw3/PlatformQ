"""Social Trading API endpoints."""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional, Annotated
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, BackgroundTasks
from pydantic import BaseModel, Field

from ...dependencies import get_current_user, get_copy_executor, get_reputation_engine
from ..models import (
    TraderProfile, TradingStrategy, CopyTradingRelation,
    SocialPost, LeaderboardEntry, CopyMode
)
from platformq_trading_common import publish_event, EventType


router = APIRouter(prefix="/social", tags=["social-trading"])


# Request/Response Models

class CreateProfileRequest(BaseModel):
    """Request to create or update trader profile."""
    username: str = Field(..., min_length=3, max_length=30)
    display_name: str = Field(..., min_length=1, max_length=50)
    bio: Optional[str] = Field(None, max_length=500)
    allows_copy_trading: bool = False
    copy_trading_fee: float = Field(0.0, ge=0, le=0.1)  # Max 10% fee
    min_copy_amount: Decimal = Field(Decimal("100"), gt=0)


class StartCopyTradingRequest(BaseModel):
    """Request to start copying a trader."""
    copy_mode: CopyMode
    allocation_amount: Optional[Decimal] = Field(None, gt=0)
    allocation_percent: Optional[float] = Field(None, gt=0, le=50)  # Max 50%
    max_position_size: Optional[Decimal] = None
    max_daily_trades: Optional[int] = Field(None, ge=1, le=100)
    stop_loss_percent: Optional[float] = Field(None, gt=0, le=20)  # Max 20%
    max_drawdown_percent: Optional[float] = Field(None, gt=0, le=30)  # Max 30%


class CreatePostRequest(BaseModel):
    """Request to create a social post."""
    content: str = Field(..., min_length=1, max_length=1000)
    tags: List[str] = Field(default=[], max_items=10)
    assets_mentioned: List[str] = Field(default=[], max_items=5)
    strategy_id: Optional[str] = None
    is_educational: bool = False


# Cache manager dependency - temporary mock
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


# Profile Endpoints

@router.get("/profile/{user_id}", response_model=TraderProfile)
async def get_trader_profile(
    user_id: str,
    cache_manager = Depends(get_cache_manager)
):
    """Get trader profile by user ID."""
    profile = await cache_manager.get_trader_profile(user_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Trader profile not found")
    return profile


@router.post("/profile", response_model=TraderProfile)
async def create_or_update_profile(
    request: CreateProfileRequest,
    current_user: dict = Depends(get_current_user),
    cache_manager = Depends(get_cache_manager)
):
    """Create or update trader profile."""
    user_id = current_user["user_id"]
    
    # Check if profile exists
    existing = await cache_manager.get_trader_profile(user_id)
    
    if existing:
        # Update existing profile
        existing.username = request.username
        existing.display_name = request.display_name
        existing.bio = request.bio
        existing.allows_copy_trading = request.allows_copy_trading
        existing.copy_trading_fee = request.copy_trading_fee
        existing.min_copy_amount = request.min_copy_amount
        existing.updated_at = datetime.utcnow()
        
        await cache_manager.update_trader_profile(existing)
        profile = existing
    else:
        # Create new profile
        profile = TraderProfile(
            user_id=user_id,
            username=request.username,
            display_name=request.display_name,
            bio=request.bio,
            allows_copy_trading=request.allows_copy_trading,
            copy_trading_fee=request.copy_trading_fee,
            min_copy_amount=request.min_copy_amount
        )
        
        await cache_manager.store_trader_profile(profile)
    
    # Publish event
    await publish_event(
        EventType.TRADER_PROFILE_UPDATED,
        {
            "user_id": user_id,
            "allows_copy_trading": profile.allows_copy_trading,
            "timestamp": datetime.utcnow().isoformat()
        }
    )
    
    return profile


# Social Following

@router.post("/follow/{leader_id}")
async def follow_trader(
    leader_id: str,
    current_user: dict = Depends(get_current_user),
    cache_manager = Depends(get_cache_manager)
):
    """Follow another trader."""
    user_id = current_user["user_id"]
    
    if user_id == leader_id:
        raise HTTPException(status_code=400, detail="Cannot follow yourself")
    
    # Check if leader exists
    leader_profile = await cache_manager.get_trader_profile(leader_id)
    if not leader_profile:
        raise HTTPException(status_code=404, detail="Leader not found")
    
    # Add follow relationship
    await cache_manager.add_follow_relationship(user_id, leader_id)
    
    # Update counts
    follower_profile = await cache_manager.get_trader_profile(user_id)
    if follower_profile:
        follower_profile.following_count += 1
        await cache_manager.update_trader_profile(follower_profile)
    
    leader_profile.followers_count += 1
    await cache_manager.update_trader_profile(leader_profile)
    
    return {"message": f"Now following {leader_profile.username}"}


@router.delete("/follow/{leader_id}")
async def unfollow_trader(
    leader_id: str,
    current_user: dict = Depends(get_current_user),
    cache_manager = Depends(get_cache_manager)
):
    """Unfollow a trader."""
    user_id = current_user["user_id"]
    
    # Remove follow relationship
    await cache_manager.remove_follow_relationship(user_id, leader_id)
    
    # Update counts
    follower_profile = await cache_manager.get_trader_profile(user_id)
    if follower_profile and follower_profile.following_count > 0:
        follower_profile.following_count -= 1
        await cache_manager.update_trader_profile(follower_profile)
    
    leader_profile = await cache_manager.get_trader_profile(leader_id)
    if leader_profile and leader_profile.followers_count > 0:
        leader_profile.followers_count -= 1
        await cache_manager.update_trader_profile(leader_profile)
    
    return {"message": "Unfollowed successfully"}


# Copy Trading

@router.post("/copy/{leader_id}", response_model=CopyTradingRelation)
async def start_copy_trading(
    leader_id: str,
    request: StartCopyTradingRequest,
    current_user: dict = Depends(get_current_user),
    copy_executor = Depends(get_copy_executor),
    cache_manager = Depends(get_cache_manager)
):
    """Start copying a trader."""
    user_id = current_user["user_id"]
    
    if user_id == leader_id:
        raise HTTPException(status_code=400, detail="Cannot copy yourself")
    
    # Check if leader allows copy trading
    leader_profile = await cache_manager.get_trader_profile(leader_id)
    if not leader_profile or not leader_profile.allows_copy_trading:
        raise HTTPException(status_code=400, detail="Leader does not allow copy trading")
    
    # Check minimum copy amount
    if request.copy_mode == CopyMode.FIXED_AMOUNT:
        if not request.allocation_amount or request.allocation_amount < leader_profile.min_copy_amount:
            raise HTTPException(
                status_code=400,
                detail=f"Minimum copy amount is {leader_profile.min_copy_amount}"
            )
    
    # Check if already copying
    existing = await cache_manager.get_copy_relation(user_id, leader_id)
    if existing and existing.is_active:
        raise HTTPException(status_code=400, detail="Already copying this trader")
    
    # Create copy relation
    relation = CopyTradingRelation(
        relation_id=str(uuid.uuid4()),
        leader_id=leader_id,
        follower_id=user_id,
        copy_mode=request.copy_mode,
        allocation_amount=request.allocation_amount,
        allocation_percent=request.allocation_percent,
        max_position_size=request.max_position_size,
        max_daily_trades=request.max_daily_trades,
        stop_loss_percent=request.stop_loss_percent,
        max_drawdown_percent=request.max_drawdown_percent
    )
    
    # Register with executor
    await copy_executor.register_copy_relation(relation)
    
    # Store relation
    await cache_manager.store_copy_relation(relation)
    
    # Publish event
    await publish_event(
        EventType.COPY_TRADING_STARTED,
        {
            "relation_id": relation.relation_id,
            "leader_id": leader_id,
            "follower_id": user_id,
            "copy_mode": relation.copy_mode.value,
            "timestamp": datetime.utcnow().isoformat()
        }
    )
    
    return relation


@router.delete("/copy/{relation_id}")
async def stop_copy_trading(
    relation_id: str,
    current_user: dict = Depends(get_current_user),
    copy_executor = Depends(get_copy_executor),
    cache_manager = Depends(get_cache_manager)
):
    """Stop copying a trader."""
    user_id = current_user["user_id"]
    
    # Get relation
    relation = await cache_manager.get_copy_relation_by_id(relation_id)
    if not relation:
        raise HTTPException(status_code=404, detail="Copy relation not found")
    
    # Check ownership
    if relation.follower_id != user_id:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Unregister from executor
    await copy_executor.unregister_copy_relation(relation_id)
    
    # Update relation
    relation.is_active = False
    relation.updated_at = datetime.utcnow()
    await cache_manager.update_copy_relation(relation)
    
    # Publish event
    await publish_event(
        EventType.COPY_TRADING_STOPPED,
        {
            "relation_id": relation_id,
            "leader_id": relation.leader_id,
            "follower_id": user_id,
            "timestamp": datetime.utcnow().isoformat()
        }
    )
    
    return {"message": "Copy trading stopped"}


@router.get("/copy/active", response_model=List[CopyTradingRelation])
async def get_active_copy_relations(
    current_user: dict = Depends(get_current_user),
    cache_manager = Depends(get_cache_manager)
):
    """Get user's active copy trading relations."""
    return await cache_manager.get_user_copy_relations(current_user["user_id"])


# Social Feed

@router.post("/posts", response_model=SocialPost)
async def create_post(
    request: CreatePostRequest,
    current_user: dict = Depends(get_current_user),
    cache_manager = Depends(get_cache_manager)
):
    """Create a social post."""
    user_id = current_user["user_id"]
    
    # Check daily post limit
    daily_posts = await cache_manager.get_user_daily_posts(user_id)
    if daily_posts >= 50:  # Default max posts per day
        raise HTTPException(status_code=429, detail="Daily post limit exceeded")
    
    # Create post
    post = SocialPost(
        post_id=str(uuid.uuid4()),
        author_id=user_id,
        content=request.content,
        tags=request.tags,
        assets_mentioned=request.assets_mentioned,
        strategy_id=request.strategy_id,
        is_educational=request.is_educational
    )
    
    # Store post
    await cache_manager.store_social_post(post)
    
    # Publish event
    await publish_event(
        EventType.SOCIAL_POST_CREATED,
        {
            "post_id": post.post_id,
            "author_id": user_id,
            "is_educational": post.is_educational,
            "timestamp": datetime.utcnow().isoformat()
        }
    )
    
    return post


@router.get("/feed", response_model=List[SocialPost])
async def get_social_feed(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user: dict = Depends(get_current_user),
    cache_manager = Depends(get_cache_manager)
):
    """Get personalized social feed."""
    user_id = current_user["user_id"]
    
    # Get posts from followed traders
    following = await cache_manager.get_following_list(user_id)
    
    # Include own posts
    following.append(user_id)
    
    # Get recent posts from followed traders
    posts = await cache_manager.get_posts_by_authors(following, limit, offset)
    
    return posts


# Leaderboard

@router.get("/leaderboard", response_model=List[LeaderboardEntry])
async def get_leaderboard(
    period: str = Query("monthly", regex="^(daily|weekly|monthly|all-time)$"),
    limit: int = Query(100, ge=1, le=1000),
    reputation_engine = Depends(get_reputation_engine),
    cache_manager = Depends(get_cache_manager)
):
    """Get trader leaderboard."""
    # Get cached leaderboard
    leaderboard = await cache_manager.get_leaderboard(period, limit)
    
    if not leaderboard:
        # Generate leaderboard
        top_traders = await reputation_engine.get_top_traders(
            limit=limit,
            min_trades=10
        )
        
        entries = []
        for rank, user_id in enumerate(top_traders, 1):
            profile = await cache_manager.get_trader_profile(user_id)
            if profile:
                # Get performance metrics
                metrics = await cache_manager.get_performance_metrics(user_id, period)
                
                entry = LeaderboardEntry(
                    rank=rank,
                    user_id=user_id,
                    username=profile.username,
                    display_name=profile.display_name,
                    total_return=metrics.total_return if metrics else Decimal("0"),
                    sharpe_ratio=metrics.sharpe_ratio if metrics else 0.0,
                    win_rate=metrics.win_rate if metrics else 0.0,
                    reputation_score=profile.reputation_score,
                    followers_count=profile.followers_count,
                    copy_traders_count=await cache_manager.get_copy_trader_count(user_id),
                    is_verified=profile.is_verified,
                    period=period
                )
                entries.append(entry)
        
        # Cache leaderboard
        await cache_manager.cache_leaderboard(period, entries)
        leaderboard = entries
    
    return leaderboard 