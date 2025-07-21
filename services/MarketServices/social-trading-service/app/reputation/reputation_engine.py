"""Reputation engine for calculating trader trust scores."""

import asyncio
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional
import logging
import numpy as np

from app.config import Settings
from app.models.social import TraderReputation, TraderProfile


logger = logging.getLogger(__name__)


class ReputationEngine:
    """Calculates and manages trader reputation scores."""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self._reputation_cache: Dict[str, TraderReputation] = {}
        self._update_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Weights for reputation components
        self.weights = {
            "performance": 0.4,
            "social": 0.3,
            "trust": 0.3
        }
        
    async def start(self):
        """Start the reputation engine."""
        self._running = True
        self._update_task = asyncio.create_task(self._periodic_update())
        logger.info("Reputation engine started")
        
    async def stop(self):
        """Stop the reputation engine."""
        self._running = False
        if self._update_task:
            self._update_task.cancel()
            try:
                await self._update_task
            except asyncio.CancelledError:
                pass
        logger.info("Reputation engine stopped")
        
    async def _periodic_update(self):
        """Periodically update all trader reputations."""
        while self._running:
            try:
                await self._update_all_reputations()
                await asyncio.sleep(self.settings.reputation_update_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic reputation update: {e}")
                await asyncio.sleep(60)
                
    async def calculate_reputation(
        self,
        user_id: str,
        profile: TraderProfile,
        performance_data: Dict,
        social_data: Dict
    ) -> TraderReputation:
        """Calculate comprehensive reputation score for a trader."""
        
        # Calculate component scores
        performance_scores = await self._calculate_performance_scores(
            user_id, performance_data
        )
        social_scores = await self._calculate_social_scores(
            user_id, profile, social_data
        )
        trust_scores = await self._calculate_trust_scores(
            user_id, profile, performance_data
        )
        
        # Calculate weighted overall score
        overall_score = (
            self.weights["performance"] * np.mean(list(performance_scores.values())) +
            self.weights["social"] * np.mean(list(social_scores.values())) +
            self.weights["trust"] * np.mean(list(trust_scores.values()))
        )
        
        # Apply decay if inactive
        last_trade_date = performance_data.get("last_trade_date")
        if last_trade_date:
            days_inactive = (datetime.utcnow() - last_trade_date).days
            if days_inactive > 30:
                decay_factor = self.settings.reputation_decay_rate ** (days_inactive / 30)
                overall_score *= decay_factor
                
        # Create reputation object
        reputation = TraderReputation(
            user_id=user_id,
            performance_score=performance_scores["overall"],
            consistency_score=performance_scores["consistency"],
            risk_management_score=performance_scores["risk_management"],
            follower_satisfaction=social_scores["follower_satisfaction"],
            community_engagement=social_scores["community_engagement"],
            content_quality=social_scores["content_quality"],
            transparency_score=trust_scores["transparency"],
            reliability_score=trust_scores["reliability"],
            dispute_resolution=trust_scores["dispute_resolution"],
            overall_score=overall_score
        )
        
        # Update cache
        self._reputation_cache[user_id] = reputation
        
        return reputation
        
    async def _calculate_performance_scores(
        self,
        user_id: str,
        performance_data: Dict
    ) -> Dict[str, float]:
        """Calculate performance-based reputation scores."""
        scores = {
            "overall": 0.0,
            "consistency": 0.0,
            "risk_management": 0.0
        }
        
        # Overall performance score based on returns
        total_return = performance_data.get("total_return", 0)
        if total_return > 0.5:  # 50%+ return
            scores["overall"] = 1.0
        elif total_return > 0.2:  # 20%+ return
            scores["overall"] = 0.8
        elif total_return > 0.1:  # 10%+ return
            scores["overall"] = 0.6
        elif total_return > 0:
            scores["overall"] = 0.4
        else:
            scores["overall"] = 0.2
            
        # Consistency score based on Sharpe ratio and win rate
        sharpe_ratio = performance_data.get("sharpe_ratio", 0)
        win_rate = performance_data.get("win_rate", 0.5)
        
        # Sharpe ratio component (0-0.5)
        if sharpe_ratio > 2:
            sharpe_score = 0.5
        elif sharpe_ratio > 1:
            sharpe_score = 0.4
        elif sharpe_ratio > 0.5:
            sharpe_score = 0.3
        elif sharpe_ratio > 0:
            sharpe_score = 0.2
        else:
            sharpe_score = 0.1
            
        # Win rate component (0-0.5)
        win_rate_score = win_rate * 0.5
        
        scores["consistency"] = sharpe_score + win_rate_score
        
        # Risk management score based on max drawdown and risk metrics
        max_drawdown = performance_data.get("max_drawdown", 1.0)
        avg_position_size = performance_data.get("avg_position_size_percent", 1.0)
        
        # Lower drawdown is better
        if max_drawdown < 0.1:  # Less than 10% drawdown
            drawdown_score = 0.5
        elif max_drawdown < 0.2:
            drawdown_score = 0.4
        elif max_drawdown < 0.3:
            drawdown_score = 0.3
        elif max_drawdown < 0.5:
            drawdown_score = 0.2
        else:
            drawdown_score = 0.1
            
        # Conservative position sizing is better
        if avg_position_size < 0.05:  # Less than 5% per position
            position_score = 0.5
        elif avg_position_size < 0.1:
            position_score = 0.4
        elif avg_position_size < 0.2:
            position_score = 0.3
        else:
            position_score = 0.2
            
        scores["risk_management"] = drawdown_score + position_score
        
        return scores
        
    async def _calculate_social_scores(
        self,
        user_id: str,
        profile: TraderProfile,
        social_data: Dict
    ) -> Dict[str, float]:
        """Calculate social-based reputation scores."""
        scores = {
            "follower_satisfaction": 0.0,
            "community_engagement": 0.0,
            "content_quality": 0.0
        }
        
        # Follower satisfaction based on retention and feedback
        follower_retention = social_data.get("follower_retention_rate", 0)
        avg_copy_performance = social_data.get("avg_follower_performance", 0)
        
        retention_score = min(follower_retention, 1.0) * 0.5
        performance_score = min(max(avg_copy_performance + 0.5, 0), 1.0) * 0.5
        
        scores["follower_satisfaction"] = retention_score + performance_score
        
        # Community engagement based on activity
        posts_per_month = social_data.get("posts_per_month", 0)
        avg_post_engagement = social_data.get("avg_post_engagement", 0)
        helpful_votes = social_data.get("helpful_votes", 0)
        
        # Activity score (0-0.3)
        if posts_per_month > 20:
            activity_score = 0.3
        elif posts_per_month > 10:
            activity_score = 0.25
        elif posts_per_month > 5:
            activity_score = 0.2
        elif posts_per_month > 0:
            activity_score = 0.1
        else:
            activity_score = 0
            
        # Engagement score (0-0.4)
        engagement_score = min(avg_post_engagement / 100, 0.4)
        
        # Helpfulness score (0-0.3)
        helpfulness_score = min(helpful_votes / 1000, 0.3)
        
        scores["community_engagement"] = activity_score + engagement_score + helpfulness_score
        
        # Content quality based on reports and moderation
        content_reports = social_data.get("content_reports", 0)
        content_removals = social_data.get("content_removals", 0)
        educational_posts = social_data.get("educational_posts", 0)
        
        # Start with perfect score and deduct for issues
        quality_score = 1.0
        quality_score -= content_reports * 0.05  # -5% per report
        quality_score -= content_removals * 0.2  # -20% per removal
        quality_score = max(quality_score, 0)
        
        # Bonus for educational content
        education_bonus = min(educational_posts * 0.02, 0.2)  # +2% per post, max 20%
        
        scores["content_quality"] = min(quality_score + education_bonus, 1.0)
        
        return scores
        
    async def _calculate_trust_scores(
        self,
        user_id: str,
        profile: TraderProfile,
        performance_data: Dict
    ) -> Dict[str, float]:
        """Calculate trust-based reputation scores."""
        scores = {
            "transparency": 0.0,
            "reliability": 0.0,
            "dispute_resolution": 0.0
        }
        
        # Transparency score based on disclosure and verification
        is_verified = profile.is_verified
        shares_strategy_details = performance_data.get("shares_strategy_details", False)
        regular_updates = performance_data.get("provides_regular_updates", False)
        
        scores["transparency"] = (
            (0.4 if is_verified else 0) +
            (0.3 if shares_strategy_details else 0) +
            (0.3 if regular_updates else 0)
        )
        
        # Reliability score based on consistency and uptime
        trade_frequency_variance = performance_data.get("trade_frequency_variance", 1.0)
        strategy_consistency = performance_data.get("strategy_consistency", 0)
        
        # Lower variance is better
        if trade_frequency_variance < 0.2:
            variance_score = 0.5
        elif trade_frequency_variance < 0.5:
            variance_score = 0.3
        else:
            variance_score = 0.1
            
        scores["reliability"] = variance_score + (strategy_consistency * 0.5)
        
        # Dispute resolution based on history
        total_disputes = performance_data.get("total_disputes", 0)
        resolved_disputes = performance_data.get("resolved_disputes", 0)
        avg_resolution_time = performance_data.get("avg_resolution_time_days", 30)
        
        if total_disputes == 0:
            scores["dispute_resolution"] = 1.0
        else:
            resolution_rate = resolved_disputes / total_disputes
            
            # Time component (faster is better)
            if avg_resolution_time < 1:
                time_score = 0.5
            elif avg_resolution_time < 3:
                time_score = 0.3
            elif avg_resolution_time < 7:
                time_score = 0.2
            else:
                time_score = 0.1
                
            scores["dispute_resolution"] = (resolution_rate * 0.5) + time_score
            
        return scores
        
    async def get_reputation(self, user_id: str) -> Optional[TraderReputation]:
        """Get cached reputation for a trader."""
        return self._reputation_cache.get(user_id)
        
    async def update_reputation_event(
        self,
        user_id: str,
        event_type: str,
        event_data: Dict
    ):
        """Update reputation based on a specific event."""
        reputation = self._reputation_cache.get(user_id)
        if not reputation:
            return
            
        # Apply immediate adjustments based on event type
        if event_type == "trade_success":
            # Small positive adjustment
            reputation.performance_score = min(
                reputation.performance_score + 0.001, 1.0
            )
        elif event_type == "follower_profit":
            # Positive adjustment to follower satisfaction
            reputation.follower_satisfaction = min(
                reputation.follower_satisfaction + 0.005, 1.0
            )
        elif event_type == "content_reported":
            # Negative adjustment to content quality
            reputation.content_quality = max(
                reputation.content_quality - 0.05, 0
            )
        elif event_type == "dispute_filed":
            # Negative adjustment to dispute resolution
            reputation.dispute_resolution = max(
                reputation.dispute_resolution - 0.1, 0
            )
            
        # Recalculate overall score
        reputation.overall_score = (
            self.weights["performance"] * np.mean([
                reputation.performance_score,
                reputation.consistency_score,
                reputation.risk_management_score
            ]) +
            self.weights["social"] * np.mean([
                reputation.follower_satisfaction,
                reputation.community_engagement,
                reputation.content_quality
            ]) +
            self.weights["trust"] * np.mean([
                reputation.transparency_score,
                reputation.reliability_score,
                reputation.dispute_resolution
            ])
        )
        
        reputation.last_calculated = datetime.utcnow()
        
    async def _update_all_reputations(self):
        """Update reputations for all active traders."""
        # In production, this would fetch all active traders
        # and recalculate their reputations
        logger.info("Updating all trader reputations...")
        
    async def get_top_traders(
        self,
        limit: int = 100,
        min_trades: Optional[int] = None
    ) -> List[str]:
        """Get top traders by reputation score."""
        # Filter by minimum trades if specified
        eligible_traders = []
        for user_id, reputation in self._reputation_cache.items():
            if min_trades:
                # In production, check actual trade count
                eligible_traders.append((user_id, reputation.overall_score))
            else:
                eligible_traders.append((user_id, reputation.overall_score))
                
        # Sort by score
        eligible_traders.sort(key=lambda x: x[1], reverse=True)
        
        # Return top user IDs
        return [user_id for user_id, _ in eligible_traders[:limit]] 