"""
Access Patterns Analytics

Tracks and analyzes how data assets are accessed to optimize
catalog organization, caching, and recommendations.
"""

import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, Counter
import asyncio
import json
import statistics

import httpx
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

from app.core.atlas_client import AtlasClient
from app.core.config import settings

logger = logging.getLogger(__name__)


class AccessType(str, Enum):
    """Types of data access"""
    VIEW = "view"
    QUERY = "query"
    DOWNLOAD = "download"
    API_CALL = "api_call"
    LINEAGE_TRACE = "lineage_trace"
    SCHEMA_INSPECT = "schema_inspect"
    QUALITY_CHECK = "quality_check"


class AccessPattern(str, Enum):
    """Common access patterns"""
    EXPLORATORY = "exploratory"        # Random browsing
    TARGETED = "targeted"              # Direct access to known assets
    ANALYTICAL = "analytical"          # Complex queries and analysis
    OPERATIONAL = "operational"        # Regular scheduled access
    DEVELOPMENT = "development"        # Schema inspection, test queries


@dataclass
class AccessEvent:
    """Represents a data access event"""
    timestamp: datetime
    user_id: str
    asset_id: str
    asset_type: str
    access_type: AccessType
    duration_ms: int
    query: Optional[str] = None
    source_app: Optional[str] = None
    session_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UserProfile:
    """User access profile"""
    user_id: str
    primary_pattern: AccessPattern
    frequent_assets: List[Tuple[str, int]]  # (asset_id, count)
    access_times: List[int]  # Hour of day distribution
    avg_session_duration: float
    preferred_access_type: AccessType
    team: Optional[str] = None
    role: Optional[str] = None


@dataclass
class AssetAccessMetrics:
    """Access metrics for a data asset"""
    asset_id: str
    total_accesses: int
    unique_users: int
    access_frequency: Dict[str, int]  # By time period
    popular_queries: List[Tuple[str, int]]  # (query, count)
    access_patterns: Dict[AccessPattern, int]
    avg_access_duration: float
    peak_hours: List[int]
    related_assets: List[Tuple[str, float]]  # (asset_id, correlation)
    churn_rate: float  # Users who accessed once and never returned


class AccessAnalyticsEngine:
    """
    Analyzes data access patterns to provide insights and optimizations
    """
    
    def __init__(
        self,
        atlas_client: AtlasClient,
        analytics_backend_url: Optional[str] = None
    ):
        self.atlas_client = atlas_client
        self.analytics_backend_url = analytics_backend_url or settings.analytics_backend_url
        self.http_client = httpx.AsyncClient(timeout=30.0)
        
        # Analytics configuration
        self.retention_days = 90
        self.cache_hot_threshold = 10  # Accesses per day
        self.pattern_window = timedelta(days=7)
        
        # In-memory buffers for real-time analysis
        self.recent_events = []
        self.event_buffer_size = 10000
        
    async def track_access(
        self,
        user_id: str,
        asset_id: str,
        access_type: AccessType,
        duration_ms: int,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Track a data access event
        """
        try:
            event = AccessEvent(
                timestamp=datetime.utcnow(),
                user_id=user_id,
                asset_id=asset_id,
                asset_type=await self._get_asset_type(asset_id),
                access_type=access_type,
                duration_ms=duration_ms,
                query=metadata.get("query") if metadata else None,
                source_app=metadata.get("source_app") if metadata else None,
                session_id=metadata.get("session_id") if metadata else None,
                metadata=metadata or {}
            )
            
            # Add to buffer
            self.recent_events.append(event)
            if len(self.recent_events) > self.event_buffer_size:
                self.recent_events.pop(0)
            
            # Send to analytics backend
            await self._send_to_backend(event)
            
            # Update real-time metrics
            await self._update_real_time_metrics(event)
            
            return True
            
        except Exception as e:
            logger.error(f"Error tracking access: {e}")
            return False
    
    async def _get_asset_type(self, asset_id: str) -> str:
        """
        Get asset type from catalog
        """
        try:
            entity = await self.atlas_client.get_entity(asset_id)
            return entity.get("typeName", "unknown")
        except:
            return "unknown"
    
    async def _send_to_backend(self, event: AccessEvent):
        """
        Send event to analytics backend
        """
        try:
            await self.http_client.post(
                f"{self.analytics_backend_url}/api/v1/events/access",
                json={
                    "timestamp": event.timestamp.isoformat(),
                    "user_id": event.user_id,
                    "asset_id": event.asset_id,
                    "asset_type": event.asset_type,
                    "access_type": event.access_type.value,
                    "duration_ms": event.duration_ms,
                    "query": event.query,
                    "source_app": event.source_app,
                    "session_id": event.session_id,
                    "metadata": event.metadata
                }
            )
        except Exception as e:
            logger.debug(f"Failed to send to backend: {e}")
    
    async def _update_real_time_metrics(self, event: AccessEvent):
        """
        Update real-time access metrics
        """
        # This would update Redis or similar for real-time dashboards
        pass
    
    async def analyze_user_patterns(
        self,
        user_id: str,
        time_range_days: int = 30
    ) -> UserProfile:
        """
        Analyze access patterns for a specific user
        """
        try:
            # Get user's access history
            events = await self._get_user_events(user_id, time_range_days)
            
            if not events:
                return self._create_default_profile(user_id)
            
            # Analyze patterns
            pattern = self._identify_user_pattern(events)
            
            # Calculate frequent assets
            asset_counter = Counter(e.asset_id for e in events)
            frequent_assets = asset_counter.most_common(10)
            
            # Access time distribution
            access_hours = [e.timestamp.hour for e in events]
            
            # Session analysis
            sessions = self._group_into_sessions(events)
            avg_duration = statistics.mean(
                [s[-1].timestamp - s[0].timestamp for s in sessions]
            ).total_seconds() / 60  # minutes
            
            # Preferred access type
            access_type_counter = Counter(e.access_type for e in events)
            preferred_type = access_type_counter.most_common(1)[0][0]
            
            return UserProfile(
                user_id=user_id,
                primary_pattern=pattern,
                frequent_assets=frequent_assets,
                access_times=access_hours,
                avg_session_duration=avg_duration,
                preferred_access_type=preferred_type,
                team=await self._get_user_team(user_id),
                role=await self._get_user_role(user_id)
            )
            
        except Exception as e:
            logger.error(f"Error analyzing user patterns: {e}")
            return self._create_default_profile(user_id)
    
    def _identify_user_pattern(self, events: List[AccessEvent]) -> AccessPattern:
        """
        Identify user's primary access pattern
        """
        # Feature extraction
        features = []
        
        # 1. Access diversity (unique assets / total accesses)
        unique_assets = len(set(e.asset_id for e in events))
        diversity = unique_assets / len(events)
        features.append(diversity)
        
        # 2. Query complexity (for query events)
        query_events = [e for e in events if e.query]
        avg_query_length = statistics.mean(
            [len(e.query) for e in query_events]
        ) if query_events else 0
        features.append(avg_query_length / 100)  # Normalize
        
        # 3. Access regularity (standard deviation of inter-access times)
        if len(events) > 1:
            inter_times = [
                (events[i+1].timestamp - events[i].timestamp).total_seconds()
                for i in range(len(events)-1)
            ]
            regularity = statistics.stdev(inter_times) if len(inter_times) > 1 else 0
            features.append(1 / (1 + regularity/3600))  # Normalize
        else:
            features.append(0)
        
        # 4. Schema inspection ratio
        schema_ratio = len([e for e in events if e.access_type == AccessType.SCHEMA_INSPECT]) / len(events)
        features.append(schema_ratio)
        
        # Pattern classification based on features
        if diversity > 0.8:
            return AccessPattern.EXPLORATORY
        elif schema_ratio > 0.3:
            return AccessPattern.DEVELOPMENT
        elif features[2] > 0.7:  # High regularity
            return AccessPattern.OPERATIONAL
        elif avg_query_length > 50:
            return AccessPattern.ANALYTICAL
        else:
            return AccessPattern.TARGETED
    
    def _group_into_sessions(
        self,
        events: List[AccessEvent],
        gap_minutes: int = 30
    ) -> List[List[AccessEvent]]:
        """
        Group events into sessions
        """
        if not events:
            return []
        
        events_sorted = sorted(events, key=lambda e: e.timestamp)
        sessions = []
        current_session = [events_sorted[0]]
        
        for i in range(1, len(events_sorted)):
            time_gap = (events_sorted[i].timestamp - events_sorted[i-1].timestamp).total_seconds() / 60
            
            if time_gap <= gap_minutes:
                current_session.append(events_sorted[i])
            else:
                sessions.append(current_session)
                current_session = [events_sorted[i]]
        
        if current_session:
            sessions.append(current_session)
        
        return sessions
    
    async def analyze_asset_access(
        self,
        asset_id: str,
        time_range_days: int = 30
    ) -> AssetAccessMetrics:
        """
        Analyze access patterns for a specific asset
        """
        try:
            # Get asset's access history
            events = await self._get_asset_events(asset_id, time_range_days)
            
            if not events:
                return self._create_default_metrics(asset_id)
            
            # Basic metrics
            total_accesses = len(events)
            unique_users = len(set(e.user_id for e in events))
            
            # Access frequency by day
            daily_counts = defaultdict(int)
            for event in events:
                day_key = event.timestamp.date().isoformat()
                daily_counts[day_key] += 1
            
            # Popular queries
            query_counter = Counter(e.query for e in events if e.query)
            popular_queries = query_counter.most_common(10)
            
            # Access patterns distribution
            pattern_counts = defaultdict(int)
            for user_id in set(e.user_id for e in events):
                user_events = [e for e in events if e.user_id == user_id]
                pattern = self._identify_user_pattern(user_events)
                pattern_counts[pattern] += 1
            
            # Average duration
            durations = [e.duration_ms for e in events if e.duration_ms > 0]
            avg_duration = statistics.mean(durations) if durations else 0
            
            # Peak hours
            hour_counts = Counter(e.timestamp.hour for e in events)
            peak_hours = [hour for hour, _ in hour_counts.most_common(3)]
            
            # Related assets (co-accessed)
            related = await self._find_related_assets(asset_id, events)
            
            # Churn rate
            user_access_counts = Counter(e.user_id for e in events)
            one_time_users = len([u for u, c in user_access_counts.items() if c == 1])
            churn_rate = one_time_users / unique_users if unique_users > 0 else 0
            
            return AssetAccessMetrics(
                asset_id=asset_id,
                total_accesses=total_accesses,
                unique_users=unique_users,
                access_frequency=dict(daily_counts),
                popular_queries=popular_queries,
                access_patterns=dict(pattern_counts),
                avg_access_duration=avg_duration,
                peak_hours=peak_hours,
                related_assets=related,
                churn_rate=churn_rate
            )
            
        except Exception as e:
            logger.error(f"Error analyzing asset access: {e}")
            return self._create_default_metrics(asset_id)
    
    async def _find_related_assets(
        self,
        asset_id: str,
        events: List[AccessEvent]
    ) -> List[Tuple[str, float]]:
        """
        Find assets commonly accessed together
        """
        related_counts = defaultdict(int)
        
        # Group by session
        user_sessions = defaultdict(list)
        for event in events:
            if event.session_id:
                user_sessions[event.session_id].append(event)
        
        # Find co-accessed assets
        for session_events in user_sessions.values():
            session_assets = set(e.asset_id for e in session_events)
            session_assets.discard(asset_id)
            
            for other_asset in session_assets:
                related_counts[other_asset] += 1
        
        # Calculate correlation scores
        total_sessions = len(user_sessions)
        related_assets = []
        
        for other_asset, count in related_counts.items():
            if count >= 2:  # Minimum threshold
                correlation = count / total_sessions
                related_assets.append((other_asset, correlation))
        
        # Sort by correlation
        related_assets.sort(key=lambda x: x[1], reverse=True)
        
        return related_assets[:10]  # Top 10
    
    async def identify_hot_assets(
        self,
        time_window_hours: int = 24,
        min_accesses: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Identify frequently accessed "hot" assets for caching
        """
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=time_window_hours)
            
            # Get recent access events
            recent_events = await self._get_recent_events(cutoff_time)
            
            # Count accesses per asset
            asset_counts = Counter(e.asset_id for e in recent_events)
            
            # Filter hot assets
            hot_assets = []
            for asset_id, count in asset_counts.items():
                if count >= min_accesses:
                    # Get asset details
                    try:
                        entity = await self.atlas_client.get_entity(asset_id)
                        hot_assets.append({
                            "asset_id": asset_id,
                            "name": entity.get("attributes", {}).get("name"),
                            "type": entity.get("typeName"),
                            "access_count": count,
                            "cache_priority": self._calculate_cache_priority(
                                count,
                                entity.get("attributes", {}).get("sizeBytes", 0)
                            )
                        })
                    except:
                        pass
            
            # Sort by cache priority
            hot_assets.sort(key=lambda x: x["cache_priority"], reverse=True)
            
            return hot_assets
            
        except Exception as e:
            logger.error(f"Error identifying hot assets: {e}")
            return []
    
    def _calculate_cache_priority(self, access_count: int, size_bytes: int) -> float:
        """
        Calculate cache priority based on access frequency and size
        """
        # Favor frequently accessed, smaller datasets
        size_mb = size_bytes / (1024 * 1024)
        if size_mb == 0:
            size_mb = 1
        
        return access_count / (1 + np.log(size_mb))
    
    async def predict_future_access(
        self,
        asset_id: str,
        days_ahead: int = 7
    ) -> Dict[str, Any]:
        """
        Predict future access patterns for an asset
        """
        try:
            # Get historical data
            historical_days = 30
            events = await self._get_asset_events(asset_id, historical_days)
            
            if len(events) < 10:
                return {
                    "asset_id": asset_id,
                    "prediction_confidence": "low",
                    "message": "Insufficient historical data"
                }
            
            # Extract time series
            daily_counts = defaultdict(int)
            for event in events:
                day = event.timestamp.date()
                daily_counts[day] += 1
            
            # Simple time series analysis
            counts = []
            for i in range(historical_days):
                day = datetime.utcnow().date() - timedelta(days=i)
                counts.append(daily_counts.get(day, 0))
            
            counts.reverse()  # Chronological order
            
            # Calculate trend
            if len(counts) > 7:
                recent_avg = statistics.mean(counts[-7:])
                older_avg = statistics.mean(counts[:-7])
                trend = "increasing" if recent_avg > older_avg else "decreasing"
            else:
                trend = "stable"
            
            # Simple prediction (moving average)
            predicted_daily = statistics.mean(counts[-7:]) if len(counts) >= 7 else statistics.mean(counts)
            predicted_total = int(predicted_daily * days_ahead)
            
            # Identify patterns
            weekday_counts = defaultdict(list)
            for event in events:
                weekday = event.timestamp.weekday()
                weekday_counts[weekday].append(1)
            
            busy_days = []
            for day, day_counts in weekday_counts.items():
                if len(day_counts) > historical_days / 7 * 1.5:  # Above average
                    busy_days.append(day)
            
            return {
                "asset_id": asset_id,
                "prediction_confidence": "medium" if len(events) > 50 else "low",
                "predicted_accesses": predicted_total,
                "trend": trend,
                "busy_weekdays": busy_days,
                "recommendations": self._generate_access_recommendations(
                    trend,
                    predicted_daily,
                    busy_days
                )
            }
            
        except Exception as e:
            logger.error(f"Error predicting access: {e}")
            return {
                "asset_id": asset_id,
                "error": "Prediction failed"
            }
    
    def _generate_access_recommendations(
        self,
        trend: str,
        predicted_daily: float,
        busy_days: List[int]
    ) -> List[str]:
        """
        Generate recommendations based on predictions
        """
        recommendations = []
        
        if trend == "increasing" and predicted_daily > 20:
            recommendations.append("Consider caching this dataset")
            recommendations.append("Optimize query performance")
        
        if busy_days:
            day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            busy_day_names = [day_names[d] for d in busy_days]
            recommendations.append(
                f"Schedule maintenance outside {', '.join(busy_day_names)}"
            )
        
        if predicted_daily < 1:
            recommendations.append("Consider archiving if unused")
        
        return recommendations
    
    async def generate_optimization_report(
        self,
        scope: str = "global"  # global, team, user
    ) -> Dict[str, Any]:
        """
        Generate comprehensive optimization report
        """
        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "scope": scope,
            "summary": {},
            "recommendations": [],
            "hot_assets": [],
            "cold_assets": [],
            "access_patterns": {},
            "user_segments": []
        }
        
        try:
            # Get hot assets
            report["hot_assets"] = await self.identify_hot_assets()
            
            # Get cold assets (rarely accessed)
            cold_assets = await self._identify_cold_assets()
            report["cold_assets"] = cold_assets
            
            # Analyze access patterns
            patterns = await self._analyze_global_patterns()
            report["access_patterns"] = patterns
            
            # User segmentation
            segments = await self._segment_users()
            report["user_segments"] = segments
            
            # Generate summary
            report["summary"] = {
                "total_assets_tracked": len(report["hot_assets"]) + len(report["cold_assets"]),
                "cache_candidates": len([a for a in report["hot_assets"] if a["cache_priority"] > 10]),
                "archive_candidates": len([a for a in report["cold_assets"] if a["days_since_access"] > 60]),
                "dominant_pattern": max(patterns.items(), key=lambda x: x[1])[0] if patterns else "unknown"
            }
            
            # Generate recommendations
            if report["summary"]["cache_candidates"] > 0:
                report["recommendations"].append({
                    "type": "caching",
                    "priority": "high",
                    "action": f"Cache {report['summary']['cache_candidates']} frequently accessed datasets",
                    "impact": "Reduce access latency by 50-80%"
                })
            
            if report["summary"]["archive_candidates"] > 0:
                report["recommendations"].append({
                    "type": "archival",
                    "priority": "medium",
                    "action": f"Archive {report['summary']['archive_candidates']} inactive datasets",
                    "impact": "Reduce storage costs"
                })
            
            # Pattern-based recommendations
            if patterns.get(AccessPattern.EXPLORATORY, 0) > 0.3:
                report["recommendations"].append({
                    "type": "discovery",
                    "priority": "medium",
                    "action": "Improve search and discovery features",
                    "impact": "Reduce time to find relevant data"
                })
            
            return report
            
        except Exception as e:
            logger.error(f"Error generating optimization report: {e}")
            return report
    
    async def _identify_cold_assets(self, threshold_days: int = 30) -> List[Dict[str, Any]]:
        """
        Identify rarely accessed assets
        """
        # This would query the analytics backend
        # Simplified for illustration
        return []
    
    async def _analyze_global_patterns(self) -> Dict[AccessPattern, float]:
        """
        Analyze global access patterns distribution
        """
        # This would aggregate from all users
        # Simplified for illustration
        return {
            AccessPattern.TARGETED: 0.4,
            AccessPattern.EXPLORATORY: 0.3,
            AccessPattern.OPERATIONAL: 0.2,
            AccessPattern.ANALYTICAL: 0.08,
            AccessPattern.DEVELOPMENT: 0.02
        }
    
    async def _segment_users(self) -> List[Dict[str, Any]]:
        """
        Segment users based on access behavior
        """
        # This would use clustering on user features
        # Simplified for illustration
        return [
            {
                "segment": "Power Users",
                "size": 50,
                "characteristics": ["High frequency", "Complex queries", "Multiple datasets"],
                "recommendations": ["Provide advanced tools", "Priority support"]
            },
            {
                "segment": "Regular Users",
                "size": 200,
                "characteristics": ["Moderate frequency", "Known datasets", "Standard queries"],
                "recommendations": ["Improve documentation", "Suggest related data"]
            }
        ]
    
    async def _get_user_events(
        self,
        user_id: str,
        days: int
    ) -> List[AccessEvent]:
        """
        Get user's access events
        """
        # This would query the analytics backend
        # Using buffer for illustration
        cutoff = datetime.utcnow() - timedelta(days=days)
        return [
            e for e in self.recent_events
            if e.user_id == user_id and e.timestamp >= cutoff
        ]
    
    async def _get_asset_events(
        self,
        asset_id: str,
        days: int
    ) -> List[AccessEvent]:
        """
        Get asset's access events
        """
        # This would query the analytics backend
        # Using buffer for illustration
        cutoff = datetime.utcnow() - timedelta(days=days)
        return [
            e for e in self.recent_events
            if e.asset_id == asset_id and e.timestamp >= cutoff
        ]
    
    async def _get_recent_events(
        self,
        since: datetime
    ) -> List[AccessEvent]:
        """
        Get recent events since timestamp
        """
        # This would query the analytics backend
        # Using buffer for illustration
        return [
            e for e in self.recent_events
            if e.timestamp >= since
        ]
    
    async def _get_user_team(self, user_id: str) -> Optional[str]:
        """
        Get user's team from auth service
        """
        # This would call auth service
        return "data-team"
    
    async def _get_user_role(self, user_id: str) -> Optional[str]:
        """
        Get user's role from auth service
        """
        # This would call auth service
        return "analyst"
    
    def _create_default_profile(self, user_id: str) -> UserProfile:
        """
        Create default user profile
        """
        return UserProfile(
            user_id=user_id,
            primary_pattern=AccessPattern.TARGETED,
            frequent_assets=[],
            access_times=[],
            avg_session_duration=0.0,
            preferred_access_type=AccessType.VIEW
        )
    
    def _create_default_metrics(self, asset_id: str) -> AssetAccessMetrics:
        """
        Create default asset metrics
        """
        return AssetAccessMetrics(
            asset_id=asset_id,
            total_accesses=0,
            unique_users=0,
            access_frequency={},
            popular_queries=[],
            access_patterns={},
            avg_access_duration=0.0,
            peak_hours=[],
            related_assets=[],
            churn_rate=0.0
        )
    
    async def cleanup(self):
        """
        Cleanup resources
        """
        await self.http_client.aclose() 