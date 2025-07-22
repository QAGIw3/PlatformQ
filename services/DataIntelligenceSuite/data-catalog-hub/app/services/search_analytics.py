"""
Search Analytics Service

Tracks search usage, analyzes patterns, and provides insights
for improving search relevance and user experience.
"""

import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
from collections import defaultdict, Counter
import json
import statistics

from elasticsearch import AsyncElasticsearch
import pandas as pd
import numpy as np
from app.core.config import settings
import redis.asyncio as redis

logger = logging.getLogger(__name__)


class SearchAnalyticsTracker:
    """Tracks search events and user interactions"""
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        redis_client: Optional[redis.Redis] = None
    ):
        self.es_client = es_client
        self.redis_client = redis_client
        self.analytics_index = "search_analytics"
        
        # Real-time counters
        self.search_counter = Counter()
        self.click_counter = Counter()
        self.last_reset = datetime.utcnow()
    
    async def initialize(self):
        """Initialize analytics tracking"""
        # Create analytics index
        mapping = {
            "mappings": {
                "properties": {
                    "event_type": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "user_id": {"type": "keyword"},
                    "session_id": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"},
                    
                    # Search event fields
                    "query": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword"}}
                    },
                    "query_normalized": {"type": "keyword"},
                    "result_count": {"type": "integer"},
                    "response_time_ms": {"type": "integer"},
                    "filters_used": {"type": "object"},
                    "search_type": {"type": "keyword"},
                    
                    # Click event fields
                    "result_id": {"type": "keyword"},
                    "result_type": {"type": "keyword"},
                    "result_position": {"type": "integer"},
                    "click_time_ms": {"type": "integer"},
                    
                    # Context
                    "device_type": {"type": "keyword"},
                    "browser": {"type": "keyword"},
                    "location": {"type": "geo_point"},
                    "referrer": {"type": "keyword"}
                }
            }
        }
        
        if not await self.es_client.indices.exists(index=self.analytics_index):
            await self.es_client.indices.create(
                index=self.analytics_index,
                body=mapping
            )
            logger.info("Created search analytics index")
    
    async def track_search(
        self,
        query: str,
        user_id: Optional[str],
        session_id: str,
        result_count: int,
        response_time_ms: int,
        filters: Optional[Dict[str, Any]] = None,
        search_type: str = "standard",
        context: Optional[Dict[str, Any]] = None
    ):
        """Track a search event"""
        try:
            event = {
                "event_type": "search",
                "timestamp": datetime.utcnow(),
                "user_id": user_id or "anonymous",
                "session_id": session_id,
                "tenant_id": context.get("tenant_id", "default") if context else "default",
                "query": query,
                "query_normalized": self._normalize_query(query),
                "result_count": result_count,
                "response_time_ms": response_time_ms,
                "filters_used": filters or {},
                "search_type": search_type
            }
            
            # Add context if provided
            if context:
                event.update({
                    "device_type": context.get("device_type"),
                    "browser": context.get("browser"),
                    "location": context.get("location"),
                    "referrer": context.get("referrer")
                })
            
            # Index event
            await self.es_client.index(
                index=self.analytics_index,
                body=event
            )
            
            # Update real-time counter
            self.search_counter[query_normalized] += 1
            
            # Track in Redis for real-time dashboard
            if self.redis_client:
                await self._update_redis_analytics("search", query)
            
        except Exception as e:
            logger.error(f"Error tracking search: {e}")
    
    async def track_click(
        self,
        user_id: Optional[str],
        session_id: str,
        query: str,
        result_id: str,
        result_type: str,
        result_position: int,
        click_time_ms: int,
        context: Optional[Dict[str, Any]] = None
    ):
        """Track a click event"""
        try:
            event = {
                "event_type": "click",
                "timestamp": datetime.utcnow(),
                "user_id": user_id or "anonymous",
                "session_id": session_id,
                "tenant_id": context.get("tenant_id", "default") if context else "default",
                "query": query,
                "query_normalized": self._normalize_query(query),
                "result_id": result_id,
                "result_type": result_type,
                "result_position": result_position,
                "click_time_ms": click_time_ms
            }
            
            # Add context if provided
            if context:
                event.update({
                    "device_type": context.get("device_type"),
                    "browser": context.get("browser"),
                    "location": context.get("location")
                })
            
            # Index event
            await self.es_client.index(
                index=self.analytics_index,
                body=event
            )
            
            # Update real-time counter
            click_key = f"{query_normalized}:{result_type}"
            self.click_counter[click_key] += 1
            
            # Track in Redis
            if self.redis_client:
                await self._update_redis_analytics("click", query, result_id)
            
        except Exception as e:
            logger.error(f"Error tracking click: {e}")
    
    def _normalize_query(self, query: str) -> str:
        """Normalize query for grouping"""
        return query.lower().strip()
    
    async def _update_redis_analytics(
        self,
        event_type: str,
        query: str,
        result_id: Optional[str] = None
    ):
        """Update Redis with real-time analytics"""
        try:
            # Popular searches (sliding window)
            if event_type == "search":
                key = "analytics:popular_searches"
                await self.redis_client.zincrby(key, 1, query)
                await self.redis_client.expire(key, 3600)  # 1 hour window
            
            # Click-through tracking
            elif event_type == "click" and result_id:
                key = f"analytics:ctr:{self._normalize_query(query)}"
                await self.redis_client.hincrby(key, result_id, 1)
                await self.redis_client.expire(key, 86400)  # 24 hour window
            
        except Exception as e:
            logger.error(f"Error updating Redis analytics: {e}")


class SearchAnalyticsAnalyzer:
    """Analyzes search patterns and generates insights"""
    
    def __init__(
        self,
        es_client: AsyncElasticsearch,
        redis_client: Optional[redis.Redis] = None
    ):
        self.es_client = es_client
        self.redis_client = redis_client
        self.analytics_index = "search_analytics"
    
    async def get_search_metrics(
        self,
        time_range: str = "24h",
        tenant_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get comprehensive search metrics"""
        try:
            # Parse time range
            now = datetime.utcnow()
            if time_range == "24h":
                start_time = now - timedelta(hours=24)
            elif time_range == "7d":
                start_time = now - timedelta(days=7)
            elif time_range == "30d":
                start_time = now - timedelta(days=30)
            else:
                start_time = now - timedelta(hours=24)
            
            # Build query
            query = {
                "bool": {
                    "filter": [
                        {"range": {"timestamp": {"gte": start_time}}},
                        {"term": {"event_type": "search"}}
                    ]
                }
            }
            
            if tenant_id:
                query["bool"]["filter"].append({"term": {"tenant_id": tenant_id}})
            
            # Get aggregations
            aggs = {
                "total_searches": {"value_count": {"field": "query.keyword"}},
                "unique_queries": {"cardinality": {"field": "query_normalized"}},
                "unique_users": {"cardinality": {"field": "user_id"}},
                "avg_response_time": {"avg": {"field": "response_time_ms"}},
                "avg_result_count": {"avg": {"field": "result_count"}},
                "zero_results": {
                    "filter": {"term": {"result_count": 0}},
                    "aggs": {
                        "count": {"value_count": {"field": "query.keyword"}}
                    }
                },
                "searches_over_time": {
                    "date_histogram": {
                        "field": "timestamp",
                        "calendar_interval": "1h" if time_range == "24h" else "1d"
                    }
                },
                "top_queries": {
                    "terms": {
                        "field": "query_normalized",
                        "size": 20
                    }
                },
                "search_types": {
                    "terms": {"field": "search_type"}
                }
            }
            
            # Execute query
            response = await self.es_client.search(
                index=self.analytics_index,
                body={
                    "query": query,
                    "aggs": aggs,
                    "size": 0
                }
            )
            
            # Extract metrics
            agg_results = response["aggregations"]
            
            metrics = {
                "time_range": time_range,
                "total_searches": agg_results["total_searches"]["value"],
                "unique_queries": agg_results["unique_queries"]["value"],
                "unique_users": agg_results["unique_users"]["value"],
                "avg_response_time_ms": round(agg_results["avg_response_time"]["value"] or 0),
                "avg_result_count": round(agg_results["avg_result_count"]["value"] or 0),
                "zero_result_searches": agg_results["zero_results"]["count"]["value"],
                "zero_result_rate": (
                    agg_results["zero_results"]["count"]["value"] /
                    agg_results["total_searches"]["value"]
                    if agg_results["total_searches"]["value"] > 0 else 0
                ),
                "searches_timeline": [
                    {
                        "timestamp": bucket["key_as_string"],
                        "count": bucket["doc_count"]
                    }
                    for bucket in agg_results["searches_over_time"]["buckets"]
                ],
                "top_queries": [
                    {
                        "query": bucket["key"],
                        "count": bucket["doc_count"]
                    }
                    for bucket in agg_results["top_queries"]["buckets"]
                ],
                "search_type_distribution": {
                    bucket["key"]: bucket["doc_count"]
                    for bucket in agg_results["search_types"]["buckets"]
                }
            }
            
            # Add real-time data if available
            if self.redis_client:
                metrics["real_time"] = await self._get_realtime_metrics()
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error getting search metrics: {e}")
            return {}
    
    async def get_click_through_rates(
        self,
        time_range: str = "24h",
        min_searches: int = 10
    ) -> List[Dict[str, Any]]:
        """Calculate click-through rates for queries"""
        try:
            # Get time range
            now = datetime.utcnow()
            if time_range == "24h":
                start_time = now - timedelta(hours=24)
            elif time_range == "7d":
                start_time = now - timedelta(days=7)
            else:
                start_time = now - timedelta(hours=24)
            
            # Get search counts
            search_query = {
                "bool": {
                    "filter": [
                        {"range": {"timestamp": {"gte": start_time}}},
                        {"term": {"event_type": "search"}}
                    ]
                }
            }
            
            search_aggs = {
                "queries": {
                    "terms": {
                        "field": "query_normalized",
                        "size": 1000,
                        "min_doc_count": min_searches
                    }
                }
            }
            
            search_response = await self.es_client.search(
                index=self.analytics_index,
                body={
                    "query": search_query,
                    "aggs": search_aggs,
                    "size": 0
                }
            )
            
            # Get click counts for each query
            ctr_data = []
            
            for bucket in search_response["aggregations"]["queries"]["buckets"]:
                query = bucket["key"]
                search_count = bucket["doc_count"]
                
                # Get clicks for this query
                click_query = {
                    "bool": {
                        "filter": [
                            {"range": {"timestamp": {"gte": start_time}}},
                            {"term": {"event_type": "click"}},
                            {"term": {"query_normalized": query}}
                        ]
                    }
                }
                
                click_response = await self.es_client.count(
                    index=self.analytics_index,
                    body={"query": click_query}
                )
                
                click_count = click_response["count"]
                ctr = click_count / search_count if search_count > 0 else 0
                
                ctr_data.append({
                    "query": query,
                    "searches": search_count,
                    "clicks": click_count,
                    "ctr": round(ctr, 4),
                    "ctr_percent": round(ctr * 100, 2)
                })
            
            # Sort by CTR (ascending to find problematic queries)
            ctr_data.sort(key=lambda x: x["ctr"])
            
            return ctr_data
            
        except Exception as e:
            logger.error(f"Error calculating CTR: {e}")
            return []
    
    async def get_query_performance(
        self,
        query: str
    ) -> Dict[str, Any]:
        """Get detailed performance metrics for a specific query"""
        try:
            normalized_query = query.lower().strip()
            
            # Get search events
            search_query = {
                "bool": {
                    "filter": [
                        {"term": {"event_type": "search"}},
                        {"term": {"query_normalized": normalized_query}}
                    ]
                }
            }
            
            # Get aggregations
            aggs = {
                "total_searches": {"value_count": {"field": "query.keyword"}},
                "avg_response_time": {"avg": {"field": "response_time_ms"}},
                "avg_result_count": {"avg": {"field": "result_count"}},
                "response_time_percentiles": {
                    "percentiles": {
                        "field": "response_time_ms",
                        "percents": [50, 90, 95, 99]
                    }
                },
                "result_count_histogram": {
                    "histogram": {
                        "field": "result_count",
                        "interval": 10
                    }
                },
                "searches_over_time": {
                    "date_histogram": {
                        "field": "timestamp",
                        "calendar_interval": "1d"
                    }
                }
            }
            
            search_response = await self.es_client.search(
                index=self.analytics_index,
                body={
                    "query": search_query,
                    "aggs": aggs,
                    "size": 0
                }
            )
            
            # Get click data
            click_query = {
                "bool": {
                    "filter": [
                        {"term": {"event_type": "click"}},
                        {"term": {"query_normalized": normalized_query}}
                    ]
                }
            }
            
            click_aggs = {
                "total_clicks": {"value_count": {"field": "result_id"}},
                "avg_click_position": {"avg": {"field": "result_position"}},
                "clicked_results": {
                    "terms": {
                        "field": "result_id",
                        "size": 20
                    },
                    "aggs": {
                        "result_type": {
                            "terms": {"field": "result_type"}
                        }
                    }
                },
                "click_position_distribution": {
                    "histogram": {
                        "field": "result_position",
                        "interval": 1
                    }
                }
            }
            
            click_response = await self.es_client.search(
                index=self.analytics_index,
                body={
                    "query": click_query,
                    "aggs": click_aggs,
                    "size": 0
                }
            )
            
            # Compile performance data
            search_aggs = search_response["aggregations"]
            click_aggs = click_response["aggregations"]
            
            total_searches = search_aggs["total_searches"]["value"]
            total_clicks = click_aggs["total_clicks"]["value"]
            
            performance = {
                "query": query,
                "total_searches": total_searches,
                "total_clicks": total_clicks,
                "ctr": round(total_clicks / total_searches if total_searches > 0 else 0, 4),
                "avg_response_time_ms": round(search_aggs["avg_response_time"]["value"] or 0),
                "response_time_percentiles": {
                    f"p{int(k)}": round(v or 0)
                    for k, v in search_aggs["response_time_percentiles"]["values"].items()
                },
                "avg_result_count": round(search_aggs["avg_result_count"]["value"] or 0),
                "avg_click_position": round(click_aggs["avg_click_position"]["value"] or 0, 1),
                "result_distribution": [
                    {
                        "result_count": bucket["key"],
                        "searches": bucket["doc_count"]
                    }
                    for bucket in search_aggs["result_count_histogram"]["buckets"]
                ],
                "click_position_distribution": [
                    {
                        "position": bucket["key"],
                        "clicks": bucket["doc_count"]
                    }
                    for bucket in click_aggs["click_position_distribution"]["buckets"]
                ],
                "top_clicked_results": [
                    {
                        "result_id": bucket["key"],
                        "clicks": bucket["doc_count"],
                        "types": [
                            type_bucket["key"]
                            for type_bucket in bucket["result_type"]["buckets"]
                        ]
                    }
                    for bucket in click_aggs["clicked_results"]["buckets"]
                ],
                "timeline": [
                    {
                        "date": bucket["key_as_string"],
                        "searches": bucket["doc_count"]
                    }
                    for bucket in search_aggs["searches_over_time"]["buckets"]
                ]
            }
            
            return performance
            
        except Exception as e:
            logger.error(f"Error getting query performance: {e}")
            return {}
    
    async def get_search_trends(
        self,
        time_range: str = "7d",
        trend_threshold: float = 0.5
    ) -> Dict[str, Any]:
        """Identify trending searches"""
        try:
            now = datetime.utcnow()
            
            if time_range == "7d":
                current_start = now - timedelta(days=3.5)
                previous_start = now - timedelta(days=7)
                previous_end = current_start
            elif time_range == "30d":
                current_start = now - timedelta(days=15)
                previous_start = now - timedelta(days=30)
                previous_end = current_start
            else:
                current_start = now - timedelta(hours=12)
                previous_start = now - timedelta(hours=24)
                previous_end = current_start
            
            # Get current period queries
            current_query = {
                "bool": {
                    "filter": [
                        {"range": {"timestamp": {"gte": current_start}}},
                        {"term": {"event_type": "search"}}
                    ]
                }
            }
            
            current_response = await self.es_client.search(
                index=self.analytics_index,
                body={
                    "query": current_query,
                    "aggs": {
                        "queries": {
                            "terms": {
                                "field": "query_normalized",
                                "size": 100
                            }
                        }
                    },
                    "size": 0
                }
            )
            
            # Get previous period queries
            previous_query = {
                "bool": {
                    "filter": [
                        {"range": {"timestamp": {"gte": previous_start, "lt": previous_end}}},
                        {"term": {"event_type": "search"}}
                    ]
                }
            }
            
            previous_response = await self.es_client.search(
                index=self.analytics_index,
                body={
                    "query": previous_query,
                    "aggs": {
                        "queries": {
                            "terms": {
                                "field": "query_normalized",
                                "size": 100
                            }
                        }
                    },
                    "size": 0
                }
            )
            
            # Build query counts
            current_counts = {
                bucket["key"]: bucket["doc_count"]
                for bucket in current_response["aggregations"]["queries"]["buckets"]
            }
            
            previous_counts = {
                bucket["key"]: bucket["doc_count"]
                for bucket in previous_response["aggregations"]["queries"]["buckets"]
            }
            
            # Calculate trends
            trending_up = []
            trending_down = []
            new_queries = []
            
            # Check current queries
            for query, current_count in current_counts.items():
                previous_count = previous_counts.get(query, 0)
                
                if previous_count == 0:
                    new_queries.append({
                        "query": query,
                        "count": current_count
                    })
                else:
                    change_rate = (current_count - previous_count) / previous_count
                    
                    if change_rate >= trend_threshold:
                        trending_up.append({
                            "query": query,
                            "current_count": current_count,
                            "previous_count": previous_count,
                            "change_rate": round(change_rate, 2),
                            "change_percent": round(change_rate * 100, 1)
                        })
                    elif change_rate <= -trend_threshold:
                        trending_down.append({
                            "query": query,
                            "current_count": current_count,
                            "previous_count": previous_count,
                            "change_rate": round(change_rate, 2),
                            "change_percent": round(change_rate * 100, 1)
                        })
            
            # Sort by change magnitude
            trending_up.sort(key=lambda x: x["change_rate"], reverse=True)
            trending_down.sort(key=lambda x: x["change_rate"])
            new_queries.sort(key=lambda x: x["count"], reverse=True)
            
            return {
                "time_range": time_range,
                "trending_up": trending_up[:10],
                "trending_down": trending_down[:10],
                "new_queries": new_queries[:10],
                "summary": {
                    "total_trending_up": len(trending_up),
                    "total_trending_down": len(trending_down),
                    "total_new": len(new_queries)
                }
            }
            
        except Exception as e:
            logger.error(f"Error getting search trends: {e}")
            return {}
    
    async def _get_realtime_metrics(self) -> Dict[str, Any]:
        """Get real-time metrics from Redis"""
        try:
            if not self.redis_client:
                return {}
            
            # Get popular searches
            popular_searches_raw = await self.redis_client.zrevrange(
                "analytics:popular_searches",
                0,
                9,
                withscores=True
            )
            
            popular_searches = [
                {
                    "query": query.decode() if isinstance(query, bytes) else query,
                    "count": int(score)
                }
                for query, score in popular_searches_raw
            ]
            
            return {
                "popular_searches_1h": popular_searches
            }
            
        except Exception as e:
            logger.error(f"Error getting realtime metrics: {e}")
            return {}


class SearchInsightsGenerator:
    """Generates actionable insights from search analytics"""
    
    def __init__(self, analyzer: SearchAnalyticsAnalyzer):
        self.analyzer = analyzer
    
    async def generate_insights(
        self,
        time_range: str = "7d"
    ) -> List[Dict[str, Any]]:
        """Generate comprehensive search insights"""
        insights = []
        
        try:
            # Get metrics
            metrics = await self.analyzer.get_search_metrics(time_range)
            ctr_data = await self.analyzer.get_click_through_rates(time_range)
            trends = await self.analyzer.get_search_trends(time_range)
            
            # Analyze zero result rate
            if metrics.get("zero_result_rate", 0) > 0.1:
                insights.append({
                    "type": "high_zero_results",
                    "severity": "high",
                    "title": "High Zero Result Rate",
                    "description": f"{metrics['zero_result_rate']*100:.1f}% of searches return no results",
                    "recommendation": "Review zero-result queries and add relevant content or improve search relevance",
                    "impact": "user_experience"
                })
            
            # Analyze CTR
            if ctr_data:
                low_ctr_queries = [q for q in ctr_data if q["ctr"] < 0.1 and q["searches"] > 20]
                if low_ctr_queries:
                    insights.append({
                        "type": "low_ctr_queries",
                        "severity": "medium",
                        "title": "Queries with Low Click-Through Rate",
                        "description": f"{len(low_ctr_queries)} popular queries have CTR below 10%",
                        "recommendation": "Improve result relevance for these queries or review result presentation",
                        "impact": "relevance",
                        "queries": low_ctr_queries[:5]  # Top 5
                    })
            
            # Analyze response time
            if metrics.get("avg_response_time_ms", 0) > 500:
                insights.append({
                    "type": "slow_response",
                    "severity": "medium",
                    "title": "Slow Search Response Time",
                    "description": f"Average response time is {metrics['avg_response_time_ms']}ms",
                    "recommendation": "Optimize search queries, add caching, or scale search infrastructure",
                    "impact": "performance"
                })
            
            # Analyze trends
            if trends.get("trending_up"):
                insights.append({
                    "type": "trending_searches",
                    "severity": "info",
                    "title": "Trending Search Topics",
                    "description": f"{len(trends['trending_up'])} queries are trending up",
                    "recommendation": "Ensure adequate content for trending topics",
                    "impact": "opportunity",
                    "queries": trends["trending_up"][:5]
                })
            
            # Analyze search diversity
            if metrics.get("unique_queries", 0) < metrics.get("total_searches", 0) * 0.1:
                insights.append({
                    "type": "low_query_diversity",
                    "severity": "low",
                    "title": "Low Search Query Diversity",
                    "description": "Users are searching for a limited set of queries",
                    "recommendation": "Consider adding search suggestions or related content discovery features",
                    "impact": "discovery"
                })
            
            return insights
            
        except Exception as e:
            logger.error(f"Error generating insights: {e}")
            return []
    
    async def generate_optimization_recommendations(
        self
    ) -> List[Dict[str, Any]]:
        """Generate specific optimization recommendations"""
        recommendations = []
        
        try:
            # Get performance data
            metrics = await self.analyzer.get_search_metrics("24h")
            
            # Check if semantic search would help
            if metrics.get("zero_result_rate", 0) > 0.05:
                recommendations.append({
                    "action": "enable_semantic_search",
                    "reason": "High zero result rate suggests exact matching is too restrictive",
                    "expected_impact": "Reduce zero results by 30-50%",
                    "priority": "high"
                })
            
            # Check if personalization would help
            unique_users = metrics.get("unique_users", 0)
            total_searches = metrics.get("total_searches", 0)
            
            if unique_users > 0 and total_searches / unique_users > 10:
                recommendations.append({
                    "action": "enable_personalization",
                    "reason": "High search frequency per user suggests personalization opportunity",
                    "expected_impact": "Improve CTR by 15-25%",
                    "priority": "medium"
                })
            
            # Check if caching would help
            unique_queries = metrics.get("unique_queries", 0)
            
            if total_searches > 0 and unique_queries / total_searches < 0.3:
                recommendations.append({
                    "action": "implement_result_caching",
                    "reason": "Many repeated queries could benefit from caching",
                    "expected_impact": "Reduce response time by 40-60%",
                    "priority": "high"
                })
            
            # Check search type distribution
            search_types = metrics.get("search_type_distribution", {})
            if "standard" in search_types and search_types["standard"] > total_searches * 0.9:
                recommendations.append({
                    "action": "promote_advanced_search",
                    "reason": "Users aren't utilizing advanced search features",
                    "expected_impact": "Improve search precision by 20-30%",
                    "priority": "low"
                })
            
            return recommendations
            
        except Exception as e:
            logger.error(f"Error generating recommendations: {e}")
            return [] 