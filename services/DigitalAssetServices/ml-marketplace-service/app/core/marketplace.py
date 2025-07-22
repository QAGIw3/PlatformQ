"""
ML Model Marketplace for sharing and discovering ML models
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
import uuid
import logging
from enum import Enum
import json

from pyignite import Client as IgniteClient
from pulsar import Client as PulsarClient, Producer, Consumer, ConsumerType

logger = logging.getLogger(__name__)


class ModelVisibility(str, Enum):
    """Model visibility levels"""
    PRIVATE = "private"
    ORGANIZATION = "organization"
    PUBLIC = "public"


class ModelCategory(str, Enum):
    """Model categories"""
    COMPUTER_VISION = "computer_vision"
    NLP = "nlp"
    TABULAR = "tabular"
    TIME_SERIES = "time_series"
    REINFORCEMENT_LEARNING = "reinforcement_learning"
    GENERATIVE = "generative"
    ANOMALY_DETECTION = "anomaly_detection"
    OTHER = "other"


class ModelLicense(str, Enum):
    """Model licenses"""
    MIT = "mit"
    APACHE_2 = "apache_2.0"
    GPL_3 = "gpl_3.0"
    BSD_3 = "bsd_3_clause"
    PROPRIETARY = "proprietary"
    CUSTOM = "custom"


class ModelMarketplace:
    """Manages model marketplace operations"""
    
    def __init__(
        self,
        ignite_host: str = "ignite",
        ignite_port: int = 10800,
        pulsar_url: str = "pulsar://pulsar:6650"
    ):
        # Initialize connections
        self.ignite_client = IgniteClient()
        self.ignite_client.connect(ignite_host, ignite_port)
        
        self.pulsar_client = PulsarClient(pulsar_url)
        self._init_pulsar_topics()
        
        # Initialize caches
        self._init_ignite_caches()
        
        # In-memory caches
        self._featured_models: List[str] = []
        self._trending_cache: Dict[str, Any] = {}
        
        # Background tasks
        self._running = True
        self._background_tasks = []
        
    def _init_ignite_caches(self):
        """Initialize Ignite caches"""
        # Model listings cache
        self.models_cache = self.ignite_client.get_or_create_cache(
            "marketplace_models"
        )
        
        # Model ratings cache
        self.ratings_cache = self.ignite_client.get_or_create_cache(
            "model_ratings"
        )
        
        # Model usage/downloads cache
        self.usage_cache = self.ignite_client.get_or_create_cache(
            "model_usage"
        )
        
        # User purchases cache
        self.purchases_cache = self.ignite_client.get_or_create_cache(
            "user_purchases"
        )
        
        # Search index cache
        self.search_cache = self.ignite_client.get_or_create_cache(
            "model_search_index"
        )
        
        logger.info("Initialized Ignite caches for marketplace")
        
    def _init_pulsar_topics(self):
        """Initialize Pulsar topics"""
        self.model_published_topic = "persistent://public/default/model-published"
        self.model_downloaded_topic = "persistent://public/default/model-downloaded"
        self.model_rated_topic = "persistent://public/default/model-rated"
        
        # Create producers
        self.event_producer = self.pulsar_client.create_producer(
            self.model_published_topic,
            producer_name="marketplace-event-producer"
        )
        
    async def initialize(self):
        """Initialize the marketplace"""
        # Start background tasks
        task = asyncio.create_task(self._update_trending_models())
        self._background_tasks.append(task)
        
        logger.info("ML Marketplace initialized")
        
    async def publish_model(
        self,
        model_id: str,
        publisher_id: str,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Publish a model to the marketplace"""
        try:
            marketplace_id = str(uuid.uuid4())
            
            # Create marketplace entry
            model_data = {
                "marketplace_id": marketplace_id,
                "model_id": model_id,
                "publisher_id": publisher_id,
                "name": metadata.get("name", f"Model {model_id}"),
                "description": metadata.get("description", ""),
                "category": metadata.get("category", ModelCategory.OTHER.value),
                "visibility": metadata.get("visibility", ModelVisibility.PRIVATE.value),
                "license": metadata.get("license", ModelLicense.PROPRIETARY.value),
                "price": metadata.get("price", 0.0),
                "currency": metadata.get("currency", "USD"),
                "tags": metadata.get("tags", []),
                "framework": metadata.get("framework", "unknown"),
                "version": metadata.get("version", "1.0.0"),
                "metrics": metadata.get("metrics", {}),
                "requirements": metadata.get("requirements", {}),
                "documentation_url": metadata.get("documentation_url", ""),
                "repository_url": metadata.get("repository_url", ""),
                "published_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat(),
                "is_active": True,
                "download_count": 0,
                "view_count": 0,
                "average_rating": 0.0,
                "rating_count": 0
            }
            
            # Store in cache
            self.models_cache.put(marketplace_id, model_data)
            
            # Update search index
            await self._update_search_index(marketplace_id, model_data)
            
            # Publish event
            event_data = {
                "marketplace_id": marketplace_id,
                "model_id": model_id,
                "publisher_id": publisher_id,
                "name": model_data["name"],
                "category": model_data["category"],
                "visibility": model_data["visibility"],
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.event_producer.send_async(
                json.dumps(event_data).encode('utf-8'),
                properties={"event_type": "model_published"}
            )
            
            logger.info(f"Published model {marketplace_id} to marketplace")
            
            return {
                "marketplace_id": marketplace_id,
                "status": "published",
                "visibility": model_data["visibility"],
                "url": f"/marketplace/models/{marketplace_id}"
            }
            
        except Exception as e:
            logger.error(f"Error publishing model: {e}")
            raise
            
    async def search_models(
        self,
        query: Optional[str] = None,
        category: Optional[str] = None,
        tags: Optional[List[str]] = None,
        min_rating: Optional[float] = None,
        max_price: Optional[float] = None,
        framework: Optional[str] = None,
        license: Optional[str] = None,
        sort_by: str = "relevance",
        limit: int = 20,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Search for models in the marketplace"""
        try:
            results = []
            
            # Get all models (in production, use proper indexing)
            for key, model_data in self.models_cache.scan():
                if not model_data.get("is_active", False):
                    continue
                    
                # Apply visibility filter
                if model_data["visibility"] == ModelVisibility.PRIVATE.value:
                    continue
                    
                # Apply filters
                if category and model_data["category"] != category:
                    continue
                    
                if framework and model_data["framework"] != framework:
                    continue
                    
                if license and model_data["license"] != license:
                    continue
                    
                if min_rating and model_data["average_rating"] < min_rating:
                    continue
                    
                if max_price and model_data["price"] > max_price:
                    continue
                    
                if tags:
                    model_tags = set(model_data.get("tags", []))
                    if not any(tag in model_tags for tag in tags):
                        continue
                        
                # Text search
                if query:
                    query_lower = query.lower()
                    searchable = [
                        model_data.get("name", "").lower(),
                        model_data.get("description", "").lower(),
                        " ".join(model_data.get("tags", [])).lower()
                    ]
                    if not any(query_lower in text for text in searchable):
                        continue
                        
                # Calculate relevance score
                score = self._calculate_relevance_score(model_data, query, tags)
                results.append((score, model_data))
                
            # Sort results
            if sort_by == "relevance":
                results.sort(key=lambda x: x[0], reverse=True)
            elif sort_by == "rating":
                results.sort(key=lambda x: x[1]["average_rating"], reverse=True)
            elif sort_by == "downloads":
                results.sort(key=lambda x: x[1]["download_count"], reverse=True)
            elif sort_by == "recent":
                results.sort(key=lambda x: x[1]["published_at"], reverse=True)
            elif sort_by == "price_low":
                results.sort(key=lambda x: x[1]["price"])
            elif sort_by == "price_high":
                results.sort(key=lambda x: x[1]["price"], reverse=True)
                
            # Apply pagination
            total = len(results)
            paginated = results[offset:offset + limit]
            
            return {
                "models": [self._format_model_summary(model[1]) for model in paginated],
                "total": total,
                "limit": limit,
                "offset": offset
            }
            
        except Exception as e:
            logger.error(f"Error searching models: {e}")
            raise
            
    async def get_model_details(self, marketplace_id: str) -> Optional[Dict[str, Any]]:
        """Get detailed information about a marketplace model"""
        try:
            model_data = self.models_cache.get(marketplace_id)
            
            if not model_data or not model_data.get("is_active", False):
                return None
                
            # Increment view count
            model_data["view_count"] = model_data.get("view_count", 0) + 1
            self.models_cache.put(marketplace_id, model_data)
            
            # Get ratings
            ratings = self._get_model_ratings(marketplace_id)
            
            # Format response
            details = self._format_model_details(model_data)
            details["ratings"] = ratings
            
            return details
            
        except Exception as e:
            logger.error(f"Error getting model details: {e}")
            raise
            
    async def download_model(
        self,
        marketplace_id: str,
        user_id: str
    ) -> Dict[str, Any]:
        """Download a model from the marketplace"""
        try:
            model_data = self.models_cache.get(marketplace_id)
            
            if not model_data or not model_data.get("is_active", False):
                raise ValueError("Model not found")
                
            # Check access permissions
            if model_data["visibility"] == ModelVisibility.PRIVATE.value:
                if model_data["publisher_id"] != user_id:
                    raise ValueError("Access denied")
                    
            # Check if purchase is required
            if model_data["price"] > 0:
                purchase_key = f"{user_id}:{marketplace_id}"
                purchased = self.purchases_cache.get(purchase_key)
                
                if not purchased:
                    return {
                        "status": "payment_required",
                        "price": model_data["price"],
                        "currency": model_data["currency"],
                        "payment_url": f"/marketplace/models/{marketplace_id}/purchase"
                    }
                    
            # Record download
            usage_id = str(uuid.uuid4())
            usage_data = {
                "usage_id": usage_id,
                "marketplace_id": marketplace_id,
                "user_id": user_id,
                "action": "download",
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.usage_cache.put(usage_id, usage_data)
            
            # Update download count
            model_data["download_count"] = model_data.get("download_count", 0) + 1
            self.models_cache.put(marketplace_id, model_data)
            
            # Publish event
            event_data = {
                "marketplace_id": marketplace_id,
                "model_id": model_data["model_id"],
                "user_id": user_id,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.event_producer.send_async(
                json.dumps(event_data).encode('utf-8'),
                properties={"event_type": "model_downloaded"}
            )
            
            return {
                "status": "success",
                "model_id": model_data["model_id"],
                "download_url": f"/models/{model_data['model_id']}/download",
                "expires_in": 3600  # 1 hour
            }
            
        except Exception as e:
            logger.error(f"Error downloading model: {e}")
            raise
            
    async def rate_model(
        self,
        marketplace_id: str,
        user_id: str,
        rating: int,
        review: Optional[str] = None
    ) -> Dict[str, Any]:
        """Rate a marketplace model"""
        try:
            if not 1 <= rating <= 5:
                raise ValueError("Rating must be between 1 and 5")
                
            model_data = self.models_cache.get(marketplace_id)
            
            if not model_data:
                raise ValueError("Model not found")
                
            # Store rating
            rating_key = f"{marketplace_id}:{user_id}"
            rating_data = {
                "marketplace_id": marketplace_id,
                "user_id": user_id,
                "rating": rating,
                "review": review,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }
            
            # Check if updating existing rating
            existing = self.ratings_cache.get(rating_key)
            if existing:
                rating_data["created_at"] = existing["created_at"]
                
            self.ratings_cache.put(rating_key, rating_data)
            
            # Update average rating
            all_ratings = self._get_model_ratings(marketplace_id)
            if all_ratings:
                avg_rating = sum(r["rating"] for r in all_ratings) / len(all_ratings)
                model_data["average_rating"] = avg_rating
                model_data["rating_count"] = len(all_ratings)
                self.models_cache.put(marketplace_id, model_data)
                
            # Publish event
            event_data = {
                "marketplace_id": marketplace_id,
                "user_id": user_id,
                "rating": rating,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            self.event_producer.send_async(
                json.dumps(event_data).encode('utf-8'),
                properties={"event_type": "model_rated"}
            )
            
            return {
                "status": "success",
                "average_rating": model_data["average_rating"],
                "rating_count": model_data["rating_count"]
            }
            
        except Exception as e:
            logger.error(f"Error rating model: {e}")
            raise
            
    async def get_trending_models(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending models based on recent activity"""
        try:
            # Check cache
            if "trending" in self._trending_cache:
                cached = self._trending_cache["trending"]
                if (datetime.utcnow() - cached["timestamp"]).seconds < 300:  # 5 min cache
                    return cached["models"][:limit]
                    
            # Calculate trending score
            trending = []
            
            for key, model_data in self.models_cache.scan():
                if not model_data.get("is_active", False):
                    continue
                    
                if model_data["visibility"] != ModelVisibility.PUBLIC.value:
                    continue
                    
                # Simple trending score based on recent activity
                score = (
                    model_data.get("download_count", 0) * 2 +
                    model_data.get("view_count", 0) * 0.1 +
                    model_data.get("average_rating", 0) * 10 +
                    model_data.get("rating_count", 0) * 0.5
                )
                
                # Boost recent models
                published_date = datetime.fromisoformat(model_data["published_at"])
                days_old = (datetime.utcnow() - published_date).days
                if days_old < 7:
                    score *= 1.5
                elif days_old < 30:
                    score *= 1.2
                    
                trending.append((score, model_data))
                
            # Sort by score
            trending.sort(key=lambda x: x[0], reverse=True)
            
            # Format results
            results = [self._format_model_summary(model[1]) for model in trending[:limit]]
            
            # Cache results
            self._trending_cache["trending"] = {
                "models": results,
                "timestamp": datetime.utcnow()
            }
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting trending models: {e}")
            return []
            
    async def get_recommendations(
        self,
        user_id: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get model recommendations for a user"""
        try:
            # Get user's download history
            user_downloads = []
            
            for key, usage_data in self.usage_cache.scan():
                if usage_data["user_id"] == user_id and usage_data["action"] == "download":
                    user_downloads.append(usage_data["marketplace_id"])
                    
            if not user_downloads:
                # Return trending for new users
                return await self.get_trending_models(limit)
                
            # Get categories and tags from downloaded models
            categories = set()
            tags = set()
            frameworks = set()
            
            for marketplace_id in user_downloads[-20:]:  # Last 20 downloads
                model_data = self.models_cache.get(marketplace_id)
                if model_data:
                    categories.add(model_data["category"])
                    tags.update(model_data.get("tags", []))
                    frameworks.add(model_data["framework"])
                    
            # Find similar models
            recommendations = []
            
            for key, model_data in self.models_cache.scan():
                if not model_data.get("is_active", False):
                    continue
                    
                if model_data["visibility"] != ModelVisibility.PUBLIC.value:
                    continue
                    
                if model_data["marketplace_id"] in user_downloads:
                    continue
                    
                # Calculate similarity score
                score = 0
                
                if model_data["category"] in categories:
                    score += 10
                    
                if model_data["framework"] in frameworks:
                    score += 5
                    
                model_tags = set(model_data.get("tags", []))
                common_tags = tags.intersection(model_tags)
                score += len(common_tags) * 2
                
                # Boost by quality
                score += model_data.get("average_rating", 0) * 2
                score += min(model_data.get("download_count", 0) / 100, 10)
                
                if score > 0:
                    recommendations.append((score, model_data))
                    
            # Sort by score
            recommendations.sort(key=lambda x: x[0], reverse=True)
            
            # Format results
            return [self._format_model_summary(model[1]) for model in recommendations[:limit]]
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            return []
            
    def _calculate_relevance_score(
        self,
        model_data: Dict[str, Any],
        query: Optional[str],
        tags: Optional[List[str]]
    ) -> float:
        """Calculate relevance score for search results"""
        score = 0.0
        
        if query:
            query_lower = query.lower()
            
            # Name match
            if query_lower in model_data.get("name", "").lower():
                score += 10
                
            # Description match
            if query_lower in model_data.get("description", "").lower():
                score += 5
                
            # Tag match
            for tag in model_data.get("tags", []):
                if query_lower in tag.lower():
                    score += 3
                    
        if tags:
            model_tags = set(model_data.get("tags", []))
            common_tags = set(tags).intersection(model_tags)
            score += len(common_tags) * 2
            
        # Quality factors
        score += model_data.get("average_rating", 0)
        score += min(model_data.get("download_count", 0) / 1000, 5)
        
        return score
        
    def _get_model_ratings(self, marketplace_id: str) -> List[Dict[str, Any]]:
        """Get all ratings for a model"""
        ratings = []
        
        for key, rating_data in self.ratings_cache.scan():
            if rating_data["marketplace_id"] == marketplace_id:
                ratings.append(rating_data)
                
        return ratings
        
    async def _update_search_index(self, marketplace_id: str, model_data: Dict[str, Any]):
        """Update search index for a model"""
        # In production, use proper search engine like Elasticsearch
        search_text = " ".join([
            model_data.get("name", ""),
            model_data.get("description", ""),
            " ".join(model_data.get("tags", [])),
            model_data.get("category", ""),
            model_data.get("framework", "")
        ]).lower()
        
        self.search_cache.put(marketplace_id, {
            "marketplace_id": marketplace_id,
            "search_text": search_text,
            "category": model_data["category"],
            "framework": model_data["framework"],
            "tags": model_data.get("tags", [])
        })
        
    def _format_model_summary(self, model_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format model data for summary view"""
        return {
            "marketplace_id": model_data["marketplace_id"],
            "name": model_data["name"],
            "description": model_data["description"][:200] + "..." if len(model_data["description"]) > 200 else model_data["description"],
            "category": model_data["category"],
            "price": model_data["price"],
            "currency": model_data["currency"],
            "average_rating": model_data["average_rating"],
            "rating_count": model_data["rating_count"],
            "download_count": model_data["download_count"],
            "tags": model_data["tags"][:5],  # First 5 tags
            "framework": model_data["framework"],
            "publisher_id": model_data["publisher_id"]
        }
        
    def _format_model_details(self, model_data: Dict[str, Any]) -> Dict[str, Any]:
        """Format model data for detailed view"""
        return {
            **model_data,
            "created_days_ago": (datetime.utcnow() - datetime.fromisoformat(model_data["published_at"])).days
        }
        
    async def _update_trending_models(self):
        """Background task to update trending models"""
        while self._running:
            try:
                await asyncio.sleep(300)  # Update every 5 minutes
                await self.get_trending_models()  # This will update the cache
                
            except Exception as e:
                logger.error(f"Error updating trending models: {e}")
                await asyncio.sleep(60)
                
    def close(self):
        """Clean up resources"""
        self._running = False
        
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
            
        # Close connections
        self.ignite_client.close()
        self.pulsar_client.close()
        
        logger.info("Marketplace closed") 