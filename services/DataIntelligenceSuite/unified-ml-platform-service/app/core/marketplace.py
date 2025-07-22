"""
Model Marketplace for sharing and discovering ML models
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
import uuid
import logging
from enum import Enum

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func
from prometheus_client import Counter, Gauge

from .config import settings
from ..models.marketplace import MarketplaceModel, ModelRating, ModelUsage
from ..integrations.event_driven_ml import EventDrivenMLIntegration, MLEventType

logger = logging.getLogger(__name__)

# Prometheus metrics
models_published_counter = Counter('ml_marketplace_models_published_total', 'Total models published')
models_downloaded_counter = Counter('ml_marketplace_models_downloaded_total', 'Total model downloads')
marketplace_revenue_gauge = Gauge('ml_marketplace_revenue_total', 'Total marketplace revenue')


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
    OTHER = "other"


class ModelMarketplace:
    """Manages model marketplace operations"""
    
    def __init__(self, db_session: Optional[AsyncSession] = None):
        self.db_session = db_session
        self.event_integration: Optional[EventDrivenMLIntegration] = None
        self._featured_models: List[str] = []
        self._trending_cache: Dict[str, List] = {}
        
    async def initialize(self, event_integration: EventDrivenMLIntegration):
        """Initialize the marketplace"""
        self.event_integration = event_integration
        logger.info("Model Marketplace initialized")
        
    async def publish_model(
        self,
        model_id: str,
        publisher_id: str,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Publish a model to the marketplace"""
        try:
            # Create marketplace entry
            marketplace_model = MarketplaceModel(
                id=str(uuid.uuid4()),
                model_id=model_id,
                publisher_id=publisher_id,
                name=metadata.get("name", f"Model {model_id}"),
                description=metadata.get("description", ""),
                category=metadata.get("category", ModelCategory.OTHER),
                visibility=metadata.get("visibility", ModelVisibility.PRIVATE),
                price=metadata.get("price", 0.0),
                tags=metadata.get("tags", []),
                framework=metadata.get("framework", "unknown"),
                version=metadata.get("version", "1.0.0"),
                metrics=metadata.get("metrics", {}),
                requirements=metadata.get("requirements", {}),
                published_at=datetime.utcnow(),
                is_active=True
            )
            
            if self.db_session:
                self.db_session.add(marketplace_model)
                await self.db_session.commit()
                
            # Update metrics
            models_published_counter.inc()
            
            # Publish event
            if self.event_integration:
                await self.event_integration.publish_event(
                    MLEventType.MODEL_PUBLISHED,
                    {
                        "marketplace_id": marketplace_model.id,
                        "model_id": model_id,
                        "publisher_id": publisher_id,
                        "name": marketplace_model.name,
                        "category": marketplace_model.category,
                        "visibility": marketplace_model.visibility,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
            return {
                "marketplace_id": marketplace_model.id,
                "status": "published",
                "visibility": marketplace_model.visibility,
                "url": f"/marketplace/models/{marketplace_model.id}"
            }
            
        except Exception as e:
            logger.error(f"Error publishing model: {e}")
            raise
            
    async def search_models(
        self,
        query: Optional[str] = None,
        category: Optional[ModelCategory] = None,
        tags: Optional[List[str]] = None,
        min_rating: Optional[float] = None,
        max_price: Optional[float] = None,
        framework: Optional[str] = None,
        sort_by: str = "relevance",
        limit: int = 20,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Search for models in the marketplace"""
        try:
            if not self.db_session:
                return {"models": [], "total": 0}
                
            # Build query
            stmt = select(MarketplaceModel).where(
                and_(
                    MarketplaceModel.is_active == True,
                    MarketplaceModel.visibility.in_([ModelVisibility.PUBLIC, ModelVisibility.ORGANIZATION])
                )
            )
            
            # Apply filters
            if query:
                search_pattern = f"%{query}%"
                stmt = stmt.where(
                    or_(
                        MarketplaceModel.name.ilike(search_pattern),
                        MarketplaceModel.description.ilike(search_pattern),
                        func.array_to_string(MarketplaceModel.tags, ',').ilike(search_pattern)
                    )
                )
                
            if category:
                stmt = stmt.where(MarketplaceModel.category == category)
                
            if tags:
                for tag in tags:
                    stmt = stmt.where(MarketplaceModel.tags.contains([tag]))
                    
            if min_rating is not None:
                stmt = stmt.where(MarketplaceModel.average_rating >= min_rating)
                
            if max_price is not None:
                stmt = stmt.where(MarketplaceModel.price <= max_price)
                
            if framework:
                stmt = stmt.where(MarketplaceModel.framework == framework)
                
            # Apply sorting
            if sort_by == "rating":
                stmt = stmt.order_by(MarketplaceModel.average_rating.desc())
            elif sort_by == "downloads":
                stmt = stmt.order_by(MarketplaceModel.download_count.desc())
            elif sort_by == "recent":
                stmt = stmt.order_by(MarketplaceModel.published_at.desc())
            elif sort_by == "price_low":
                stmt = stmt.order_by(MarketplaceModel.price.asc())
            elif sort_by == "price_high":
                stmt = stmt.order_by(MarketplaceModel.price.desc())
                
            # Get total count
            count_stmt = select(func.count()).select_from(stmt.subquery())
            total_result = await self.db_session.execute(count_stmt)
            total = total_result.scalar()
            
            # Apply pagination
            stmt = stmt.limit(limit).offset(offset)
            
            # Execute query
            result = await self.db_session.execute(stmt)
            models = result.scalars().all()
            
            return {
                "models": [self._model_to_dict(model) for model in models],
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
            if not self.db_session:
                return None
                
            stmt = select(MarketplaceModel).where(
                and_(
                    MarketplaceModel.id == marketplace_id,
                    MarketplaceModel.is_active == True
                )
            )
            
            result = await self.db_session.execute(stmt)
            model = result.scalar_one_or_none()
            
            if not model:
                return None
                
            # Increment view count
            model.view_count += 1
            await self.db_session.commit()
            
            return self._model_to_dict(model, detailed=True)
            
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
            if not self.db_session:
                raise Exception("Database session not available")
                
            # Get model
            stmt = select(MarketplaceModel).where(
                and_(
                    MarketplaceModel.id == marketplace_id,
                    MarketplaceModel.is_active == True
                )
            )
            
            result = await self.db_session.execute(stmt)
            model = result.scalar_one_or_none()
            
            if not model:
                raise Exception("Model not found")
                
            # Check access permissions
            if model.visibility == ModelVisibility.PRIVATE:
                if model.publisher_id != user_id:
                    raise Exception("Access denied")
                    
            # Record download
            usage = ModelUsage(
                id=str(uuid.uuid4()),
                marketplace_model_id=marketplace_id,
                user_id=user_id,
                action="download",
                timestamp=datetime.utcnow()
            )
            
            self.db_session.add(usage)
            
            # Update download count
            model.download_count += 1
            
            # Handle payment if needed
            if model.price > 0:
                # TODO: Implement payment processing
                marketplace_revenue_gauge.inc(model.price)
                
            await self.db_session.commit()
            
            # Update metrics
            models_downloaded_counter.inc()
            
            # Publish event
            if self.event_integration:
                await self.event_integration.publish_event(
                    MLEventType.MODEL_DOWNLOADED,
                    {
                        "marketplace_id": marketplace_id,
                        "model_id": model.model_id,
                        "user_id": user_id,
                        "price": model.price,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                )
                
            return {
                "status": "success",
                "model_id": model.model_id,
                "download_url": f"/models/{model.model_id}/download",
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
            if not self.db_session:
                raise Exception("Database session not available")
                
            # Check if model exists
            model_stmt = select(MarketplaceModel).where(
                MarketplaceModel.id == marketplace_id
            )
            model_result = await self.db_session.execute(model_stmt)
            model = model_result.scalar_one_or_none()
            
            if not model:
                raise Exception("Model not found")
                
            # Check if user already rated
            rating_stmt = select(ModelRating).where(
                and_(
                    ModelRating.marketplace_model_id == marketplace_id,
                    ModelRating.user_id == user_id
                )
            )
            rating_result = await self.db_session.execute(rating_stmt)
            existing_rating = rating_result.scalar_one_or_none()
            
            if existing_rating:
                # Update existing rating
                existing_rating.rating = rating
                existing_rating.review = review
                existing_rating.updated_at = datetime.utcnow()
            else:
                # Create new rating
                new_rating = ModelRating(
                    id=str(uuid.uuid4()),
                    marketplace_model_id=marketplace_id,
                    user_id=user_id,
                    rating=rating,
                    review=review,
                    created_at=datetime.utcnow()
                )
                self.db_session.add(new_rating)
                
            # Update model average rating
            avg_stmt = select(func.avg(ModelRating.rating)).where(
                ModelRating.marketplace_model_id == marketplace_id
            )
            avg_result = await self.db_session.execute(avg_stmt)
            avg_rating = avg_result.scalar()
            
            model.average_rating = float(avg_rating) if avg_rating else 0.0
            model.rating_count = await self._get_rating_count(marketplace_id)
            
            await self.db_session.commit()
            
            return {
                "status": "success",
                "average_rating": model.average_rating,
                "rating_count": model.rating_count
            }
            
        except Exception as e:
            logger.error(f"Error rating model: {e}")
            raise
            
    async def get_trending_models(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get trending models based on recent activity"""
        try:
            # Check cache
            cache_key = f"trending:{limit}"
            if cache_key in self._trending_cache:
                cached_data = self._trending_cache[cache_key]
                if (datetime.utcnow() - cached_data["timestamp"]).seconds < 300:  # 5 min cache
                    return cached_data["models"]
                    
            if not self.db_session:
                return []
                
            # Calculate trending score based on recent downloads and views
            # This is a simplified version - in production, use more sophisticated algorithm
            stmt = select(MarketplaceModel).where(
                and_(
                    MarketplaceModel.is_active == True,
                    MarketplaceModel.visibility == ModelVisibility.PUBLIC
                )
            ).order_by(
                (MarketplaceModel.download_count + MarketplaceModel.view_count * 0.1).desc()
            ).limit(limit)
            
            result = await self.db_session.execute(stmt)
            models = result.scalars().all()
            
            trending = [self._model_to_dict(model) for model in models]
            
            # Cache results
            self._trending_cache[cache_key] = {
                "models": trending,
                "timestamp": datetime.utcnow()
            }
            
            return trending
            
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
            if not self.db_session:
                return []
                
            # Get user's download history
            usage_stmt = select(ModelUsage).where(
                and_(
                    ModelUsage.user_id == user_id,
                    ModelUsage.action == "download"
                )
            ).order_by(ModelUsage.timestamp.desc()).limit(20)
            
            usage_result = await self.db_session.execute(usage_stmt)
            user_downloads = usage_result.scalars().all()
            
            if not user_downloads:
                # Return popular models for new users
                return await self.get_trending_models(limit)
                
            # Get categories and tags from user's downloads
            downloaded_ids = [u.marketplace_model_id for u in user_downloads]
            
            models_stmt = select(MarketplaceModel).where(
                MarketplaceModel.id.in_(downloaded_ids)
            )
            models_result = await self.db_session.execute(models_stmt)
            downloaded_models = models_result.scalars().all()
            
            # Extract preferences
            categories = set()
            tags = set()
            frameworks = set()
            
            for model in downloaded_models:
                categories.add(model.category)
                tags.update(model.tags)
                frameworks.add(model.framework)
                
            # Find similar models
            rec_stmt = select(MarketplaceModel).where(
                and_(
                    MarketplaceModel.is_active == True,
                    MarketplaceModel.visibility == ModelVisibility.PUBLIC,
                    MarketplaceModel.id.notin_(downloaded_ids),
                    or_(
                        MarketplaceModel.category.in_(categories),
                        MarketplaceModel.framework.in_(frameworks),
                        *[MarketplaceModel.tags.contains([tag]) for tag in list(tags)[:5]]
                    )
                )
            ).order_by(
                MarketplaceModel.average_rating.desc(),
                MarketplaceModel.download_count.desc()
            ).limit(limit)
            
            rec_result = await self.db_session.execute(rec_stmt)
            recommendations = rec_result.scalars().all()
            
            return [self._model_to_dict(model) for model in recommendations]
            
        except Exception as e:
            logger.error(f"Error getting recommendations: {e}")
            return []
            
    async def _get_rating_count(self, marketplace_id: str) -> int:
        """Get rating count for a model"""
        if not self.db_session:
            return 0
            
        stmt = select(func.count()).select_from(ModelRating).where(
            ModelRating.marketplace_model_id == marketplace_id
        )
        result = await self.db_session.execute(stmt)
        return result.scalar() or 0
        
    def _model_to_dict(self, model: MarketplaceModel, detailed: bool = False) -> Dict[str, Any]:
        """Convert marketplace model to dictionary"""
        data = {
            "marketplace_id": model.id,
            "model_id": model.model_id,
            "name": model.name,
            "description": model.description,
            "category": model.category,
            "price": model.price,
            "currency": "USD",
            "average_rating": model.average_rating,
            "rating_count": model.rating_count,
            "download_count": model.download_count,
            "tags": model.tags,
            "framework": model.framework,
            "version": model.version,
            "published_at": model.published_at.isoformat(),
            "publisher_id": model.publisher_id
        }
        
        if detailed:
            data.update({
                "metrics": model.metrics,
                "requirements": model.requirements,
                "view_count": model.view_count,
                "updated_at": model.updated_at.isoformat() if model.updated_at else None
            })
            
        return data 