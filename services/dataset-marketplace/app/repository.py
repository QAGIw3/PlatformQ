"""
Dataset Marketplace Repository

Handles database operations for dataset listings and transactions.
"""

from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from sqlalchemy import select, and_, or_, func, desc
from sqlalchemy.orm import Session
import json

from platformq_shared.repository import BaseRepository, AsyncBaseRepository
from .models import (
    DatasetListing,
    DatasetPurchase,
    DatasetReview,
    DatasetAnalytics,
    DatasetQualityCheck,
    DatasetType,
    LicenseType,
    DatasetStatus,
    PurchaseStatus
)


class DatasetListingRepository(BaseRepository[DatasetListing]):
    """Repository for dataset listings"""
    
    def __init__(self, db: Session):
        super().__init__(DatasetListing, db)
    
    def search_datasets(
        self,
        query: Optional[str] = None,
        dataset_type: Optional[DatasetType] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        min_samples: Optional[int] = None,
        license_type: Optional[LicenseType] = None,
        tags: Optional[List[str]] = None,
        tenant_id: Optional[str] = None,
        sort_by: str = "created_at",
        limit: int = 50,
        offset: int = 0
    ) -> List[DatasetListing]:
        """Search datasets with filters"""
        q = self.db.query(DatasetListing).filter(
            DatasetListing.status == DatasetStatus.ACTIVE
        )
        
        # Text search
        if query:
            q = q.filter(
                or_(
                    DatasetListing.name.ilike(f"%{query}%"),
                    DatasetListing.description.ilike(f"%{query}%")
                )
            )
        
        # Filters
        if dataset_type:
            q = q.filter(DatasetListing.dataset_type == dataset_type)
        
        if min_price is not None:
            q = q.filter(DatasetListing.price >= min_price)
        
        if max_price is not None:
            q = q.filter(DatasetListing.price <= max_price)
        
        if min_samples:
            q = q.filter(DatasetListing.num_samples >= min_samples)
        
        if license_type:
            q = q.filter(DatasetListing.license_type == license_type)
        
        if tenant_id:
            q = q.filter(DatasetListing.tenant_id == tenant_id)
        
        if tags:
            # Filter by tags (all tags must be present)
            for tag in tags:
                q = q.filter(DatasetListing.tags.contains([tag]))
        
        # Sorting
        if sort_by == "price_asc":
            q = q.order_by(DatasetListing.price)
        elif sort_by == "price_desc":
            q = q.order_by(desc(DatasetListing.price))
        elif sort_by == "rating":
            q = q.order_by(desc(DatasetListing.rating))
        elif sort_by == "popularity":
            q = q.order_by(desc(DatasetListing.licenses_sold))
        else:
            q = q.order_by(desc(DatasetListing.created_at))
        
        return q.offset(offset).limit(limit).all()
    
    def get_by_listing_id(self, listing_id: str) -> Optional[DatasetListing]:
        """Get dataset by listing ID"""
        return self.db.query(DatasetListing).filter(
            DatasetListing.listing_id == listing_id
        ).first()
    
    def get_seller_datasets(self, seller_id: str, tenant_id: str) -> List[DatasetListing]:
        """Get all datasets from a seller"""
        return self.db.query(DatasetListing).filter(
            and_(
                DatasetListing.seller_id == seller_id,
                DatasetListing.tenant_id == tenant_id
            )
        ).order_by(desc(DatasetListing.created_at)).all()
    
    def increment_view_count(self, listing_id: str):
        """Increment view count for a dataset"""
        listing = self.get_by_listing_id(listing_id)
        if listing:
            listing.view_count += 1
            self.db.commit()
    
    def update_rating(self, listing_id: str):
        """Update average rating based on reviews"""
        avg_rating = self.db.query(
            func.avg(DatasetReview.rating)
        ).filter(
            DatasetReview.listing_id == listing_id
        ).scalar()
        
        count = self.db.query(
            func.count(DatasetReview.id)
        ).filter(
            DatasetReview.listing_id == listing_id
        ).scalar()
        
        listing = self.get_by_listing_id(listing_id)
        if listing:
            listing.rating = avg_rating or 0
            listing.rating_count = count
            self.db.commit()
    
    def get_trending_datasets(self, days: int = 7, limit: int = 10) -> List[DatasetListing]:
        """Get trending datasets based on recent activity"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # Get datasets with most purchases in recent days
        trending = self.db.query(
            DatasetListing,
            func.count(DatasetPurchase.id).label('recent_purchases')
        ).join(
            DatasetPurchase,
            DatasetListing.listing_id == DatasetPurchase.listing_id
        ).filter(
            and_(
                DatasetListing.status == DatasetStatus.ACTIVE,
                DatasetPurchase.created_at >= cutoff_date,
                DatasetPurchase.status == PurchaseStatus.COMPLETED
            )
        ).group_by(
            DatasetListing.id
        ).order_by(
            desc('recent_purchases')
        ).limit(limit).all()
        
        return [item[0] for item in trending]


class DatasetPurchaseRepository(BaseRepository[DatasetPurchase]):
    """Repository for dataset purchases"""
    
    def __init__(self, db: Session):
        super().__init__(DatasetPurchase, db)
    
    def get_by_purchase_id(self, purchase_id: str) -> Optional[DatasetPurchase]:
        """Get purchase by ID"""
        return self.db.query(DatasetPurchase).filter(
            DatasetPurchase.purchase_id == purchase_id
        ).first()
    
    def get_by_access_token(self, access_token: str) -> Optional[DatasetPurchase]:
        """Get purchase by access token"""
        return self.db.query(DatasetPurchase).filter(
            DatasetPurchase.access_token == access_token
        ).first()
    
    def get_buyer_purchases(
        self, 
        buyer_id: str, 
        tenant_id: str,
        status: Optional[PurchaseStatus] = None
    ) -> List[DatasetPurchase]:
        """Get all purchases by a buyer"""
        q = self.db.query(DatasetPurchase).filter(
            and_(
                DatasetPurchase.buyer_id == buyer_id,
                DatasetPurchase.tenant_id == tenant_id
            )
        )
        
        if status:
            q = q.filter(DatasetPurchase.status == status)
        
        return q.order_by(desc(DatasetPurchase.created_at)).all()
    
    def check_existing_purchase(self, buyer_id: str, listing_id: str) -> bool:
        """Check if buyer already purchased dataset"""
        existing = self.db.query(DatasetPurchase).filter(
            and_(
                DatasetPurchase.buyer_id == buyer_id,
                DatasetPurchase.listing_id == listing_id,
                DatasetPurchase.status == PurchaseStatus.COMPLETED
            )
        ).first()
        
        return existing is not None
    
    def increment_download_count(self, purchase_id: str, ip_address: str):
        """Increment download count and track IP"""
        purchase = self.get(purchase_id)
        if purchase:
            purchase.download_count += 1
            purchase.last_download_at = datetime.utcnow()
            
            if not purchase.first_download_at:
                purchase.first_download_at = datetime.utcnow()
            
            # Track download IP
            if purchase.download_ips:
                ips = purchase.download_ips
            else:
                ips = []
            
            if ip_address not in ips:
                ips.append(ip_address)
                purchase.download_ips = ips
            
            self.db.commit()
    
    def get_revenue_stats(self, seller_id: str, tenant_id: str, days: int = 30) -> Dict[str, Any]:
        """Get revenue statistics for a seller"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        stats = self.db.query(
            func.count(DatasetPurchase.id).label('total_sales'),
            func.sum(DatasetPurchase.total_amount).label('total_revenue'),
            func.avg(DatasetPurchase.total_amount).label('avg_sale_amount')
        ).join(
            DatasetListing,
            DatasetPurchase.listing_id == DatasetListing.listing_id
        ).filter(
            and_(
                DatasetListing.seller_id == seller_id,
                DatasetListing.tenant_id == tenant_id,
                DatasetPurchase.status == PurchaseStatus.COMPLETED,
                DatasetPurchase.created_at >= cutoff_date
            )
        ).first()
        
        return {
            "total_sales": stats.total_sales or 0,
            "total_revenue": float(stats.total_revenue or 0),
            "avg_sale_amount": float(stats.avg_sale_amount or 0)
        }


class DatasetReviewRepository(BaseRepository[DatasetReview]):
    """Repository for dataset reviews"""
    
    def __init__(self, db: Session):
        super().__init__(DatasetReview, db)
    
    def get_dataset_reviews(
        self, 
        listing_id: str,
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "helpful"
    ) -> List[DatasetReview]:
        """Get reviews for a dataset"""
        q = self.db.query(DatasetReview).filter(
            DatasetReview.listing_id == listing_id
        )
        
        if sort_by == "helpful":
            q = q.order_by(desc(DatasetReview.helpful_count))
        elif sort_by == "recent":
            q = q.order_by(desc(DatasetReview.created_at))
        elif sort_by == "rating_high":
            q = q.order_by(desc(DatasetReview.rating))
        elif sort_by == "rating_low":
            q = q.order_by(DatasetReview.rating)
        
        return q.offset(offset).limit(limit).all()
    
    def get_buyer_review(self, buyer_id: str, listing_id: str) -> Optional[DatasetReview]:
        """Get review by a specific buyer for a dataset"""
        return self.db.query(DatasetReview).filter(
            and_(
                DatasetReview.buyer_id == buyer_id,
                DatasetReview.listing_id == listing_id
            )
        ).first()
    
    def increment_helpful_count(self, review_id: str):
        """Mark review as helpful"""
        review = self.get(review_id)
        if review:
            review.helpful_count += 1
            self.db.commit()


class DatasetAnalyticsRepository(BaseRepository[DatasetAnalytics]):
    """Repository for dataset analytics"""
    
    def __init__(self, db: Session):
        super().__init__(DatasetAnalytics, db)
    
    def get_analytics(
        self, 
        listing_id: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[DatasetAnalytics]:
        """Get analytics for a dataset in date range"""
        return self.db.query(DatasetAnalytics).filter(
            and_(
                DatasetAnalytics.listing_id == listing_id,
                DatasetAnalytics.date >= start_date,
                DatasetAnalytics.date <= end_date
            )
        ).order_by(DatasetAnalytics.date).all()
    
    def record_view(self, listing_id: str, viewer_info: Dict[str, Any]):
        """Record a dataset view"""
        today = datetime.utcnow().date()
        
        # Get or create today's analytics record
        analytics = self.db.query(DatasetAnalytics).filter(
            and_(
                DatasetAnalytics.listing_id == listing_id,
                func.date(DatasetAnalytics.date) == today
            )
        ).first()
        
        if not analytics:
            analytics = DatasetAnalytics(
                listing_id=listing_id,
                date=datetime.combine(today, datetime.min.time())
            )
            self.db.add(analytics)
        
        # Update metrics
        analytics.views += 1
        
        # Update country stats
        country = viewer_info.get('country', 'Unknown')
        if analytics.viewer_countries:
            countries = analytics.viewer_countries
        else:
            countries = {}
        
        countries[country] = countries.get(country, 0) + 1
        analytics.viewer_countries = countries
        
        # Update referrer stats
        referrer = viewer_info.get('referrer', 'Direct')
        if analytics.referrers:
            referrers = analytics.referrers
        else:
            referrers = {}
        
        referrers[referrer] = referrers.get(referrer, 0) + 1
        analytics.referrers = referrers
        
        self.db.commit()


class DatasetQualityCheckRepository(BaseRepository[DatasetQualityCheck]):
    """Repository for dataset quality checks"""
    
    def __init__(self, db: Session):
        super().__init__(DatasetQualityCheck, db)
    
    def get_latest_checks(self, listing_id: str) -> List[DatasetQualityCheck]:
        """Get latest quality check for each type"""
        # Subquery to get latest check per type
        subq = self.db.query(
            DatasetQualityCheck.listing_id,
            DatasetQualityCheck.check_type,
            func.max(DatasetQualityCheck.started_at).label('max_date')
        ).filter(
            DatasetQualityCheck.listing_id == listing_id
        ).group_by(
            DatasetQualityCheck.listing_id,
            DatasetQualityCheck.check_type
        ).subquery()
        
        # Get the actual records
        return self.db.query(DatasetQualityCheck).join(
            subq,
            and_(
                DatasetQualityCheck.listing_id == subq.c.listing_id,
                DatasetQualityCheck.check_type == subq.c.check_type,
                DatasetQualityCheck.started_at == subq.c.max_date
            )
        ).all()
    
    def get_failed_checks(self, listing_id: str) -> List[DatasetQualityCheck]:
        """Get all failed quality checks"""
        return self.db.query(DatasetQualityCheck).filter(
            and_(
                DatasetQualityCheck.listing_id == listing_id,
                DatasetQualityCheck.status == "failed"
            )
        ).order_by(desc(DatasetQualityCheck.started_at)).all() 