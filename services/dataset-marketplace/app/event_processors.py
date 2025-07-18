"""
Dataset Marketplace Event Processors

Handles events for dataset listings, purchases, and quality checks.
"""

import logging
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import uuid
import hashlib
import secrets

from platformq_shared.event_framework import EventProcessor, event_handler
from platformq_shared.events import (
    AssetUsed,
    DatasetListingCreated,
    DatasetPurchaseRequested,
    DatasetPurchaseCompleted,
    DatasetDownloadRequested,
    DatasetQualityCheckRequested,
    DatasetQualityCheckCompleted,
    PaymentProcessed,
    DatasetReviewSubmitted
)
from platformq_shared.database import get_db
from .repository import (
    DatasetListingRepository,
    DatasetPurchaseRepository,
    DatasetReviewRepository,
    DatasetAnalyticsRepository,
    DatasetQualityCheckRepository
)
from .models import (
    DatasetListing,
    DatasetPurchase,
    DatasetStatus,
    PurchaseStatus
)

logger = logging.getLogger(__name__)


class DatasetMarketplaceEventProcessor(EventProcessor):
    """Event processor for dataset marketplace operations"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.listing_repo = None
        self.purchase_repo = None
        self.review_repo = None
        self.analytics_repo = None
        self.quality_repo = None
        
    def initialize_repos(self):
        """Initialize repositories"""
        if not self.listing_repo:
            db = next(get_db())
            self.listing_repo = DatasetListingRepository(db)
            self.purchase_repo = DatasetPurchaseRepository(db)
            self.review_repo = DatasetReviewRepository(db)
            self.analytics_repo = DatasetAnalyticsRepository(db)
            self.quality_repo = DatasetQualityCheckRepository(db)
    
    @event_handler("persistent://public/default/dataset-listing-events")
    async def handle_listing_created(self, event: DatasetListingCreated):
        """Handle new dataset listing creation"""
        self.initialize_repos()
        
        try:
            # Trigger quality checks for new listing
            await self.publish_event(
                DatasetQualityCheckRequested(
                    listing_id=event.listing_id,
                    check_types=["schema", "format", "content", "completeness"],
                    tenant_id=event.tenant_id
                ),
                "persistent://public/default/dataset-quality-events"
            )
            
            logger.info(f"Dataset listing {event.listing_id} created, quality checks initiated")
            
        except Exception as e:
            logger.error(f"Error handling listing created: {e}")
    
    @event_handler("persistent://public/default/dataset-purchase-events")
    async def handle_purchase_request(self, event: DatasetPurchaseRequested):
        """Handle dataset purchase request"""
        self.initialize_repos()
        
        try:
            # Get listing
            listing = self.listing_repo.get_by_listing_id(event.listing_id)
            if not listing or listing.status != DatasetStatus.ACTIVE:
                logger.error(f"Dataset {event.listing_id} not available")
                await self._publish_purchase_failed(event, "Dataset not available")
                return
            
            # Check if already purchased
            if self.purchase_repo.check_existing_purchase(event.buyer_id, event.listing_id):
                await self._publish_purchase_failed(event, "Dataset already purchased")
                return
            
            # Check license availability
            if listing.max_licenses and listing.licenses_sold >= listing.max_licenses:
                await self._publish_purchase_failed(event, "No licenses available")
                return
            
            # Calculate total amount
            total_amount = listing.price * event.license_count
            
            # Create purchase record
            access_token = secrets.token_urlsafe(32)
            purchase = self.purchase_repo.create({
                "purchase_id": str(uuid.uuid4()),
                "tenant_id": event.tenant_id,
                "buyer_id": event.buyer_id,
                "listing_id": event.listing_id,
                "price": listing.price,
                "license_count": event.license_count,
                "total_amount": total_amount,
                "access_token": access_token,
                "download_expiry": datetime.utcnow() + timedelta(days=30),
                "status": PurchaseStatus.PENDING
            })
            
            # Publish payment request
            await self.publish_event(
                {
                    "payment_id": str(uuid.uuid4()),
                    "purchase_id": purchase.purchase_id,
                    "buyer_id": event.buyer_id,
                    "amount": total_amount,
                    "currency": listing.currency,
                    "payment_type": "dataset_purchase",
                    "reference_id": purchase.purchase_id,
                    "tenant_id": event.tenant_id
                },
                "persistent://public/default/payment-events"
            )
            
            logger.info(f"Purchase {purchase.purchase_id} created for dataset {event.listing_id}")
            
        except Exception as e:
            logger.error(f"Error handling purchase request: {e}")
            await self._publish_purchase_failed(event, str(e))
    
    @event_handler("persistent://public/default/payment-events")
    async def handle_payment_processed(self, event: PaymentProcessed):
        """Handle payment confirmation for dataset purchase"""
        self.initialize_repos()
        
        try:
            # Only process dataset-related payments
            if event.payment_type != "dataset_purchase":
                return
            
            purchase = self.purchase_repo.get_by_purchase_id(event.reference_id)
            if not purchase:
                logger.error(f"Purchase {event.reference_id} not found")
                return
            
            if event.status == "success":
                # Update purchase status
                self.purchase_repo.update(purchase.id, {
                    "status": PurchaseStatus.COMPLETED,
                    "completed_at": datetime.utcnow(),
                    "transaction_id": event.transaction_id
                })
                
                # Update listing statistics
                listing = self.listing_repo.get_by_listing_id(purchase.listing_id)
                if listing:
                    self.listing_repo.update(listing.id, {
                        "licenses_sold": listing.licenses_sold + purchase.license_count,
                        "total_revenue": listing.total_revenue + purchase.total_amount
                    })
                
                # Publish completion event
                await self.publish_event(
                    DatasetPurchaseCompleted(
                        purchase_id=purchase.purchase_id,
                        listing_id=purchase.listing_id,
                        buyer_id=purchase.buyer_id,
                        access_token=purchase.access_token,
                        download_url=f"/api/v1/datasets/{purchase.listing_id}/download",
                        tenant_id=purchase.tenant_id
                    ),
                    "persistent://public/default/dataset-purchase-events"
                )
                
                # Track asset usage
                await self.publish_event(
                    AssetUsed(
                        tenant_id=purchase.tenant_id,
                        asset_id=purchase.listing_id,
                        user_id=purchase.buyer_id,
                        usage_duration_minutes=0,  # Not time-based
                        usage_type="dataset_purchase",
                        event_timestamp=int(datetime.utcnow().timestamp() * 1000)
                    ),
                    "persistent://public/default/asset-usage-events"
                )
                
            else:
                # Payment failed
                self.purchase_repo.update(purchase.id, {
                    "status": PurchaseStatus.FAILED
                })
                
        except Exception as e:
            logger.error(f"Error handling payment processed: {e}")
    
    @event_handler("persistent://public/default/dataset-download-events")
    async def handle_download_request(self, event: DatasetDownloadRequested):
        """Handle dataset download request"""
        self.initialize_repos()
        
        try:
            # Validate access token
            purchase = self.purchase_repo.get_by_access_token(event.access_token)
            if not purchase:
                logger.error(f"Invalid access token for download")
                return
            
            # Check download limits
            if purchase.download_count >= purchase.max_downloads:
                logger.error(f"Download limit exceeded for purchase {purchase.purchase_id}")
                await self._publish_download_failed(
                    event,
                    "Download limit exceeded"
                )
                return
            
            # Check expiry
            if purchase.download_expiry and datetime.utcnow() > purchase.download_expiry:
                logger.error(f"Download expired for purchase {purchase.purchase_id}")
                await self._publish_download_failed(
                    event,
                    "Download link expired"
                )
                return
            
            # Record download
            self.purchase_repo.increment_download_count(
                purchase.id,
                event.ip_address
            )
            
            # Get dataset URL
            listing = self.listing_repo.get_by_listing_id(purchase.listing_id)
            if not listing:
                logger.error(f"Listing {purchase.listing_id} not found")
                return
            
            # Publish download authorized event
            await self.publish_event(
                {
                    "purchase_id": purchase.purchase_id,
                    "listing_id": purchase.listing_id,
                    "buyer_id": purchase.buyer_id,
                    "download_url": listing.download_url,
                    "encryption_key": listing.encryption_key,
                    "format": listing.format,
                    "size_bytes": listing.size_bytes,
                    "download_count": purchase.download_count + 1
                },
                "persistent://public/default/dataset-download-authorized-events"
            )
            
        except Exception as e:
            logger.error(f"Error handling download request: {e}")
    
    @event_handler("persistent://public/default/dataset-quality-events")
    async def handle_quality_check_completed(self, event: DatasetQualityCheckCompleted):
        """Handle dataset quality check completion"""
        self.initialize_repos()
        
        try:
            # Get all quality checks for the listing
            checks = self.quality_repo.get_latest_checks(event.listing_id)
            
            # Calculate overall quality score
            total_score = 0
            check_count = 0
            has_failures = False
            
            for check in checks:
                if check.score:
                    total_score += check.score
                    check_count += 1
                if check.status == "failed":
                    has_failures = True
            
            quality_score = (total_score / check_count) if check_count > 0 else 0
            
            # Update listing with quality info
            listing = self.listing_repo.get_by_listing_id(event.listing_id)
            if listing:
                update_data = {
                    "quality_score": quality_score,
                    "validation_results": {
                        "last_check": datetime.utcnow().isoformat(),
                        "has_failures": has_failures,
                        "check_count": len(checks)
                    }
                }
                
                # Auto-activate if quality is good and currently draft
                if listing.status == DatasetStatus.DRAFT and quality_score >= 70 and not has_failures:
                    update_data["status"] = DatasetStatus.ACTIVE
                    update_data["published_at"] = datetime.utcnow()
                
                self.listing_repo.update(listing.id, update_data)
                
                logger.info(f"Dataset {event.listing_id} quality score: {quality_score}")
            
        except Exception as e:
            logger.error(f"Error handling quality check completed: {e}")
    
    @event_handler("persistent://public/default/dataset-review-events")
    async def handle_review_submitted(self, event: DatasetReviewSubmitted):
        """Handle dataset review submission"""
        self.initialize_repos()
        
        try:
            # Update listing rating
            self.listing_repo.update_rating(event.listing_id)
            
            # Record analytics
            self.analytics_repo.record_view(
                event.listing_id,
                {
                    "type": "review",
                    "rating": event.rating
                }
            )
            
            logger.info(f"Review submitted for dataset {event.listing_id}")
            
        except Exception as e:
            logger.error(f"Error handling review submission: {e}")
    
    @event_handler("persistent://public/default/dataset-view-events")
    async def handle_dataset_viewed(self, event: Dict[str, Any]):
        """Handle dataset view tracking"""
        self.initialize_repos()
        
        try:
            listing_id = event.get("listing_id")
            viewer_info = event.get("viewer_info", {})
            
            # Increment view count
            self.listing_repo.increment_view_count(listing_id)
            
            # Record analytics
            self.analytics_repo.record_view(listing_id, viewer_info)
            
        except Exception as e:
            logger.error(f"Error handling dataset view: {e}")
    
    async def _publish_purchase_failed(self, event: DatasetPurchaseRequested, reason: str):
        """Publish purchase failed event"""
        await self.publish_event(
            {
                "request_id": event.request_id,
                "buyer_id": event.buyer_id,
                "listing_id": event.listing_id,
                "reason": reason,
                "timestamp": datetime.utcnow().isoformat()
            },
            "persistent://public/default/dataset-purchase-failed-events"
        )
    
    async def _publish_download_failed(self, event: DatasetDownloadRequested, reason: str):
        """Publish download failed event"""
        await self.publish_event(
            {
                "access_token": event.access_token,
                "reason": reason,
                "timestamp": datetime.utcnow().isoformat()
            },
            "persistent://public/default/dataset-download-failed-events"
        ) 