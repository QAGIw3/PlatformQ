"""
Model Marketplace Manager

Handles marketplace operations for ML models including browsing,
purchasing, licensing, reviews, and analytics.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import httpx
import json
from decimal import Decimal

logger = logging.getLogger(__name__)


class ModelMarketplaceManager:
    """Manages model marketplace operations"""
    
    def __init__(self, digital_asset_service_url: str, blockchain_service_url: str):
        self.digital_asset_service_url = digital_asset_service_url
        self.blockchain_service_url = blockchain_service_url
        self.purchase_cache = {}  # In production, use persistent storage
        
    async def browse_models(self, 
                          framework: Optional[str] = None,
                          min_accuracy: Optional[float] = None,
                          max_price: Optional[float] = None,
                          license_type: Optional[str] = None,
                          exclude_tenant: Optional[str] = None) -> List[Dict[str, Any]]:
        """Browse available models in the marketplace"""
        try:
            # Query digital asset service for ML_MODEL assets
            async with httpx.AsyncClient() as client:
                params = {
                    "asset_type": "ML_MODEL",
                    "is_for_sale": "true",
                    "is_licensable": "true"
                }
                
                response = await client.get(
                    f"{self.digital_asset_service_url}/api/v1/marketplace/assets",
                    params=params
                )
                
                if response.status_code != 200:
                    logger.error(f"Failed to fetch marketplace models: {response.text}")
                    return []
                
                models = response.json()
            
            # Filter models based on criteria
            filtered_models = []
            for model in models:
                metadata = model.get("metadata", {})
                
                # Skip tenant's own models
                if exclude_tenant and metadata.get("tenant_id") == exclude_tenant:
                    continue
                
                # Apply filters
                if framework and metadata.get("framework", "").lower() != framework.lower():
                    continue
                
                if min_accuracy:
                    accuracy = metadata.get("metrics", {}).get("accuracy", 0)
                    if float(accuracy) < min_accuracy:
                        continue
                
                if max_price and model.get("sale_price"):
                    if float(model["sale_price"]) > max_price:
                        continue
                
                if license_type and model.get("license_terms"):
                    if model["license_terms"].get("type") != license_type:
                        continue
                
                # Get model statistics
                model_info = {
                    "cid": model["cid"],
                    "name": model["asset_name"],
                    "owner_id": model["owner_id"],
                    "framework": metadata.get("framework", "unknown"),
                    "algorithm": metadata.get("algorithm", "unknown"),
                    "accuracy": metadata.get("metrics", {}).get("accuracy", 0),
                    "model_size_mb": metadata.get("model_size_mb", 0),
                    "training_dataset": metadata.get("training_dataset", "unknown"),
                    "sale_price": model.get("sale_price"),
                    "license_terms": model.get("license_terms"),
                    "royalty_percentage": model.get("royalty_percentage", 250),
                    "created_at": model.get("created_at"),
                    "rating": await self._get_model_rating(model["cid"]),
                    "download_count": await self._get_download_count(model["cid"]),
                    "revenue_generated": await self._get_revenue_generated(model["cid"])
                }
                
                filtered_models.append(model_info)
            
            # Sort by relevance/popularity
            filtered_models.sort(key=lambda x: (
                x["rating"],
                x["download_count"],
                -x.get("sale_price", 0) if x.get("sale_price") else 0
            ), reverse=True)
            
            return filtered_models
            
        except Exception as e:
            logger.error(f"Failed to browse models: {e}")
            raise
    
    async def purchase_model(self,
                           buyer_id: str,
                           buyer_tenant_id: str,
                           model_cid: str,
                           license_type: str,
                           license_duration_days: Optional[int] = None,
                           usage_limit: Optional[int] = None) -> Dict[str, Any]:
        """Purchase or license a model"""
        try:
            # Get model details
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.digital_asset_service_url}/api/v1/digital-assets/{model_cid}"
                )
                
                if response.status_code != 200:
                    raise Exception(f"Model not found: {model_cid}")
                
                model = response.json()
            
            # Validate purchase parameters
            if not model.get("is_for_sale") and not model.get("is_licensable"):
                raise Exception("Model is not available for purchase or licensing")
            
            # Calculate price based on license type
            if license_type == "perpetual":
                if not model.get("sale_price"):
                    raise Exception("Model not available for perpetual purchase")
                price = float(model["sale_price"])
                
            elif license_type == "time_based":
                if not model.get("license_terms") or not license_duration_days:
                    raise Exception("Invalid time-based license parameters")
                
                daily_rate = model["license_terms"].get("daily_rate", 0.1)
                price = daily_rate * license_duration_days
                
            elif license_type == "usage_based":
                if not model.get("license_terms") or not usage_limit:
                    raise Exception("Invalid usage-based license parameters")
                
                per_use_price = model["license_terms"].get("price_per_prediction", 0.001)
                price = per_use_price * usage_limit
                
            else:
                raise Exception(f"Unknown license type: {license_type}")
            
            # Call blockchain bridge instead of creating transaction dict
            async with httpx.AsyncClient() as client:
                purchase_data = {
                    "chain_id": "ethereum",
                    "asset_id": model_cid,
                    "offer_index": 0,  # Assuming default offer
                    "license_type": {"perpetual": 0, "time_based": 1, "usage_based": 2}[license_type]
                }
                response = await client.post(f"{self.blockchain_service_url}/api/v1/marketplace/purchase-license", json=purchase_data)
                if response.status_code == 200:
                    tx_hash = response.json()["tx_hash"]
                    return {"status": "purchased", "tx_hash": tx_hash, "price": price}
                else:
                    raise Exception(f"Blockchain purchase failed: {response.text}")
            
        except Exception as e:
            logger.error(f"Failed to purchase model: {e}")
            raise
    
    async def submit_review(self,
                          reviewer_id: str,
                          model_cid: str,
                          rating: int,
                          review_text: str,
                          verified_purchase: bool) -> Dict[str, Any]:
        """Submit a review for a model"""
        try:
            # Verify purchase if claimed
            if verified_purchase:
                # Check if user has purchased this model
                # In production, check persistent storage
                pass
            
            # Submit review to digital asset service
            review_data = {
                "reviewer_id": reviewer_id,
                "rating": rating,
                "review_text": review_text,
                "verified_purchase": verified_purchase,
                "created_at": datetime.utcnow().isoformat()
            }
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.digital_asset_service_url}/api/v1/digital-assets/{model_cid}/reviews",
                    json=review_data,
                    headers={"X-User-ID": reviewer_id}
                )
                
                if response.status_code != 201:
                    raise Exception(f"Failed to submit review: {response.text}")
                
                review_result = response.json()
            
            return {
                "status": "success",
                "review_id": review_result["review_id"],
                "model_cid": model_cid,
                "rating": rating,
                "verified_purchase": verified_purchase
            }
            
        except Exception as e:
            logger.error(f"Failed to submit review: {e}")
            raise
    
    async def get_model_reviews(self, model_cid: str) -> List[Dict[str, Any]]:
        """Get reviews for a model"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    f"{self.digital_asset_service_url}/api/v1/digital-assets/{model_cid}/reviews"
                )
                
                if response.status_code != 200:
                    logger.error(f"Failed to get reviews: {response.text}")
                    return []
                
                reviews = response.json()
                
                # Sort by helpful votes and date
                reviews.sort(key=lambda x: (
                    x.get("helpful_votes", 0),
                    x.get("created_at", "")
                ), reverse=True)
                
                return reviews
                
        except Exception as e:
            logger.error(f"Failed to get reviews: {e}")
            return []
    
    async def get_top_models(self,
                           period: str = "week",
                           metric: str = "revenue",
                           limit: int = 10) -> List[Dict[str, Any]]:
        """Get top performing models in marketplace"""
        try:
            # In production, this would query analytics database
            # For now, return mock data
            models = await self.browse_models()
            
            # Sort by specified metric
            if metric == "revenue":
                models.sort(key=lambda x: x.get("revenue_generated", 0), reverse=True)
            elif metric == "downloads":
                models.sort(key=lambda x: x.get("download_count", 0), reverse=True)
            elif metric == "rating":
                models.sort(key=lambda x: x.get("rating", 0), reverse=True)
            
            return models[:limit]
            
        except Exception as e:
            logger.error(f"Failed to get top models: {e}")
            raise
    
    async def get_model_revenue(self,
                              tenant_id: str,
                              model_name: str) -> Dict[str, Any]:
        """Get revenue analytics for a model"""
        try:
            # In production, query analytics database
            # For now, return mock data
            return {
                "model_name": model_name,
                "tenant_id": tenant_id,
                "total_revenue": 15420.50,
                "currency": "ETH",
                "total_sales": 342,
                "total_licenses": 1250,
                "average_sale_price": 45.0,
                "revenue_by_month": {
                    "2024-01": 2340.0,
                    "2024-02": 3120.0,
                    "2024-03": 4560.0,
                    "2024-04": 5400.5
                },
                "revenue_by_license_type": {
                    "perpetual": 8500.0,
                    "time_based": 4200.5,
                    "usage_based": 2720.0
                },
                "top_buyers": [
                    {"tenant_id": "tenant-123", "total_spent": 1200.0},
                    {"tenant_id": "tenant-456", "total_spent": 890.5}
                ],
                "royalties_earned": 385.5,
                "last_updated": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get revenue data: {e}")
            raise
    
    async def _get_model_rating(self, model_cid: str) -> float:
        """Get average rating for a model"""
        try:
            reviews = await self.get_model_reviews(model_cid)
            if not reviews:
                return 0.0
            
            total_rating = sum(r.get("rating", 0) for r in reviews)
            return total_rating / len(reviews)
            
        except:
            return 0.0
    
    async def _get_download_count(self, model_cid: str) -> int:
        """Get download count for a model"""
        # In production, this would query analytics database
        return 0
    
    async def _increment_download_count(self, model_cid: str):
        """Increment download count for a model"""
        # In production, this would update analytics database
        pass
    
    async def _get_revenue_generated(self, model_cid: str) -> float:
        """Get total revenue generated by a model"""
        # In production, this would query analytics database
        return 0.0
    
    async def check_model_access(self,
                               tenant_id: str,
                               user_id: str,
                               model_cid: str) -> Dict[str, Any]:
        """Check if user has access to a model"""
        try:
            # Check purchase cache
            purchase_key = f"{tenant_id}:{model_cid}"
            license_record = self.purchase_cache.get(purchase_key)
            
            if not license_record:
                return {
                    "has_access": False,
                    "reason": "no_license"
                }
            
            # Check license validity
            if license_record["license_type"] == "time_based":
                valid_until = datetime.fromisoformat(license_record["valid_until"])
                if datetime.utcnow() > valid_until:
                    return {
                        "has_access": False,
                        "reason": "license_expired",
                        "expired_at": license_record["valid_until"]
                    }
            
            elif license_record["license_type"] == "usage_based":
                if license_record["usage_remaining"] <= 0:
                    return {
                        "has_access": False,
                        "reason": "usage_limit_reached",
                        "usage_limit": license_record.get("usage_limit", 0)
                    }
            
            return {
                "has_access": True,
                "license_type": license_record["license_type"],
                "valid_until": license_record.get("valid_until"),
                "usage_remaining": license_record.get("usage_remaining"),
                "mlflow_uri": license_record.get("mlflow_uri")
            }
            
        except Exception as e:
            logger.error(f"Failed to check model access: {e}")
            return {
                "has_access": False,
                "reason": "error",
                "error": str(e)
            }
    
    async def track_model_usage(self,
                              tenant_id: str,
                              user_id: str,
                              model_cid: str,
                              usage_count: int = 1):
        """Track usage for usage-based licenses"""
        try:
            purchase_key = f"{tenant_id}:{model_cid}"
            license_record = self.purchase_cache.get(purchase_key)
            
            if license_record and license_record["license_type"] == "usage_based":
                license_record["usage_remaining"] -= usage_count
                
                # Emit usage event for billing
                # In production, this would update persistent storage
                
        except Exception as e:
            logger.error(f"Failed to track usage: {e}") 

    async def check_user_license(self, user_id: str, tenant_id: str, model_cid: str) -> bool:
        """Check if user has valid license for model"""
        try:
            # Check in local cache first
            cache_key = f"{tenant_id}:{user_id}:{model_cid}"
            if cache_key in self.purchase_cache:
                license = self.purchase_cache[cache_key]
                # Check if license is still valid
                if license.get("license_type") == "perpetual":
                    return True
                elif license.get("valid_until"):
                    return datetime.fromisoformat(license["valid_until"]) > datetime.utcnow()
                elif license.get("usage_remaining", 0) > 0:
                    return True
            
            # Check with blockchain if not in cache
            # This would query the UsageLicense contract
            return False
            
        except Exception as e:
            logger.error(f"Failed to check license: {e}")
            return False 

    async def get_model_analytics(self, model_name: str, tenant_id: str) -> Dict[str, Any]:
        """Get analytics for a model in the marketplace"""
        try:
            # In production, this would query Elasticsearch or analytics service
            # For now, return mock data from cache
            analytics = {
                "total_sales": 0,
                "total_revenue": 0.0,
                "total_licenses": 0,
                "active_licenses": 0,
                "royalties_earned": 0.0,
                "average_rating": 4.5,
                "download_count": 0,
                "revenue_by_month": {},
                "top_buyers": []
            }
            
            # Count purchases in cache
            for key, purchase in self.purchase_cache.items():
                if model_name in key:
                    analytics["total_licenses"] += 1
                    analytics["total_revenue"] += purchase.get("price_paid", 0)
                    
                    # Check if license is active
                    if purchase.get("license_type") == "perpetual":
                        analytics["active_licenses"] += 1
                    elif purchase.get("valid_until"):
                        if datetime.fromisoformat(purchase["valid_until"]) > datetime.utcnow():
                            analytics["active_licenses"] += 1
                    elif purchase.get("usage_remaining", 0) > 0:
                        analytics["active_licenses"] += 1
            
            # In production, query blockchain for actual royalty data
            analytics["royalties_earned"] = analytics["total_revenue"] * 0.025  # 2.5% default
            
            return analytics
            
        except Exception as e:
            logger.error(f"Failed to get model analytics: {e}")
            return {} 