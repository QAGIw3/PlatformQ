"""
Asset Nexus Integration for Digital Asset Service

Supports valuation, collateralization, and bundling of digital assets
with compute resources and ML models.
"""

import logging
from typing import Dict, Any, Optional, List
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass
import asyncio
import httpx

from app.integrations import IgniteCache, PulsarEventPublisher

logger = logging.getLogger(__name__)


@dataclass
class AssetMetadata:
    """Extended metadata for assets used in compute nexus"""
    asset_id: str
    asset_type: str
    creator_id: str
    creation_date: datetime
    performance_metrics: Dict[str, Any]
    usage_statistics: Dict[str, Any]
    revenue_history: List[Dict[str, Any]]
    quality_score: float
    unique_features: List[str]


class AssetNexusIntegration:
    """Integration support for asset-compute-model nexus"""
    
    def __init__(
        self,
        ignite_cache: Optional[IgniteCache] = None,
        pulsar_publisher: Optional[PulsarEventPublisher] = None
    ):
        self.ignite_cache = ignite_cache
        self.pulsar_publisher = pulsar_publisher
        
        # Market data tracking
        self.price_history = {}
        self.volume_history = {}
        
    async def get_asset_for_nexus(
        self,
        asset_id: str
    ) -> Dict[str, Any]:
        """Get asset details optimized for nexus valuation"""
        try:
            # Get base asset data
            asset_data = await self._get_asset_base_data(asset_id)
            
            # Enhance with performance metrics
            if asset_data.get("type") == "model":
                asset_data["model_metrics"] = await self._get_model_metrics(
                    asset_data.get("model_id")
                )
            elif asset_data.get("type") == "dataset":
                asset_data["dataset_metrics"] = await self._get_dataset_metrics(
                    asset_id
                )
            
            # Add market data
            asset_data["market_data"] = await self.get_asset_market_data(asset_id)
            
            # Add quality assessment
            asset_data["quality_score"] = await self._assess_asset_quality(asset_data)
            
            return asset_data
            
        except Exception as e:
            logger.error(f"Error getting asset for nexus: {e}")
            return {}
    
    async def get_asset_market_data(
        self,
        asset_id: str
    ) -> Dict[str, Any]:
        """Get market data for asset valuation"""
        try:
            # Get from cache first
            if self.ignite_cache:
                cached = await self.ignite_cache.get(f"market_data:{asset_id}")
                if cached:
                    return cached
            
            # Calculate from history
            market_data = {
                "last_price": await self._get_last_traded_price(asset_id),
                "volume_24h": await self._get_24h_volume(asset_id),
                "trades_24h": await self._get_24h_trade_count(asset_id),
                "price_change_24h": await self._get_24h_price_change(asset_id),
                "liquidity_score": await self._calculate_liquidity_score(asset_id)
            }
            
            # Cache for 5 minutes
            if self.ignite_cache:
                await self.ignite_cache.set(
                    f"market_data:{asset_id}",
                    market_data,
                    ttl=300
                )
            
            return market_data
            
        except Exception as e:
            logger.error(f"Error getting market data: {e}")
            return {
                "last_price": "0",
                "volume_24h": "0",
                "trades_24h": 0,
                "price_change_24h": 0,
                "liquidity_score": 0
            }
    
    async def record_asset_bundling(
        self,
        asset_id: str,
        bundle_id: str,
        bundle_type: str,
        bundle_details: Dict[str, Any]
    ):
        """Record when an asset is bundled with compute/models"""
        try:
            bundling_record = {
                "asset_id": asset_id,
                "bundle_id": bundle_id,
                "bundle_type": bundle_type,
                "bundle_details": bundle_details,
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Store bundling record
            if self.ignite_cache:
                await self.ignite_cache.set(
                    f"asset_bundle:{asset_id}:{bundle_id}",
                    bundling_record,
                    ttl=86400 * 90  # 90 days
                )
            
            # Publish bundling event
            if self.pulsar_publisher:
                await self.pulsar_publisher.publish(
                    "persistent://platformq/assets/bundling-events",
                    bundling_record
                )
            
            # Update asset metadata
            await self._update_asset_bundle_count(asset_id)
            
        except Exception as e:
            logger.error(f"Error recording asset bundling: {e}")
    
    async def get_model_performance_history(
        self,
        model_id: str,
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """Get performance history for ML model assets"""
        try:
            history = []
            
            # Get from cache/storage
            if self.ignite_cache:
                for i in range(days):
                    date = datetime.utcnow() - timedelta(days=i)
                    key = f"model_performance:{model_id}:{date.date()}"
                    data = await self.ignite_cache.get(key)
                    if data:
                        history.append(data)
            
            return history
            
        except Exception as e:
            logger.error(f"Error getting model performance history: {e}")
            return []
    
    async def calculate_asset_volatility(
        self,
        asset_id: str,
        window_days: int = 30
    ) -> Decimal:
        """Calculate price volatility for an asset"""
        try:
            # Get price history
            prices = await self._get_price_history(asset_id, window_days)
            
            if len(prices) < 2:
                return Decimal("0.5")  # Default high volatility
            
            # Calculate daily returns
            returns = []
            for i in range(1, len(prices)):
                if prices[i-1] > 0:
                    daily_return = (prices[i] - prices[i-1]) / prices[i-1]
                    returns.append(float(daily_return))
            
            if not returns:
                return Decimal("0.5")
            
            # Calculate standard deviation
            import numpy as np
            volatility = Decimal(str(np.std(returns)))
            
            # Annualize (sqrt(365))
            annualized_vol = volatility * Decimal("19.1")  # sqrt(365)
            
            return min(annualized_vol, Decimal("2.0"))  # Cap at 200%
            
        except Exception as e:
            logger.error(f"Error calculating volatility: {e}")
            return Decimal("0.5")
    
    async def get_dataset_quality_metrics(
        self,
        dataset_id: str
    ) -> Dict[str, Any]:
        """Get quality metrics for dataset assets"""
        try:
            # This would integrate with data quality assessment tools
            metrics = {
                "completeness": 0.95,  # % of non-null values
                "accuracy": 0.92,      # % of accurate values
                "consistency": 0.98,   # % following rules
                "timeliness": 0.90,    # How current the data is
                "uniqueness": 0.99,    # % of unique records
                "validity": 0.94       # % of valid values
            }
            
            # Calculate overall quality score
            quality_score = sum(metrics.values()) / len(metrics)
            
            return {
                **metrics,
                "overall_quality": quality_score,
                "assessed_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting dataset quality: {e}")
            return {"overall_quality": 0.5}
    
    # Private helper methods
    async def _get_asset_base_data(self, asset_id: str) -> Dict[str, Any]:
        """Get base asset data from storage"""
        if self.ignite_cache:
            return await self.ignite_cache.get(f"asset:{asset_id}") or {}
        return {}
    
    async def _get_model_metrics(self, model_id: str) -> Dict[str, Any]:
        """Get ML model performance metrics"""
        # This would integrate with MLOps service
        return {
            "accuracy": 0.92,
            "precision": 0.90,
            "recall": 0.88,
            "f1_score": 0.89,
            "inference_count": 15000,
            "revenue_generated": "2500.00"
        }
    
    async def _get_dataset_metrics(self, dataset_id: str) -> Dict[str, Any]:
        """Get dataset usage metrics"""
        return {
            "record_count": 1000000,
            "feature_count": 50,
            "access_count": 250,
            "derivative_count": 5
        }
    
    async def _assess_asset_quality(self, asset_data: Dict[str, Any]) -> float:
        """Assess overall asset quality"""
        quality_factors = []
        
        # Model quality
        if "model_metrics" in asset_data:
            metrics = asset_data["model_metrics"]
            quality_factors.append(metrics.get("f1_score", 0.5))
        
        # Dataset quality
        if "dataset_metrics" in asset_data:
            quality_factors.append(0.8)  # Placeholder
        
        # Market acceptance
        if "market_data" in asset_data:
            liquidity = asset_data["market_data"].get("liquidity_score", 0)
            quality_factors.append(liquidity)
        
        # Average quality
        if quality_factors:
            return sum(quality_factors) / len(quality_factors)
        return 0.5
    
    async def _get_last_traded_price(self, asset_id: str) -> str:
        """Get last traded price"""
        if asset_id in self.price_history:
            prices = self.price_history[asset_id]
            if prices:
                return str(prices[-1])
        return "0"
    
    async def _get_24h_volume(self, asset_id: str) -> str:
        """Get 24 hour trading volume"""
        if asset_id in self.volume_history:
            cutoff = datetime.utcnow() - timedelta(hours=24)
            volume = sum(
                v["amount"] for v in self.volume_history[asset_id]
                if datetime.fromisoformat(v["timestamp"]) > cutoff
            )
            return str(volume)
        return "0"
    
    async def _get_24h_trade_count(self, asset_id: str) -> int:
        """Get 24 hour trade count"""
        if asset_id in self.volume_history:
            cutoff = datetime.utcnow() - timedelta(hours=24)
            count = len([
                v for v in self.volume_history[asset_id]
                if datetime.fromisoformat(v["timestamp"]) > cutoff
            ])
            return count
        return 0
    
    async def _get_24h_price_change(self, asset_id: str) -> float:
        """Get 24 hour price change percentage"""
        if asset_id in self.price_history:
            if len(self.price_history[asset_id]) >= 2:
                current = self.price_history[asset_id][-1]
                past = self.price_history[asset_id][0]
                if past > 0:
                    return ((current - past) / past) * 100
        return 0.0
    
    async def _calculate_liquidity_score(self, asset_id: str) -> float:
        """Calculate liquidity score (0-1)"""
        volume = float(await self._get_24h_volume(asset_id))
        trades = await self._get_24h_trade_count(asset_id)
        
        # Simple scoring based on volume and trade count
        volume_score = min(volume / 10000, 1.0)  # Normalize to 10k
        trade_score = min(trades / 100, 1.0)      # Normalize to 100 trades
        
        return (volume_score + trade_score) / 2
    
    async def _update_asset_bundle_count(self, asset_id: str):
        """Update count of bundles containing this asset"""
        key = f"asset_bundle_count:{asset_id}"
        
        if self.ignite_cache:
            count = await self.ignite_cache.get(key) or 0
            await self.ignite_cache.set(key, count + 1, ttl=None)
    
    async def _get_price_history(
        self,
        asset_id: str,
        days: int
    ) -> List[Decimal]:
        """Get historical prices"""
        prices = []
        
        if self.ignite_cache:
            for i in range(days):
                date = datetime.utcnow() - timedelta(days=i)
                key = f"price:{asset_id}:{date.date()}"
                price = await self.ignite_cache.get(key)
                if price:
                    prices.append(Decimal(str(price)))
        
        return prices 