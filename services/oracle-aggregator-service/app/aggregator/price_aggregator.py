import asyncio
from typing import List, Dict, Optional, Tuple
from decimal import Decimal
import numpy as np
from datetime import datetime, timedelta
import logging

from app.config.oracle_config import ORACLE_CONFIG, ORACLE_SOURCES, AssetOracleConfig
from app.sources.chainlink import ChainlinkOracle
from app.sources.band_protocol import BandProtocolOracle
from app.sources.internal import InternalAggregator
from app.sources.ai_discovery import AIDiscoveryOracle

logger = logging.getLogger(__name__)

class PriceAggregator:
    """
    Aggregates prices from multiple sources with outlier detection
    """
    
    def __init__(self, ignite_client, pulsar_client):
        self.ignite = ignite_client
        self.pulsar = pulsar_client
        
        # Initialize oracle sources
        self.oracles = {
            "chainlink": ChainlinkOracle(),
            "band_protocol": BandProtocolOracle(),
            "internal_aggregator": InternalAggregator(ignite_client),
            "ai_discovery": AIDiscoveryOracle()
        }
        
        # Price cache
        self.price_cache = {}
        
    async def get_price(
        self, 
        asset_id: str, 
        asset_type: str = "crypto_assets"
    ) -> Tuple[Decimal, Dict[str, any]]:
        """
        Get aggregated price with metadata
        """
        # Get asset-specific configuration
        config = self._get_asset_config(asset_type)
        
        # Fetch prices from all sources in parallel
        price_tasks = []
        for source_name, source_config in ORACLE_SOURCES.items():
            if source_name in self.oracles:
                task = self._fetch_price_with_timeout(
                    source_name,
                    asset_id,
                    source_config.timeout
                )
                price_tasks.append((source_name, task))
        
        # Gather results
        results = await asyncio.gather(
            *[task for _, task in price_tasks],
            return_exceptions=True
        )
        
        # Process results
        valid_prices = []
        source_prices = {}
        
        for (source_name, _), result in zip(price_tasks, results):
            if isinstance(result, Exception):
                logger.warning(f"Failed to get price from {source_name}: {result}")
                if ORACLE_SOURCES[source_name].required:
                    raise ValueError(f"Required oracle {source_name} failed")
            else:
                price, timestamp = result
                if self._is_price_fresh(timestamp, config.stale_threshold):
                    valid_prices.append({
                        "source": source_name,
                        "price": price,
                        "weight": ORACLE_SOURCES[source_name].weight,
                        "timestamp": timestamp
                    })
                    source_prices[source_name] = float(price)
        
        # Check minimum sources
        if len(valid_prices) < config.min_sources:
            raise ValueError(
                f"Insufficient price sources: {len(valid_prices)} < {config.min_sources}"
            )
        
        # Detect and remove outliers
        cleaned_prices = self._remove_outliers(valid_prices, config)
        
        # Calculate weighted median price
        final_price = self._calculate_weighted_median(cleaned_prices)
        
        # Update cache and publish
        await self._update_price_cache(asset_id, final_price, source_prices)
        await self._publish_price_update(asset_id, final_price, source_prices)
        
        return final_price, {
            "sources_used": len(cleaned_prices),
            "sources_total": len(valid_prices),
            "outliers_removed": len(valid_prices) - len(cleaned_prices),
            "source_prices": source_prices,
            "timestamp": datetime.utcnow(),
            "confidence": self._calculate_confidence(cleaned_prices, config)
        }
    
    async def _fetch_price_with_timeout(
        self,
        source_name: str,
        asset_id: str,
        timeout_ms: int
    ) -> Tuple[Decimal, datetime]:
        """
        Fetch price with timeout
        """
        try:
            return await asyncio.wait_for(
                self.oracles[source_name].get_price(asset_id),
                timeout=timeout_ms / 1000.0
            )
        except asyncio.TimeoutError:
            raise TimeoutError(f"{source_name} timeout after {timeout_ms}ms")
    
    def _remove_outliers(
        self,
        prices: List[Dict],
        config: AssetOracleConfig
    ) -> List[Dict]:
        """
        Remove statistical outliers using modified Z-score
        """
        if len(prices) <= 2:
            return prices
        
        price_values = [float(p["price"]) for p in prices]
        median = np.median(price_values)
        mad = np.median(np.abs(price_values - median))  # Median Absolute Deviation
        
        if mad == 0:
            return prices
        
        # Modified Z-score
        m_z_scores = 0.6745 * (price_values - median) / mad
        
        # Filter outliers
        cleaned = []
        for i, price_data in enumerate(prices):
            if abs(m_z_scores[i]) <= 3.5:  # Standard threshold
                # Additional check: not more than threshold% from median
                deviation = abs(float(price_data["price"]) - median) / median
                if deviation <= config.outlier_threshold:
                    cleaned.append(price_data)
                else:
                    logger.warning(
                        f"Removed outlier from {price_data['source']}: "
                        f"{price_data['price']} deviates {deviation*100:.2f}% from median"
                    )
        
        return cleaned
    
    def _calculate_weighted_median(self, prices: List[Dict]) -> Decimal:
        """
        Calculate weighted median price
        """
        # Sort by price
        sorted_prices = sorted(prices, key=lambda x: float(x["price"]))
        
        # Calculate cumulative weights
        total_weight = sum(p["weight"] for p in sorted_prices)
        cumulative_weight = 0
        
        for price_data in sorted_prices:
            cumulative_weight += price_data["weight"]
            if cumulative_weight >= total_weight / 2:
                return Decimal(str(price_data["price"]))
        
        # Fallback to last price
        return Decimal(str(sorted_prices[-1]["price"]))
    
    def _calculate_confidence(
        self,
        prices: List[Dict],
        config: AssetOracleConfig
    ) -> float:
        """
        Calculate confidence score (0-1)
        """
        if not prices:
            return 0.0
        
        # Factor 1: Number of sources
        source_factor = min(len(prices) / 5, 1.0)  # Max out at 5 sources
        
        # Factor 2: Price convergence (inverse of standard deviation)
        price_values = [float(p["price"]) for p in prices]
        if len(price_values) > 1:
            cv = np.std(price_values) / np.mean(price_values)  # Coefficient of variation
            convergence_factor = max(0, 1 - cv * 10)  # Penalize high variation
        else:
            convergence_factor = 0.5
        
        # Factor 3: Source reliability (based on weights)
        weight_factor = sum(p["weight"] for p in prices) / len(prices)
        
        # Combined confidence
        confidence = (source_factor * 0.3 + convergence_factor * 0.5 + weight_factor * 0.2)
        
        return round(confidence, 3)
    
    def _is_price_fresh(self, timestamp: datetime, stale_threshold: int) -> bool:
        """
        Check if price is fresh enough
        """
        age = (datetime.utcnow() - timestamp).total_seconds()
        return age <= stale_threshold
    
    def _get_asset_config(self, asset_type: str) -> AssetOracleConfig:
        """
        Get configuration for asset type
        """
        # Parse asset type hierarchy
        parts = asset_type.split(".")
        config = ORACLE_CONFIG
        
        for part in parts:
            if part in config:
                config = config[part]
            else:
                # Default config
                return AssetOracleConfig()
        
        return config
    
    async def _update_price_cache(
        self,
        asset_id: str,
        price: Decimal,
        source_prices: Dict[str, float]
    ):
        """
        Update Ignite cache
        """
        cache_key = f"price:{asset_id}"
        cache_value = {
            "price": str(price),
            "source_prices": source_prices,
            "timestamp": datetime.utcnow().isoformat(),
            "ttl": 300  # 5 minutes
        }
        
        await self.ignite.put_async(cache_key, cache_value)
    
    async def _publish_price_update(
        self,
        asset_id: str,
        price: Decimal,
        source_prices: Dict[str, float]
    ):
        """
        Publish price update to Pulsar
        """
        event = {
            "event_type": "price_update",
            "asset_id": asset_id,
            "price": str(price),
            "source_prices": source_prices,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        topic = f"persistent://public/default/price-updates"
        await self.pulsar.send_async(topic, event) 