"""
Price Aggregator with Multi-source Support
Implements robust price aggregation with outlier detection
"""

import asyncio
from typing import Dict, List, Optional, Set
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass
import statistics
import numpy as np
from collections import defaultdict
import logging
import aiohttp
import json

logger = logging.getLogger(__name__)


@dataclass
class OracleSource:
    """Configuration for an oracle source"""
    name: str
    endpoint: str
    api_key: Optional[str]
    weight: Decimal  # Weight in aggregation
    timeout: int  # Request timeout in seconds
    enabled: bool
    supported_assets: List[str]


class PriceAggregator:
    """
    Aggregates prices from multiple oracle sources with outlier detection
    """
    
    def __init__(
        self,
        ignite,
        pulsar,
        influxdb,
        oracle_sources: List[OracleSource]
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.influxdb = influxdb
        self.oracle_sources = {source.name: source for source in oracle_sources}
        
        # Aggregation parameters
        self.min_sources_required = 3  # Minimum sources for valid price
        self.outlier_threshold = Decimal('0.02')  # 2% deviation threshold
        self.confidence_decay_time = 60  # Seconds before confidence starts decaying
        self.max_price_age = 300  # Maximum age of price data in seconds
        
        # Price cache
        self.price_cache: Dict[str, Dict[str, PriceFeed]] = defaultdict(dict)
        self.aggregated_cache: Dict[str, AggregatedPrice] = {}
        
        # Monitoring
        self.oracle_health: Dict[str, OracleStatus] = {}
        self.fetch_failures: Dict[str, int] = defaultdict(int)
        
        self._running = False
        
    async def start_price_collection(self):
        """Start continuous price collection from all sources"""
        self._running = True
        
        # Initialize oracle connections
        await self._initialize_oracles()
        
        # Start collection tasks for each oracle
        tasks = []
        for source_name, source in self.oracle_sources.items():
            if source.enabled:
                task = asyncio.create_task(
                    self._collect_from_source(source)
                )
                tasks.append(task)
                
        # Start aggregation task
        aggregation_task = asyncio.create_task(self._aggregation_loop())
        tasks.append(aggregation_task)
        
        # Start health monitoring
        health_task = asyncio.create_task(self._health_monitoring_loop())
        tasks.append(health_task)
        
        logger.info(f"Started price collection with {len(tasks)-2} oracle sources")
        
        # Wait for all tasks
        await asyncio.gather(*tasks)
        
    async def stop(self):
        """Stop price collection"""
        self._running = False
        logger.info("Stopping price aggregator")
        
    async def get_aggregated_price(self, asset: str) -> Optional[AggregatedPrice]:
        """Get aggregated price for an asset"""
        
        # Check cache first
        if asset in self.aggregated_cache:
            cached_price = self.aggregated_cache[asset]
            age = (datetime.utcnow() - cached_price.timestamp).total_seconds()
            
            if age < self.max_price_age:
                return cached_price
                
        # Aggregate fresh price
        return await self._aggregate_price(asset)
        
    async def get_oracle_statuses(self) -> Dict[str, str]:
        """Get status of all oracle sources"""
        return {
            name: status.value 
            for name, status in self.oracle_health.items()
        }
        
    async def get_active_feed_count(self) -> int:
        """Get count of active price feeds"""
        count = 0
        for asset_feeds in self.price_cache.values():
            for feed in asset_feeds.values():
                age = (datetime.utcnow() - feed.timestamp).total_seconds()
                if age < self.max_price_age:
                    count += 1
        return count
        
    async def get_oracle_sources(self) -> List[Dict]:
        """Get list of oracle sources with metadata"""
        sources = []
        for name, source in self.oracle_sources.items():
            sources.append({
                'name': name,
                'enabled': source.enabled,
                'weight': float(source.weight),
                'status': self.oracle_health.get(name, OracleStatus.OFFLINE).value,
                'supported_assets': source.supported_assets,
                'failures': self.fetch_failures.get(name, 0)
            })
        return sources
        
    async def update_oracle_status(self, source_name: str, status: OracleStatus):
        """Update oracle source status"""
        if source_name in self.oracle_sources:
            self.oracle_health[source_name] = status
            if status == OracleStatus.OFFLINE:
                self.oracle_sources[source_name].enabled = False
            else:
                self.oracle_sources[source_name].enabled = True
                
    async def _initialize_oracles(self):
        """Initialize oracle connections and check health"""
        for source_name, source in self.oracle_sources.items():
            try:
                # Test connection
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                        f"{source.endpoint}/health",
                        timeout=aiohttp.ClientTimeout(total=5)
                    ) as response:
                        if response.status == 200:
                            self.oracle_health[source_name] = OracleStatus.HEALTHY
                        else:
                            self.oracle_health[source_name] = OracleStatus.DEGRADED
                            
            except Exception as e:
                logger.error(f"Failed to initialize oracle {source_name}: {e}")
                self.oracle_health[source_name] = OracleStatus.OFFLINE
                source.enabled = False
                
    async def _collect_from_source(self, source: OracleSource):
        """Collect prices from a single oracle source"""
        
        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    # Fetch prices for all supported assets
                    prices = await self._fetch_prices_from_source(
                        session, source
                    )
                    
                    # Update cache
                    for asset, price_feed in prices.items():
                        self.price_cache[asset][source.name] = price_feed
                        
                    # Store in time series database
                    await self._store_price_feeds(prices)
                    
                    # Reset failure count on success
                    self.fetch_failures[source.name] = 0
                    
                    # Update health status
                    if self.oracle_health.get(source.name) != OracleStatus.HEALTHY:
                        self.oracle_health[source.name] = OracleStatus.HEALTHY
                        
                except Exception as e:
                    logger.error(f"Error collecting from {source.name}: {e}")
                    self.fetch_failures[source.name] += 1
                    
                    # Update health based on failures
                    if self.fetch_failures[source.name] > 5:
                        self.oracle_health[source.name] = OracleStatus.OFFLINE
                    elif self.fetch_failures[source.name] > 2:
                        self.oracle_health[source.name] = OracleStatus.DEGRADED
                        
                # Wait before next collection
                await asyncio.sleep(10)  # Collect every 10 seconds
                
    async def _fetch_prices_from_source(
        self,
        session: aiohttp.ClientSession,
        source: OracleSource
    ) -> Dict[str, PriceFeed]:
        """Fetch prices from a specific oracle source"""
        
        prices = {}
        
        # Different API patterns for different oracles
        if source.name == 'chainlink':
            prices = await self._fetch_chainlink_prices(session, source)
        elif source.name == 'band_protocol':
            prices = await self._fetch_band_prices(session, source)
        elif source.name == 'dia':
            prices = await self._fetch_dia_prices(session, source)
        elif source.name == 'pyth':
            prices = await self._fetch_pyth_prices(session, source)
        elif source.name == 'uma':
            prices = await self._fetch_uma_prices(session, source)
        else:
            # Generic API pattern
            prices = await self._fetch_generic_prices(session, source)
            
        return prices
        
    async def _fetch_chainlink_prices(
        self,
        session: aiohttp.ClientSession,
        source: OracleSource
    ) -> Dict[str, PriceFeed]:
        """Fetch prices from Chainlink oracles"""
        
        prices = {}
        
        # Chainlink API endpoint
        url = f"{source.endpoint}/v1/prices"
        headers = {'X-API-Key': source.api_key} if source.api_key else {}
        
        try:
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=source.timeout)
            ) as response:
                data = await response.json()
                
                for asset_data in data.get('data', []):
                    asset = asset_data['symbol']
                    if asset in source.supported_assets:
                        prices[asset] = PriceFeed(
                            source=source.name,
                            asset=asset,
                            price=Decimal(str(asset_data['price'])),
                            timestamp=datetime.fromisoformat(asset_data['timestamp']),
                            confidence=Decimal('0.99'),  # Chainlink has high confidence
                            volume_24h=Decimal(str(asset_data.get('volume24h', '0')))
                        )
                        
        except Exception as e:
            logger.error(f"Error fetching Chainlink prices: {e}")
            
        return prices
        
    async def _fetch_pyth_prices(
        self,
        session: aiohttp.ClientSession,
        source: OracleSource
    ) -> Dict[str, PriceFeed]:
        """Fetch prices from Pyth Network"""
        
        prices = {}
        
        # Pyth uses different endpoints for different assets
        for asset in source.supported_assets:
            try:
                url = f"{source.endpoint}/api/latest_price_feeds"
                params = {'ids[]': self._get_pyth_price_id(asset)}
                
                async with session.get(
                    url,
                    params=params,
                    timeout=aiohttp.ClientTimeout(total=source.timeout)
                ) as response:
                    data = await response.json()
                    
                    for price_data in data:
                        price = Decimal(str(price_data['price']['price'])) / \
                               Decimal(10 ** price_data['price']['expo'])
                        
                        confidence = Decimal(str(price_data['price']['conf'])) / \
                                   Decimal(10 ** price_data['price']['expo'])
                        
                        prices[asset] = PriceFeed(
                            source=source.name,
                            asset=asset,
                            price=price,
                            timestamp=datetime.fromtimestamp(price_data['price']['publish_time']),
                            confidence=Decimal('1') - (confidence / price),  # Convert to confidence score
                            volume_24h=None
                        )
                        
            except Exception as e:
                logger.error(f"Error fetching Pyth price for {asset}: {e}")
                
        return prices
        
    async def _fetch_generic_prices(
        self,
        session: aiohttp.ClientSession,
        source: OracleSource
    ) -> Dict[str, PriceFeed]:
        """Generic price fetching for standard APIs"""
        
        prices = {}
        
        try:
            url = f"{source.endpoint}/prices"
            headers = {'Authorization': f'Bearer {source.api_key}'} if source.api_key else {}
            
            async with session.get(
                url,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=source.timeout)
            ) as response:
                data = await response.json()
                
                for asset, price_data in data.items():
                    if asset in source.supported_assets:
                        prices[asset] = PriceFeed(
                            source=source.name,
                            asset=asset,
                            price=Decimal(str(price_data['price'])),
                            timestamp=datetime.utcnow(),
                            confidence=Decimal(str(price_data.get('confidence', '0.95'))),
                            volume_24h=Decimal(str(price_data.get('volume', '0')))
                        )
                        
        except Exception as e:
            logger.error(f"Error fetching generic prices from {source.name}: {e}")
            
        return prices
        
    async def _aggregation_loop(self):
        """Continuously aggregate prices for all assets"""
        
        while self._running:
            try:
                # Get all unique assets from cache
                all_assets = set()
                for asset_feeds in self.price_cache.values():
                    all_assets.update(asset_feeds.keys())
                    
                # Aggregate price for each asset
                for asset in all_assets:
                    aggregated = await self._aggregate_price(asset)
                    if aggregated:
                        self.aggregated_cache[asset] = aggregated
                        
                        # Publish aggregated price
                        await self.pulsar.publish('price.aggregated', {
                            'asset': asset,
                            'price': str(aggregated.price),
                            'confidence': float(aggregated.confidence),
                            'sources': aggregated.sources_count,
                            'timestamp': aggregated.timestamp.isoformat()
                        })
                        
            except Exception as e:
                logger.error(f"Error in aggregation loop: {e}")
                
            await asyncio.sleep(1)  # Aggregate every second
            
    async def _aggregate_price(self, asset: str) -> Optional[AggregatedPrice]:
        """Aggregate price from multiple sources with outlier detection"""
        
        if asset not in self.price_cache:
            return None
            
        # Get fresh price feeds
        fresh_feeds = []
        for source_name, feed in self.price_cache[asset].items():
            age = (datetime.utcnow() - feed.timestamp).total_seconds()
            if age < self.max_price_age and self.oracle_sources[source_name].enabled:
                fresh_feeds.append(feed)
                
        # Check minimum sources
        if len(fresh_feeds) < self.min_sources_required:
            logger.warning(f"Insufficient sources for {asset}: {len(fresh_feeds)}")
            return None
            
        # Extract prices and weights
        prices = [feed.price for feed in fresh_feeds]
        weights = [self.oracle_sources[feed.source].weight for feed in fresh_feeds]
        
        # Detect and remove outliers
        cleaned_feeds = self._remove_outliers(fresh_feeds, prices)
        
        if len(cleaned_feeds) < self.min_sources_required:
            logger.warning(f"Too many outliers removed for {asset}")
            return None
            
        # Calculate weighted average
        total_weight = Decimal('0')
        weighted_sum = Decimal('0')
        
        for feed in cleaned_feeds:
            weight = self.oracle_sources[feed.source].weight
            weighted_sum += feed.price * weight
            total_weight += weight
            
        aggregated_price = weighted_sum / total_weight
        
        # Calculate confidence score
        confidence = self._calculate_confidence(cleaned_feeds, aggregated_price)
        
        # Calculate volatility
        volatility_1h = await self.volatility_calculator.calculate_volatility(asset, 1)
        volatility_24h = await self.volatility_calculator.calculate_volatility(asset, 24)
        
        return AggregatedPrice(
            asset=asset,
            price=aggregated_price,
            timestamp=datetime.utcnow(),
            sources_count=len(cleaned_feeds),
            confidence=confidence,
            volatility_1h=volatility_1h['realized'],
            volatility_24h=volatility_24h['realized'],
            price_sources=cleaned_feeds
        )
        
    def _remove_outliers(
        self,
        feeds: List[PriceFeed],
        prices: List[Decimal]
    ) -> List[PriceFeed]:
        """Remove price outliers using statistical methods"""
        
        if len(prices) < 4:
            # Not enough data for outlier detection
            return feeds
            
        # Calculate median and MAD (Median Absolute Deviation)
        median_price = statistics.median(prices)
        deviations = [abs(price - median_price) for price in prices]
        mad = statistics.median(deviations)
        
        # Modified Z-score method
        cleaned_feeds = []
        for i, feed in enumerate(feeds):
            deviation = abs(prices[i] - median_price)
            if mad > 0:
                modified_z_score = Decimal('0.6745') * deviation / mad
                if modified_z_score < Decimal('3.5'):  # Threshold for outliers
                    cleaned_feeds.append(feed)
            else:
                # If MAD is 0, check percentage deviation
                pct_deviation = deviation / median_price
                if pct_deviation < self.outlier_threshold:
                    cleaned_feeds.append(feed)
                    
        return cleaned_feeds
        
    def _calculate_confidence(
        self,
        feeds: List[PriceFeed],
        aggregated_price: Decimal
    ) -> Decimal:
        """Calculate confidence score for aggregated price"""
        
        # Start with base confidence
        confidence = Decimal('1.0')
        
        # Factor 1: Number of sources
        source_factor = min(len(feeds) / 5, 1)  # Max confidence at 5+ sources
        confidence *= Decimal(str(source_factor))
        
        # Factor 2: Price deviation
        deviations = []
        for feed in feeds:
            deviation = abs(feed.price - aggregated_price) / aggregated_price
            deviations.append(float(deviation))
            
        avg_deviation = statistics.mean(deviations)
        deviation_factor = max(0, 1 - avg_deviation * 10)  # Penalize high deviation
        confidence *= Decimal(str(deviation_factor))
        
        # Factor 3: Source reliability
        total_weight = sum(self.oracle_sources[feed.source].weight for feed in feeds)
        avg_weight = total_weight / len(feeds)
        weight_factor = float(avg_weight)
        confidence *= Decimal(str(weight_factor))
        
        # Factor 4: Data freshness
        max_age = max((datetime.utcnow() - feed.timestamp).total_seconds() for feed in feeds)
        freshness_factor = max(0, 1 - max_age / self.max_price_age)
        confidence *= Decimal(str(freshness_factor))
        
        return min(confidence, Decimal('0.99'))  # Cap at 99%
        
    async def _store_price_feeds(self, feeds: Dict[str, PriceFeed]):
        """Store price feeds in time series database"""
        
        points = []
        for asset, feed in feeds.items():
            point = {
                "measurement": "price_feeds",
                "tags": {
                    "asset": asset,
                    "source": feed.source
                },
                "time": feed.timestamp.isoformat(),
                "fields": {
                    "price": float(feed.price),
                    "confidence": float(feed.confidence),
                    "volume_24h": float(feed.volume_24h) if feed.volume_24h else 0
                }
            }
            points.append(point)
            
        if points:
            await self.influxdb.write_points(points)
            
    async def _health_monitoring_loop(self):
        """Monitor oracle health and emit alerts"""
        
        while self._running:
            try:
                # Check each oracle
                for source_name, status in self.oracle_health.items():
                    if status == OracleStatus.OFFLINE:
                        # Emit alert
                        await self.pulsar.publish('oracle.health.alert', {
                            'source': source_name,
                            'status': status.value,
                            'failures': self.fetch_failures.get(source_name, 0),
                            'timestamp': datetime.utcnow().isoformat()
                        })
                        
                # Calculate overall health metrics
                total_oracles = len(self.oracle_sources)
                healthy_oracles = sum(
                    1 for status in self.oracle_health.values()
                    if status == OracleStatus.HEALTHY
                )
                
                health_percentage = healthy_oracles / total_oracles if total_oracles > 0 else 0
                
                # Store health metrics
                await self.ignite.put('oracle_health_metrics', {
                    'total': total_oracles,
                    'healthy': healthy_oracles,
                    'degraded': sum(1 for s in self.oracle_health.values() if s == OracleStatus.DEGRADED),
                    'offline': sum(1 for s in self.oracle_health.values() if s == OracleStatus.OFFLINE),
                    'health_percentage': health_percentage,
                    'timestamp': datetime.utcnow().isoformat()
                })
                
            except Exception as e:
                logger.error(f"Error in health monitoring: {e}")
                
            await asyncio.sleep(30)  # Check every 30 seconds
            
    def _get_pyth_price_id(self, asset: str) -> str:
        """Get Pyth price feed ID for an asset"""
        # Mapping of assets to Pyth price IDs
        pyth_ids = {
            'BTC': 'e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43',
            'ETH': 'ff61491a931112ddf1bd8147cd1b641375f79f5825126d665480874634fd0ace',
            'SOL': 'ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d',
            # Add more mappings as needed
        }
        return pyth_ids.get(asset, '') 