"""
Price Aggregator Oracle

Aggregates price feeds from multiple sources for compute resources.
Provides reliable pricing data for DeFi protocols with outlier detection.
"""

from typing import Dict, Any, List, Optional, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
import logging
import asyncio
import statistics
import numpy as np
from collections import defaultdict, deque
from enum import Enum

from web3 import Web3
from fastapi import HTTPException
from prometheus_client import Counter, Gauge, Histogram
import aiohttp

from ..core.blockchain import BlockchainClient
from ..models.pricing import PriceData, PriceSource, PriceFeed
from ..utils.signing import sign_oracle_data

logger = logging.getLogger(__name__)

# Metrics
PRICE_UPDATES = Counter(
    'oracle_price_updates_total',
    'Total price updates',
    ['resource_type', 'source']
)
CURRENT_PRICE = Gauge(
    'oracle_current_price',
    'Current aggregated price',
    ['resource_type', 'currency']
)
PRICE_DEVIATION = Gauge(
    'oracle_price_deviation_percent',
    'Price deviation between sources',
    ['resource_type']
)
AGGREGATION_LATENCY = Histogram(
    'oracle_price_aggregation_seconds',
    'Time to aggregate prices'
)


class PriceSourceType(str, Enum):
    MARKET = "market"           # Direct from compute markets
    AMM = "amm"                # From AMM pools
    ORACLE = "oracle"          # Other oracle services
    EXCHANGE = "exchange"      # Centralized exchanges
    SYNTHETIC = "synthetic"    # Calculated/derived prices


class PriceAggregator:
    """Aggregates price feeds for compute resources"""
    
    def __init__(
        self,
        blockchain_client: BlockchainClient,
        oracle_contract_address: str,
        signing_key: str,
        market_addresses: Dict[str, str],
        amm_addresses: Dict[str, str]
    ):
        self.blockchain = blockchain_client
        self.oracle_contract_address = oracle_contract_address
        self.signing_key = signing_key
        self.market_addresses = market_addresses
        self.amm_addresses = amm_addresses
        
        # Price sources configuration
        self._price_sources = {
            'quantum': [
                {
                    'type': PriceSourceType.MARKET,
                    'name': 'quantum_market',
                    'address': market_addresses.get('quantum'),
                    'weight': 0.4
                },
                {
                    'type': PriceSourceType.AMM,
                    'name': 'quantum_amm',
                    'address': amm_addresses.get('quantum'),
                    'weight': 0.3
                },
                {
                    'type': PriceSourceType.ORACLE,
                    'name': 'chainlink',
                    'endpoint': 'https://api.chain.link/quantum',
                    'weight': 0.3
                }
            ],
            'ai': [
                {
                    'type': PriceSourceType.MARKET,
                    'name': 'ai_market',
                    'address': market_addresses.get('ai'),
                    'weight': 0.35
                },
                {
                    'type': PriceSourceType.AMM,
                    'name': 'ai_amm',
                    'address': amm_addresses.get('ai'),
                    'weight': 0.35
                },
                {
                    'type': PriceSourceType.EXCHANGE,
                    'name': 'compute_exchange',
                    'endpoint': 'https://api.computex.io/ai',
                    'weight': 0.3
                }
            ],
            'network': [
                {
                    'type': PriceSourceType.MARKET,
                    'name': 'network_market',
                    'address': market_addresses.get('network'),
                    'weight': 0.5
                },
                {
                    'type': PriceSourceType.AMM,
                    'name': 'network_amm',
                    'address': amm_addresses.get('network'),
                    'weight': 0.5
                }
            ]
        }
        
        # Price cache
        self._price_cache = {}  # resource_type -> price_data
        self._price_history = defaultdict(lambda: deque(maxlen=1000))  # Rolling history
        
        # Aggregation parameters
        self.cache_ttl = 60  # 1 minute
        self.outlier_threshold = 0.2  # 20% deviation
        self.min_sources = 2  # Minimum sources for valid price
        self.confidence_threshold = 0.8  # 80% confidence required
        
        # TWAP parameters
        self.twap_window = 300  # 5 minutes
        self.vwap_window = 3600  # 1 hour
        
        # HTTP session
        self._session = None
        
    async def initialize(self):
        """Initialize the price aggregator"""
        self._session = aiohttp.ClientSession()
        
    async def shutdown(self):
        """Shutdown the aggregator"""
        if self._session:
            await self._session.close()
    
    async def get_price(
        self,
        resource_type: str,
        base_currency: str = "USD",
        include_sources: bool = False
    ) -> Dict[str, Any]:
        """
        Get aggregated price for a resource type
        
        Args:
            resource_type: Type of compute resource
            base_currency: Currency to price in
            include_sources: Include individual source prices
            
        Returns:
            Aggregated price data
        """
        try:
            # Check cache first
            cache_key = f"{resource_type}:{base_currency}"
            cached_data = self._price_cache.get(cache_key)
            
            if cached_data and (datetime.utcnow() - cached_data['timestamp']).seconds < self.cache_ttl:
                return cached_data['data']
            
            # Aggregate fresh prices
            with AGGREGATION_LATENCY.time():
                price_data = await self._aggregate_prices(
                    resource_type,
                    base_currency,
                    include_sources
                )
            
            # Cache the result
            self._price_cache[cache_key] = {
                'data': price_data,
                'timestamp': datetime.utcnow()
            }
            
            # Update metrics
            CURRENT_PRICE.labels(
                resource_type=resource_type,
                currency=base_currency
            ).set(float(price_data['price']))
            
            # Store in history
            self._update_history(resource_type, price_data)
            
            return price_data
            
        except Exception as e:
            logger.error(f"Failed to get price: {e}")
            raise HTTPException(status_code=500, detail=str(e))
    
    async def get_twap(
        self,
        resource_type: str,
        window_seconds: int = None,
        base_currency: str = "USD"
    ) -> Dict[str, Any]:
        """
        Get Time-Weighted Average Price
        
        Args:
            resource_type: Type of compute resource
            window_seconds: TWAP window (default: 5 minutes)
            base_currency: Currency
            
        Returns:
            TWAP data
        """
        try:
            window = window_seconds or self.twap_window
            history = list(self._price_history[resource_type])
            
            # Filter by time window
            cutoff_time = datetime.utcnow() - timedelta(seconds=window)
            window_prices = [
                p for p in history
                if p['timestamp'] > cutoff_time
            ]
            
            if not window_prices:
                # Fallback to current price
                current = await self.get_price(resource_type, base_currency)
                return {
                    'twap': current['price'],
                    'window_seconds': window,
                    'data_points': 1,
                    'confidence': 0.5
                }
            
            # Calculate TWAP
            twap = self._calculate_twap(window_prices)
            
            return {
                'resource_type': resource_type,
                'twap': twap,
                'window_seconds': window,
                'data_points': len(window_prices),
                'start_time': window_prices[0]['timestamp'],
                'end_time': window_prices[-1]['timestamp'],
                'confidence': min(len(window_prices) / 10, 1.0)  # More data = higher confidence
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate TWAP: {e}")
            raise
    
    async def get_volatility(
        self,
        resource_type: str,
        window_hours: int = 24
    ) -> Dict[str, Any]:
        """
        Get price volatility metrics
        
        Args:
            resource_type: Type of compute resource
            window_hours: Period for volatility calculation
            
        Returns:
            Volatility metrics
        """
        try:
            history = list(self._price_history[resource_type])
            
            # Filter by time window
            cutoff_time = datetime.utcnow() - timedelta(hours=window_hours)
            window_prices = [
                p['price'] for p in history
                if p['timestamp'] > cutoff_time
            ]
            
            if len(window_prices) < 2:
                return {
                    'resource_type': resource_type,
                    'volatility': 0,
                    'window_hours': window_hours,
                    'data_points': len(window_prices)
                }
            
            # Calculate returns
            returns = []
            for i in range(1, len(window_prices)):
                if window_prices[i-1] > 0:
                    ret = (window_prices[i] - window_prices[i-1]) / window_prices[i-1]
                    returns.append(ret)
            
            # Calculate volatility (annualized)
            if returns:
                volatility = float(np.std(returns) * np.sqrt(365 * 24))  # Hourly to annual
            else:
                volatility = 0
            
            return {
                'resource_type': resource_type,
                'volatility': volatility,
                'window_hours': window_hours,
                'data_points': len(window_prices),
                'min_price': min(window_prices),
                'max_price': max(window_prices),
                'price_range': max(window_prices) - min(window_prices)
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate volatility: {e}")
            raise
    
    async def sign_price_data(
        self,
        resource_type: str,
        price_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Sign price data for on-chain submission
        
        Args:
            resource_type: Type of compute resource
            price_data: Price data to sign
            
        Returns:
            Signed price data
        """
        try:
            # Prepare oracle data
            oracle_data = {
                'resource_type': resource_type,
                'price': int(price_data['price'] * 10**8),  # 8 decimals
                'confidence': int(price_data['confidence'] * 100),
                'timestamp': int(datetime.utcnow().timestamp()),
                'sources': len(price_data.get('sources', []))
            }
            
            # Sign the data
            signed_data = sign_oracle_data(
                oracle_data,
                self.signing_key,
                self.oracle_contract_address
            )
            
            return {
                'oracle_data': oracle_data,
                'signature': signed_data['signature'],
                'message_hash': signed_data['message_hash'],
                'signer': signed_data['signer']
            }
            
        except Exception as e:
            logger.error(f"Failed to sign price data: {e}")
            raise
    
    async def submit_price_update(
        self,
        resource_type: str,
        signed_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Submit price update to blockchain
        
        Args:
            resource_type: Type of compute resource
            signed_data: Signed price data
            
        Returns:
            Transaction result
        """
        try:
            oracle_contract = await self.blockchain.get_contract(
                self.oracle_contract_address,
                "PriceOracle"
            )
            
            tx = await oracle_contract.functions.updatePrice(
                resource_type,
                signed_data['oracle_data']['price'],
                signed_data['oracle_data']['confidence'],
                signed_data['oracle_data']['timestamp'],
                signed_data['signature']
            ).transact()
            
            receipt = await self.blockchain.wait_for_transaction(tx)
            
            return {
                'tx_hash': tx,
                'block_number': receipt['blockNumber'],
                'gas_used': receipt['gasUsed']
            }
            
        except Exception as e:
            logger.error(f"Failed to submit price update: {e}")
            raise
    
    # Private aggregation methods
    
    async def _aggregate_prices(
        self,
        resource_type: str,
        base_currency: str,
        include_sources: bool
    ) -> Dict[str, Any]:
        """Aggregate prices from multiple sources"""
        
        sources = self._price_sources.get(resource_type, [])
        if not sources:
            raise ValueError(f"No price sources configured for {resource_type}")
        
        # Fetch prices from all sources
        price_results = await asyncio.gather(
            *[self._fetch_price(source, resource_type, base_currency) for source in sources],
            return_exceptions=True
        )
        
        # Process results
        valid_prices = []
        source_data = []
        
        for i, result in enumerate(price_results):
            if isinstance(result, Exception):
                logger.warning(f"Failed to fetch from {sources[i]['name']}: {result}")
                continue
            
            if result and result.get('price'):
                valid_prices.append({
                    'price': result['price'],
                    'weight': sources[i]['weight'],
                    'source': sources[i]['name']
                })
                
                if include_sources:
                    source_data.append({
                        'name': sources[i]['name'],
                        'type': sources[i]['type'],
                        'price': result['price'],
                        'timestamp': result.get('timestamp', datetime.utcnow())
                    })
                
                # Update metrics
                PRICE_UPDATES.labels(
                    resource_type=resource_type,
                    source=sources[i]['name']
                ).inc()
        
        # Check minimum sources
        if len(valid_prices) < self.min_sources:
            raise ValueError(
                f"Insufficient price sources: {len(valid_prices)} < {self.min_sources}"
            )
        
        # Remove outliers
        cleaned_prices = self._remove_outliers(valid_prices)
        
        if not cleaned_prices:
            raise ValueError("No valid prices after outlier removal")
        
        # Calculate weighted average
        total_weight = sum(p['weight'] for p in cleaned_prices)
        weighted_sum = sum(p['price'] * p['weight'] for p in cleaned_prices)
        aggregated_price = weighted_sum / total_weight
        
        # Calculate confidence
        confidence = self._calculate_confidence(valid_prices, cleaned_prices)
        
        # Calculate price deviation
        prices_only = [p['price'] for p in valid_prices]
        if len(prices_only) > 1:
            deviation = statistics.stdev(prices_only) / statistics.mean(prices_only)
            PRICE_DEVIATION.labels(resource_type=resource_type).set(deviation * 100)
        else:
            deviation = 0
        
        result = {
            'resource_type': resource_type,
            'price': aggregated_price,
            'currency': base_currency,
            'confidence': confidence,
            'deviation': deviation,
            'sources_used': len(cleaned_prices),
            'sources_total': len(sources),
            'timestamp': datetime.utcnow()
        }
        
        if include_sources:
            result['sources'] = source_data
        
        return result
    
    async def _fetch_price(
        self,
        source: Dict[str, Any],
        resource_type: str,
        base_currency: str
    ) -> Optional[Dict[str, Any]]:
        """Fetch price from a single source"""
        
        try:
            if source['type'] == PriceSourceType.MARKET:
                return await self._fetch_market_price(
                    source['address'],
                    resource_type
                )
            
            elif source['type'] == PriceSourceType.AMM:
                return await self._fetch_amm_price(
                    source['address'],
                    resource_type
                )
            
            elif source['type'] == PriceSourceType.ORACLE:
                return await self._fetch_oracle_price(
                    source['endpoint'],
                    resource_type,
                    base_currency
                )
            
            elif source['type'] == PriceSourceType.EXCHANGE:
                return await self._fetch_exchange_price(
                    source['endpoint'],
                    resource_type,
                    base_currency
                )
            
            else:
                logger.warning(f"Unknown source type: {source['type']}")
                return None
                
        except Exception as e:
            logger.error(f"Error fetching from {source['name']}: {e}")
            return None
    
    async def _fetch_market_price(
        self,
        market_address: str,
        resource_type: str
    ) -> Dict[str, Any]:
        """Fetch price from on-chain market"""
        
        market_contract = await self.blockchain.get_contract(
            market_address,
            f"{resource_type.title()}Market"
        )
        
        # Get average price from recent trades
        price_data = await market_contract.functions.getAveragePrice().call()
        
        return {
            'price': Decimal(str(price_data)) / 10**18,
            'timestamp': datetime.utcnow()
        }
    
    async def _fetch_amm_price(
        self,
        amm_address: str,
        resource_type: str
    ) -> Dict[str, Any]:
        """Fetch price from AMM pool"""
        
        amm_contract = await self.blockchain.get_contract(
            amm_address,
            "ComputeResourceAMM"
        )
        
        # Get reserves and calculate price
        reserves = await amm_contract.functions.getReserves().call()
        
        # Assuming token0 is compute token, token1 is USDC
        price = Decimal(str(reserves[1])) / Decimal(str(reserves[0]))
        
        return {
            'price': price,
            'timestamp': datetime.utcnow()
        }
    
    async def _fetch_oracle_price(
        self,
        endpoint: str,
        resource_type: str,
        base_currency: str
    ) -> Dict[str, Any]:
        """Fetch price from external oracle"""
        
        async with self._session.get(
            f"{endpoint}/price/{resource_type}/{base_currency}",
            timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            if response.status == 200:
                data = await response.json()
                return {
                    'price': Decimal(str(data['price'])),
                    'timestamp': datetime.fromisoformat(data['timestamp'])
                }
            else:
                raise ValueError(f"Oracle returned {response.status}")
    
    async def _fetch_exchange_price(
        self,
        endpoint: str,
        resource_type: str,
        base_currency: str
    ) -> Dict[str, Any]:
        """Fetch price from exchange API"""
        
        async with self._session.get(
            endpoint,
            params={
                'symbol': f"{resource_type.upper()}{base_currency}",
                'interval': '1m'
            },
            timeout=aiohttp.ClientTimeout(total=10)
        ) as response:
            if response.status == 200:
                data = await response.json()
                return {
                    'price': Decimal(str(data['last'])),
                    'timestamp': datetime.utcnow()
                }
            else:
                raise ValueError(f"Exchange returned {response.status}")
    
    def _remove_outliers(
        self,
        prices: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Remove price outliers using IQR method"""
        
        if len(prices) < 3:
            return prices  # Not enough data for outlier detection
        
        price_values = [p['price'] for p in prices]
        
        # Calculate quartiles
        q1 = np.percentile(price_values, 25)
        q3 = np.percentile(price_values, 75)
        iqr = q3 - q1
        
        # Define outlier bounds
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        # Alternative: use percentage deviation from median
        median = statistics.median(price_values)
        
        cleaned_prices = []
        for price_data in prices:
            price = price_data['price']
            
            # Check IQR bounds
            if lower_bound <= price <= upper_bound:
                # Also check percentage deviation
                deviation = abs(price - median) / median
                if deviation <= self.outlier_threshold:
                    cleaned_prices.append(price_data)
                else:
                    logger.warning(
                        f"Removed outlier: {price} deviates {deviation*100:.1f}% from median"
                    )
            else:
                logger.warning(f"Removed outlier: {price} outside IQR bounds")
        
        return cleaned_prices
    
    def _calculate_confidence(
        self,
        all_prices: List[Dict[str, Any]],
        used_prices: List[Dict[str, Any]]
    ) -> float:
        """Calculate confidence score for aggregated price"""
        
        # Factor 1: Source coverage
        source_coverage = len(used_prices) / len(all_prices) if all_prices else 0
        
        # Factor 2: Price agreement (low deviation)
        if len(used_prices) > 1:
            prices = [p['price'] for p in used_prices]
            cv = statistics.stdev(prices) / statistics.mean(prices)  # Coefficient of variation
            price_agreement = max(0, 1 - cv * 2)  # Lower CV = higher agreement
        else:
            price_agreement = 0.5  # Default for single source
        
        # Factor 3: Weight coverage
        total_weight = sum(p['weight'] for p in all_prices)
        used_weight = sum(p['weight'] for p in used_prices)
        weight_coverage = used_weight / total_weight if total_weight > 0 else 0
        
        # Combine factors
        confidence = (source_coverage * 0.3 + price_agreement * 0.5 + weight_coverage * 0.2)
        
        return min(confidence, 1.0)
    
    def _calculate_twap(self, price_history: List[Dict[str, Any]]) -> Decimal:
        """Calculate time-weighted average price"""
        
        if not price_history:
            return Decimal("0")
        
        if len(price_history) == 1:
            return price_history[0]['price']
        
        total_weighted = Decimal("0")
        total_duration = Decimal("0")
        
        for i in range(1, len(price_history)):
            # Time duration
            duration = (price_history[i]['timestamp'] - price_history[i-1]['timestamp']).total_seconds()
            
            # Average price for the period
            avg_price = (price_history[i]['price'] + price_history[i-1]['price']) / 2
            
            total_weighted += avg_price * Decimal(str(duration))
            total_duration += Decimal(str(duration))
        
        if total_duration > 0:
            return total_weighted / total_duration
        else:
            return price_history[-1]['price']
    
    def _update_history(self, resource_type: str, price_data: Dict[str, Any]):
        """Update price history"""
        self._price_history[resource_type].append({
            'price': price_data['price'],
            'timestamp': price_data['timestamp'],
            'confidence': price_data['confidence']
        })
    
    async def start_periodic_updates(self, interval: int = 30):
        """Start periodic price updates"""
        while True:
            try:
                # Update prices for all resource types
                for resource_type in self._price_sources.keys():
                    try:
                        # Get fresh price
                        price_data = await self.get_price(resource_type)
                        
                        # Sign and submit if confidence is high
                        if price_data['confidence'] >= self.confidence_threshold:
                            signed_data = await self.sign_price_data(resource_type, price_data)
                            await self.submit_price_update(resource_type, signed_data)
                            
                    except Exception as e:
                        logger.error(f"Error updating {resource_type} price: {e}")
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                logger.error(f"Error in periodic price updates: {e}")
                await asyncio.sleep(interval) 