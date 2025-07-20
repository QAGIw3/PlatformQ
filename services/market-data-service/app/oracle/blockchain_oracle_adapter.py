"""
Blockchain Oracle Adapter for on-chain price feeds

Integrates with various on-chain oracle protocols to fetch
and aggregate price data alongside traditional market data.
"""

import logging
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime
from abc import ABC, abstractmethod
import asyncio

from web3 import Web3
from eth_abi import decode_abi

logger = logging.getLogger(__name__)


class OracleAdapter(ABC):
    """Base class for blockchain oracle adapters"""
    
    @abstractmethod
    async def get_latest_price(self, asset_pair: str) -> Dict[str, Any]:
        """Get latest price for an asset pair"""
        pass
    
    @abstractmethod
    async def get_historical_prices(self, asset_pair: str, from_timestamp: int, to_timestamp: int) -> List[Dict[str, Any]]:
        """Get historical prices for an asset pair"""
        pass
    
    @abstractmethod
    async def is_healthy(self) -> bool:
        """Check if oracle is healthy and responsive"""
        pass


class ChainlinkAdapter(OracleAdapter):
    """Adapter for Chainlink price feeds"""
    
    # Chainlink Aggregator V3 Interface ABI (simplified)
    AGGREGATOR_ABI = [
        {
            "inputs": [],
            "name": "latestRoundData",
            "outputs": [
                {"name": "roundId", "type": "uint80"},
                {"name": "answer", "type": "int256"},
                {"name": "startedAt", "type": "uint256"},
                {"name": "updatedAt", "type": "uint256"},
                {"name": "answeredInRound", "type": "uint80"}
            ],
            "type": "function"
        },
        {
            "inputs": [],
            "name": "decimals",
            "outputs": [{"name": "", "type": "uint8"}],
            "type": "function"
        }
    ]
    
    def __init__(self, web3: Web3, feed_addresses: Dict[str, str]):
        """
        Initialize Chainlink adapter
        
        Args:
            web3: Web3 instance connected to blockchain
            feed_addresses: Mapping of asset pairs to Chainlink feed addresses
        """
        self.web3 = web3
        self.feed_addresses = feed_addresses
        self.contracts = {}
        
        # Initialize contracts
        for pair, address in feed_addresses.items():
            self.contracts[pair] = self.web3.eth.contract(
                address=Web3.to_checksum_address(address),
                abi=self.AGGREGATOR_ABI
            )
    
    async def get_latest_price(self, asset_pair: str) -> Dict[str, Any]:
        """Get latest price from Chainlink"""
        if asset_pair not in self.contracts:
            raise ValueError(f"No Chainlink feed for {asset_pair}")
        
        try:
            contract = self.contracts[asset_pair]
            
            # Get latest round data
            round_data = await asyncio.to_thread(
                contract.functions.latestRoundData().call
            )
            
            # Get decimals
            decimals = await asyncio.to_thread(
                contract.functions.decimals().call
            )
            
            # Parse response
            round_id, answer, started_at, updated_at, answered_in_round = round_data
            
            # Convert price based on decimals
            price = Decimal(answer) / Decimal(10 ** decimals)
            
            return {
                "source": "chainlink",
                "asset_pair": asset_pair,
                "price": str(price),
                "timestamp": updated_at,
                "round_id": round_id,
                "confidence": 1.0 if (datetime.now().timestamp() - updated_at) < 3600 else 0.8
            }
            
        except Exception as e:
            logger.error(f"Error fetching Chainlink price for {asset_pair}: {e}")
            raise
    
    async def get_historical_prices(self, asset_pair: str, from_timestamp: int, to_timestamp: int) -> List[Dict[str, Any]]:
        """Get historical prices - not implemented for Chainlink"""
        # Chainlink doesn't provide easy historical data access
        # Would need to use events or external indexer
        return []
    
    async def is_healthy(self) -> bool:
        """Check if Chainlink feeds are healthy"""
        try:
            # Check a primary feed (e.g., ETH/USD)
            if "ETH/USD" in self.contracts:
                await self.get_latest_price("ETH/USD")
            return True
        except:
            return False


class BandProtocolAdapter(OracleAdapter):
    """Adapter for Band Protocol price feeds"""
    
    def __init__(self, reference_data_address: str, web3: Web3):
        self.reference_data_address = reference_data_address
        self.web3 = web3
        # Initialize Band Protocol contract
        # Implementation details...
    
    async def get_latest_price(self, asset_pair: str) -> Dict[str, Any]:
        """Get latest price from Band Protocol"""
        # Implementation for Band Protocol
        pass
    
    async def get_historical_prices(self, asset_pair: str, from_timestamp: int, to_timestamp: int) -> List[Dict[str, Any]]:
        """Get historical prices from Band Protocol"""
        pass
    
    async def is_healthy(self) -> bool:
        """Check if Band Protocol is healthy"""
        pass


class UniswapV3TWAPAdapter(OracleAdapter):
    """Adapter for Uniswap V3 Time-Weighted Average Price"""
    
    def __init__(self, web3: Web3, pool_addresses: Dict[str, str]):
        self.web3 = web3
        self.pool_addresses = pool_addresses
        # Initialize Uniswap V3 pool contracts
        # Implementation details...
    
    async def get_latest_price(self, asset_pair: str) -> Dict[str, Any]:
        """Get TWAP from Uniswap V3"""
        # Implementation for Uniswap V3 TWAP
        pass
    
    async def get_historical_prices(self, asset_pair: str, from_timestamp: int, to_timestamp: int) -> List[Dict[str, Any]]:
        """Get historical TWAPs"""
        pass
    
    async def is_healthy(self) -> bool:
        """Check if Uniswap V3 pools are healthy"""
        pass


class OracleAggregator:
    """Aggregates prices from multiple oracle sources"""
    
    def __init__(self):
        self.adapters: Dict[str, OracleAdapter] = {}
        self.weights: Dict[str, float] = {}
        self._running = False
    
    def add_adapter(self, name: str, adapter: OracleAdapter, weight: float = 1.0):
        """Add an oracle adapter with optional weight"""
        self.adapters[name] = adapter
        self.weights[name] = weight
    
    async def get_aggregated_price(self, asset_pair: str) -> Dict[str, Any]:
        """Get aggregated price from all sources"""
        prices = []
        sources = []
        
        # Fetch from all adapters in parallel
        tasks = []
        for name, adapter in self.adapters.items():
            tasks.append(self._fetch_with_timeout(name, adapter, asset_pair))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for name, result in zip(self.adapters.keys(), results):
            if isinstance(result, Exception):
                logger.warning(f"Failed to fetch from {name}: {result}")
                continue
            
            if result:
                prices.append(Decimal(result["price"]))
                sources.append({
                    "name": name,
                    "price": result["price"],
                    "confidence": result.get("confidence", 1.0)
                })
        
        if not prices:
            raise ValueError(f"No oracle data available for {asset_pair}")
        
        # Calculate weighted average
        weighted_sum = Decimal(0)
        total_weight = Decimal(0)
        
        for i, (name, _) in enumerate(sources):
            weight = Decimal(self.weights.get(name, 1.0))
            weighted_sum += prices[i] * weight
            total_weight += weight
        
        aggregated_price = weighted_sum / total_weight
        
        return {
            "asset_pair": asset_pair,
            "aggregated_price": str(aggregated_price),
            "sources": sources,
            "timestamp": datetime.now().timestamp(),
            "num_sources": len(sources)
        }
    
    async def _fetch_with_timeout(self, name: str, adapter: OracleAdapter, asset_pair: str, timeout: int = 5):
        """Fetch price with timeout"""
        try:
            return await asyncio.wait_for(
                adapter.get_latest_price(asset_pair),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching from {name}")
            return None
        except Exception as e:
            logger.error(f"Error fetching from {name}: {e}")
            return None
    
    async def start_monitoring(self):
        """Start monitoring oracle health"""
        self._running = True
        while self._running:
            try:
                # Check health of all adapters
                for name, adapter in self.adapters.items():
                    try:
                        is_healthy = await adapter.is_healthy()
                        if not is_healthy:
                            logger.warning(f"Oracle {name} is unhealthy")
                    except Exception as e:
                        logger.error(f"Error checking health of {name}: {e}")
                
                await asyncio.sleep(60)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in oracle monitoring: {e}")
    
    def stop(self):
        """Stop monitoring"""
        self._running = False 