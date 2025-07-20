"""Market data client integration"""

from typing import Dict, Optional, List
from decimal import Decimal
import aiohttp
import asyncio
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


class MarketDataClient:
    """Client for fetching market data from market data service"""
    
    def __init__(self, base_url: str = "http://market-data-service:8083"):
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_price(self, market_id: str) -> Dict:
        """Get current price for a market"""
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        try:
            async with self.session.get(
                f"{self.base_url}/api/v1/prices/{market_id}"
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        "market_id": market_id,
                        "price": data.get("last_price", data.get("price")),
                        "bid": data.get("best_bid"),
                        "ask": data.get("best_ask"),
                        "timestamp": data.get("timestamp")
                    }
                else:
                    logger.error(f"Failed to get price for {market_id}: {response.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"Error fetching price for {market_id}: {e}")
            return None
    
    async def get_prices_bulk(self, market_ids: List[str]) -> Dict[str, Dict]:
        """Get prices for multiple markets"""
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        tasks = [self.get_price(market_id) for market_id in market_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        prices = {}
        for market_id, result in zip(market_ids, results):
            if isinstance(result, dict) and result:
                prices[market_id] = result
        
        return prices
    
    async def get_historical_returns(
        self,
        market_id: str,
        days: int = 30
    ) -> List[float]:
        """Get historical returns for risk calculations"""
        if not self.session:
            self.session = aiohttp.ClientSession()
        
        try:
            async with self.session.get(
                f"{self.base_url}/api/v1/candles/{market_id}",
                params={
                    "interval": "1d",
                    "limit": days
                }
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    candles = data.get("candles", [])
                    
                    # Calculate returns
                    returns = []
                    for i in range(1, len(candles)):
                        prev_close = float(candles[i-1]["close"])
                        curr_close = float(candles[i]["close"])
                        if prev_close > 0:
                            ret = (curr_close - prev_close) / prev_close
                            returns.append(ret)
                    
                    return returns
                else:
                    logger.error(f"Failed to get historical data for {market_id}: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error fetching historical data for {market_id}: {e}")
            return [] 