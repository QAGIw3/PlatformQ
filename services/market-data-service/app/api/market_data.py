"""Market data API endpoints"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional, List, Dict
from datetime import datetime, timedelta
from decimal import Decimal

from ..core.aggregator import MarketDataAggregator
from ..cache.cache_manager import CacheManager
from ..dependencies import get_aggregator, get_cache_manager


router = APIRouter(prefix="/api/v1", tags=["market-data"])


@router.get("/prices/{market_id}")
async def get_price(
    market_id: str,
    aggregator: MarketDataAggregator = Depends(get_aggregator)
):
    """Get current price for a market"""
    try:
        state = await aggregator.get_market_state(market_id)
        if not state:
            raise HTTPException(status_code=404, detail="Market not found")
        
        return {
            "market_id": market_id,
            "price": str(state.last_price),
            "best_bid": str(state.best_bid) if state.best_bid else None,
            "best_ask": str(state.best_ask) if state.best_ask else None,
            "volume_24h": str(state.volume_24h),
            "high_24h": str(state.high_24h),
            "low_24h": str(state.low_24h),
            "timestamp": state.last_update.isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/prices")
async def get_prices(
    market_ids: str = Query(..., description="Comma-separated market IDs"),
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """Get prices for multiple markets"""
    try:
        ids = [id.strip() for id in market_ids.split(",")]
        prices = await cache_manager.get_prices_bulk(ids)
        
        return {
            "prices": prices,
            "count": len(prices)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/orderbook/{market_id}")
async def get_orderbook(
    market_id: str,
    depth: int = Query(default=20, ge=1, le=100),
    aggregator: MarketDataAggregator = Depends(get_aggregator)
):
    """Get order book for a market"""
    try:
        orderbook = await aggregator.get_orderbook(market_id)
        if not orderbook:
            raise HTTPException(status_code=404, detail="Order book not found")
        
        # Limit depth
        ob_dict = orderbook.to_dict()
        ob_dict["bids"] = ob_dict["bids"][:depth]
        ob_dict["asks"] = ob_dict["asks"][:depth]
        
        return ob_dict
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trades/{market_id}")
async def get_recent_trades(
    market_id: str,
    limit: int = Query(default=100, ge=1, le=1000),
    aggregator: MarketDataAggregator = Depends(get_aggregator)
):
    """Get recent trades for a market"""
    try:
        trades = await aggregator.get_recent_trades(market_id, limit)
        
        return {
            "market_id": market_id,
            "trades": [t.to_dict() for t in trades],
            "count": len(trades)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/candles/{market_id}")
async def get_candles(
    market_id: str,
    interval: str = Query(..., pattern="^(1m|5m|15m|30m|1h|4h|1d|1w)$"),
    start_time: Optional[int] = Query(None, description="Start timestamp (seconds)"),
    end_time: Optional[int] = Query(None, description="End timestamp (seconds)"),
    limit: int = Query(default=100, ge=1, le=1000),
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """Get candlestick data for a market"""
    try:
        # Default time range if not specified
        if not end_time:
            end_dt = datetime.utcnow()
        else:
            end_dt = datetime.fromtimestamp(end_time)
        
        if not start_time:
            # Default to appropriate range based on interval
            interval_hours = {
                "1m": 1,
                "5m": 6,
                "15m": 12,
                "30m": 24,
                "1h": 48,
                "4h": 168,  # 1 week
                "1d": 720,  # 30 days
                "1w": 2160  # 90 days
            }
            hours = interval_hours.get(interval, 24)
            start_dt = end_dt - timedelta(hours=hours)
        else:
            start_dt = datetime.fromtimestamp(start_time)
        
        # Get candles from cache
        candles = await cache_manager.get_candles(
            market_id, interval, start_dt, end_dt, limit
        )
        
        # TODO: If not in cache, fetch from Cassandra
        
        return {
            "market_id": market_id,
            "interval": interval,
            "candles": candles,
            "count": len(candles)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ticker/{market_id}")
async def get_ticker(
    market_id: str,
    aggregator: MarketDataAggregator = Depends(get_aggregator),
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """Get 24h ticker statistics for a market"""
    try:
        # Get from cache first
        stats = await cache_manager.get_market_stats(market_id)
        
        if not stats:
            # Generate from aggregator
            state = await aggregator.get_market_state(market_id)
            if not state:
                raise HTTPException(status_code=404, detail="Market not found")
            
            # Calculate 24h change
            # TODO: Get open price from 24h ago
            open_24h = state.last_price  # Placeholder
            change_24h = state.last_price - open_24h
            change_percent = (change_24h / open_24h * 100) if open_24h > 0 else Decimal(0)
            
            stats = {
                "market_id": market_id,
                "last_price": str(state.last_price),
                "best_bid": str(state.best_bid) if state.best_bid else None,
                "best_ask": str(state.best_ask) if state.best_ask else None,
                "price_change_24h": str(change_24h),
                "price_change_percent_24h": str(change_percent),
                "high_24h": str(state.high_24h),
                "low_24h": str(state.low_24h),
                "volume_24h": str(state.volume_24h),
                "trade_count_24h": state.trade_count_24h,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Cache it
            await cache_manager.set_market_stats(market_id, stats)
        
        return stats
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ticker")
async def get_all_tickers(
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """Get 24h ticker statistics for all markets"""
    try:
        all_stats = await cache_manager.get_all_market_stats()
        
        return {
            "tickers": list(all_stats.values()),
            "count": len(all_stats)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/markets")
async def get_markets(
    status: Optional[str] = Query(None, pattern="^(active|suspended|delisted)$"),
    market_type: Optional[str] = Query(None, pattern="^(spot|futures|perpetual|options)$"),
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """Get market information"""
    try:
        markets = await cache_manager.get_all_markets()
        
        # Filter by status
        if status:
            markets = [m for m in markets if m.get("status") == status]
        
        # Filter by type
        if market_type:
            markets = [m for m in markets if m.get("market_type") == market_type]
        
        return {
            "markets": markets,
            "count": len(markets)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/markets/{market_id}")
async def get_market_info(
    market_id: str,
    cache_manager: CacheManager = Depends(get_cache_manager)
):
    """Get information for a specific market"""
    try:
        info = await cache_manager.get_market_info(market_id)
        if not info:
            raise HTTPException(status_code=404, detail="Market not found")
        
        return info
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 