"""Analytics API endpoints for Market Making Service"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime, timedelta
from enum import Enum

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.dependencies import get_ignite_client, get_redis_client
from app.monitoring import liquidity_gauge, swap_volume, fee_revenue, strategy_pnl
from app.config import settings

router = APIRouter()


class TimeFrame(str, Enum):
    """Time frame options"""
    HOUR_1 = "1h"
    HOUR_24 = "24h"
    DAY_7 = "7d"
    DAY_30 = "30d"
    ALL = "all"


class MetricType(str, Enum):
    """Available metrics"""
    TVL = "tvl"
    VOLUME = "volume"
    FEES = "fees"
    APY = "apy"
    TRANSACTIONS = "transactions"
    UNIQUE_USERS = "unique_users"


@router.get("/tvl")
async def get_total_value_locked(
    pool_id: Optional[str] = Query(None, description="Filter by pool"),
    time_frame: TimeFrame = TimeFrame.HOUR_24
):
    """Get total value locked (TVL) metrics"""
    try:
        ignite = await get_ignite_client()
        pool_cache = await ignite.get_or_create_cache("pools")
        
        total_tvl = Decimal("0")
        pool_tvls = []
        
        async for p_id, pool_data in pool_cache.scan():
            if pool_id and p_id != pool_id:
                continue
                
            pool_tvl = Decimal(pool_data.get("total_liquidity", "0"))
            total_tvl += pool_tvl
            
            pool_tvls.append({
                "pool_id": p_id,
                "tokens": [pool_data["token_a"], pool_data["token_b"]],
                "tvl": str(pool_tvl),
                "pool_type": pool_data["pool_type"],
                "change_24h": "5.67"  # Mock percentage change
            })
        
        # Sort by TVL
        pool_tvls.sort(key=lambda x: Decimal(x["tvl"]), reverse=True)
        
        return {
            "total_tvl": str(total_tvl),
            "change_24h": "7.89",  # Mock
            "pools": pool_tvls[:10],  # Top 10
            "time_frame": time_frame.value,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/volume")
async def get_volume_stats(
    pool_id: Optional[str] = Query(None, description="Filter by pool"),
    time_frame: TimeFrame = TimeFrame.HOUR_24
):
    """Get trading volume statistics"""
    try:
        ignite = await get_ignite_client()
        pool_cache = await ignite.get_or_create_cache("pools")
        
        total_volume = Decimal("0")
        volumes_by_pool = []
        
        async for p_id, pool_data in pool_cache.scan():
            if pool_id and p_id != pool_id:
                continue
                
            # In production, calculate based on time frame
            # For now, use 24h volume
            volume = Decimal(pool_data.get("volume_24h", "0"))
            total_volume += volume
            
            if volume > 0:
                volumes_by_pool.append({
                    "pool_id": p_id,
                    "volume": str(volume),
                    "trades": 123,  # Mock
                    "avg_trade_size": str(volume / 123) if volume > 0 else "0"
                })
        
        # Sort by volume
        volumes_by_pool.sort(key=lambda x: Decimal(x["volume"]), reverse=True)
        
        return {
            "total_volume": str(total_volume),
            "volumes_by_pool": volumes_by_pool[:10],
            "time_frame": time_frame.value,
            "change_from_previous": "15.34",  # Mock percentage
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/fees")
async def get_fee_analytics(
    pool_id: Optional[str] = Query(None, description="Filter by pool"),
    time_frame: TimeFrame = TimeFrame.HOUR_24
):
    """Get fee revenue analytics"""
    try:
        ignite = await get_ignite_client()
        pool_cache = await ignite.get_or_create_cache("pools")
        
        total_fees = Decimal("0")
        fees_by_pool = []
        
        async for p_id, pool_data in pool_cache.scan():
            if pool_id and p_id != pool_id:
                continue
                
            volume = Decimal(pool_data.get("volume_24h", "0"))
            fee_tier = Decimal(pool_data.get("fee_tier", "0.003"))
            pool_fees = volume * fee_tier
            total_fees += pool_fees
            
            if pool_fees > 0:
                fees_by_pool.append({
                    "pool_id": p_id,
                    "fees_collected": str(pool_fees),
                    "fee_tier": str(fee_tier),
                    "lp_share": str(pool_fees * Decimal("0.7")),  # 70% to LPs
                    "protocol_share": str(pool_fees * Decimal("0.3"))  # 30% to protocol
                })
        
        # Sort by fees
        fees_by_pool.sort(key=lambda x: Decimal(x["fees_collected"]), reverse=True)
        
        return {
            "total_fees": str(total_fees),
            "lp_earnings": str(total_fees * Decimal("0.7")),
            "protocol_revenue": str(total_fees * Decimal("0.3")),
            "fees_by_pool": fees_by_pool[:10],
            "time_frame": time_frame.value,
            "avg_fee_tier": "0.0028",  # Mock average
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/il")
async def get_impermanent_loss_metrics(
    pool_id: Optional[str] = Query(None, description="Filter by pool"),
    time_frame: TimeFrame = TimeFrame.DAY_7
):
    """Get impermanent loss metrics"""
    try:
        # In production, calculate actual IL based on price movements
        # For now, return mock data
        il_data = {
            "average_il": "-2.45",  # Negative percentage
            "pools_with_il": [
                {
                    "pool_id": "ETH_USDC_concentrated",
                    "il_percentage": "-3.21",
                    "price_change": "15.67",
                    "il_protected": True,
                    "protection_payout": "123.45"
                },
                {
                    "pool_id": "BTC_ETH_constant_product",
                    "il_percentage": "-1.89",
                    "price_change": "8.34",
                    "il_protected": False,
                    "protection_payout": "0"
                }
            ],
            "total_il_protection_paid": "5678.90",
            "time_frame": time_frame.value,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return il_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategy-performance")
async def get_strategy_performance(
    strategy_type: Optional[str] = Query(None, description="Filter by strategy type"),
    time_frame: TimeFrame = TimeFrame.DAY_30
):
    """Get aggregated strategy performance"""
    try:
        ignite = await get_ignite_client()
        strategy_cache = await ignite.get_or_create_cache("strategies")
        
        total_pnl = Decimal("0")
        strategies_data = []
        strategy_count = 0
        
        async for strat_id, strat_data in strategy_cache.scan():
            if strategy_type and strat_data["strategy_type"] != strategy_type:
                continue
                
            strategy_count += 1
            pnl = Decimal(strat_data.get("total_pnl", "0"))
            total_pnl += pnl
            
            perf = strat_data.get("performance", {})
            strategies_data.append({
                "strategy_type": strat_data["strategy_type"],
                "count": 1,
                "total_pnl": str(pnl),
                "avg_return": perf.get("return_30d", "0"),
                "win_rate": perf.get("win_rate", 0)
            })
        
        # Aggregate by type
        aggregated = {}
        for strat in strategies_data:
            key = strat["strategy_type"]
            if key not in aggregated:
                aggregated[key] = {
                    "strategy_type": key,
                    "count": 0,
                    "total_pnl": Decimal("0"),
                    "avg_win_rate": 0
                }
            aggregated[key]["count"] += 1
            aggregated[key]["total_pnl"] += Decimal(strat["total_pnl"])
            aggregated[key]["avg_win_rate"] += strat["win_rate"]
        
        # Calculate averages
        for key in aggregated:
            count = aggregated[key]["count"]
            aggregated[key]["avg_win_rate"] /= count
            aggregated[key]["total_pnl"] = str(aggregated[key]["total_pnl"])
        
        return {
            "total_strategies": strategy_count,
            "total_pnl": str(total_pnl),
            "avg_return": "12.34",  # Mock
            "strategies_by_type": list(aggregated.values()),
            "time_frame": time_frame.value,
            "best_performing_type": "grid",  # Mock
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/top-performers")
async def get_top_performers(
    metric: MetricType = MetricType.VOLUME,
    limit: int = Query(10, ge=1, le=100)
):
    """Get top performing pools/strategies by metric"""
    try:
        ignite = await get_ignite_client()
        
        if metric in [MetricType.TVL, MetricType.VOLUME, MetricType.FEES]:
            # Get pool data
            pool_cache = await ignite.get_or_create_cache("pools")
            items = []
            
            async for pool_id, pool_data in pool_cache.scan():
                if metric == MetricType.TVL:
                    value = Decimal(pool_data.get("total_liquidity", "0"))
                elif metric == MetricType.VOLUME:
                    value = Decimal(pool_data.get("volume_24h", "0"))
                else:  # FEES
                    volume = Decimal(pool_data.get("volume_24h", "0"))
                    fee_tier = Decimal(pool_data.get("fee_tier", "0.003"))
                    value = volume * fee_tier
                
                items.append({
                    "id": pool_id,
                    "type": "pool",
                    "name": f"{pool_data['token_a']}/{pool_data['token_b']}",
                    "value": str(value),
                    "pool_type": pool_data["pool_type"]
                })
            
            # Sort and limit
            items.sort(key=lambda x: Decimal(x["value"]), reverse=True)
            items = items[:limit]
            
        else:
            # Mock data for other metrics
            items = []
            for i in range(min(limit, 10)):
                items.append({
                    "id": f"item_{i}",
                    "type": "user",
                    "name": f"User {i+1}",
                    "value": str(10000 - (i * 1000))
                })
        
        return {
            "metric": metric.value,
            "top_performers": items,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/historical/{metric}")
async def get_historical_data(
    metric: MetricType,
    pool_id: Optional[str] = Query(None, description="Filter by pool"),
    interval: str = Query("1h", pattern="^(5m|15m|1h|4h|1d)$"),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get historical data for a metric"""
    try:
        # In production, fetch from time-series database
        # For now, generate mock historical data
        now = datetime.utcnow()
        data_points = []
        
        # Generate data points
        for i in range(limit):
            if interval == "5m":
                timestamp = now - timedelta(minutes=5 * i)
            elif interval == "15m":
                timestamp = now - timedelta(minutes=15 * i)
            elif interval == "1h":
                timestamp = now - timedelta(hours=i)
            elif interval == "4h":
                timestamp = now - timedelta(hours=4 * i)
            else:  # 1d
                timestamp = now - timedelta(days=i)
            
            # Generate mock value with some variation
            base_value = 1000000
            variation = (i % 10) * 50000
            value = base_value + variation
            
            data_points.append({
                "timestamp": timestamp.isoformat(),
                "value": str(value)
            })
        
        # Reverse to have oldest first
        data_points.reverse()
        
        return {
            "metric": metric.value,
            "pool_id": pool_id,
            "interval": interval,
            "data_points": data_points,
            "start_time": data_points[0]["timestamp"],
            "end_time": data_points[-1]["timestamp"]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
async def get_analytics_summary():
    """Get overall analytics summary"""
    try:
        ignite = await get_ignite_client()
        
        # Get pool statistics
        pool_cache = await ignite.get_or_create_cache("pools")
        total_pools = 0
        total_tvl = Decimal("0")
        total_volume = Decimal("0")
        
        async for pool_id, pool_data in pool_cache.scan():
            total_pools += 1
            total_tvl += Decimal(pool_data.get("total_liquidity", "0"))
            total_volume += Decimal(pool_data.get("volume_24h", "0"))
        
        # Get strategy statistics
        strategy_cache = await ignite.get_or_create_cache("strategies")
        total_strategies = 0
        active_strategies = 0
        
        async for strat_id, strat_data in strategy_cache.scan():
            total_strategies += 1
            if strat_data["status"] == "running":
                active_strategies += 1
        
        # Calculate fees
        avg_fee_tier = Decimal("0.003")
        total_fees = total_volume * avg_fee_tier
        
        return {
            "pools": {
                "total": total_pools,
                "total_tvl": str(total_tvl),
                "change_24h": "7.89"  # Mock
            },
            "volume": {
                "volume_24h": str(total_volume),
                "change_24h": "15.34"  # Mock
            },
            "fees": {
                "fees_24h": str(total_fees),
                "annual_rate": str(total_fees * 365)
            },
            "strategies": {
                "total": total_strategies,
                "active": active_strategies,
                "success_rate": "67.89"  # Mock
            },
            "users": {
                "active_24h": 1234,  # Mock
                "total": 5678  # Mock
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 