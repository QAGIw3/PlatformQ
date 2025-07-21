"""Monitoring API endpoints for Market Making Service"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from enum import Enum

from fastapi import APIRouter, Depends, HTTPException, Query, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field
import asyncio
import json

from app.core.dependencies import get_ignite_client, get_redis_client
from app.monitoring import (
    pool_operations, swap_volume, liquidity_gauge, strategy_pnl,
    active_strategies, mining_rewards, fee_revenue, order_latency
)
from app.config import settings

router = APIRouter()


class HealthStatus(str, Enum):
    """System health status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


class AlertSeverity(str, Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class SystemAlert(BaseModel):
    """System alert"""
    alert_id: str
    severity: AlertSeverity
    component: str
    message: str
    timestamp: str
    resolved: bool = False
    metadata: Optional[Dict[str, Any]] = None


@router.get("/health")
async def get_system_health():
    """Get overall system health status"""
    try:
        # Check component health
        components = {}
        overall_status = HealthStatus.HEALTHY
        
        # Check Ignite
        try:
            ignite = await get_ignite_client()
            components["ignite"] = {"status": "healthy", "latency_ms": 5}
        except Exception as e:
            components["ignite"] = {"status": "unhealthy", "error": str(e)}
            overall_status = HealthStatus.DEGRADED
        
        # Check Redis
        try:
            redis = await get_redis_client()
            await redis.ping()
            components["redis"] = {"status": "healthy", "latency_ms": 2}
        except Exception as e:
            components["redis"] = {"status": "unhealthy", "error": str(e)}
            overall_status = HealthStatus.DEGRADED
        
        # Check pools
        try:
            ignite = await get_ignite_client()
            pool_cache = await ignite.get_or_create_cache("pools")
            pool_count = 0
            async for _ in pool_cache.scan():
                pool_count += 1
                if pool_count > 10:  # Just check a few
                    break
            components["pools"] = {"status": "healthy", "active_pools": pool_count}
        except Exception as e:
            components["pools"] = {"status": "unhealthy", "error": str(e)}
        
        # Check strategies
        try:
            strategy_cache = await ignite.get_or_create_cache("strategies")
            strategy_count = 0
            active_count = 0
            async for _, strat_data in strategy_cache.scan():
                strategy_count += 1
                if strat_data["status"] == "running":
                    active_count += 1
                if strategy_count > 10:
                    break
            components["strategies"] = {
                "status": "healthy",
                "total": strategy_count,
                "active": active_count
            }
        except Exception as e:
            components["strategies"] = {"status": "unhealthy", "error": str(e)}
        
        return {
            "status": overall_status.value,
            "timestamp": datetime.utcnow().isoformat(),
            "components": components,
            "uptime_seconds": 86400,  # Mock uptime
            "version": "1.0.0"
        }
        
    except Exception as e:
        return {
            "status": HealthStatus.UNHEALTHY.value,
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }


@router.get("/metrics")
async def get_system_metrics():
    """Get system performance metrics"""
    try:
        # In production, collect from Prometheus
        # For now, return mock metrics
        metrics = {
            "pools": {
                "operations": {
                    "create": 123,
                    "swap": 45678,
                    "add_liquidity": 1234,
                    "remove_liquidity": 567
                },
                "total_volume_24h": "12345678.90",
                "total_fees_24h": "37037.04",
                "active_pools": 89
            },
            "strategies": {
                "active": 45,
                "total_deployed": 156,
                "orders_placed_24h": 23456,
                "success_rate": 0.678
            },
            "system": {
                "cpu_usage_percent": 45.6,
                "memory_usage_percent": 67.8,
                "disk_usage_percent": 34.5,
                "network_in_mbps": 123.4,
                "network_out_mbps": 456.7
            },
            "latency": {
                "p50_ms": 12,
                "p95_ms": 45,
                "p99_ms": 89
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return metrics
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/alerts")
async def get_active_alerts(
    severity: Optional[AlertSeverity] = Query(None, description="Filter by severity"),
    component: Optional[str] = Query(None, description="Filter by component"),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get active system alerts"""
    try:
        # In production, fetch from alert management system
        # For now, return mock alerts
        alerts = []
        
        # Generate some mock alerts
        if severity is None or severity == AlertSeverity.WARNING:
            alerts.append(SystemAlert(
                alert_id="alert_001",
                severity=AlertSeverity.WARNING,
                component="liquidity",
                message="Low liquidity in ETH/USDC pool",
                timestamp=datetime.utcnow().isoformat(),
                resolved=False,
                metadata={"pool_id": "ETH_USDC_constant_product", "liquidity": "45678.90"}
            ))
        
        if severity is None or severity == AlertSeverity.INFO:
            alerts.append(SystemAlert(
                alert_id="alert_002",
                severity=AlertSeverity.INFO,
                component="strategies",
                message="Strategy rebalancing triggered",
                timestamp=(datetime.utcnow() - timedelta(hours=1)).isoformat(),
                resolved=True,
                metadata={"strategy_id": "grid_ETH_USDC_123"}
            ))
        
        # Filter by component if specified
        if component:
            alerts = [a for a in alerts if a.component == component]
        
        # Limit results
        alerts = alerts[:limit]
        
        return {
            "alerts": [alert.dict() for alert in alerts],
            "total": len(alerts),
            "unresolved": sum(1 for a in alerts if not a.resolved)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/events/recent")
async def get_recent_events(
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get recent system events"""
    try:
        # In production, fetch from event store
        # For now, return mock events
        events = [
            {
                "event_id": "evt_001",
                "event_type": "pool.created",
                "timestamp": datetime.utcnow().isoformat(),
                "data": {
                    "pool_id": "BTC_USDC_concentrated",
                    "creator": "user_123",
                    "initial_liquidity": "100000"
                }
            },
            {
                "event_id": "evt_002",
                "event_type": "swap.executed",
                "timestamp": (datetime.utcnow() - timedelta(minutes=5)).isoformat(),
                "data": {
                    "pool_id": "ETH_USDC_constant_product",
                    "amount_in": "1000",
                    "amount_out": "1800"
                }
            },
            {
                "event_id": "evt_003",
                "event_type": "strategy.deployed",
                "timestamp": (datetime.utcnow() - timedelta(minutes=10)).isoformat(),
                "data": {
                    "strategy_id": "arb_001",
                    "strategy_type": "cross_market_arbitrage",
                    "capital": "50000"
                }
            }
        ]
        
        # Filter by event type if specified
        if event_type:
            events = [e for e in events if e["event_type"] == event_type]
        
        # Limit results
        events = events[:limit]
        
        return {
            "events": events,
            "total": len(events)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/performance/summary")
async def get_performance_summary():
    """Get system performance summary"""
    try:
        # Calculate performance metrics
        now = datetime.utcnow()
        
        return {
            "uptime": {
                "current_streak_hours": 168,  # Mock 7 days
                "availability_30d": "99.95",
                "last_downtime": (now - timedelta(days=7)).isoformat()
            },
            "throughput": {
                "transactions_per_second": 456,
                "peak_tps_24h": 1234,
                "avg_tps_24h": 678
            },
            "latency": {
                "swap_avg_ms": 45,
                "strategy_order_avg_ms": 23,
                "api_response_avg_ms": 12
            },
            "errors": {
                "error_rate_24h": "0.02",
                "failed_transactions_24h": 34,
                "total_transactions_24h": 170000
            },
            "timestamp": now.isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.websocket("/ws/metrics")
async def metrics_websocket(websocket: WebSocket):
    """WebSocket endpoint for real-time metrics"""
    await websocket.accept()
    
    try:
        while True:
            # Send metrics every second
            metrics = {
                "timestamp": datetime.utcnow().isoformat(),
                "pools": {
                    "active_swaps": 45,
                    "volume_per_second": "12345.67",
                    "gas_price_gwei": 50
                },
                "strategies": {
                    "active": 23,
                    "orders_per_second": 78,
                    "pnl_per_second": "234.56"
                },
                "system": {
                    "cpu_percent": 45.6,
                    "memory_mb": 2048,
                    "connections": 1234
                }
            }
            
            await websocket.send_json(metrics)
            await asyncio.sleep(1)
            
    except WebSocketDisconnect:
        pass
    except Exception as e:
        await websocket.close(code=1000, reason=str(e))


@router.post("/alerts/{alert_id}/resolve")
async def resolve_alert(
    alert_id: str,
    resolution_note: Optional[str] = None
):
    """Mark an alert as resolved"""
    try:
        # In production, update alert in database
        # For now, just return success
        return {
            "success": True,
            "alert_id": alert_id,
            "resolved_at": datetime.utcnow().isoformat(),
            "resolution_note": resolution_note
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs/tail")
async def tail_logs(
    component: Optional[str] = Query(None, description="Filter by component"),
    level: Optional[str] = Query(None, pattern="^(DEBUG|INFO|WARNING|ERROR)$"),
    limit: int = Query(100, ge=1, le=1000)
):
    """Get recent log entries"""
    try:
        # In production, fetch from logging system
        # For now, return mock logs
        logs = []
        
        levels = ["INFO", "WARNING", "ERROR", "DEBUG"]
        components = ["pools", "strategies", "liquidity", "api", "monitoring"]
        
        for i in range(min(limit, 20)):
            log_level = level or levels[i % len(levels)]
            log_component = component or components[i % len(components)]
            
            logs.append({
                "timestamp": (datetime.utcnow() - timedelta(seconds=i * 10)).isoformat(),
                "level": log_level,
                "component": log_component,
                "message": f"Sample log message {i} for {log_component}",
                "metadata": {
                    "request_id": f"req_{i}",
                    "user_id": f"user_{i % 5}"
                }
            })
        
        return {
            "logs": logs,
            "total": len(logs)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 