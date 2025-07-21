"""Market Making Strategies API endpoints"""

from typing import Dict, Any, List, Optional
from decimal import Decimal
from datetime import datetime
from enum import Enum

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.core.dependencies import get_ignite_client, get_redis_client, get_service_clients
from app.core.events import publish_event, EventType
from app.monitoring import active_strategies, strategy_pnl, order_latency
from app.config import settings

router = APIRouter()


class StrategyType(str, Enum):
    """Available strategy types"""
    GRID = "grid"
    CROSS_MARKET_ARBITRAGE = "cross_market_arbitrage"
    DELTA_NEUTRAL = "delta_neutral"
    VOLATILITY_ARBITRAGE = "volatility_arbitrage"
    LIQUIDITY_PROVISION = "liquidity_provision"
    CUSTOM = "custom"


class StrategyStatus(str, Enum):
    """Strategy status"""
    DEPLOYED = "deployed"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


class DeployStrategyRequest(BaseModel):
    """Request to deploy a market making strategy"""
    strategy_type: StrategyType
    name: str = Field(..., min_length=3, max_length=100)
    market_id: str = Field(..., description="Primary market/pool ID")
    
    # Common parameters
    capital_allocation: Decimal = Field(..., gt=0, description="Capital to allocate")
    max_position_size: Decimal = Field(..., gt=0)
    stop_loss_percent: Decimal = Field(default=5, ge=0, le=50)
    take_profit_percent: Optional[Decimal] = Field(None, ge=0, le=1000)
    
    # Grid strategy specific
    grid_levels: Optional[int] = Field(None, ge=2, le=100)
    grid_spacing_percent: Optional[Decimal] = Field(None, ge=0.1, le=10)
    
    # Arbitrage specific
    secondary_markets: Optional[List[str]] = Field(None, description="Additional markets for arb")
    min_profit_threshold: Optional[Decimal] = Field(None, ge=0)
    
    # Risk parameters
    max_drawdown_percent: Decimal = Field(default=10, ge=1, le=50)
    position_limit_percent: Decimal = Field(default=20, ge=1, le=100)
    
    # Custom parameters
    custom_params: Optional[Dict[str, Any]] = Field(None)


class UpdateStrategyRequest(BaseModel):
    """Request to update strategy parameters"""
    capital_adjustment: Optional[Decimal] = Field(None, description="Adjust capital (+/-)")
    max_position_size: Optional[Decimal] = Field(None, gt=0)
    stop_loss_percent: Optional[Decimal] = Field(None, ge=0, le=50)
    take_profit_percent: Optional[Decimal] = Field(None, ge=0, le=1000)
    custom_params: Optional[Dict[str, Any]] = Field(None)
    action: Optional[str] = Field(None, pattern="^(pause|resume|rebalance)$")


class StrategyResponse(BaseModel):
    """Strategy information response"""
    strategy_id: str
    strategy_type: str
    name: str
    status: str
    market_id: str
    capital_allocated: str
    capital_deployed: str
    total_pnl: str
    unrealized_pnl: str
    realized_pnl: str
    win_rate: float
    sharpe_ratio: float
    max_drawdown: str
    orders_placed: int
    orders_filled: int
    created_at: str
    last_updated: str
    
    # Performance metrics
    return_24h: str
    return_7d: str
    return_30d: str
    
    # Risk metrics
    current_exposure: str
    var_95: str
    position_count: int


@router.post("/deploy", response_model=StrategyResponse)
async def deploy_strategy(
    request: DeployStrategyRequest,
    user_id: str = Depends(lambda: "mock_user")
):
    """Deploy a new market making strategy"""
    try:
        # Generate strategy ID
        strategy_id = f"{request.strategy_type.value}_{request.market_id}_{int(datetime.utcnow().timestamp())}"
        
        # Store strategy data
        ignite = await get_ignite_client()
        strategy_cache = await ignite.get_or_create_cache("strategies")
        
        strategy_data = {
            "strategy_id": strategy_id,
            "strategy_type": request.strategy_type.value,
            "name": request.name,
            "status": StrategyStatus.DEPLOYED.value,
            "market_id": request.market_id,
            "user_id": user_id,
            "capital_allocated": str(request.capital_allocation),
            "capital_deployed": "0",
            "total_pnl": "0",
            "unrealized_pnl": "0",
            "realized_pnl": "0",
            "orders_placed": 0,
            "orders_filled": 0,
            "created_at": datetime.utcnow().isoformat(),
            "last_updated": datetime.utcnow().isoformat(),
            "parameters": {
                "max_position_size": str(request.max_position_size),
                "stop_loss_percent": str(request.stop_loss_percent),
                "take_profit_percent": str(request.take_profit_percent) if request.take_profit_percent else None,
                "max_drawdown_percent": str(request.max_drawdown_percent),
                "position_limit_percent": str(request.position_limit_percent),
                "grid_levels": request.grid_levels,
                "grid_spacing_percent": str(request.grid_spacing_percent) if request.grid_spacing_percent else None,
                "secondary_markets": request.secondary_markets,
                "min_profit_threshold": str(request.min_profit_threshold) if request.min_profit_threshold else None,
                "custom_params": request.custom_params
            },
            "performance": {
                "return_24h": "0",
                "return_7d": "0",
                "return_30d": "0",
                "win_rate": 0,
                "sharpe_ratio": 0,
                "max_drawdown": "0",
                "current_exposure": "0",
                "var_95": "0",
                "position_count": 0
            }
        }
        
        await strategy_cache.put(strategy_id, strategy_data)
        
        # Update metrics
        active_strategies.labels(strategy_type=request.strategy_type.value).inc()
        
        # Publish event
        await publish_event(
            EventType.STRATEGY_DEPLOYED,
            {
                "strategy_id": strategy_id,
                "strategy_type": request.strategy_type.value,
                "market_id": request.market_id,
                "capital_allocated": str(request.capital_allocation)
            },
            user_id=user_id
        )
        
        # Start strategy execution (in production, this would trigger actual strategy)
        strategy_data["status"] = StrategyStatus.RUNNING.value
        await strategy_cache.put(strategy_id, strategy_data)
        
        return StrategyResponse(
            strategy_id=strategy_id,
            strategy_type=request.strategy_type.value,
            name=request.name,
            status=StrategyStatus.RUNNING.value,
            market_id=request.market_id,
            capital_allocated=strategy_data["capital_allocated"],
            capital_deployed="0",
            total_pnl="0",
            unrealized_pnl="0",
            realized_pnl="0",
            win_rate=0,
            sharpe_ratio=0,
            max_drawdown="0",
            orders_placed=0,
            orders_filled=0,
            created_at=strategy_data["created_at"],
            last_updated=strategy_data["last_updated"],
            return_24h="0",
            return_7d="0",
            return_30d="0",
            current_exposure="0",
            var_95="0",
            position_count=0
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("", response_model=List[StrategyResponse])
async def list_strategies(
    strategy_type: Optional[StrategyType] = None,
    status: Optional[StrategyStatus] = None,
    market_id: Optional[str] = None,
    user_id: str = Depends(lambda: "mock_user")
):
    """List user's strategies"""
    try:
        ignite = await get_ignite_client()
        strategy_cache = await ignite.get_or_create_cache("strategies")
        
        strategies = []
        async for strat_id, strat_data in strategy_cache.scan():
            # Filter by user
            if strat_data.get("user_id") != user_id:
                continue
                
            # Apply filters
            if strategy_type and strat_data["strategy_type"] != strategy_type.value:
                continue
            if status and strat_data["status"] != status.value:
                continue
            if market_id and strat_data["market_id"] != market_id:
                continue
            
            perf = strat_data.get("performance", {})
            strategies.append(StrategyResponse(
                strategy_id=strat_id,
                strategy_type=strat_data["strategy_type"],
                name=strat_data["name"],
                status=strat_data["status"],
                market_id=strat_data["market_id"],
                capital_allocated=strat_data["capital_allocated"],
                capital_deployed=strat_data.get("capital_deployed", "0"),
                total_pnl=strat_data.get("total_pnl", "0"),
                unrealized_pnl=strat_data.get("unrealized_pnl", "0"),
                realized_pnl=strat_data.get("realized_pnl", "0"),
                win_rate=perf.get("win_rate", 0),
                sharpe_ratio=perf.get("sharpe_ratio", 0),
                max_drawdown=perf.get("max_drawdown", "0"),
                orders_placed=strat_data.get("orders_placed", 0),
                orders_filled=strat_data.get("orders_filled", 0),
                created_at=strat_data["created_at"],
                last_updated=strat_data["last_updated"],
                return_24h=perf.get("return_24h", "0"),
                return_7d=perf.get("return_7d", "0"),
                return_30d=perf.get("return_30d", "0"),
                current_exposure=perf.get("current_exposure", "0"),
                var_95=perf.get("var_95", "0"),
                position_count=perf.get("position_count", 0)
            ))
        
        return strategies
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{strategy_id}", response_model=StrategyResponse)
async def get_strategy(
    strategy_id: str,
    user_id: str = Depends(lambda: "mock_user")
):
    """Get strategy details"""
    try:
        ignite = await get_ignite_client()
        strategy_cache = await ignite.get_or_create_cache("strategies")
        
        strat_data = await strategy_cache.get(strategy_id)
        if not strat_data:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Verify ownership
        if strat_data.get("user_id") != user_id:
            raise HTTPException(status_code=403, detail="Not authorized")
        
        perf = strat_data.get("performance", {})
        return StrategyResponse(
            strategy_id=strategy_id,
            strategy_type=strat_data["strategy_type"],
            name=strat_data["name"],
            status=strat_data["status"],
            market_id=strat_data["market_id"],
            capital_allocated=strat_data["capital_allocated"],
            capital_deployed=strat_data.get("capital_deployed", "0"),
            total_pnl=strat_data.get("total_pnl", "0"),
            unrealized_pnl=strat_data.get("unrealized_pnl", "0"),
            realized_pnl=strat_data.get("realized_pnl", "0"),
            win_rate=perf.get("win_rate", 0),
            sharpe_ratio=perf.get("sharpe_ratio", 0),
            max_drawdown=perf.get("max_drawdown", "0"),
            orders_placed=strat_data.get("orders_placed", 0),
            orders_filled=strat_data.get("orders_filled", 0),
            created_at=strat_data["created_at"],
            last_updated=strat_data["last_updated"],
            return_24h=perf.get("return_24h", "0"),
            return_7d=perf.get("return_7d", "0"),
            return_30d=perf.get("return_30d", "0"),
            current_exposure=perf.get("current_exposure", "0"),
            var_95=perf.get("var_95", "0"),
            position_count=perf.get("position_count", 0)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{strategy_id}")
async def update_strategy(
    strategy_id: str,
    request: UpdateStrategyRequest,
    user_id: str = Depends(lambda: "mock_user")
):
    """Update strategy parameters"""
    try:
        ignite = await get_ignite_client()
        strategy_cache = await ignite.get_or_create_cache("strategies")
        
        strat_data = await strategy_cache.get(strategy_id)
        if not strat_data:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Verify ownership
        if strat_data.get("user_id") != user_id:
            raise HTTPException(status_code=403, detail="Not authorized")
        
        # Handle actions
        if request.action:
            if request.action == "pause":
                strat_data["status"] = StrategyStatus.PAUSED.value
            elif request.action == "resume":
                strat_data["status"] = StrategyStatus.RUNNING.value
            elif request.action == "rebalance":
                # Trigger rebalance (in production)
                pass
        
        # Update parameters
        if request.capital_adjustment:
            current_capital = Decimal(strat_data["capital_allocated"])
            new_capital = current_capital + request.capital_adjustment
            if new_capital <= 0:
                raise HTTPException(status_code=400, detail="Insufficient capital")
            strat_data["capital_allocated"] = str(new_capital)
        
        params = strat_data.get("parameters", {})
        if request.max_position_size:
            params["max_position_size"] = str(request.max_position_size)
        if request.stop_loss_percent is not None:
            params["stop_loss_percent"] = str(request.stop_loss_percent)
        if request.take_profit_percent is not None:
            params["take_profit_percent"] = str(request.take_profit_percent)
        if request.custom_params:
            params.update(request.custom_params)
        
        strat_data["parameters"] = params
        strat_data["last_updated"] = datetime.utcnow().isoformat()
        
        await strategy_cache.put(strategy_id, strat_data)
        
        # Publish event
        await publish_event(
            EventType.STRATEGY_UPDATED,
            {
                "strategy_id": strategy_id,
                "updates": request.dict(exclude_unset=True)
            },
            user_id=user_id
        )
        
        return {
            "success": True,
            "strategy_id": strategy_id,
            "status": strat_data["status"],
            "message": "Strategy updated successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{strategy_id}")
async def stop_strategy(
    strategy_id: str,
    user_id: str = Depends(lambda: "mock_user")
):
    """Stop and remove a strategy"""
    try:
        ignite = await get_ignite_client()
        strategy_cache = await ignite.get_or_create_cache("strategies")
        
        strat_data = await strategy_cache.get(strategy_id)
        if not strat_data:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Verify ownership
        if strat_data.get("user_id") != user_id:
            raise HTTPException(status_code=403, detail="Not authorized")
        
        # Update status
        strat_data["status"] = StrategyStatus.STOPPED.value
        strat_data["last_updated"] = datetime.utcnow().isoformat()
        
        await strategy_cache.put(strategy_id, strat_data)
        
        # Update metrics
        active_strategies.labels(strategy_type=strat_data["strategy_type"]).dec()
        
        # Publish event
        await publish_event(
            EventType.STRATEGY_STOPPED,
            {
                "strategy_id": strategy_id,
                "final_pnl": strat_data.get("total_pnl", "0")
            },
            user_id=user_id
        )
        
        return {
            "success": True,
            "strategy_id": strategy_id,
            "message": "Strategy stopped successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{strategy_id}/performance")
async def get_strategy_performance(
    strategy_id: str,
    period: str = Query("24h", pattern="^(1h|24h|7d|30d|all)$"),
    user_id: str = Depends(lambda: "mock_user")
):
    """Get detailed performance metrics for a strategy"""
    try:
        ignite = await get_ignite_client()
        strategy_cache = await ignite.get_or_create_cache("strategies")
        
        strat_data = await strategy_cache.get(strategy_id)
        if not strat_data:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Verify ownership
        if strat_data.get("user_id") != user_id:
            raise HTTPException(status_code=403, detail="Not authorized")
        
        # In production, fetch actual performance data
        # For now, return mock data
        perf = strat_data.get("performance", {})
        
        return {
            "strategy_id": strategy_id,
            "period": period,
            "metrics": {
                "total_return": perf.get(f"return_{period}", "0"),
                "sharpe_ratio": perf.get("sharpe_ratio", 0),
                "max_drawdown": perf.get("max_drawdown", "0"),
                "win_rate": perf.get("win_rate", 0),
                "profit_factor": 1.5,  # Mock
                "avg_win": "50",  # Mock
                "avg_loss": "30",  # Mock
                "best_trade": "500",  # Mock
                "worst_trade": "-200",  # Mock
                "total_trades": strat_data.get("orders_filled", 0),
                "winning_trades": int(strat_data.get("orders_filled", 0) * 0.6),  # Mock
                "losing_trades": int(strat_data.get("orders_filled", 0) * 0.4),  # Mock
            },
            "time_series": {
                "timestamps": [],  # Would contain actual timestamps
                "pnl": [],  # Would contain P&L values
                "equity": [],  # Would contain equity curve
                "drawdown": []  # Would contain drawdown values
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{strategy_id}/orders")
async def get_strategy_orders(
    strategy_id: str,
    status: Optional[str] = Query(None, pattern="^(pending|filled|cancelled)$"),
    limit: int = Query(100, ge=1, le=1000),
    user_id: str = Depends(lambda: "mock_user")
):
    """Get orders placed by a strategy"""
    try:
        # In production, fetch from order management system
        # For now, return mock data
        return {
            "strategy_id": strategy_id,
            "orders": [],
            "total": 0
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 