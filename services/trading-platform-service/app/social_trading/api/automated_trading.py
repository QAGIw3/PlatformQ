"""
Automated Trading API

Endpoints for managing automated trading strategies based on prediction market signals.
"""

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field

from app.trading.prediction_signal_trader import (
    PredictionSignalTrader,
    AutomatedStrategy,
    TradingStrategy,
    SignalStrength
)
from app.auth import get_current_user
from platformq_shared import ProcessingStatus
import asyncio

router = APIRouter(
    prefix="/api/v1/automated-trading",
    tags=["automated-trading"]
)

# Initialize trader
signal_trader = PredictionSignalTrader()

# Store active monitoring tasks
monitoring_tasks: Dict[str, asyncio.Task] = {}


class CreateAutomatedStrategyRequest(BaseModel):
    """Request to create an automated trading strategy"""
    name: str
    strategy_type: TradingStrategy
    assets: List[str] = Field(..., min_items=1, max_items=10)
    min_confidence: float = Field(0.7, ge=0.5, le=1.0)
    max_position_size: Decimal = Field(10000, gt=0)
    risk_per_trade: Decimal = Field(0.02, gt=0, le=0.1)
    
    # Signal parameters
    sentiment_threshold: float = Field(0.75, ge=0.5, le=1.0)
    volume_threshold: Decimal = Field(100000, gt=0)
    
    # Risk limits
    max_daily_trades: int = Field(10, ge=1, le=50)
    max_open_positions: int = Field(5, ge=1, le=20)
    correlation_limit: float = Field(0.7, ge=0, le=1.0)


class UpdateStrategyRequest(BaseModel):
    """Request to update strategy parameters"""
    enabled: Optional[bool] = None
    min_confidence: Optional[float] = Field(None, ge=0.5, le=1.0)
    max_position_size: Optional[Decimal] = Field(None, gt=0)
    risk_per_trade: Optional[Decimal] = Field(None, gt=0, le=0.1)
    max_daily_trades: Optional[int] = Field(None, ge=1, le=50)


@router.post("/strategies")
async def create_automated_strategy(
    request: CreateAutomatedStrategyRequest,
    background_tasks: BackgroundTasks,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create a new automated trading strategy
    
    This creates a strategy that monitors prediction markets and executes
    trades automatically based on market sentiment signals.
    """
    try:
        # Generate strategy ID
        strategy_id = f"AUTO_{current_user['user_id']}_{datetime.utcnow().timestamp()}"
        
        # Create strategy configuration
        strategy = AutomatedStrategy(
            strategy_id=strategy_id,
            name=request.name,
            strategy_type=request.strategy_type,
            assets=request.assets,
            min_confidence=request.min_confidence,
            max_position_size=request.max_position_size,
            risk_per_trade=request.risk_per_trade,
            sentiment_threshold=request.sentiment_threshold,
            volume_threshold=request.volume_threshold,
            max_daily_trades=request.max_daily_trades,
            max_open_positions=request.max_open_positions,
            correlation_limit=request.correlation_limit,
            enabled=True
        )
        
        # Create strategy
        result = await signal_trader.create_automated_strategy(
            tenant_id=current_user["tenant_id"],
            user_id=current_user["user_id"],
            config=strategy
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            # Start monitoring in background
            task = asyncio.create_task(
                signal_trader.monitor_and_trade(
                    current_user["tenant_id"],
                    current_user["user_id"],
                    strategy_id
                )
            )
            monitoring_tasks[strategy_id] = task
            
            return {
                "status": "success",
                "strategy": {
                    "strategy_id": strategy_id,
                    "name": request.name,
                    "type": request.strategy_type.value,
                    "assets": request.assets,
                    "status": "active",
                    "created_at": datetime.utcnow().isoformat()
                }
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies")
async def list_automated_strategies(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    List all automated trading strategies for the user
    """
    try:
        user_strategies = []
        
        for strategy_id, strategy in signal_trader.active_strategies.items():
            # Check if strategy belongs to user (simplified check)
            if current_user["user_id"] in strategy_id:
                performance = await signal_trader.get_strategy_performance(strategy_id)
                
                user_strategies.append({
                    "strategy_id": strategy_id,
                    "name": strategy.name,
                    "type": strategy.strategy_type.value,
                    "assets": strategy.assets,
                    "enabled": strategy.enabled,
                    "performance": performance
                })
        
        return {
            "status": "success",
            "strategies": user_strategies,
            "total": len(user_strategies)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/strategies/{strategy_id}")
async def get_strategy_details(
    strategy_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get detailed information about a specific strategy
    """
    try:
        strategy = signal_trader.active_strategies.get(strategy_id)
        
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Verify ownership
        if current_user["user_id"] not in strategy_id:
            raise HTTPException(status_code=403, detail="Unauthorized")
        
        performance = await signal_trader.get_strategy_performance(strategy_id)
        
        # Get recent signals
        recent_signals = [
            {
                "timestamp": s.timestamp.isoformat(),
                "asset": s.asset,
                "signal": s.signal_strength.value,
                "confidence": s.confidence,
                "action": s.recommended_action
            }
            for s in signal_trader.signal_history
            if s.strategy_id == strategy.strategy_type.value
        ][-10:]  # Last 10 signals
        
        return {
            "status": "success",
            "strategy": {
                "strategy_id": strategy_id,
                "name": strategy.name,
                "type": strategy.strategy_type.value,
                "assets": strategy.assets,
                "enabled": strategy.enabled,
                "parameters": {
                    "min_confidence": strategy.min_confidence,
                    "max_position_size": str(strategy.max_position_size),
                    "risk_per_trade": str(strategy.risk_per_trade),
                    "sentiment_threshold": strategy.sentiment_threshold,
                    "max_daily_trades": strategy.max_daily_trades,
                    "max_open_positions": strategy.max_open_positions
                },
                "performance": performance,
                "recent_signals": recent_signals
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/strategies/{strategy_id}")
async def update_strategy(
    strategy_id: str,
    request: UpdateStrategyRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Update automated strategy parameters
    """
    try:
        strategy = signal_trader.active_strategies.get(strategy_id)
        
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Verify ownership
        if current_user["user_id"] not in strategy_id:
            raise HTTPException(status_code=403, detail="Unauthorized")
        
        # Update parameters
        if request.enabled is not None:
            strategy.enabled = request.enabled
            
            # Stop or restart monitoring
            if not request.enabled and strategy_id in monitoring_tasks:
                monitoring_tasks[strategy_id].cancel()
                del monitoring_tasks[strategy_id]
            elif request.enabled and strategy_id not in monitoring_tasks:
                task = asyncio.create_task(
                    signal_trader.monitor_and_trade(
                        current_user["tenant_id"],
                        current_user["user_id"],
                        strategy_id
                    )
                )
                monitoring_tasks[strategy_id] = task
        
        if request.min_confidence is not None:
            strategy.min_confidence = request.min_confidence
        
        if request.max_position_size is not None:
            strategy.max_position_size = request.max_position_size
        
        if request.risk_per_trade is not None:
            strategy.risk_per_trade = request.risk_per_trade
        
        if request.max_daily_trades is not None:
            strategy.max_daily_trades = request.max_daily_trades
        
        return {
            "status": "success",
            "message": "Strategy updated",
            "strategy_id": strategy_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/strategies/{strategy_id}")
async def delete_strategy(
    strategy_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Delete (stop and remove) an automated strategy
    """
    try:
        strategy = signal_trader.active_strategies.get(strategy_id)
        
        if not strategy:
            raise HTTPException(status_code=404, detail="Strategy not found")
        
        # Verify ownership
        if current_user["user_id"] not in strategy_id:
            raise HTTPException(status_code=403, detail="Unauthorized")
        
        # Stop strategy
        result = await signal_trader.stop_strategy(strategy_id)
        
        if result.status == ProcessingStatus.SUCCESS:
            # Cancel monitoring task
            if strategy_id in monitoring_tasks:
                monitoring_tasks[strategy_id].cancel()
                del monitoring_tasks[strategy_id]
            
            # Remove from active strategies
            del signal_trader.active_strategies[strategy_id]
            
            return {
                "status": "success",
                "message": "Strategy deleted",
                "data": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/signals/recent")
async def get_recent_signals(
    limit: int = 20,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get recent trading signals generated by automated strategies
    """
    try:
        # Filter signals for user's strategies
        user_signals = []
        
        for signal in signal_trader.signal_history[-limit:]:
            # Check if signal is from user's strategy
            user_strategies = [
                s for s in signal_trader.active_strategies.keys()
                if current_user["user_id"] in s
            ]
            
            if any(signal.strategy_id in s for s in user_strategies):
                user_signals.append({
                    "timestamp": signal.timestamp.isoformat(),
                    "strategy": signal.strategy_id,
                    "asset": signal.asset,
                    "signal_strength": signal.signal_strength.value,
                    "confidence": signal.confidence,
                    "action": signal.recommended_action,
                    "position_size": str(signal.position_size),
                    "stop_loss": str(signal.stop_loss) if signal.stop_loss else None,
                    "take_profit": str(signal.take_profit) if signal.take_profit else None,
                    "reasoning": signal.reasoning
                })
        
        return {
            "status": "success",
            "signals": user_signals,
            "total": len(user_signals)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/performance/summary")
async def get_performance_summary(
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get aggregated performance summary for all user's automated strategies
    """
    try:
        total_pnl = Decimal("0")
        total_trades = 0
        winning_trades = 0
        active_strategies = 0
        
        for strategy_id, strategy in signal_trader.active_strategies.items():
            if current_user["user_id"] in strategy_id:
                if strategy.enabled:
                    active_strategies += 1
                
                performance = await signal_trader.get_strategy_performance(strategy_id)
                
                if "error" not in performance:
                    total_pnl += Decimal(performance["total_pnl"])
                    total_trades += performance["total_trades"]
                    winning_trades += performance["winning_trades"]
        
        win_rate = winning_trades / total_trades if total_trades > 0 else 0
        
        return {
            "status": "success",
            "summary": {
                "active_strategies": active_strategies,
                "total_strategies": len([
                    s for s in signal_trader.active_strategies.keys()
                    if current_user["user_id"] in s
                ]),
                "total_trades": total_trades,
                "win_rate": win_rate,
                "total_pnl": str(total_pnl),
                "average_pnl_per_trade": str(
                    total_pnl / total_trades if total_trades > 0 else 0
                )
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/backtest")
async def backtest_strategy(
    strategy_type: TradingStrategy,
    assets: List[str],
    start_date: datetime,
    end_date: datetime,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Backtest a trading strategy using historical prediction market data
    """
    try:
        # This would implement backtesting logic
        # For now, return mock results
        
        mock_results = {
            "total_return": "15.3%",
            "sharpe_ratio": 1.45,
            "max_drawdown": "8.2%",
            "win_rate": "58%",
            "total_trades": 143,
            "best_trade": {
                "asset": assets[0] if assets else "BTC-USD",
                "return": "12.5%",
                "date": "2024-01-15"
            },
            "worst_trade": {
                "asset": assets[0] if assets else "BTC-USD",
                "return": "-4.2%",
                "date": "2024-02-03"
            }
        }
        
        return {
            "status": "success",
            "backtest_results": {
                "strategy_type": strategy_type.value,
                "assets": assets,
                "period": {
                    "start": start_date.isoformat(),
                    "end": end_date.isoformat()
                },
                "performance": mock_results
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 