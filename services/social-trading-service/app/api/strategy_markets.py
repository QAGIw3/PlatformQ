"""
Strategy Markets API

Endpoints for creating and managing prediction markets on trading strategies.
"""

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, List, Optional, Any
from decimal import Decimal
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.integrations.prediction_markets import (
    PredictionMarketsIntegration,
    StrategyMarketConfig,
    MarketType
)
from app.auth import get_current_user
from platformq_shared import ProcessingStatus

router = APIRouter(
    prefix="/api/v1/strategy-markets",
    tags=["strategy-markets"]
)

# Initialize integration
markets_integration = PredictionMarketsIntegration()


class CreateStrategyMarketRequest(BaseModel):
    """Request to create a prediction market for a strategy"""
    strategy_id: str
    market_type: MarketType
    target_value: Decimal = Field(..., description="Target value for the metric")
    time_period_days: int = Field(..., ge=1, le=365)
    initial_liquidity: Decimal = Field(..., ge=100)
    fee_tier: Optional[Decimal] = Field(0.02, ge=0, le=0.1)
    metadata: Optional[Dict[str, Any]] = None


class CreateCompetitionRequest(BaseModel):
    """Request to create a strategy competition market"""
    strategy_ids: List[str] = Field(..., min_items=2, max_items=20)
    competition_days: int = Field(..., ge=7, le=90)
    prize_pool: Decimal = Field(..., ge=1000)


class HedgeStrategyRequest(BaseModel):
    """Request to create hedging markets for a strategy"""
    strategy_id: str
    risk_metrics: Dict[str, float]


@router.post("/create")
async def create_strategy_market(
    request: CreateStrategyMarketRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create a prediction market for a trading strategy's performance
    
    Allows traders to create markets on their own strategies where users
    can bet on performance metrics like returns, drawdowns, or consistency.
    """
    try:
        config = StrategyMarketConfig(
            market_type=request.market_type,
            target_value=request.target_value,
            time_period=timedelta(days=request.time_period_days),
            initial_liquidity=request.initial_liquidity,
            fee_tier=request.fee_tier
        )
        
        result = await markets_integration.create_strategy_market(
            tenant_id=current_user["tenant_id"],
            strategy_id=request.strategy_id,
            trader_id=current_user["user_id"],
            config=config,
            metadata=request.metadata
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "market": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/competition")
async def create_strategy_competition(
    request: CreateCompetitionRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Create a categorical prediction market for strategy competition
    
    Users can bet on which strategy will perform best over the competition period.
    """
    try:
        result = await markets_integration.create_strategy_competition_market(
            tenant_id=current_user["tenant_id"],
            strategy_ids=request.strategy_ids,
            competition_period=timedelta(days=request.competition_days),
            prize_pool=request.prize_pool
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "competition": result.data
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/hedge")
async def hedge_strategy_risks(
    request: HedgeStrategyRequest,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Automatically create prediction markets to hedge strategy risks
    
    Based on risk metrics, creates appropriate markets for drawdown protection,
    volatility hedging, etc.
    """
    try:
        # Add trader_id to risk metrics
        risk_metrics = request.risk_metrics.copy()
        risk_metrics["trader_id"] = current_user["user_id"]
        
        result = await markets_integration.hedge_strategy_risk_with_markets(
            tenant_id=current_user["tenant_id"],
            strategy_id=request.strategy_id,
            risk_metrics=risk_metrics
        )
        
        if result.status == ProcessingStatus.SUCCESS:
            return {
                "status": "success",
                "hedging_markets": result.data["hedging_markets"]
            }
        else:
            raise HTTPException(status_code=400, detail=result.error)
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sentiment/{strategy_id}")
async def get_strategy_sentiment(
    strategy_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get market sentiment about a strategy from prediction markets
    
    Returns aggregated sentiment scores based on market prices and volumes.
    """
    try:
        sentiment = await markets_integration.get_strategy_market_sentiment(
            tenant_id=current_user["tenant_id"],
            strategy_id=strategy_id
        )
        
        if "error" in sentiment:
            raise HTTPException(status_code=400, detail=sentiment["error"])
            
        return {
            "status": "success",
            "strategy_id": strategy_id,
            "sentiment": sentiment
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/markets/{strategy_id}")
async def list_strategy_markets(
    strategy_id: str,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    List all prediction markets for a specific strategy
    """
    try:
        # Use the prediction markets client to fetch markets
        client = markets_integration.prediction_client
        response = await client.get(
            "/api/v1/markets",
            params={
                "tenant_id": current_user["tenant_id"],
                "metadata.strategy_id": strategy_id
            }
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail="Failed to fetch markets"
            )
            
        return {
            "status": "success",
            "strategy_id": strategy_id,
            "markets": response.json().get("markets", [])
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opportunities")
async def get_market_opportunities(
    min_liquidity: Optional[Decimal] = 1000,
    max_spread: Optional[float] = 0.1,
    current_user=Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Find arbitrage or betting opportunities in strategy markets
    
    Returns markets with significant price inefficiencies or high potential returns.
    """
    try:
        # Query prediction markets for opportunities
        client = markets_integration.prediction_client
        response = await client.get(
            "/api/v1/markets",
            params={
                "tenant_id": current_user["tenant_id"],
                "category": "trading_performance",
                "status": "active",
                "min_liquidity": float(min_liquidity)
            }
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail="Failed to fetch markets"
            )
            
        markets = response.json().get("markets", [])
        
        # Filter for opportunities
        opportunities = []
        for market in markets:
            prices = market.get("current_prices", {})
            
            # Check for price inefficiencies
            if market["market_type"] == "binary":
                yes_price = prices.get("yes", 0.5)
                no_price = prices.get("no", 0.5)
                
                # Check if prices don't sum to ~1 (arbitrage opportunity)
                if abs((yes_price + no_price) - 1.0) > max_spread:
                    opportunities.append({
                        "market_id": market["market_id"],
                        "type": "arbitrage",
                        "yes_price": yes_price,
                        "no_price": no_price,
                        "spread": abs((yes_price + no_price) - 1.0),
                        "strategy_id": market["metadata"].get("strategy_id")
                    })
                    
                # Check for extreme prices (high conviction bets)
                elif yes_price < 0.2 or yes_price > 0.8:
                    opportunities.append({
                        "market_id": market["market_id"],
                        "type": "high_conviction",
                        "yes_price": yes_price,
                        "conviction": "bearish" if yes_price < 0.2 else "bullish",
                        "strategy_id": market["metadata"].get("strategy_id")
                    })
        
        return {
            "status": "success",
            "opportunities": opportunities,
            "total_markets_analyzed": len(markets)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) 