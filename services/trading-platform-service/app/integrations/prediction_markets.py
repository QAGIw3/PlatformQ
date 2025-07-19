"""
Prediction Markets Integration for Social Trading

Enables traders to create prediction markets on their own strategy performance,
allowing the community to bet on trader success and creating additional incentives.
"""

import asyncio
from typing import Dict, List, Optional, Any, Tuple
from decimal import Decimal
from datetime import datetime, timedelta
from enum import Enum
import logging
import httpx
from dataclasses import dataclass

from platformq_shared import ServiceClient, ProcessingResult, ProcessingStatus

logger = logging.getLogger(__name__)


class MarketType(Enum):
    """Types of prediction markets for trading strategies"""
    MONTHLY_RETURN = "monthly_return"  # Will strategy achieve X% return this month?
    DRAWDOWN = "drawdown"  # Will strategy exceed Y% drawdown?
    SHARPE_RATIO = "sharpe_ratio"  # Will strategy maintain Sharpe > Z?
    OUTPERFORM_INDEX = "outperform_index"  # Will strategy beat benchmark?
    CONSISTENCY = "consistency"  # Will trader maintain win rate?
    AUM_MILESTONE = "aum_milestone"  # Will strategy reach AUM target?


@dataclass
class StrategyMarketConfig:
    """Configuration for a strategy prediction market"""
    market_type: MarketType
    target_value: Decimal
    time_period: timedelta
    initial_liquidity: Decimal
    fee_tier: Decimal = Decimal("0.02")  # 2% default fee
    

class PredictionMarketsIntegration:
    """
    Integrates social trading with prediction markets platform
    """
    
    def __init__(self):
        self.prediction_client = ServiceClient(
            service_name="prediction-markets-service",
            circuit_breaker_threshold=5,
            rate_limit=50.0
        )
        
        self.derivatives_client = ServiceClient(
            service_name="derivatives-engine-service",
            circuit_breaker_threshold=5,
            rate_limit=100.0
        )
        
    async def create_strategy_market(
        self,
        tenant_id: str,
        strategy_id: str,
        trader_id: str,
        config: StrategyMarketConfig,
        metadata: Optional[Dict[str, Any]] = None
    ) -> ProcessingResult:
        """
        Create a prediction market for a trading strategy's performance
        """
        try:
            # Get current strategy performance
            strategy_data = await self._get_strategy_performance(tenant_id, strategy_id)
            
            # Generate market description
            description = self._generate_market_description(
                strategy_data,
                config,
                trader_id
            )
            
            # Determine resolution criteria
            resolution_source = self._get_resolution_source(config.market_type)
            
            # Create the prediction market
            market_request = {
                "tenant_id": tenant_id,
                "title": f"Strategy {strategy_id} - {config.market_type.value}",
                "description": description,
                "market_type": "scalar" if config.market_type in [
                    MarketType.MONTHLY_RETURN,
                    MarketType.SHARPE_RATIO
                ] else "binary",
                "category": "trading_performance",
                "resolution_source": resolution_source,
                "resolution_date": datetime.utcnow() + config.time_period,
                "target_value": float(config.target_value),
                "initial_liquidity": float(config.initial_liquidity),
                "fee_tier": float(config.fee_tier),
                "metadata": {
                    "strategy_id": strategy_id,
                    "trader_id": trader_id,
                    "market_type": config.market_type.value,
                    "current_performance": strategy_data,
                    **(metadata or {})
                }
            }
            
            response = await self.prediction_client.post(
                "/api/v1/markets",
                json=market_request
            )
            
            if response.status_code == 201:
                market_data = response.json()
                
                # Create derivatives on the market shares for hedging
                await self._create_market_derivatives(
                    tenant_id,
                    market_data["market_id"],
                    strategy_id
                )
                
                logger.info(
                    f"Created prediction market {market_data['market_id']} "
                    f"for strategy {strategy_id}"
                )
                
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    data=market_data
                )
            else:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=f"Failed to create market: {response.text}"
                )
                
        except Exception as e:
            logger.error(f"Error creating strategy market: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def create_strategy_competition_market(
        self,
        tenant_id: str,
        strategy_ids: List[str],
        competition_period: timedelta,
        prize_pool: Decimal
    ) -> ProcessingResult:
        """
        Create a categorical market for strategy competition
        """
        try:
            # Get all strategy performances
            strategies_data = await asyncio.gather(*[
                self._get_strategy_performance(tenant_id, sid)
                for sid in strategy_ids
            ])
            
            # Create categorical market
            market_request = {
                "tenant_id": tenant_id,
                "title": f"Strategy Competition - {len(strategy_ids)} Strategies",
                "description": self._generate_competition_description(
                    strategies_data,
                    competition_period,
                    prize_pool
                ),
                "market_type": "categorical",
                "category": "strategy_competition",
                "options": [
                    {
                        "id": sid,
                        "name": f"Strategy {sid}",
                        "description": f"Current return: {data.get('total_return', 0):.2%}"
                    }
                    for sid, data in zip(strategy_ids, strategies_data)
                ],
                "resolution_source": "automated",
                "resolution_date": datetime.utcnow() + competition_period,
                "initial_liquidity": float(prize_pool * Decimal("0.1")),  # 10% of prize
                "metadata": {
                    "competition_type": "highest_return",
                    "prize_pool": float(prize_pool),
                    "participating_strategies": strategy_ids
                }
            }
            
            response = await self.prediction_client.post(
                "/api/v1/markets",
                json=market_request
            )
            
            if response.status_code == 201:
                return ProcessingResult(
                    status=ProcessingStatus.SUCCESS,
                    data=response.json()
                )
            else:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=response.text
                )
                
        except Exception as e:
            logger.error(f"Error creating competition market: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def hedge_strategy_risk_with_markets(
        self,
        tenant_id: str,
        strategy_id: str,
        risk_metrics: Dict[str, float]
    ) -> ProcessingResult:
        """
        Create prediction markets to hedge strategy risks
        """
        try:
            hedging_markets = []
            
            # Create drawdown protection market
            if risk_metrics.get("max_drawdown_risk", 0) > 0.15:
                drawdown_market = await self.create_strategy_market(
                    tenant_id,
                    strategy_id,
                    risk_metrics["trader_id"],
                    StrategyMarketConfig(
                        market_type=MarketType.DRAWDOWN,
                        target_value=Decimal("0.20"),  # 20% drawdown threshold
                        time_period=timedelta(days=30),
                        initial_liquidity=Decimal(risk_metrics["aum"]) * Decimal("0.01")
                    )
                )
                if drawdown_market.status == ProcessingStatus.SUCCESS:
                    hedging_markets.append(drawdown_market.data)
            
            # Create volatility hedge market
            if risk_metrics.get("volatility", 0) > 0.25:
                vol_market = await self.create_strategy_market(
                    tenant_id,
                    strategy_id,
                    risk_metrics["trader_id"],
                    StrategyMarketConfig(
                        market_type=MarketType.SHARPE_RATIO,
                        target_value=Decimal("1.0"),  # Sharpe ratio target
                        time_period=timedelta(days=90),
                        initial_liquidity=Decimal(risk_metrics["aum"]) * Decimal("0.005")
                    )
                )
                if vol_market.status == ProcessingStatus.SUCCESS:
                    hedging_markets.append(vol_market.data)
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={"hedging_markets": hedging_markets}
            )
            
        except Exception as e:
            logger.error(f"Error creating hedging markets: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def get_strategy_market_sentiment(
        self,
        tenant_id: str,
        strategy_id: str
    ) -> Dict[str, Any]:
        """
        Get market sentiment about a strategy from prediction markets
        """
        try:
            # Query all markets related to this strategy
            response = await self.prediction_client.get(
                "/api/v1/markets",
                params={
                    "tenant_id": tenant_id,
                    "metadata.strategy_id": strategy_id,
                    "status": "active"
                }
            )
            
            if response.status_code != 200:
                return {"error": "Failed to fetch markets"}
            
            markets = response.json()["markets"]
            
            # Analyze sentiment from market prices
            sentiment_data = {
                "bullish_score": 0,
                "bearish_score": 0,
                "confidence_level": 0,
                "market_predictions": []
            }
            
            for market in markets:
                prices = market.get("current_prices", {})
                
                if market["market_type"] == "binary":
                    yes_price = prices.get("yes", 0.5)
                    sentiment_data["market_predictions"].append({
                        "market_id": market["market_id"],
                        "type": market["metadata"]["market_type"],
                        "bullish_probability": yes_price,
                        "volume": market.get("total_volume", 0)
                    })
                    
                    # Weight by volume
                    volume_weight = min(market.get("total_volume", 0) / 10000, 1.0)
                    if yes_price > 0.5:
                        sentiment_data["bullish_score"] += yes_price * volume_weight
                    else:
                        sentiment_data["bearish_score"] += (1 - yes_price) * volume_weight
            
            # Normalize scores
            total_score = sentiment_data["bullish_score"] + sentiment_data["bearish_score"]
            if total_score > 0:
                sentiment_data["bullish_score"] /= total_score
                sentiment_data["bearish_score"] /= total_score
                sentiment_data["confidence_level"] = min(total_score / len(markets), 1.0)
            
            return sentiment_data
            
        except Exception as e:
            logger.error(f"Error getting market sentiment: {str(e)}")
            return {"error": str(e)}
    
    async def _get_strategy_performance(
        self,
        tenant_id: str,
        strategy_id: str
    ) -> Dict[str, Any]:
        """Get current strategy performance metrics"""
        # This would typically call the social trading service
        # For now, return mock data
        return {
            "total_return": 0.15,  # 15%
            "monthly_return": 0.03,  # 3%
            "sharpe_ratio": 1.5,
            "max_drawdown": 0.08,  # 8%
            "win_rate": 0.65,  # 65%
            "aum": 1000000  # $1M
        }
    
    async def _create_market_derivatives(
        self,
        tenant_id: str,
        market_id: str,
        strategy_id: str
    ) -> None:
        """Create derivatives on prediction market shares for hedging"""
        try:
            # Create options on market shares
            derivatives_request = {
                "tenant_id": tenant_id,
                "underlying_asset": f"MARKET_SHARE_{market_id}",
                "derivative_type": "binary_option",
                "metadata": {
                    "market_id": market_id,
                    "strategy_id": strategy_id,
                    "purpose": "hedging"
                }
            }
            
            await self.derivatives_client.post(
                "/api/v1/derivatives/create-custom",
                json=derivatives_request
            )
            
        except Exception as e:
            logger.error(f"Error creating market derivatives: {str(e)}")
    
    def _generate_market_description(
        self,
        strategy_data: Dict[str, Any],
        config: StrategyMarketConfig,
        trader_id: str
    ) -> str:
        """Generate detailed market description"""
        descriptions = {
            MarketType.MONTHLY_RETURN: (
                f"Will strategy achieve {config.target_value}% return in the next "
                f"{config.time_period.days} days? Current monthly return: "
                f"{strategy_data.get('monthly_return', 0):.2%}"
            ),
            MarketType.DRAWDOWN: (
                f"Will strategy experience a drawdown exceeding {config.target_value}%? "
                f"Current max drawdown: {strategy_data.get('max_drawdown', 0):.2%}"
            ),
            MarketType.SHARPE_RATIO: (
                f"Will strategy maintain a Sharpe ratio above {config.target_value}? "
                f"Current Sharpe: {strategy_data.get('sharpe_ratio', 0):.2f}"
            ),
            MarketType.OUTPERFORM_INDEX: (
                f"Will strategy outperform the benchmark by {config.target_value}%? "
                f"Current outperformance: {strategy_data.get('alpha', 0):.2%}"
            ),
            MarketType.CONSISTENCY: (
                f"Will trader maintain win rate above {config.target_value}%? "
                f"Current win rate: {strategy_data.get('win_rate', 0):.2%}"
            ),
            MarketType.AUM_MILESTONE: (
                f"Will strategy reach ${config.target_value:,.0f} AUM? "
                f"Current AUM: ${strategy_data.get('aum', 0):,.0f}"
            )
        }
        
        return descriptions.get(
            config.market_type,
            f"Market for strategy performance metric {config.market_type.value}"
        )
    
    def _generate_competition_description(
        self,
        strategies_data: List[Dict[str, Any]],
        period: timedelta,
        prize_pool: Decimal
    ) -> str:
        """Generate competition market description"""
        return (
            f"Which strategy will achieve the highest return over the next "
            f"{period.days} days? Prize pool: ${prize_pool:,.2f}. "
            f"Current leader: {max(strategies_data, key=lambda x: x.get('total_return', 0)).get('total_return', 0):.2%}"
        )
    
    def _get_resolution_source(self, market_type: MarketType) -> str:
        """Determine resolution source for market type"""
        automated_types = [
            MarketType.MONTHLY_RETURN,
            MarketType.DRAWDOWN,
            MarketType.SHARPE_RATIO,
            MarketType.AUM_MILESTONE
        ]
        
        return "automated" if market_type in automated_types else "oracle" 