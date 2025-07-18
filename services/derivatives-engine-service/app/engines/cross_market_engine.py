"""
Cross-Market Integration Engine

Enables sophisticated trading strategies across all PlatformQ markets.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any, Set
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import pandas as pd
from collections import defaultdict
import logging

from app.integrations import (
    IgniteCache,
    PulsarEventPublisher,
    OracleAggregatorClient,
    DigitalAssetServiceClient,
    GraphIntelligenceClient
)
from app.models.market import Market
from app.models.order import Order, OrderSide

logger = logging.getLogger(__name__)


@dataclass
class ArbitrageOpportunity:
    """Represents an arbitrage opportunity across markets"""
    id: str
    markets: List[str]
    strategy_type: str
    expected_profit: Decimal
    required_capital: Decimal
    risk_score: float
    execution_steps: List[Dict]
    expires_at: datetime
    confidence: float


@dataclass
class CrossMarketPosition:
    """Position across multiple related markets"""
    position_id: str
    user_id: str
    strategy: str
    legs: List[Dict]  # Market positions
    total_collateral: Decimal
    unrealized_pnl: Decimal
    correlation_hedge_ratio: float
    created_at: datetime


class CrossMarketEngine:
    """
    Manages cross-market strategies and correlations
    """
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        asset_service: DigitalAssetServiceClient,
        graph_service: GraphIntelligenceClient
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.asset_service = asset_service
        self.graph_service = graph_service
        
        # Market correlations
        self.correlation_matrix: Optional[pd.DataFrame] = None
        self.correlation_window = 30  # days
        
        # Active strategies
        self.active_arbitrage: Dict[str, ArbitrageOpportunity] = {}
        self.cross_positions: Dict[str, CrossMarketPosition] = {}
        
        # Market relationships
        self.market_dependencies = {
            "compute_gpu": ["ai_model_training", "rendering_jobs"],
            "ai_model": ["compute_gpu", "training_data"],
            "training_data": ["storage_capacity", "data_quality"],
            "digital_asset": ["compute_rendering", "storage_capacity"]
        }
        
    async def start_monitoring(self):
        """Start monitoring for cross-market opportunities"""
        asyncio.create_task(self._arbitrage_scanner())
        asyncio.create_task(self._correlation_updater())
        asyncio.create_task(self._position_manager())
        
    async def _arbitrage_scanner(self):
        """Continuously scan for arbitrage opportunities"""
        while True:
            try:
                # Scan different arbitrage types
                opportunities = []
                
                # 1. Compute-Storage Arbitrage
                compute_storage = await self._scan_compute_storage_arbitrage()
                opportunities.extend(compute_storage)
                
                # 2. Model-Data-Compute Triangle
                triangle = await self._scan_triangle_arbitrage()
                opportunities.extend(triangle)
                
                # 3. Cross-Chain Price Differences
                cross_chain = await self._scan_cross_chain_arbitrage()
                opportunities.extend(cross_chain)
                
                # 4. Time-based Arbitrage (Futures vs Spot)
                temporal = await self._scan_temporal_arbitrage()
                opportunities.extend(temporal)
                
                # Filter and rank opportunities
                ranked = self._rank_opportunities(opportunities)
                
                # Update active opportunities
                for opp in ranked[:10]:  # Top 10
                    self.active_arbitrage[opp.id] = opp
                    
                    # Publish opportunity
                    await self.pulsar.publish(
                        "cross-market-arbitrage",
                        {
                            "opportunity": opp.__dict__,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                    )
                    
            except Exception as e:
                logger.error(f"Error in arbitrage scanner: {e}")
                
            await asyncio.sleep(5)  # Scan every 5 seconds
            
    async def _scan_compute_storage_arbitrage(self) -> List[ArbitrageOpportunity]:
        """
        Arbitrage between compute and storage markets
        
        Strategy: When compute is expensive relative to storage,
        pre-compute and cache results
        """
        opportunities = []
        
        # Get current prices
        compute_price = await self.oracle.get_price("compute_gpu_hour")
        storage_price = await self.oracle.get_price("storage_gb_month")
        
        if not compute_price or not storage_price:
            return opportunities
            
        # Calculate relative value
        # 1 GPU hour vs storing 100GB for a month
        compute_storage_ratio = compute_price / (storage_price * 100)
        historical_ratio = await self._get_historical_ratio("compute_storage")
        
        if compute_storage_ratio > historical_ratio * Decimal("1.2"):  # 20% above normal
            # Compute is expensive - opportunity to pre-compute and store
            opportunity = ArbitrageOpportunity(
                id=f"cs_arb_{datetime.utcnow().timestamp()}",
                markets=["compute_gpu_futures", "storage_futures"],
                strategy_type="compute_storage_arbitrage",
                expected_profit=self._calculate_expected_profit(
                    compute_price, storage_price, compute_storage_ratio
                ),
                required_capital=compute_price * 100,  # 100 GPU hours
                risk_score=0.3,  # Low risk
                execution_steps=[
                    {
                        "action": "buy",
                        "market": "storage_futures",
                        "quantity": 10000,  # 10TB
                        "duration": "3_months"
                    },
                    {
                        "action": "sell",
                        "market": "compute_gpu_futures",
                        "quantity": 100,
                        "delivery": "1_month"
                    },
                    {
                        "action": "execute",
                        "operation": "pre_compute_common_workloads"
                    }
                ],
                expires_at=datetime.utcnow() + timedelta(minutes=5),
                confidence=0.85
            )
            opportunities.append(opportunity)
            
        return opportunities
        
    async def _scan_triangle_arbitrage(self) -> List[ArbitrageOpportunity]:
        """
        Triangle arbitrage between Model -> Data -> Compute -> Model
        """
        opportunities = []
        
        # Get prices for the triangle
        model_price = await self.oracle.get_price("ai_model_gpt_equivalent")
        data_price = await self.oracle.get_price("training_data_tb")
        compute_price = await self.oracle.get_price("compute_gpu_hour")
        
        if not all([model_price, data_price, compute_price]):
            return opportunities
            
        # Calculate if we can profit from the cycle
        # Buy data + compute to create model, then sell model
        model_creation_cost = (data_price * 10) + (compute_price * 1000)  # 10TB data, 1000 GPU hours
        model_market_price = model_price
        
        profit_margin = (model_market_price - model_creation_cost) / model_creation_cost
        
        if profit_margin > Decimal("0.15"):  # 15% profit margin
            opportunity = ArbitrageOpportunity(
                id=f"triangle_mdc_{datetime.utcnow().timestamp()}",
                markets=["ai_model_market", "data_market", "compute_market"],
                strategy_type="model_creation_arbitrage",
                expected_profit=model_market_price - model_creation_cost,
                required_capital=model_creation_cost,
                risk_score=0.6,  # Medium risk due to execution complexity
                execution_steps=[
                    {
                        "action": "buy",
                        "market": "training_data_futures",
                        "quantity": 10,  # 10TB
                        "quality_score": "> 0.9"
                    },
                    {
                        "action": "buy",
                        "market": "compute_gpu_futures",
                        "quantity": 1000,
                        "gpu_type": "A100"
                    },
                    {
                        "action": "execute",
                        "operation": "train_model",
                        "estimated_time": "7_days"
                    },
                    {
                        "action": "sell",
                        "market": "ai_model_market",
                        "expected_price": model_market_price
                    }
                ],
                expires_at=datetime.utcnow() + timedelta(hours=1),
                confidence=0.7
            )
            opportunities.append(opportunity)
            
        return opportunities
        
    async def _scan_cross_chain_arbitrage(self) -> List[ArbitrageOpportunity]:
        """
        Arbitrage same assets across different chains
        """
        opportunities = []
        
        # Check major assets across chains
        assets = ["platform_token", "compute_token", "data_token"]
        chains = ["ethereum", "polygon", "arbitrum"]
        
        for asset in assets:
            prices = {}
            for chain in chains:
                price = await self.oracle.get_price(f"{asset}_{chain}")
                if price:
                    prices[chain] = price
                    
            if len(prices) < 2:
                continue
                
            # Find price differences
            max_chain = max(prices, key=prices.get)
            min_chain = min(prices, key=prices.get)
            
            price_diff = (prices[max_chain] - prices[min_chain]) / prices[min_chain]
            
            # Account for bridge fees (typically 0.1-0.3%)
            bridge_fee = Decimal("0.003")
            
            if price_diff > bridge_fee * 2:  # 2x bridge fees for profit
                opportunity = ArbitrageOpportunity(
                    id=f"cross_chain_{asset}_{datetime.utcnow().timestamp()}",
                    markets=[f"{asset}_{min_chain}", f"{asset}_{max_chain}"],
                    strategy_type="cross_chain_arbitrage",
                    expected_profit=(price_diff - bridge_fee) * prices[min_chain] * 1000,
                    required_capital=prices[min_chain] * 1000,
                    risk_score=0.4,  # Low-medium risk
                    execution_steps=[
                        {
                            "action": "buy",
                            "market": f"{asset}_{min_chain}",
                            "quantity": 1000
                        },
                        {
                            "action": "bridge",
                            "from_chain": min_chain,
                            "to_chain": max_chain,
                            "estimated_time": "15_minutes"
                        },
                        {
                            "action": "sell",
                            "market": f"{asset}_{max_chain}",
                            "quantity": 1000
                        }
                    ],
                    expires_at=datetime.utcnow() + timedelta(minutes=10),
                    confidence=0.9
                )
                opportunities.append(opportunity)
                
        return opportunities
        
    async def _scan_temporal_arbitrage(self) -> List[ArbitrageOpportunity]:
        """
        Arbitrage between spot and futures markets
        """
        opportunities = []
        
        # Check compute futures vs spot
        spot_compute = await self.oracle.get_price("compute_gpu_spot")
        futures_compute_1m = await self.oracle.get_price("compute_gpu_futures_1m")
        
        if spot_compute and futures_compute_1m:
            # Calculate implied carry cost
            time_to_expiry = 30  # days
            implied_rate = ((futures_compute_1m / spot_compute) - 1) * (365 / time_to_expiry)
            
            # If implied rate is too high, we can do cash-and-carry
            if implied_rate > Decimal("0.20"):  # 20% annualized
                opportunity = ArbitrageOpportunity(
                    id=f"temporal_compute_{datetime.utcnow().timestamp()}",
                    markets=["compute_gpu_spot", "compute_gpu_futures_1m"],
                    strategy_type="cash_and_carry",
                    expected_profit=futures_compute_1m - spot_compute - (spot_compute * Decimal("0.02")),
                    required_capital=spot_compute * 100,
                    risk_score=0.2,  # Very low risk
                    execution_steps=[
                        {
                            "action": "buy",
                            "market": "compute_gpu_spot",
                            "quantity": 100
                        },
                        {
                            "action": "sell",
                            "market": "compute_gpu_futures_1m",
                            "quantity": 100
                        },
                        {
                            "action": "hold",
                            "duration": "30_days"
                        }
                    ],
                    expires_at=datetime.utcnow() + timedelta(minutes=30),
                    confidence=0.95
                )
                opportunities.append(opportunity)
                
        return opportunities
        
    async def _correlation_updater(self):
        """Update correlation matrix between markets"""
        while True:
            try:
                # Get price histories
                markets = [
                    "compute_gpu", "compute_cpu", "storage",
                    "ai_model", "training_data", "digital_asset",
                    "platform_token"
                ]
                
                price_data = {}
                for market in markets:
                    history = await self._get_price_history(market, self.correlation_window)
                    if history:
                        price_data[market] = history
                        
                if len(price_data) > 2:
                    # Calculate returns
                    df = pd.DataFrame(price_data)
                    returns = df.pct_change().dropna()
                    
                    # Calculate correlation matrix
                    self.correlation_matrix = returns.corr()
                    
                    # Find highly correlated pairs for pairs trading
                    await self._identify_pairs_trading_opportunities()
                    
            except Exception as e:
                logger.error(f"Error updating correlations: {e}")
                
            await asyncio.sleep(3600)  # Update hourly
            
    async def _identify_pairs_trading_opportunities(self):
        """Identify pairs trading opportunities from correlation matrix"""
        if self.correlation_matrix is None:
            return
            
        # Find highly correlated pairs (> 0.8)
        high_corr_pairs = []
        
        for i in range(len(self.correlation_matrix.columns)):
            for j in range(i+1, len(self.correlation_matrix.columns)):
                corr = self.correlation_matrix.iloc[i, j]
                if corr > 0.8:
                    market1 = self.correlation_matrix.columns[i]
                    market2 = self.correlation_matrix.columns[j]
                    high_corr_pairs.append((market1, market2, corr))
                    
        # Check for divergence in highly correlated pairs
        for market1, market2, correlation in high_corr_pairs:
            await self._check_pairs_divergence(market1, market2, correlation)
            
    async def _check_pairs_divergence(self, market1: str, market2: str, correlation: float):
        """Check if a correlated pair has diverged"""
        # Get recent prices
        price1 = await self.oracle.get_price(market1)
        price2 = await self.oracle.get_price(market2)
        
        if not price1 or not price2:
            return
            
        # Get historical ratio
        hist_ratio = await self._get_historical_ratio(f"{market1}_{market2}")
        current_ratio = price1 / price2
        
        # Check for significant divergence (> 2 standard deviations)
        if abs(current_ratio - hist_ratio) > hist_ratio * Decimal("0.1"):
            # Create pairs trading opportunity
            if current_ratio > hist_ratio:
                # Market1 expensive relative to market2
                long_market = market2
                short_market = market1
            else:
                long_market = market1
                short_market = market2
                
            opportunity = ArbitrageOpportunity(
                id=f"pairs_{market1}_{market2}_{datetime.utcnow().timestamp()}",
                markets=[long_market, short_market],
                strategy_type="pairs_trading",
                expected_profit=abs(current_ratio - hist_ratio) * price2 * 100,
                required_capital=(price1 + price2) * 100,
                risk_score=0.3 + (1 - correlation) * 0.5,  # Risk based on correlation
                execution_steps=[
                    {
                        "action": "buy",
                        "market": long_market,
                        "quantity": 100
                    },
                    {
                        "action": "sell",
                        "market": short_market,
                        "quantity": 100 * float(current_ratio)
                    },
                    {
                        "action": "monitor",
                        "target_ratio": float(hist_ratio),
                        "stop_loss": 0.05  # 5% stop loss
                    }
                ],
                expires_at=datetime.utcnow() + timedelta(hours=4),
                confidence=correlation * 0.9
            )
            
            self.active_arbitrage[opportunity.id] = opportunity
            
    async def create_cross_market_position(
        self,
        user_id: str,
        strategy: str,
        legs: List[Dict],
        collateral: Decimal
    ) -> CrossMarketPosition:
        """Create a position across multiple markets"""
        position = CrossMarketPosition(
            position_id=f"xm_{user_id}_{datetime.utcnow().timestamp()}",
            user_id=user_id,
            strategy=strategy,
            legs=legs,
            total_collateral=collateral,
            unrealized_pnl=Decimal("0"),
            correlation_hedge_ratio=await self._calculate_hedge_ratio(legs),
            created_at=datetime.utcnow()
        )
        
        self.cross_positions[position.position_id] = position
        
        # Execute trades for each leg
        for leg in legs:
            await self._execute_leg(position.position_id, leg)
            
        return position
        
    async def _execute_leg(self, position_id: str, leg: Dict):
        """Execute a single leg of a cross-market position"""
        # Create order for the leg
        order = Order(
            market_id=leg["market"],
            user_id=self.cross_positions[position_id].user_id,
            side=OrderSide[leg["side"].upper()],
            size=Decimal(str(leg["quantity"])),
            order_type=leg.get("order_type", "market")
        )
        
        # Submit to matching engine
        # This would integrate with the main matching engine
        
    async def _calculate_hedge_ratio(self, legs: List[Dict]) -> float:
        """Calculate optimal hedge ratio for cross-market position"""
        if len(legs) < 2:
            return 1.0
            
        # Use correlation matrix to calculate hedge ratios
        if self.correlation_matrix is not None:
            # Simplified - would use more sophisticated methods
            markets = [leg["market"] for leg in legs]
            
            if all(m in self.correlation_matrix.columns for m in markets):
                # Calculate variance-minimizing hedge ratio
                var1 = self.correlation_matrix.loc[markets[0], markets[0]]
                var2 = self.correlation_matrix.loc[markets[1], markets[1]]
                cov = self.correlation_matrix.loc[markets[0], markets[1]]
                
                hedge_ratio = cov / var2
                return float(hedge_ratio)
                
        return 1.0
        
    async def _position_manager(self):
        """Manage cross-market positions"""
        while True:
            try:
                for position_id, position in self.cross_positions.items():
                    # Update PnL
                    pnl = await self._calculate_position_pnl(position)
                    position.unrealized_pnl = pnl
                    
                    # Check for rebalancing needs
                    if await self._needs_rebalancing(position):
                        await self._rebalance_position(position)
                        
                    # Check stop loss / take profit
                    await self._check_position_limits(position)
                    
            except Exception as e:
                logger.error(f"Error in position manager: {e}")
                
            await asyncio.sleep(10)
            
    async def _calculate_position_pnl(self, position: CrossMarketPosition) -> Decimal:
        """Calculate PnL for cross-market position"""
        total_pnl = Decimal("0")
        
        for leg in position.legs:
            current_price = await self.oracle.get_price(leg["market"])
            if current_price:
                entry_price = Decimal(str(leg["entry_price"]))
                quantity = Decimal(str(leg["quantity"]))
                
                if leg["side"] == "buy":
                    leg_pnl = (current_price - entry_price) * quantity
                else:
                    leg_pnl = (entry_price - current_price) * quantity
                    
                total_pnl += leg_pnl
                
        return total_pnl
        
    async def _needs_rebalancing(self, position: CrossMarketPosition) -> bool:
        """Check if position needs rebalancing"""
        if position.strategy == "pairs_trading":
            # Check if ratio has moved significantly
            leg1 = position.legs[0]
            leg2 = position.legs[1]
            
            price1 = await self.oracle.get_price(leg1["market"])
            price2 = await self.oracle.get_price(leg2["market"])
            
            if price1 and price2:
                current_ratio = price1 / price2
                target_ratio = Decimal(str(leg1["target_ratio"]))
                
                if abs(current_ratio - target_ratio) / target_ratio > Decimal("0.05"):
                    return True
                    
        return False
        
    async def _rebalance_position(self, position: CrossMarketPosition):
        """Rebalance a cross-market position"""
        logger.info(f"Rebalancing position {position.position_id}")
        
        # Calculate required adjustments
        # This would create new orders to rebalance
        
    async def _check_position_limits(self, position: CrossMarketPosition):
        """Check stop loss and take profit levels"""
        pnl_percentage = position.unrealized_pnl / position.total_collateral
        
        # Stop loss at -5%
        if pnl_percentage < Decimal("-0.05"):
            await self._close_position(position, "stop_loss")
            
        # Take profit at strategy-specific levels
        if position.strategy == "pairs_trading" and pnl_percentage > Decimal("0.10"):
            await self._close_position(position, "take_profit")
            
    async def _close_position(self, position: CrossMarketPosition, reason: str):
        """Close a cross-market position"""
        logger.info(f"Closing position {position.position_id} due to {reason}")
        
        # Create closing orders for each leg
        for leg in position.legs:
            closing_side = "sell" if leg["side"] == "buy" else "buy"
            await self._execute_leg(position.position_id, {
                "market": leg["market"],
                "side": closing_side,
                "quantity": leg["quantity"],
                "order_type": "market"
            })
            
        # Remove from active positions
        del self.cross_positions[position.position_id]
        
    def _rank_opportunities(self, opportunities: List[ArbitrageOpportunity]) -> List[ArbitrageOpportunity]:
        """Rank opportunities by risk-adjusted return"""
        for opp in opportunities:
            # Calculate Sharpe ratio equivalent
            expected_return = opp.expected_profit / opp.required_capital
            opp.score = expected_return / (opp.risk_score + 0.01) * opp.confidence
            
        return sorted(opportunities, key=lambda x: x.score, reverse=True)
        
    def _calculate_expected_profit(
        self,
        price1: Decimal,
        price2: Decimal,
        ratio: Decimal
    ) -> Decimal:
        """Calculate expected profit for arbitrage"""
        # Simplified calculation
        return (ratio - Decimal("1")) * min(price1, price2) * 100
        
    async def _get_historical_ratio(self, pair: str) -> Decimal:
        """Get historical average ratio for a pair"""
        # In practice, would query historical data
        return Decimal("1.5")  # Placeholder
        
    async def _get_price_history(self, market: str, days: int) -> List[float]:
        """Get price history for a market"""
        # In practice, would query time series data
        # Return synthetic data for now
        return [float(100 + i + np.random.randn() * 10) for i in range(days)] 