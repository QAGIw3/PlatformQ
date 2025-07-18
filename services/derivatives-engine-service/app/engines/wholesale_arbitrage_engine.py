"""
Wholesale Arbitrage Engine

Identifies and executes arbitrage opportunities between wholesale and retail compute prices.
"""

from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
import numpy as np
from collections import defaultdict

from app.integrations import IgniteCache, PulsarEventPublisher, OracleAggregatorClient
from app.engines.partner_capacity_manager import (
    PartnerCapacityManager,
    PartnerProvider,
    CapacityInventory
)
from app.engines.compute_futures_engine import ComputeFuturesEngine

logger = logging.getLogger(__name__)


@dataclass
class ArbitrageOpportunity:
    """Represents an arbitrage opportunity"""
    opportunity_id: str
    provider: PartnerProvider
    resource_type: str
    region: str
    quantity: Decimal
    wholesale_price: Decimal
    retail_price: Decimal
    futures_price: Decimal
    expected_profit: Decimal
    profit_margin: Decimal
    risk_score: float
    confidence: float
    execution_window: timedelta
    created_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class ArbitrageExecution:
    """Record of arbitrage execution"""
    execution_id: str
    opportunity_id: str
    wholesale_order_id: str
    futures_contract_id: Optional[str]
    quantity_purchased: Decimal
    wholesale_cost: Decimal
    expected_revenue: Decimal
    actual_revenue: Optional[Decimal] = None
    execution_time: datetime = field(default_factory=datetime.utcnow)
    status: str = "pending"  # pending, completed, failed


class ArbitrageStrategy(Enum):
    """Arbitrage strategies"""
    SPOT_FUTURES = "spot_futures"  # Buy wholesale, sell futures
    CROSS_PROVIDER = "cross_provider"  # Price diff between providers
    TEMPORAL = "temporal"  # Time-based price differences
    REGIONAL = "regional"  # Geographic price differences
    QUALITY = "quality"  # Different SLA levels


class WholesaleArbitrageEngine:
    """Identifies and executes arbitrage between wholesale and futures prices"""
    
    def __init__(
        self,
        ignite: IgniteCache,
        pulsar: PulsarEventPublisher,
        oracle: OracleAggregatorClient,
        partner_manager: PartnerCapacityManager,
        futures_engine: ComputeFuturesEngine
    ):
        self.ignite = ignite
        self.pulsar = pulsar
        self.oracle = oracle
        self.partner_manager = partner_manager
        self.futures_engine = futures_engine
        
        # Configuration
        self.min_profit_threshold = Decimal("100")  # Minimum $100 profit
        self.min_profit_margin = Decimal("0.15")  # Minimum 15% margin
        self.max_risk_score = 0.7  # Maximum acceptable risk
        self.execution_delay = timedelta(seconds=5)  # Delay before execution
        
        # Tracking
        self.opportunities: Dict[str, ArbitrageOpportunity] = {}
        self.executions: Dict[str, ArbitrageExecution] = {}
        self.price_history: defaultdict = defaultdict(list)
        
        # Background tasks
        self._scanner_task = None
        self._executor_task = None
        self._monitor_task = None
        
    async def start(self):
        """Start arbitrage scanning and execution"""
        self._scanner_task = asyncio.create_task(self._scanner_loop())
        self._executor_task = asyncio.create_task(self._executor_loop())
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        
        logger.info("Wholesale arbitrage engine started")
        
    async def stop(self):
        """Stop arbitrage engine"""
        if self._scanner_task:
            self._scanner_task.cancel()
        if self._executor_task:
            self._executor_task.cancel()
        if self._monitor_task:
            self._monitor_task.cancel()
            
    async def analyze_arbitrage_opportunity(
        self,
        resource_type: str,
        region: str,
        quantity: Decimal,
        delivery_date: datetime
    ) -> Optional[ArbitrageOpportunity]:
        """Analyze potential arbitrage opportunity"""
        # Get wholesale prices from all providers
        provider, wholesale_price = await self.partner_manager.get_best_price(
            resource_type,
            region,
            quantity,
            timedelta(hours=24)  # Daily rate
        )
        
        if not provider:
            return None
            
        # Get current futures price
        futures_market = await self.futures_engine.get_day_ahead_market(
            delivery_date,
            resource_type
        )
        
        futures_price = futures_market.cleared_prices.get(
            delivery_date.hour,
            Decimal("0")
        )
        
        # Get retail price estimate
        retail_price = await self.oracle.get_price(f"{resource_type}_retail_{region}")
        if not retail_price:
            retail_price = futures_price * Decimal("1.2")  # 20% markup estimate
            
        # Calculate profit potential
        cost = wholesale_price * quantity
        futures_revenue = futures_price * quantity
        retail_revenue = retail_price * quantity
        
        expected_profit = max(futures_revenue, retail_revenue) - cost
        profit_margin = expected_profit / cost if cost > 0 else Decimal("0")
        
        if expected_profit < self.min_profit_threshold or profit_margin < self.min_profit_margin:
            return None
            
        # Calculate risk score
        risk_score = await self._calculate_risk_score(
            provider,
            resource_type,
            region,
            quantity,
            delivery_date
        )
        
        if risk_score > self.max_risk_score:
            return None
            
        # Calculate confidence
        confidence = await self._calculate_confidence(
            resource_type,
            region,
            wholesale_price,
            futures_price
        )
        
        opportunity = ArbitrageOpportunity(
            opportunity_id=f"ARB_{datetime.utcnow().timestamp()}",
            provider=provider,
            resource_type=resource_type,
            region=region,
            quantity=quantity,
            wholesale_price=wholesale_price,
            retail_price=retail_price,
            futures_price=futures_price,
            expected_profit=expected_profit,
            profit_margin=profit_margin,
            risk_score=risk_score,
            confidence=confidence,
            execution_window=timedelta(hours=1)
        )
        
        return opportunity
        
    async def execute_arbitrage(
        self,
        opportunity: ArbitrageOpportunity
    ) -> ArbitrageExecution:
        """Execute arbitrage opportunity"""
        execution = ArbitrageExecution(
            execution_id=f"EXEC_{datetime.utcnow().timestamp()}",
            opportunity_id=opportunity.opportunity_id,
            wholesale_order_id="",
            futures_contract_id=None,
            quantity_purchased=opportunity.quantity,
            wholesale_cost=opportunity.wholesale_price * opportunity.quantity,
            expected_revenue=opportunity.futures_price * opportunity.quantity
        )
        
        try:
            # Step 1: Purchase wholesale capacity
            purchase_order = await self.partner_manager.purchase_capacity(
                opportunity.provider,
                opportunity.resource_type,
                opportunity.region,
                opportunity.quantity,
                timedelta(hours=24),
                datetime.utcnow() + timedelta(hours=12)  # Start in 12 hours
            )
            
            if purchase_order.status != "confirmed":
                execution.status = "failed"
                return execution
                
            execution.wholesale_order_id = purchase_order.order_id
            
            # Step 2: Create futures contract to sell capacity
            futures_contract = await self.futures_engine.create_futures_contract(
                creator_id="platform_arbitrage",
                resource_type=opportunity.resource_type,
                quantity=opportunity.quantity,
                delivery_start=datetime.utcnow() + timedelta(hours=12),
                duration_hours=24,
                contract_months=1,
                location_zone=opportunity.region
            )
            
            execution.futures_contract_id = futures_contract["id"]
            execution.status = "completed"
            
            # Store execution
            self.executions[execution.execution_id] = execution
            await self._store_execution(execution)
            
            # Publish event
            await self.pulsar.publish(
                "persistent://platformq/arbitrage/execution-completed",
                {
                    "execution_id": execution.execution_id,
                    "opportunity_id": opportunity.opportunity_id,
                    "provider": opportunity.provider.value,
                    "quantity": str(opportunity.quantity),
                    "expected_profit": str(opportunity.expected_profit),
                    "status": execution.status
                }
            )
            
        except Exception as e:
            logger.error(f"Error executing arbitrage: {e}")
            execution.status = "failed"
            
        return execution
        
    async def get_arbitrage_metrics(self) -> Dict[str, Any]:
        """Get arbitrage performance metrics"""
        total_opportunities = len(self.opportunities)
        total_executions = len(self.executions)
        
        successful_executions = [
            e for e in self.executions.values()
            if e.status == "completed"
        ]
        
        total_profit = sum(
            e.expected_revenue - e.wholesale_cost
            for e in successful_executions
        )
        
        avg_profit_margin = Decimal("0")
        if successful_executions:
            margins = [
                (e.expected_revenue - e.wholesale_cost) / e.wholesale_cost
                for e in successful_executions
                if e.wholesale_cost > 0
            ]
            avg_profit_margin = sum(margins) / len(margins)
            
        return {
            "total_opportunities_found": total_opportunities,
            "total_executions": total_executions,
            "successful_executions": len(successful_executions),
            "success_rate": len(successful_executions) / total_executions if total_executions > 0 else 0,
            "total_profit": str(total_profit),
            "average_profit_margin": str(avg_profit_margin),
            "active_opportunities": len([o for o in self.opportunities.values() if o.created_at > datetime.utcnow() - timedelta(hours=1)])
        }
        
    # Private methods
    async def _calculate_risk_score(
        self,
        provider: PartnerProvider,
        resource_type: str,
        region: str,
        quantity: Decimal,
        delivery_date: datetime
    ) -> float:
        """Calculate risk score for arbitrage opportunity"""
        risk_factors = []
        
        # Provider reliability risk
        provider_reliability = {
            PartnerProvider.RACKSPACE: 0.1,
            PartnerProvider.AWS: 0.05,
            PartnerProvider.AZURE: 0.08,
            PartnerProvider.GCP: 0.07
        }
        risk_factors.append(provider_reliability.get(provider, 0.2))
        
        # Quantity risk (larger = riskier)
        if quantity > Decimal("1000"):
            risk_factors.append(0.3)
        elif quantity > Decimal("500"):
            risk_factors.append(0.2)
        else:
            risk_factors.append(0.1)
            
        # Time risk (further out = riskier)
        hours_until_delivery = (delivery_date - datetime.utcnow()).total_seconds() / 3600
        if hours_until_delivery > 168:  # More than a week
            risk_factors.append(0.4)
        elif hours_until_delivery > 48:
            risk_factors.append(0.2)
        else:
            risk_factors.append(0.1)
            
        # Price volatility risk
        volatility = await self._calculate_price_volatility(resource_type, region)
        risk_factors.append(min(volatility, 0.5))
        
        # Average risk score
        return sum(risk_factors) / len(risk_factors)
        
    async def _calculate_confidence(
        self,
        resource_type: str,
        region: str,
        wholesale_price: Decimal,
        futures_price: Decimal
    ) -> float:
        """Calculate confidence in arbitrage opportunity"""
        confidence_factors = []
        
        # Price spread confidence
        spread = futures_price - wholesale_price
        spread_ratio = spread / wholesale_price if wholesale_price > 0 else 0
        confidence_factors.append(min(float(spread_ratio) * 2, 1.0))
        
        # Historical success rate
        historical_success = await self._get_historical_success_rate(
            resource_type,
            region
        )
        confidence_factors.append(historical_success)
        
        # Market liquidity
        liquidity = await self._estimate_market_liquidity(resource_type, region)
        confidence_factors.append(liquidity)
        
        # Data freshness
        data_age = await self._get_price_data_age(resource_type, region)
        freshness = max(0, 1 - (data_age.total_seconds() / 3600))  # Decay over 1 hour
        confidence_factors.append(freshness)
        
        # Average confidence
        return sum(confidence_factors) / len(confidence_factors)
        
    async def _calculate_price_volatility(
        self,
        resource_type: str,
        region: str
    ) -> float:
        """Calculate historical price volatility"""
        key = f"{resource_type}:{region}"
        history = self.price_history[key]
        
        if len(history) < 10:
            return 0.5  # Default medium volatility
            
        prices = [float(h["price"]) for h in history[-30:]]  # Last 30 prices
        
        if len(prices) < 2:
            return 0.5
            
        # Calculate standard deviation / mean
        std_dev = np.std(prices)
        mean = np.mean(prices)
        
        if mean == 0:
            return 0.5
            
        volatility = std_dev / mean
        
        return min(volatility, 1.0)
        
    async def _get_historical_success_rate(
        self,
        resource_type: str,
        region: str
    ) -> float:
        """Get historical success rate for similar arbitrages"""
        successes = 0
        total = 0
        
        for execution in self.executions.values():
            # Find similar executions
            opp = self.opportunities.get(execution.opportunity_id)
            if opp and opp.resource_type == resource_type and opp.region == region:
                total += 1
                if execution.status == "completed":
                    successes += 1
                    
        if total == 0:
            return 0.7  # Default optimistic rate
            
        return successes / total
        
    async def _estimate_market_liquidity(
        self,
        resource_type: str,
        region: str
    ) -> float:
        """Estimate market liquidity"""
        # Check futures market depth
        market = await self.futures_engine.get_day_ahead_market(
            datetime.utcnow() + timedelta(days=1),
            resource_type
        )
        
        total_bids = sum(
            len(bids) for bids in market.bids.values()
        )
        
        total_offers = sum(
            len(offers) for offers in market.offers.values()
        )
        
        # Normalize to 0-1 scale
        liquidity = min((total_bids + total_offers) / 100, 1.0)
        
        return liquidity
        
    async def _get_price_data_age(
        self,
        resource_type: str,
        region: str
    ) -> timedelta:
        """Get age of price data"""
        key = f"{resource_type}:{region}"
        history = self.price_history[key]
        
        if not history:
            return timedelta(hours=24)  # Assume old if no data
            
        latest = history[-1]["timestamp"]
        return datetime.utcnow() - latest
        
    async def _scanner_loop(self):
        """Background task to scan for arbitrage opportunities"""
        while True:
            try:
                # Scan each resource type and region combination
                resource_types = ["gpu", "cpu", "memory", "storage"]
                regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
                
                for resource_type in resource_types:
                    for region in regions:
                        # Try different quantities
                        for quantity in [Decimal("10"), Decimal("50"), Decimal("100"), Decimal("500")]:
                            opportunity = await self.analyze_arbitrage_opportunity(
                                resource_type,
                                region,
                                quantity,
                                datetime.utcnow() + timedelta(days=1)
                            )
                            
                            if opportunity:
                                self.opportunities[opportunity.opportunity_id] = opportunity
                                await self._store_opportunity(opportunity)
                                
                                logger.info(
                                    f"Found arbitrage opportunity: {opportunity.opportunity_id} "
                                    f"Expected profit: ${opportunity.expected_profit}"
                                )
                                
                # Update price history
                await self._update_price_history()
                
                await asyncio.sleep(300)  # Scan every 5 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in arbitrage scanner: {e}")
                await asyncio.sleep(60)
                
    async def _executor_loop(self):
        """Background task to execute profitable arbitrages"""
        while True:
            try:
                # Find best opportunity
                best_opportunity = None
                best_score = 0
                
                for opp in self.opportunities.values():
                    # Skip if too old
                    if opp.created_at < datetime.utcnow() - opp.execution_window:
                        continue
                        
                    # Calculate score
                    score = float(opp.expected_profit) * opp.confidence * (1 - opp.risk_score)
                    
                    if score > best_score:
                        best_score = score
                        best_opportunity = opp
                        
                if best_opportunity:
                    # Execute with delay
                    await asyncio.sleep(self.execution_delay.total_seconds())
                    
                    execution = await self.execute_arbitrage(best_opportunity)
                    
                    logger.info(
                        f"Executed arbitrage: {execution.execution_id} "
                        f"Status: {execution.status}"
                    )
                    
                    # Remove from opportunities
                    del self.opportunities[best_opportunity.opportunity_id]
                    
                await asyncio.sleep(30)  # Check every 30 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in arbitrage executor: {e}")
                await asyncio.sleep(60)
                
    async def _monitor_loop(self):
        """Monitor executed arbitrages and update metrics"""
        while True:
            try:
                # Update actual revenue for completed executions
                for execution in self.executions.values():
                    if execution.status == "completed" and execution.actual_revenue is None:
                        # Check if futures contract was filled
                        if execution.futures_contract_id:
                            # In practice, check futures engine for fill status
                            execution.actual_revenue = execution.expected_revenue
                            await self._store_execution(execution)
                            
                # Publish metrics
                metrics = await self.get_arbitrage_metrics()
                await self.pulsar.publish(
                    "persistent://platformq/arbitrage/metrics",
                    metrics
                )
                
                # Clean old opportunities
                cutoff = datetime.utcnow() - timedelta(hours=24)
                old_opps = [
                    opp_id for opp_id, opp in self.opportunities.items()
                    if opp.created_at < cutoff
                ]
                for opp_id in old_opps:
                    del self.opportunities[opp_id]
                    
                await asyncio.sleep(600)  # Monitor every 10 minutes
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in arbitrage monitor: {e}")
                await asyncio.sleep(300)
                
    async def _update_price_history(self):
        """Update price history for all resources"""
        resource_types = ["gpu", "cpu", "memory", "storage"]
        regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
        
        for resource_type in resource_types:
            for region in regions:
                # Get current wholesale price
                provider, price = await self.partner_manager.get_best_price(
                    resource_type,
                    region,
                    Decimal("100"),
                    timedelta(hours=24)
                )
                
                if provider:
                    key = f"{resource_type}:{region}"
                    self.price_history[key].append({
                        "price": price,
                        "provider": provider.value,
                        "timestamp": datetime.utcnow()
                    })
                    
                    # Keep only last 100 prices
                    if len(self.price_history[key]) > 100:
                        self.price_history[key] = self.price_history[key][-100:]
                        
    async def _store_opportunity(self, opportunity: ArbitrageOpportunity):
        """Store opportunity in cache"""
        await self.ignite.set(
            f"arbitrage_opportunity:{opportunity.opportunity_id}",
            opportunity.__dict__
        )
        
    async def _store_execution(self, execution: ArbitrageExecution):
        """Store execution in cache"""
        await self.ignite.set(
            f"arbitrage_execution:{execution.execution_id}",
            execution.__dict__
        ) 