"""
Arbitrage Detector Service
"""
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import uuid
from pyignite import Client

from ..models.aggregation import (
    ArbitrageOpportunity, ArbitrageExecution, ArbitrageType,
    ResourceType, ArbitrageSearchRequest
)
from ..core.market_client import MarketClient
from ..config import settings


logger = logging.getLogger(__name__)


class ArbitrageDetector:
    """Detects and executes arbitrage opportunities across compute markets"""
    
    def __init__(self, market_client: MarketClient):
        self.market_client = market_client
        self.ignite_client = None
        self.arbitrage_cache = None
        self.active_opportunities = {}
        
    async def initialize(self):
        """Initialize arbitrage detector"""
        try:
            self.ignite_client = Client()
            self.ignite_client.connect(settings.IGNITE_HOST, settings.IGNITE_PORT)
            self.arbitrage_cache = self.ignite_client.get_or_create_cache(
                settings.IGNITE_CACHE_ARBITRAGE
            )
            logger.info("Arbitrage detector initialized")
        except Exception as e:
            logger.error(f"Failed to initialize arbitrage detector: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup connections"""
        if self.ignite_client:
            self.ignite_client.close()
    
    async def search_arbitrage_opportunities(
        self,
        request: ArbitrageSearchRequest
    ) -> List[ArbitrageOpportunity]:
        """Search for arbitrage opportunities"""
        opportunities = []
        
        try:
            # Search each resource type
            resource_types = request.resource_types or list(ResourceType)
            
            for resource_type in resource_types:
                if resource_type == ResourceType.QUANTUM:
                    quantum_opps = await self._search_quantum_arbitrage(request)
                    opportunities.extend(quantum_opps)
                elif resource_type == ResourceType.AI:
                    ai_opps = await self._search_ai_arbitrage(request)
                    opportunities.extend(ai_opps)
                elif resource_type == ResourceType.NETWORK:
                    network_opps = await self._search_network_arbitrage(request)
                    opportunities.extend(network_opps)
            
            # Filter by criteria
            filtered = []
            for opp in opportunities:
                if opp.profit_margin >= request.min_profit_margin:
                    if opp.risk_score <= request.max_risk_score:
                        if opp.expires_at > datetime.utcnow() + timedelta(minutes=request.time_horizon_minutes):
                            filtered.append(opp)
            
            # Sort by profit potential
            filtered.sort(key=lambda x: x.potential_profit, reverse=True)
            
            # Cache opportunities
            for opp in filtered:
                self.arbitrage_cache.put(
                    opp.opportunity_id,
                    opp.dict(),
                    ttl=int((opp.expires_at - datetime.utcnow()).total_seconds())
                )
                self.active_opportunities[opp.opportunity_id] = opp
            
            return filtered
            
        except Exception as e:
            logger.error(f"Failed to search arbitrage opportunities: {e}")
            return []
    
    async def _search_quantum_arbitrage(
        self,
        request: ArbitrageSearchRequest
    ) -> List[ArbitrageOpportunity]:
        """Search for quantum resource arbitrage"""
        opportunities = []
        
        try:
            # Get spot prices
            spot_resources = await self.market_client.get_quantum_spot_prices()
            
            # Get futures prices
            futures_resources = await self.market_client.get_quantum_futures_prices()
            
            # Compare prices
            for spot in spot_resources:
                qpu_id = spot['qpu_id']
                
                # Find matching futures
                matching_futures = [
                    f for f in futures_resources 
                    if f['qpu_id'] == qpu_id
                ]
                
                for future in matching_futures:
                    # Calculate arbitrage opportunity
                    spot_price = spot['price_per_minute']
                    future_price = future['price_per_minute']
                    
                    if spot_price != future_price:
                        # Determine arbitrage direction
                        if spot_price < future_price:
                            # Buy spot, sell futures
                            buy_market = "spot"
                            sell_market = "futures"
                            buy_price = spot_price
                            sell_price = future_price
                        else:
                            # Buy futures, sell spot (if possible)
                            buy_market = "futures"
                            sell_market = "spot"
                            buy_price = future_price
                            sell_price = spot_price
                        
                        # Calculate profit
                        quantity = future.get('min_minutes', 10)  # Minimum quantum time
                        potential_profit = (sell_price - buy_price) * quantity
                        profit_margin = (sell_price - buy_price) / buy_price
                        
                        if profit_margin >= request.min_profit_margin:
                            # Assess risk
                            risk_score = self._calculate_risk_score(
                                resource_type=ResourceType.QUANTUM,
                                price_volatility=spot.get('volatility', 0.1),
                                time_to_expiry=future.get('expiry_hours', 24),
                                market_depth=spot.get('available_minutes', 1000)
                            )
                            
                            opportunity = ArbitrageOpportunity(
                                opportunity_id=f"arb_q_{uuid.uuid4().hex[:8]}",
                                arbitrage_type=ArbitrageType.PRICE_DIFFERENTIAL,
                                resource_type=ResourceType.QUANTUM,
                                resource_id=qpu_id,
                                market_a=buy_market,
                                market_b=sell_market,
                                price_a=buy_price,
                                price_b=sell_price,
                                quantity=quantity,
                                potential_profit=potential_profit,
                                profit_margin=profit_margin,
                                expires_at=datetime.utcnow() + timedelta(hours=future.get('expiry_hours', 24)),
                                confidence=0.8,
                                execution_time_estimate=5.0,
                                risk_score=risk_score
                            )
                            
                            opportunities.append(opportunity)
            
            # Check quality arbitrage
            quality_opportunities = await self._search_quality_arbitrage_quantum()
            opportunities.extend(quality_opportunities)
            
            return opportunities
            
        except Exception as e:
            logger.error(f"Failed to search quantum arbitrage: {e}")
            return []
    
    async def _search_ai_arbitrage(
        self,
        request: ArbitrageSearchRequest
    ) -> List[ArbitrageOpportunity]:
        """Search for AI accelerator arbitrage"""
        opportunities = []
        
        try:
            # Get different market prices
            spot_prices = await self.market_client.get_ai_spot_prices()
            reserved_prices = await self.market_client.get_ai_reserved_prices()
            
            # Compare spot vs reserved
            for spot in spot_prices:
                accelerator_id = spot['accelerator_id']
                accelerator_type = spot['type']
                
                # Find matching reserved instances
                matching_reserved = [
                    r for r in reserved_prices 
                    if r['accelerator_id'] == accelerator_id or r['type'] == accelerator_type
                ]
                
                for reserved in matching_reserved:
                    # Calculate effective hourly rate for reserved
                    reservation_hours = reserved.get('reservation_hours', 720)  # 30 days default
                    upfront_cost = reserved.get('upfront_cost', 0)
                    hourly_rate = reserved.get('hourly_rate', 0)
                    
                    effective_hourly = (upfront_cost / reservation_hours) + hourly_rate
                    spot_hourly = spot['price_per_hour']
                    
                    # Check if arbitrage opportunity exists
                    if effective_hourly < spot_hourly * 0.7:  # 30% cheaper threshold
                        # Calculate profit for reselling reserved capacity
                        quantity = reservation_hours
                        potential_profit = (spot_hourly - effective_hourly) * quantity * 0.8  # 80% utilization
                        profit_margin = (spot_hourly - effective_hourly) / effective_hourly
                        
                        if profit_margin >= request.min_profit_margin:
                            risk_score = self._calculate_risk_score(
                                resource_type=ResourceType.AI,
                                price_volatility=spot.get('volatility', 0.15),
                                time_to_expiry=reservation_hours,
                                market_depth=spot.get('available_hours', 10000)
                            )
                            
                            opportunity = ArbitrageOpportunity(
                                opportunity_id=f"arb_ai_{uuid.uuid4().hex[:8]}",
                                arbitrage_type=ArbitrageType.TIME_ARBITRAGE,
                                resource_type=ResourceType.AI,
                                resource_id=accelerator_id,
                                market_a="reserved",
                                market_b="spot_resale",
                                price_a=effective_hourly,
                                price_b=spot_hourly,
                                quantity=quantity,
                                potential_profit=potential_profit,
                                profit_margin=profit_margin,
                                expires_at=datetime.utcnow() + timedelta(hours=24),
                                confidence=0.75,
                                execution_time_estimate=300.0,  # 5 minutes for reservation
                                risk_score=risk_score
                            )
                            
                            opportunities.append(opportunity)
            
            return opportunities
            
        except Exception as e:
            logger.error(f"Failed to search AI arbitrage: {e}")
            return []
    
    async def _search_network_arbitrage(
        self,
        request: ArbitrageSearchRequest
    ) -> List[ArbitrageOpportunity]:
        """Search for network bandwidth arbitrage"""
        opportunities = []
        
        try:
            # Get different QoS pricing
            paths = await self.market_client.get_network_paths()
            
            for path in paths:
                path_id = path['path_id']
                
                # Get pricing for different QoS levels
                qos_pricing = await self.market_client.get_network_qos_pricing(path_id)
                
                # Check for QoS arbitrage
                if len(qos_pricing) >= 2:
                    # Sort by price
                    sorted_qos = sorted(qos_pricing, key=lambda x: x['price_per_mbps_hour'])
                    
                    for i in range(len(sorted_qos) - 1):
                        lower_qos = sorted_qos[i]
                        higher_qos = sorted_qos[i + 1]
                        
                        # Check if performance difference justifies price difference
                        price_ratio = higher_qos['price_per_mbps_hour'] / lower_qos['price_per_mbps_hour']
                        quality_ratio = higher_qos.get('quality_score', 80) / lower_qos.get('quality_score', 70)
                        
                        if price_ratio > 1.5 and quality_ratio < 1.2:  # Overpriced higher QoS
                            # Arbitrage: Use lower QoS with redundancy
                            quantity = 1000  # 1 Gbps
                            duration = 24  # hours
                            
                            lower_cost = lower_qos['price_per_mbps_hour'] * quantity * duration * 2  # 2x for redundancy
                            higher_cost = higher_qos['price_per_mbps_hour'] * quantity * duration
                            
                            potential_profit = higher_cost - lower_cost
                            profit_margin = potential_profit / lower_cost
                            
                            if profit_margin >= request.min_profit_margin:
                                opportunity = ArbitrageOpportunity(
                                    opportunity_id=f"arb_net_{uuid.uuid4().hex[:8]}",
                                    arbitrage_type=ArbitrageType.QUALITY_ARBITRAGE,
                                    resource_type=ResourceType.NETWORK,
                                    resource_id=path_id,
                                    market_a=f"qos_{lower_qos['qos_class']}",
                                    market_b=f"qos_{higher_qos['qos_class']}",
                                    price_a=lower_qos['price_per_mbps_hour'],
                                    price_b=higher_qos['price_per_mbps_hour'],
                                    quantity=quantity * duration,
                                    potential_profit=potential_profit,
                                    profit_margin=profit_margin,
                                    expires_at=datetime.utcnow() + timedelta(hours=48),
                                    confidence=0.7,
                                    execution_time_estimate=10.0,
                                    risk_score=0.3
                                )
                                
                                opportunities.append(opportunity)
            
            return opportunities
            
        except Exception as e:
            logger.error(f"Failed to search network arbitrage: {e}")
            return []
    
    async def _search_quality_arbitrage_quantum(self) -> List[ArbitrageOpportunity]:
        """Search for quality-based arbitrage in quantum resources"""
        opportunities = []
        
        try:
            # Get resources with quality scores
            resources = await self.market_client.get_quantum_resources_with_quality()
            
            # Group by similar specifications
            spec_groups = {}
            for resource in resources:
                key = f"{resource['qubit_count']}_{resource.get('gate_types', 'all')}"
                if key not in spec_groups:
                    spec_groups[key] = []
                spec_groups[key].append(resource)
            
            # Find quality arbitrage within groups
            for spec_key, group in spec_groups.items():
                if len(group) < 2:
                    continue
                
                # Sort by quality score
                sorted_group = sorted(group, key=lambda x: x.get('quality_score', 80))
                
                for i in range(len(sorted_group) - 1):
                    lower_quality = sorted_group[i]
                    higher_quality = sorted_group[i + 1]
                    
                    # Check if price difference is less than quality difference
                    price_ratio = higher_quality['price_per_minute'] / lower_quality['price_per_minute']
                    quality_ratio = higher_quality['quality_score'] / lower_quality['quality_score']
                    
                    if quality_ratio > price_ratio * 1.2:  # Higher quality is underpriced
                        quantity = 60  # 1 hour
                        potential_profit = (
                            lower_quality['price_per_minute'] * quality_ratio - 
                            higher_quality['price_per_minute']
                        ) * quantity
                        
                        if potential_profit > 0:
                            profit_margin = potential_profit / (higher_quality['price_per_minute'] * quantity)
                            
                            opportunity = ArbitrageOpportunity(
                                opportunity_id=f"arb_q_qual_{uuid.uuid4().hex[:8]}",
                                arbitrage_type=ArbitrageType.QUALITY_ARBITRAGE,
                                resource_type=ResourceType.QUANTUM,
                                resource_id=higher_quality['qpu_id'],
                                market_a=higher_quality['qpu_id'],
                                market_b=lower_quality['qpu_id'],
                                price_a=higher_quality['price_per_minute'],
                                price_b=lower_quality['price_per_minute'] * quality_ratio,
                                quantity=quantity,
                                potential_profit=potential_profit,
                                profit_margin=profit_margin,
                                expires_at=datetime.utcnow() + timedelta(hours=12),
                                confidence=0.85,
                                execution_time_estimate=2.0,
                                risk_score=0.2
                            )
                            
                            opportunities.append(opportunity)
            
            return opportunities
            
        except Exception as e:
            logger.error(f"Failed to search quality arbitrage: {e}")
            return []
    
    def _calculate_risk_score(
        self,
        resource_type: ResourceType,
        price_volatility: float,
        time_to_expiry: float,
        market_depth: float
    ) -> float:
        """Calculate risk score for arbitrage opportunity"""
        # Base risk by resource type
        base_risk = {
            ResourceType.QUANTUM: 0.3,  # Higher due to technical complexity
            ResourceType.AI: 0.2,       # Moderate
            ResourceType.NETWORK: 0.15  # Lower, more stable
        }
        
        risk = base_risk.get(resource_type, 0.25)
        
        # Adjust for volatility
        risk += price_volatility * 0.3
        
        # Adjust for time (longer = more risk)
        time_factor = min(time_to_expiry / 168, 1.0)  # Normalize to 1 week
        risk += time_factor * 0.2
        
        # Adjust for market depth (less depth = more risk)
        depth_factor = 1.0 - min(market_depth / 10000, 1.0)
        risk += depth_factor * 0.2
        
        return min(risk, 1.0)
    
    async def execute_arbitrage(
        self,
        opportunity_id: str
    ) -> ArbitrageExecution:
        """Execute an arbitrage opportunity"""
        start_time = datetime.utcnow()
        
        try:
            # Get opportunity details
            opportunity = self.active_opportunities.get(opportunity_id)
            if not opportunity:
                cached = self.arbitrage_cache.get(opportunity_id)
                if cached:
                    opportunity = ArbitrageOpportunity(**cached)
                else:
                    raise ValueError(f"Opportunity {opportunity_id} not found")
            
            # Check if still valid
            if opportunity.expires_at < datetime.utcnow():
                raise ValueError("Opportunity has expired")
            
            # Execute based on resource type
            if opportunity.resource_type == ResourceType.QUANTUM:
                execution = await self._execute_quantum_arbitrage(opportunity)
            elif opportunity.resource_type == ResourceType.AI:
                execution = await self._execute_ai_arbitrage(opportunity)
            else:  # NETWORK
                execution = await self._execute_network_arbitrage(opportunity)
            
            # Record execution time
            execution.execution_time_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            # Store execution record
            self.arbitrage_cache.put(
                f"exec_{execution.execution_id}",
                execution.dict(),
                ttl=86400  # Keep for 24 hours
            )
            
            # Remove from active opportunities
            self.active_opportunities.pop(opportunity_id, None)
            
            return execution
            
        except Exception as e:
            logger.error(f"Failed to execute arbitrage: {e}")
            
            # Create failed execution record
            return ArbitrageExecution(
                execution_id=f"exec_{uuid.uuid4().hex[:8]}",
                opportunity_id=opportunity_id,
                executed_at=datetime.utcnow(),
                buy_market="",
                sell_market="",
                quantity_executed=0,
                buy_price=0,
                sell_price=0,
                actual_profit=0,
                fees=0,
                net_profit=0,
                execution_time_ms=(datetime.utcnow() - start_time).total_seconds() * 1000,
                success=False,
                error_message=str(e)
            )
    
    async def _execute_quantum_arbitrage(
        self,
        opportunity: ArbitrageOpportunity
    ) -> ArbitrageExecution:
        """Execute quantum resource arbitrage"""
        # Simulate execution
        await asyncio.sleep(settings.ARBITRAGE_EXECUTION_DELAY)
        
        # In production, would:
        # 1. Reserve resources in buy market
        # 2. List resources in sell market
        # 3. Handle the actual transfer
        
        # Calculate actual execution
        quantity_executed = opportunity.quantity * 0.95  # 95% fill rate
        buy_price = opportunity.price_a * 1.001  # 0.1% slippage
        sell_price = opportunity.price_b * 0.999  # 0.1% slippage
        
        fees = (buy_price + sell_price) * quantity_executed * 0.002  # 0.2% fees
        actual_profit = (sell_price - buy_price) * quantity_executed
        net_profit = actual_profit - fees
        
        return ArbitrageExecution(
            execution_id=f"exec_{uuid.uuid4().hex[:8]}",
            opportunity_id=opportunity.opportunity_id,
            executed_at=datetime.utcnow(),
            buy_market=opportunity.market_a,
            sell_market=opportunity.market_b,
            quantity_executed=quantity_executed,
            buy_price=buy_price,
            sell_price=sell_price,
            actual_profit=actual_profit,
            fees=fees,
            net_profit=net_profit,
            execution_time_ms=0,  # Will be set by caller
            success=net_profit > 0,
            blockchain_tx_hashes=[
                f"0x{uuid.uuid4().hex}",  # Buy transaction
                f"0x{uuid.uuid4().hex}"   # Sell transaction
            ]
        )
    
    async def _execute_ai_arbitrage(
        self,
        opportunity: ArbitrageOpportunity
    ) -> ArbitrageExecution:
        """Execute AI accelerator arbitrage"""
        # Simulate reserved instance purchase
        await asyncio.sleep(settings.ARBITRAGE_EXECUTION_DELAY * 2)
        
        # For time arbitrage (reserved vs spot)
        if opportunity.arbitrage_type == ArbitrageType.TIME_ARBITRAGE:
            # Simulate purchasing reserved instance and reselling capacity
            quantity_executed = opportunity.quantity * 0.8  # 80% utilization
            buy_price = opportunity.price_a
            sell_price = opportunity.price_b * 0.95  # 5% discount for resale
            
            fees = buy_price * opportunity.quantity * 0.01  # 1% upfront fee
            actual_profit = (sell_price - buy_price) * quantity_executed
            net_profit = actual_profit - fees
            
            return ArbitrageExecution(
                execution_id=f"exec_{uuid.uuid4().hex[:8]}",
                opportunity_id=opportunity.opportunity_id,
                executed_at=datetime.utcnow(),
                buy_market=opportunity.market_a,
                sell_market=opportunity.market_b,
                quantity_executed=quantity_executed,
                buy_price=buy_price,
                sell_price=sell_price,
                actual_profit=actual_profit,
                fees=fees,
                net_profit=net_profit,
                execution_time_ms=0,
                success=net_profit > 0,
                blockchain_tx_hashes=[f"0x{uuid.uuid4().hex}"]
            )
        
        # Default execution for other types
        return await self._execute_quantum_arbitrage(opportunity)
    
    async def _execute_network_arbitrage(
        self,
        opportunity: ArbitrageOpportunity
    ) -> ArbitrageExecution:
        """Execute network bandwidth arbitrage"""
        # For QoS arbitrage
        if opportunity.arbitrage_type == ArbitrageType.QUALITY_ARBITRAGE:
            # Simulate provisioning lower QoS with redundancy
            await asyncio.sleep(settings.ARBITRAGE_EXECUTION_DELAY)
            
            quantity_executed = opportunity.quantity
            buy_price = opportunity.price_a * 2  # 2x for redundancy
            sell_price = opportunity.price_b
            
            fees = (buy_price + sell_price) * quantity_executed * 0.001  # 0.1% fees
            actual_profit = sell_price * quantity_executed - buy_price * quantity_executed
            net_profit = actual_profit - fees
            
            return ArbitrageExecution(
                execution_id=f"exec_{uuid.uuid4().hex[:8]}",
                opportunity_id=opportunity.opportunity_id,
                executed_at=datetime.utcnow(),
                buy_market=opportunity.market_a,
                sell_market=opportunity.market_b,
                quantity_executed=quantity_executed,
                buy_price=buy_price,
                sell_price=sell_price,
                actual_profit=actual_profit,
                fees=fees,
                net_profit=net_profit,
                execution_time_ms=0,
                success=net_profit > 0,
                blockchain_tx_hashes=[f"0x{uuid.uuid4().hex}"]
            )
        
        # Default execution
        return await self._execute_quantum_arbitrage(opportunity)
    
    async def monitor_arbitrage_opportunities(self):
        """Background task to continuously monitor for arbitrage"""
        while True:
            try:
                # Default search parameters
                request = ArbitrageSearchRequest(
                    min_profit_margin=settings.ARBITRAGE_MIN_PROFIT_MARGIN,
                    max_risk_score=0.5,
                    time_horizon_minutes=60
                )
                
                # Search for opportunities
                opportunities = await self.search_arbitrage_opportunities(request)
                
                # Auto-execute high-confidence opportunities
                for opp in opportunities:
                    if opp.confidence > 0.8 and opp.risk_score < 0.3:
                        if opp.potential_profit > 100 and opp.potential_profit < settings.MAX_ARBITRAGE_VALUE:
                            logger.info(f"Auto-executing arbitrage opportunity: {opp.opportunity_id}")
                            await self.execute_arbitrage(opp.opportunity_id)
                
                # Wait before next scan
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                logger.error(f"Error in arbitrage monitoring: {e}")
                await asyncio.sleep(300)  # Wait 5 minutes on error 