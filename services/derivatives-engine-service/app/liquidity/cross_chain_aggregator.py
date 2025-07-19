"""
Cross-Chain Liquidity Aggregator

Aggregates liquidity across multiple chains with compliance checks,
finding the best execution path while respecting regulatory requirements.
"""

import asyncio
from typing import Dict, List, Optional, Tuple, Any, Set
from decimal import Decimal
from datetime import datetime, timedelta
from enum import Enum
import logging
from dataclasses import dataclass
import numpy as np

from platformq_shared import ServiceClient, ProcessingResult, ProcessingStatus
from app.integrations.compliance_bridge import ComplianceBridge, ComplianceConfig

logger = logging.getLogger(__name__)


class Chain(Enum):
    """Supported blockchain networks"""
    ETHEREUM = "ethereum"
    POLYGON = "polygon"
    ARBITRUM = "arbitrum"
    AVALANCHE = "avalanche"
    BSC = "bsc"
    SOLANA = "solana"
    # HYPERLEDGER = "hyperledger"  # For institutional


class ExecutionType(Enum):
    """Types of order execution"""
    MARKET = "market"
    LIMIT = "limit"
    SMART_ROUTE = "smart_route"
    CROSS_CHAIN_SWAP = "cross_chain_swap"
    AGGREGATED = "aggregated"


@dataclass
class LiquiditySource:
    """Represents a source of liquidity"""
    source_id: str
    chain: Chain
    protocol: str  # Uniswap, SushiSwap, etc.
    pool_address: str
    token_pair: Tuple[str, str]
    
    # Liquidity metrics
    total_liquidity: Decimal
    available_liquidity: Decimal
    price: Decimal
    price_impact: Decimal  # For given size
    
    # Fees
    protocol_fee: Decimal
    gas_estimate: Decimal
    bridge_fee: Optional[Decimal] = None
    
    # Compliance
    kyc_required: bool = False
    min_kyc_tier: int = 0
    allowed_jurisdictions: List[str] = None
    blocked_jurisdictions: List[str] = None
    
    # Performance
    avg_execution_time: timedelta = timedelta(seconds=30)
    success_rate: float = 0.99


@dataclass
class ExecutionRoute:
    """Optimal execution route across chains"""
    route_id: str
    total_cost: Decimal
    expected_output: Decimal
    price_impact: Decimal
    total_fees: Decimal
    
    # Route steps
    steps: List[Dict[str, Any]]
    chains_involved: List[Chain]
    
    # Timing
    estimated_time: timedelta
    expires_at: datetime
    
    # Compliance
    compliance_checked: bool
    required_kyc_tier: int
    
    # Risk metrics
    slippage_tolerance: Decimal
    confidence_score: float


class CrossChainLiquidityAggregator:
    """
    Aggregates liquidity across multiple chains with compliance
    """
    
    def __init__(self):
        self.compliance_bridge = ComplianceBridge()
        
        self.blockchain_client = ServiceClient(
            service_name="blockchain-gateway-service",
            circuit_breaker_threshold=5,
            rate_limit=100.0
        )
        
        self.oracle_client = ServiceClient(
            service_name="blockchain-gateway-service",
            circuit_breaker_threshold=5,
            rate_limit=100.0
        )
        
        # Liquidity source registry
        self.liquidity_sources: Dict[str, LiquiditySource] = {}
        self.chain_adapters: Dict[Chain, Any] = {}
        
        # Routing optimization
        self.max_route_splits = 5  # Max number of splits
        self.min_split_size = Decimal("100")  # Min order size per split
        self.price_deviation_threshold = Decimal("0.02")  # 2% max deviation
        
        # Gas prices by chain (would be dynamic)
        self.gas_prices = {
            Chain.ETHEREUM: Decimal("50"),  # Gwei
            Chain.POLYGON: Decimal("30"),
            Chain.ARBITRUM: Decimal("0.1"),
            Chain.AVALANCHE: Decimal("25"),
            Chain.BSC: Decimal("5"),
            Chain.SOLANA: Decimal("0.00025")  # SOL
        }
        
        # Bridge fees (would be dynamic)
        self.bridge_fees = {
            (Chain.ETHEREUM, Chain.POLYGON): Decimal("0.001"),  # 0.1%
            (Chain.ETHEREUM, Chain.ARBITRUM): Decimal("0.0005"),  # 0.05%
            (Chain.POLYGON, Chain.AVALANCHE): Decimal("0.0015"),  # 0.15%
            # Add more pairs
        }
        
    async def find_best_execution_route(
        self,
        tenant_id: str,
        user_id: str,
        token_in: str,
        token_out: str,
        amount_in: Decimal,
        execution_type: ExecutionType = ExecutionType.SMART_ROUTE,
        max_slippage: Decimal = Decimal("0.01"),  # 1%
        preferred_chains: Optional[List[Chain]] = None
    ) -> ProcessingResult:
        """
        Find the best execution route across chains with compliance
        """
        try:
            # Check user compliance status
            compliance_status = await self._get_user_compliance_status(
                tenant_id,
                user_id
            )
            
            # Get available liquidity sources
            sources = await self._get_liquidity_sources(
                token_in,
                token_out,
                preferred_chains
            )
            
            # Filter by compliance
            compliant_sources = await self._filter_by_compliance(
                sources,
                compliance_status
            )
            
            if not compliant_sources:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="No compliant liquidity sources available"
                )
            
            # Get current prices from oracle
            oracle_prices = await self._get_oracle_prices([token_in, token_out])
            
            # Calculate optimal routes
            routes = await self._calculate_optimal_routes(
                compliant_sources,
                amount_in,
                max_slippage,
                oracle_prices
            )
            
            if not routes:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error="No viable routes found"
                )
            
            # Select best route
            best_route = self._select_best_route(routes, execution_type)
            
            # Validate route
            validation = await self._validate_route(
                best_route,
                amount_in,
                max_slippage
            )
            
            if not validation["valid"]:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=validation["reason"]
                )
            
            logger.info(
                f"Found optimal route for {amount_in} {token_in} -> {token_out}: "
                f"{len(best_route.steps)} steps across {len(best_route.chains_involved)} chains"
            )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "route": self._serialize_route(best_route),
                    "alternatives": [
                        self._serialize_route(r) for r in routes[:3]
                        if r.route_id != best_route.route_id
                    ]
                }
            )
            
        except Exception as e:
            logger.error(f"Error finding execution route: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def execute_cross_chain_order(
        self,
        tenant_id: str,
        user_id: str,
        route: ExecutionRoute,
        amount_in: Decimal,
        deadline: Optional[datetime] = None
    ) -> ProcessingResult:
        """
        Execute order across multiple chains
        """
        try:
            # Re-validate compliance
            compliance_check = await self._check_execution_compliance(
                tenant_id,
                user_id,
                route
            )
            
            if not compliance_check["compliant"]:
                return ProcessingResult(
                    status=ProcessingStatus.FAILED,
                    error=f"Compliance check failed: {compliance_check['reason']}"
                )
            
            # Set deadline
            if not deadline:
                deadline = datetime.utcnow() + route.estimated_time * 2
            
            # Execute route steps
            execution_results = []
            current_amount = amount_in
            current_token = route.steps[0]["token_in"]
            
            for i, step in enumerate(route.steps):
                logger.info(f"Executing step {i+1}/{len(route.steps)}: {step['action']}")
                
                if step["action"] == "swap":
                    result = await self._execute_swap(
                        tenant_id,
                        user_id,
                        step,
                        current_amount,
                        deadline
                    )
                    
                elif step["action"] == "bridge":
                    result = await self._execute_bridge(
                        tenant_id,
                        user_id,
                        step,
                        current_amount,
                        deadline
                    )
                    
                elif step["action"] == "aggregate":
                    result = await self._execute_aggregated_swap(
                        tenant_id,
                        user_id,
                        step,
                        current_amount,
                        deadline
                    )
                    
                else:
                    raise ValueError(f"Unknown action: {step['action']}")
                
                if result["success"]:
                    execution_results.append(result)
                    current_amount = result["output_amount"]
                    current_token = result["output_token"]
                else:
                    # Rollback previous steps if possible
                    await self._handle_execution_failure(
                        execution_results,
                        i,
                        result["error"]
                    )
                    
                    return ProcessingResult(
                        status=ProcessingStatus.FAILED,
                        error=f"Execution failed at step {i+1}: {result['error']}"
                    )
            
            # Verify final output
            final_output = current_amount
            expected_output = route.expected_output
            slippage = abs(final_output - expected_output) / expected_output
            
            if slippage > route.slippage_tolerance:
                logger.warning(
                    f"High slippage detected: {slippage:.2%} vs "
                    f"tolerance {route.slippage_tolerance:.2%}"
                )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "execution_id": f"EXEC_{datetime.utcnow().timestamp()}",
                    "route_id": route.route_id,
                    "input_amount": str(amount_in),
                    "output_amount": str(final_output),
                    "actual_slippage": str(slippage),
                    "execution_steps": execution_results,
                    "total_fees": str(sum(
                        Decimal(r.get("fees", "0"))
                        for r in execution_results
                    ))
                }
            )
            
        except Exception as e:
            logger.error(f"Error executing cross-chain order: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def get_liquidity_depth(
        self,
        token_pair: Tuple[str, str],
        chains: Optional[List[Chain]] = None
    ) -> ProcessingResult:
        """
        Get aggregated liquidity depth across chains
        """
        try:
            # Get all sources for token pair
            sources = await self._get_liquidity_sources(
                token_pair[0],
                token_pair[1],
                chains
            )
            
            # Aggregate liquidity by price level
            price_levels = {}
            
            for source in sources:
                # Get order book or AMM curve
                depth_data = await self._get_source_depth(source)
                
                for price, liquidity in depth_data.items():
                    if price not in price_levels:
                        price_levels[price] = {
                            "total_liquidity": Decimal("0"),
                            "sources": []
                        }
                    
                    price_levels[price]["total_liquidity"] += liquidity
                    price_levels[price]["sources"].append({
                        "source_id": source.source_id,
                        "chain": source.chain.value,
                        "liquidity": str(liquidity)
                    })
            
            # Sort by price
            sorted_levels = sorted(price_levels.items(), key=lambda x: x[0])
            
            # Calculate cumulative depth
            cumulative_bid = Decimal("0")
            cumulative_ask = Decimal("0")
            mid_price = sorted_levels[len(sorted_levels)//2][0] if sorted_levels else Decimal("0")
            
            depth_chart = {
                "bids": [],
                "asks": []
            }
            
            for price, data in sorted_levels:
                if price < mid_price:
                    cumulative_bid += data["total_liquidity"]
                    depth_chart["bids"].append({
                        "price": str(price),
                        "quantity": str(data["total_liquidity"]),
                        "cumulative": str(cumulative_bid),
                        "sources": data["sources"]
                    })
                else:
                    cumulative_ask += data["total_liquidity"]
                    depth_chart["asks"].append({
                        "price": str(price),
                        "quantity": str(data["total_liquidity"]),
                        "cumulative": str(cumulative_ask),
                        "sources": data["sources"]
                    })
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "token_pair": token_pair,
                    "total_liquidity": str(cumulative_bid + cumulative_ask),
                    "chains_aggregated": len(set(s.chain for s in sources)),
                    "sources_count": len(sources),
                    "depth_chart": depth_chart,
                    "best_bid": depth_chart["bids"][-1] if depth_chart["bids"] else None,
                    "best_ask": depth_chart["asks"][0] if depth_chart["asks"] else None
                }
            )
            
        except Exception as e:
            logger.error(f"Error getting liquidity depth: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def monitor_arbitrage_opportunities(
        self,
        tenant_id: str,
        user_id: str,
        token_pairs: List[Tuple[str, str]],
        min_profit_threshold: Decimal = Decimal("50")  # $50 minimum
    ) -> ProcessingResult:
        """
        Monitor for cross-chain arbitrage opportunities
        """
        try:
            opportunities = []
            
            for token_pair in token_pairs:
                # Get prices across all chains
                chain_prices = await self._get_prices_by_chain(
                    token_pair[0],
                    token_pair[1]
                )
                
                # Find price discrepancies
                for chain1, price1 in chain_prices.items():
                    for chain2, price2 in chain_prices.items():
                        if chain1 == chain2:
                            continue
                        
                        # Calculate potential profit
                        price_diff = abs(price1 - price2)
                        price_diff_pct = price_diff / min(price1, price2)
                        
                        if price_diff_pct > Decimal("0.005"):  # 0.5% threshold
                            # Estimate costs
                            bridge_fee = self._get_bridge_fee(chain1, chain2)
                            gas_costs = self._estimate_gas_costs(
                                chain1,
                                chain2,
                                token_pair
                            )
                            
                            # Calculate net profit for standard size
                            test_amount = Decimal("10000")  # $10k test size
                            gross_profit = test_amount * price_diff_pct
                            total_costs = (
                                test_amount * bridge_fee +
                                gas_costs +
                                test_amount * Decimal("0.003")  # 0.3% swap fees
                            )
                            net_profit = gross_profit - total_costs
                            
                            if net_profit > min_profit_threshold:
                                opportunity = {
                                    "token_pair": token_pair,
                                    "buy_chain": chain1.value if price1 < price2 else chain2.value,
                                    "sell_chain": chain2.value if price1 < price2 else chain1.value,
                                    "price_difference": str(price_diff),
                                    "price_difference_pct": f"{float(price_diff_pct)*100:.3f}%",
                                    "estimated_profit": str(net_profit),
                                    "roi": f"{float(net_profit/test_amount)*100:.2f}%",
                                    "confidence": self._calculate_arb_confidence(
                                        chain_prices,
                                        chain1,
                                        chain2
                                    )
                                }
                                
                                # Check if executable with compliance
                                if await self._can_execute_arbitrage(
                                    tenant_id,
                                    user_id,
                                    chain1,
                                    chain2
                                ):
                                    opportunity["executable"] = True
                                    opportunities.append(opportunity)
            
            # Sort by profit potential
            opportunities.sort(
                key=lambda x: Decimal(x["estimated_profit"]),
                reverse=True
            )
            
            return ProcessingResult(
                status=ProcessingStatus.SUCCESS,
                data={
                    "opportunities": opportunities[:10],  # Top 10
                    "total_found": len(opportunities),
                    "monitoring_pairs": len(token_pairs),
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
        except Exception as e:
            logger.error(f"Error monitoring arbitrage: {str(e)}")
            return ProcessingResult(
                status=ProcessingStatus.FAILED,
                error=str(e)
            )
    
    async def _get_liquidity_sources(
        self,
        token_in: str,
        token_out: str,
        chains: Optional[List[Chain]] = None
    ) -> List[LiquiditySource]:
        """
        Get all available liquidity sources
        """
        sources = []
        
        # Query each chain adapter
        target_chains = chains or list(Chain)
        
        for chain in target_chains:
            try:
                # Get sources from blockchain bridge
                response = await self.blockchain_client.post(
                    "/api/v1/liquidity/sources",
                    json={
                        "chain": chain.value,
                        "token_in": token_in,
                        "token_out": token_out
                    }
                )
                
                if response.status_code == 200:
                    chain_sources = response.json()["sources"]
                    
                    for source_data in chain_sources:
                        source = LiquiditySource(
                            source_id=source_data["id"],
                            chain=chain,
                            protocol=source_data["protocol"],
                            pool_address=source_data["address"],
                            token_pair=(token_in, token_out),
                            total_liquidity=Decimal(source_data["liquidity"]),
                            available_liquidity=Decimal(source_data["available"]),
                            price=Decimal(source_data["price"]),
                            price_impact=Decimal(source_data["price_impact"]),
                            protocol_fee=Decimal(source_data["fee"]),
                            gas_estimate=self._estimate_gas_cost(chain),
                            kyc_required=source_data.get("kyc_required", False),
                            min_kyc_tier=source_data.get("min_kyc_tier", 0)
                        )
                        sources.append(source)
                        
            except Exception as e:
                logger.error(f"Error fetching sources from {chain}: {str(e)}")
        
        return sources
    
    async def _filter_by_compliance(
        self,
        sources: List[LiquiditySource],
        compliance_status: Dict[str, Any]
    ) -> List[LiquiditySource]:
        """
        Filter sources by user's compliance status
        """
        filtered = []
        
        user_kyc_tier = compliance_status.get("kyc_tier", 0)
        user_jurisdiction = compliance_status.get("jurisdiction", "unknown")
        
        for source in sources:
            # Check KYC requirements
            if source.kyc_required and user_kyc_tier < source.min_kyc_tier:
                continue
            
            # Check jurisdiction restrictions
            if source.allowed_jurisdictions and user_jurisdiction not in source.allowed_jurisdictions:
                continue
            
            if source.blocked_jurisdictions and user_jurisdiction in source.blocked_jurisdictions:
                continue
            
            filtered.append(source)
        
        return filtered
    
    async def _calculate_optimal_routes(
        self,
        sources: List[LiquiditySource],
        amount_in: Decimal,
        max_slippage: Decimal,
        oracle_prices: Dict[str, Decimal]
    ) -> List[ExecutionRoute]:
        """
        Calculate optimal execution routes
        """
        routes = []
        
        # Single-source routes
        for source in sources:
            if source.available_liquidity >= amount_in * Decimal("0.5"):  # At least 50%
                route = self._create_single_source_route(
                    source,
                    amount_in,
                    max_slippage
                )
                if route:
                    routes.append(route)
        
        # Multi-source routes (split orders)
        if len(sources) >= 2:
            split_routes = await self._calculate_split_routes(
                sources,
                amount_in,
                max_slippage
            )
            routes.extend(split_routes)
        
        # Cross-chain routes
        cross_chain_routes = await self._calculate_cross_chain_routes(
            sources,
            amount_in,
            max_slippage,
            oracle_prices
        )
        routes.extend(cross_chain_routes)
        
        return routes
    
    def _create_single_source_route(
        self,
        source: LiquiditySource,
        amount_in: Decimal,
        max_slippage: Decimal
    ) -> Optional[ExecutionRoute]:
        """
        Create route using single liquidity source
        """
        # Calculate expected output
        expected_output = amount_in / source.price
        
        # Apply price impact
        actual_output = expected_output * (Decimal("1") - source.price_impact)
        
        # Check slippage
        slippage = (expected_output - actual_output) / expected_output
        if slippage > max_slippage:
            return None
        
        # Calculate total fees
        total_fees = (
            amount_in * source.protocol_fee +
            source.gas_estimate
        )
        
        return ExecutionRoute(
            route_id=f"ROUTE_{source.source_id}_{datetime.utcnow().timestamp()}",
            total_cost=amount_in + total_fees,
            expected_output=actual_output,
            price_impact=source.price_impact,
            total_fees=total_fees,
            steps=[{
                "action": "swap",
                "source": source.source_id,
                "chain": source.chain.value,
                "protocol": source.protocol,
                "token_in": source.token_pair[0],
                "token_out": source.token_pair[1],
                "amount_in": str(amount_in),
                "expected_output": str(actual_output)
            }],
            chains_involved=[source.chain],
            estimated_time=source.avg_execution_time,
            expires_at=datetime.utcnow() + timedelta(minutes=5),
            compliance_checked=True,
            required_kyc_tier=source.min_kyc_tier,
            slippage_tolerance=max_slippage,
            confidence_score=0.95
        )
    
    async def _calculate_split_routes(
        self,
        sources: List[LiquiditySource],
        amount_in: Decimal,
        max_slippage: Decimal
    ) -> List[ExecutionRoute]:
        """
        Calculate routes that split order across multiple sources
        """
        routes = []
        
        # Try different split combinations
        for split_count in range(2, min(len(sources), self.max_route_splits) + 1):
            # Sort by price
            sorted_sources = sorted(sources, key=lambda s: s.price)
            
            # Take best sources
            selected_sources = sorted_sources[:split_count]
            
            # Optimize split amounts
            split_amounts = self._optimize_split_amounts(
                selected_sources,
                amount_in
            )
            
            if split_amounts:
                route = self._create_split_route(
                    selected_sources,
                    split_amounts,
                    max_slippage
                )
                if route:
                    routes.append(route)
        
        return routes
    
    def _optimize_split_amounts(
        self,
        sources: List[LiquiditySource],
        total_amount: Decimal
    ) -> Optional[List[Decimal]]:
        """
        Optimize how to split amount across sources
        """
        # Simple proportional split based on available liquidity
        # In production, would use more sophisticated optimization
        
        total_liquidity = sum(s.available_liquidity for s in sources)
        if total_liquidity < total_amount:
            return None
        
        amounts = []
        remaining = total_amount
        
        for i, source in enumerate(sources):
            if i == len(sources) - 1:
                # Last source gets remaining
                amount = remaining
            else:
                # Proportional to liquidity
                proportion = source.available_liquidity / total_liquidity
                amount = min(
                    total_amount * proportion,
                    source.available_liquidity,
                    remaining
                )
            
            amounts.append(amount)
            remaining -= amount
        
        return amounts if remaining == 0 else None
    
    async def _calculate_cross_chain_routes(
        self,
        sources: List[LiquiditySource],
        amount_in: Decimal,
        max_slippage: Decimal,
        oracle_prices: Dict[str, Decimal]
    ) -> List[ExecutionRoute]:
        """
        Calculate routes that bridge across chains
        """
        routes = []
        
        # Group sources by chain
        by_chain = {}
        for source in sources:
            if source.chain not in by_chain:
                by_chain[source.chain] = []
            by_chain[source.chain].append(source)
        
        # Find bridgeable pairs
        for chain1, sources1 in by_chain.items():
            for chain2, sources2 in by_chain.items():
                if chain1 == chain2:
                    continue
                
                # Check if bridge exists
                if (chain1, chain2) not in self.bridge_fees:
                    continue
                
                # Find best source on each chain
                best_source1 = min(sources1, key=lambda s: s.price)
                best_source2 = min(sources2, key=lambda s: s.price)
                
                # Create cross-chain route
                route = self._create_cross_chain_route(
                    best_source1,
                    best_source2,
                    amount_in,
                    max_slippage
                )
                
                if route:
                    routes.append(route)
        
        return routes
    
    def _create_cross_chain_route(
        self,
        source1: LiquiditySource,
        source2: LiquiditySource,
        amount_in: Decimal,
        max_slippage: Decimal
    ) -> Optional[ExecutionRoute]:
        """
        Create route that bridges between chains
        """
        # This is simplified - would include actual bridge logic
        bridge_fee = self._get_bridge_fee(source1.chain, source2.chain)
        
        # Calculate outputs
        output1 = amount_in / source1.price * (Decimal("1") - source1.price_impact)
        bridge_output = output1 * (Decimal("1") - bridge_fee)
        final_output = bridge_output * (Decimal("1") - source2.price_impact)
        
        # Total fees
        total_fees = (
            amount_in * source1.protocol_fee +
            source1.gas_estimate +
            output1 * bridge_fee +
            source2.gas_estimate
        )
        
        return ExecutionRoute(
            route_id=f"XCHAIN_{datetime.utcnow().timestamp()}",
            total_cost=amount_in + total_fees,
            expected_output=final_output,
            price_impact=source1.price_impact + source2.price_impact,
            total_fees=total_fees,
            steps=[
                {
                    "action": "swap",
                    "chain": source1.chain.value,
                    "source": source1.source_id,
                    "amount_in": str(amount_in)
                },
                {
                    "action": "bridge",
                    "from_chain": source1.chain.value,
                    "to_chain": source2.chain.value,
                    "amount": str(output1)
                },
                {
                    "action": "swap",
                    "chain": source2.chain.value,
                    "source": source2.source_id,
                    "amount_in": str(bridge_output)
                }
            ],
            chains_involved=[source1.chain, source2.chain],
            estimated_time=timedelta(minutes=10),
            expires_at=datetime.utcnow() + timedelta(minutes=15),
            compliance_checked=True,
            required_kyc_tier=max(source1.min_kyc_tier, source2.min_kyc_tier),
            slippage_tolerance=max_slippage,
            confidence_score=0.85
        )
    
    def _select_best_route(
        self,
        routes: List[ExecutionRoute],
        execution_type: ExecutionType
    ) -> ExecutionRoute:
        """
        Select best route based on execution type
        """
        if execution_type == ExecutionType.MARKET:
            # Fastest execution
            return min(routes, key=lambda r: r.estimated_time)
        
        elif execution_type == ExecutionType.SMART_ROUTE:
            # Balance of price and speed
            def score_route(route):
                price_score = route.expected_output / route.total_cost
                time_penalty = route.estimated_time.total_seconds() / 3600  # Hours
                fee_penalty = float(route.total_fees / route.total_cost)
                return price_score - time_penalty * 0.1 - fee_penalty * 2
            
            return max(routes, key=score_route)
        
        else:
            # Best price
            return max(routes, key=lambda r: r.expected_output)
    
    async def _execute_swap(
        self,
        tenant_id: str,
        user_id: str,
        step: Dict[str, Any],
        amount: Decimal,
        deadline: datetime
    ) -> Dict[str, Any]:
        """
        Execute a swap on a specific chain
        """
        try:
            # Call blockchain bridge to execute
            response = await self.blockchain_client.post(
                "/api/v1/execute/swap",
                json={
                    "tenant_id": tenant_id,
                    "user_id": user_id,
                    "chain": step["chain"],
                    "protocol": step.get("protocol"),
                    "token_in": step["token_in"],
                    "token_out": step["token_out"],
                    "amount_in": str(amount),
                    "min_output": str(Decimal(step["expected_output"]) * Decimal("0.99")),
                    "deadline": deadline.isoformat()
                }
            )
            
            if response.status_code == 200:
                result = response.json()
                return {
                    "success": True,
                    "output_amount": Decimal(result["output_amount"]),
                    "output_token": step["token_out"],
                    "tx_hash": result["tx_hash"],
                    "fees": result["fees"]
                }
            else:
                return {
                    "success": False,
                    "error": response.text
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _execute_bridge(
        self,
        tenant_id: str,
        user_id: str,
        step: Dict[str, Any],
        amount: Decimal,
        deadline: datetime
    ) -> Dict[str, Any]:
        """
        Execute a cross-chain bridge
        """
        # Implementation would call bridge protocol
        return {
            "success": True,
            "output_amount": amount * Decimal("0.999"),  # Mock 0.1% bridge fee
            "output_token": step.get("token", "USDC"),
            "bridge_tx": f"BRIDGE_{datetime.utcnow().timestamp()}",
            "fees": str(amount * Decimal("0.001"))
        }
    
    async def _get_user_compliance_status(
        self,
        tenant_id: str,
        user_id: str
    ) -> Dict[str, Any]:
        """
        Get user's compliance status
        """
        return await self.compliance_bridge._get_user_compliance_status(
            tenant_id,
            user_id
        )
    
    def _get_bridge_fee(self, chain1: Chain, chain2: Chain) -> Decimal:
        """
        Get bridge fee between chains
        """
        return self.bridge_fees.get(
            (chain1, chain2),
            self.bridge_fees.get(
                (chain2, chain1),
                Decimal("0.002")  # Default 0.2%
            )
        )
    
    def _estimate_gas_cost(self, chain: Chain) -> Decimal:
        """
        Estimate gas cost for chain
        """
        gas_price = self.gas_prices.get(chain, Decimal("30"))
        
        # Estimate gas units (would be dynamic)
        gas_units = {
            Chain.ETHEREUM: 150000,
            Chain.POLYGON: 150000,
            Chain.ARBITRUM: 500000,
            Chain.AVALANCHE: 150000,
            Chain.BSC: 150000,
            Chain.SOLANA: 5000
        }
        
        units = gas_units.get(chain, 150000)
        
        # Convert to USD (simplified)
        eth_price = Decimal("2000")  # Would get from oracle
        
        if chain == Chain.SOLANA:
            return gas_price * units  # SOL price
        else:
            return gas_price * units * eth_price / Decimal("1e9")  # Gwei to ETH
    
    def _serialize_route(self, route: ExecutionRoute) -> Dict[str, Any]:
        """
        Serialize route for API response
        """
        return {
            "route_id": route.route_id,
            "total_cost": str(route.total_cost),
            "expected_output": str(route.expected_output),
            "price_impact": str(route.price_impact),
            "total_fees": str(route.total_fees),
            "steps": route.steps,
            "chains": [c.value for c in route.chains_involved],
            "estimated_time_seconds": route.estimated_time.total_seconds(),
            "expires_at": route.expires_at.isoformat(),
            "required_kyc_tier": route.required_kyc_tier,
            "confidence_score": route.confidence_score
        } 