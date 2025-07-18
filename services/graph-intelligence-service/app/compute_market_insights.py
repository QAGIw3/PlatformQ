"""
Compute Market Insights

Graph-based intelligence for compute market analysis, trust-based pricing,
and risk assessment for the derivatives engine.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List, Tuple, Set
from datetime import datetime, timedelta
from decimal import Decimal
from dataclasses import dataclass, field
from enum import Enum
import networkx as nx
import numpy as np
from collections import defaultdict

from gremlin_python.process.traversal import T
from gremlin_python.process.graph_traversal import __

import httpx
from platformq_shared.cache import CacheManager
from platformq_shared.event_publisher import EventPublisher

logger = logging.getLogger(__name__)


class MarketParticipantType(Enum):
    PROVIDER = "provider"
    CONSUMER = "consumer"
    MARKET_MAKER = "market_maker"
    ARBITRAGEUR = "arbitrageur"
    HEDGER = "hedger"


class RiskCategory(Enum):
    COUNTERPARTY = "counterparty"
    OPERATIONAL = "operational"
    MARKET = "market"
    LIQUIDITY = "liquidity"
    SYSTEMIC = "systemic"


@dataclass
class MarketParticipant:
    """Market participant in compute markets"""
    participant_id: str
    participant_type: MarketParticipantType
    trust_score: float
    trading_volume: Decimal
    success_rate: float
    active_contracts: int
    historical_defaults: int
    reputation_score: float
    collateral_posted: Decimal
    
    def get_risk_profile(self) -> str:
        """Determine risk profile"""
        if self.trust_score > 0.9 and self.historical_defaults == 0:
            return "low_risk"
        elif self.trust_score > 0.7 and self.historical_defaults < 2:
            return "medium_risk"
        else:
            return "high_risk"


@dataclass
class MarketRelationship:
    """Relationship between market participants"""
    from_participant: str
    to_participant: str
    relationship_type: str  # trades_with, provides_to, hedges_with
    volume: Decimal
    success_count: int
    failure_count: int
    average_settlement_time: float
    trust_multiplier: float


@dataclass
class MarketInsight:
    """Insights about compute market dynamics"""
    timestamp: datetime
    market_concentration: float  # Herfindahl index
    liquidity_score: float
    volatility_estimate: float
    network_resilience: float
    systemic_risk_score: float
    dominant_participants: List[str]
    market_inefficiencies: List[Dict[str, Any]]
    arbitrage_opportunities: List[Dict[str, Any]]
    risk_clusters: List[Set[str]]


class ComputeMarketIntelligence:
    """Graph-based intelligence for compute markets"""
    
    def __init__(
        self,
        janusgraph_client,
        derivatives_engine_url: str = "http://derivatives-engine-service:8000",
        cache_manager: Optional[CacheManager] = None,
        event_publisher: Optional[EventPublisher] = None
    ):
        self.graph = janusgraph_client
        self.derivatives_url = derivatives_engine_url
        self.cache = cache_manager or CacheManager()
        self.event_publisher = event_publisher
        self.http_client = httpx.AsyncClient(timeout=30.0)
        self.market_graph = nx.DiGraph()  # In-memory graph for analysis
        
    async def analyze_market_structure(self) -> MarketInsight:
        """Analyze overall market structure and dynamics"""
        try:
            # Get all market participants
            participants = await self._get_market_participants()
            
            # Build relationship graph
            relationships = await self._get_market_relationships()
            self._build_market_graph(participants, relationships)
            
            # Calculate market metrics
            concentration = self._calculate_market_concentration(participants)
            liquidity = self._calculate_liquidity_score(participants, relationships)
            volatility = await self._estimate_market_volatility()
            resilience = self._calculate_network_resilience()
            systemic_risk = self._calculate_systemic_risk()
            
            # Identify patterns
            dominant = self._identify_dominant_participants()
            inefficiencies = await self._find_market_inefficiencies()
            arbitrage = await self._find_arbitrage_opportunities()
            risk_clusters = self._identify_risk_clusters()
            
            return MarketInsight(
                timestamp=datetime.utcnow(),
                market_concentration=concentration,
                liquidity_score=liquidity,
                volatility_estimate=volatility,
                network_resilience=resilience,
                systemic_risk_score=systemic_risk,
                dominant_participants=dominant,
                market_inefficiencies=inefficiencies,
                arbitrage_opportunities=arbitrage,
                risk_clusters=risk_clusters
            )
            
        except Exception as e:
            logger.error(f"Failed to analyze market structure: {e}")
            raise
            
    async def _get_market_participants(self) -> List[MarketParticipant]:
        """Get all participants from graph database"""
        try:
            # Query participants from JanusGraph
            query = self.graph.g.V().hasLabel('market_participant').valueMap(True).toList()
            
            participants = []
            for vertex in query:
                properties = vertex
                
                participant = MarketParticipant(
                    participant_id=properties.get('participant_id', [None])[0],
                    participant_type=MarketParticipantType(properties.get('type', ['consumer'])[0]),
                    trust_score=float(properties.get('trust_score', [0.5])[0]),
                    trading_volume=Decimal(str(properties.get('trading_volume', [0])[0])),
                    success_rate=float(properties.get('success_rate', [0.95])[0]),
                    active_contracts=int(properties.get('active_contracts', [0])[0]),
                    historical_defaults=int(properties.get('historical_defaults', [0])[0]),
                    reputation_score=float(properties.get('reputation_score', [0.5])[0]),
                    collateral_posted=Decimal(str(properties.get('collateral_posted', [0])[0]))
                )
                
                participants.append(participant)
                
            return participants
            
        except Exception as e:
            logger.error(f"Failed to get market participants: {e}")
            return []
            
    async def _get_market_relationships(self) -> List[MarketRelationship]:
        """Get trading relationships from graph"""
        try:
            # Query edges from JanusGraph
            query = self.graph.g.E().hasLabel('trades_with', 'provides_to', 'hedges_with').valueMap(True).toList()
            
            relationships = []
            for edge in query:
                properties = edge
                
                relationship = MarketRelationship(
                    from_participant=properties.get('from_participant', [None])[0],
                    to_participant=properties.get('to_participant', [None])[0],
                    relationship_type=properties.get('type', ['trades_with'])[0],
                    volume=Decimal(str(properties.get('volume', [0])[0])),
                    success_count=int(properties.get('success_count', [0])[0]),
                    failure_count=int(properties.get('failure_count', [0])[0]),
                    average_settlement_time=float(properties.get('avg_settlement_time', [24.0])[0]),
                    trust_multiplier=float(properties.get('trust_multiplier', [1.0])[0])
                )
                
                relationships.append(relationship)
                
            return relationships
            
        except Exception as e:
            logger.error(f"Failed to get market relationships: {e}")
            return []
            
    def _build_market_graph(self, participants: List[MarketParticipant], relationships: List[MarketRelationship]):
        """Build NetworkX graph for analysis"""
        # Add nodes
        for p in participants:
            self.market_graph.add_node(
                p.participant_id,
                type=p.participant_type.value,
                trust_score=p.trust_score,
                volume=float(p.trading_volume),
                risk_profile=p.get_risk_profile()
            )
            
        # Add edges
        for r in relationships:
            self.market_graph.add_edge(
                r.from_participant,
                r.to_participant,
                type=r.relationship_type,
                volume=float(r.volume),
                trust=r.trust_multiplier
            )
            
    def _calculate_market_concentration(self, participants: List[MarketParticipant]) -> float:
        """Calculate Herfindahl-Hirschman Index for market concentration"""
        total_volume = sum(p.trading_volume for p in participants)
        
        if total_volume == 0:
            return 0.0
            
        # Calculate market shares
        shares = [float(p.trading_volume / total_volume) for p in participants]
        
        # HHI = sum of squared market shares
        hhi = sum(s ** 2 for s in shares)
        
        # Normalize to 0-1 scale (max HHI is 1.0)
        return hhi
        
    def _calculate_liquidity_score(self, participants: List[MarketParticipant], relationships: List[MarketRelationship]) -> float:
        """Calculate market liquidity score"""
        # Factors: number of active participants, trading volume, relationship density
        
        active_participants = len([p for p in participants if p.active_contracts > 0])
        total_volume = sum(r.volume for r in relationships)
        
        # Relationship density
        max_relationships = len(participants) * (len(participants) - 1)
        actual_relationships = len(relationships)
        density = actual_relationships / max_relationships if max_relationships > 0 else 0
        
        # Weighted liquidity score
        liquidity = (
            0.3 * min(active_participants / 100, 1.0) +  # Participant score
            0.4 * min(float(total_volume) / 1000000, 1.0) +  # Volume score
            0.3 * density  # Connectivity score
        )
        
        return liquidity
        
    async def _estimate_market_volatility(self) -> float:
        """Estimate market volatility from price history"""
        try:
            # Get historical prices from derivatives engine
            response = await self.http_client.get(
                f"{self.derivatives_url}/api/v1/compute/price-history",
                params={"days": 30}
            )
            
            if response.status_code == 200:
                prices = response.json().get("prices", [])
                
                if len(prices) > 1:
                    # Calculate returns
                    returns = []
                    for i in range(1, len(prices)):
                        ret = (prices[i]["price"] - prices[i-1]["price"]) / prices[i-1]["price"]
                        returns.append(ret)
                        
                    # Standard deviation of returns (annualized)
                    if returns:
                        volatility = np.std(returns) * np.sqrt(365)
                        return volatility
                        
            return 0.2  # Default volatility
            
        except Exception as e:
            logger.error(f"Failed to estimate volatility: {e}")
            return 0.2
            
    def _calculate_network_resilience(self) -> float:
        """Calculate network resilience to participant failures"""
        if not self.market_graph.nodes():
            return 0.0
            
        # Calculate various resilience metrics
        
        # 1. Connectivity - how well connected is the network
        if nx.is_connected(self.market_graph.to_undirected()):
            connectivity_score = 1.0
        else:
            # Fraction of nodes in largest component
            largest_cc = max(nx.connected_components(self.market_graph.to_undirected()), key=len)
            connectivity_score = len(largest_cc) / self.market_graph.number_of_nodes()
            
        # 2. Redundancy - average node degree
        avg_degree = sum(dict(self.market_graph.degree()).values()) / self.market_graph.number_of_nodes()
        redundancy_score = min(avg_degree / 10, 1.0)  # Normalize to 0-1
        
        # 3. Critical nodes - nodes whose removal would disconnect the graph
        critical_nodes = list(nx.articulation_points(self.market_graph.to_undirected()))
        criticality_score = 1.0 - (len(critical_nodes) / self.market_graph.number_of_nodes())
        
        # Combined resilience score
        resilience = (
            0.4 * connectivity_score +
            0.3 * redundancy_score +
            0.3 * criticality_score
        )
        
        return resilience
        
    def _calculate_systemic_risk(self) -> float:
        """Calculate systemic risk score"""
        # Factors: concentration, interconnectedness, contagion potential
        
        # Get high-risk nodes
        high_risk_nodes = [
            n for n, d in self.market_graph.nodes(data=True)
            if d.get('risk_profile') == 'high_risk'
        ]
        
        # Calculate contagion risk
        contagion_risk = 0.0
        for node in high_risk_nodes:
            # PageRank as proxy for systemic importance
            if self.market_graph.nodes():
                pr = nx.pagerank(self.market_graph, alpha=0.85)
                importance = pr.get(node, 0)
                contagion_risk += importance
                
        # Interconnectedness (average clustering coefficient)
        clustering = nx.average_clustering(self.market_graph) if self.market_graph.nodes() else 0
        
        # Concentration risk (from HHI)
        concentration = self._calculate_market_concentration([])  # Would use actual participants
        
        # Combined systemic risk
        systemic_risk = min(
            0.4 * contagion_risk +
            0.3 * clustering +
            0.3 * concentration,
            1.0
        )
        
        return systemic_risk
        
    def _identify_dominant_participants(self) -> List[str]:
        """Identify dominant market participants"""
        if not self.market_graph.nodes():
            return []
            
        # Use PageRank to identify influential participants
        pr = nx.pagerank(self.market_graph, alpha=0.85, weight='volume')
        
        # Sort by PageRank score
        sorted_participants = sorted(pr.items(), key=lambda x: x[1], reverse=True)
        
        # Return top 10 or those with significant influence
        dominant = []
        for participant, score in sorted_participants[:10]:
            if score > 0.05:  # 5% threshold
                dominant.append(participant)
                
        return dominant
        
    async def _find_market_inefficiencies(self) -> List[Dict[str, Any]]:
        """Identify market inefficiencies"""
        inefficiencies = []
        
        try:
            # Get current market data
            response = await self.http_client.get(
                f"{self.derivatives_url}/api/v1/compute/market-inefficiencies"
            )
            
            if response.status_code == 200:
                data = response.json()
                
                # Price disparities across regions
                if "regional_prices" in data:
                    for region, price in data["regional_prices"].items():
                        avg_price = sum(data["regional_prices"].values()) / len(data["regional_prices"])
                        if abs(price - avg_price) / avg_price > 0.1:  # 10% deviation
                            inefficiencies.append({
                                "type": "regional_disparity",
                                "region": region,
                                "price": price,
                                "average_price": avg_price,
                                "deviation": (price - avg_price) / avg_price
                            })
                            
                # Liquidity gaps
                if "liquidity_gaps" in data:
                    inefficiencies.extend(data["liquidity_gaps"])
                    
        except Exception as e:
            logger.error(f"Failed to find inefficiencies: {e}")
            
        return inefficiencies
        
    async def _find_arbitrage_opportunities(self) -> List[Dict[str, Any]]:
        """Find arbitrage opportunities in the market"""
        opportunities = []
        
        try:
            # Get cross-market prices
            response = await self.http_client.get(
                f"{self.derivatives_url}/api/v1/compute/arbitrage-scan"
            )
            
            if response.status_code == 200:
                data = response.json()
                
                for opp in data.get("opportunities", []):
                    if opp["expected_profit"] > 0:
                        opportunities.append(opp)
                        
        except Exception as e:
            logger.error(f"Failed to find arbitrage: {e}")
            
        return opportunities
        
    def _identify_risk_clusters(self) -> List[Set[str]]:
        """Identify clusters of correlated risk"""
        if not self.market_graph.nodes():
            return []
            
        # Find communities in the graph
        communities = nx.community.greedy_modularity_communities(self.market_graph.to_undirected())
        
        risk_clusters = []
        for community in communities:
            # Check if community has high-risk participants
            high_risk_count = sum(
                1 for node in community
                if self.market_graph.nodes[node].get('risk_profile') == 'high_risk'
            )
            
            if high_risk_count > len(community) * 0.3:  # 30% threshold
                risk_clusters.append(set(community))
                
        return risk_clusters
        
    async def calculate_trust_adjusted_margin(
        self,
        participant_id: str,
        base_margin: Decimal
    ) -> Dict[str, Any]:
        """Calculate trust-adjusted margin requirements"""
        try:
            # Get participant from graph
            query = self.graph.g.V().has('participant_id', participant_id).valueMap(True).next()
            
            trust_score = float(query.get('trust_score', [0.5])[0])
            reputation = float(query.get('reputation_score', [0.5])[0])
            defaults = int(query.get('historical_defaults', [0])[0])
            
            # Calculate trust multiplier
            if defaults == 0 and trust_score > 0.9:
                trust_multiplier = 0.5  # 50% margin reduction
            elif defaults == 0 and trust_score > 0.8:
                trust_multiplier = 0.7  # 30% margin reduction
            elif defaults < 2 and trust_score > 0.6:
                trust_multiplier = 1.0  # No adjustment
            else:
                trust_multiplier = 1.5  # 50% margin increase
                
            # Calculate adjusted margin
            adjusted_margin = base_margin * Decimal(str(trust_multiplier))
            
            # Maximum unsecured credit based on reputation
            max_unsecured = Decimal("0")
            if reputation > 0.95 and defaults == 0:
                max_unsecured = Decimal("50000")  # $50k for top users
            elif reputation > 0.85 and defaults == 0:
                max_unsecured = Decimal("20000")  # $20k
            elif reputation > 0.75:
                max_unsecured = Decimal("5000")   # $5k
                
            return {
                "participant_id": participant_id,
                "base_margin": float(base_margin),
                "trust_score": trust_score,
                "trust_multiplier": trust_multiplier,
                "adjusted_margin": float(adjusted_margin),
                "max_unsecured_credit": float(max_unsecured),
                "reputation_score": reputation,
                "historical_defaults": defaults
            }
            
        except Exception as e:
            logger.error(f"Failed to calculate trust-adjusted margin: {e}")
            return {
                "participant_id": participant_id,
                "base_margin": float(base_margin),
                "adjusted_margin": float(base_margin),
                "error": str(e)
            }
            
    async def recommend_risk_mitigation(
        self,
        participant_id: str,
        exposure: Decimal
    ) -> List[Dict[str, Any]]:
        """Recommend risk mitigation strategies"""
        recommendations = []
        
        try:
            # Get participant risk profile
            query = self.graph.g.V().has('participant_id', participant_id).valueMap(True).next()
            
            risk_profile = query.get('risk_profile', ['medium_risk'])[0]
            trust_score = float(query.get('trust_score', [0.5])[0])
            
            # Get market conditions
            market_insight = await self.analyze_market_structure()
            
            # Strategy recommendations based on profile
            if risk_profile == 'high_risk':
                recommendations.append({
                    "strategy": "require_full_collateral",
                    "reason": "High risk profile requires full collateralization",
                    "implementation": "Set margin requirement to 100%"
                })
                
            if market_insight.volatility_estimate > 0.3:
                recommendations.append({
                    "strategy": "use_options_hedging",
                    "reason": f"High market volatility ({market_insight.volatility_estimate:.2%})",
                    "implementation": "Buy protective puts for downside protection"
                })
                
            if exposure > Decimal("100000"):
                recommendations.append({
                    "strategy": "position_limits",
                    "reason": "Large exposure requires position limits",
                    "implementation": f"Limit to {float(exposure * Decimal('0.5'))}"
                })
                
            if participant_id in market_insight.dominant_participants:
                recommendations.append({
                    "strategy": "enhanced_monitoring",
                    "reason": "Dominant market participant requires closer monitoring",
                    "implementation": "Real-time position tracking and alerts"
                })
                
            # Network-based recommendations
            neighbors = list(self.market_graph.neighbors(participant_id)) if participant_id in self.market_graph else []
            
            high_risk_neighbors = [
                n for n in neighbors
                if self.market_graph.nodes[n].get('risk_profile') == 'high_risk'
            ]
            
            if high_risk_neighbors:
                recommendations.append({
                    "strategy": "counterparty_diversification",
                    "reason": f"Connected to {len(high_risk_neighbors)} high-risk counterparties",
                    "implementation": "Require diversification across multiple counterparties"
                })
                
        except Exception as e:
            logger.error(f"Failed to recommend risk mitigation: {e}")
            recommendations.append({
                "strategy": "standard_risk_controls",
                "reason": "Unable to analyze specific risk profile",
                "implementation": "Apply standard margin and position limits"
            })
            
        return recommendations
        
    async def predict_market_impact(
        self,
        order_type: str,
        order_size: Decimal,
        resource_type: str
    ) -> Dict[str, Any]:
        """Predict market impact of large orders"""
        try:
            # Get current market depth
            response = await self.http_client.get(
                f"{self.derivatives_url}/api/v1/compute/market-depth",
                params={"resource_type": resource_type}
            )
            
            if response.status_code == 200:
                depth = response.json()
                
                total_liquidity = Decimal(str(depth.get("total_liquidity", 1000000)))
                
                # Calculate impact based on order size relative to liquidity
                size_ratio = order_size / total_liquidity
                
                # Price impact estimation (simplified square-root model)
                price_impact = float(np.sqrt(float(size_ratio)) * 0.1)  # 10% max impact
                
                # Slippage estimation
                slippage = price_impact * 0.5  # Half of price impact
                
                # Execution risk
                if size_ratio > 0.1:
                    execution_risk = "high"
                elif size_ratio > 0.05:
                    execution_risk = "medium"
                else:
                    execution_risk = "low"
                    
                return {
                    "order_type": order_type,
                    "order_size": float(order_size),
                    "resource_type": resource_type,
                    "estimated_price_impact": price_impact,
                    "estimated_slippage": slippage,
                    "execution_risk": execution_risk,
                    "recommended_execution": self._recommend_execution(size_ratio),
                    "market_liquidity": float(total_liquidity)
                }
                
        except Exception as e:
            logger.error(f"Failed to predict market impact: {e}")
            
        return {
            "error": "Unable to predict market impact",
            "order_size": float(order_size)
        }
        
    def _recommend_execution(self, size_ratio: float) -> str:
        """Recommend execution strategy based on order size"""
        if size_ratio > 0.1:
            return "Split into smaller orders over time (TWAP/VWAP)"
        elif size_ratio > 0.05:
            return "Use iceberg orders to hide size"
        elif size_ratio > 0.01:
            return "Execute as limit order with patience"
        else:
            return "Execute as market order"
            
    async def close(self):
        """Cleanup resources"""
        await self.http_client.aclose() 