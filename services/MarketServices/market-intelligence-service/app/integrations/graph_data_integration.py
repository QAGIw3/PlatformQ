"""
Graph Data Integration for Market Intelligence

Leverages JanusGraph knowledge graph to enhance market predictions with:
- Trader relationship networks
- Asset correlation graphs
- Market manipulation detection
- Systemic risk propagation
"""

import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime, timedelta
from decimal import Decimal
import asyncio
import networkx as nx
from collections import defaultdict

from platformq_shared import ServiceClient
from ..models.market_insight import MarketInsight, TraderBehavior
from ..analytics.anomaly_detector import AnomalyDetector

logger = logging.getLogger(__name__)


class GraphDataIntegration:
    """Integration with graph-intelligence-service for enhanced market insights"""
    
    def __init__(self):
        self.graph_client = ServiceClient(
            service_name="graph-intelligence-service",
            circuit_breaker_threshold=5,
            rate_limit=30.0
        )
        
        # Cache for graph data
        self.trader_networks: Dict[str, nx.Graph] = {}
        self.asset_correlations: Dict[str, Dict[str, float]] = {}
        self.influence_scores: Dict[str, float] = {}
        
        # Background tasks
        self._network_update_task = None
        self._influence_calc_task = None
        
    async def initialize(self):
        """Initialize graph data integration"""
        logger.info("Initializing Graph Data Integration")
        
        # Start background tasks
        self._network_update_task = asyncio.create_task(self._update_networks_loop())
        self._influence_calc_task = asyncio.create_task(self._calculate_influence_loop())
        
        # Load initial data
        await self._load_initial_graph_data()
        
    async def get_trader_network_insights(self, trader_id: str) -> Dict[str, Any]:
        """Get network-based insights for a trader"""
        try:
            # Get trader's network from graph service
            network_data = await self.graph_client.request(
                method="POST",
                path="/api/v1/entity/network",
                json={
                    "entity_id": trader_id,
                    "entity_type": "trader",
                    "depth": 2,
                    "relationship_types": ["follows", "copies", "trades_with"],
                    "include_metrics": True
                }
            )
            
            # Build local network graph
            G = nx.Graph()
            for node in network_data["nodes"]:
                G.add_node(node["id"], **node["properties"])
            
            for edge in network_data["edges"]:
                G.add_edge(
                    edge["source"],
                    edge["target"],
                    weight=edge.get("strength", 1.0),
                    type=edge["type"]
                )
            
            # Calculate network metrics
            insights = {
                "network_size": G.number_of_nodes(),
                "connectivity": nx.density(G),
                "centrality": nx.degree_centrality(G).get(trader_id, 0),
                "betweenness": nx.betweenness_centrality(G).get(trader_id, 0),
                "clustering": nx.clustering(G).get(trader_id, 0),
                "influence_score": self._calculate_influence(G, trader_id),
                "copy_risk": self._calculate_copy_risk(G, trader_id),
                "network_sentiment": await self._get_network_sentiment(G, trader_id)
            }
            
            # Detect trading cliques
            cliques = self._detect_trading_cliques(G, trader_id)
            if cliques:
                insights["clique_membership"] = cliques
                insights["manipulation_risk"] = self._assess_manipulation_risk(cliques)
            
            return insights
            
        except Exception as e:
            logger.error(f"Failed to get trader network insights: {e}")
            return {}
            
    async def detect_market_manipulation(self, 
                                       market_id: str,
                                       time_window: timedelta) -> List[Dict[str, Any]]:
        """Detect potential market manipulation using graph patterns"""
        try:
            # Get trading relationships in the market
            market_graph = await self.graph_client.request(
                method="POST",
                path="/api/v1/market/trading-graph",
                json={
                    "market_id": market_id,
                    "start_time": (datetime.utcnow() - time_window).isoformat(),
                    "end_time": datetime.utcnow().isoformat(),
                    "min_trade_count": 5
                }
            )
            
            # Build temporal trading graph
            G = nx.DiGraph()
            for trade in market_graph["trades"]:
                G.add_edge(
                    trade["buyer"],
                    trade["seller"],
                    timestamp=trade["timestamp"],
                    price=trade["price"],
                    volume=trade["volume"]
                )
            
            # Detect manipulation patterns
            manipulations = []
            
            # 1. Wash trading detection
            wash_trades = self._detect_wash_trading(G)
            if wash_trades:
                manipulations.extend([{
                    "type": "wash_trading",
                    "severity": "high",
                    "traders": list(traders),
                    "pattern": pattern,
                    "confidence": confidence
                } for traders, pattern, confidence in wash_trades])
            
            # 2. Pump and dump detection
            pump_dumps = await self._detect_pump_and_dump(G, market_id)
            if pump_dumps:
                manipulations.extend([{
                    "type": "pump_and_dump",
                    "severity": "critical",
                    "orchestrator": pd["orchestrator"],
                    "participants": pd["participants"],
                    "timeline": pd["timeline"],
                    "confidence": pd["confidence"]
                } for pd in pump_dumps])
            
            # 3. Spoofing detection
            spoofing = self._detect_spoofing_patterns(G, market_graph.get("orders", []))
            if spoofing:
                manipulations.extend([{
                    "type": "spoofing",
                    "severity": "medium",
                    "trader": spoof["trader"],
                    "pattern": spoof["pattern"],
                    "frequency": spoof["frequency"]
                } for spoof in spoofing])
            
            return manipulations
            
        except Exception as e:
            logger.error(f"Failed to detect market manipulation: {e}")
            return []
            
    async def analyze_systemic_risk(self, 
                                  market_ids: List[str]) -> Dict[str, Any]:
        """Analyze systemic risk across markets using graph propagation"""
        try:
            # Get cross-market relationships
            cross_market = await self.graph_client.request(
                method="POST",
                path="/api/v1/analysis/cross-market-graph",
                json={
                    "market_ids": market_ids,
                    "relationship_types": ["correlated", "arbitrage", "hedged"],
                    "min_correlation": 0.5
                }
            )
            
            # Build market correlation graph
            G = nx.Graph()
            for market in cross_market["markets"]:
                G.add_node(market["id"], **market["properties"])
            
            for edge in cross_market["correlations"]:
                G.add_edge(
                    edge["market1"],
                    edge["market2"],
                    weight=edge["correlation"],
                    type=edge["relationship"]
                )
            
            # Calculate systemic risk metrics
            risk_analysis = {
                "connectivity": nx.density(G),
                "clustering": nx.average_clustering(G),
                "contagion_paths": self._find_contagion_paths(G),
                "central_markets": self._identify_central_markets(G),
                "risk_clusters": self._identify_risk_clusters(G),
                "propagation_speed": self._estimate_propagation_speed(G)
            }
            
            # Simulate shock propagation
            shock_scenarios = []
            for market in risk_analysis["central_markets"][:3]:
                scenario = await self._simulate_shock_propagation(
                    G, 
                    market["id"], 
                    shock_size=0.2
                )
                shock_scenarios.append(scenario)
            
            risk_analysis["shock_scenarios"] = shock_scenarios
            
            return risk_analysis
            
        except Exception as e:
            logger.error(f"Failed to analyze systemic risk: {e}")
            return {}
            
    async def get_asset_correlation_graph(self, 
                                        asset_ids: List[str],
                                        time_period: str = "30d") -> Dict[str, Any]:
        """Get correlation graph for assets"""
        try:
            correlation_data = await self.graph_client.request(
                method="POST",
                path="/api/v1/assets/correlation-graph",
                json={
                    "asset_ids": asset_ids,
                    "time_period": time_period,
                    "correlation_threshold": 0.3
                }
            )
            
            # Cache correlations
            for asset1, correlations in correlation_data["correlations"].items():
                if asset1 not in self.asset_correlations:
                    self.asset_correlations[asset1] = {}
                self.asset_correlations[asset1].update(correlations)
            
            return correlation_data
            
        except Exception as e:
            logger.error(f"Failed to get asset correlation graph: {e}")
            return {}
            
    def _calculate_influence(self, G: nx.Graph, trader_id: str) -> float:
        """Calculate trader's influence in the network"""
        if trader_id not in G:
            return 0.0
            
        # PageRank-based influence
        pagerank = nx.pagerank(G, weight='weight')
        
        # Copy trading influence
        copy_influence = 0
        for neighbor in G.neighbors(trader_id):
            edge_data = G.get_edge_data(trader_id, neighbor)
            if edge_data.get('type') == 'copies':
                copy_influence += 1
                
        # Combined influence score
        influence = (pagerank.get(trader_id, 0) * 0.7 + 
                    copy_influence / max(G.number_of_nodes(), 1) * 0.3)
        
        return min(influence * 100, 100)  # Normalize to 0-100
        
    def _calculate_copy_risk(self, G: nx.Graph, trader_id: str) -> float:
        """Calculate risk from copy trading cascades"""
        if trader_id not in G:
            return 0.0
            
        # Find all traders copying this trader
        copiers = set()
        for neighbor in G.neighbors(trader_id):
            edge_data = G.get_edge_data(trader_id, neighbor)
            if edge_data.get('type') == 'copies':
                copiers.add(neighbor)
                
        # Calculate cascade depth
        cascade_depth = 0
        current_level = copiers
        while current_level and cascade_depth < 5:
            next_level = set()
            for copier in current_level:
                for neighbor in G.neighbors(copier):
                    edge_data = G.get_edge_data(copier, neighbor)
                    if edge_data.get('type') == 'copies':
                        next_level.add(neighbor)
            current_level = next_level
            cascade_depth += 1
            
        # Risk score based on cascade size and depth
        risk = min((len(copiers) * 0.1 + cascade_depth * 0.2) * 100, 100)
        
        return risk
        
    def _detect_trading_cliques(self, G: nx.Graph, trader_id: str) -> List[Set[str]]:
        """Detect trading cliques that include the trader"""
        cliques = []
        
        # Find all maximal cliques
        all_cliques = list(nx.find_cliques(G.to_undirected()))
        
        # Filter cliques that include the trader
        for clique in all_cliques:
            if trader_id in clique and len(clique) >= 3:
                cliques.append(set(clique))
                
        return cliques
        
    def _assess_manipulation_risk(self, cliques: List[Set[str]]) -> float:
        """Assess risk of coordinated manipulation from cliques"""
        if not cliques:
            return 0.0
            
        # Risk factors
        max_clique_size = max(len(clique) for clique in cliques)
        num_cliques = len(cliques)
        
        # Calculate risk score
        risk = min((max_clique_size * 0.15 + num_cliques * 0.1) * 100, 100)
        
        return risk
        
    def _detect_wash_trading(self, G: nx.DiGraph) -> List[Tuple[Set[str], str, float]]:
        """Detect wash trading patterns"""
        wash_trades = []
        
        # Look for cycles in trading graph
        try:
            cycles = list(nx.simple_cycles(G))
            
            for cycle in cycles:
                if 2 <= len(cycle) <= 4:  # Small cycles indicate wash trading
                    # Check if trades happen in quick succession
                    cycle_edges = [(cycle[i], cycle[(i+1)%len(cycle)]) 
                                  for i in range(len(cycle))]
                    
                    timestamps = []
                    for u, v in cycle_edges:
                        edge_data = G.get_edge_data(u, v)
                        if edge_data:
                            timestamps.append(edge_data.get('timestamp'))
                    
                    if timestamps and all(timestamps):
                        # Check if all trades happened within 5 minutes
                        time_span = max(timestamps) - min(timestamps)
                        if time_span.total_seconds() < 300:
                            confidence = 0.9 if len(cycle) == 2 else 0.7
                            wash_trades.append((
                                set(cycle),
                                f"{len(cycle)}-party wash",
                                confidence
                            ))
                            
        except nx.NetworkXError:
            pass
            
        return wash_trades
        
    async def _detect_pump_and_dump(self, 
                                   G: nx.DiGraph,
                                   market_id: str) -> List[Dict[str, Any]]:
        """Detect pump and dump schemes"""
        pump_dumps = []
        
        # Get price data
        price_data = await self.graph_client.request(
            method="GET",
            path=f"/api/v1/market/{market_id}/price-history",
            params={"period": "7d"}
        )
        
        # Find sudden price spikes
        prices = price_data.get("prices", [])
        for i in range(1, len(prices)):
            if prices[i]["price"] > prices[i-1]["price"] * 1.2:  # 20% spike
                # Look for coordinated buying before spike
                spike_time = datetime.fromisoformat(prices[i]["timestamp"])
                pre_spike_traders = set()
                
                for u, v, data in G.edges(data=True):
                    trade_time = data.get("timestamp")
                    if trade_time and (spike_time - trade_time).total_seconds() < 3600:
                        pre_spike_traders.add(u)
                        
                # Look for coordinated selling after spike
                post_spike_traders = set()
                for u, v, data in G.edges(data=True):
                    trade_time = data.get("timestamp")
                    if trade_time and 0 < (trade_time - spike_time).total_seconds() < 3600:
                        post_spike_traders.add(v)
                        
                # Find overlap
                orchestrators = pre_spike_traders & post_spike_traders
                if orchestrators:
                    pump_dumps.append({
                        "orchestrator": list(orchestrators)[0],
                        "participants": list(pre_spike_traders | post_spike_traders),
                        "timeline": {
                            "pump_start": (spike_time - timedelta(hours=1)).isoformat(),
                            "spike": spike_time.isoformat(),
                            "dump_end": (spike_time + timedelta(hours=1)).isoformat()
                        },
                        "confidence": min(0.6 + len(orchestrators) * 0.1, 0.9)
                    })
                    
        return pump_dumps
        
    def _detect_spoofing_patterns(self, 
                                 G: nx.DiGraph,
                                 orders: List[Dict]) -> List[Dict[str, Any]]:
        """Detect spoofing patterns in order data"""
        spoofing_patterns = []
        
        # Group orders by trader
        trader_orders = defaultdict(list)
        for order in orders:
            trader_orders[order["trader_id"]].append(order)
            
        # Look for patterns
        for trader_id, trader_order_list in trader_orders.items():
            cancelled_large = 0
            executed_small = 0
            
            for order in trader_order_list:
                if order["status"] == "cancelled" and order["size"] > 1000:
                    cancelled_large += 1
                elif order["status"] == "executed" and order["size"] < 100:
                    executed_small += 1
                    
            # Spoofing pattern: many cancelled large orders, few executed small orders
            if cancelled_large > 10 and executed_small > 0:
                ratio = cancelled_large / max(executed_small, 1)
                if ratio > 5:
                    spoofing_patterns.append({
                        "trader": trader_id,
                        "pattern": "large_cancel_small_execute",
                        "frequency": cancelled_large,
                        "confidence": min(0.5 + ratio * 0.05, 0.9)
                    })
                    
        return spoofing_patterns
        
    async def _get_network_sentiment(self, G: nx.Graph, trader_id: str) -> float:
        """Get sentiment of trader's network"""
        if trader_id not in G:
            return 0.0
            
        # Get sentiment for connected traders
        sentiments = []
        for neighbor in G.neighbors(trader_id):
            trader_data = G.nodes[neighbor]
            if 'sentiment' in trader_data:
                sentiments.append(trader_data['sentiment'])
                
        if not sentiments:
            return 0.0
            
        # Weighted average based on edge strength
        weighted_sum = 0
        total_weight = 0
        
        for neighbor in G.neighbors(trader_id):
            edge_weight = G[trader_id][neighbor].get('weight', 1.0)
            trader_sentiment = G.nodes[neighbor].get('sentiment', 0.0)
            
            weighted_sum += trader_sentiment * edge_weight
            total_weight += edge_weight
            
        return weighted_sum / max(total_weight, 1)
        
    def _find_contagion_paths(self, G: nx.Graph) -> List[List[str]]:
        """Find potential contagion paths in market graph"""
        paths = []
        
        # Find paths between highly connected nodes
        central_nodes = [node for node, degree in G.degree() if degree > 3]
        
        for i in range(len(central_nodes)):
            for j in range(i + 1, len(central_nodes)):
                try:
                    path = nx.shortest_path(G, central_nodes[i], central_nodes[j])
                    if 2 < len(path) < 6:  # Meaningful contagion paths
                        paths.append(path)
                except nx.NetworkXNoPath:
                    pass
                    
        return paths[:10]  # Top 10 paths
        
    def _identify_central_markets(self, G: nx.Graph) -> List[Dict[str, Any]]:
        """Identify systemically important markets"""
        centrality_measures = {
            "degree": nx.degree_centrality(G),
            "betweenness": nx.betweenness_centrality(G),
            "eigenvector": nx.eigenvector_centrality(G, max_iter=1000)
        }
        
        # Combine centrality measures
        combined_scores = {}
        for node in G.nodes():
            combined_scores[node] = (
                centrality_measures["degree"].get(node, 0) * 0.3 +
                centrality_measures["betweenness"].get(node, 0) * 0.4 +
                centrality_measures["eigenvector"].get(node, 0) * 0.3
            )
            
        # Sort by importance
        sorted_markets = sorted(
            combined_scores.items(), 
            key=lambda x: x[1], 
            reverse=True
        )
        
        return [
            {
                "id": market_id,
                "importance_score": score * 100,
                "metrics": {
                    "degree": centrality_measures["degree"].get(market_id, 0),
                    "betweenness": centrality_measures["betweenness"].get(market_id, 0),
                    "eigenvector": centrality_measures["eigenvector"].get(market_id, 0)
                }
            }
            for market_id, score in sorted_markets[:10]
        ]
        
    def _identify_risk_clusters(self, G: nx.Graph) -> List[Set[str]]:
        """Identify clusters of correlated markets"""
        # Use community detection
        communities = list(nx.community.greedy_modularity_communities(G))
        
        # Filter significant clusters
        risk_clusters = [
            set(community) for community in communities 
            if len(community) >= 3
        ]
        
        return risk_clusters
        
    def _estimate_propagation_speed(self, G: nx.Graph) -> float:
        """Estimate speed of risk propagation"""
        if G.number_of_nodes() < 2:
            return 0.0
            
        # Average shortest path length
        try:
            avg_path_length = nx.average_shortest_path_length(G)
            
            # Propagation speed inversely related to path length
            speed = 1.0 / max(avg_path_length, 1)
            
            # Adjust for edge weights (correlation strength)
            avg_weight = sum(data['weight'] for _, _, data in G.edges(data=True)) / G.number_of_edges()
            
            return min(speed * avg_weight * 100, 100)
            
        except nx.NetworkXError:
            return 50.0  # Default medium speed
            
    async def _simulate_shock_propagation(self,
                                        G: nx.Graph,
                                        shock_market: str,
                                        shock_size: float) -> Dict[str, Any]:
        """Simulate how a shock propagates through the market network"""
        if shock_market not in G:
            return {}
            
        # Initialize shock levels
        shock_levels = {node: 0.0 for node in G.nodes()}
        shock_levels[shock_market] = shock_size
        
        # Propagate shock (3 iterations)
        propagation_history = [shock_levels.copy()]
        
        for iteration in range(3):
            new_shock_levels = shock_levels.copy()
            
            for node in G.nodes():
                if node == shock_market:
                    continue
                    
                # Propagate from neighbors
                total_propagation = 0
                for neighbor in G.neighbors(node):
                    if shock_levels[neighbor] > 0:
                        correlation = G[node][neighbor].get('weight', 0.5)
                        propagation = shock_levels[neighbor] * correlation * 0.7
                        total_propagation += propagation
                        
                new_shock_levels[node] = min(
                    shock_levels[node] + total_propagation,
                    shock_size  # Cap at original shock size
                )
                
            shock_levels = new_shock_levels
            propagation_history.append(shock_levels.copy())
            
        # Calculate impact metrics
        affected_markets = [
            node for node, level in shock_levels.items() 
            if level > shock_size * 0.1
        ]
        
        return {
            "shock_market": shock_market,
            "shock_size": shock_size,
            "affected_markets": affected_markets,
            "max_propagation": max(shock_levels.values()),
            "avg_impact": sum(shock_levels.values()) / len(shock_levels),
            "propagation_history": propagation_history
        }
        
    async def _update_networks_loop(self):
        """Periodically update trader networks"""
        while True:
            try:
                # Update influential traders' networks
                for trader_id in list(self.influence_scores.keys())[:50]:
                    await self.get_trader_network_insights(trader_id)
                    
                await asyncio.sleep(300)  # Every 5 minutes
                
            except Exception as e:
                logger.error(f"Error in network update loop: {e}")
                await asyncio.sleep(600)
                
    async def _calculate_influence_loop(self):
        """Periodically recalculate influence scores"""
        while True:
            try:
                # Get top traders
                top_traders = await self.graph_client.request(
                    method="GET",
                    path="/api/v1/traders/top",
                    params={"limit": 100, "metric": "activity"}
                )
                
                # Update influence scores
                for trader in top_traders.get("traders", []):
                    insights = await self.get_trader_network_insights(trader["id"])
                    self.influence_scores[trader["id"]] = insights.get("influence_score", 0)
                    
                await asyncio.sleep(3600)  # Every hour
                
            except Exception as e:
                logger.error(f"Error in influence calculation loop: {e}")
                await asyncio.sleep(3600)
                
    async def _load_initial_graph_data(self):
        """Load initial graph data on startup"""
        try:
            # Load market correlations
            markets = await self.graph_client.request(
                method="GET",
                path="/api/v1/markets/active"
            )
            
            if markets.get("markets"):
                market_ids = [m["id"] for m in markets["markets"][:20]]
                await self.get_asset_correlation_graph(market_ids)
                
        except Exception as e:
            logger.error(f"Failed to load initial graph data: {e}") 