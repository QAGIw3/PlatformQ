"""
Graph Manager

Main coordinator for all graph operations including JanusGraph, GraphX analytics,
temporal analysis, trust networks, and lineage tracking.
"""

import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from enum import Enum

from data_intelligence_common import StructuredLogger, EventBus
from data_intelligence_common.vault_consul import VaultConsulIntegration

from .janusgraph_client import JanusGraphClient
from .graphx_analytics import GraphXAnalytics
from .temporal_analyzer import TemporalAnalyzer
from .trust_network import TrustNetwork
from .lineage_tracker import LineageTracker

logger = StructuredLogger.get_logger(__name__)


class GraphType(Enum):
    """Types of graphs supported"""
    KNOWLEDGE = "knowledge"
    TRUST = "trust"
    LINEAGE = "lineage"
    TEMPORAL = "temporal"
    MARKET = "market"
    SOCIAL = "social"


class GraphManager:
    """
    Central manager for all graph operations
    """
    
    def __init__(self, vault_consul: VaultConsulIntegration, event_bus: EventBus):
        self.vault_consul = vault_consul
        self.event_bus = event_bus
        
        # Core components
        self.janusgraph_client: Optional[JanusGraphClient] = None
        self.graphx_analytics: Optional[GraphXAnalytics] = None
        self.temporal_analyzer: Optional[TemporalAnalyzer] = None
        self.trust_network: Optional[TrustNetwork] = None
        self.lineage_tracker: Optional[LineageTracker] = None
        
        # Configuration
        self.config = {
            "janusgraph_url": "ws://janusgraph:8182/gremlin",
            "spark_master": "spark://spark-master:7077",
            "cache_ttl": 300,
            "batch_size": 100,
            "enable_ml": True,
            "enable_temporal": True
        }
        
        # Metrics
        self.metrics = {
            "vertices_created": 0,
            "edges_created": 0,
            "queries_executed": 0,
            "analytics_jobs": 0,
            "avg_query_time_ms": 0
        }
    
    async def initialize(self):
        """Initialize graph manager and all components"""
        logger.info("initializing_graph_manager")
        
        try:
            # Load configuration
            await self._load_configuration()
            
            # Initialize JanusGraph client
            self.janusgraph_client = JanusGraphClient(
                self.config["janusgraph_url"],
                self.vault_consul
            )
            await self.janusgraph_client.initialize()
            
            # Initialize GraphX analytics
            self.graphx_analytics = GraphXAnalytics(
                self.config["spark_master"],
                self.janusgraph_client
            )
            await self.graphx_analytics.initialize()
            
            # Initialize temporal analyzer
            if self.config["enable_temporal"]:
                self.temporal_analyzer = TemporalAnalyzer(
                    self.janusgraph_client,
                    self.event_bus
                )
                await self.temporal_analyzer.initialize()
            
            # Initialize trust network
            self.trust_network = TrustNetwork(
                self.janusgraph_client,
                self.event_bus
            )
            await self.trust_network.initialize()
            
            # Initialize lineage tracker
            self.lineage_tracker = LineageTracker(
                self.janusgraph_client,
                self.event_bus
            )
            await self.lineage_tracker.initialize()
            
            # Setup event handlers
            await self._setup_event_handlers()
            
            logger.info("graph_manager_initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize graph manager: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup all resources"""
        logger.info("cleaning_up_graph_manager")
        
        components = [
            self.janusgraph_client,
            self.graphx_analytics,
            self.temporal_analyzer,
            self.trust_network,
            self.lineage_tracker
        ]
        
        for component in components:
            if component:
                await component.cleanup()
        
        logger.info("graph_manager_cleaned_up")
    
    async def _load_configuration(self):
        """Load configuration from Consul"""
        try:
            config = await self.vault_consul.consul.kv.get("config/graph-manager")
            if config:
                import json
                self.config.update(json.loads(config["Value"].decode()))
        except Exception as e:
            logger.warning(f"Failed to load config from Consul: {e}")
    
    async def _setup_event_handlers(self):
        """Setup event handlers for graph updates"""
        # Handle data lineage updates
        await self.event_bus.subscribe(
            "data.lineage.update",
            self._handle_lineage_update
        )
        
        # Handle trust score updates
        await self.event_bus.subscribe(
            "trust.score.update",
            self._handle_trust_update
        )
        
        # Handle temporal events
        await self.event_bus.subscribe(
            "temporal.event",
            self._handle_temporal_event
        )
    
    # Core graph operations
    async def create_vertex(self, vertex_type: str, properties: Dict[str, Any]) -> str:
        """Create a new vertex"""
        start_time = datetime.utcnow()
        
        try:
            vertex_id = await self.janusgraph_client.create_vertex(vertex_type, properties)
            
            # Update metrics
            self.metrics["vertices_created"] += 1
            
            # Emit event
            await self.event_bus.publish(
                "graph.vertex.created",
                {
                    "vertex_id": vertex_id,
                    "type": vertex_type,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            # Update query time
            query_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_avg_query_time(query_time)
            
            return vertex_id
            
        except Exception as e:
            logger.error(f"Failed to create vertex: {e}")
            raise
    
    async def create_edge(self, edge_type: str, source_id: str, 
                         target_id: str, properties: Dict[str, Any] = None) -> str:
        """Create a new edge"""
        start_time = datetime.utcnow()
        
        try:
            edge_id = await self.janusgraph_client.create_edge(
                edge_type, source_id, target_id, properties
            )
            
            # Update metrics
            self.metrics["edges_created"] += 1
            
            # Handle special edge types
            if edge_type == "DERIVES_FROM":
                await self.lineage_tracker.update_lineage(source_id, target_id)
            elif edge_type == "TRUSTS":
                await self.trust_network.update_trust(source_id, target_id, properties)
            
            # Emit event
            await self.event_bus.publish(
                "graph.edge.created",
                {
                    "edge_id": edge_id,
                    "type": edge_type,
                    "source": source_id,
                    "target": target_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
            
            # Update query time
            query_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_avg_query_time(query_time)
            
            return edge_id
            
        except Exception as e:
            logger.error(f"Failed to create edge: {e}")
            raise
    
    async def query_graph(self, query: str, bindings: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute a Gremlin query"""
        start_time = datetime.utcnow()
        
        try:
            results = await self.janusgraph_client.execute_query(query, bindings)
            
            # Update metrics
            self.metrics["queries_executed"] += 1
            
            # Update query time
            query_time = (datetime.utcnow() - start_time).total_seconds() * 1000
            self._update_avg_query_time(query_time)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise
    
    async def find_shortest_path(self, source_id: str, target_id: str,
                               max_depth: int = 10) -> Optional[List[str]]:
        """Find shortest path between two vertices"""
        query = """
        g.V(source).repeat(out().simplePath()).until(hasId(target).or().loops().is(max_depth))
         .path().limit(1).by('id')
        """
        
        results = await self.query_graph(query, {
            "source": source_id,
            "target": target_id,
            "max_depth": max_depth
        })
        
        return results[0] if results else None
    
    async def get_neighbors(self, vertex_id: str, edge_label: Optional[str] = None,
                          direction: str = "both", limit: int = 100) -> List[Dict[str, Any]]:
        """Get neighbors of a vertex"""
        if edge_label:
            if direction == "out":
                query = "g.V(vertex_id).out(edge_label).limit(limit).valueMap(true)"
            elif direction == "in":
                query = "g.V(vertex_id).in(edge_label).limit(limit).valueMap(true)"
            else:
                query = "g.V(vertex_id).both(edge_label).limit(limit).valueMap(true)"
        else:
            if direction == "out":
                query = "g.V(vertex_id).out().limit(limit).valueMap(true)"
            elif direction == "in":
                query = "g.V(vertex_id).in().limit(limit).valueMap(true)"
            else:
                query = "g.V(vertex_id).both().limit(limit).valueMap(true)"
        
        return await self.query_graph(query, {
            "vertex_id": vertex_id,
            "edge_label": edge_label,
            "limit": limit
        })
    
    # Analytics operations
    async def run_analytics(self, algorithm: str, graph_id: str,
                          params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Run graph analytics algorithm"""
        self.metrics["analytics_jobs"] += 1
        
        algorithms = {
            "pagerank": self.graphx_analytics.compute_pagerank,
            "community_detection": self.graphx_analytics.detect_communities,
            "centrality": self.graphx_analytics.compute_centrality,
            "clustering": self.graphx_analytics.compute_clustering_coefficient,
            "triangle_count": self.graphx_analytics.count_triangles,
            "connected_components": self.graphx_analytics.find_connected_components,
            "shortest_paths": self.graphx_analytics.compute_shortest_paths,
            "influence_propagation": self.graphx_analytics.simulate_influence_propagation
        }
        
        if algorithm not in algorithms:
            raise ValueError(f"Unknown algorithm: {algorithm}")
        
        # Run the algorithm
        result = await algorithms[algorithm](graph_id, params or {})
        
        # Emit event
        await self.event_bus.publish(
            "graph.analytics.completed",
            {
                "algorithm": algorithm,
                "graph_id": graph_id,
                "timestamp": datetime.utcnow().isoformat()
            }
        )
        
        return result
    
    # Temporal operations
    async def create_temporal_event(self, event_type: str, entity_id: str,
                                  timestamp: datetime, properties: Dict[str, Any] = None) -> str:
        """Create a temporal event"""
        if not self.temporal_analyzer:
            raise RuntimeError("Temporal analysis not enabled")
        
        return await self.temporal_analyzer.create_event(
            event_type, entity_id, timestamp, properties
        )
    
    async def query_temporal_range(self, entity_id: str, start_time: datetime,
                                 end_time: datetime) -> List[Dict[str, Any]]:
        """Query temporal events in a time range"""
        if not self.temporal_analyzer:
            raise RuntimeError("Temporal analysis not enabled")
        
        return await self.temporal_analyzer.query_time_range(
            entity_id, start_time, end_time
        )
    
    async def detect_temporal_patterns(self, entity_id: str,
                                     pattern_type: str = "periodic") -> List[Dict[str, Any]]:
        """Detect temporal patterns"""
        if not self.temporal_analyzer:
            raise RuntimeError("Temporal analysis not enabled")
        
        return await self.temporal_analyzer.detect_patterns(entity_id, pattern_type)
    
    # Trust operations
    async def calculate_trust_score(self, source_id: str, target_id: str) -> float:
        """Calculate trust score between entities"""
        return await self.trust_network.calculate_trust_score(source_id, target_id)
    
    async def get_trust_path(self, source_id: str, target_id: str,
                           min_trust: float = 0.5) -> Optional[List[Tuple[str, float]]]:
        """Get trust path between entities"""
        return await self.trust_network.find_trust_path(source_id, target_id, min_trust)
    
    async def propagate_trust(self, entity_id: str, initial_trust: float = 1.0,
                            decay_factor: float = 0.8, max_hops: int = 3) -> Dict[str, float]:
        """Propagate trust from an entity"""
        return await self.trust_network.propagate_trust(
            entity_id, initial_trust, decay_factor, max_hops
        )
    
    # Lineage operations
    async def get_lineage(self, entity_id: str, direction: str = "both",
                        max_depth: int = 5) -> Dict[str, Any]:
        """Get data lineage"""
        return await self.lineage_tracker.get_lineage(entity_id, direction, max_depth)
    
    async def get_impact_analysis(self, entity_id: str) -> Dict[str, Any]:
        """Analyze impact of changes to an entity"""
        return await self.lineage_tracker.analyze_impact(entity_id)
    
    async def validate_lineage(self, entity_id: str) -> Dict[str, Any]:
        """Validate lineage consistency"""
        return await self.lineage_tracker.validate_lineage(entity_id)
    
    # Event handlers
    async def _handle_lineage_update(self, event: Dict[str, Any]):
        """Handle lineage update events"""
        entity_id = event.get("entity_id")
        if entity_id:
            await self.lineage_tracker.refresh_lineage(entity_id)
    
    async def _handle_trust_update(self, event: Dict[str, Any]):
        """Handle trust update events"""
        source_id = event.get("source_id")
        target_id = event.get("target_id")
        trust_score = event.get("trust_score")
        
        if source_id and target_id and trust_score is not None:
            await self.trust_network.update_trust(source_id, target_id, {"score": trust_score})
    
    async def _handle_temporal_event(self, event: Dict[str, Any]):
        """Handle temporal events"""
        if self.temporal_analyzer:
            await self.temporal_analyzer.process_event(event)
    
    def _update_avg_query_time(self, query_time: float):
        """Update average query time metric"""
        total_queries = self.metrics["queries_executed"]
        
        if total_queries == 1:
            self.metrics["avg_query_time_ms"] = query_time
        else:
            current_avg = self.metrics["avg_query_time_ms"]
            self.metrics["avg_query_time_ms"] = (
                (current_avg * (total_queries - 1) + query_time) / total_queries
            )
    
    async def health_check(self) -> Dict[str, Any]:
        """Check graph manager health"""
        components_health = {}
        
        # Check each component
        if self.janusgraph_client:
            components_health["janusgraph"] = await self.janusgraph_client.health_check()
        
        if self.graphx_analytics:
            components_health["graphx"] = await self.graphx_analytics.health_check()
        
        if self.temporal_analyzer:
            components_health["temporal"] = await self.temporal_analyzer.health_check()
        
        if self.trust_network:
            components_health["trust"] = await self.trust_network.health_check()
        
        if self.lineage_tracker:
            components_health["lineage"] = await self.lineage_tracker.health_check()
        
        # Overall health
        all_healthy = all(h.get("healthy", False) for h in components_health.values())
        
        return {
            "healthy": all_healthy,
            "components": components_health,
            "metrics": self.metrics
        } 