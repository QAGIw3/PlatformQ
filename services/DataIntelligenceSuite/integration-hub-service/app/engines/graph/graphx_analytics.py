"""
GraphX Analytics Engine

Runs graph analytics algorithms using Apache Spark GraphX/GraphFrames.
"""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
from pyspark.sql import SparkSession
from graphframes import GraphFrame

from data_intelligence_common import StructuredLogger
from .janusgraph_client import JanusGraphClient
from .algorithms import (
    PageRankAlgorithm,
    CommunityDetectionAlgorithm,
    CentralityAlgorithm,
    ClusteringAlgorithm,
    ShortestPathAlgorithm,
    InfluencePropagationAlgorithm
)

logger = StructuredLogger.get_logger(__name__)


class GraphXAnalytics:
    """
    GraphX analytics engine for large-scale graph processing
    """
    
    def __init__(self, spark_master: str, janusgraph_client: JanusGraphClient):
        self.spark_master = spark_master
        self.janusgraph_client = janusgraph_client
        self.spark: Optional[SparkSession] = None
        
        # Algorithm instances
        self.algorithms = {}
        
        # Cache for loaded graphs
        self.graph_cache: Dict[str, GraphFrame] = {}
        self.cache_ttl = 3600  # 1 hour
        self.cache_timestamps: Dict[str, datetime] = {}
    
    async def initialize(self):
        """Initialize Spark session and algorithms"""
        logger.info("initializing_graphx_analytics")
        
        try:
            # Create Spark session
            self.spark = SparkSession.builder \
                .appName("GraphXAnalytics") \
                .master(self.spark_master) \
                .config("spark.jars.packages", "graphframes:graphframes:0.8.2-spark3.2-s_2.12") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            # Initialize algorithms
            self._initialize_algorithms()
            
            logger.info("graphx_analytics_initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize GraphX analytics: {e}")
            raise
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.spark:
            self.spark.stop()
    
    def _initialize_algorithms(self):
        """Initialize algorithm instances"""
        self.algorithms = {
            "pagerank": PageRankAlgorithm(self.spark),
            "community_detection": CommunityDetectionAlgorithm(self.spark),
            "centrality": CentralityAlgorithm(self.spark),
            "clustering": ClusteringAlgorithm(self.spark),
            "shortest_path": ShortestPathAlgorithm(self.spark),
            "influence_propagation": InfluencePropagationAlgorithm(self.spark)
        }
    
    async def load_graph_from_janusgraph(self, graph_id: str, 
                                        vertex_filter: Optional[str] = None,
                                        edge_filter: Optional[str] = None) -> GraphFrame:
        """
        Load graph data from JanusGraph into GraphFrame
        
        Args:
            graph_id: Graph identifier
            vertex_filter: Optional Gremlin filter for vertices
            edge_filter: Optional Gremlin filter for edges
            
        Returns:
            GraphFrame with vertices and edges
        """
        # Check cache first
        if graph_id in self.graph_cache:
            cache_time = self.cache_timestamps.get(graph_id)
            if cache_time and (datetime.utcnow() - cache_time).seconds < self.cache_ttl:
                logger.info(f"Using cached graph: {graph_id}")
                return self.graph_cache[graph_id]
        
        logger.info(f"Loading graph {graph_id} from JanusGraph")
        
        # Load vertices
        vertex_query = "g.V()"
        if vertex_filter:
            vertex_query += f".{vertex_filter}"
        vertex_query += ".valueMap(true)"
        
        vertices_data = await self.janusgraph_client.execute_query(vertex_query)
        
        # Convert to Spark DataFrame
        vertices_rows = []
        for v in vertices_data:
            row = {"id": v.get("id", [None])[0]}
            for key, value in v.items():
                if key != "id":
                    row[key] = value[0] if isinstance(value, list) and len(value) == 1 else value
            vertices_rows.append(row)
        
        vertices_df = self.spark.createDataFrame(vertices_rows)
        
        # Load edges
        edge_query = "g.E()"
        if edge_filter:
            edge_query += f".{edge_filter}"
        edge_query += ".project('src', 'dst', 'relationship', 'properties')" \
                     ".by(outV().id()).by(inV().id()).by(label()).by(valueMap())"
        
        edges_data = await self.janusgraph_client.execute_query(edge_query)
        
        # Convert to Spark DataFrame
        edges_rows = []
        for e in edges_data:
            row = {
                "src": e["src"],
                "dst": e["dst"],
                "relationship": e["relationship"]
            }
            # Add edge properties
            for key, value in e.get("properties", {}).items():
                row[key] = value[0] if isinstance(value, list) and len(value) == 1 else value
            edges_rows.append(row)
        
        edges_df = self.spark.createDataFrame(edges_rows)
        
        # Create GraphFrame
        graph = GraphFrame(vertices_df, edges_df)
        
        # Cache the graph
        self.graph_cache[graph_id] = graph
        self.cache_timestamps[graph_id] = datetime.utcnow()
        
        return graph
    
    async def compute_pagerank(self, graph_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Compute PageRank scores"""
        graph = await self.load_graph_from_janusgraph(graph_id)
        algorithm = self.algorithms["pagerank"]
        return await algorithm.run(graph, params)
    
    async def detect_communities(self, graph_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Detect communities in the graph"""
        graph = await self.load_graph_from_janusgraph(graph_id)
        algorithm = self.algorithms["community_detection"]
        return await algorithm.run(graph, params)
    
    async def compute_centrality(self, graph_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Compute centrality measures"""
        graph = await self.load_graph_from_janusgraph(graph_id)
        algorithm = self.algorithms["centrality"]
        return await algorithm.run(graph, params)
    
    async def compute_clustering_coefficient(self, graph_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Compute clustering coefficients"""
        graph = await self.load_graph_from_janusgraph(graph_id)
        algorithm = self.algorithms["clustering"]
        return await algorithm.run(graph, params)
    
    async def count_triangles(self, graph_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Count triangles in the graph"""
        graph = await self.load_graph_from_janusgraph(graph_id)
        
        # Use GraphFrames triangle counting
        results = graph.triangleCount()
        
        # Get statistics
        triangle_counts = results.select("count").rdd.map(lambda x: x[0]).collect()
        total_triangles = sum(triangle_counts) // 3  # Each triangle counted 3 times
        
        return {
            "algorithm": "triangle_count",
            "total_triangles": total_triangles,
            "vertices_with_triangles": len([c for c in triangle_counts if c > 0]),
            "max_triangles_per_vertex": max(triangle_counts) if triangle_counts else 0
        }
    
    async def find_connected_components(self, graph_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Find connected components"""
        graph = await self.load_graph_from_janusgraph(graph_id)
        
        # Run connected components
        results = graph.connectedComponents()
        
        # Get component statistics
        components = results.select("component").distinct().count()
        component_sizes = results.groupBy("component").count().collect()
        
        return {
            "algorithm": "connected_components",
            "num_components": components,
            "largest_component_size": max(c["count"] for c in component_sizes) if component_sizes else 0,
            "component_sizes": [
                {"component_id": c["component"], "size": c["count"]}
                for c in sorted(component_sizes, key=lambda x: x["count"], reverse=True)[:10]
            ]
        }
    
    async def compute_shortest_paths(self, graph_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Compute shortest paths"""
        graph = await self.load_graph_from_janusgraph(graph_id)
        algorithm = self.algorithms["shortest_path"]
        return await algorithm.run(graph, params)
    
    async def simulate_influence_propagation(self, graph_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Simulate influence propagation"""
        graph = await self.load_graph_from_janusgraph(graph_id)
        algorithm = self.algorithms["influence_propagation"]
        return await algorithm.run(graph, params)
    
    async def run_custom_algorithm(self, graph_id: str, algorithm_code: str,
                                 params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run custom algorithm provided as code
        
        Args:
            graph_id: Graph identifier
            algorithm_code: Python code defining the algorithm
            params: Algorithm parameters
            
        Returns:
            Algorithm results
        """
        graph = await self.load_graph_from_janusgraph(graph_id)
        
        # Create execution context
        context = {
            "graph": graph,
            "spark": self.spark,
            "params": params,
            "results": {}
        }
        
        # Execute custom algorithm
        exec(algorithm_code, context)
        
        return context.get("results", {})
    
    def clear_cache(self, graph_id: Optional[str] = None):
        """Clear graph cache"""
        if graph_id:
            self.graph_cache.pop(graph_id, None)
            self.cache_timestamps.pop(graph_id, None)
        else:
            self.graph_cache.clear()
            self.cache_timestamps.clear()
    
    async def health_check(self) -> Dict[str, Any]:
        """Check GraphX analytics health"""
        try:
            # Check Spark session
            if self.spark and self.spark._jsc:
                spark_status = "active"
            else:
                spark_status = "inactive"
            
            return {
                "healthy": spark_status == "active",
                "spark_status": spark_status,
                "cached_graphs": list(self.graph_cache.keys()),
                "available_algorithms": list(self.algorithms.keys())
            }
            
        except Exception as e:
            return {
                "healthy": False,
                "error": str(e)
            } 