"""
Community Detection implementation using Spark GraphFrames

This module implements community detection algorithms for finding
cohesive groups in the PlatformQ graph.
"""

import sys
import json
import logging
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, desc, count, collect_list, avg, max as spark_max, min as spark_min
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from graph_base import GraphFramesBase
import networkx as nx
from networkx.algorithms import community
import community as community_louvain

logger = logging.getLogger(__name__)


class CommunityDetectionAnalyzer(GraphFramesBase):
    """Community detection implementation for graph segmentation"""
    
    def run_analysis(self, vertices_df: DataFrame, edges_df: DataFrame, **kwargs) -> DataFrame:
        """
        Run community detection algorithms on the graph
        
        :param vertices_df: Vertices DataFrame
        :param edges_df: Edges DataFrame
        :param kwargs: Additional parameters (algorithm, resolution, max_iter)
        :return: DataFrame with community assignments
        """
        algorithm = kwargs.get('algorithm', 'label_propagation')
        
        if algorithm == 'label_propagation':
            return self._run_label_propagation(vertices_df, edges_df, **kwargs)
        elif algorithm == 'louvain':
            return self._run_louvain(vertices_df, edges_df, **kwargs)
        elif algorithm == 'connected_components':
            return self._run_connected_components(vertices_df, edges_df, **kwargs)
        else:
            raise ValueError(f"Unknown algorithm: {algorithm}")
    
    def _run_label_propagation(self, vertices_df: DataFrame, edges_df: DataFrame, **kwargs) -> DataFrame:
        """Run Label Propagation Algorithm for community detection"""
        max_iter = kwargs.get('max_iter', 5)
        
        # Create GraphFrame
        graph = self.create_graph_frame(vertices_df, edges_df)
        
        logger.info(f"Running Label Propagation Algorithm with max_iter={max_iter}")
        
        # Run LPA
        communities = graph.labelPropagation(maxIter=max_iter)
        
        # Extract community assignments
        community_df = communities.select(
            col("id"),
            col("label").alias("community_id")
        )
        
        # Calculate community statistics
        community_stats = community_df.groupBy("community_id").agg(
            count("*").alias("size"),
            collect_list("id").alias("members")
        )
        
        # Join with original vertex data
        results_df = community_df.join(
            vertices_df,
            community_df.id == vertices_df.id,
            "inner"
        ).drop(vertices_df.id)
        
        # Add community-level metrics
        results_df = results_df.join(
            community_stats,
            results_df.community_id == community_stats.community_id,
            "left"
        ).drop(community_stats.community_id)
        
        return results_df
    
    def _run_louvain(self, vertices_df: DataFrame, edges_df: DataFrame, **kwargs) -> DataFrame:
        """Run Louvain method for community detection"""
        resolution = kwargs.get('resolution', 1.0)
        
        # For Louvain, we need to collect the graph to driver (for small-medium graphs)
        # In production, use a distributed implementation
        logger.info(f"Running Louvain method with resolution={resolution}")
        
        # Collect vertices and edges
        vertices = vertices_df.select("id").collect()
        edges_data = edges_df.select("src", "dst").collect()
        
        # Create NetworkX graph
        G = nx.Graph()
        G.add_nodes_from([row.id for row in vertices])
        G.add_edges_from([(row.src, row.dst) for row in edges_data])
        
        # Run Louvain
        partition = community_louvain.best_partition(G, resolution=resolution)
        
        # Convert back to DataFrame
        community_data = [(node, comm) for node, comm in partition.items()]
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("community_id", IntegerType(), False)
        ])
        
        community_df = self.spark.createDataFrame(community_data, schema)
        
        # Calculate modularity
        modularity = community_louvain.modularity(partition, G)
        logger.info(f"Louvain modularity: {modularity}")
        
        # Join with original vertex data
        results_df = community_df.join(
            vertices_df,
            community_df.id == vertices_df.id,
            "inner"
        ).drop(vertices_df.id)
        
        # Add modularity as a column
        results_df = results_df.withColumn("modularity", col("community_id") * 0 + modularity)
        
        return results_df
    
    def _run_connected_components(self, vertices_df: DataFrame, edges_df: DataFrame, **kwargs) -> DataFrame:
        """Run connected components to find disconnected subgraphs"""
        
        # Create GraphFrame
        graph = self.create_graph_frame(vertices_df, edges_df)
        
        logger.info("Running Connected Components")
        
        # Run connected components
        components = graph.connectedComponents()
        
        # Extract component assignments
        component_df = components.select(
            col("id"),
            col("component").alias("component_id")
        )
        
        # Calculate component statistics
        component_stats = component_df.groupBy("component_id").agg(
            count("*").alias("size"),
            collect_list("id").alias("members")
        )
        
        # Find giant component
        giant_component = component_stats.orderBy(col("size").desc()).first()
        logger.info(f"Giant component size: {giant_component['size']}")
        
        # Join with original vertex data
        results_df = component_df.join(
            vertices_df,
            component_df.id == vertices_df.id,
            "inner"
        ).drop(vertices_df.id)
        
        # Add component-level metrics
        results_df = results_df.join(
            component_stats,
            results_df.component_id == component_stats.component_id,
            "left"
        ).drop(component_stats.component_id)
        
        # Mark if vertex is in giant component
        results_df = results_df.withColumn(
            "is_giant_component",
            col("component_id") == giant_component["component_id"]
        )
        
        return results_df
    
    def analyze_community_structure(self, vertices_df: DataFrame, edges_df: DataFrame, 
                                    communities_df: DataFrame) -> Dict[str, Any]:
        """
        Analyze the structure and quality of detected communities
        
        :param vertices_df: Original vertices
        :param edges_df: Original edges
        :param communities_df: DataFrame with community assignments
        :return: Dictionary with community metrics
        """
        # Internal edge ratio for each community
        internal_edges = edges_df.alias("e").join(
            communities_df.alias("c1"),
            col("e.src") == col("c1.id")
        ).join(
            communities_df.alias("c2"),
            col("e.dst") == col("c2.id")
        ).filter(
            col("c1.community_id") == col("c2.community_id")
        ).groupBy("c1.community_id").count()
        
        # Total edges per community
        total_edges = edges_df.alias("e").join(
            communities_df.alias("c"),
            (col("e.src") == col("c.id")) | (col("e.dst") == col("c.id"))
        ).groupBy("c.community_id").count()
        
        # Calculate metrics
        metrics = internal_edges.join(
            total_edges,
            internal_edges.community_id == total_edges.community_id
        ).select(
            internal_edges.community_id,
            (col("internal_edges.count") / col("total_edges.count")).alias("internal_ratio")
        ).collect()
        
        return {
            "community_metrics": [row.asDict() for row in metrics],
            "num_communities": communities_df.select("community_id").distinct().count(),
            "avg_community_size": communities_df.groupBy("community_id").count().agg(avg("count")).collect()[0][0]
        }


def main():
    """Main entry point for Spark job submission"""
    if len(sys.argv) < 3:
        print("Usage: spark-submit community_detection.py <tenant_id> <parameters_json> [output_table]")
        sys.exit(1)
    
    tenant_id = sys.argv[1]
    parameters = json.loads(sys.argv[2])
    output_table = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Create analyzer
    analyzer = CommunityDetectionAnalyzer(tenant_id)
    
    # Run analysis
    result = analyzer.execute(**parameters)
    
    # Save to output table if specified
    if output_table and result['status'] == 'success':
        # Save results to Cassandra
        spark = SparkSession.builder.getOrCreate()
        
        # Convert results to DataFrame
        results_df = spark.createDataFrame(result['top_results'])
        
        # Write to table
        results_df.write \
            .mode("overwrite") \
            .option("table", output_table) \
            .option("keyspace", f"tenant_{tenant_id}") \
            .format("org.apache.spark.sql.cassandra") \
            .save()
    
    # Output result as JSON
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main() 