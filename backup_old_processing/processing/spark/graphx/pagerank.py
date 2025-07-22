"""
PageRank implementation using Spark GraphFrames

This module implements PageRank algorithm for finding influential
nodes in the PlatformQ graph.
"""

import sys
import json
import logging
from typing import Dict, Any, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, desc, sum as spark_sum
from graph_base import GraphFramesBase

logger = logging.getLogger(__name__)


class PageRankAnalyzer(GraphFramesBase):
    """PageRank implementation for influence analysis"""
    
    def run_analysis(self, vertices_df: DataFrame, edges_df: DataFrame, **kwargs) -> DataFrame:
        """
        Run PageRank algorithm on the graph
        
        :param vertices_df: Vertices DataFrame
        :param edges_df: Edges DataFrame
        :param kwargs: Additional parameters (tolerance, max_iter, reset_probability)
        :return: DataFrame with PageRank scores
        """
        # Get parameters
        tolerance = kwargs.get('tolerance', 0.001)
        max_iter = kwargs.get('max_iter', 20)
        reset_probability = kwargs.get('reset_probability', 0.15)
        
        # Create GraphFrame
        graph = self.create_graph_frame(vertices_df, edges_df)
        
        # Run PageRank
        logger.info(f"Running PageRank with tolerance={tolerance}, max_iter={max_iter}")
        
        results = graph.pageRank(
            resetProbability=reset_probability,
            tol=tolerance,
            maxIter=max_iter
        )
        
        # Extract vertex PageRank scores
        pagerank_df = results.vertices.select(
            col("id"),
            col("pagerank").alias("pagerank_score")
        )
        
        # Join with original vertex data for context
        results_df = pagerank_df.join(
            vertices_df,
            pagerank_df.id == vertices_df.id,
            "inner"
        ).drop(vertices_df.id)
        
        # Add normalized scores (0-100 scale)
        max_score = results_df.agg({"pagerank_score": "max"}).collect()[0][0]
        results_df = results_df.withColumn(
            "normalized_score",
            (col("pagerank_score") / max_score) * 100
        )
        
        return results_df
    
    def find_influencers_by_type(self, vertices_df: DataFrame, edges_df: DataFrame, 
                                 node_type: str, **kwargs) -> DataFrame:
        """
        Find top influencers of a specific node type
        
        :param vertices_df: Vertices DataFrame
        :param edges_df: Edges DataFrame
        :param node_type: Type of node to analyze (e.g., 'user', 'project', 'asset')
        :return: Top influencers DataFrame
        """
        # Filter vertices by type
        filtered_vertices = vertices_df.filter(col("label") == node_type)
        
        # Run PageRank on the full graph
        results_df = self.run_analysis(vertices_df, edges_df, **kwargs)
        
        # Filter results for the specific node type
        typed_results = results_df.filter(col("label") == node_type)
        
        return typed_results
    
    def analyze_influence_propagation(self, vertices_df: DataFrame, edges_df: DataFrame,
                                      source_nodes: List[str], **kwargs) -> DataFrame:
        """
        Analyze how influence propagates from specific source nodes
        
        :param vertices_df: Vertices DataFrame
        :param edges_df: Edges DataFrame
        :param source_nodes: List of source node IDs
        :return: Influence propagation results
        """
        # Create personalized PageRank with source nodes
        graph = self.create_graph_frame(vertices_df, edges_df)
        
        # Set personalization vector (higher weight for source nodes)
        personalization = {node: 1.0 for node in source_nodes}
        
        # Run personalized PageRank
        results = graph.pageRank(
            resetProbability=kwargs.get('reset_probability', 0.15),
            sourceId=source_nodes,
            maxIter=kwargs.get('max_iter', 20)
        )
        
        # Extract and process results
        influence_df = results.vertices.select(
            col("id"),
            col("pagerank").alias("influence_score")
        )
        
        # Calculate distance from source nodes
        # This would require additional graph traversal
        
        return influence_df


def main():
    """Main entry point for Spark job submission"""
    if len(sys.argv) < 3:
        print("Usage: spark-submit pagerank.py <tenant_id> <parameters_json> [output_table]")
        sys.exit(1)
    
    tenant_id = sys.argv[1]
    parameters = json.loads(sys.argv[2])
    output_table = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Create analyzer
    analyzer = PageRankAnalyzer(tenant_id)
    
    # Run analysis
    result = analyzer.execute(**parameters)
    
    # Save to output table if specified
    if output_table and result['status'] == 'success':
        # Save top results to Cassandra or other storage
        from pyspark.sql import SparkSession
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