"""
Fraud Detection using Spark GraphX

This module implements graph-based fraud detection algorithms for
identifying suspicious patterns in the PlatformQ trust network.
"""

import sys
import json
import logging
from typing import Dict, Any, List, Tuple, Optional, Set
from datetime import datetime, timedelta
from collections import defaultdict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, desc, count, avg, stddev, sum as spark_sum,
    collect_list, collect_set, when, lit, array_contains,
    max as spark_max, min as spark_min, abs as spark_abs
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType
from graph_base import GraphFramesBase
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = logging.getLogger(__name__)


class FraudDetectionAnalyzer(GraphFramesBase):
    """Graph-based fraud detection for trust networks"""
    
    def run_analysis(self, vertices_df: DataFrame, edges_df: DataFrame, **kwargs) -> DataFrame:
        """
        Run fraud detection analysis on the graph
        
        :param vertices_df: Vertices DataFrame
        :param edges_df: Edges DataFrame
        :param kwargs: Additional parameters
        :return: DataFrame with fraud scores and flags
        """
        # Run multiple detection algorithms
        sybil_scores = self._detect_sybil_attacks(vertices_df, edges_df, **kwargs)
        collusion_scores = self._detect_collusion_rings(vertices_df, edges_df, **kwargs)
        anomaly_scores = self._detect_anomalous_patterns(vertices_df, edges_df, **kwargs)
        
        # Combine scores
        results_df = vertices_df.join(sybil_scores, "id", "left") \
            .join(collusion_scores, "id", "left") \
            .join(anomaly_scores, "id", "left")
        
        # Calculate composite fraud score
        results_df = results_df.withColumn(
            "fraud_score",
            (col("sybil_score") * 0.3 + 
             col("collusion_score") * 0.3 + 
             col("anomaly_score") * 0.4)
        )
        
        # Flag high-risk entities
        threshold = kwargs.get('fraud_threshold', 0.7)
        results_df = results_df.withColumn(
            "is_suspicious",
            col("fraud_score") > threshold
        )
        
        return results_df
    
    def _detect_sybil_attacks(self, vertices_df: DataFrame, edges_df: DataFrame, **kwargs) -> DataFrame:
        """
        Detect Sybil attacks using graph structure analysis
        
        Sybil attacks involve creating multiple fake identities to gain
        disproportionate influence in the network.
        """
        # Create GraphFrame
        graph = self.create_graph_frame(vertices_df, edges_df)
        
        # Calculate degree metrics
        in_degrees = graph.inDegrees
        out_degrees = graph.outDegrees
        
        # Join degrees
        degrees_df = in_degrees.join(out_degrees, "id", "outer").fillna(0)
        degrees_df = degrees_df.withColumn(
            "total_degree",
            col("inDegree") + col("outDegree")
        )
        
        # Calculate clustering coefficient (simplified version)
        # In a real implementation, use GraphX's clustering coefficient
        triangles = graph.triangleCount()
        
        clustering_df = triangles.join(degrees_df, "id") \
            .withColumn(
                "clustering_coefficient",
                when(col("total_degree") > 1,
                     2.0 * col("count") / (col("total_degree") * (col("total_degree") - 1))
                ).otherwise(0)
            )
        
        # Detect suspiciously low clustering coefficient with high degree
        # (Sybil nodes often have star-like patterns)
        mean_clustering = clustering_df.agg(avg("clustering_coefficient")).collect()[0][0]
        std_clustering = clustering_df.agg(stddev("clustering_coefficient")).collect()[0][0]
        
        sybil_df = clustering_df.withColumn(
            "sybil_score",
            when(
                (col("total_degree") > 10) & 
                (col("clustering_coefficient") < mean_clustering - 2 * std_clustering),
                1.0
            ).when(
                (col("total_degree") > 5) & 
                (col("clustering_coefficient") < mean_clustering - std_clustering),
                0.7
            ).otherwise(0.0)
        )
        
        # Check for rapid edge creation (time-based if available)
        if "created_at" in edges_df.columns:
            rapid_edges = edges_df.groupBy("src").agg(
                count("*").alias("edge_count"),
                (spark_max("created_at") - spark_min("created_at")).alias("time_span")
            ).withColumn(
                "edges_per_day",
                col("edge_count") / (col("time_span") / 86400.0)  # Convert to days
            )
            
            # Flag accounts creating many edges quickly
            rapid_df = rapid_edges.withColumn(
                "rapid_growth_flag",
                col("edges_per_day") > 10  # More than 10 edges per day
            )
            
            sybil_df = sybil_df.join(rapid_df.select("src", "rapid_growth_flag"), 
                                      sybil_df.id == rapid_df.src, "left")
            sybil_df = sybil_df.withColumn(
                "sybil_score",
                when(col("rapid_growth_flag") == True, 
                     spark_min(col("sybil_score") + 0.3, lit(1.0))
                ).otherwise(col("sybil_score"))
            )
        
        return sybil_df.select("id", "sybil_score")
    
    def _detect_collusion_rings(self, vertices_df: DataFrame, edges_df: DataFrame, **kwargs) -> DataFrame:
        """
        Detect collusion rings using strongly connected components
        
        Collusion rings are groups of accounts that artificially boost
        each other's reputation through reciprocal interactions.
        """
        # Create GraphFrame
        graph = self.create_graph_frame(vertices_df, edges_df)
        
        # Find strongly connected components
        scc = graph.stronglyConnectedComponents(maxIter=10)
        
        # Calculate component sizes and reciprocity
        component_stats = scc.groupBy("component").agg(
            count("*").alias("size"),
            collect_set("id").alias("members")
        )
        
        # Filter out large components (likely legitimate)
        suspicious_components = component_stats.filter(
            (col("size") >= 3) & (col("size") <= 20)  # Small to medium groups
        )
        
        # For each suspicious component, calculate reciprocity
        reciprocity_scores = []
        
        for row in suspicious_components.collect():
            component_id = row["component"]
            members = row["members"]
            
            # Get edges within component
            internal_edges = edges_df.filter(
                (array_contains(lit(members), col("src"))) &
                (array_contains(lit(members), col("dst")))
            )
            
            # Calculate reciprocity (ratio of bidirectional edges)
            edge_pairs = internal_edges.select("src", "dst").collect()
            reciprocal_count = 0
            total_pairs = len(edge_pairs)
            
            edge_set = {(e.src, e.dst) for e in edge_pairs}
            for src, dst in edge_set:
                if (dst, src) in edge_set:
                    reciprocal_count += 1
            
            reciprocity = reciprocal_count / (total_pairs * 2) if total_pairs > 0 else 0
            
            # High reciprocity in small groups is suspicious
            if reciprocity > 0.7:
                for member in members:
                    reciprocity_scores.append((member, min(reciprocity, 1.0)))
        
        # Create DataFrame from scores
        if reciprocity_scores:
            schema = StructType([
                StructField("id", StringType(), False),
                StructField("collusion_score", DoubleType(), False)
            ])
            collusion_df = self.spark.createDataFrame(reciprocity_scores, schema)
        else:
            # No collusion detected
            collusion_df = vertices_df.select("id").withColumn("collusion_score", lit(0.0))
        
        return collusion_df
    
    def _detect_anomalous_patterns(self, vertices_df: DataFrame, edges_df: DataFrame, **kwargs) -> DataFrame:
        """
        Detect anomalous patterns using graph metrics and machine learning
        """
        # Create GraphFrame
        graph = self.create_graph_frame(vertices_df, edges_df)
        
        # Calculate various graph metrics
        pagerank = graph.pageRank(resetProbability=0.15, tol=0.01)
        in_degrees = graph.inDegrees
        out_degrees = graph.outDegrees
        
        # Combine metrics
        metrics_df = pagerank.vertices.select("id", col("pagerank")) \
            .join(in_degrees, "id", "left") \
            .join(out_degrees, "id", "left") \
            .fillna(0)
        
        # Calculate ratios and derived features
        metrics_df = metrics_df.withColumn(
            "in_out_ratio",
            when(col("outDegree") > 0, col("inDegree") / col("outDegree")).otherwise(0)
        ).withColumn(
            "pagerank_degree_ratio",
            when(col("inDegree") > 0, col("pagerank") / col("inDegree")).otherwise(0)
        )
        
        # Collect features for anomaly detection
        features_df = metrics_df.select(
            "id",
            "pagerank",
            "inDegree",
            "outDegree",
            "in_out_ratio",
            "pagerank_degree_ratio"
        )
        
        # Convert to numpy array for sklearn
        features_data = features_df.collect()
        ids = [row["id"] for row in features_data]
        
        feature_matrix = np.array([
            [row["pagerank"], row["inDegree"], row["outDegree"], 
             row["in_out_ratio"], row["pagerank_degree_ratio"]]
            for row in features_data
        ])
        
        # Normalize features
        scaler = StandardScaler()
        feature_matrix_scaled = scaler.fit_transform(feature_matrix)
        
        # Apply Isolation Forest for anomaly detection
        iso_forest = IsolationForest(contamination=0.1, random_state=42)
        anomaly_predictions = iso_forest.fit_predict(feature_matrix_scaled)
        anomaly_scores = iso_forest.score_samples(feature_matrix_scaled)
        
        # Normalize scores to 0-1 range
        min_score = np.min(anomaly_scores)
        max_score = np.max(anomaly_scores)
        normalized_scores = (anomaly_scores - min_score) / (max_score - min_score)
        normalized_scores = 1 - normalized_scores  # Invert so higher score = more anomalous
        
        # Create result DataFrame
        anomaly_data = [(ids[i], float(normalized_scores[i])) 
                        for i in range(len(ids))]
        
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("anomaly_score", DoubleType(), False)
        ])
        
        anomaly_df = self.spark.createDataFrame(anomaly_data, schema)
        
        return anomaly_df
    
    def analyze_fraud_patterns(self, results_df: DataFrame) -> Dict[str, Any]:
        """
        Analyze detected fraud patterns and generate insights
        """
        total_entities = results_df.count()
        suspicious_entities = results_df.filter(col("is_suspicious") == True).count()
        
        # Get top suspicious entities
        top_suspicious = results_df.filter(col("is_suspicious") == True) \
            .orderBy(col("fraud_score").desc()) \
            .limit(10) \
            .collect()
        
        # Analyze fraud type distribution
        fraud_types = results_df.agg(
            avg("sybil_score").alias("avg_sybil_score"),
            avg("collusion_score").alias("avg_collusion_score"),
            avg("anomaly_score").alias("avg_anomaly_score"),
            count(when(col("sybil_score") > 0.5, True)).alias("high_sybil_count"),
            count(when(col("collusion_score") > 0.5, True)).alias("high_collusion_count"),
            count(when(col("anomaly_score") > 0.5, True)).alias("high_anomaly_count")
        ).collect()[0]
        
        return {
            "total_entities": total_entities,
            "suspicious_entities": suspicious_entities,
            "fraud_rate": suspicious_entities / total_entities if total_entities > 0 else 0,
            "top_suspicious": [row.asDict() for row in top_suspicious],
            "fraud_type_analysis": {
                "sybil_attacks": {
                    "avg_score": fraud_types["avg_sybil_score"],
                    "high_risk_count": fraud_types["high_sybil_count"]
                },
                "collusion_rings": {
                    "avg_score": fraud_types["avg_collusion_score"],
                    "high_risk_count": fraud_types["high_collusion_count"]
                },
                "anomalous_behavior": {
                    "avg_score": fraud_types["avg_anomaly_score"],
                    "high_risk_count": fraud_types["high_anomaly_count"]
                }
            }
        }


def main():
    """Main entry point for Spark job submission"""
    if len(sys.argv) < 3:
        print("Usage: spark-submit fraud_detection.py <tenant_id> <parameters_json> [output_table]")
        sys.exit(1)
    
    tenant_id = sys.argv[1]
    parameters = json.loads(sys.argv[2])
    output_table = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Create analyzer
    analyzer = FraudDetectionAnalyzer(tenant_id)
    
    # Run analysis
    result = analyzer.execute(**parameters)
    
    # Analyze patterns if successful
    if result['status'] == 'success' and result.get('top_results'):
        # Convert top results back to DataFrame for analysis
        spark = SparkSession.builder.getOrCreate()
        results_df = spark.createDataFrame(result['top_results'])
        
        # Analyze fraud patterns
        pattern_analysis = analyzer.analyze_fraud_patterns(results_df)
        result['pattern_analysis'] = pattern_analysis
        
        # Save to output table if specified
        if output_table:
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