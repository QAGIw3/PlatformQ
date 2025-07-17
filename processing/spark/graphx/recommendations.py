"""
Recommendation Engine using Spark GraphX

This module implements graph-based recommendation algorithms for
generating personalized recommendations in PlatformQ.
"""

import sys
import json
import logging
from typing import Dict, Any, List, Tuple, Optional, Set
from datetime import datetime
from collections import defaultdict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, desc, count, avg, sum as spark_sum, collect_list, 
    collect_set, when, lit, array_contains, explode, size,
    max as spark_max, min as spark_min
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer, IndexToString
from graph_base import GraphFramesBase
from pyignite import Client

logger = logging.getLogger(__name__)


class RecommendationEngine(GraphFramesBase):
    """Graph-based recommendation engine"""
    
    def run_analysis(self, vertices_df: DataFrame, edges_df: DataFrame, **kwargs) -> DataFrame:
        """
        Generate recommendations using multiple algorithms
        
        :param vertices_df: Vertices DataFrame
        :param edges_df: Edges DataFrame
        :param kwargs: Additional parameters (user_id, recommendation_type, limit)
        :return: DataFrame with recommendations
        """
        user_id = kwargs.get('user_id')
        rec_type = kwargs.get('recommendation_type', 'general')
        limit = kwargs.get('limit', 10)
        
        # Run appropriate recommendation algorithm
        if rec_type == 'similar_assets':
            return self._recommend_similar_assets(vertices_df, edges_df, user_id, limit)
        elif rec_type == 'collaborative':
            return self._collaborative_filtering(vertices_df, edges_df, user_id, limit)
        elif rec_type == 'graph_based':
            return self._graph_based_recommendations(vertices_df, edges_df, user_id, limit)
        else:
            # Hybrid approach combining multiple methods
            return self._hybrid_recommendations(vertices_df, edges_df, user_id, limit)
    
    def _recommend_similar_assets(self, vertices_df: DataFrame, edges_df: DataFrame, 
                                  user_id: str, limit: int) -> DataFrame:
        """
        Recommend assets similar to what the user has interacted with
        """
        # Get user's recent interactions
        user_items = edges_df.filter(
            (col("src") == user_id) & 
            (col("label").isin(["CREATED", "LIKED", "VIEWED", "PURCHASED"]))
        ).select("dst").distinct()
        
        if user_items.count() == 0:
            # Cold start - return popular items
            return self._get_popular_items(vertices_df, edges_df, limit)
        
        # Find similar items through shared properties or connections
        user_item_list = [row.dst for row in user_items.collect()]
        
        # Get items with similar tags/categories
        similar_items = edges_df.filter(
            (col("label") == "SIMILAR_TO") & 
            (col("src").isin(user_item_list))
        ).select(col("dst").alias("item_id"))
        
        # Also find items through collaborative patterns
        collaborative_items = edges_df.alias("e1").join(
            edges_df.alias("e2"),
            (col("e1.dst").isin(user_item_list)) & 
            (col("e1.src") != user_id) &
            (col("e1.src") == col("e2.src")) &
            (col("e2.label").isin(["CREATED", "LIKED", "PURCHASED"]))
        ).select(col("e2.dst").alias("item_id")).distinct()
        
        # Combine and score recommendations
        all_recommendations = similar_items.union(collaborative_items)
        
        # Count occurrences as relevance score
        scored_recommendations = all_recommendations.groupBy("item_id").agg(
            count("*").alias("score")
        ).filter(
            ~col("item_id").isin(user_item_list)  # Exclude already interacted items
        )
        
        # Join with item metadata
        final_recommendations = scored_recommendations.join(
            vertices_df.filter(col("label") == "Asset"),
            scored_recommendations.item_id == vertices_df.id
        ).select(
            col("item_id"),
            col("name").alias("item_name"),
            col("score"),
            lit("content_similarity").alias("reason")
        ).orderBy(col("score").desc()).limit(limit)
        
        return final_recommendations
    
    def _collaborative_filtering(self, vertices_df: DataFrame, edges_df: DataFrame,
                                 user_id: str, limit: int) -> DataFrame:
        """
        Use collaborative filtering to find recommendations
        """
        # Prepare data for ALS
        # Create user-item interaction matrix
        interactions = edges_df.filter(
            col("label").isin(["LIKED", "PURCHASED", "RATED"])
        ).select(
            col("src").alias("user"),
            col("dst").alias("item"),
            when(col("properties.rating").isNotNull(), col("properties.rating"))
            .otherwise(1.0).alias("rating")
        )
        
        # Index users and items
        user_indexer = StringIndexer(inputCol="user", outputCol="user_id")
        item_indexer = StringIndexer(inputCol="item", outputCol="item_id")
        
        indexed_interactions = user_indexer.fit(interactions).transform(interactions)
        indexed_interactions = item_indexer.fit(indexed_interactions).transform(indexed_interactions)
        
        # Train ALS model
        als = ALS(
            maxIter=10,
            regParam=0.1,
            userCol="user_id",
            itemCol="item_id",
            ratingCol="rating",
            coldStartStrategy="drop",
            nonnegative=True
        )
        
        model = als.fit(indexed_interactions)
        
        # Get user index
        user_df = indexed_interactions.filter(col("user") == user_id).select("user_id").distinct()
        
        if user_df.count() == 0:
            # User not found - return popular items
            return self._get_popular_items(vertices_df, edges_df, limit)
        
        user_index = user_df.collect()[0].user_id
        
        # Generate recommendations for the user
        user_recs = model.recommendForUserSubset(
            self.spark.createDataFrame([(user_index,)], ["user_id"]), 
            limit * 2  # Get more to filter out already interacted
        )
        
        # Convert back to original IDs
        recommendations = user_recs.select(explode("recommendations").alias("rec"))
        recommendations = recommendations.select(
            col("rec.item_id"),
            col("rec.rating").alias("score")
        )
        
        # Map back to original item IDs
        item_id_map = indexed_interactions.select("item", "item_id").distinct()
        recommendations = recommendations.join(
            item_id_map,
            recommendations.item_id == item_id_map.item_id
        ).select(
            col("item").alias("item_id"),
            col("score")
        )
        
        # Filter out already interacted items
        user_items = interactions.filter(col("user") == user_id).select("item").distinct()
        user_item_list = [row.item for row in user_items.collect()]
        
        recommendations = recommendations.filter(
            ~col("item_id").isin(user_item_list)
        )
        
        # Join with item metadata
        final_recommendations = recommendations.join(
            vertices_df,
            recommendations.item_id == vertices_df.id
        ).select(
            col("item_id"),
            col("name").alias("item_name"),
            col("score"),
            lit("collaborative_filtering").alias("reason")
        ).orderBy(col("score").desc()).limit(limit)
        
        return final_recommendations
    
    def _graph_based_recommendations(self, vertices_df: DataFrame, edges_df: DataFrame,
                                     user_id: str, limit: int) -> DataFrame:
        """
        Use graph algorithms for recommendations
        """
        # Create GraphFrame
        graph = self.create_graph_frame(vertices_df, edges_df)
        
        # Run PersonalizedPageRank from the user
        # This finds nodes that are important relative to the user
        ppagerank = graph.pageRank(
            resetProbability=0.15,
            sourceId=user_id,
            maxIter=10
        )
        
        # Get top ranked items (excluding users)
        recommendations = ppagerank.vertices.filter(
            (col("label") == "Asset") & 
            (col("id") != user_id)
        ).select(
            col("id").alias("item_id"),
            col("pagerank").alias("score")
        )
        
        # Filter out items user already interacted with
        user_items = edges_df.filter(
            (col("src") == user_id) & 
            (col("label").isin(["CREATED", "LIKED", "VIEWED", "PURCHASED"]))
        ).select("dst").distinct()
        user_item_list = [row.dst for row in user_items.collect()]
        
        recommendations = recommendations.filter(
            ~col("item_id").isin(user_item_list)
        )
        
        # Join with metadata
        final_recommendations = recommendations.join(
            vertices_df,
            recommendations.item_id == vertices_df.id
        ).select(
            col("item_id"),
            col("name").alias("item_name"),
            col("score"),
            lit("graph_importance").alias("reason")
        ).orderBy(col("score").desc()).limit(limit)
        
        return final_recommendations
    
    def _hybrid_recommendations(self, vertices_df: DataFrame, edges_df: DataFrame,
                                user_id: str, limit: int) -> DataFrame:
        """
        Combine multiple recommendation approaches
        """
        # Get recommendations from different methods
        content_recs = self._recommend_similar_assets(
            vertices_df, edges_df, user_id, limit
        ).withColumn("method_weight", lit(0.3))
        
        collab_recs = self._collaborative_filtering(
            vertices_df, edges_df, user_id, limit
        ).withColumn("method_weight", lit(0.4))
        
        graph_recs = self._graph_based_recommendations(
            vertices_df, edges_df, user_id, limit
        ).withColumn("method_weight", lit(0.3))
        
        # Combine all recommendations
        all_recs = content_recs.union(collab_recs).union(graph_recs)
        
        # Aggregate scores with weights
        hybrid_scores = all_recs.groupBy("item_id", "item_name").agg(
            spark_sum(col("score") * col("method_weight")).alias("hybrid_score"),
            collect_list("reason").alias("reasons")
        )
        
        # Final recommendations
        final_recommendations = hybrid_scores.select(
            col("item_id"),
            col("item_name"),
            col("hybrid_score").alias("score"),
            col("reasons")
        ).orderBy(col("hybrid_score").desc()).limit(limit)
        
        # Cache results in Ignite
        self._cache_recommendations(user_id, final_recommendations)
        
        return final_recommendations
    
    def _get_popular_items(self, vertices_df: DataFrame, edges_df: DataFrame, 
                           limit: int) -> DataFrame:
        """
        Get popular items for cold start scenarios
        """
        # Count interactions per item
        popular_items = edges_df.filter(
            col("label").isin(["LIKED", "PURCHASED", "VIEWED"])
        ).groupBy("dst").agg(
            count("*").alias("popularity_score")
        )
        
        # Join with item metadata
        recommendations = popular_items.join(
            vertices_df.filter(col("label") == "Asset"),
            popular_items.dst == vertices_df.id
        ).select(
            col("dst").alias("item_id"),
            col("name").alias("item_name"),
            col("popularity_score").alias("score"),
            lit("popular").alias("reason")
        ).orderBy(col("popularity_score").desc()).limit(limit)
        
        return recommendations
    
    def _cache_recommendations(self, user_id: str, recommendations_df: DataFrame):
        """
        Cache recommendations in Ignite for fast retrieval
        """
        try:
            # Convert to list of dicts
            recs_list = [
                {
                    "item_id": row.item_id,
                    "item_name": row.item_name,
                    "score": float(row.score),
                    "reasons": row.reasons if hasattr(row, 'reasons') else [row.reason]
                }
                for row in recommendations_df.collect()
            ]
            
            # Connect to Ignite
            ignite_client = Client()
            ignite_nodes = [
                ('ignite-0.ignite', 10800),
                ('ignite-1.ignite', 10800),
                ('ignite-2.ignite', 10800)
            ]
            ignite_client.connect(ignite_nodes)
            
            # Cache with TTL
            cache_key = f"recommendations:{user_id}"
            cache_value = {
                "recommendations": recs_list,
                "generated_at": datetime.utcnow().isoformat(),
                "algorithm": "hybrid"
            }
            
            ignite_client.put(cache_key, cache_value)
            ignite_client.close()
            
            logger.info(f"Cached {len(recs_list)} recommendations for user {user_id}")
            
        except Exception as e:
            logger.error(f"Failed to cache recommendations: {e}")
    
    def analyze_recommendation_quality(self, vertices_df: DataFrame, edges_df: DataFrame,
                                       test_users: List[str]) -> Dict[str, Any]:
        """
        Analyze the quality of recommendations
        """
        # For each test user, generate recommendations and check against actual interactions
        results = []
        
        for user_id in test_users[:10]:  # Limit to 10 users for performance
            # Get recommendations
            recs = self._hybrid_recommendations(vertices_df, edges_df, user_id, 20)
            rec_items = [row.item_id for row in recs.collect()]
            
            # Get actual future interactions (would need timestamp filtering in production)
            actual_items = edges_df.filter(
                (col("src") == user_id) & 
                (col("label").isin(["LIKED", "PURCHASED"]))
            ).select("dst").distinct()
            actual_list = [row.dst for row in actual_items.collect()]
            
            # Calculate metrics
            if len(actual_list) > 0 and len(rec_items) > 0:
                precision = len(set(rec_items) & set(actual_list)) / len(rec_items)
                recall = len(set(rec_items) & set(actual_list)) / len(actual_list)
                
                results.append({
                    "user_id": user_id,
                    "precision": precision,
                    "recall": recall,
                    "f1_score": 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
                })
        
        # Aggregate results
        if results:
            avg_precision = sum(r["precision"] for r in results) / len(results)
            avg_recall = sum(r["recall"] for r in results) / len(results)
            avg_f1 = sum(r["f1_score"] for r in results) / len(results)
            
            return {
                "num_users_evaluated": len(results),
                "avg_precision": avg_precision,
                "avg_recall": avg_recall,
                "avg_f1_score": avg_f1,
                "individual_results": results
            }
        else:
            return {"error": "No results to evaluate"}


def main():
    """Main entry point for Spark job submission"""
    if len(sys.argv) < 3:
        print("Usage: spark-submit recommendations.py <tenant_id> <parameters_json> [output_table]")
        sys.exit(1)
    
    tenant_id = sys.argv[1]
    parameters = json.loads(sys.argv[2])
    output_table = sys.argv[3] if len(sys.argv) > 3 else None
    
    # Create recommendation engine
    engine = RecommendationEngine(tenant_id)
    
    # Run analysis
    result = engine.execute(**parameters)
    
    # Save to output table if specified
    if output_table and result['status'] == 'success':
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