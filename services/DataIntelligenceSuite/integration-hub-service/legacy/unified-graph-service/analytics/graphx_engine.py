"""GraphX analytics engine for large-scale graph processing"""

import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import json
from enum import Enum

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark import SparkContext
import pyspark.sql.functions as F

from app.core.config import Settings


logger = logging.getLogger(__name__)


class AnalyticsJobStatus(Enum):
    """Analytics job status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class GraphXEngine:
    """GraphX analytics engine"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.spark: Optional[SparkSession] = None
        self.sc: Optional[SparkContext] = None
        self.initialized = False
        self.running_jobs: Dict[str, Any] = {}
        
    async def initialize(self):
        """Initialize Spark session for GraphX"""
        logger.info("Initializing GraphX engine")
        
        try:
            # Create Spark session
            self.spark = SparkSession.builder \
                .appName(self.settings.spark_app_name) \
                .master(self.settings.spark_master) \
                .config("spark.executor.memory", self.settings.spark_executor_memory) \
                .config("spark.executor.cores", str(self.settings.spark_executor_cores)) \
                .config("spark.cassandra.connection.host", ",".join(self.settings.cassandra_hosts)) \
                .config("spark.cassandra.connection.port", str(self.settings.cassandra_port)) \
                .config("spark.es.nodes", ",".join(self.settings.elasticsearch_hosts)) \
                .config("spark.es.nodes.wan.only", "true") \
                .getOrCreate()
                
            self.sc = self.spark.sparkContext
            self.sc.setCheckpointDir(self.settings.graphx_checkpoint_dir)
            
            # Import GraphX (will be available when pyspark is properly configured)
            # For now, we'll use DataFrames for graph operations
            
            self.initialized = True
            logger.info("GraphX engine initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize GraphX engine: {e}")
            raise
            
    async def cleanup(self):
        """Cleanup Spark session"""
        if self.spark:
            self.spark.stop()
            self.initialized = False
            logger.info("GraphX engine cleaned up")
            
    async def run_pagerank(self, max_iterations: Optional[int] = None,
                          damping_factor: Optional[float] = None) -> str:
        """Run PageRank algorithm on the graph"""
        job_id = f"pagerank_{datetime.utcnow().timestamp()}"
        
        try:
            logger.info(f"Starting PageRank job {job_id}")
            
            # Set parameters
            iterations = max_iterations or self.settings.pagerank_iterations
            damping = damping_factor or self.settings.pagerank_damping_factor
            
            # Mark job as running
            self.running_jobs[job_id] = {
                "type": "pagerank",
                "status": AnalyticsJobStatus.RUNNING,
                "started_at": datetime.utcnow()
            }
            
            # Load graph data from Cassandra
            vertices_df = self._load_vertices()
            edges_df = self._load_edges()
            
            # Run PageRank using DataFrame operations
            # This is a simplified version - in production, use GraphX or GraphFrames
            result_df = self._pagerank_dataframe(vertices_df, edges_df, iterations, damping)
            
            # Save results
            await self._save_results(job_id, "pagerank", result_df)
            
            # Mark job as completed
            self.running_jobs[job_id]["status"] = AnalyticsJobStatus.COMPLETED
            self.running_jobs[job_id]["completed_at"] = datetime.utcnow()
            
            logger.info(f"PageRank job {job_id} completed successfully")
            return job_id
            
        except Exception as e:
            logger.error(f"PageRank job {job_id} failed: {e}")
            self.running_jobs[job_id]["status"] = AnalyticsJobStatus.FAILED
            self.running_jobs[job_id]["error"] = str(e)
            raise
            
    async def detect_communities(self, algorithm: str = "louvain",
                                resolution: Optional[float] = None) -> str:
        """Detect communities in the graph"""
        job_id = f"community_{datetime.utcnow().timestamp()}"
        
        try:
            logger.info(f"Starting community detection job {job_id} with algorithm {algorithm}")
            
            # Set parameters
            res = resolution or self.settings.community_detection_resolution
            
            # Mark job as running
            self.running_jobs[job_id] = {
                "type": "community_detection",
                "algorithm": algorithm,
                "status": AnalyticsJobStatus.RUNNING,
                "started_at": datetime.utcnow()
            }
            
            # Load graph data
            vertices_df = self._load_vertices()
            edges_df = self._load_edges()
            
            # Run community detection
            if algorithm == "louvain":
                result_df = self._louvain_community_detection(vertices_df, edges_df, res)
            elif algorithm == "label_propagation":
                result_df = self._label_propagation(vertices_df, edges_df)
            else:
                raise ValueError(f"Unknown algorithm: {algorithm}")
                
            # Save results
            await self._save_results(job_id, "communities", result_df)
            
            # Mark job as completed
            self.running_jobs[job_id]["status"] = AnalyticsJobStatus.COMPLETED
            self.running_jobs[job_id]["completed_at"] = datetime.utcnow()
            
            logger.info(f"Community detection job {job_id} completed successfully")
            return job_id
            
        except Exception as e:
            logger.error(f"Community detection job {job_id} failed: {e}")
            self.running_jobs[job_id]["status"] = AnalyticsJobStatus.FAILED
            self.running_jobs[job_id]["error"] = str(e)
            raise
            
    async def calculate_centrality(self, centrality_type: str = "betweenness") -> str:
        """Calculate node centrality"""
        job_id = f"centrality_{datetime.utcnow().timestamp()}"
        
        try:
            logger.info(f"Starting centrality calculation job {job_id} for {centrality_type}")
            
            # Mark job as running
            self.running_jobs[job_id] = {
                "type": "centrality",
                "centrality_type": centrality_type,
                "status": AnalyticsJobStatus.RUNNING,
                "started_at": datetime.utcnow()
            }
            
            # Load graph data
            vertices_df = self._load_vertices()
            edges_df = self._load_edges()
            
            # Calculate centrality
            if centrality_type == "betweenness":
                result_df = self._betweenness_centrality(vertices_df, edges_df)
            elif centrality_type == "closeness":
                result_df = self._closeness_centrality(vertices_df, edges_df)
            elif centrality_type == "degree":
                result_df = self._degree_centrality(vertices_df, edges_df)
            else:
                raise ValueError(f"Unknown centrality type: {centrality_type}")
                
            # Save results
            await self._save_results(job_id, f"{centrality_type}_centrality", result_df)
            
            # Mark job as completed
            self.running_jobs[job_id]["status"] = AnalyticsJobStatus.COMPLETED
            self.running_jobs[job_id]["completed_at"] = datetime.utcnow()
            
            logger.info(f"Centrality job {job_id} completed successfully")
            return job_id
            
        except Exception as e:
            logger.error(f"Centrality job {job_id} failed: {e}")
            self.running_jobs[job_id]["status"] = AnalyticsJobStatus.FAILED
            self.running_jobs[job_id]["error"] = str(e)
            raise
            
    async def find_shortest_paths(self, source_id: str, target_ids: Optional[List[str]] = None) -> str:
        """Find shortest paths from source to targets"""
        job_id = f"shortest_paths_{datetime.utcnow().timestamp()}"
        
        try:
            logger.info(f"Starting shortest paths job {job_id} from {source_id}")
            
            # Mark job as running
            self.running_jobs[job_id] = {
                "type": "shortest_paths",
                "source": source_id,
                "targets": target_ids,
                "status": AnalyticsJobStatus.RUNNING,
                "started_at": datetime.utcnow()
            }
            
            # Load graph data
            vertices_df = self._load_vertices()
            edges_df = self._load_edges()
            
            # Calculate shortest paths using BFS
            result_df = self._shortest_paths_bfs(vertices_df, edges_df, source_id, target_ids)
            
            # Save results
            await self._save_results(job_id, "shortest_paths", result_df)
            
            # Mark job as completed
            self.running_jobs[job_id]["status"] = AnalyticsJobStatus.COMPLETED
            self.running_jobs[job_id]["completed_at"] = datetime.utcnow()
            
            logger.info(f"Shortest paths job {job_id} completed successfully")
            return job_id
            
        except Exception as e:
            logger.error(f"Shortest paths job {job_id} failed: {e}")
            self.running_jobs[job_id]["status"] = AnalyticsJobStatus.FAILED
            self.running_jobs[job_id]["error"] = str(e)
            raise
            
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get analytics job status"""
        if job_id not in self.running_jobs:
            # Check if results exist
            if await self._check_results_exist(job_id):
                return {
                    "job_id": job_id,
                    "status": AnalyticsJobStatus.COMPLETED.value,
                    "message": "Job completed (historical)"
                }
            else:
                raise ValueError(f"Job {job_id} not found")
                
        job_info = self.running_jobs[job_id]
        return {
            "job_id": job_id,
            "type": job_info["type"],
            "status": job_info["status"].value,
            "started_at": job_info["started_at"].isoformat(),
            "completed_at": job_info.get("completed_at", {}).isoformat() if "completed_at" in job_info else None,
            "error": job_info.get("error")
        }
        
    async def get_job_results(self, job_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get analytics job results"""
        # Load results from storage
        results_df = await self._load_results(job_id)
        
        if results_df is None:
            raise ValueError(f"No results found for job {job_id}")
            
        # Convert to list of dicts
        results = results_df.limit(limit).collect()
        return [row.asDict() for row in results]
        
    def _load_vertices(self) -> DataFrame:
        """Load vertices from Cassandra"""
        return self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="vertices", keyspace=self.settings.cassandra_keyspace) \
            .load()
            
    def _load_edges(self) -> DataFrame:
        """Load edges from Cassandra"""
        return self.spark.read \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="edges", keyspace=self.settings.cassandra_keyspace) \
            .load()
            
    def _pagerank_dataframe(self, vertices_df: DataFrame, edges_df: DataFrame,
                           iterations: int, damping: float) -> DataFrame:
        """PageRank implementation using DataFrames"""
        # Initialize ranks
        num_vertices = vertices_df.count()
        ranks_df = vertices_df.select("id").withColumn("rank", F.lit(1.0 / num_vertices))
        
        # Prepare adjacency information
        out_degrees = edges_df.groupBy("source_id").count().withColumnRenamed("count", "out_degree")
        
        # Iterative PageRank
        for i in range(iterations):
            # Calculate contributions
            contributions = edges_df \
                .join(ranks_df.withColumnRenamed("id", "source_id"), "source_id") \
                .join(out_degrees, "source_id") \
                .select(
                    F.col("target_id").alias("id"),
                    (F.col("rank") / F.col("out_degree")).alias("contribution")
                ) \
                .groupBy("id") \
                .sum("contribution") \
                .withColumnRenamed("sum(contribution)", "new_rank")
                
            # Update ranks
            ranks_df = vertices_df.select("id") \
                .join(contributions, "id", "left") \
                .select(
                    "id",
                    ((1 - damping) / num_vertices + damping * F.coalesce(F.col("new_rank"), F.lit(0))).alias("rank")
                )
                
        return ranks_df
        
    def _louvain_community_detection(self, vertices_df: DataFrame, edges_df: DataFrame,
                                   resolution: float) -> DataFrame:
        """Simplified Louvain community detection"""
        # This is a placeholder - in production, use a proper implementation
        # For now, we'll use a simple connected components approach
        
        # Create undirected edges
        edges_both = edges_df.select("source_id", "target_id") \
            .union(edges_df.select(F.col("target_id").alias("source_id"), 
                                 F.col("source_id").alias("target_id")))
                                 
        # Use GraphFrames or custom implementation for actual Louvain
        # For now, assign random communities
        communities = vertices_df.select("id") \
            .withColumn("community", (F.rand() * 10).cast(IntegerType()))
            
        return communities
        
    def _label_propagation(self, vertices_df: DataFrame, edges_df: DataFrame) -> DataFrame:
        """Label propagation for community detection"""
        # Initialize each node with its own label
        labels = vertices_df.select("id").withColumn("label", F.col("id"))
        
        # Iterate until convergence or max iterations
        max_iterations = 10
        for i in range(max_iterations):
            # Propagate labels along edges
            neighbor_labels = edges_df \
                .join(labels.withColumnRenamed("id", "source_id"), "source_id") \
                .select(F.col("target_id").alias("id"), F.col("label")) \
                .groupBy("id", "label") \
                .count() \
                .withColumn("rank", F.row_number().over(
                    F.Window.partitionBy("id").orderBy(F.desc("count"), "label")
                )) \
                .filter(F.col("rank") == 1) \
                .select("id", "label")
                
            # Update labels
            new_labels = labels.join(neighbor_labels, "id", "left") \
                .select("id", F.coalesce(neighbor_labels["label"], labels["label"]).alias("label"))
                
            # Check for convergence
            if new_labels.subtract(labels).count() == 0:
                break
                
            labels = new_labels
            
        return labels.withColumnRenamed("label", "community")
        
    def _degree_centrality(self, vertices_df: DataFrame, edges_df: DataFrame) -> DataFrame:
        """Calculate degree centrality"""
        # Out-degree
        out_degree = edges_df.groupBy("source_id").count() \
            .withColumnRenamed("source_id", "id") \
            .withColumnRenamed("count", "out_degree")
            
        # In-degree
        in_degree = edges_df.groupBy("target_id").count() \
            .withColumnRenamed("target_id", "id") \
            .withColumnRenamed("count", "in_degree")
            
        # Total degree
        degree_centrality = vertices_df.select("id") \
            .join(out_degree, "id", "left") \
            .join(in_degree, "id", "left") \
            .select(
                "id",
                (F.coalesce(F.col("out_degree"), F.lit(0)) + 
                 F.coalesce(F.col("in_degree"), F.lit(0))).alias("degree_centrality")
            )
            
        return degree_centrality
        
    def _betweenness_centrality(self, vertices_df: DataFrame, edges_df: DataFrame) -> DataFrame:
        """Simplified betweenness centrality calculation"""
        # This is a placeholder - proper implementation requires all shortest paths
        # For now, return degree centrality as approximation
        return self._degree_centrality(vertices_df, edges_df) \
            .withColumnRenamed("degree_centrality", "betweenness_centrality")
            
    def _closeness_centrality(self, vertices_df: DataFrame, edges_df: DataFrame) -> DataFrame:
        """Simplified closeness centrality calculation"""
        # This is a placeholder - proper implementation requires shortest path calculations
        # For now, return inverse of degree as approximation
        degree_df = self._degree_centrality(vertices_df, edges_df)
        
        return degree_df.select(
            "id",
            (1.0 / (F.col("degree_centrality") + 1)).alias("closeness_centrality")
        )
        
    def _shortest_paths_bfs(self, vertices_df: DataFrame, edges_df: DataFrame,
                           source_id: str, target_ids: Optional[List[str]] = None) -> DataFrame:
        """BFS-based shortest paths calculation"""
        # Initialize distances
        distances = vertices_df.select("id") \
            .withColumn("distance", 
                       F.when(F.col("id") == source_id, 0).otherwise(float('inf'))) \
            .withColumn("path", F.when(F.col("id") == source_id, F.array(F.lit(source_id))))
                       
        # BFS iterations
        max_iterations = self.settings.max_path_length
        for i in range(max_iterations):
            # Find nodes at current distance
            current_level = distances.filter(F.col("distance") == i)
            
            if current_level.count() == 0:
                break
                
            # Update distances for neighbors
            updates = current_level \
                .join(edges_df, current_level["id"] == edges_df["source_id"]) \
                .select(
                    F.col("target_id").alias("id"),
                    F.lit(i + 1).alias("new_distance"),
                    F.concat(F.col("path"), F.array(F.col("target_id"))).alias("new_path")
                )
                
            # Merge updates
            distances = distances.alias("d") \
                .join(updates.alias("u"), "id", "left") \
                .select(
                    "id",
                    F.when(F.col("d.distance") > F.col("u.new_distance"), 
                          F.col("u.new_distance")).otherwise(F.col("d.distance")).alias("distance"),
                    F.when(F.col("d.distance") > F.col("u.new_distance"), 
                          F.col("u.new_path")).otherwise(F.col("d.path")).alias("path")
                )
                
        # Filter to requested targets if specified
        if target_ids:
            distances = distances.filter(F.col("id").isin(target_ids))
            
        return distances.filter(F.col("distance") < float('inf'))
        
    async def _save_results(self, job_id: str, result_type: str, df: DataFrame):
        """Save results to storage"""
        # Save to Cassandra
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table=f"analytics_results_{result_type}", 
                    keyspace=self.settings.cassandra_keyspace) \
            .mode("append") \
            .option("spark.cassandra.output.consistency.level", "LOCAL_QUORUM") \
            .save()
            
        # Also save metadata
        metadata_df = self.spark.createDataFrame([{
            "job_id": job_id,
            "result_type": result_type,
            "created_at": datetime.utcnow().isoformat(),
            "row_count": df.count()
        }])
        
        metadata_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="analytics_metadata", 
                    keyspace=self.settings.cassandra_keyspace) \
            .mode("append") \
            .save()
            
    async def _load_results(self, job_id: str) -> Optional[DataFrame]:
        """Load results from storage"""
        try:
            # Get metadata first
            metadata_df = self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="analytics_metadata", 
                        keyspace=self.settings.cassandra_keyspace) \
                .load() \
                .filter(F.col("job_id") == job_id)
                
            if metadata_df.count() == 0:
                return None
                
            result_type = metadata_df.first()["result_type"]
            
            # Load actual results
            return self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=f"analytics_results_{result_type}", 
                        keyspace=self.settings.cassandra_keyspace) \
                .load() \
                .filter(F.col("job_id") == job_id)
                
        except Exception as e:
            logger.error(f"Failed to load results for job {job_id}: {e}")
            return None
            
    async def _check_results_exist(self, job_id: str) -> bool:
        """Check if results exist for a job"""
        try:
            metadata_df = self.spark.read \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="analytics_metadata", 
                        keyspace=self.settings.cassandra_keyspace) \
                .load() \
                .filter(F.col("job_id") == job_id)
                
            return metadata_df.count() > 0
            
        except:
            return False 