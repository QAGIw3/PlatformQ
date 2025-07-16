"""
Spark/GraphX Integration for Graph Intelligence Service

This module provides integration between the Graph Intelligence Service
and Apache Spark GraphX for large-scale graph analytics.
"""

import os
import json
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import asyncio
from concurrent.futures import ThreadPoolExecutor

import httpx
from pyspark.sql import SparkSession
import py4j

logger = logging.getLogger(__name__)


class SparkGraphAnalytics:
    """Integration layer for Spark-based graph analytics"""
    
    def __init__(self, spark_master_url: Optional[str] = None):
        """
        Initialize Spark integration
        
        :param spark_master_url: Spark master URL (default from env)
        """
        self.spark_master_url = spark_master_url or os.getenv(
            'SPARK_MASTER_URL', 
            'spark://spark_master:7077'
        )
        self.spark_submit_url = os.getenv(
            'SPARK_SUBMIT_URL',
            'http://spark_master:6066/v1/submissions'
        )
        self._executor = ThreadPoolExecutor(max_workers=4)
    
    async def submit_graph_job(self, 
                              algorithm: str,
                              tenant_id: str,
                              parameters: Dict[str, Any]) -> str:
        """
        Submit a graph analytics job to Spark
        
        :param algorithm: Algorithm to run (pagerank, connected_components, etc.)
        :param tenant_id: Tenant ID for multi-tenant isolation
        :param parameters: Algorithm-specific parameters
        :return: Spark job ID
        """
        # Map algorithm to Spark application
        algorithm_map = {
            'pagerank': '/opt/spark-jobs/graphx/pagerank.py',
            'connected_components': '/opt/spark-jobs/graphx/connected_components.py',
            'triangle_count': '/opt/spark-jobs/graphx/triangle_count.py',
            'community_detection': '/opt/spark-jobs/graphx/community_detection.py',
            'shortest_paths': '/opt/spark-jobs/graphx/shortest_paths.py',
            'label_propagation': '/opt/spark-jobs/graphx/label_propagation.py'
        }
        
        if algorithm not in algorithm_map:
            raise ValueError(f"Unknown algorithm: {algorithm}")
        
        application = algorithm_map[algorithm]
        
        # Prepare Spark job submission
        submission = {
            "action": "CreateSubmissionRequest",
            "appArgs": [
                tenant_id,
                json.dumps(parameters)
            ],
            "appResource": f"file://{application}",
            "clientSparkVersion": "3.5.0",
            "environmentVariables": {
                "TENANT_ID": tenant_id
            },
            "mainClass": "org.apache.spark.deploy.PythonRunner",
            "sparkProperties": {
                "spark.app.name": f"GraphX-{algorithm}-{tenant_id}",
                "spark.master": self.spark_master_url,
                "spark.submit.deployMode": "cluster",
                "spark.executor.memory": parameters.get("executor_memory", "4g"),
                "spark.executor.cores": str(parameters.get("executor_cores", 2)),
                "spark.executor.instances": str(parameters.get("executor_instances", 4)),
                # GraphX specific
                "spark.graphx.pregel.checkpointInterval": "10",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryo.registrator": "org.apache.spark.graphx.GraphKryoRegistrator",
                # Multi-tenant isolation
                "spark.platformq.tenant.id": tenant_id
            }
        }
        
        # Submit job via REST API
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.spark_submit_url + "/create",
                json=submission,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to submit Spark job: {response.text}")
            
            result = response.json()
            return result["submissionId"]
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get the status of a Spark job
        
        :param job_id: Spark job ID
        :return: Job status information
        """
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self.spark_submit_url}/status/{job_id}"
            )
            
            if response.status_code != 200:
                raise Exception(f"Failed to get job status: {response.text}")
            
            return response.json()
    
    async def get_job_result(self, job_id: str) -> Dict[str, Any]:
        """
        Get the result of a completed Spark job
        
        :param job_id: Spark job ID
        :return: Job results
        """
        # Poll for job completion
        max_attempts = 60  # 5 minutes with 5-second intervals
        for _ in range(max_attempts):
            status = await self.get_job_status(job_id)
            
            if status["driverState"] == "FINISHED":
                # TODO: Retrieve actual results from storage
                # Results would typically be saved to Cassandra/S3
                return {
                    "status": "completed",
                    "job_id": job_id,
                    "message": "Results saved to storage"
                }
            elif status["driverState"] in ["FAILED", "KILLED", "ERROR"]:
                return {
                    "status": "failed",
                    "job_id": job_id,
                    "error": status.get("message", "Job failed")
                }
            
            await asyncio.sleep(5)
        
        return {
            "status": "timeout",
            "job_id": job_id,
            "error": "Job did not complete within timeout"
        }
    
    async def run_pagerank(self, 
                          tenant_id: str,
                          vertex_label: Optional[str] = None,
                          edge_label: Optional[str] = None,
                          tolerance: float = 0.001,
                          max_iterations: int = 20,
                          top_k: int = 100) -> Dict[str, Any]:
        """
        Run PageRank algorithm on the graph
        
        :param tenant_id: Tenant ID
        :param vertex_label: Filter vertices by label
        :param edge_label: Filter edges by label
        :param tolerance: Convergence tolerance
        :param max_iterations: Maximum iterations
        :param top_k: Number of top results to return
        :return: PageRank results
        """
        parameters = {
            "vertex_label": vertex_label,
            "edge_label": edge_label,
            "tolerance": tolerance,
            "max_iter": max_iterations,
            "top_k": top_k,
            "save_to_graph": True,
            "result_property": "pagerank_score"
        }
        
        job_id = await self.submit_graph_job("pagerank", tenant_id, parameters)
        
        return {
            "job_id": job_id,
            "status": "submitted",
            "algorithm": "pagerank",
            "parameters": parameters
        }
    
    async def detect_communities(self,
                                tenant_id: str,
                                algorithm: str = "label_propagation",
                                max_iterations: int = 10) -> Dict[str, Any]:
        """
        Detect communities in the graph
        
        :param tenant_id: Tenant ID
        :param algorithm: Community detection algorithm
        :param max_iterations: Maximum iterations
        :return: Community detection results
        """
        parameters = {
            "max_iterations": max_iterations,
            "save_to_graph": True,
            "result_property": "community_id"
        }
        
        job_id = await self.submit_graph_job(algorithm, tenant_id, parameters)
        
        return {
            "job_id": job_id,
            "status": "submitted",
            "algorithm": algorithm,
            "parameters": parameters
        }
    
    async def find_shortest_paths(self,
                                 tenant_id: str,
                                 source_vertices: List[str],
                                 target_vertices: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Find shortest paths in the graph
        
        :param tenant_id: Tenant ID
        :param source_vertices: Source vertex IDs
        :param target_vertices: Target vertex IDs (None for all)
        :return: Shortest paths results
        """
        parameters = {
            "source_vertices": source_vertices,
            "target_vertices": target_vertices,
            "save_to_graph": False  # Results typically too large
        }
        
        job_id = await self.submit_graph_job("shortest_paths", tenant_id, parameters)
        
        return {
            "job_id": job_id,
            "status": "submitted",
            "algorithm": "shortest_paths",
            "parameters": parameters
        }
    
    async def analyze_graph_structure(self, tenant_id: str) -> Dict[str, Any]:
        """
        Comprehensive graph structure analysis
        
        :param tenant_id: Tenant ID
        :return: Structural analysis results
        """
        # Submit multiple analysis jobs in parallel
        jobs = []
        
        # Degree distribution
        degree_job = await self.submit_graph_job(
            "degree_distribution",
            tenant_id,
            {"save_to_graph": False}
        )
        jobs.append(("degree_distribution", degree_job))
        
        # Connected components
        cc_job = await self.submit_graph_job(
            "connected_components",
            tenant_id,
            {"save_to_graph": True, "result_property": "component_id"}
        )
        jobs.append(("connected_components", cc_job))
        
        # Triangle count
        triangle_job = await self.submit_graph_job(
            "triangle_count",
            tenant_id,
            {"save_to_graph": True, "result_property": "triangle_count"}
        )
        jobs.append(("triangle_count", triangle_job))
        
        return {
            "analysis_id": f"analysis_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            "jobs": [
                {
                    "algorithm": algo,
                    "job_id": job_id,
                    "status": "submitted"
                }
                for algo, job_id in jobs
            ]
        }
    
    async def run_custom_analysis(self,
                                 tenant_id: str,
                                 spark_code: str,
                                 parameters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run custom Spark/GraphX analysis code
        
        :param tenant_id: Tenant ID
        :param spark_code: Custom Spark code to execute
        :param parameters: Parameters for the code
        :return: Analysis results
        """
        # Security: This should be carefully validated and sandboxed
        # In production, only allow pre-approved code templates
        
        raise NotImplementedError("Custom code execution not yet implemented")


class GraphXEnhancedQueries:
    """Enhanced graph queries leveraging Spark results"""
    
    def __init__(self, janusgraph_client, spark_analytics: SparkGraphAnalytics):
        self.janusgraph = janusgraph_client
        self.spark = spark_analytics
    
    async def get_top_influencers(self, tenant_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get top influencers based on PageRank scores
        
        :param tenant_id: Tenant ID
        :param limit: Number of results
        :return: List of top influencers
        """
        query = """
        g.V().has('tenant_id', tenant_id)
            .has('pagerank_score')
            .order().by('pagerank_score', desc)
            .limit(limit)
            .project('id', 'type', 'name', 'score')
                .by(id)
                .by(label)
                .by('name')
                .by('pagerank_score')
        """
        
        bindings = {
            'tenant_id': tenant_id,
            'limit': limit
        }
        
        result = await self.janusgraph.submit(query, bindings)
        return result.all().result()
    
    async def get_community_members(self, tenant_id: str, community_id: int) -> List[Dict[str, Any]]:
        """
        Get all members of a community
        
        :param tenant_id: Tenant ID
        :param community_id: Community ID
        :return: List of community members
        """
        query = """
        g.V().has('tenant_id', tenant_id)
            .has('community_id', community_id)
            .project('id', 'type', 'name')
                .by(id)
                .by(label)
                .by('name')
        """
        
        bindings = {
            'tenant_id': tenant_id,
            'community_id': community_id
        }
        
        result = await self.janusgraph.submit(query, bindings)
        return result.all().result()
    
    async def get_influence_path(self, 
                                tenant_id: str,
                                source_id: str,
                                target_id: str) -> List[Dict[str, Any]]:
        """
        Get the influence path between two nodes
        
        :param tenant_id: Tenant ID
        :param source_id: Source node ID
        :param target_id: Target node ID
        :return: Path of influence
        """
        # This combines shortest path with PageRank scores
        query = """
        g.V(source_id).has('tenant_id', tenant_id)
            .repeat(outE().inV().simplePath())
            .until(id().is(target_id).or().loops().is(gte(5)))
            .path()
            .by(project('id', 'score')
                .by(id)
                .by(coalesce(values('pagerank_score'), constant(0.0))))
        """
        
        bindings = {
            'tenant_id': tenant_id,
            'source_id': source_id,
            'target_id': target_id
        }
        
        result = await self.janusgraph.submit(query, bindings)
        return result.all().result() 