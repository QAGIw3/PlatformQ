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
        self.job_store = {}  # In production, use persistent storage
    
    async def submit_job(self, job_type: str, tenant_id: str, 
                              parameters: Dict[str, Any]) -> str:
        """
        Submit a Spark job for graph analytics
        
        :param job_type: Type of job (pagerank, community_detection, etc.)
        :param tenant_id: Tenant ID for isolation
        :param parameters: Job-specific parameters
        :return: Job ID
        """
        job_id = f"{job_type}_{tenant_id}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        
        # Prepare job configuration
        job_config = self._prepare_job_config(job_type, tenant_id, parameters)
        
        # Submit job asynchronously
        loop = asyncio.get_event_loop()
        future = loop.run_in_executor(
            self._executor,
            self._submit_spark_job,
            job_id,
            job_config
        )
        
        # Store job info
        self.job_store[job_id] = {
            'status': 'submitted',
            'type': job_type,
            'tenant_id': tenant_id,
            'parameters': parameters,
            'submitted_at': datetime.utcnow().isoformat()
        }
        
        # Don't wait for completion
        asyncio.create_task(self._monitor_job(job_id, future))
        
        return job_id
    
    def _prepare_job_config(self, job_type: str, tenant_id: str, 
                            parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare Spark job configuration"""
        
        # Map job types to Python files
        job_map = {
            'pagerank': '/opt/spark/jobs/graphx/pagerank.py',
            'community_detection': '/opt/spark/jobs/graphx/community_detection.py',
            'fraud_detection': '/opt/spark/jobs/graphx/fraud_detection.py',
            'recommendation_engine': '/opt/spark/jobs/graphx/recommendations.py'
        }
        
        if job_type not in job_map:
            raise ValueError(f"Unknown job type: {job_type}")
        
        # Base configuration
        config = {
            'action': 'CreateSubmissionRequest',
            'appResource': job_map[job_type],
            'clientSparkVersion': '3.2.0',
            'appArgs': [tenant_id, json.dumps(parameters)],
            'environmentVariables': {
                'TENANT_ID': tenant_id,
                'JANUSGRAPH_URL': 'ws://janusgraph:8182/gremlin',
                'CASSANDRA_HOST': 'cassandra',
                'IGNITE_NODES': 'ignite-0.ignite:10800,ignite-1.ignite:10800'
            },
            'mainClass': 'org.apache.spark.deploy.SparkSubmit',
            'sparkProperties': {
                'spark.app.name': f'GraphAnalytics-{job_type}-{tenant_id}',
                'spark.master': self.spark_master_url,
                'spark.submit.deployMode': 'cluster',
                'spark.executor.memory': '2g',
                'spark.executor.cores': '2',
                'spark.executor.instances': '3',
                'spark.driver.memory': '2g',
                'spark.driver.cores': '2',
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                'spark.kryo.registrator': 'org.apache.spark.graphx.GraphKryoRegistrator',
                'spark.jars': '/opt/spark/jars/janusgraph-spark.jar,/opt/spark/jars/graphframes.jar'
            }
        }
        
        # Job-specific configurations
        if job_type == 'pagerank':
            config['sparkProperties']['spark.graphx.pregel.checkpointInterval'] = '10'
        elif job_type == 'community_detection':
            config['sparkProperties']['spark.graphx.iterations'] = parameters.get('max_iter', '20')
        elif job_type == 'fraud_detection':
            config['sparkProperties']['spark.ml.maxIter'] = '100'
            
        return config
    
    def _submit_spark_job(self, job_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Submit job to Spark cluster"""
        try:
            # In production, use proper Spark submission API
            # For now, simulate with HTTP request to Spark REST API
            response = httpx.post(
                self.spark_submit_url,
                json=config,
                timeout=30.0
            )
            
            if response.status_code == 200:
                result = response.json()
                return {
                    'submission_id': result.get('submissionId'),
                    'success': result.get('success', False),
                    'message': result.get('message', '')
                }
            else:
                raise Exception(f"Job submission failed: {response.text}")
                
        except Exception as e:
            logger.error(f"Error submitting Spark job {job_id}: {e}")
            return {
                'submission_id': None,
                'success': False,
                'message': str(e)
            }
            
    async def _monitor_job(self, job_id: str, future):
        """Monitor job execution"""
        try:
            # Wait for submission result
            result = await future
            
            if result['success']:
                self.job_store[job_id]['status'] = 'running'
                self.job_store[job_id]['submission_id'] = result['submission_id']
                
                # Poll for completion
                await self._poll_job_status(job_id, result['submission_id'])
            else:
                self.job_store[job_id]['status'] = 'failed'
                self.job_store[job_id]['error'] = result['message']
                
        except Exception as e:
            logger.error(f"Error monitoring job {job_id}: {e}")
            self.job_store[job_id]['status'] = 'error'
            self.job_store[job_id]['error'] = str(e)
    
    async def _poll_job_status(self, job_id: str, submission_id: str):
        """Poll Spark for job status"""
        max_attempts = 60  # 5 minutes with 5 second intervals
        
        for attempt in range(max_attempts):
            try:
                # Check job status
                response = httpx.get(
                    f"{self.spark_submit_url}/status/{submission_id}",
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    status_data = response.json()
                    driver_state = status_data.get('driverState', 'UNKNOWN')
                    
                    if driver_state == 'FINISHED':
                        self.job_store[job_id]['status'] = 'completed'
                        # Fetch results from output location
                        await self._fetch_job_results(job_id)
                        break
                    elif driver_state in ['FAILED', 'KILLED', 'ERROR']:
                        self.job_store[job_id]['status'] = 'failed'
                        self.job_store[job_id]['error'] = status_data.get('message', 'Job failed')
                        break
                        
                # Wait before next poll
                await asyncio.sleep(5)
                
            except Exception as e:
                logger.error(f"Error polling job status: {e}")
                
        if self.job_store[job_id]['status'] == 'running':
            self.job_store[job_id]['status'] = 'timeout'
    
    async def _fetch_job_results(self, job_id: str):
        """Fetch job results from storage"""
        try:
            # Results would be stored in MinIO/S3 or Cassandra
            # For now, simulate result fetching
            job_type = self.job_store[job_id]['type']
            
            if job_type == 'pagerank':
                self.job_store[job_id]['results'] = {
                    'top_nodes': [
                        {'id': 'node1', 'pagerank_score': 0.95},
                        {'id': 'node2', 'pagerank_score': 0.87}
                    ],
                    'execution_time': 45.2
                }
            elif job_type == 'fraud_detection':
                self.job_store[job_id]['results'] = {
                    'pattern_analysis': {
                        'total_entities': 1000,
                        'suspicious_entities': 23,
                        'fraud_rate': 0.023
                    },
                    'suspicious_entities': ['entity1', 'entity2']
                }
                
        except Exception as e:
            logger.error(f"Error fetching job results: {e}")
            self.job_store[job_id]['error'] = f"Failed to fetch results: {e}"
    
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get current job status"""
        return self.job_store.get(job_id)
    
    async def get_job_results(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job results if available"""
        job_info = self.job_store.get(job_id)
        
        if job_info and job_info['status'] == 'completed':
            return job_info.get('results')
        
        return None
    
    async def analyze_graph_structure(self, tenant_id: str) -> Dict[str, Any]:
        """
        Run comprehensive structural analysis on the graph
        
        :param tenant_id: Tenant ID
        :return: Analysis results
        """
        # Submit multiple analysis jobs
        jobs = []
        
        # PageRank for influence
        pagerank_job = await self.submit_job('pagerank', tenant_id, {
            'tolerance': 0.001,
            'max_iterations': 20
        })
        jobs.append(('pagerank', pagerank_job))
        
        # Community detection
        community_job = await self.submit_job('community_detection', tenant_id, {
            'algorithm': 'louvain',
            'resolution': 1.0
        })
        jobs.append(('community', community_job))
        
        # Wait for jobs to complete (with timeout)
        results = {}
        max_wait = 300  # 5 minutes
        start_time = datetime.utcnow()
        
        while (datetime.utcnow() - start_time).seconds < max_wait:
            all_complete = True
            
            for job_type, job_id in jobs:
                status = await self.get_job_status(job_id)
                
                if status['status'] == 'completed':
                    results[job_type] = await self.get_job_results(job_id)
                elif status['status'] in ['failed', 'error']:
                    results[job_type] = {'error': status.get('error', 'Job failed')}
                else:
                    all_complete = False
            
            if all_complete:
                break
                
            await asyncio.sleep(5)
        
        # Combine results
        return {
            'tenant_id': tenant_id,
            'analysis_timestamp': datetime.utcnow().isoformat(),
            'pagerank_analysis': results.get('pagerank', {}),
            'community_analysis': results.get('community', {}),
            'graph_metrics': await self._get_basic_graph_metrics(tenant_id)
        }
    
    async def _get_basic_graph_metrics(self, tenant_id: str) -> Dict[str, Any]:
        """Get basic graph metrics from JanusGraph"""
        try:
            from gremlin_python.driver import client, serializer
            
            gremlin_client = client.Client(
                'ws://janusgraph:8182/gremlin',
                'g',
                message_serializer=serializer.GraphSONSerializersV3d0()
            )
            
            # Get node and edge counts
            metrics_query = f"""
                g.V().has('tenant_id', '{tenant_id}').count().as('nodes')
                .V().has('tenant_id', '{tenant_id}').outE().count().as('edges')
                .select('nodes', 'edges')
            """
            
            result = gremlin_client.submit(metrics_query).all().result()
            gremlin_client.close()
            
            if result:
                metrics = result[0]
                nodes = metrics.get('nodes', 0)
                edges = metrics.get('edges', 0)
                
                return {
                    'total_nodes': nodes,
                    'total_edges': edges,
                    'average_degree': edges / nodes if nodes > 0 else 0,
                    'density': (2 * edges) / (nodes * (nodes - 1)) if nodes > 1 else 0
                }
            
            return {}
            
        except Exception as e:
            logger.error(f"Error getting graph metrics: {e}")
            return {}


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