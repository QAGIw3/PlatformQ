"""
Base class for GraphX operations integrated with JanusGraph

This module provides the foundation for running distributed graph
analytics using Spark GraphX on PlatformQ's graph data.
"""

import json
import logging
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, collect_list, struct
from py2neo import Graph as Neo4jGraph
from gremlin_python.driver import client, serializer

logger = logging.getLogger(__name__)


class GraphXBase(ABC):
    """Base class for GraphX analytics operations"""
    
    def __init__(self, tenant_id: str, spark: Optional[SparkSession] = None):
        """
        Initialize GraphX base
        
        :param tenant_id: Tenant ID for multi-tenant isolation
        :param spark: SparkSession (created if not provided)
        """
        self.tenant_id = tenant_id
        self._spark = spark
        self._janusgraph_client = None
        
    @property
    def spark(self) -> SparkSession:
        """Get or create Spark session"""
        if self._spark is None:
            self._spark = self._create_spark_session()
        return self._spark
    
    def _create_spark_session(self) -> SparkSession:
        """Create Spark session with GraphX support"""
        return SparkSession.builder \
            .appName(f"GraphX-{self.__class__.__name__}-{self.tenant_id}") \
            .config("spark.jars", "/opt/spark/jars/janusgraph-spark.jar") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.kryo.registrator", "org.apache.spark.graphx.GraphKryoRegistrator") \
            .getOrCreate()
    
    @property
    def janusgraph(self):
        """Get JanusGraph client"""
        if self._janusgraph_client is None:
            self._janusgraph_client = client.Client(
                'ws://janusgraph:8182/gremlin',
                'g',
                message_serializer=serializer.GraphSONSerializersV3d0()
            )
        return self._janusgraph_client
    
    def load_graph_from_janusgraph(self, 
                                   vertex_label: Optional[str] = None,
                                   edge_label: Optional[str] = None) -> Tuple[DataFrame, DataFrame]:
        """
        Load graph data from JanusGraph into DataFrames
        
        :param vertex_label: Filter vertices by label
        :param edge_label: Filter edges by label
        :return: Tuple of (vertices_df, edges_df)
        """
        # Load vertices
        vertex_query = f"g.V().hasLabel('{vertex_label}')" if vertex_label else "g.V()"
        vertex_query += f".has('tenant_id', '{self.tenant_id}')"
        vertex_query += ".project('id', 'label', 'properties').by(id).by(label).by(valueMap())"
        
        vertices_result = self.janusgraph.submit(vertex_query).all().result()
        
        # Convert to DataFrame
        vertices_data = []
        for v in vertices_result:
            vertex_data = {
                'id': v['id'],
                'label': v['label'],
                'tenant_id': self.tenant_id
            }
            # Flatten properties
            for key, values in v.get('properties', {}).items():
                vertex_data[key] = values[0] if values else None
            vertices_data.append(vertex_data)
        
        vertices_df = self.spark.createDataFrame(vertices_data)
        
        # Load edges
        edge_query = f"g.E().hasLabel('{edge_label}')" if edge_label else "g.E()"
        edge_query += f".has('tenant_id', '{self.tenant_id}')"
        edge_query += ".project('id', 'label', 'outV', 'inV', 'properties')"
        edge_query += ".by(id).by(label).by(outV().id()).by(inV().id()).by(valueMap())"
        
        edges_result = self.janusgraph.submit(edge_query).all().result()
        
        # Convert to DataFrame
        edges_data = []
        for e in edges_result:
            edge_data = {
                'id': e['id'],
                'src': e['outV'],
                'dst': e['inV'],
                'label': e['label'],
                'tenant_id': self.tenant_id
            }
            # Flatten properties
            for key, values in e.get('properties', {}).items():
                edge_data[key] = values[0] if values else None
            edges_data.append(edge_data)
        
        edges_df = self.spark.createDataFrame(edges_data)
        
        return vertices_df, edges_df
    
    def save_results_to_janusgraph(self, results_df: DataFrame, 
                                   property_name: str,
                                   vertex_id_col: str = 'id'):
        """
        Save analysis results back to JanusGraph as vertex properties
        
        :param results_df: DataFrame with analysis results
        :param property_name: Name of the property to store results
        :param vertex_id_col: Column name containing vertex IDs
        """
        # Collect results
        results = results_df.collect()
        
        # Batch update vertices
        batch_size = 100
        for i in range(0, len(results), batch_size):
            batch = results[i:i + batch_size]
            
            # Build Gremlin query for batch update
            query = "g"
            for row in batch:
                vertex_id = row[vertex_id_col]
                value = row[property_name] if property_name in row else row.asDict()
                
                query += f".V({vertex_id}).property('{property_name}', {json.dumps(value)})"
            
            # Execute batch update
            self.janusgraph.submit(query).all().result()
    
    def create_graphx_graph(self, vertices_df: DataFrame, edges_df: DataFrame):
        """
        Create GraphX graph from DataFrames
        
        Note: This requires Scala/Java interop which is complex in pure Python.
        In practice, this would use the GraphFrames library or custom Scala code.
        """
        # This is a placeholder - actual implementation would use GraphFrames
        # or custom Scala/Java code for GraphX integration
        logger.warning("GraphX graph creation requires Scala interop - using GraphFrames instead")
        
        # For now, we'll work with DataFrames directly
        return vertices_df, edges_df
    
    @abstractmethod
    def run_analysis(self, vertices_df: DataFrame, edges_df: DataFrame) -> DataFrame:
        """
        Run the specific graph analysis algorithm
        
        :param vertices_df: Vertices DataFrame
        :param edges_df: Edges DataFrame
        :return: Results DataFrame
        """
        pass
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """
        Main execution method
        
        :param kwargs: Algorithm-specific parameters
        :return: Analysis results
        """
        start_time = datetime.utcnow()
        
        try:
            # Load graph from JanusGraph
            logger.info(f"Loading graph for tenant {self.tenant_id}")
            vertices_df, edges_df = self.load_graph_from_janusgraph(
                vertex_label=kwargs.get('vertex_label'),
                edge_label=kwargs.get('edge_label')
            )
            
            logger.info(f"Loaded {vertices_df.count()} vertices and {edges_df.count()} edges")
            
            # Run analysis
            results_df = self.run_analysis(vertices_df, edges_df, **kwargs)
            
            # Save results if requested
            if kwargs.get('save_to_graph', True):
                property_name = kwargs.get('result_property', f"{self.__class__.__name__.lower()}_score")
                self.save_results_to_janusgraph(results_df, property_name)
            
            # Collect top results
            top_k = kwargs.get('top_k', 100)
            top_results = results_df.orderBy(col(property_name).desc()).limit(top_k).collect()
            
            return {
                'status': 'success',
                'algorithm': self.__class__.__name__,
                'tenant_id': self.tenant_id,
                'vertices_analyzed': vertices_df.count(),
                'edges_analyzed': edges_df.count(),
                'top_results': [row.asDict() for row in top_results],
                'execution_time': (datetime.utcnow() - start_time).total_seconds()
            }
            
        except Exception as e:
            logger.error(f"Error in graph analysis: {str(e)}")
            return {
                'status': 'error',
                'algorithm': self.__class__.__name__,
                'tenant_id': self.tenant_id,
                'error': str(e),
                'execution_time': (datetime.utcnow() - start_time).total_seconds()
            }
        finally:
            if self._janusgraph_client:
                self._janusgraph_client.close()


class GraphFramesBase(GraphXBase):
    """Base class using GraphFrames for easier Python integration"""
    
    def create_graph_frame(self, vertices_df: DataFrame, edges_df: DataFrame):
        """Create a GraphFrame from vertices and edges DataFrames"""
        from graphframes import GraphFrame
        
        # Ensure required columns exist
        if 'id' not in vertices_df.columns:
            raise ValueError("Vertices DataFrame must have 'id' column")
        
        if 'src' not in edges_df.columns or 'dst' not in edges_df.columns:
            raise ValueError("Edges DataFrame must have 'src' and 'dst' columns")
        
        # Create GraphFrame
        graph = GraphFrame(vertices_df, edges_df)
        
        return graph 