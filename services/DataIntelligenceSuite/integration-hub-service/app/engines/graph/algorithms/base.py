"""
Base Algorithm Class

Base class for all GraphX analytics algorithms.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from graphframes import GraphFrame

from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class BaseAlgorithm(ABC):
    """
    Base class for graph analytics algorithms
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.name = self.__class__.__name__
        self.parameters = {}
        self.results = None
    
    @abstractmethod
    async def run(self, graph: GraphFrame, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run the algorithm on the graph
        
        Args:
            graph: GraphFrame containing vertices and edges
            params: Algorithm-specific parameters
            
        Returns:
            Dictionary containing algorithm results
        """
        pass
    
    @abstractmethod
    def validate_params(self, params: Dict[str, Any]) -> bool:
        """
        Validate algorithm parameters
        
        Args:
            params: Parameters to validate
            
        Returns:
            True if valid, raises exception otherwise
        """
        pass
    
    def preprocess_graph(self, graph: GraphFrame) -> GraphFrame:
        """
        Preprocess the graph if needed
        
        Args:
            graph: Input GraphFrame
            
        Returns:
            Preprocessed GraphFrame
        """
        return graph
    
    def postprocess_results(self, results: Any) -> Dict[str, Any]:
        """
        Postprocess algorithm results
        
        Args:
            results: Raw algorithm results
            
        Returns:
            Formatted results dictionary
        """
        return {"raw_results": results}
    
    def get_default_params(self) -> Dict[str, Any]:
        """
        Get default parameters for the algorithm
        
        Returns:
            Dictionary of default parameters
        """
        return {}
    
    def merge_params(self, user_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge user parameters with defaults
        
        Args:
            user_params: User-provided parameters
            
        Returns:
            Merged parameters
        """
        params = self.get_default_params()
        params.update(user_params)
        return params
    
    def create_subgraph(self, graph: GraphFrame, vertex_filter: str = None,
                       edge_filter: str = None) -> GraphFrame:
        """
        Create a subgraph based on filters
        
        Args:
            graph: Original GraphFrame
            vertex_filter: SQL filter for vertices
            edge_filter: SQL filter for edges
            
        Returns:
            Filtered GraphFrame
        """
        vertices = graph.vertices
        edges = graph.edges
        
        if vertex_filter:
            vertices = vertices.filter(vertex_filter)
        
        if edge_filter:
            edges = edges.filter(edge_filter)
        
        return GraphFrame(vertices, edges)
    
    def save_results(self, results: DataFrame, output_path: str,
                    format: str = "parquet"):
        """
        Save results to storage
        
        Args:
            results: Results DataFrame
            output_path: Storage path
            format: Output format (parquet, json, csv)
        """
        try:
            results.write.mode("overwrite").format(format).save(output_path)
            logger.info(f"Saved {self.name} results to {output_path}")
        except Exception as e:
            logger.error(f"Failed to save results: {e}")
            raise
    
    def get_execution_metrics(self) -> Dict[str, Any]:
        """
        Get algorithm execution metrics
        
        Returns:
            Dictionary of metrics
        """
        return {
            "algorithm": self.name,
            "parameters": self.parameters,
            "result_count": len(self.results) if self.results else 0
        } 