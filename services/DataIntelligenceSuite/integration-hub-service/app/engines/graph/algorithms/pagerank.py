"""
PageRank Algorithm

Implementation of PageRank algorithm for graph analytics.
"""

from typing import Dict, Any
from graphframes import GraphFrame

from .base import BaseAlgorithm
from data_intelligence_common import StructuredLogger

logger = StructuredLogger.get_logger(__name__)


class PageRankAlgorithm(BaseAlgorithm):
    """
    PageRank algorithm implementation using GraphFrames
    """
    
    def get_default_params(self) -> Dict[str, Any]:
        """Get default PageRank parameters"""
        return {
            "resetProbability": 0.15,
            "maxIter": 20,
            "tol": 0.01,
            "sourceId": None,  # For personalized PageRank
            "sinkId": None
        }
    
    def validate_params(self, params: Dict[str, Any]) -> bool:
        """Validate PageRank parameters"""
        reset_prob = params.get("resetProbability", 0.15)
        if not 0 < reset_prob < 1:
            raise ValueError("resetProbability must be between 0 and 1")
        
        max_iter = params.get("maxIter", 20)
        if max_iter < 1:
            raise ValueError("maxIter must be at least 1")
        
        tol = params.get("tol", 0.01)
        if tol <= 0:
            raise ValueError("tol must be positive")
        
        return True
    
    async def run(self, graph: GraphFrame, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run PageRank algorithm
        
        Args:
            graph: Input GraphFrame
            params: Algorithm parameters
            
        Returns:
            Dictionary with PageRank results
        """
        logger.info(f"Running PageRank with params: {params}")
        
        # Merge and validate parameters
        self.parameters = self.merge_params(params)
        self.validate_params(self.parameters)
        
        try:
            # Run PageRank
            if self.parameters.get("sourceId"):
                # Personalized PageRank
                results = graph.pageRank(
                    resetProbability=self.parameters["resetProbability"],
                    sourceId=self.parameters["sourceId"],
                    maxIter=self.parameters["maxIter"],
                    tol=self.parameters["tol"]
                )
            else:
                # Standard PageRank
                results = graph.pageRank(
                    resetProbability=self.parameters["resetProbability"],
                    maxIter=self.parameters["maxIter"],
                    tol=self.parameters["tol"]
                )
            
            # Extract vertex scores
            vertex_scores = results.vertices.select("id", "pagerank")
            
            # Get top vertices by PageRank
            top_vertices = vertex_scores.orderBy("pagerank", ascending=False).limit(100)
            
            # Calculate statistics
            stats = vertex_scores.agg({
                "pagerank": "avg",
                "pagerank": "max",
                "pagerank": "min",
                "pagerank": "stddev"
            }).collect()[0]
            
            # Store results
            self.results = {
                "vertices": vertex_scores,
                "edges": results.edges
            }
            
            # Return formatted results
            return {
                "algorithm": "pagerank",
                "parameters": self.parameters,
                "top_vertices": [
                    {"id": row.id, "score": row.pagerank}
                    for row in top_vertices.collect()
                ],
                "statistics": {
                    "avg_score": stats[0],
                    "max_score": stats[1],
                    "min_score": stats[2],
                    "stddev_score": stats[3]
                },
                "vertex_count": vertex_scores.count()
            }
            
        except Exception as e:
            logger.error(f"PageRank failed: {e}")
            raise
    
    def get_influential_vertices(self, threshold: float = 0.01) -> list:
        """
        Get vertices with PageRank above threshold
        
        Args:
            threshold: PageRank score threshold
            
        Returns:
            List of influential vertices
        """
        if not self.results:
            return []
        
        return self.results["vertices"].filter(
            f"pagerank > {threshold}"
        ).collect() 