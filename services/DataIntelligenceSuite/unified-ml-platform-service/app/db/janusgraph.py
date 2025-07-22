"""
JanusGraph client for ML lineage tracking
"""

import logging
from typing import Optional
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.structure.graph import Graph

logger = logging.getLogger(__name__)


class JanusGraph:
    """Minimal JanusGraph client for ML lineage tracking"""
    
    def __init__(self):
        self.g = None
        self._connection = None
        
    def connect(self, gremlin_url: str = "ws://janusgraph:8182/gremlin"):
        """Connect to JanusGraph"""
        try:
            self._connection = DriverRemoteConnection(gremlin_url, 'g')
            self.g = Graph().traversal().withRemote(self._connection)
            logger.info(f"Connected to JanusGraph at {gremlin_url}")
        except Exception as e:
            logger.error(f"Failed to connect to JanusGraph: {e}")
            raise
            
    def close(self):
        """Close JanusGraph connection"""
        try:
            if self._connection:
                self._connection.close()
                logger.info("JanusGraph connection closed")
        except Exception as e:
            logger.error(f"Error closing JanusGraph connection: {e}")
            
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 