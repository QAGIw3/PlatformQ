"""JanusGraph Client"""

from gremlin_python.driver import client
import logging

logger = logging.getLogger(__name__)


class JanusGraphClient:
    """Client for JanusGraph"""
    
    def __init__(self, url: str = "ws://janusgraph:8182/gremlin"):
        self.url = url
        self.client = client.Client(url, 'g')
    
    async def execute(self, query: str):
        """Execute Gremlin query"""
        return self.client.submit(query)
    
    async def close(self):
        self.client.close() 