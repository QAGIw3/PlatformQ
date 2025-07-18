"""Cassandra Client"""

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging

logger = logging.getLogger(__name__)


class CassandraClient:
    """Client for Apache Cassandra"""
    
    def __init__(self, contact_points: list = None):
        self.contact_points = contact_points or ['cassandra']
        self.cluster = Cluster(self.contact_points)
        self.session = self.cluster.connect()
    
    async def execute(self, query: str, parameters: tuple = None):
        """Execute CQL query"""
        return self.session.execute(query, parameters)
    
    async def close(self):
        self.cluster.shutdown() 