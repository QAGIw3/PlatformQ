from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.driver.aiohttp.transport import AiohttpTransport
import logging
from ..core.config import settings

logger = logging.getLogger(__name__)

class JanusGraph:
    def __init__(self):
        self.connection = None
        self.g = None

    def connect(self):
        """
        Establishes a connection to the JanusGraph server.
        """
        if self.g and not self.connection.closed:
            logger.info("Reusing existing JanusGraph connection.")
            return

        try:
            # Using AiohttpTransport for asyncio compatibility
            self.connection = DriverRemoteConnection(
                settings.JANUSGRAPH_URL,
                'g',
                transport_factory=lambda: AiohttpTransport()
            )
            self.g = Graph().traversal().withRemote(self.connection)
            logger.info("Successfully connected to JanusGraph.")
        except Exception as e:
            logger.error(f"Failed to connect to JanusGraph: {e}")
            self.close()
            raise

    def close(self):
        """
        Closes the connection to the JanusGraph server.
        """
        if self.connection and not self.connection.closed:
            self.connection.close()
            logger.info("JanusGraph connection closed.")
            self.g = None
            self.connection = None

    def __enter__(self):
        self.connect()
        return self.g

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 