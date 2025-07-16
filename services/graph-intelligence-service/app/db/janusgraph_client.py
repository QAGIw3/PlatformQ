from janusgraph_python.driver import JanusGraph
from janusgraph_python.driver.Connection import JanusGraphConnection

# TODO: Move to config
JANUSGRAPH_HOST = "localhost"
JANUSGRAPH_PORT = 8182

class JanusGraphService:
    def __init__(self):
        try:
            self.graph = JanusGraph()
            self.connection = self.graph.connect(
                host=JANUSGRAPH_HOST,
                port=JANUSGRAPH_PORT,
                traversal_source="g"
            )
            self.g = self.connection.g
            print("Successfully connected to JanusGraph")
        except Exception as e:
            print(f"Failed to connect to JanusGraph: {e}")
            self.graph = None
            self.connection = None
            self.g = None

    def close(self):
        if self.connection:
            self.connection.close()
        if self.graph:
            self.graph.close()

janusgraph_service = JanusGraphService()

import atexit
atexit.register(janusgraph_service.close) 