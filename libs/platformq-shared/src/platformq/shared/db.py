import logging

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session

logger = logging.getLogger(__name__)


class CassandraSessionManager:
    def __init__(self, hosts: list, port: int, user: str, password: str):
        self.hosts = hosts
        self.port = port
        self.user = user
        self.password = password
        self.cluster = None
        self.session = None

    def connect(self):
        try:
            auth_provider = PlainTextAuthProvider(
                username=self.user, password=self.password
            )

            self.cluster = Cluster(
                contact_points=self.hosts, port=self.port, auth_provider=auth_provider
            )
            self.session = self.cluster.connect()
            logger.info("Successfully connected to Cassandra.")
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {e}")
            raise

    def close(self):
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed.")

    def get_session(self) -> Session:
        if not self.session:
            self.connect()
        return self.session
