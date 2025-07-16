from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

class CassandraManager:
    def __init__(self, hosts, port, auth_provider=None, keyspace=None):
        if auth_provider:
            self.cluster = Cluster(hosts, port=port, auth_provider=auth_provider)
        else:
            self.cluster = Cluster(hosts, port=port)
        self.session = self.cluster.connect(keyspace)

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown() 