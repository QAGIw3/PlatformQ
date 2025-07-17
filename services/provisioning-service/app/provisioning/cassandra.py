from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider

def create_cassandra_keyspace(session: Session, tenant_id: str):
    """
    Creates a new keyspace for a tenant.
    """
    keyspace_name = f"tenant_{tenant_id.replace('-', '_')}"
    
    # This is a simplified replication strategy. In a production environment,
    # you would want to use a more robust strategy, such as NetworkTopologyStrategy.
    replication_strategy = "{'class': 'SimpleStrategy', 'replication_factor': 1}"
    
    query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
    WITH replication = {replication_strategy}
    """
    
    session.execute(query)
    
    # Here you would also run any necessary table creation scripts
    # for the new keyspace. 