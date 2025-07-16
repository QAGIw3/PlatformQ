from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
import os

CASSANDRA_HOSTS = os.environ.get("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_KEYSPACE = "proposals_keyspace"

def get_cassandra_session() -> Session:
    # In a real app, you would use a more robust configuration system
    # and handle authentication properly.
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect()
    
    # Create keyspace if it doesn't exist
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
        AND durable_writes = true;
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)
    
    return session

def create_proposals_table(session: Session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS proposals (
            id uuid PRIMARY KEY,
            title text,
            description text,
            proposer text,
            targets list<text>,
            values list<int>,
            calldatas list<text>,
            is_onchain boolean,
            onchain_proposal_id text,
            status text
        );
    """)
    session.execute("CREATE INDEX IF NOT EXISTS on proposals (onchain_proposal_id);")

# This is a simple way to ensure the table exists when the service starts.
# In a production app, you might use a more sophisticated migration tool.
try:
    session = get_cassandra_session()
    create_proposals_table(session)
    session.shutdown()
except Exception as e:
    print(f"Failed to connect to Cassandra or create table: {e}") 