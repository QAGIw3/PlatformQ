from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
import os

CASSANDRA_HOSTS = os.environ.get("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_KEYSPACE = "digital_assets_keyspace"

def get_cassandra_session() -> Session:
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect()
    
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }}
        AND durable_writes = true;
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)
    
    return session

def create_tables(session: Session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS digital_assets (
            id uuid PRIMARY KEY,
            name text,
            description text,
            s3_url text,
            owner_id text
        );
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS peer_reviews (
            id uuid PRIMARY KEY,
            asset_id uuid,
            reviewer_id text,
            review_content text,
            created_at timestamp
        );
    """)
    # It's often useful to have a way to look up reviews by asset
    session.execute("""
        CREATE INDEX IF NOT EXISTS on peer_reviews (asset_id);
    """)

try:
    session = get_cassandra_session()
    create_tables(session)
    session.shutdown()
except Exception as e:
    print(f"Failed to connect to Cassandra or create tables: {e}") 