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
            status text,
            is_cross_chain boolean,
            cross_chain_details map<text, text>,
            created_at timestamp,
            updated_at timestamp
        );
    """)
    session.execute("CREATE INDEX IF NOT EXISTS on proposals (onchain_proposal_id);")
    session.execute("CREATE INDEX IF NOT EXISTS on proposals (status);")
    session.execute("CREATE INDEX IF NOT EXISTS on proposals (is_cross_chain);")
    session.execute("CREATE INDEX IF NOT EXISTS on proposals (proposer);")

def create_votes_table(session: Session):
    """Create table for tracking votes"""
    session.execute("""
        CREATE TABLE IF NOT EXISTS proposal_votes (
            proposal_id uuid,
            voter_id text,
            chain_id text,
            vote_type text,  -- 'for', 'against', 'abstain'
            vote_weight text,
            transaction_hash text,
            voted_at timestamp,
            PRIMARY KEY ((proposal_id), voter_id, chain_id)
        );
    """)
    session.execute("CREATE INDEX IF NOT EXISTS on proposal_votes (voter_id);")

def initialize_database():
    """Initialize all database tables"""
    session = get_cassandra_session()
    create_proposals_table(session)
    create_votes_table(session)
    return session

# This is a simple way to ensure the table exists when the service starts.
# In a production app, you might use a more sophisticated migration tool.
try:
    session = get_cassandra_session()
    create_proposals_table(session)
    session.shutdown()
except Exception as e:
    print(f"Failed to connect to Cassandra or create table: {e}") 