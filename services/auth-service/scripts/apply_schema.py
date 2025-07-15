import logging
import os
import sys

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster

# Add the parent directory to the path to allow imports from the 'app' module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.core.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def apply_schema():
    """
    Connects to Cassandra and applies the schema defined in schema.cql.
    """
    try:
        auth_provider = PlainTextAuthProvider(
            username=settings.CASSANDRA_USER, password=settings.CASSANDRA_PASSWORD
        )
        contact_points = [h.strip() for h in settings.CASSANDRA_HOSTS.split(",")]

        cluster = Cluster(
            contact_points=contact_points,
            port=settings.CASSANDRA_PORT,
            auth_provider=auth_provider,
        )
        session = cluster.connect()
        logger.info("Successfully connected to Cassandra.")

        # Create Keyspace with NetworkTopologyStrategy
        logger.info(f"Creating keyspace '{settings.CASSANDRA_KEYSPACE}' if it does not exist...")
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {settings.CASSANDRA_KEYSPACE}
            WITH replication = {{ 
                'class': 'NetworkTopologyStrategy', 
                'dc1': '3',  -- Replication factor for datacenter 1
                'dc2': '3'   -- Replication factor for datacenter 2
            }}
            AND durable_writes = true;
        """)
        session.set_keyspace(settings.CASSANDRA_KEYSPACE)

        # Apply Schema from CQL file
        cql_file_path = os.path.join(os.path.dirname(__file__), "schema.cql")
        with open(cql_file_path, "r") as f:
            cql_script = f.read()

        statements = [s.strip() for s in cql_script.split(";") if s.strip()]
        logger.info("Applying schema from schema.cql...")
        for statement in statements:
            try:
                logger.info(f"Executing: {statement}")
                session.execute(statement)
            except Exception as e:
                logger.error(f"Failed to execute statement: {statement}\n{e}")
                raise

        logger.info("Schema applied successfully.")

    except Exception as e:
        logger.error(f"An error occurred during schema migration: {e}")
    finally:
        if "cluster" in locals() and cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed.")


if __name__ == "__main__":
    apply_schema()
