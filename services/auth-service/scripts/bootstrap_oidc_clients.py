import logging
import sys
import os
from cassandra.cluster import Cluster

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.core.security import get_password_hash

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def bootstrap_oidc_clients():
    """
    Populates the oidc_clients table with initial clients.
    """
    try:
        cluster = Cluster(['localhost'], port=9042)
        session = cluster.connect()
        session.set_keyspace('auth_keyspace')
        
        # --- Nextcloud Client ---
        client_id = "nextcloud"
        client_secret = "nextcloud-secret" # This should be stored in Vault for the Nextcloud app
        client_name = "Nextcloud"
        redirect_uris = ["http://localhost:8080/apps/oidc/callback"] # Example
        allowed_scopes = ["openid", "profile", "email"]
        
        query = "INSERT INTO oidc_clients (client_id, client_secret_hash, client_name, redirect_uris, allowed_scopes) VALUES (%s, %s, %s, %s, %s)"
        prepared = session.prepare(query)
        session.execute(prepared, [client_id, get_password_hash(client_secret), client_name, redirect_uris, allowed_scopes])
        logger.info(f"  - Registered OIDC client: {client_name}")

        # --- Zulip Client ---
        client_id = "zulip"
        client_secret = "zulip-secret"
        client_name = "Zulip"
        redirect_uris = ["http://<ZULIP_IP>/complete/oidc/"] # Replace with your Zulip instance's callback URL
        allowed_scopes = ["openid", "profile", "email"]
        
        session.execute(prepared, [client_id, get_password_hash(client_secret), client_name, redirect_uris, allowed_scopes])
        logger.info(f"  - Registered OIDC client: {client_name}")

        # --- OpenProject Client ---
        client_id = "openproject"
        client_secret = "openproject-secret"
        client_name = "OpenProject"
        redirect_uris = ["http://<OPENPROJECT_IP>/auth/openid_connect/callback"] # Replace with your instance's callback URL
        allowed_scopes = ["openid", "profile", "email"]
        
        session.execute(prepared, [client_id, get_password_hash(client_secret), client_name, redirect_uris, allowed_scopes])
        logger.info(f"  - Registered OIDC client: {client_name}")

        # --- OpenKM Client ---
        client_id = "openkm"
        client_secret = "openkm-secret"
        client_name = "OpenKM"
        redirect_uris = ["http://localhost:8081/OpenKM/oidc"] # Example callback URL
        allowed_scopes = ["openid", "profile", "email"]
        
        session.execute(prepared, [client_id, get_password_hash(client_secret), client_name, redirect_uris, allowed_scopes])
        logger.info(f"  - Registered OIDC client: {client_name}")

        # --- Superset Client ---
        client_id = "superset"
        client_secret = "superset-secret"
        client_name = "Apache Superset"
        redirect_uris = ["http://<SUPERSET_IP>/oauth-authorized/platformq"] # Callback URL
        allowed_scopes = ["openid", "profile", "email", "roles"]
        
        session.execute(prepared, [client_id, get_password_hash(client_secret), client_name, redirect_uris, allowed_scopes])
        logger.info(f"  - Registered OIDC client: {client_name}")

        # --- MLflow Client ---
        client_id = "mlflow"
        client_secret = "mlflow-secret"
        client_name = "MLflow"
        redirect_uris = ["http://<MLFLOW_IP>/oauth2/callback"] # Replace with your instance's callback URL
        allowed_scopes = ["openid", "profile", "email"]
        
        session.execute(prepared, [client_id, get_password_hash(client_secret), client_name, redirect_uris, allowed_scopes])
        logger.info(f"  - Registered OIDC client: {client_name}")

        logger.info("OIDC client bootstrap complete.")

    except Exception as e:
        logger.error(f"Failed to bootstrap OIDC clients: {e}")
        raise
    finally:
        if 'cluster' in locals():
            cluster.shutdown()

if __name__ == "__main__":
    bootstrap_oidc_clients() 